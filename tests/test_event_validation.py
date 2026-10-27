"""Bộ kiểm tra phải bắt được vi phạm hợp đồng, không chỉ chấp nhận dữ liệu tốt."""

import json
import time
from dataclasses import asdict
from pathlib import Path

import pytest

from ai_math_tutor.event_validation import DEFAULT_MATH_TIMEOUT_S, load_problems, validate_events, validate_iter
from ai_math_tutor.safe_math import SafeMath

SAMPLE = Path(__file__).resolve().parents[1] / "data_sample" / "events-week1-sample.jsonl"
PROBLEMS = SAMPLE.with_name("problems-week1-sample.jsonl")


def _first_event(event_type: str) -> dict:
    for line in SAMPLE.read_text().splitlines():
        event = json.loads(line)
        if event["event_type"] == event_type:
            return event
    raise LookupError(event_type)


def _validate(tmp_path, events, problems=None):
    path = tmp_path / "events.jsonl"
    path.write_text("".join(json.dumps(e) + "\n" for e in events))
    return validate_events(path, problems)


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda e: e.update(event_time="27/10/2026 08:00"), "event_time"),
        (lambda e: e.update(event_id="not-a-uuid"), "event_id"),
        (lambda e: e.update(learner_id="Nguyen Van A"), "learner_id"),
        (lambda e: e["payload"].update(is_correct="yes"), "payload"),
        (lambda e: e["payload"].pop("expression"), "payload"),
        (lambda e: e.update(email="a@b.c"), "<gốc>"),
    ],
)
def test_schema_violations_reported(tmp_path, mutate, message):
    event = _first_event("submit_step")
    mutate(event)
    report = _validate(tmp_path, [event])
    assert not report.ok
    assert any(message in error for error in report.errors), report.errors


def test_conflicting_duplicate_and_reused_seq_reported(tmp_path):
    original = _first_event("submit_step")
    conflicting = json.loads(json.dumps(original))
    conflicting["payload"]["expression"] = "x = 99"
    reused_seq = json.loads(json.dumps(original))
    reused_seq["event_id"] = "00000000-0000-4000-8000-000000000000"
    report = _validate(tmp_path, [original, conflicting, reused_seq])
    assert any("nội dung khác" in e for e in report.errors)
    assert any("session_seq" in e for e in report.errors)


def test_wrong_math_label_reported(tmp_path):
    event = _first_event("submit_step")
    event["payload"]["is_correct"] = not event["payload"]["is_correct"]
    report = _validate(tmp_path, [event], PROBLEMS)
    assert any("SymPy" in e for e in report.errors)


def test_sent_before_event_time_reported(tmp_path):
    event = _first_event("open_problem")
    event["sent_time"] = "2026-10-27T07:00:00.000Z"
    assert any("sent_time" in e for e in _validate(tmp_path, [event]).errors)


def _rows(path: Path) -> list[dict]:
    return [json.loads(line) for line in path.read_text().splitlines() if line.strip()]


def test_validate_iter_matches_validate_events_on_week1_sample():
    from_file = validate_events(SAMPLE, PROBLEMS)
    from_rows = validate_iter(_rows(SAMPLE), load_problems(PROBLEMS))
    assert asdict(from_rows) == asdict(from_file)
    assert from_rows.ok and from_rows.math_labels_checked == 12
    assert asdict(validate_iter(_rows(SAMPLE))) == asdict(validate_events(SAMPLE))


def test_validate_iter_reports_non_object_rows():
    report = validate_iter([None, ["not", "an", "event"]])
    assert report.lines == 2
    assert sum("<gốc>" in error for error in report.errors) == 2


def test_invalid_json_line_reported(tmp_path):
    path = tmp_path / "events.jsonl"
    path.write_text(json.dumps(_first_event("open_problem")) + "\n{broken\n")
    report = validate_events(path)
    assert report.lines == 2 and report.unique_events == 1
    assert any("dòng 2: JSON lỗi" in e for e in report.errors)


def test_malicious_expression_rejected_without_hanging():
    event = _first_event("submit_step")
    event["payload"]["expression"] = "9^9^9 = x"
    start = time.perf_counter()
    report = validate_iter([event], load_problems(PROBLEMS))
    assert time.perf_counter() - start < DEFAULT_MATH_TIMEOUT_S
    assert any("số mũ" in e for e in report.errors), report.errors
    assert report.math_labels_checked == 0


def _spin(*_args):
    while True:
        pass


def test_hanging_math_check_times_out_and_validation_continues():
    events = [_first_event("submit_step"), _first_event("complete")]
    with SafeMath(timeout_s=0.3, extra_functions={"diagnose": _spin}) as checker:
        checker.call("arith_step", "1 + 1 = 2")  # worker sẵn sàng trước khi đo
        start = time.perf_counter()
        report = validate_iter(events, load_problems(PROBLEMS), checker=checker)
        elapsed = time.perf_counter() - start
    # Hàm _spin không bao giờ trả về: chỉ cần chứng minh validator không treo. Biên rộng vì mỗi lần
    # quá hạn phải giết và khởi động lại worker, chậm hơn nhiều khi CPU đang bị tác vụ khác chiếm.
    assert elapsed < len(events) * 3.0, elapsed
    assert sum("không kiểm chứng được" in e for e in report.errors) == 2
    assert report.unique_events == 2 and report.math_labels_checked == 0


ARITHMETIC_PROBLEM = {
    "problem_id": "P-ARITH-001",
    "kind": "arithmetic",
    "answer": "30",
    "steps": ["48/2 = 24", "24 + 6 = 30"],
    "misconceptions": [{"type": "arithmetic_slip", "step_index": 0, "expression": "48/2 = 25"}],
}


@pytest.mark.parametrize(
    ("event_type", "field", "value", "is_correct", "ok"),
    [
        ("submit_step", "expression", "48/2 = 24", True, True),
        ("submit_step", "expression", "48/2 = 25", False, True),
        ("submit_step", "expression", "48/2 = 25", True, False),
        ("complete", "answer", "30", None, True),
        ("complete", "answer", "31", None, False),
        ("wrong_answer", "answer", "31", None, True),
        ("submit_step", "expression", "24", True, False),  # bước phải có '='
        ("complete", "answer", "x = 30", None, False),  # đáp án không chứa '='
    ],
)
def test_arithmetic_problem_labels_checked(event_type, field, value, is_correct, ok):
    event = _first_event(event_type)
    event["problem_id"] = ARITHMETIC_PROBLEM["problem_id"]
    event["payload"][field] = value
    if is_correct is not None:
        event["payload"]["is_correct"] = is_correct
    report = validate_iter([event], {ARITHMETIC_PROBLEM["problem_id"]: ARITHMETIC_PROBLEM})
    assert report.ok is ok, report.errors
    if ok:
        assert report.math_labels_checked == 1
