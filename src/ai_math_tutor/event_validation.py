"""Kiểm tra tệp sự kiện JSONL theo data contract LearnerEvent v0.2 (nhận cả sự kiện v0.1).

Dùng cho dữ liệu mẫu, đầu ra của bộ mô phỏng và đối soát sau khi chạy pipeline.
Báo cáo gồm lỗi (làm hỏng hợp đồng) và thống kê luồng (trùng, muộn, đảo thứ tự,
khoảng trống session_seq) để đối chiếu với ground truth của bộ mô phỏng.
Nhãn toán học được kiểm trong worker `safe_math` có thời hạn cứng, nên một sự
kiện độc hại không thể làm treo bộ kiểm tra.
Heartbeat (v0.2) chỉ được kiểm schema rồi đếm riêng, không vào thống kê người học.

Chạy: python -m ai_math_tutor.event_validation EVENTS.jsonl [--problems PROBLEMS.jsonl]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import defaultdict
from collections.abc import Iterable, Iterator
from dataclasses import asdict, dataclass, field
from datetime import datetime
from importlib import resources
from pathlib import Path
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from ai_math_tutor import math_check
from ai_math_tutor.safe_math import SafeMath, SafeMathError

# v0.2 là bản phụ tương thích ngược: mọi sự kiện học tập v0.1 vẫn hợp lệ, thêm loại heartbeat.
SCHEMA_RESOURCE = "learner_event_v0_2.schema.json"
SCHEMA_RESOURCE_V0_1 = "learner_event_v0_1.schema.json"
HEARTBEAT_EVENT_TYPE = "heartbeat"
HEARTBEAT_LEARNER_ID = "L-0000000000000000"
DEFAULT_LATE_THRESHOLD_S = 30.0
DEFAULT_MATH_TIMEOUT_S = 5.0


def load_schema(resource: str = SCHEMA_RESOURCE) -> dict:
    text = resources.files("ai_math_tutor.contracts").joinpath(resource).read_text(encoding="utf-8")
    return json.loads(text)


def is_heartbeat(event: Any) -> bool:
    """Bản ghi kỹ thuật đẩy watermark; pipeline lọc nó trước khi cập nhật trạng thái người học."""
    return isinstance(event, dict) and event.get("event_type") == HEARTBEAT_EVENT_TYPE


def _parse_time(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


@dataclass
class ValidationReport:
    lines: int = 0
    unique_events: int = 0
    duplicate_deliveries: int = 0
    late_events: int = 0
    out_of_order_events: int = 0
    sessions: int = 0
    seq_gaps: dict[str, list[int]] = field(default_factory=dict)
    math_labels_checked: int = 0
    heartbeats: int = 0
    errors: list[str] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return not self.errors


def _read_jsonl(path: Path) -> Iterator[tuple[int, Any, str | None]]:
    """(số dòng, bản ghi, lỗi giải mã); đọc từng dòng nên không nạp cả tệp vào bộ nhớ."""
    with path.open(encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                yield number, json.loads(raw), None
            except json.JSONDecodeError as exc:
                yield number, None, f"JSON lỗi: {exc}"


def load_problems(path: Path) -> dict[str, dict]:
    return {row["problem_id"]: row for _, row, error in _read_jsonl(path) if error is None}


def _math_label(checker: SafeMath, problem: dict, text: str, *, is_step: bool) -> bool:
    """Kết quả kiểm chứng của một bước (phải có '=') hoặc một đáp án (không có '='), chạy trong worker."""
    if is_step and "=" not in text:
        raise math_check.MathInputError(f"bước phải là phương trình có dấu '=': {text!r}")
    if not is_step and "=" in text:
        raise math_check.MathInputError(f"đáp án không được chứa '=': {text!r}")
    return bool(checker.call("diagnose", problem, text)["equivalent"])


def _check_math_label(
    event: dict, problems: dict[str, dict], line: int, report: ValidationReport, checker: SafeMath
) -> None:
    """So nhãn is_correct / loại sự kiện với kết quả kiểm chứng SymPy."""
    problem = problems.get(event["problem_id"])
    if problem is None:
        report.errors.append(f"dòng {line}: problem_id {event['problem_id']} không có trong kho bài")
        return
    payload, kind = event["payload"], event["event_type"]
    try:
        if kind == "submit_step":
            expected = _math_label(checker, problem, payload["expression"], is_step=True)
            if expected != payload["is_correct"]:
                report.errors.append(f"dòng {line}: is_correct={payload['is_correct']} nhưng SymPy cho {expected}")
        elif kind in ("wrong_answer", "complete"):
            correct = _math_label(checker, problem, payload["answer"], is_step=False)
            if correct != (kind == "complete"):
                report.errors.append(f"dòng {line}: {kind} với đáp án {payload['answer']!r} mâu thuẫn kiểm chứng SymPy")
        else:
            return
    except math_check.MathInputError as exc:
        report.errors.append(f"dòng {line}: {exc}")
        return
    except SafeMathError as exc:
        # Gồm cả quá hạn: worker đã bị giết, validator đi tiếp sang dòng sau.
        report.errors.append(f"dòng {line}: không kiểm chứng được nhãn toán: {exc}")
        return
    report.math_labels_checked += 1


def validate_iter(
    rows: Iterable[Any],
    problems: dict[str, dict] | None = None,
    late_threshold_s: float = DEFAULT_LATE_THRESHOLD_S,
    *,
    checker: SafeMath | None = None,
) -> ValidationReport:
    """Kiểm tra các bản ghi đã giải mã (dict) theo thứ tự đến — dùng cho Parquet, bản ghi Kafka.

    `problems`: problem_id → bài (như `load_problems`). `checker`: dùng lại một SafeMath
    sẵn có; không truyền thì tạo riêng cho lần kiểm tra này và đóng khi xong.
    Thông báo lỗi đánh số "dòng" theo thứ tự bản ghi, từ 1.
    """
    return _validate(((number, row, None) for number, row in enumerate(rows, start=1)), problems, late_threshold_s, checker)


def validate_events(
    events_path: Path,
    problems_path: Path | None = None,
    late_threshold_s: float = DEFAULT_LATE_THRESHOLD_S,
) -> ValidationReport:
    """Kiểm tra tệp sự kiện theo thứ tự đến (thứ tự dòng = thứ tự broker nhận)."""
    problems = load_problems(problems_path) if problems_path else None
    return _validate(_read_jsonl(events_path), problems, late_threshold_s, None)


def _validate(
    items: Iterable[tuple[int, Any, str | None]],
    problems: dict[str, dict] | None,
    late_threshold_s: float,
    checker: SafeMath | None,
) -> ValidationReport:
    """Lõi chung của validate_events và validate_iter: (số dòng, bản ghi, lỗi giải mã) theo thứ tự đến."""
    owned_checker = SafeMath(timeout_s=DEFAULT_MATH_TIMEOUT_S) if checker is None and problems is not None else None
    try:
        return _run_checks(items, problems, late_threshold_s, checker or owned_checker)
    finally:
        if owned_checker is not None:
            owned_checker.close()


def _run_checks(
    items: Iterable[tuple[int, Any, str | None]],
    problems: dict[str, dict] | None,
    late_threshold_s: float,
    checker: SafeMath | None,
) -> ValidationReport:
    validator = Draft202012Validator(load_schema(), format_checker=FormatChecker())
    report = ValidationReport()
    seen: dict[str, str] = {}
    last_seq: dict[str, int] = {}
    seqs: dict[str, set[int]] = defaultdict(set)

    for line, event, decode_error in items:
        report.lines += 1
        if decode_error is not None:
            report.errors.append(f"dòng {line}: {decode_error}")
            continue
        schema_errors = sorted(validator.iter_errors(event), key=lambda e: list(e.path))
        if schema_errors:
            report.errors.extend(f"dòng {line}: {'/'.join(map(str, e.path)) or '<gốc>'}: {e.message}" for e in schema_errors)
            continue
        if is_heartbeat(event):
            report.heartbeats += 1
            continue

        canonical = json.dumps(event, sort_keys=True, ensure_ascii=False)
        if event["event_id"] in seen:
            # Gửi lại hợp lệ phải giống hệt bản gốc; khác nội dung là xung đột khóa.
            if seen[event["event_id"]] != canonical:
                report.errors.append(f"dòng {line}: event_id {event['event_id']} trùng nhưng nội dung khác")
            report.duplicate_deliveries += 1
            continue
        seen[event["event_id"]] = canonical
        report.unique_events += 1

        event_time, sent_time = _parse_time(event["event_time"]), _parse_time(event["sent_time"])
        if sent_time < event_time:
            report.errors.append(f"dòng {line}: sent_time sớm hơn event_time")
        if (sent_time - event_time).total_seconds() > late_threshold_s:
            report.late_events += 1

        session, seq = event["session_id"], event["session_seq"]
        if seq in seqs[session]:
            report.errors.append(f"dòng {line}: session_seq {seq} dùng lại trong {session} với event_id khác")
        seqs[session].add(seq)
        if seq < last_seq.get(session, 0):
            report.out_of_order_events += 1
        last_seq[session] = max(seq, last_seq.get(session, 0))

        if problems is not None and checker is not None:
            _check_math_label(event, problems, line, report, checker)

    report.sessions = len(seqs)
    for session, values in sorted(seqs.items()):
        missing = sorted(set(range(1, max(values) + 1)) - values)
        if missing:
            report.seq_gaps[session] = missing
    return report


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("events", type=Path)
    parser.add_argument("--problems", type=Path, help="kho bài JSONL để kiểm chứng nhãn toán học")
    parser.add_argument("--late-threshold-s", type=float, default=DEFAULT_LATE_THRESHOLD_S)
    args = parser.parse_args(argv)
    report = validate_events(args.events, args.problems, args.late_threshold_s)
    print(json.dumps({**asdict(report), "ok": report.ok}, ensure_ascii=False, indent=2))
    return 0 if report.ok else 1


if __name__ == "__main__":
    sys.exit(main())
