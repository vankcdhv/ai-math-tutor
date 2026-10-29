"""Trang Học sinh, phần logic thuần: kiểm bước bằng bộ kiểm của dịch vụ gợi ý, sự kiện LearnerEvent hợp lệ
theo hợp đồng (kể cả nhãn toán khi validator kiểm lại bằng SymPy), phong bì gợi ý được dịch vụ thật chấp nhận.

Không có mạng: bộ kiểm là `math_check` thật (gọi thẳng, hoặc qua `SafeMath`), dịch vụ gợi ý là `HintService`
thật trên kho bài tổng hợp, không Redis.
"""

from __future__ import annotations

import json

import pytest

from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, validate_iter
from ai_math_tutor.hints.envelope import envelope_errors
from ai_math_tutor.hints.generator import HintEngine
from ai_math_tutor.problem_bank import ProblemBank
from ai_math_tutor.safe_math import ALLOWED_FUNCTIONS, SafeMath, SafeMathOverloaded, SafeMathTimeout
from ai_math_tutor.student import practice
from ai_math_tutor.student.practice import CORRECT, INVALID, UNCHECKED, WRONG
from test_hint_service import make_service
from test_hint_verify import synthetic_bank, synthetic_rows

T0 = 1_793_000_000_000  # epoch ms bất kỳ (2026-10-26)


class DirectChecker:
    """Cùng giao diện `SafeMath.call` nhưng gọi thẳng `math_check` trong tiến trình (test nhanh)."""

    def call(self, func_name, *args):
        return ALLOWED_FUNCTIONS[func_name](*args)


class FailingChecker:
    def __init__(self, exc: Exception) -> None:
        self.exc = exc

    def call(self, func_name, *args):
        raise self.exc


@pytest.fixture(scope="module")
def bank() -> ProblemBank:
    return synthetic_bank()


@pytest.fixture(scope="module")
def safe_checker():
    with SafeMath(timeout_s=5.0, max_pending=8, workers=1) as instance:
        yield instance


def session() -> practice.LearnerSession:
    return practice.LearnerSession.start("C-SIM-0001", "L-00000000000000b2")


# ---------------------------------------------------------------- bút danh, phiên


def test_new_ids_follow_contract_and_skip_reserved_heartbeat_id():
    tokens = iter(["0" * 16, "abcdef0123456789"])
    assert practice.new_learner_id(lambda n: next(tokens)) == "L-abcdef0123456789"
    assert practice.new_learner_id() != HEARTBEAT_LEARNER_ID
    started = practice.LearnerSession.start("C-SIM-0003")
    assert started.learner_id.startswith("L-") and len(started.learner_id) == 18
    assert started.session_id.startswith("S-") and len(started.session_id) == 10 and started.next_seq == 1
    for bad_class in ("lop-1", "C-", "", "C-sim-0001"):
        with pytest.raises(ValueError):
            practice.LearnerSession.start(bad_class)
    for bad_learner in ("L-XYZ", HEARTBEAT_LEARNER_ID, "L-0123"):
        with pytest.raises(ValueError):
            practice.LearnerSession.start("C-SIM-0001", bad_learner)


def test_verified_problems_keeps_only_the_practice_split(bank):
    problems = practice.verified_problems(bank)
    assert {p.problem_id for p in problems} == {"P-TLIN-001", "P-TLIN-002", "P-TQUAD-001", "P-TARI-001", "P-TLIN-003"}
    assert {p.problem_id for p in practice.problems_matching(problems, kind="arithmetic")} == {"P-TARI-001"}
    assert {p.problem_id for p in practice.problems_matching(problems, skill="linear_equation.distribute")} == {
        "P-TLIN-002"}


# ---------------------------------------------------------------- kiểm bước


def test_equation_step_written_in_the_original_variable(bank):
    problem = bank.get("P-TLIN-003")  # đề dùng ẩn y, kho lưu theo x
    checker = DirectChecker()
    good = practice.check_input(checker, problem, "  3*y = 9 ", 1)
    assert good.status == CORRECT and good.expression == "3*y = 9" and good.machine == "3*x = 9"
    assert good.contract_correct
    wrong = practice.check_input(checker, problem, "3*y = 5", 1)
    assert wrong.status == WRONG and not wrong.contract_correct and wrong.machine == "3*x = 5"
    # Phương trình: mọi bước tương đương đều đúng, kể cả viết thẳng nghiệm.
    assert practice.check_input(checker, problem, "y = 3", 1).status == CORRECT
    assert practice.check_input(checker, problem, "3", 3).status == CORRECT  # đáp án cuối (bước n + 1)
    assert practice.check_input(checker, problem, "4", 3).status == WRONG


@pytest.mark.parametrize("text, step", [
    ("", 1), ("   ", 1),
    ("3*y", 1),            # bước phải có dấu =
    ("y = 3 = 3", 1),      # nhiều dấu =
    ("y = 3", 3),          # đáp án không có dấu =
    ("3*Y = 9", 1),        # chữ hoa ngoài bảng ký tự
    ("3*y = 9; import os", 1),
    ("1" * 201 + " = 1", 1),
    ("3*z = 9", 1),        # ẩn lạ: math_check từ chối
])
def test_invalid_input_is_rejected_with_a_message_and_no_label(bank, text, step):
    result = practice.check_input(DirectChecker(), bank.get("P-TLIN-003"), text, step)
    assert result.status == INVALID and result.message and not result.contract_correct


def test_arithmetic_step_must_be_the_current_step_but_label_follows_contract(bank):
    problem = bank.get("P-TARI-001")  # 3 * 12 = 36, rồi 36 - 8 = 28
    checker = DirectChecker()
    assert practice.check_input(checker, problem, "3 * 12 = 36", 1).status == CORRECT
    ahead = practice.check_input(checker, problem, "36 - 8 = 28", 1)
    assert ahead.status == WRONG and ahead.contract_correct  # đúng trên đường giải mẫu, chưa phải bước 1
    slip = practice.check_input(checker, problem, "3 + 12 = 15", 1)
    assert slip.status == WRONG and not slip.contract_correct
    assert practice.check_input(checker, problem, "28", 3).status == CORRECT


def test_checker_failures_are_unchecked_not_wrong(bank):
    problem = bank.get("P-TLIN-001")
    for exc in (SafeMathTimeout("quá hạn"), SafeMathOverloaded("bận")):
        result = practice.check_input(FailingChecker(exc), problem, "2*x = 8", 1)
        assert result.status == UNCHECKED and result.message


# ---------------------------------------------------------------- luồng làm bài → sự kiện


def test_attempt_flow_counts_and_events(bank):
    learner, checker = session(), DirectChecker()
    attempt, opened = practice.open_problem(learner, bank.get("P-TLIN-001"), T0)
    assert opened["event_type"] == "open_problem" and opened["payload"] == {} and opened["session_seq"] == 1

    invalid = practice.check_input(checker, attempt.problem, "2*x", 1)
    assert practice.record_check(learner, attempt, invalid, T0 + 1) is None
    assert learner.next_seq == 2 and attempt.wrong_total == 0  # đầu vào hỏng không phải một lần thử

    wrong = practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "2*x = 14", 1), T0 + 2)
    assert wrong["payload"] == {"step_index": 1, "expression": "2*x = 14", "is_correct": False}
    assert (attempt.step, attempt.wrong_at_step, attempt.wrong_total, attempt.last_expression) == (1, 1, 1, "2*x = 14")

    ok = practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "2*x = 8", 1), T0 + 3)
    assert ok["payload"]["is_correct"] is True and ok["session_seq"] == 3
    assert (attempt.step, attempt.wrong_at_step, attempt.last_expression) == (2, 0, None)
    assert attempt.solved_steps == ("2*x = 8",)

    practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "x = 4", 2), T0 + 4)
    assert attempt.answering
    miss = practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "5", 3), T0 + 5)
    assert miss["event_type"] == "wrong_answer" and miss["payload"] == {"answer": "5", "attempt": 1}
    done = practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "4", 3), T0 + 6)
    assert done["event_type"] == "complete" and done["payload"] == {"answer": "4", "attempts": 2, "hints_used": 0}
    assert attempt.completed and attempt.final_answer == "4" and attempt.wrong_total == 2
    with pytest.raises(ValueError):
        practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "4", 3), T0 + 7)


def test_hint_envelope_is_contract_valid_and_only_a_served_hint_consumes_the_sequence(bank):
    learner, checker = session(), DirectChecker()
    attempt, _ = practice.open_problem(learner, bank.get("P-TLIN-003"), T0)
    envelope = practice.hint_envelope(learner, attempt, T0 + 10)
    assert envelope_errors(envelope) == [] and "expression" not in envelope
    assert envelope["step_index"] == 1 and envelope["session_seq"] == 2 and learner.next_seq == 2

    practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "3*y = 5", 1), T0 + 20)
    failed = practice.hint_envelope(learner, attempt, T0 + 30)  # dịch vụ không trả lời: không tiêu số thứ tự
    served = practice.hint_envelope(learner, attempt, T0 + 31)
    assert failed["session_seq"] == served["session_seq"] == 3 and failed["event_id"] != served["event_id"]
    assert served["expression"] == "3*y = 5" and envelope_errors(served) == []

    event = practice.record_hint(learner, attempt, served, 2)
    assert (event["event_id"], event["session_seq"], event["event_time"]) == (
        served["event_id"], 3, served["event_time"])
    assert event["payload"] == {"hint_level": 2, "step_index": 1} and attempt.hints_used == 1
    with pytest.raises(ValueError):
        practice.record_hint(learner, attempt, failed, 1)  # phong bì cũ: số thứ tự đã được tiêu

    attempt.step = attempt.n_steps + 1  # đang nộp đáp án cuối
    final = practice.hint_envelope(learner, attempt, T0 + 40)
    assert final["step_index"] == attempt.n_steps + 1 and envelope_errors(final) == []
    assert practice.record_hint(learner, attempt, final, 1)["payload"]["step_index"] == attempt.n_steps


def test_encode_event_stamps_sent_time_and_refuses_contract_violations(bank):
    learner = session()
    _, event = practice.open_problem(learner, bank.get("P-TLIN-001"), T0)
    key, value = practice.encode_event(event, T0 + 250)
    decoded = json.loads(value)
    assert key == b"L-00000000000000b2"
    assert decoded["event_time"] == "2026-10-26T07:33:20.000Z" and decoded["sent_time"] == "2026-10-26T07:33:20.250Z"
    assert practice.contract_errors(decoded) == []
    assert json.loads(practice.encode_event(event, T0 - 5)[1])["sent_time"] == decoded["event_time"]  # không sớm hơn
    bad = practice.learner_event(learner, bank.get("P-TLIN-001"), "request_hint", {"hint_level": 4, "step_index": 1},
                                 decoded["event_time"], seq=9)
    with pytest.raises(ValueError):
        practice.encode_event(bad, T0)


def test_role_play_events_pass_the_contract_validator_with_sympy_labels(bank, safe_checker):
    """Mọi sự kiện một lượt chơi sinh ra qua được `event_validation` (schema + nhãn toán kiểm lại bằng SymPy)."""
    learner = session()
    events, sent = [], T0
    plays = {"P-TARI-001": ["3 + 12 = 15", "36 - 8 = 28", "3 * 12 = 36", "36 - 8 = 28", "27", "28"],
             "P-TLIN-003": ["3*y = 5", "3*y = 9", "y = 3", "{3}"],
             "P-TQUAD-001": ["x^2 - 3*x = 0", "x*(x - 3) = 0", "3", "{0, 3}"]}
    for problem_id, inputs in plays.items():
        attempt, opened = practice.open_problem(learner, bank.get(problem_id), sent)
        events.append(opened)
        for text in inputs:
            sent += 1000
            result = practice.check_input(safe_checker, attempt.problem, text, attempt.step)
            event = practice.record_check(learner, attempt, result, sent)
            events.append(event)
            if result.status == WRONG:
                envelope = practice.hint_envelope(learner, attempt, sent + 10)
                events.append(practice.record_hint(learner, attempt, envelope, 1))
        assert attempt.completed, problem_id
    rows = [json.loads(practice.encode_event(e, T0 + 60_000)[1]) for e in events]
    problems = {row["problem_id"]: row for row in synthetic_rows()}
    report = validate_iter(rows, problems, checker=safe_checker)
    assert report.ok, report.errors
    labelled = sum(r["event_type"] in ("submit_step", "wrong_answer", "complete") for r in rows)
    assert report.math_labels_checked == labelled and report.sessions == 1 and report.seq_gaps == {}
    assert [r["session_seq"] for r in rows] == list(range(1, len(rows) + 1))


# ---------------------------------------------------------------- dịch vụ gợi ý thật


@pytest.fixture(scope="module")
def engine(bank):
    return HintEngine.from_bank(bank)


def test_envelope_is_served_by_the_real_hint_service(engine, safe_checker, bank):
    service = make_service(engine, safe_checker)
    learner, checker = session(), DirectChecker()
    attempt, _ = practice.open_problem(learner, bank.get("P-TLIN-002"), T0)
    practice.record_check(learner, attempt, practice.check_input(checker, attempt.problem, "5*x - 225 = 6*x - 9", 1),
                          T0 + 1)
    outcome = service.handle(practice.hint_envelope(learner, attempt, T0 + 2))
    assert outcome.status == 200, outcome.body
    hint = practice.Hint.from_body(outcome.body)
    assert hint.verified and 1 <= hint.hint_level <= 3 and hint.text_vi
    assert hint.state_source == "default"  # không Redis: fail-closed
    assert outcome.after["audit"]["expression_status"] == "wrong"


@pytest.mark.parametrize("body", [
    None, [], {"hint_level": 0, "text_vi": "a", "math": [], "verified": True, "state_source": "redis", "source": "x"},
    {"hint_level": True, "text_vi": "a", "math": [], "verified": True, "state_source": "redis", "source": "x"},
    {"hint_level": 1, "text_vi": "", "math": [], "verified": True, "state_source": "redis", "source": "x"},
    {"hint_level": 1, "text_vi": "a", "math": [1], "verified": True, "state_source": "redis", "source": "x"},
    {"hint_level": 1, "text_vi": "a", "math": [], "verified": "yes", "state_source": "redis", "source": "x"},
])
def test_hint_response_is_validated_at_the_boundary(body):
    with pytest.raises(ValueError):
        practice.Hint.from_body(body)

