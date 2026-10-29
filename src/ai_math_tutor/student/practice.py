"""Logic thuần của trang Học sinh: phiên người học, kiểm từng bước, sự kiện LearnerEvent, phong bì gợi ý.

- **Kiểm bước** dùng đúng bộ kiểm của dịch vụ gợi ý: `math_check.diagnose` chạy qua `SafeMath` (worker
  có thời hạn), biểu thức viết theo ẩn gốc của đề được đổi về ẩn `x` bằng `to_machine_expression`.
  Trang chỉ cho qua bước khi biểu thức đúng **ở bước đang làm** (`diagnose(..., step_index)`, như dịch vụ
  gợi ý chẩn đoán). Nhãn `is_correct` của `submit_step` là nhãn của hợp đồng (`diagnose` không kèm bước,
  như `event_validation`). Hai nhãn chỉ khác nhau ở bài số học, khi người học viết đúng phép tính của một
  bước khác trên đường giải mẫu: sự kiện ghi đúng, trang vẫn chờ bước đang làm.
- **Sự kiện** theo hợp đồng LearnerEvent v0.2: mỗi thao tác một `event_id` mới, `session_seq` tăng dần
  trong phiên, `event_time` = lúc thao tác, `sent_time` = lúc gửi lần đầu (`encode_event`). Mọi sự kiện
  được kiểm schema trước khi rời trang. Đầu vào hỏng hoặc chưa kiểm được không sinh sự kiện: đó không phải
  một lần thử (validator sẽ coi nhãn toán của nó là lỗi hợp đồng).
- **Gợi ý**: phong bì `POST /hints` dùng `session_seq` kế tiếp nhưng chưa tiêu; chỉ khi dịch vụ trả gợi ý
  thì `record_hint` tiêu số đó cho sự kiện `request_hint` cùng `event_id`, nên phiên không có khoảng trống.

Không có I/O ở đây (Kafka, Redis, HTTP nằm ở `backends`).
"""

from __future__ import annotations

import json
import re
import secrets
import uuid
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from jsonschema import Draft202012Validator, FormatChecker

from ai_math_tutor import math_check
from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, load_schema
from ai_math_tutor.hints.envelope import EXPRESSION_PATTERN, MAX_EXPRESSION_LENGTH
from ai_math_tutor.hints.generator import to_machine_expression
from ai_math_tutor.hints.verify import build_all_facts
from ai_math_tutor.problem_bank import Problem, ProblemBank
from ai_math_tutor.safe_math import SafeMathError, SafeMathOverloaded
from ai_math_tutor.simulator.output import iso_from_epoch_ms

SCHEMA_VERSION = "0.2"
CLIENT = {"platform": "web", "app_version": "0.1.0", "connection": "wifi"}
PRACTICE_SPLIT = "test"  # cùng tập bài mà bộ mô phỏng của demo dùng
CORRECT, WRONG, INVALID, UNCHECKED = "correct", "wrong", "invalid", "unchecked"

_CONTRACT = load_schema()
_VALIDATOR = Draft202012Validator(_CONTRACT, format_checker=FormatChecker())
_LEARNER_ID_RE = re.compile(_CONTRACT["properties"]["learner_id"]["pattern"])
_CLASS_ID_RE = re.compile(_CONTRACT["properties"]["class_id"]["pattern"])
_INPUT_RE = re.compile(EXPRESSION_PATTERN)


class Checker(Protocol):
    """Giao diện của `SafeMath`: gọi một hàm của `math_check` theo tên."""

    def call(self, func_name: str, *args: Any) -> Any: ...


def new_learner_id(token_hex: Callable[[int], str] = secrets.token_hex) -> str:
    """Bút danh ngẫu nhiên `L-` + 16 hex: không suy từ danh tính nào nên không chứa PII; không trùng mã heartbeat."""
    while True:
        learner = f"L-{token_hex(8)}"
        if learner != HEARTBEAT_LEARNER_ID:
            return learner


def new_session_id(token_hex: Callable[[int], str] = secrets.token_hex) -> str:
    return f"S-{token_hex(4)}"


@dataclass
class LearnerSession:
    """Một người học trên trang: bút danh, lớp, phiên và số thứ tự sự kiện kế tiếp trong phiên."""

    learner_id: str
    class_id: str
    session_id: str
    next_seq: int = 1

    @classmethod
    def start(cls, class_id: str, learner_id: str | None = None) -> LearnerSession:
        learner = learner_id or new_learner_id()
        if not _LEARNER_ID_RE.match(learner) or learner == HEARTBEAT_LEARNER_ID:
            raise ValueError(f"bút danh không hợp lệ theo hợp đồng: {learner!r}")
        if not isinstance(class_id, str) or not _CLASS_ID_RE.match(class_id):
            raise ValueError(f"mã lớp không hợp lệ theo hợp đồng: {class_id!r}")
        return cls(learner_id=learner, class_id=class_id, session_id=new_session_id())

    def take_seq(self) -> int:
        seq = self.next_seq
        self.next_seq += 1
        return seq


@dataclass
class ProblemAttempt:
    """Một lượt làm bài: bước đang làm (`n_steps + 1` là lúc nộp đáp án cuối) và các bộ đếm."""

    problem: Problem
    step: int = 1
    wrong_total: int = 0
    wrong_at_step: int = 0
    answer_attempts: int = 0
    hints_used: int = 0
    last_expression: str | None = None  # biểu thức sai gần nhất ở bước đang làm (gửi kèm yêu cầu gợi ý)
    completed: bool = False
    final_answer: str | None = None

    @property
    def n_steps(self) -> int:
        return len(self.problem.steps)

    @property
    def answering(self) -> bool:
        return self.step > self.n_steps

    @property
    def solved_steps(self) -> tuple[str, ...]:
        """Bước mẫu đã kiểm của các bước người học đã qua, theo ẩn gốc của đề."""
        return self.problem.display_steps[: self.step - 1]


@dataclass(frozen=True)
class CheckResult:
    status: str  # CORRECT | WRONG | INVALID | UNCHECKED
    expression: str = ""  # chữ người học gõ (đã bỏ khoảng trắng đầu/cuối)
    machine: str = ""  # cùng biểu thức theo ẩn x — dạng ghi vào sự kiện (validator chỉ kiểm ẩn x)
    contract_correct: bool = False  # nhãn is_correct theo hợp đồng
    message: str = ""  # lý do khi INVALID/UNCHECKED


def verified_problems(bank: ProblemBank, split: str = PRACTICE_SPLIT) -> tuple[Problem, ...]:
    """Bài của `split` đã qua kiểm SymPy (cùng `build_all_facts` mà dịch vụ gợi ý dựng lúc khởi động)."""
    selected = bank.select(split)
    facts = build_all_facts(selected.problems)
    return tuple(p for p in selected.problems if facts[p.problem_id].verified)


def _invalid(expression: str, message: str) -> CheckResult:
    return CheckResult(INVALID, expression=expression, message=message)


def check_input(checker: Checker, problem: Problem, text: str, step_index: int) -> CheckResult:
    """Kiểm một bước (1..n) hoặc đáp án cuối (`step_index` = n + 1) của người học."""
    expression = (text or "").strip()
    answering = step_index > len(problem.steps)
    variable = problem.display_variable
    if not expression:
        return _invalid(expression, "Hãy nhập đáp án." if answering else "Hãy nhập biểu thức của bước này.")
    if len(expression) > MAX_EXPRESSION_LENGTH or not _INPUT_RE.match(expression):
        return _invalid(expression, f"Chỉ dùng chữ số, ẩn {variable}, các phép + - * / ^, ngoặc, dấu = và dấu phẩy "
                                    f"(tối đa {MAX_EXPRESSION_LENGTH} ký tự).")
    if answering and "=" in expression:
        return _invalid(expression, "Đáp án chỉ ghi giá trị, không có dấu =, ví dụ 4 hoặc {2, 3}.")
    if not answering and expression.count("=") != 1:
        return _invalid(expression, "Mỗi bước là một đẳng thức có đúng một dấu =, ví dụ 2*x = 8 hoặc 3 * 12 = 36.")
    machine = to_machine_expression(expression, problem.variable)
    row = problem.to_dict()
    step_arg = None if answering else step_index
    try:
        if checker.call("diagnose", row, machine, step_arg)["equivalent"]:
            return CheckResult(CORRECT, expression=expression, machine=machine, contract_correct=True)
        contract = False
        if step_arg is not None and problem.kind == "arithmetic":
            contract = bool(checker.call("diagnose", row, machine, None)["equivalent"])
    except math_check.MathInputError:
        return _invalid(expression, f"Không đọc được biểu thức. Viết phép nhân bằng * (2*{variable}), "
                                    f"lũy thừa bằng ^ ({variable}^2), chỉ dùng ẩn {variable}.")
    except SafeMathOverloaded:
        return CheckResult(UNCHECKED, expression=expression, message="Bộ kiểm đang bận, thử lại sau giây lát.")
    except SafeMathError:
        return CheckResult(UNCHECKED, expression=expression,
                           message="Không kiểm kịp biểu thức này (quá thời hạn hoặc lỗi bộ kiểm). Hãy viết gọn hơn.")
    return CheckResult(WRONG, expression=expression, machine=machine, contract_correct=contract)


# --- sự kiện LearnerEvent -----------------------------------------------------------------------


def learner_event(session: LearnerSession, problem: Problem, event_type: str, payload: Mapping[str, Any],
                  event_time: str, *, seq: int, event_id: str | None = None) -> dict[str, Any]:
    """Một sự kiện học tập theo thứ tự trường của hợp đồng; `sent_time` tạm bằng `event_time` tới khi gửi."""
    return {
        "schema_version": SCHEMA_VERSION,
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": event_type,
        "learner_id": session.learner_id,
        "class_id": session.class_id,
        "session_id": session.session_id,
        "session_seq": seq,
        "problem_id": problem.problem_id,
        "skill_ids": list(problem.skill_ids),
        "event_time": event_time,
        "sent_time": event_time,
        "client": dict(CLIENT),
        "payload": dict(payload),
    }


def contract_errors(event: Any) -> list[str]:
    """Lỗi theo schema LearnerEvent v0.2, dạng `đường/dẫn: thông báo` (rỗng = hợp lệ)."""
    errors = sorted(_VALIDATOR.iter_errors(event), key=lambda e: list(e.path))
    return [f"{'/'.join(map(str, e.path)) or '<gốc>'}: {e.message}" for e in errors]


def encode_event(event: Mapping[str, Any], sent_ms: int) -> tuple[bytes, bytes]:
    """(key, value) để gửi Kafka: `sent_time` = lúc gửi (không sớm hơn `event_time`), key = `learner_id`.

    Gửi lại (retry) phải dùng đúng các byte này, kể cả `sent_time` (quy tắc khử trùng của hợp đồng).
    Sự kiện sai hợp đồng ném ValueError: đó là lỗi của trang, không bao giờ được gửi đi.
    """
    stamped = dict(event)
    stamped["sent_time"] = max(iso_from_epoch_ms(sent_ms), str(event["event_time"]))  # cùng định dạng độ dài cố định
    errors = contract_errors(stamped)
    if errors:
        raise ValueError(f"sự kiện sai hợp đồng LearnerEvent: {errors[:3]}")
    return str(stamped["learner_id"]).encode("utf-8"), json.dumps(stamped).encode("utf-8")


def open_problem(session: LearnerSession, problem: Problem, event_ms: int) -> tuple[ProblemAttempt, dict[str, Any]]:
    attempt = ProblemAttempt(problem)
    event = learner_event(session, problem, "open_problem", {}, iso_from_epoch_ms(event_ms), seq=session.take_seq())
    return attempt, event


def record_check(session: LearnerSession, attempt: ProblemAttempt, result: CheckResult,
                 event_ms: int) -> dict[str, Any] | None:
    """Áp kết quả kiểm vào lượt làm bài; trả sự kiện cần gửi, hoặc None khi đầu vào hỏng/chưa kiểm được."""
    if attempt.completed:
        raise ValueError("bài đã hoàn thành")
    if result.status not in (CORRECT, WRONG):
        return None
    correct = result.status == CORRECT
    if attempt.answering:
        attempt.answer_attempts += 1
        if correct:
            event_type = "complete"
            payload: dict[str, Any] = {"answer": result.machine, "attempts": attempt.answer_attempts,
                                       "hints_used": attempt.hints_used}
            attempt.completed, attempt.final_answer = True, result.expression
        else:
            event_type = "wrong_answer"
            payload = {"answer": result.machine, "attempt": attempt.answer_attempts}
    else:
        event_type = "submit_step"
        payload = {"step_index": attempt.step, "expression": result.machine, "is_correct": result.contract_correct}
        if correct:
            attempt.step += 1
    if correct:
        attempt.wrong_at_step, attempt.last_expression = 0, None
    else:
        attempt.wrong_total += 1
        attempt.wrong_at_step += 1
        attempt.last_expression = result.expression
    return learner_event(session, attempt.problem, event_type, payload, iso_from_epoch_ms(event_ms),
                         seq=session.take_seq())


# --- gợi ý --------------------------------------------------------------------------------------


def hint_envelope(session: LearnerSession, attempt: ProblemAttempt, event_ms: int,
                  event_id: str | None = None) -> dict[str, Any]:
    """Thân `POST /hints`: bước đang làm (n + 1 khi nộp đáp án cuối), kèm biểu thức sai gần nhất ở bước đó.

    Dùng `session.next_seq` nhưng không tiêu nó: yêu cầu thất bại không để lại khoảng trống trong phiên.
    """
    stamp = iso_from_epoch_ms(event_ms)
    body: dict[str, Any] = {
        "event_id": event_id or str(uuid.uuid4()),
        "learner_id": session.learner_id,
        "class_id": session.class_id,
        "session_id": session.session_id,
        "session_seq": session.next_seq,
        "problem_id": attempt.problem.problem_id,
        "skill_ids": list(attempt.problem.skill_ids),
        "step_index": attempt.step,
        "event_time": stamp,
        "sent_time": stamp,
        "client": dict(CLIENT),
    }
    if attempt.last_expression:
        body["expression"] = attempt.last_expression
    return body


@dataclass(frozen=True)
class Hint:
    """Phản hồi 200 của `POST /hints`, đã kiểm kiểu ở ranh giới."""

    hint_level: int
    text_vi: str
    math: tuple[str, ...]
    verified: bool
    state_source: str
    source: str
    misconception: str | None = None

    @classmethod
    def from_body(cls, body: Any) -> Hint:
        if not isinstance(body, Mapping):
            raise ValueError("phản hồi gợi ý phải là object JSON")
        level, text, math = body.get("hint_level"), body.get("text_vi"), body.get("math")
        if isinstance(level, bool) or not isinstance(level, int) or not 1 <= level <= 3:
            raise ValueError(f"hint_level không hợp lệ: {level!r}")
        if not isinstance(text, str) or not text.strip():
            raise ValueError("text_vi phải là chuỗi khác rỗng")
        if not isinstance(math, list) or not all(isinstance(item, str) for item in math):
            raise ValueError("math phải là danh sách chuỗi")
        verified, state_source, source = body.get("verified"), body.get("state_source"), body.get("source")
        if not isinstance(verified, bool) or not isinstance(state_source, str) or not isinstance(source, str):
            raise ValueError("thiếu verified/state_source/source")
        misconception = body.get("misconception")
        if misconception is not None and not isinstance(misconception, str):
            raise ValueError("misconception phải là chuỗi hoặc null")
        return cls(level, text, tuple(math), verified, state_source, source, misconception)


def record_hint(session: LearnerSession, attempt: ProblemAttempt, envelope: Mapping[str, Any],
                hint_level: int) -> dict[str, Any]:
    """Sự kiện `request_hint` cho gợi ý vừa nhận: cùng `event_id`, `session_seq`, `event_time` với phong bì.

    Lúc nộp đáp án cuối, `step_index` của sự kiện là bước cuối (như bộ mô phỏng); phong bì gửi n + 1.
    """
    if envelope["session_seq"] != session.next_seq or envelope["learner_id"] != session.learner_id:
        raise ValueError("phong bì gợi ý không thuộc thao tác hiện tại của phiên")
    attempt.hints_used += 1
    payload = {"hint_level": hint_level, "step_index": min(attempt.step, attempt.n_steps)}
    return learner_event(session, attempt.problem, "request_hint", payload, envelope["event_time"],
                         seq=session.take_seq(), event_id=envelope["event_id"])


def problems_matching(problems: Iterable[Problem], kind: str | None = None,
                      skill: str | None = None) -> tuple[Problem, ...]:
    """Lọc theo loại bài và kỹ năng chính (None = mọi giá trị)."""
    return tuple(p for p in problems
                 if (kind is None or p.kind == kind) and (skill is None or p.primary_skill == skill))
