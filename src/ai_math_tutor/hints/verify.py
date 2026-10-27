"""Kiểm chứng gợi ý cá nhân hóa: sự thật đã kiểm của từng bài + kiểm từng bản nháp trước khi trả.

Hai giai đoạn, để đường nhanh không phải chạy SymPy trên dữ liệu của chính mình:

1. `build_facts(problem)` — chạy **lúc khởi động** (hoặc offline), SymPy trong tiến trình qua
   `problem_bank.verify_problem`: mọi bước giải tương đương đề (bài phương trình) hoặc tính đúng
   chính xác (bài số học), đáp án khớp, mọi lỗi đã khai bị chấm sai đúng loại. Chỉ bài qua hết
   mới có `verified=True`. Mỗi bước được đánh dấu `reveals_answer` nếu bước đó để lộ đáp án cuối.
2. `verify_draft(draft, facts, example_facts, allow_answer)` — chạy **mỗi yêu cầu**, thuần Python:
   - mỗi biểu thức trong `math` phải là một bước đã kiểm của bài (vai `step`) hoặc đúng phương trình
     đã kiểm của bài tương tự thuộc tập dev (vai `example`), và xuất hiện nguyên văn trong `text_vi`;
   - ngoài các biểu thức đó và các "dạng bước" (bước đã kiểm với số thay bằng □), câu chữ không có '=';
   - khi chưa được phép đưa đáp án (`allow_answer=False`): không bước nào `reveals_answer`, và phần
     câu chữ nói về bài của người học không chứa giá trị đáp án (trừ số vốn có trong đề).

"Lộ đáp án" được hiểu chặt (fail-closed): bước dạng `x = số`; bước số học có kết quả bằng đáp án;
hoặc trong câu có một số (nguyên, thập phân hay phân số a/b) bằng trị tuyệt đối một nghiệm mà đề
không có sẵn số đó. Hệ quả: một số bước kề cuối (vd. `-x = 66` khi đáp án −66) cũng bị khóa.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from fractions import Fraction

from ai_math_tutor import math_check
from ai_math_tutor.hints.phrases import FORM_SLOT
from ai_math_tutor.problem_bank import Problem, verify_problem

ROLE_OWN = "own"          # câu nói về bài của người học — bị kiểm lộ đáp án
ROLE_EXAMPLE = "example"  # câu về bài tương tự (bài khác, tập dev)
ROLE_GENERIC = "generic"  # câu khái niệm chung của kỹ năng
ROLE_FORM = "form"        # dạng của một bước đã kiểm, số đã thay bằng □
ROLES = (ROLE_OWN, ROLE_EXAMPLE, ROLE_GENERIC, ROLE_FORM)
CLAIM_STEP = "step"
CLAIM_EXAMPLE = "example"
CLAIM_KINDS = (CLAIM_STEP, CLAIM_EXAMPLE)

# Số trong câu: số nguyên/thập phân, không dính vào chữ số hay dấu chấm bên cạnh.
_NUMBER = re.compile(r"(?<![0-9.])[0-9]+(?:\.[0-9]+)?(?![0-9])")
_FRACTION = re.compile(r"(?<![0-9.])([0-9]+(?:\.[0-9]+)?)\s*/\s*([0-9]+(?:\.[0-9]+)?)(?![0-9.])")
# Số bị che trong dạng bước: mọi số trừ số mũ (giữ "x^2" để thấy bậc của bước).
_MASKABLE = re.compile(r"(?<![0-9.^])(?<!\*\*)[0-9]+(?:\.[0-9]+)?")


@dataclass(frozen=True, slots=True)
class StepFact:
    index: int  # đánh số từ 1, như `step_index` của hợp đồng sự kiện
    text: str
    form: str
    reveals_answer: bool


@dataclass(frozen=True, slots=True)
class ProblemFacts:
    """Những gì đã kiểm về một bài; dùng lại cho mọi yêu cầu gợi ý về bài đó."""

    problem_id: str
    kind: str
    split: str
    skill_ids: tuple[str, ...]
    equation: str | None
    steps: tuple[StepFact, ...]
    leak_values: frozenset[Fraction]
    verified: bool
    errors: tuple[str, ...]

    def step(self, index: int) -> StepFact | None:
        return self.steps[index - 1] if 1 <= index <= len(self.steps) else None


@dataclass(frozen=True, slots=True)
class Part:
    text: str
    role: str
    step_index: int | None = None  # chỉ với ROLE_FORM: bước được che số


@dataclass(frozen=True, slots=True)
class Claim:
    expression: str
    kind: str
    step_index: int | None = None  # chỉ với CLAIM_STEP


@dataclass(frozen=True, slots=True)
class HintDraft:
    """Bản nháp gợi ý: câu ghép từ các phần có vai trò + các biểu thức toán cần kiểm."""

    level: int
    parts: tuple[Part, ...]
    claims: tuple[Claim, ...]

    @property
    def text_vi(self) -> str:
        return "".join(part.text for part in self.parts).strip()

    @property
    def math(self) -> tuple[str, ...]:
        return tuple(claim.expression for claim in self.claims)


def number_values(text: str) -> set[Fraction]:
    """Trị tuyệt đối của mọi số trong câu: từng số nguyên/thập phân và từng phân số a/b."""
    values = {Fraction(token) for token in _NUMBER.findall(text)}
    for numerator, denominator in _FRACTION.findall(text):
        if Fraction(denominator) != 0:
            values.add(abs(Fraction(numerator) / Fraction(denominator)))
    return values


def mask_step(step: str) -> str:
    """Dạng của bước: mỗi số (trừ số mũ) thành □, `**` viết thành `^` cho dễ đọc."""
    return _MASKABLE.sub(FORM_SLOT, step).replace("**", "^")


def _is_solved_form(step: str) -> bool:
    """`x = <không chứa x>` hoặc ngược lại: chính là đáp án."""
    lhs, sep, rhs = step.partition("=")
    if not sep:
        return False
    lhs, rhs = lhs.strip(), rhs.strip()
    return (lhs == "x" and "x" not in rhs) or (rhs == "x" and "x" not in lhs)


def _answer_values(problem: Problem) -> frozenset[Fraction]:
    values = math_check.parse_answer(problem.answer)
    return frozenset(abs(Fraction(int(v.p), int(v.q))) for v in values.args)


def _arith_result(step: str) -> Fraction | None:
    _, sep, rhs = step.rpartition("=")
    if not sep:
        return None
    value = math_check.parse_answer(rhs)
    (only,) = value.args
    return abs(Fraction(int(only.p), int(only.q)))


def build_facts(problem: Problem) -> ProblemFacts:
    """Kiểm toán học một bài bằng SymPy (trong tiến trình) — chỉ gọi lúc khởi động hoặc offline."""
    errors = list(verify_problem(problem))
    steps: list[StepFact] = []
    leak: frozenset[Fraction] = frozenset()
    try:
        answers = _answer_values(problem)
        own_numbers = number_values(" ".join(filter(None, (problem.equation, problem.statement, problem.statement_vi))))
        leak = frozenset(answers - own_numbers)
        for index, text in enumerate(problem.steps, start=1):
            reveals = _is_solved_form(text) or bool(number_values(text) & leak)
            if problem.kind == "arithmetic":
                reveals = reveals or _arith_result(text) in answers
            steps.append(StepFact(index=index, text=text, form=mask_step(text), reveals_answer=reveals))
    except (math_check.MathInputError, ValueError) as exc:
        errors.append(f"{problem.problem_id}: không tính được sự thật về đáp án: {exc}")
    return ProblemFacts(
        problem_id=problem.problem_id,
        kind=problem.kind,
        split=problem.split,
        skill_ids=problem.skill_ids,
        equation=problem.equation,
        steps=tuple(steps),
        leak_values=leak,
        verified=not errors,
        errors=tuple(errors),
    )


def build_all_facts(problems: Iterable[Problem]) -> dict[str, ProblemFacts]:
    return {problem.problem_id: build_facts(problem) for problem in problems}


def verify_draft(
    draft: HintDraft,
    facts: ProblemFacts,
    example_facts: Mapping[str, ProblemFacts] | None = None,
    *,
    allow_answer: bool = False,
) -> list[str]:
    """Danh sách lỗi kiểm chứng của bản nháp (rỗng = đạt). Không gọi SymPy."""
    errors: list[str] = []
    if not facts.verified:
        return [f"bài {facts.problem_id} chưa qua kiểm chứng SymPy"]
    examples = example_facts or {}
    own_text = "".join(p.text for p in draft.parts if p.role == ROLE_OWN)
    example_text = "".join(p.text for p in draft.parts if p.role == ROLE_EXAMPLE)
    for claim in draft.claims:
        if claim.kind == CLAIM_STEP:
            step = facts.step(claim.step_index or 0)
            if step is None or step.text != claim.expression:
                errors.append(f"{claim.expression!r} không phải bước đã kiểm của {facts.problem_id}")
            elif step.reveals_answer and not allow_answer:
                errors.append(f"bước {claim.expression!r} lộ đáp án khi chưa được phép")
            if claim.expression not in own_text:
                errors.append(f"biểu thức {claim.expression!r} không có trong câu về bài này")
        elif claim.kind == CLAIM_EXAMPLE:
            match = [f for f in examples.values() if f.equation == claim.expression]
            if not any(f.verified and f.split == "dev" and f.problem_id != facts.problem_id for f in match):
                errors.append(f"{claim.expression!r} không phải đề đã kiểm của một bài tương tự (dev)")
            if claim.expression not in example_text:
                errors.append(f"biểu thức {claim.expression!r} không có trong câu về bài tương tự")
        else:
            errors.append(f"loại biểu thức lạ: {claim.kind!r}")
    for part in draft.parts:
        if part.role not in ROLES:
            errors.append(f"vai trò lạ: {part.role!r}")
            continue
        if part.role == ROLE_FORM:
            step = facts.step(part.step_index or 0)
            if step is None or part.text != step.form:
                errors.append(f"dạng {part.text!r} không khớp bước đã kiểm nào")
            continue
        remaining = part.text
        for claim in draft.claims:
            remaining = remaining.replace(claim.expression, "")
        if "=" in remaining:
            errors.append("câu chữ chứa dấu '=' ngoài các biểu thức đã kiểm")
        if part.role == ROLE_OWN and not allow_answer and number_values(part.text) & facts.leak_values:
            errors.append("câu về bài này chứa giá trị đáp án")
    return errors
