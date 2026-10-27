"""Sinh gợi ý: giao diện `HintGenerator` với hai cách (trục ablation `--hint-generator template|ladder`).

- `template`: gợi ý mẫu không cá nhân hóa (`templates.py`), đã kiểm bằng SymPy lúc khởi động.
- `ladder`: thang 3 bậc cá nhân hóa theo bài của người học, ghép từ bảng câu mẫu (`phrases.py`),
  bước giải đã kiểm của bài (`verify.ProblemFacts`) và bài tương tự từ chỉ mục đảo (`related_index`):
  bậc 1 = khái niệm kỹ năng + lời nhắc theo loại lỗi + bài tương tự (tập dev);
  bậc 2 = chẩn đoán loại lỗi + dạng của bước tiếp (số thay bằng □);
  bậc 3 = bước tiếp theo cụ thể đã kiểm. Bước lộ đáp án chỉ được đưa khi `answer_allowed`; khi chưa được
  phép, bậc 3 nói rõ bước tiếp ra đáp án và chỉ đưa **dạng** của chính bước đó (cách ghép của bậc 2),
  ghi chú kiểm toán `answer_locked`.

Mọi câu khẳng định về bước tiếp theo của bài đều suy ra từ sự thật của chính bài (`ProblemFacts`: bước đã
kiểm hoặc dạng của nó); câu chung của kỹ năng chỉ nói khái niệm. Hai đường rơi về mẫu cùng bậc còn giữ để
fail-closed — bài chưa qua kiểm SymPy (`problem_unverified`) và bản nháp không qua `verify_draft`
(`verify_failed`) — không xảy ra trên kho bài hiện tại (test quét toàn kho kiểm điều này).

Không dùng LLM: mọi câu chữ là câu mẫu, mọi biểu thức là bước đã kiểm. `HintEngine` dựng một lần
từ kho bài (SymPy chạy ở đây, lúc khởi động) rồi phục vụ mọi yêu cầu bằng tra bảng thuần Python.
"""

from __future__ import annotations

import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.hints import phrases
from ai_math_tutor.hints.ladder import LevelDecision
from ai_math_tutor.hints.templates import (
    LEVELS,
    MISCONCEPTIONS,
    SKILL_TEMPLATES,
    HintTemplate,
    generic_hint,
    template_for_skills,
    verify_template,
)
from ai_math_tutor.hints.verify import (
    CLAIM_EXAMPLE,
    CLAIM_STEP,
    ROLE_EXAMPLE,
    ROLE_FORM,
    ROLE_GENERIC,
    ROLE_OWN,
    Claim,
    HintDraft,
    Part,
    ProblemFacts,
    StepFact,
    build_all_facts,
    verify_draft,
)
from ai_math_tutor.problem_bank import Problem, ProblemBank, display_expression
from ai_math_tutor.related_index import RelatedIndex

BANK_CANDIDATES = ("problem-bank-v2.jsonl", "problem-bank-v1.jsonl")
DEFAULT_BANK_DIR = REPO_ROOT / "data_sample"
GENERATORS = ("template", "ladder")
SOURCE_TEMPLATE = "template"
SOURCE_LADDER = "ladder"
NOTE_ANSWER_LOCKED = "answer_locked"  # bậc 3 ladder chỉ đưa dạng bước vì bước tiếp lộ đáp án
RELATED_CANDIDATES = 5
# Đề bài tương tự (tiếng Anh từ NuminaMath) chỉ được chép vào câu khi ngắn và không có công thức.
_MAX_SIMILAR_STATEMENT = 280
_UNSAFE_STATEMENT_CHARS = ("=", "$", "\\")


# "x" đứng riêng trong câu tiếng Việt ("chứa x", "của x"); \w Unicode nên "xét", "xác" không bị đụng.
_PROSE_VARIABLE = re.compile(r"(?<!\w)x(?!\w)")


def to_machine_expression(text: str, variable: str | None) -> str:
    """Biểu thức người học viết theo ẩn gốc của đề (`2*y = 8`) → theo ẩn `x` cho `math_check`.

    Chiều ngược của `problem_bank.display_expression`; ẩn `x` vẫn giữ nguyên nếu người học dùng nó.
    """
    if not variable or variable == "x":
        return text
    return re.sub(rf"(?<![A-Za-z]){re.escape(variable)}(?![A-Za-z])", "x", text)


def _render_text(text: str, expressions: Iterable[str], variable: str | None) -> str:
    """Biểu thức đã kiểm → dạng đọc (`display_expression`); câu chữ → chỉ đổi chữ "x" đứng riêng."""
    expressions = sorted({e for e in expressions if e and e in text}, key=len, reverse=True)
    pieces = re.split("(" + "|".join(map(re.escape, expressions)) + ")", text) if expressions else [text]
    rendered = []
    for piece in pieces:
        if piece in expressions:
            rendered.append(display_expression(piece, variable))
        elif variable and variable != "x":
            rendered.append(_PROSE_VARIABLE.sub(variable, piece))
        else:
            rendered.append(piece)
    return "".join(rendered)


def render_draft(draft: HintDraft, variable: str | None, example_variable: str | None) -> tuple[str, tuple[str, ...]]:
    """Bản nháp đã kiểm (ẩn x) → (`text_vi`, `math`) hiển thị theo ẩn gốc của từng bài.

    Kiểm chứng luôn chạy trên dạng máy; chỉ sau khi đạt mới đổi ẩn — phép đổi tên ẩn không đổi
    tập nghiệm hay con số nào, nên kết luận kiểm chứng vẫn đúng với dạng hiển thị.
    """
    own = [c.expression for c in draft.claims if c.kind == CLAIM_STEP]
    examples = [c.expression for c in draft.claims if c.kind == CLAIM_EXAMPLE]
    parts = []
    for part in draft.parts:
        if part.role == ROLE_FORM:
            parts.append(display_expression(part.text, variable))
        elif part.role == ROLE_EXAMPLE:
            parts.append(_render_text(part.text, examples, example_variable))
        else:
            parts.append(_render_text(part.text, own, variable))
    math = tuple(display_expression(c.expression, example_variable if c.kind == CLAIM_EXAMPLE else variable)
                 for c in draft.claims)
    return "".join(parts).strip(), math


def default_problem_bank_path(data_dir: Path = DEFAULT_BANK_DIR) -> Path:
    """Kho bài v2 nếu đã có, nếu không thì v1 (cùng lược đồ, cùng API `problem_bank`)."""
    for name in BANK_CANDIDATES:
        path = data_dir / name
        if path.is_file():
            return path
    raise FileNotFoundError(f"không tìm thấy kho bài ({', '.join(BANK_CANDIDATES)}) trong {data_dir}")


@dataclass(frozen=True, slots=True)
class HintContext:
    """Một yêu cầu gợi ý đã chuẩn hóa: bài, bước, kết quả chẩn đoán biểu thức người học."""

    problem: Problem
    facts: ProblemFacts
    step_index: int  # 1..len(steps) là đang làm bước đó; len(steps)+1 là đang nộp đáp án cuối
    expression_status: str = "none"  # phrases.EXPRESSION_STATUSES
    misconception: str | None = None
    example: Problem | None = None
    example_facts: ProblemFacts | None = None

    def __post_init__(self) -> None:
        if self.expression_status not in phrases.EXPRESSION_STATUSES:
            raise ValueError(f"expression_status lạ: {self.expression_status!r}")
        if self.misconception not in MISCONCEPTIONS:
            raise ValueError(f"loại lỗi lạ: {self.misconception!r}")
        if not 1 <= self.step_index <= len(self.problem.steps) + 1:
            raise ValueError(f"step_index {self.step_index} ngoài [1, {len(self.problem.steps) + 1}]")


@dataclass(frozen=True, slots=True)
class Hint:
    hint_level: int
    text_vi: str
    math: tuple[str, ...]
    verified: bool
    misconception: str | None
    source: str
    note: str = ""  # chỉ để kiểm toán, không trả cho người học: lý do rơi về mẫu, hoặc NOTE_ANSWER_LOCKED
    machine_math: tuple[str, ...] | None = None  # `math` theo ẩn x (đã kiểm); None = giống `math`

    def response(self, state_source: str) -> dict[str, Any]:
        """Thân phản hồi `POST /hints` theo hợp đồng."""
        return {
            "hint_level": self.hint_level,
            "text_vi": self.text_vi,
            "math": list(self.math),
            "verified": self.verified,
            "misconception": self.misconception,
            "source": self.source,
            "state_source": state_source,
        }


class HintGenerator(Protocol):
    name: str

    def generate(self, ctx: HintContext, decision: LevelDecision) -> Hint: ...


class TemplateLibrary:
    """Gợi ý mẫu đã kiểm lúc khởi động; mẫu không qua kiểm bị thay bằng gợi ý chung không có biểu thức."""

    def __init__(self) -> None:
        self._verified: dict[tuple[str, int, str | None], bool] = {}
        for skill_id in SKILL_TEMPLATES:
            for level in LEVELS:
                for misconception in MISCONCEPTIONS:
                    template = template_for_skills([skill_id], SKILL_TEMPLATES[skill_id].kind, level, misconception)
                    self._verified[(skill_id, level, misconception)] = not verify_template(template)

    def get(self, skill_ids: Iterable[str], kind: str, level: int, misconception: str | None) -> HintTemplate:
        template = template_for_skills(skill_ids, kind, level, misconception)
        if template.skill_id in SKILL_TEMPLATES and not self._verified[(template.skill_id, level, misconception)]:
            return generic_hint(template.skill_id, kind, level, misconception)
        return template

    @property
    def failed(self) -> list[tuple[str, int, str | None]]:
        return [key for key, ok in self._verified.items() if not ok]


class TemplateGenerator:
    name = SOURCE_TEMPLATE

    def __init__(self, library: TemplateLibrary) -> None:
        self._library = library

    def generate(self, ctx: HintContext, decision: LevelDecision, note: str = "") -> Hint:
        template = self._library.get(ctx.problem.skill_ids, ctx.problem.kind, decision.level, ctx.misconception)
        return Hint(
            hint_level=decision.level,
            text_vi=template.text_vi,
            math=template.math,
            verified=True,  # mẫu đã qua verify_template lúc khởi động; mẫu chung không có biểu thức
            misconception=ctx.misconception,
            source=SOURCE_TEMPLATE,
            note=note,
        )


class _Builder:
    def __init__(self) -> None:
        self.parts: list[Part] = []
        self.claims: list[Claim] = []

    def add(self, text: str, role: str, step_index: int | None = None) -> None:
        """Thêm một phần, cách phần trước một dấu cách."""
        if not text:
            return
        if self.parts:
            self.parts.append(Part(" ", ROLE_GENERIC))
        self.parts.append(Part(text, role, step_index))

    def inline(self, text: str, role: str) -> None:
        self.parts.append(Part(text, role))

    def claim(self, expression: str, kind: str, step_index: int | None = None) -> None:
        self.claims.append(Claim(expression, kind, step_index))

    def draft(self, level: int) -> HintDraft:
        return HintDraft(level=level, parts=tuple(self.parts), claims=tuple(self.claims))


def _similar_statement(problem: Problem) -> str | None:
    for text in (problem.statement_vi, problem.statement):
        if text and len(text) <= _MAX_SIMILAR_STATEMENT and not any(c in text for c in _UNSAFE_STATEMENT_CHARS):
            return " ".join(text.split())
    return None


class LadderGenerator:
    name = SOURCE_LADDER

    def __init__(self, fallback: TemplateGenerator) -> None:
        self._fallback = fallback

    def target_step(self, ctx: HintContext) -> int:
        """Bước gợi ý nhắm tới: bước đang làm; bước sau nếu biểu thức vừa gửi đã đúng; bước cuối khi nộp đáp án."""
        n = len(ctx.problem.steps)
        index = ctx.step_index + 1 if ctx.expression_status == "correct" else ctx.step_index
        return min(index, n)

    def answer_locked(self, ctx: HintContext, decision: LevelDecision) -> bool:
        """Bậc 3 nhắm tới bước lộ đáp án mà chưa được phép đưa đáp án."""
        target = ctx.facts.step(self.target_step(ctx))
        return (decision.level == 3 and target is not None and target.reveals_answer
                and not decision.answer_allowed)

    def draft(self, ctx: HintContext, decision: LevelDecision) -> HintDraft | None:
        """Bản nháp cho bậc đã chọn; None chỉ khi không có bước đã kiểm để nhắm tới (bài chưa qua kiểm)."""
        kind, level, misconception = ctx.problem.kind, decision.level, ctx.misconception
        target = ctx.facts.step(self.target_step(ctx))
        b = _Builder()
        if level == 1:
            skill = next((SKILL_TEMPLATES[s] for s in ctx.problem.skill_ids if s in SKILL_TEMPLATES), None)
            b.add(skill.concept_vi if skill else "", ROLE_GENERIC)
            b.add(phrases.status_phrase(ctx.expression_status), ROLE_OWN)
            b.add(phrases.phrase(kind, 1, misconception), ROLE_OWN)
            self._similar(b, ctx)
            return b.draft(level)
        if target is None:
            return None
        b.add(phrases.status_phrase(ctx.expression_status), ROLE_OWN)
        if level == 2:
            undiagnosed = misconception is None and ctx.expression_status != "wrong"
            b.add(phrases.UNDIAGNOSED_L2_VI[kind] if undiagnosed else phrases.phrase(kind, 2, misconception), ROLE_OWN)
            self._form(b, phrases.FORM_LEAD_VI[kind], target)
            return b.draft(level)
        locked = self.answer_locked(ctx, decision)
        # Câu bậc 3 không có loại lỗi là "làm theo đúng bước dưới đây": sai khi bước đó đang bị che.
        if not (locked and misconception is None):
            b.add(phrases.phrase(kind, 3, misconception), ROLE_OWN)
        if locked:
            self._form(b, phrases.LOCKED_FORM_LEAD_VI[kind], target)
            return b.draft(level)
        b.add(f"{phrases.STEP_LEAD_VI[kind]} {target.text}{phrases.STEP_TAIL_VI}", ROLE_OWN)
        b.claim(target.text, CLAIM_STEP, target.index)
        return b.draft(level)

    @staticmethod
    def _form(b: _Builder, lead: str, target: StepFact) -> None:
        """Lời dẫn + dạng của bước đích (số thay bằng □) + câu giải thích ô trống; không có biểu thức cần kiểm."""
        b.add(lead, ROLE_OWN)
        b.add(target.form, ROLE_FORM, target.index)
        b.inline(phrases.FORM_TAIL_VI, ROLE_OWN)

    def _similar(self, b: _Builder, ctx: HintContext) -> None:
        example, facts = ctx.example, ctx.example_facts
        if example is None or facts is None or not facts.verified:
            return
        if example.equation:
            b.add(phrases.SIMILAR_EQUATION_VI.format(problem_id=example.problem_id, equation=example.equation),
                  ROLE_EXAMPLE)
            b.claim(example.equation, CLAIM_EXAMPLE)
            return
        statement = _similar_statement(example)
        if statement:
            b.add(phrases.SIMILAR_TEXT_VI.format(problem_id=example.problem_id, statement=statement), ROLE_EXAMPLE)
        else:
            b.add(phrases.SIMILAR_REFERENCE_VI.format(problem_id=example.problem_id), ROLE_EXAMPLE)

    def generate(self, ctx: HintContext, decision: LevelDecision) -> Hint:
        if not ctx.facts.verified:
            return self._fallback.generate(ctx, decision, note="problem_unverified")
        draft = self.draft(ctx, decision)
        if draft is None:
            return self._fallback.generate(ctx, decision, note="verify_failed: không có bước đã kiểm để nhắm tới")
        examples = {ctx.example_facts.problem_id: ctx.example_facts} if ctx.example_facts else {}
        errors = verify_draft(draft, ctx.facts, examples, allow_answer=decision.answer_allowed)
        if errors:
            return self._fallback.generate(ctx, decision, note="verify_failed: " + "; ".join(errors))
        text_vi, math = render_draft(draft, ctx.problem.variable,
                                     ctx.example.variable if ctx.example is not None else None)
        return Hint(
            hint_level=decision.level,
            text_vi=text_vi,
            math=math,
            verified=True,
            misconception=ctx.misconception,
            source=SOURCE_LADDER,
            note=NOTE_ANSWER_LOCKED if self.answer_locked(ctx, decision) else "",
            machine_math=draft.math,
        )


@dataclass
class HintEngine:
    """Kho bài + sự thật đã kiểm + bài tương tự + hai bộ sinh; dựng một lần, dùng cho mọi yêu cầu."""

    bank: ProblemBank
    facts: Mapping[str, ProblemFacts]
    related: Mapping[str, str | None]
    library: TemplateLibrary
    generators: dict[str, HintGenerator] = field(default_factory=dict)

    @classmethod
    def from_bank(cls, bank: ProblemBank) -> HintEngine:
        facts = build_all_facts(bank.problems)
        index = RelatedIndex.build(bank.problems)
        related: dict[str, str | None] = {}
        for problem in bank.problems:
            candidates = index.related(problem, limit=RELATED_CANDIDATES)
            related[problem.problem_id] = next((pid for pid in candidates if facts[pid].verified), None)
        library = TemplateLibrary()
        template = TemplateGenerator(library)
        generators: dict[str, HintGenerator] = {SOURCE_TEMPLATE: template, SOURCE_LADDER: LadderGenerator(template)}
        return cls(bank=bank, facts=facts, related=related, library=library, generators=generators)

    def generator(self, name: str) -> HintGenerator:
        if name not in GENERATORS:
            raise ValueError(f"hint generator phải thuộc {GENERATORS}, nhận {name!r}")
        return self.generators[name]

    def context(self, problem_id: str, step_index: int, expression_status: str = "none",
                misconception: str | None = None) -> HintContext:
        problem = self.bank.get(problem_id)
        example_id = self.related.get(problem_id)
        return HintContext(
            problem=problem,
            facts=self.facts[problem_id],
            step_index=step_index,
            expression_status=expression_status,
            misconception=misconception,
            example=self.bank.get(example_id) if example_id else None,
            example_facts=self.facts[example_id] if example_id else None,
        )

    @property
    def unverified_problems(self) -> list[str]:
        return sorted(pid for pid, f in self.facts.items() if not f.verified)
