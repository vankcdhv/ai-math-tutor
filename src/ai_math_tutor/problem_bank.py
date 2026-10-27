"""Kho bài toán: lược đồ một bài, nạp/kiểm tra từ JSONL và kiểm chứng toán học.

Mỗi dòng JSONL là một bài với các trường:
`problem_id`, `source` (`self` | `numina:<hàng>`), `statement`, `kind`
(`equation` | `arithmetic`), `equation` (bắt buộc khi kind = equation), `answer`,
`skill_ids`, `steps` (chuỗi bước giải đúng), `misconceptions`
(`{type, step_index, expression}` — biểu thức sai tại bước `step_index`, đánh số từ 1),
`difficulty` (1–5), `split` (`dev` | `test`), `solution_source` (`authored` | `derived_from_cot` | `generated_sympy`);
tùy chọn `statement_vi`, `verified_by`, `variable`.

`variable` (chỉ bài phương trình) là chữ cái ẩn gốc của đề (`m`, `y`, …). `equation`, `steps`,
`misconceptions[].expression` LUÔN viết theo ẩn `x` vì `math_check` chỉ kiểm ẩn `x`; bài vắng
`variable` coi như ẩn `x`. Khi hiển thị cho người đọc, thay `x` bằng ẩn gốc qua
`display_expression` / `Problem.display_steps`; không bao giờ đưa chuỗi đã thay vào bộ kiểm.

Nạp chỉ kiểm cấu trúc (rẻ). `verify_problem` kiểm toán học bằng `math_check`
(chậm — dùng khi build kho và trong test, không dùng trong vòng sinh sự kiện).
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any

from ai_math_tutor import math_check

KINDS = ("equation", "arithmetic")
SPLITS = ("dev", "test")
# Nguồn gốc lời giải: viết trực tiếp; suy từ lời giải CoT của bộ dữ liệu; sinh bằng SymPy.
SOLUTION_SOURCES = ("authored", "derived_from_cot", "generated_sympy")
MISCONCEPTION_TYPES = (
    "sign_error",
    "distribution_error",
    "arithmetic_slip",
    "missing_root",
    "wrong_factor",
    "wrong_operation",
)
REQUIRED_FIELDS = (
    "problem_id", "source", "statement", "kind", "answer", "skill_ids",
    "steps", "misconceptions", "difficulty", "split", "solution_source",
)
OPTIONAL_FIELDS = ("equation", "statement_vi", "verified_by", "variable")
_SOURCE_RE = re.compile(r"^(self|numina:[0-9]+)$")
# Ẩn gốc: một chữ cái thường, trừ e và i (hằng Euler, đơn vị ảo) như bộ nhận diện phương trình.
_VARIABLE_RE = re.compile(r"^[a-df-hj-z]$")
_MACHINE_VARIABLE = re.compile(r"(?<![A-Za-z])x(?![A-Za-z])")


def display_expression(text: str, variable: str | None) -> str:
    """Biểu thức máy (ẩn `x`, `**`) → dạng đọc: ẩn gốc của đề và `^` (`3*x**2` với ẩn m → `3*m^2`).

    Chỉ dùng để hiển thị; bộ kiểm luôn nhận chuỗi gốc theo ẩn `x`.
    """
    shown = text.replace("**", "^")
    if variable and variable != "x":
        shown = _MACHINE_VARIABLE.sub(variable, shown)
    return shown


def _contract_pattern(field_name: str, item: bool = False) -> re.Pattern[str]:
    """Lấy regex từ hợp đồng sự kiện để mã bài/kỹ năng trong kho luôn hợp lệ khi phát sự kiện."""
    text = resources.files("ai_math_tutor.contracts").joinpath("learner_event_v0_1.schema.json").read_text(encoding="utf-8")
    spec = json.loads(text)["properties"][field_name]
    return re.compile((spec["items"] if item else spec)["pattern"])


_PROBLEM_ID_RE = _contract_pattern("problem_id")
_SKILL_ID_RE = _contract_pattern("skill_ids", item=True)


class ProblemBankError(ValueError):
    """Bài toán sai lược đồ hoặc kho bài không nhất quán."""


@dataclass(frozen=True, slots=True)
class Misconception:
    type: str
    step_index: int
    expression: str


@dataclass(frozen=True, slots=True)
class Problem:
    problem_id: str
    source: str
    statement: str
    kind: str
    equation: str | None
    answer: str
    skill_ids: tuple[str, ...]
    steps: tuple[str, ...]
    misconceptions: tuple[Misconception, ...]
    difficulty: int
    split: str
    solution_source: str
    statement_vi: str | None = None
    verified_by: tuple[str, ...] = ()
    variable: str | None = None

    @property
    def primary_skill(self) -> str:
        return self.skill_ids[0]

    @property
    def display_variable(self) -> str:
        """Ẩn hiển thị: ẩn gốc của đề, mặc định `x`."""
        return self.variable or "x"

    def display(self, text: str) -> str:
        """Một bước/biểu thức máy của bài này viết theo ẩn gốc của đề (chỉ để hiển thị)."""
        return display_expression(text, self.variable)

    @property
    def display_steps(self) -> tuple[str, ...]:
        return tuple(self.display(step) for step in self.steps)

    def to_dict(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "problem_id": self.problem_id,
            "source": self.source,
            "statement": self.statement,
            "kind": self.kind,
        }
        if self.equation is not None:
            row["equation"] = self.equation
        row.update({
            "answer": self.answer,
            "skill_ids": list(self.skill_ids),
            "steps": list(self.steps),
            "misconceptions": [
                {"type": m.type, "step_index": m.step_index, "expression": m.expression} for m in self.misconceptions
            ],
            "difficulty": self.difficulty,
            "split": self.split,
            "solution_source": self.solution_source,
        })
        if self.statement_vi is not None:
            row["statement_vi"] = self.statement_vi
        if self.verified_by:
            row["verified_by"] = list(self.verified_by)
        if self.variable is not None:
            row["variable"] = self.variable
        return row


def _non_empty_str(row: Mapping[str, Any], key: str, pid: str) -> str:
    value = row.get(key)
    if not isinstance(value, str) or not value.strip():
        raise ProblemBankError(f"{pid}: '{key}' phải là chuỗi khác rỗng")
    return value


def _str_list(row: Mapping[str, Any], key: str, pid: str) -> tuple[str, ...]:
    value = row.get(key)
    if not isinstance(value, list) or not value or not all(isinstance(v, str) and v.strip() for v in value):
        raise ProblemBankError(f"{pid}: '{key}' phải là danh sách chuỗi khác rỗng")
    return tuple(value)


def _choice(row: Mapping[str, Any], key: str, allowed: tuple[str, ...], pid: str) -> str:
    value = row.get(key)
    if value not in allowed:
        raise ProblemBankError(f"{pid}: '{key}' phải thuộc {allowed}, nhận {value!r}")
    return value


def parse_problem(row: Mapping[str, Any]) -> Problem:
    """Kiểm lược đồ một bài (không kiểm toán học) và trả `Problem`."""
    if not isinstance(row, Mapping):
        raise ProblemBankError("mỗi bài phải là object JSON")
    pid = row.get("problem_id")
    if not isinstance(pid, str) or not _PROBLEM_ID_RE.match(pid):
        raise ProblemBankError(f"problem_id không hợp lệ theo hợp đồng sự kiện: {pid!r}")
    missing = [key for key in REQUIRED_FIELDS if key not in row]
    if missing:
        raise ProblemBankError(f"{pid}: thiếu trường {missing}")
    unknown = sorted(set(row) - set(REQUIRED_FIELDS) - set(OPTIONAL_FIELDS))
    if unknown:
        raise ProblemBankError(f"{pid}: trường lạ {unknown}")

    source = _non_empty_str(row, "source", pid)
    if not _SOURCE_RE.match(source):
        raise ProblemBankError(f"{pid}: source phải là 'self' hoặc 'numina:<hàng>', nhận {source!r}")
    kind = _choice(row, "kind", KINDS, pid)
    equation = row.get("equation")
    if kind == "equation":
        if not isinstance(equation, str) or equation.count("=") != 1:
            raise ProblemBankError(f"{pid}: bài equation cần 'equation' có đúng một dấu '='")
    elif equation is not None:
        raise ProblemBankError(f"{pid}: bài arithmetic không có 'equation'")

    skill_ids = _str_list(row, "skill_ids", pid)
    if len(set(skill_ids)) != len(skill_ids) or not all(_SKILL_ID_RE.match(s) for s in skill_ids):
        raise ProblemBankError(f"{pid}: skill_ids phải duy nhất và khớp mẫu hợp đồng: {skill_ids}")
    steps = _str_list(row, "steps", pid)

    raw_misconceptions = row.get("misconceptions")
    if not isinstance(raw_misconceptions, list) or not raw_misconceptions:
        raise ProblemBankError(f"{pid}: cần ít nhất một misconception")
    misconceptions = []
    for item in raw_misconceptions:
        if not isinstance(item, Mapping) or set(item) != {"type", "step_index", "expression"}:
            raise ProblemBankError(f"{pid}: misconception phải có đúng {{type, step_index, expression}}: {item!r}")
        step_index = item["step_index"]
        if isinstance(step_index, bool) or not isinstance(step_index, int) or not 1 <= step_index <= len(steps):
            raise ProblemBankError(f"{pid}: misconception.step_index ngoài [1, {len(steps)}]: {step_index!r}")
        if item["type"] not in MISCONCEPTION_TYPES:
            raise ProblemBankError(f"{pid}: loại misconception lạ {item['type']!r}")
        expression = item["expression"]
        if not isinstance(expression, str) or not expression.strip():
            raise ProblemBankError(f"{pid}: misconception.expression rỗng")
        misconceptions.append(Misconception(item["type"], step_index, expression))

    difficulty = row.get("difficulty")
    if isinstance(difficulty, bool) or not isinstance(difficulty, int) or not 1 <= difficulty <= 5:
        raise ProblemBankError(f"{pid}: difficulty phải là số nguyên 1–5, nhận {difficulty!r}")
    statement_vi = row.get("statement_vi")
    if statement_vi is not None and (not isinstance(statement_vi, str) or not statement_vi.strip()):
        raise ProblemBankError(f"{pid}: statement_vi phải là chuỗi khác rỗng")
    verified_by = _str_list(row, "verified_by", pid) if "verified_by" in row else ()
    variable = row.get("variable")
    if variable is not None:
        if kind != "equation":
            raise ProblemBankError(f"{pid}: 'variable' chỉ dùng cho bài equation")
        if not isinstance(variable, str) or not _VARIABLE_RE.match(variable):
            raise ProblemBankError(f"{pid}: 'variable' phải là một chữ cái thường (không e, i), nhận {variable!r}")

    return Problem(
        problem_id=pid,
        source=source,
        statement=_non_empty_str(row, "statement", pid),
        kind=kind,
        equation=equation,
        answer=_non_empty_str(row, "answer", pid),
        skill_ids=skill_ids,
        steps=steps,
        misconceptions=tuple(misconceptions),
        difficulty=difficulty,
        split=_choice(row, "split", SPLITS, pid),
        solution_source=_choice(row, "solution_source", SOLUTION_SOURCES, pid),
        statement_vi=statement_vi,
        verified_by=verified_by,
        variable=variable,
    )


def iter_jsonl_rows(path: Path) -> Iterator[tuple[int, Any]]:
    with Path(path).open(encoding="utf-8") as handle:
        for number, raw in enumerate(handle, start=1):
            if not raw.strip():
                continue
            try:
                yield number, json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ProblemBankError(f"{path}:{number}: JSON lỗi: {exc}") from exc


def load_problems(path: Path) -> list[Problem]:
    """Nạp và kiểm lược đồ mọi bài trong tệp JSONL; báo lỗi kèm số dòng."""
    problems: list[Problem] = []
    for number, row in iter_jsonl_rows(path):
        try:
            problems.append(parse_problem(row))
        except ProblemBankError as exc:
            raise ProblemBankError(f"{path}:{number}: {exc}") from exc
    return problems


def file_checksum(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


class ProblemBank:
    """Tập bài bất biến có chỉ mục theo kỹ năng; giao diện mà bộ mô phỏng và dịch vụ gợi ý dùng."""

    def __init__(self, problems: Iterable[Problem]):
        self._problems = tuple(problems)
        self._by_id: dict[str, Problem] = {}
        for problem in self._problems:
            if problem.problem_id in self._by_id:
                raise ProblemBankError(f"problem_id trùng: {problem.problem_id}")
            self._by_id[problem.problem_id] = problem
        self._by_primary: dict[str, list[Problem]] = {}
        for problem in self._problems:
            self._by_primary.setdefault(problem.primary_skill, []).append(problem)

    @classmethod
    def from_jsonl(cls, path: Path) -> ProblemBank:
        return cls(load_problems(path))

    @property
    def problems(self) -> tuple[Problem, ...]:
        return self._problems

    def __len__(self) -> int:
        return len(self._problems)

    def get(self, problem_id: str) -> Problem:
        try:
            return self._by_id[problem_id]
        except KeyError as exc:
            raise ProblemBankError(f"không có bài {problem_id}") from exc

    def select(self, split: str | None) -> ProblemBank:
        """Tập con theo split (None = toàn bộ)."""
        if split is None:
            return self
        if split not in SPLITS:
            raise ProblemBankError(f"split phải thuộc {SPLITS}, nhận {split!r}")
        return ProblemBank(p for p in self._problems if p.split == split)

    def primary_skills(self) -> tuple[str, ...]:
        """Kỹ năng chính (skill_ids[0]) có ít nhất một bài, theo thứ tự xuất hiện trong kho."""
        return tuple(self._by_primary)

    def by_primary_skill(self, skill: str) -> tuple[Problem, ...]:
        return tuple(self._by_primary.get(skill, ()))


def _arith_step_checker():
    checker = getattr(math_check, "arith_step", None)
    if checker is None:
        raise ProblemBankError("math_check chưa có arith_step: chưa kiểm được bài arithmetic")
    return checker


def _final_value_matches(step: str, answer: str) -> bool:
    """Vế phải của bước số học cuối bằng đáp án (so tập giá trị)."""
    _, _, rhs = step.rpartition("=")
    return math_check.parse_answer(rhs) == math_check.parse_answer(answer)


def _check_misconception(problem_row: dict[str, Any], item: Misconception) -> str | None:
    """Khứ hồi qua `diagnose`: biểu thức lỗi phải bị chấm sai và được nhận đúng loại lỗi đã khai.

    Không dùng "biểu thức tự nó sai" vì lỗi wrong_operation (48 * 2 = 96 thay vì 48 / 2 = 24)
    hay sign_error trừ ngược (5 - 8 = -3) là phép tính đúng nhưng sai so với bước cần làm.
    """
    diagnosis = math_check.diagnose(problem_row, item.expression, item.step_index)
    if diagnosis["equivalent"]:
        return f"misconception {item.expression!r} (bước {item.step_index}) lại được chấm đúng"
    if diagnosis["misconception"] != item.type:
        return (f"misconception {item.expression!r} (bước {item.step_index}) khai {item.type!r} "
                f"nhưng diagnose nhận {diagnosis['misconception']!r}")
    return None


def verify_problem(problem: Problem) -> list[str]:
    """Kiểm toán học một bài: mọi bước đúng, đáp án khớp, mọi misconception bị chấm sai đúng loại. Trả danh sách lỗi."""
    errors: list[str] = []
    pid = problem.problem_id
    try:
        if problem.kind == "equation":
            equation = problem.equation or ""  # parse_problem đảm bảo có phương trình
            if not math_check.answer_matches(equation, problem.answer):
                errors.append(f"{pid}: đáp án {problem.answer!r} không khớp tập nghiệm của đề")
            for index, step in enumerate(problem.steps, start=1):
                if not math_check.step_is_equivalent(equation, step):
                    errors.append(f"{pid}: bước {index} {step!r} không tương đương đề")
        else:
            arith_step = _arith_step_checker()
            for index, step in enumerate(problem.steps, start=1):
                if not arith_step(step):
                    errors.append(f"{pid}: bước số học {index} {step!r} sai")
            if not _final_value_matches(problem.steps[-1], problem.answer):
                errors.append(f"{pid}: bước cuối không cho đáp án {problem.answer!r}")
        row = problem.to_dict()
        for item in problem.misconceptions:
            error = _check_misconception(row, item)
            if error:
                errors.append(f"{pid}: {error}")
    except math_check.MathInputError as exc:
        errors.append(f"{pid}: {exc}")
    return errors
