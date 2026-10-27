"""Kiểm chứng toán học cho phương trình một ẩn và bước tính số học bằng SymPy.

Một bước biến đổi phương trình được coi là đúng khi tập nghiệm thực của nó bằng
tập nghiệm của đề bài (biến đổi tương đương). Một bước số học "a = b" (không có
ẩn) đúng khi vế trái tính chính xác (số hữu tỉ) ra đúng vế phải. Đây là lõi kiểm
chứng dùng chung cho nhãn dữ liệu, bộ kiểm kho bài và dịch vụ gợi ý.

An toàn (chống DoS và chống chạy mã):
- sympy.parse_expr dùng eval bên trong, nên mọi chuỗi đầu vào phải qua danh sách
  ký tự cho phép trước khi phân tích. Không bỏ bước kiểm tra này.
- Phân tích với evaluate=False để SymPy KHÔNG tính gì lúc parse (trước đây
  `9^9^9` bị tính ngay khi parse và treo tiến trình). Sau đó duyệt cây chưa đánh
  giá và từ chối: lũy thừa lồng nhau, số mũ không phải số nguyên trong [-10, 10],
  bậc theo ẩn > 2, độ sâu cây > 20, số nút > 200, số có > 15 chữ số. Chỉ khi cây
  qua hết giới hạn mới đánh giá và gọi solveset, nên chi phí tính bị chặn.
- Mã gọi từ dữ liệu không tin cậy (validator, dịch vụ gợi ý) vẫn nên chạy qua
  `safe_math` để có thời hạn cứng.
"""

from __future__ import annotations

import functools
import itertools
import re
from collections.abc import Mapping
from typing import Any

import sympy
from sympy.parsing.sympy_parser import (
    convert_xor,
    implicit_multiplication_application,
    parse_expr,
    rationalize,
    standard_transformations,
)

VARIABLE = sympy.Symbol("x", real=True)

PROBLEM_KINDS = ("equation", "arithmetic")
MISCONCEPTION_TYPES = (
    "sign_error",
    "distribution_error",
    "arithmetic_slip",
    "missing_root",
    "wrong_factor",
    "wrong_operation",
)

# Chỉ chữ số, ẩn x, toán tử số học, ngoặc, dấu chấm, khoảng trắng (đáp án thêm {} và dấu phẩy).
_ALLOWED_EXPRESSION = re.compile(r"^[0-9x+\-*/^().\s]+$")
_ALLOWED_ANSWER = re.compile(r"^\{?[0-9x+\-*/^().,\s]+\}?$")
_NUMBER_RUN = re.compile(r"[0-9.]+")
_VALID_NUMBER = re.compile(r"\d+(?:\.\d*)?|\.\d+")
# "1 000" hay "1 .5" bị implicit multiplication hiểu thành tích — mơ hồ, từ chối.
_ADJACENT_NUMBERS = re.compile(r"[0-9.]\s+[0-9.]")
_MAX_LENGTH = 200
_MAX_DEPTH = 20
_MAX_NODES = 200
_MAX_DIGITS = 15
_MAX_EXPONENT = 10
_MAX_DEGREE = 2
# rationalize: số thập phân thành Rational chính xác (0.5 -> 1/2), không dùng Float.
_TRANSFORMS = standard_transformations + (implicit_multiplication_application, convert_xor, rationalize)
_NON_FINITE = (sympy.zoo, sympy.nan, sympy.oo, -sympy.oo)
_CACHE_SIZE = 4096


class MathInputError(ValueError):
    """Chuỗi đầu vào không hợp lệ hoặc không an toàn để phân tích."""


def _check_text(text: str) -> None:
    """Kiểm tra rẻ trên chuỗi thô, trước khi đưa vào parser."""
    if not text or len(text) > _MAX_LENGTH or not _ALLOWED_EXPRESSION.match(text) or "__" in text:
        raise MathInputError(f"biểu thức không hợp lệ: {text!r}")
    # `//` (chia lấy nguyên) không được chuyển sang dạng chưa đánh giá: Python sẽ tính ngay lúc parse.
    if "//" in text:
        raise MathInputError(f"không hỗ trợ phép '//': {text!r}")
    if _ADJACENT_NUMBERS.search(text):
        raise MathInputError(f"hai số đứng liền nhau không có phép toán: {text!r}")
    depth = 0
    for char in text:
        if char == "(":
            depth += 1
            if depth > _MAX_DEPTH:
                raise MathInputError(f"ngoặc lồng quá {_MAX_DEPTH} tầng: {text[:40]!r}…")
        elif char == ")":
            depth -= 1
            if depth < 0:
                raise MathInputError(f"ngoặc không cân: {text!r}")
    if depth != 0:
        raise MathInputError(f"ngoặc không cân: {text!r}")
    for number in _NUMBER_RUN.findall(text):
        if not _VALID_NUMBER.fullmatch(number):
            raise MathInputError(f"số không hợp lệ: {number!r}")
        if sum(char.isdigit() for char in number) > _MAX_DIGITS:
            raise MathInputError(f"số quá {_MAX_DIGITS} chữ số: {number[:20]!r}…")


def _evaluate(node: sympy.Basic) -> sympy.Expr:
    """Đánh giá cây đã qua kiểm tra, từ lá lên gốc (chỉ Add/Mul/Pow/số/x)."""
    if not node.args:
        return node
    return node.func(*(_evaluate(arg) for arg in node.args))


def _check_tree(expr: sympy.Basic) -> int:
    """Duyệt cây chưa đánh giá, từ chối thứ có thể làm SymPy tính lâu; trả về bậc theo x.

    Bậc tính theo cấu trúc (cận trên, không khai triển): Add lấy max, Mul cộng,
    Pow nhân với |số mũ|. Pow có số mũ -1 là phép chia (a/b được parse thành
    a*b^-1) nên không tính là lũy thừa lồng nhau; mọi Pow khác nằm trong cơ số
    của một Pow khác thì bị từ chối. Số mũ chỉ gồm số, cộng, nhân (không ẩn,
    không chia, không lũy thừa) và phải ra số nguyên trong [-10, 10].
    """
    budget = [_MAX_NODES]

    def visit(node: sympy.Basic, depth: int, in_power: bool, in_exponent: bool) -> int:
        budget[0] -= 1
        if budget[0] < 0:
            raise MathInputError(f"biểu thức quá {_MAX_NODES} nút")
        if depth > _MAX_DEPTH:
            raise MathInputError(f"cây biểu thức sâu quá {_MAX_DEPTH} tầng")
        if node.is_Number:
            if not node.is_Rational:
                raise MathInputError(f"số không hợp lệ: {node}")
            return 0
        if node.is_Symbol:
            if in_exponent:
                raise MathInputError("số mũ không được chứa ẩn")
            if node != VARIABLE:
                raise MathInputError(f"chỉ cho phép ẩn x, gặp {node}")
            return 1
        if isinstance(node, sympy.Pow):
            if in_exponent:
                raise MathInputError("số mũ phải là số nguyên, không chứa lũy thừa hay phép chia")
            base, exponent = node.args
            visit(exponent, depth + 1, in_power, True)
            value = _evaluate(exponent)
            if not value.is_Integer or abs(value) > _MAX_EXPONENT:
                raise MathInputError(f"số mũ phải là số nguyên trong [-{_MAX_EXPONENT}, {_MAX_EXPONENT}], gặp {value}")
            is_division = value == -1
            if in_power and not is_division:
                raise MathInputError("không cho phép lũy thừa lồng nhau")
            return visit(base, depth + 1, in_power or not is_division, False) * abs(int(value))
        if isinstance(node, (sympy.Add, sympy.Mul)):
            degrees = [visit(arg, depth + 1, in_power, in_exponent) for arg in node.args]
            return max(degrees) if isinstance(node, sympy.Add) else sum(degrees)
        raise MathInputError(f"phép toán không hỗ trợ: {type(node).__name__}")

    return visit(expr, 1, False, False)


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _parse_side(text: str) -> sympy.Expr:
    text = text.strip()
    _check_text(text)
    try:
        tree = parse_expr(text, local_dict={"x": VARIABLE}, transformations=_TRANSFORMS, evaluate=False)
    except Exception as exc:  # noqa: BLE001 — ranh giới đầu vào: mọi lỗi parser (SyntaxError, TokenError, ...) là đầu vào hỏng
        raise MathInputError(f"không phân tích được {text!r}: {type(exc).__name__}") from exc
    if not isinstance(tree, sympy.Basic):
        raise MathInputError(f"không phải biểu thức: {text!r}")
    degree = _check_tree(tree)
    if degree > _MAX_DEGREE:
        raise MathInputError(f"bậc theo x vượt quá {_MAX_DEGREE}: {text!r}")
    expr = _evaluate(tree)
    if expr.has(*_NON_FINITE):
        raise MathInputError(f"biểu thức không xác định (chia cho 0?): {text!r}")
    return expr


def parse_equation(text: str) -> sympy.Eq:
    """Phân tích chuỗi dạng 'vế trái = vế phải' thành sympy.Eq."""
    parts = text.split("=")
    if len(parts) != 2:
        raise MathInputError(f"cần đúng một dấu '=': {text!r}")
    return sympy.Eq(_parse_side(parts[0]), _parse_side(parts[1]), evaluate=False)


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _solve(equation_text: str) -> sympy.Set:
    eq = parse_equation(equation_text)
    return sympy.solveset(eq.lhs - eq.rhs, VARIABLE, domain=sympy.S.Reals)


def solution_set(equation_text: str) -> sympy.Set:
    """Tập nghiệm thực của phương trình một ẩn x."""
    return _solve(equation_text)


def step_is_equivalent(problem_equation: str, step_equation: str) -> bool:
    """Bước làm đúng khi giữ nguyên tập nghiệm của đề bài."""
    return solution_set(problem_equation) == solution_set(step_equation)


def parse_answer(text: str) -> sympy.Set:
    """Đáp án dạng '4' hoặc '{2, 3}' thành tập hữu hạn các số."""
    text = text.strip()
    if not text or len(text) > _MAX_LENGTH or not _ALLOWED_ANSWER.match(text):
        raise MathInputError(f"đáp án không hợp lệ: {text!r}")
    items = [item for item in text.strip("{}").split(",") if item.strip()]
    if not items:
        raise MathInputError(f"đáp án rỗng: {text!r}")
    values = [_parse_side(item) for item in items]
    if any(value.free_symbols for value in values):
        raise MathInputError(f"đáp án còn chứa ẩn: {text!r}")
    return sympy.FiniteSet(*values)


def answer_matches(problem_equation: str, answer_text: str) -> bool:
    """Đáp án đúng khi trùng khớp toàn bộ tập nghiệm (thiếu nghiệm là sai)."""
    return solution_set(problem_equation) == parse_answer(answer_text)


def _number(text: str) -> sympy.Rational:
    """Một giá trị số không chứa ẩn, tính chính xác."""
    value = _parse_side(text)
    if value.free_symbols or not value.is_Rational:
        raise MathInputError(f"cần một giá trị số không chứa ẩn: {text!r}")
    return value


def _arith_sides(text: str) -> tuple[sympy.Rational, sympy.Rational]:
    eq = parse_equation(text)
    if eq.lhs.free_symbols or eq.rhs.free_symbols or not (eq.lhs.is_Rational and eq.rhs.is_Rational):
        raise MathInputError(f"bước số học không được chứa ẩn: {text!r}")
    return eq.lhs, eq.rhs


def arith_step(text: str) -> bool:
    """Bước số học 'a = b' đúng khi vế trái tính chính xác (Rational) bằng vế phải.

    Chỉ số, + - * / ^, ngoặc, số thập phân (đổi thành phân số chính xác: 0.1 + 0.2 = 0.3
    là đúng). Có ẩn x thì ném MathInputError vì đó không phải bước số học.
    """
    lhs, rhs = _arith_sides(text)
    return lhs == rhs


def problem_kind(problem: Mapping[str, Any]) -> str:
    """Loại bài: trường `kind`; bài cũ không có `kind` nhưng có `equation` là `equation`."""
    kind = problem.get("kind")
    if kind is None and "equation" in problem:
        kind = "equation"
    if kind not in PROBLEM_KINDS:
        raise MathInputError(f"loại bài không hỗ trợ: {kind!r}")
    return kind


def _text_field(record: Mapping[str, Any], key: str) -> str:
    value = record.get(key)
    if not isinstance(value, str):
        raise MathInputError(f"thiếu trường chuỗi {key!r} trong {record.get('problem_id', 'bài')}")
    return value


def _misconceptions(problem: Mapping[str, Any], step_index: int | None) -> list[tuple[str, str]]:
    """(type, expression) của các lỗi đã biết; lỗi gắn đúng step_index được thử trước."""
    raw = problem.get("misconceptions") or []
    if not isinstance(raw, list):
        raise MathInputError("misconceptions phải là danh sách")
    exact, others = [], []
    for item in raw:
        if not isinstance(item, Mapping) or item.get("type") not in MISCONCEPTION_TYPES:
            raise MathInputError(f"misconception không hợp lệ: {item!r}")
        entry = (item["type"], _text_field(item, "expression"))
        (exact if step_index is not None and item.get("step_index") == step_index else others).append(entry)
    return exact + others


def _result(equivalent: bool, misconception: str | None) -> dict[str, Any]:
    return {"equivalent": equivalent, "misconception": misconception}


def _equation_solutions(text: str) -> sympy.Set:
    """Bước có '=' là phương trình; không có '=' là đáp án ('4' hoặc '{2, 3}')."""
    return solution_set(text) if "=" in text else parse_answer(text)


def _is_partial_negation(student: sympy.FiniteSet, truth: sympy.FiniteSet) -> bool:
    """student có được từ truth bằng cách đổi dấu một phần hoặc toàn bộ nghiệm."""
    roots = truth.args
    if len(student.args) != len(roots) or len(roots) > 4:
        return False
    for signs in itertools.product((1, -1), repeat=len(roots)):
        if -1 in signs and sympy.FiniteSet(*(s * r for s, r in zip(signs, roots))) == student:
            return True
    return False


def _diagnose_equation(problem: Mapping[str, Any], expression: str, known: list[tuple[str, str]]) -> dict[str, Any]:
    truth = solution_set(_text_field(problem, "equation"))
    student = _equation_solutions(expression)
    if student == truth:
        return _result(True, None)
    for kind, text in known:
        if _equation_solutions(text) == student:
            return _result(False, kind)
    if isinstance(truth, sympy.FiniteSet) and isinstance(student, sympy.FiniteSet):
        if student.is_subset(truth) is True:
            return _result(False, "missing_root")
        if _is_partial_negation(student, truth):
            return _result(False, "sign_error")
    return _result(False, None)


def _arith_pair(text: str) -> tuple[sympy.Rational | None, sympy.Rational]:
    """(vế trái, vế phải) của bước 'a = b'; giá trị trần (không '=') cho (None, giá trị)."""
    if "=" in text:
        return _arith_sides(text)
    return None, _number(text)


def _same_arith(a: tuple[sympy.Rational | None, sympy.Rational], b: tuple[sympy.Rational | None, sympy.Rational]) -> bool:
    if a[0] is None or b[0] is None:
        return a[1] == b[1]
    return a == b


def _diagnose_arithmetic(
    problem: Mapping[str, Any], expression: str, step_index: int | None, known: list[tuple[str, str]]
) -> dict[str, Any]:
    steps = problem.get("steps") or []
    if not isinstance(steps, list) or not all(isinstance(step, str) for step in steps):
        raise MathInputError("steps phải là danh sách chuỗi")
    student = _arith_pair(expression)
    lhs, claimed = student
    valid = lhs is None or lhs == claimed
    if step_index is not None:
        if isinstance(step_index, bool) or not 1 <= step_index <= len(steps):
            raise MathInputError(f"step_index {step_index} ngoài [1, {len(steps)}]")
        expected = {_arith_sides(steps[step_index - 1])[1]}
    else:
        expected = {_number(_text_field(problem, "answer"))}
        if lhs is not None:
            expected |= {_arith_sides(step)[1] for step in steps}
    if valid and claimed in expected:
        return _result(True, None)
    for kind, text in known:
        if _same_arith(_arith_pair(text), student):
            return _result(False, kind)
    if any(value != 0 and claimed == -value for value in expected):
        return _result(False, "sign_error")
    if lhs is None:
        return _result(False, None)
    return _result(False, "wrong_operation" if valid else "arithmetic_slip")


def diagnose(problem: Mapping[str, Any], expression: str, step_index: int | None = None) -> dict[str, Any]:
    """Chẩn đoán một bước/đáp án của người học: {"equivalent": bool, "misconception": str | None}.

    `problem` theo lược đồ kho bài: `kind` ("equation" | "arithmetic"; thiếu thì suy
    từ `equation`), `equation` (bài phương trình), `answer`, `steps` (chuỗi bước mẫu),
    `misconceptions` ([{type, step_index, expression}]). `step_index` đánh số từ 1
    (bước thứ step_index trong `steps`), cùng quy ước với `misconceptions[].step_index`
    và với `payload.step_index` của sự kiện submit_step.

    Bài phương trình (`expression` có '=' là một bước, không có '=' là đáp án '4' / '{2, 3}'):
    1. equivalent khi tập nghiệm của bước bằng tập nghiệm của đề.
    2. Nếu không: khớp lỗi đã biết khi tập nghiệm bằng tập nghiệm của biểu thức lỗi
       (lỗi gắn đúng step_index thử trước).
    3. Quy tắc chung: tập nghiệm là tập con thực sự, khác rỗng của tập nghiệm đúng
       → missing_root; có được từ nghiệm đúng bằng đổi dấu một phần/toàn bộ
       → sign_error; còn lại → None.

    Bài số học (`expression` 'a = b' không có ẩn, hoặc giá trị trần là kết quả):
    1. Giá trị kỳ vọng: vế phải của bước thứ step_index nếu có step_index; nếu không
       là `answer` cộng vế phải của mọi bước mẫu (giá trị trần chỉ so với `answer`).
    2. equivalent khi bước tính đúng (vế trái = vế phải, chính xác) và kết quả
       nằm trong giá trị kỳ vọng.
    3. Nếu không: khớp lỗi đã biết khi cả hai vế bằng giá trị (chỉ vế phải nếu một
       bên là giá trị trần).
    4. Quy tắc chung: kết quả là số đối của một giá trị kỳ vọng khác 0 → sign_error;
       bước tính sai → arithmetic_slip; tính đúng nhưng kết quả khác kỳ vọng
       → wrong_operation; giá trị trần sai không rõ nguyên nhân → None.

    Hạn chế: bài số học so với đường giải mẫu, nên một cách giải khác đúng nhưng
    có kết quả trung gian lạ sẽ bị coi là wrong_operation.
    Đầu vào hỏng (của người học hoặc của kho bài) ném MathInputError.
    """
    if not isinstance(expression, str):
        raise MathInputError("expression phải là chuỗi")
    kind = problem_kind(problem)
    known = _misconceptions(problem, step_index)
    if kind == "equation":
        return _diagnose_equation(problem, expression, known)
    return _diagnose_arithmetic(problem, expression, step_index, known)
