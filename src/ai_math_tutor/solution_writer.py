"""Viết lời giải từng bước có kiểm chứng và sinh misconception cho kho bài.

Ba cách viết lời giải, ghi trung thực trong `solution_source` của từng bài:

- `derived_from_cot`: bài số học lời văn (chỉ nguồn gsm8k, orca_math). Lấy các phép tính
  `a op b = c` từ chính lời giải CoT của NuminaMath, chuẩn hóa LaTeX/đơn vị/dấu phân cách
  nghìn thành biểu thức phẳng, chỉ giữ bước mà `math_check.arith_step` xác nhận đúng chính
  xác, bỏ bước đồng nhất (`0 + a = a`, `1 ^ n = 1`, …) và cắt chuỗi ở bước cuối cùng cho ra
  đúng đáp án. Chuỗi phải có ≥ 2 bước, mọi số dùng tới truy được về đề, mọi số liệu của đề
  được dùng trong chuỗi, mọi kết quả trung gian được dùng lại (xem `chain_coverage`), và giá
  trị cuối cùng mà CoT viết ra phải trùng `\\boxed{}` (`cot_final_value`).
- `generated_sympy`: bài phương trình. Sinh bước bằng chương trình: vế chỉ có số thì tính
  dần từng lượt (quy đồng, nhân nghịch đảo, nhận dạng (a ± b)², đặt thừa số chung); bậc nhất
  thì phá ngoặc / quy đồng → chuyển vế → chia hệ số; bậc hai thì dạng chuẩn → tách hạng tử
  giữa → nhóm → nhân tử. Mọi bước giữ nguyên tập nghiệm của đề, tối thiểu 2 bước.
- `authored`: bước viết trực tiếp; vẫn qua cùng bộ kiểm và cùng các bộ lọc chuỗi.

Bước phương trình luôn viết theo ẩn `x` vì `math_check` chỉ kiểm ẩn `x`. Ẩn gốc của đề
(`m`, `y`, …) được lưu riêng ở trường `variable` của kho bài và chỉ thay vào lúc hiển thị
(`problem_bank.display_expression`).

Misconception sinh tự động từ các bước đã kiểm và chỉ được giữ khi
`math_check.diagnose(bài, biểu thức, step_index)` trả về đúng loại của nó (đi một
vòng khép kín). Bài không viết được lời giải kiểm được thì bị loại kèm lý do,
không nới bộ kiểm chính xác (bài phụ thuộc làm tròn bị loại `rounding_dependent`).

Mọi biểu thức đi qua `math_check` đã gia cố (whitelist ký tự, giới hạn cây, bậc,
số chữ số) nên chi phí tính bị chặn kể cả với văn bản lấy từ dữ liệu ngoài.
"""

from __future__ import annotations

import math
import re
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from fractions import Fraction
from typing import Any

import sympy
from sympy.parsing.sympy_parser import (
    convert_xor,
    implicit_multiplication_application,
    parse_expr,
    rationalize,
    standard_transformations,
)

from ai_math_tutor import math_check, numina

X = math_check.VARIABLE

STRATEGIES = ("derived_from_cot", "generated_sympy", "authored")
DROP_REASONS = (
    "arithmetic_source_excluded",  # bài số học từ nguồn không phải bài lời văn (math, synthetic_math)
    "rounding_dependent",     # đáp án chỉ đạt được khi làm tròn / xấp xỉ
    "no_verifiable_steps",    # không tách được phép tính nào đúng chính xác
    "answer_not_reached",     # có phép tính đúng nhưng không bước nào ra đáp án
    "too_few_steps",          # ít hơn MIN_STEPS bước không tầm thường (sau khi bỏ bước đồng nhất)
    "incomplete_chain",       # bước dùng số không có trong đề và không do bước trước tính ra
    "disconnected_steps",     # có bước mà kết quả không được bước sau dùng (lời giải rẽ nhánh)
    "unused_given",           # số liệu của đề không được chuỗi bước dùng tới (đáp án trùng hợp)
    "negative_quantity",      # bài lời văn có kết quả trung gian/đáp án âm (đại lượng đếm/đo không âm)
    "cot_answer_conflict",    # giá trị cuối cùng CoT viết ra khác đáp án \boxed
    "cot_arithmetic_error",   # CoT viết một đẳng thức sai giữa hai phép tính số (lập luận gốc có lỗi)
    "equation_unsupported",   # phương trình ngoài dạng sinh được (nghiệm vô tỉ, ...)
    "no_misconception",       # không có misconception nào đi vòng diagnose khép kín
    "verification_failed",    # lời giải sinh ra không qua bộ kiểm cuối
)

# Số bước không tầm thường tối thiểu của một lời giải NuminaMath (rà soát vòng 1: lời giải
# một bước "x = kết quả" hay "5 * 5 = 25" không cho người học thấy lập luận).
MIN_STEPS = 2

# Kỹ năng trong configs/skills.yaml mà bộ gán kỹ năng dùng.
SKILL_MULTI_STEP = "arithmetic.multi_step"
SKILL_FRACTIONS = "arithmetic.fractions"
SKILL_PERCENT = "arithmetic.percent"
SKILL_RATES = "arithmetic.rates"
SKILL_ISOLATE = "linear_equation.isolate_variable"
SKILL_DISTRIBUTE = "linear_equation.distribute"
SKILL_BOTH_SIDES = "linear_equation.both_sides"
SKILL_LINEAR_FRACTIONS = "linear_equation.fractions"
SKILL_FACTORIZATION = "quadratic_equation.factorization"
SKILL_SQUARE_ROOT = "quadratic_equation.square_root"
SKILL_MISSING_ROOT = "quadratic_equation.missing_root"
USED_SKILLS = (
    SKILL_MULTI_STEP, SKILL_FRACTIONS, SKILL_PERCENT, SKILL_RATES,
    SKILL_ISOLATE, SKILL_DISTRIBUTE, SKILL_BOTH_SIDES, SKILL_LINEAR_FRACTIONS,
    SKILL_FACTORIZATION, SKILL_SQUARE_ROOT, SKILL_MISSING_ROOT,
)

MAX_MISCONCEPTIONS = 2


class SolutionRejected(ValueError):
    """Không viết được lời giải kiểm được cho bài; `reason` thuộc DROP_REASONS."""

    def __init__(self, reason: str, detail: str = ""):
        if reason not in DROP_REASONS:
            raise ValueError(f"lý do loại lạ: {reason!r}")
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason
        self.detail = detail


@dataclass(frozen=True)
class WrittenSolution:
    """Lời giải đã kiểm của một bài, sẵn sàng ghép vào bản ghi kho bài."""

    steps: tuple[str, ...]
    misconceptions: tuple[dict[str, Any], ...]
    solution_source: str
    skill_ids: tuple[str, ...]
    difficulty: int


# ---------------------------------------------------------------------------
# Giá trị số chính xác và định dạng
# ---------------------------------------------------------------------------


def _value(text: str) -> sympy.Rational | None:
    """Giá trị hữu tỉ chính xác của biểu thức số (qua bộ kiểm đã gia cố); None nếu không phải."""
    try:
        values = math_check.parse_answer(text)
    except math_check.MathInputError:
        return None
    if len(values.args) != 1:
        return None
    (value,) = values.args
    return value if value.is_Rational else None


def _fraction(value: sympy.Rational | Fraction) -> Fraction:
    return Fraction(int(value.p), int(value.q)) if isinstance(value, sympy.Rational) else Fraction(value)


def format_number(value: sympy.Rational | Fraction) -> str:
    """Số hữu tỉ → chuỗi: số nguyên, thập phân hữu hạn (≤ 6 chữ số lẻ) hoặc phân số `p/q`."""
    frac = _fraction(value)
    if frac.denominator == 1:
        return str(frac.numerator)
    denominator = frac.denominator
    for prime in (2, 5):
        while denominator % prime == 0:
            denominator //= prime
    if denominator == 1:
        for places in range(1, 7):
            scaled = frac * 10**places
            if scaled.denominator == 1:
                sign = "-" if scaled.numerator < 0 else ""
                digits = str(abs(scaled.numerator)).rjust(places + 1, "0")
                return f"{sign}{digits[:-places]}.{digits[-places:]}"
    return f"{frac.numerator}/{frac.denominator}"


def _answer_text(value: sympy.Rational | Fraction, answer: str | None) -> str:
    """Giá trị cuối viết theo đúng dạng của đáp án `\\boxed{}` khi trùng giá trị (36/5 thay vì 7.2)."""
    if answer is not None and "{" not in answer:
        try:
            if numina.answer_value(answer) == _fraction(value):
                return answer
        except numina.AnswerRejected:
            pass
    return format_number(value)


# ---------------------------------------------------------------------------
# Cây biểu thức số (không ẩn): nhận diện bước đồng nhất và tính dần từng lượt
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class _Num:
    """Số không âm viết sẵn (nguyên hoặc thập phân); `text` giữ nguyên cách viết."""

    value: Fraction
    text: str


@dataclass(frozen=True)
class _Neg:
    operand: _Node


@dataclass(frozen=True)
class _Op:
    op: str  # một trong + - * / ^
    left: _Node
    right: _Node


_Node = _Num | _Neg | _Op

_EXPR_TOKEN = re.compile(r"\s*(\d+(?:\.\d+)?|\.\d+|\*\*|[-+*/^()])")
_MAX_EXPR_TOKENS = 200
_MAX_EVAL_STEPS = 12
_PRECEDENCE = {"+": 1, "-": 1, "*": 2, "/": 2, "^": 4}


def _expr_tokens(text: str) -> list[str]:
    tokens: list[str] = []
    position = 0
    text = text.strip()
    while position < len(text):
        match = _EXPR_TOKEN.match(text, position)
        if match is None:
            raise ValueError(f"ký tự lạ trong biểu thức số: {text[position:position + 10]!r}")
        token = match.group(1)
        tokens.append("^" if token == "**" else token)
        position = match.end()
        if len(tokens) > _MAX_EXPR_TOKENS:
            raise ValueError("biểu thức số quá dài")
    return tokens


class _ExprParser:
    """Đệ quy xuống cho biểu thức số phẳng (`+ - * / ^`, ngoặc, nhân ngầm `2(17)(4)`).

    Chỉ dùng cho chuỗi đã qua kiểm an toàn của `math_check` hoặc do chương trình sinh;
    không bao giờ eval.
    """

    def __init__(self, text: str):
        self.tokens = _expr_tokens(text)
        self.position = 0

    def parse(self) -> _Node:
        if not self.tokens:
            raise ValueError("biểu thức rỗng")
        node = self._sum()
        if self.position != len(self.tokens):
            raise ValueError(f"thừa ký hiệu: {self.tokens[self.position:]}")
        return node

    def _peek(self) -> str | None:
        return self.tokens[self.position] if self.position < len(self.tokens) else None

    def _take(self) -> str:
        token = self._peek()
        if token is None:
            raise ValueError("biểu thức kết thúc sớm")
        self.position += 1
        return token

    def _sum(self) -> _Node:
        node = self._product()
        while self._peek() in ("+", "-"):
            op = self._take()
            node = _Op(op, node, self._product())
        return node

    def _product(self) -> _Node:
        node = self._unary()
        while True:
            token = self._peek()
            if token in ("*", "/"):
                op = self._take()
                node = _Op(op, node, self._unary())
            elif token == "(" or (token is not None and token[0].isdigit() and self.tokens[self.position - 1] == ")"):
                node = _Op("*", node, self._unary())  # nhân ngầm: 2(17)(4), (3)4
            else:
                return node

    def _unary(self) -> _Node:
        token = self._peek()
        if token == "-":
            self._take()
            return _Neg(self._unary())
        if token == "+":
            self._take()
            return self._unary()
        return self._power()

    def _power(self) -> _Node:
        base = self._atom()
        if self._peek() == "^":
            self._take()
            return _Op("^", base, self._unary())
        return base

    def _atom(self) -> _Node:
        token = self._take()
        if token == "(":
            node = self._sum()
            if self._take() != ")":
                raise ValueError("thiếu ngoặc đóng")
            return node
        if token[0].isdigit() or token[0] == ".":
            return _Num(Fraction(token), token)
        raise ValueError(f"ký hiệu không mong đợi: {token!r}")


def _is_integer_num(node: _Node) -> bool:
    return isinstance(node, _Num) and "." not in node.text


def _is_fraction_literal(node: _Node) -> bool:
    """`p/q` hai số nguyên viết sẵn: coi là một giá trị (phân số), không phải một phép chia cần tính."""
    return (isinstance(node, _Op) and node.op == "/" and _is_integer_num(node.left) and _is_integer_num(node.right)
            and node.right.value != 0)


def _unsigned(node: _Node) -> _Node:
    return node.operand if isinstance(node, _Neg) else node


def _value_of(node: _Node) -> Fraction | None:
    """Giá trị nếu nút là số viết sẵn, phân số viết sẵn hoặc số đối của chúng; None nếu còn phép tính."""
    if isinstance(node, _Num):
        return node.value
    if isinstance(node, _Neg):
        inner = _value_of(node.operand)
        return -inner if inner is not None else None
    if _is_fraction_literal(node):
        return node.left.value / node.right.value
    return None


def _evaluate(node: _Node) -> Fraction:
    if isinstance(node, _Num):
        return node.value
    if isinstance(node, _Neg):
        return -_evaluate(node.operand)
    left, right = _evaluate(node.left), _evaluate(node.right)
    if node.op == "+":
        return left + right
    if node.op == "-":
        return left - right
    if node.op == "*":
        return left * right
    if node.op == "/":
        if right == 0:
            raise ValueError("chia cho 0")
        return left / right
    if right.denominator != 1 or abs(right) > 10 or (left == 0 and right < 0):
        raise ValueError("số mũ phải là số nguyên trong [-10, 10]")
    return left ** int(right)


def _int_node(value: int) -> _Node:
    return _Num(Fraction(value), str(value)) if value >= 0 else _Neg(_Num(Fraction(-value), str(-value)))


def _value_node(value: Fraction, fractional: bool) -> _Node:
    """Giá trị → nút: số nguyên; phân số `p/q` khi phép tính có phân số hoặc không ra thập phân hữu hạn."""
    if value < 0:
        return _Neg(_value_node(-value, fractional))
    if value.denominator == 1:
        return _int_node(value.numerator)
    text = format_number(value)
    if fractional or "/" in text:
        return _Op("/", _int_node(value.numerator), _int_node(value.denominator))
    return _Num(value, text)


def _render(node: _Node, parent: str | None = None, right: bool = False) -> str:
    """Nút → chuỗi với ít ngoặc nhất mà vẫn đúng thứ tự tính; phân số viết sẵn liền `25/4`."""
    if isinstance(node, _Num):
        return node.text
    if _is_fraction_literal(node):
        text = f"{node.left.text}/{node.right.text}"
        return f"({text})" if parent == "^" or (parent == "/" and right) else text
    if isinstance(node, _Neg):
        text = "-" + _render(node.operand, "neg")
        return f"({text})" if parent is not None and (parent in ("^", "neg") or right) else text
    precedence = _PRECEDENCE[node.op]
    if node.op == "^":
        text = f"{_render(node.left, '^')}^{_render(node.right, '^', True)}"
    else:
        text = f"{_render(node.left, node.op)} {node.op} {_render(node.right, node.op, True)}"
    if parent is None:
        return text
    if parent == "neg":
        needs = precedence < _PRECEDENCE["^"]
    else:
        outer = _PRECEDENCE[parent]
        needs = precedence < outer or (precedence == outer and (parent == "^" or (right and parent in ("-", "/"))))
    return f"({text})" if needs else text


def _additive_terms(node: _Node) -> list[tuple[int, _Node]]:
    if isinstance(node, _Op) and node.op in ("+", "-"):
        terms = _additive_terms(node.left)
        terms.append((1 if node.op == "+" else -1, node.right))
        return terms
    return [(1, node)]


def _build_additive(terms: Sequence[tuple[int, _Node]]) -> _Node:
    sign, first = terms[0]
    node = first if sign > 0 else _Neg(first)
    for sign, term in terms[1:]:
        node = _Op("+" if sign > 0 else "-", node, term)
    return node


def _multiplicative_factors(node: _Node) -> list[tuple[str, _Node]]:
    if isinstance(node, _Op) and node.op in ("*", "/") and not _is_fraction_literal(node):
        factors = _multiplicative_factors(node.left)
        factors.append((node.op, node.right))
        return factors
    return [("*", node)]


def _build_multiplicative(factors: Sequence[tuple[str, _Node]]) -> _Node:
    node = factors[0][1]
    for op, factor in factors[1:]:
        node = _Op(op, node, factor)
    return node


def _square_root_of(node: _Node) -> int | None:
    """a khi nút là `a^2` viết sẵn hoặc số chính phương a² (a > 1)."""
    if (isinstance(node, _Op) and node.op == "^" and _is_integer_num(node.left) and _is_integer_num(node.right)
            and node.right.value == 2):
        return int(node.left.value)
    if _is_integer_num(node):
        root = math.isqrt(int(node.value))
        return root if root > 1 and root * root == node.value else None
    return None


def _perfect_square(node: _Node) -> _Node | None:
    """`a² ± 2·a·b + b²` (tích 2·a·b viết rõ) → `(a ± b)^2`: hằng đẳng thức bình phương của tổng/hiệu."""
    if not (isinstance(node, _Op) and node.op in ("+", "-")):
        return None
    terms = _additive_terms(node)
    if len(terms) != 3 or terms[0][0] != 1 or terms[2][0] != 1:
        return None
    first, last = _square_root_of(terms[0][1]), _square_root_of(terms[2][1])
    if first is None or last is None:
        return None
    middle = _multiplicative_factors(terms[1][1])
    if len(middle) != 3 or any(op != "*" or not _is_integer_num(factor) for op, factor in middle):
        return None
    numbers = [int(factor.value) for _, factor in middle]
    if 2 not in numbers:
        return None
    numbers.remove(2)
    if sorted(numbers) != sorted((first, last)):
        return None
    return _Op("^", _Op("+" if terms[1][0] > 0 else "-", _int_node(first), _int_node(last)), _int_node(2))


def _is_power_of(node: _Node, base: int, exponent: int) -> bool:
    if exponent == 1:
        return _is_integer_num(node) and node.value == base
    return (isinstance(node, _Op) and node.op == "^" and _is_integer_num(node.left) and node.left.value == base
            and _is_integer_num(node.right) and node.right.value == exponent)


def _factor_common(node: _Node) -> _Node | None:
    """`(a^2 ± a) / a` hoặc `/ a^2` → `a * (a ± 1) / …`: đặt thừa số chung a ở tử số."""
    if not (isinstance(node, _Op) and node.op == "/") or _is_fraction_literal(node):
        return None
    numerator, denominator = node.left, node.right
    if not (isinstance(numerator, _Op) and numerator.op in ("+", "-")):
        return None
    terms = _additive_terms(numerator)
    if len(terms) != 2 or terms[0][0] != 1:
        return None
    first, (sign, second) = terms[0][1], terms[1]
    if not (isinstance(first, _Op) and first.op == "^" and _is_integer_num(first.left)):
        return None
    base = int(first.left.value)
    if base <= 1 or not _is_power_of(first, base, 2) or not _is_power_of(second, base, 1):
        return None
    if not (_is_power_of(denominator, base, 1) or _is_power_of(denominator, base, 2)):
        return None
    factored = _Op("*", _int_node(base), _Op("+" if sign > 0 else "-", _int_node(base), _int_node(1)))
    return _Op("/", factored, denominator)


def _cancel_common(node: _Node) -> _Node | None:
    """`a * B / a` → `B`; `a * B / a^2` → `B / a` (rút gọn thừa số chung)."""
    if not (isinstance(node, _Op) and node.op == "/") or _is_fraction_literal(node):
        return None
    numerator, denominator = node.left, node.right
    if not (isinstance(numerator, _Op) and numerator.op == "*" and _is_integer_num(numerator.left)):
        return None
    base = int(numerator.left.value)
    if base <= 1:
        return None
    if _is_power_of(denominator, base, 1):
        return numerator.right
    if _is_power_of(denominator, base, 2):
        return _Op("/", numerator.right, _int_node(base))
    return None


def _reduce_sum(node: _Op) -> _Node:
    terms = _additive_terms(node)
    values = [_value_of(term) for _, term in terms]
    if any(value is None for value in values):
        return _build_additive([(sign, term if value is not None else _reduce(term))
                                for (sign, term), value in zip(terms, values)])
    has_fraction = any(_is_fraction_literal(_unsigned(term)) for _, term in terms)
    has_decimal = any(isinstance(_unsigned(term), _Num) and "." in _unsigned(term).text for _, term in terms)
    if has_fraction and not has_decimal:
        denominators = [int(_unsigned(term).right.value) if _is_fraction_literal(_unsigned(term)) else 1
                        for _, term in terms]
        if len(set(denominators)) > 1:  # quy đồng mẫu trước khi cộng/trừ
            common = math.lcm(*denominators)
            rewritten = []
            for (sign, _), value in zip(terms, values):
                signed = sign * value
                literal = _Op("/", _int_node(int(abs(signed) * common)), _int_node(common))
                rewritten.append((1 if signed >= 0 else -1, literal))
            return _build_additive(rewritten)
    total = sum((sign * value for (sign, _), value in zip(terms, values)), Fraction(0))
    return _value_node(total, has_fraction)


def _reduce_product(node: _Op) -> _Node:
    factors = _multiplicative_factors(node)
    values = [_value_of(factor) for _, factor in factors]
    if any(value is None for value in values):
        return _build_multiplicative([(op, factor if value is not None else _reduce(factor))
                                      for (op, factor), value in zip(factors, values)])
    if any(op == "/" and _is_fraction_literal(_unsigned(factor)) for op, factor in factors):
        rewritten: list[tuple[str, _Node]] = []
        for op, factor in factors:  # chia cho phân số = nhân với phân số nghịch đảo
            literal = _unsigned(factor)
            if op == "/" and _is_fraction_literal(literal):
                reciprocal = literal.right if literal.left.value == 1 else _Op("/", literal.right, literal.left)
                rewritten.append(("*", _Neg(reciprocal) if isinstance(factor, _Neg) else reciprocal))
            else:
                rewritten.append((op, factor))
        return _build_multiplicative(rewritten)
    result = values[0]
    for (op, _), value in zip(factors[1:], values[1:]):
        if op == "*":
            result *= value
        elif value == 0:
            raise ValueError("chia cho 0")
        else:
            result /= value
    return _value_node(result, any(_is_fraction_literal(_unsigned(factor)) for _, factor in factors))


def _reduce_power(node: _Op) -> _Node:
    base, exponent = _value_of(node.left), _value_of(node.right)
    if base is None:
        return _Op("^", _reduce(node.left), node.right)
    if exponent is None:
        return _Op("^", node.left, _reduce(node.right))
    if exponent.denominator != 1 or abs(exponent) > 10 or (base == 0 and exponent < 0):
        raise ValueError("số mũ phải là số nguyên trong [-10, 10]")
    return _value_node(base ** int(exponent), _is_fraction_literal(_unsigned(node.left)))


def _reduce(node: _Node) -> _Node:
    """Một lượt rút gọn: mẫu hằng đẳng thức nếu khớp, ngược lại tính mọi phép tính trong cùng (song song)."""
    for rule in (_perfect_square, _factor_common, _cancel_common):
        rewritten = rule(node)
        if rewritten is not None:
            return rewritten
    if isinstance(node, _Neg):
        return _Neg(_reduce(node.operand))
    if not isinstance(node, _Op):
        raise ValueError("không rút gọn được số viết sẵn")
    if node.op in ("+", "-"):
        return _reduce_sum(node)
    if node.op in ("*", "/"):
        return _reduce_product(node)
    return _reduce_power(node)


def evaluation_steps(expression: str) -> tuple[list[str], Fraction]:
    """Các dạng trung gian khi tính dần một biểu thức số, mỗi lượt một bước, và giá trị cuối.

    Ví dụ `45/(7 - 3/4)` → `45 / (28/4 - 3/4)`, `45 / (25/4)`, `45 * 4/25`, `36/5`;
    `289 + 2(17)(4) + 16` → `(17 + 4)^2`, `21^2`, `441`. Biểu thức đã là một số → danh sách rỗng.
    Ném ValueError khi không phân tích được (có chữ, chia cho 0, quá nhiều bước).
    """
    node = _ExprParser(expression).parse()
    value = _evaluate(node)
    texts: list[str] = []
    while _value_of(node) is None:
        node = _reduce(node)
        texts.append(_render(node))
        if len(texts) > _MAX_EVAL_STEPS:
            raise ValueError(f"tính {expression!r} cần quá {_MAX_EVAL_STEPS} bước")
    if _evaluate(node) != value:
        raise ValueError(f"rút gọn {expression!r} làm đổi giá trị")
    return texts, value


def _strip_identities(node: _Node) -> _Node:
    if isinstance(node, _Neg):
        return _Neg(_strip_identities(node.operand))
    if not isinstance(node, _Op):
        return node
    left, right = _strip_identities(node.left), _strip_identities(node.right)
    left_value, right_value = _value_of(left), _value_of(right)
    if node.op in ("+", "-") and right_value == 0:
        return left
    if node.op == "+" and left_value == 0:
        return right
    if node.op in ("*", "/") and right_value == 1:
        return left
    if node.op == "*" and left_value == 1:
        return right
    if node.op == "^" and (right_value == 1 or left_value == 1):
        return left
    return _Op(node.op, left, right)


def without_identity_operations(step: str) -> str:
    """Bỏ phép đồng nhất nằm bên trong một bước có tính toán thật: `45 * (1000 / 1) * (1 / 3600) = 12.5`
    → `45 * 1000 * 1/3600 = 12.5`. Bước không có phép đồng nhất (hoặc không phân tích được) giữ nguyên.
    """
    lhs, _, rhs = step.rpartition("=")
    try:
        node = _ExprParser(lhs).parse()
    except ValueError:
        return step
    stripped = _strip_identities(node)
    if stripped == node or isinstance(_unsigned(stripped), _Num):
        return step
    return f"{_render(stripped)} = {rhs.strip()}"


def is_identity_step(step: str) -> bool:
    """Bước không tính thêm gì: `a + 0`, `0 + a`, `a - 0`, `a * 1`, `1 * a`, `a / 1`, `a ^ 1`, `1 ^ n`, `a = a`.

    Bỏ các phép đồng nhất ở vế trái; nếu chỉ còn một số viết sẵn thì bước là tầm thường.
    Phép chia hai số (`12 / 4 = 3`) không bao giờ là tầm thường.
    """
    lhs = step.rpartition("=")[0]
    try:
        node = _ExprParser(lhs).parse()
    except ValueError:
        return False
    return isinstance(_unsigned(_strip_identities(node)), _Num)


# ---------------------------------------------------------------------------
# Tách phép tính số học từ lời giải CoT
# ---------------------------------------------------------------------------

_APPROX_MARK = "\u2248"
_TEXT_WRAPPER = re.compile(
    r"\\(?:text|textbf|textit|textrm|mbox|mathrm|mathbf|mathit|operatorname)\s*\{[^{}]*\}"
)
_BOX_COMMAND = re.compile(r"\\(?:boxed|fbox)\s*(?=\{)")
_APPROX = re.compile(r"\\approx|\\simeq|\\sim(?![A-Za-z])|≈|~=|\bapprox(?:imately)?\b|\babout\b", re.IGNORECASE)
_CLAUSE_BREAK = re.compile(
    r"\\(?:implies|Rightarrow|rightarrow|Longrightarrow|to|quad|qquad)(?![A-Za-z])|⇒|→|\\\\|\n"
)
_LATEX_SPACING = re.compile(r"\\[,;:!> ]")
_LATEX_LEFT_RIGHT = re.compile(r"\\(?:left|right|big|Big|bigg|Bigg)(?![A-Za-z])")
_LATEX_TIMES = re.compile(r"\\(?:times|cdot|ast)(?![A-Za-z])")
_LATEX_DIV = re.compile(r"\\div(?![A-Za-z])")
_LATEX_FRAC = re.compile(r"\\[dt]?frac(?![A-Za-z])")
_LATEX_COMMAND = re.compile(r"\\[A-Za-z]+")
_LATEX_THOUSANDS = re.compile(r"(?<=\d)\{,\}(?=\d{3})")
_PLAIN_THOUSANDS = re.compile(r"(?<![\d.,])\d{1,3}(?:,\d{3})+(?![\d])")
_PERCENT = re.compile(r"(\d+(?:\.\d+)?)\s*%")
# Phép cộng/trừ toàn phần trăm "100% - 40% - 20% = 40%": tính theo đơn vị phần trăm (100 - 40 - 20 = 40).
_PERCENT_EQUATION = re.compile(
    r"\d+(?:\.\d+)?\s*%(?:\s*[-+]\s*\d+(?:\.\d+)?\s*%)+\s*=\s*\d+(?:\.\d+)?\s*%"
)
_LETTER_TIMES = re.compile(r"(?<=\d)\s+[x×]\s+(?=[\d(])")
_COMPOUND_UNIT = re.compile(r"[A-Za-z]+(?:\^?[23])?\s*/\s*[A-Za-z]+(?:\^?[23])?")
_SYMBOLIC = re.compile(r"(?:\d|\))[a-z](?![a-z])|(?<![A-Za-z])[a-z]\s*\^|(?<![A-Za-z])[a-z]\s*\(")
_WORD = re.compile(r"[A-Za-z_][A-Za-z_0-9']*")
_CLAUSE_SPLIT = re.compile(r",\s|;|:\s|:$|\.\s|\.$|\?|!|\band\b|\bso\b|\bthen\b|\bthus\b|\bhence\b|\bwhich\b")
_ALLOWED_SEGMENT = re.compile(r"^[0-9+\-*/^().\s]+$")
_NUMBER_LITERAL = re.compile(r"^-?\d+(?:\.\d+)?$")
_FRACTION_LITERAL = re.compile(r"^-?(\d+)\s*/\s*(\d+)$")
_BULLET = re.compile(r"^\s*(?:[-*•]|\d{1,2}[.)])\s+")
_LEADING_DOT = re.compile(r"(?<![\d.])\.(\d)")
# Bỏ ngoặc quanh số dương trần "(5)"; giữ "(-1)" vì "-1 ^ 2" khác "(-1) ^ 2".
_PAREN_NUMBER = re.compile(r"(?<![\d)])\((\d+(?:\.\d+)?)\)(?![\d(])")
_NEGATIVE_GAP = re.compile(r"\(\s*-\s+(?=[\d(])")
_IMPLICIT_PAREN = re.compile(r"(?<=[\d)])\s*(?=\()")
_IMPLICIT_AFTER_PAREN = re.compile(r"(?<=\))\s*(?=[\d.])")
_BINARY_OPERATOR = re.compile(r"(?<=[\d)])\s*([+\-*/^])\s*(?=[\d(.\-])")
_ROUNDING_WORDS = re.compile(r"\bround(?:ed|ing)?\b|\bnearest\b|\bapprox|\\approx|≈", re.IGNORECASE)


def _balanced_end(text: str, open_index: int) -> int | None:
    depth = 0
    for index in range(open_index, len(text)):
        if text[index] == "{":
            depth += 1
        elif text[index] == "}":
            depth -= 1
            if depth == 0:
                return index
    return None


def _unwrap_boxes(text: str) -> str:
    """`\\boxed{X}` → `X` (ngoặc cân bằng); hộp hỏng thì bỏ nguyên lệnh."""
    while True:
        match = _BOX_COMMAND.search(text)
        if match is None:
            return text
        open_index = match.end()
        close = _balanced_end(text, open_index)
        if close is None:
            return text[: match.start()] + text[match.end():]
        text = text[: match.start()] + text[open_index + 1 : close] + text[close + 1 :]


def _replace_fracs(text: str) -> str:
    """`\\frac{a}{b}` → `((a)/(b))`, kể cả dạng tắt `\\frac12`; ngoặc lệch thì để '@' làm hỏng đoạn đó."""
    text = re.sub(r"\\frac\s*(\d)\s*(\d)", r"\\frac{\1}{\2}", text)
    while True:
        index = text.find("\\frac")
        if index < 0:
            return text
        open_a = index + len("\\frac")
        while open_a < len(text) and text[open_a] == " ":
            open_a += 1
        close_a = _balanced_end(text, open_a) if open_a < len(text) and text[open_a] == "{" else None
        open_b = close_a + 1 if close_a is not None else None
        while open_b is not None and open_b < len(text) and text[open_b] == " ":
            open_b += 1
        close_b = _balanced_end(text, open_b) if open_b is not None and open_b < len(text) and text[open_b] == "{" else None
        if close_a is None or open_b is None or close_b is None:
            text = text[:index] + "@" + text[index + len("\\frac"):]
            continue
        numerator = text[open_a + 1 : close_a]
        denominator = text[open_b + 1 : close_b]
        text = f"{text[:index]}(({numerator})/({denominator})){text[close_b + 1:]}"


def normalize_cot_line(line: str) -> str:
    """Một dòng lời giải CoT → văn bản phẳng: LaTeX → `* / ( )`, bỏ đơn vị bọc \\text, `%` → `/100`.

    Đẳng thức chỉ gồm các phần trăm cộng trừ nhau (`100% - 40% - 20% = 40%`) được giữ theo đơn
    vị phần trăm (`100 - 40 - 20 = 40`). Lệnh LaTeX không hiểu được thay bằng '@' để đoạn chứa
    nó không bao giờ thành bước.
    """
    text = _BULLET.sub(" ", line)
    text = text.replace("\u2212", "-").replace("×", "*").replace("÷", "/").replace("·", "*")
    text = text.replace("\\$", " ").replace("$", " ").replace("\\%", "%")
    text = _PERCENT_EQUATION.sub(lambda m: re.sub(r"\s*%", "", m.group(0)), text)
    text = text.replace("\\[", " ").replace("\\]", " ").replace("\\(", " ").replace("\\)", " ")
    text = _unwrap_boxes(text)
    text = _TEXT_WRAPPER.sub(" ", text)
    text = _APPROX.sub(f" {_APPROX_MARK} ", text)
    text = _LATEX_SPACING.sub(" ", text)
    text = _LATEX_LEFT_RIGHT.sub("", text)
    text = _LATEX_TIMES.sub("*", text)
    text = _LATEX_DIV.sub("/", text)
    text = _LATEX_FRAC.sub(r"\\frac", text)
    text = _LATEX_THOUSANDS.sub("", text)
    text = _replace_fracs(text)
    text = re.sub(r"\^\s*\{\s*(-?\d+)\s*\}", r"^\1", text)
    text = _LATEX_COMMAND.sub("@", text)
    text = text.replace("{", "(").replace("}", ")").replace("&", " ")
    text = _PLAIN_THOUSANDS.sub(lambda m: m.group(0).replace(",", ""), text)
    text = _PERCENT.sub(r"(\1/100)", text)
    text = _LETTER_TIMES.sub(" * ", text)
    text = text.replace("**", "^")
    return _LEADING_DOT.sub(r"0.\1", text)


def _clean_segment(raw: str) -> str | None:
    """Một vế giữa hai dấu '=' → biểu thức số phẳng hoặc None (có ẩn, ký tự lạ, rỗng)."""
    text = _COMPOUND_UNIT.sub(" ", raw)
    if _SYMBOLIC.search(text) or "@" in text:
        return None
    text = _WORD.sub(" ", text)
    text = text.replace("'", " ").replace('"', " ")
    text = re.sub(r"\s+", " ", text).strip().strip(".,;:")
    text = re.sub(r"\(\s+", "(", re.sub(r"\s+\)", ")", text)).strip()
    # "( )" còn lại sau khi bỏ chữ nghĩa là đoạn này có đại lượng chữ, không phải phép tính số.
    if not text or "()" in text or not _ALLOWED_SEGMENT.match(text) or not re.search(r"\d", text):
        return None
    previous = None
    while previous != text:
        previous = text
        text = _PAREN_NUMBER.sub(r"\1", text)
    text = _IMPLICIT_PAREN.sub(" * ", text)
    text = _IMPLICIT_AFTER_PAREN.sub(" * ", text)
    text = _BINARY_OPERATOR.sub(lambda m: f" {m.group(1)} ", text)
    text = _NEGATIVE_GAP.sub("(-", re.sub(r"\s+", " ", text)).strip()
    if not _balanced_parens(text):
        return None
    text = _drop_double_parens(text)
    while text.startswith("(") and _outer_pair(text):
        text = text[1:-1].strip()
    return text or None


def _balanced_parens(text: str) -> bool:
    depth = 0
    for char in text:
        depth += {"(": 1, ")": -1}.get(char, 0)
        if depth < 0:
            return False
    return depth == 0


def _drop_double_parens(text: str) -> str:
    """`((a / b))` → `(a / b)`: bỏ cặp ngoặc chỉ bọc đúng một cặp ngoặc khác (chuỗi đã cân ngoặc)."""
    while True:
        stack: list[int] = []
        pairs: dict[int, int] = {}
        for index, char in enumerate(text):
            if char == "(":
                stack.append(index)
            elif char == ")":
                pairs[stack.pop()] = index
        redundant = next((i for i, j in pairs.items() if pairs.get(i + 1) == j - 1), None)
        if redundant is None:
            return text
        close = pairs[redundant]
        text = text[:redundant] + text[redundant + 1 : close] + text[close + 1 :]


def _outer_pair(text: str) -> bool:
    """Ngoặc mở đầu chuỗi đóng đúng ở ký tự cuối (ngoặc bao toàn bộ)."""
    if not text.endswith(")"):
        return False
    depth = 0
    for index, char in enumerate(text):
        depth += {"(": 1, ")": -1}.get(char, 0)
        if depth == 0:
            return index == len(text) - 1
    return False


def _is_result(text: str) -> bool:
    """Kết quả của bước: số nguyên/thập phân hoặc phân số tối giản `p/q` (q > 1)."""
    if _NUMBER_LITERAL.match(text):
        return True
    match = _FRACTION_LITERAL.match(text)
    return bool(match) and int(match.group(2)) > 1 and math.gcd(int(match.group(1)), int(match.group(2))) == 1


def _has_operation(text: str) -> bool:
    """Có phép toán hai ngôi (kể cả `420 / 4`), không phải một số trần."""
    return bool(_BINARY_OPERATOR.search(text)) and not _NUMBER_LITERAL.match(text)


def _label(raw: str) -> str | None:
    """Nhãn chữ ở vế trái của dòng (`Relative speed = ...`): chữ thường, gộp khoảng trắng."""
    stripped = _WORD.sub(" ", _COMPOUND_UNIT.sub(" ", raw))
    if re.search(r"\d", stripped):
        return None
    words = " ".join(word.lower() for word in _WORD.findall(raw))
    return words or None


@dataclass
class _Chain:
    label: str | None
    segments: list[str | None]


def _clauses(solution: str) -> list[str]:
    clauses: list[str] = []
    for line in _CLAUSE_BREAK.split(solution):
        normalized = normalize_cot_line(line)
        clauses.extend(part for part in _CLAUSE_SPLIT.split(normalized) if part and part.strip())
    return clauses


def _chains(solution: str) -> tuple[list[_Chain], bool]:
    """Các chuỗi đẳng thức `e1 = e2 = ...`; dòng cùng nhãn liên tiếp được nối thành một chuỗi.

    Trả thêm cờ "có xấp xỉ" (≈, approximately, …) để phân loại lý do loại.
    """
    chains: list[_Chain] = []
    saw_approx = False
    for clause in _clauses(solution):
        if "=" not in clause and _APPROX_MARK not in clause:
            continue
        pieces = re.split(rf"=|{_APPROX_MARK}", clause)
        marks = re.findall(rf"=|{_APPROX_MARK}", clause)
        label = _label(pieces[0])
        segments: list[str | None] = []
        if label is None:
            segments.append(_clean_segment(pieces[0]))
        for mark, piece in zip(marks, pieces[1:]):
            if mark == _APPROX_MARK:
                saw_approx = True
                segments.append(None)  # xấp xỉ cắt chuỗi: hai vế không bằng nhau chính xác
            segments.append(_clean_segment(piece))
        if label is not None and chains and chains[-1].label == label:
            chains[-1].segments.extend(segments)
        else:
            chains.append(_Chain(label, segments))
    return chains, saw_approx


def _steps_from_chain(segments: Sequence[str | None]) -> tuple[list[str], bool]:
    """Bước `biểu thức = số` từ mỗi đoạn liên tiếp có cùng giá trị chính xác.

    Trả thêm cờ "gần bằng nhưng không bằng" (dấu hiệu làm tròn).
    """
    steps: list[str] = []
    near_miss = False
    run: list[tuple[str, sympy.Rational]] = []

    def flush() -> None:
        lhs = next((text for text, _ in run if _has_operation(text)), None)
        if lhs is None:
            return
        after = [text for text, _ in run[run.index(next(item for item in run if item[0] == lhs)) + 1 :]]
        rhs = next((text for text in reversed(after) if _is_result(text)), None)
        if rhs is not None and rhs != lhs:
            steps.append(f"{lhs} = {rhs.replace(' ', '')}")

    for segment in segments:
        value = _value(segment) if segment is not None else None
        if value is None:
            flush()
            run = []
            continue
        if run and run[-1][1] != value:
            previous = run[-1][1]
            if previous != 0 and abs(float((value - previous) / previous)) < 0.02:
                near_miss = True
            flush()
            run = []
        run.append((segment, value))
    flush()
    return steps, near_miss


# Lệnh LaTeX chỉ là phép toán/định dạng (không phải chữ của văn bản) khi xét "công thức số thuần".
_LATEX_OPERATOR_COMMANDS = re.compile(
    r"\\(?:times|cdot|div|[dt]?frac|left|right|boxed|fbox|quad|qquad|[,;:!\[\]()])(?![A-Za-z])|\$"
)
_MIXED_NUMBER_LATEX = re.compile(r"\d\s*\\[dt]?frac")


def _near(first: sympy.Rational, second: sympy.Rational) -> bool:
    return first != 0 and abs(float((second - first) / first)) < 0.02


def cot_false_equalities(solution: str) -> list[str]:
    """Các đẳng thức SAI mà CoT viết ra: hai vế kề nhau đều là phép tính số, giá trị chính xác khác nhau.

    Chỉ xét dòng mà phần sau dấu '=' đầu tiên là công thức số thuần: không chữ cái (kể cả chữ trong
    \\text{} — vế có ẩn/đơn vị bị bỏ chữ thì mất nghĩa, "2B - 16" thành "2 - 16"), không '%' (phần trăm
    vừa là số vừa là đơn vị: "0.604 × 100% = 60.4%"), không hỗn số ("1\\frac{1}{2}"). Hai vế phải
    cùng có phép toán (vế số trần như "1 = 3.8" là đổi đơn vị) và lệch ≥ 2% (lệch nhỏ hơn là làm
    tròn, xử lý riêng bằng `rounding_dependent`). Hai dòng liền nhau cùng nhãn ("t = 72 * 5 / 60"
    rồi "t = 72 / 12 * 5") được coi như một chuỗi bằng nhau.
    """
    found: list[str] = []
    previous_label: str | None = None
    previous_segment: str | None = None
    for line in _CLAUSE_BREAK.split(solution):
        if "=" not in line:
            if line.strip():
                previous_label = None  # dòng văn xuôi xen giữa: hai công thức không còn là một chuỗi
            continue
        formula = _LATEX_OPERATOR_COMMANDS.sub(" ", line)
        if "%" in line or _MIXED_NUMBER_LATEX.search(line) or re.search(r"[A-Za-z]", formula.partition("=")[2]):
            previous_label = None
            continue
        pieces = re.split(rf"=|{_APPROX_MARK}", normalize_cot_line(line))
        marks = re.findall(rf"=|{_APPROX_MARK}", normalize_cot_line(line))
        label = _label(pieces[0])
        head_has_letters = bool(re.search(r"[A-Za-z]", formula.partition("=")[0]))
        # Vế đầu lẫn chữ và số ("15C - 11C", "\frac{1}{2}P") là biểu thức có ẩn: không so.
        segments: list[str | None] = (
            [] if label is not None else [None if head_has_letters else _clean_segment(pieces[0])])
        if label is not None and label == previous_label:
            segments.append(previous_segment)
        for mark, piece in zip(marks, pieces[1:]):
            if mark == _APPROX_MARK:
                segments.append(None)
            segments.append(_clean_segment(piece))
        for first, second in zip(segments, segments[1:]):
            if first is None or second is None or not (_has_operation(first) and _has_operation(second)):
                continue
            first_value, second_value = _value(first), _value(second)
            if (first_value is not None and second_value is not None and first_value != second_value
                    and not _near(first_value, second_value)):
                found.append(f"{first} = {second}")
        previous_label, previous_segment = label, segments[-1] if segments else None
    return found


def cot_final_value(solution: str) -> Fraction | None:
    """Giá trị số cuối cùng mà CoT viết ra trong một đẳng thức trước `\\boxed` cuối cùng.

    Là vế số trần cuối cùng của chuỗi đẳng thức cuối cùng có vế số trần (`Selling Price = Rs 60`
    → 60; `4 * 750 = \\boxed{3000}` → 3000, vì hộp nằm ngay trong đẳng thức). None khi không có,
    hoặc khi lời giải dùng xấp xỉ/làm tròn (không kết luận được).
    """
    index = max(solution.rfind("\\boxed"), solution.rfind("\\fbox"))
    before = solution
    if index >= 0:
        open_index = solution.find("{", index)
        close = _balanced_end(solution, open_index) if open_index >= 0 else None
        before = solution[: close + 1] if close is not None else solution[:index]
    chains, saw_approx = _chains(before)
    if saw_approx or _ROUNDING_WORDS.search(before):
        return None
    for chain in reversed(chains):
        for segment in reversed(chain.segments):
            if segment is not None and _is_result(segment):
                value = _value(segment)
                return _fraction(value) if value is not None else None
    return None


def extract_arith_steps(solution: str, answer: str) -> tuple[str, ...]:
    """Chuỗi bước `a op b = c` từ lời giải CoT, mỗi bước đúng chính xác, bước cuối ra `answer`.

    Bước đồng nhất (`0 + a = a`, `1 ^ n = 1`, …) bị bỏ trước khi cắt chuỗi. Ném
    SolutionRejected: `rounding_dependent` khi lời giải dùng xấp xỉ/làm tròn mà không bước
    chính xác nào ra đáp án; `answer_not_reached` / `no_verifiable_steps` khi khác.
    """
    target = _value(answer)
    if target is None:
        raise SolutionRejected("no_verifiable_steps", f"đáp án không phải số: {answer!r}")
    chains, saw_approx = _chains(solution)
    steps: list[str] = []
    near_miss = False
    for chain in chains:
        found, near = _steps_from_chain(chain.segments)
        near_miss = near_miss or near
        for step in found:
            step = without_identity_operations(step)
            try:
                ok = math_check.arith_step(step)
            except math_check.MathInputError:
                ok = False
            if ok and step not in steps and not is_identity_step(step):
                steps.append(step)
    last = None
    for index, step in enumerate(steps):
        if _value(step.rpartition("=")[2]) == target:
            last = index
    if last is None:
        rounding = saw_approx or near_miss or bool(_ROUNDING_WORDS.search(solution))
        if rounding:
            raise SolutionRejected("rounding_dependent", f"đáp án {answer} chỉ đạt khi làm tròn/xấp xỉ")
        if steps:
            raise SolutionRejected("answer_not_reached", f"{len(steps)} bước đúng nhưng không bước nào ra {answer}")
        raise SolutionRejected("no_verifiable_steps")
    return tuple(steps[: last + 1])


# ---------------------------------------------------------------------------
# Kiểm chuỗi bước số học so với đề: số có nguồn gốc, số liệu được dùng, kết quả được dùng
# ---------------------------------------------------------------------------

_NUMBER_TOKEN = re.compile(r"\d+(?:\.\d+)?")
_NUMBER_WORDS = {
    "one": 1, "two": 2, "three": 3, "four": 4, "five": 5, "six": 6, "seven": 7, "eight": 8, "nine": 9,
    "ten": 10, "eleven": 11, "twelve": 12, "thirteen": 13, "fourteen": 14, "fifteen": 15, "sixteen": 16,
    "seventeen": 17, "eighteen": 18, "nineteen": 19, "twenty": 20, "thirty": 30, "forty": 40, "fifty": 50,
    "sixty": 60, "seventy": 70, "eighty": 80, "ninety": 90, "hundred": 100, "thousand": 1000,
    "dozen": 12, "twice": 2, "double": 2, "triple": 3, "half": 2, "halves": 2, "third": 3, "thirds": 3,
    "quarter": 4, "quarters": 4, "fourth": 4, "fifth": 5, "percent": 100, "score": 20,
}
# Hằng số đổi đơn vị chỉ được dùng mà không có trong đề khi đề nhắc tới ngữ cảnh tương ứng
# (giờ/phút → 60, tuần → 7, phần trăm → 100, ...). Chỉ 0 và 1 (phần bù 1 - p) luôn được dùng; 2 cần
# ngữ cảnh (nửa, gấp đôi, cặp, trung bình, chu vi, bình phương, bắt tay đôi một) để "14 / 2" vô cớ bị loại.
_ALWAYS_KNOWN = frozenset({Fraction(0), Fraction(1)})
_CONTEXT_CONSTANTS: tuple[tuple[re.Pattern[str], tuple[int, ...]], ...] = tuple(
    (re.compile(pattern, re.IGNORECASE), values)
    for pattern, values in (
        (r"\btwo\b|\btwice\b|\bhalf\b|\bhalves\b|\bdouble|\bpairs?\b|\bboth\b|\baverage\b|\bmean\b|\bmidpoint\b|"
         r"\bperimeter\b|\brectangle|\bsquare|\bhandshakes?\b|\beach other\b|\beven\b|\^\s*\{?2|²", (2,)),
        (r"%|\bpercent", (100,)),
        (r"\bminutes?\b|\bhours?\b|\bseconds?\b|km/h|kmph|\bmph\b|m/s|\bspeed\b", (60, 3600)),
        (r"\bdays?\b|\bdaily\b|\bhours?\b", (24,)),
        (r"\bweeks?\b|\bweekly\b", (7, 52)),
        (r"\bmonths?\b|\bmonthly\b|\bdozens?\b|\bfeet\b|\bfoot\b|\binch(?:es)?\b", (12,)),
        (r"\byears?\b|\bannual", (12, 52, 365)),
        (r"\bmonths?\b", (30,)),
        (r"\bkm\b|kilo|\bmeters?\b|\bmetres?\b|\blit(?:er|re)s?\b|\bml\b|milli|\bgrams?\b|\bkg\b|thousand", (100, 1000)),
        # "$" trần không phải ngữ cảnh tiền: đề LaTeX dùng "$...$" cho công thức.
        (r"\bcm\b|centi|\bcents?\b|\bdollars?\b", (100,)),
        (r"\bdegrees?\b|\bcircle\b|\bangles?\b|\btriangle", (90, 180, 360)),
        (r"\bquarters?\b", (4, 25)),
        (r"\bdimes?\b", (10,)),
        (r"\bnickels?\b", (5,)),
        (r"\btriangle|\btriple|\bthird", (3,)),
        (r"\bsquare|\brectangle|\bquarter", (4,)),
    )
)
_PERCENT_CUE_TEXT = re.compile(r"%|\bpercent", re.IGNORECASE)


def _context_constants(statement: str) -> set[Fraction]:
    found = set(_ALWAYS_KNOWN)
    for pattern, values in _CONTEXT_CONSTANTS:
        if pattern.search(statement):
            found.update(Fraction(value) for value in values)
    return found


def _numbers(text: str) -> set[Fraction]:
    return {Fraction(token) for token in _NUMBER_TOKEN.findall(text)}


_STATEMENT_FRACTION = re.compile(r"(\d+(?:\.\d+)?)\)*\s*/\s*\(*(\d+(?:\.\d+)?)")


def statement_numbers(statement: str) -> set[Fraction]:
    """Các số xuất hiện trong đề (kể cả số viết bằng chữ, phân số LaTeX, phần trăm)."""
    normalized = normalize_cot_line(statement)
    found = _numbers(normalized)
    for numerator, denominator in _STATEMENT_FRACTION.findall(normalized):
        if Fraction(denominator) != 0:
            found.add(Fraction(numerator) / Fraction(denominator))
    for word in re.findall(r"[a-z]+", statement.lower()):
        if word in _NUMBER_WORDS:
            found.add(Fraction(_NUMBER_WORDS[word]))
    return found


def ungrounded_numbers(statement: str, steps: Sequence[str]) -> list[str]:
    """Số ở vế trái của bước không có trong đề, không là hằng đổi đơn vị hợp ngữ cảnh, không do bước trước tính ra.

    Bước như vậy nghĩa là lời giải CoT có phép tính trung gian mà bộ tách không lấy được,
    chuỗi bước sẽ thiếu mắt xích với người đọc. Với đề có phần trăm, `p/100` của số trong đề
    hoặc của kết quả trước cũng được coi là đã biết (40% → 0.4).
    """
    given = statement_numbers(statement)
    known = given | _context_constants(statement)
    percent = bool(_PERCENT_CUE_TEXT.search(statement))
    if percent:
        known |= {value / 100 for value in given}
    missing: list[str] = []
    for step in steps:
        lhs, _, rhs = step.rpartition("=")
        for value in _numbers(lhs):
            if value not in known:
                missing.append(f"{step}: {value}")
        known |= _numbers(lhs) | _numbers(rhs)
        result = _value(rhs)
        if result is not None:
            known.add(_fraction(result))
            if percent:
                known.add(_fraction(result) / 100)
    return missing


# Số liệu của đề: phần trăm (đã chuẩn hóa thành "p/100"), phân số a/b, hoặc số trần.
_GIVEN_TOKEN = re.compile(
    r"(?P<pct>\d+(?:\.\d+)?)\)*\s*/\s*\(*100(?![\d.])"
    r"|(?P<num>\d+(?:\.\d+)?)\)*\s*/\s*\(*(?P<den>\d+(?:\.\d+)?)"
    r"|(?P<plain>\d+(?:\.\d+)?)"
)
# Từ chỉ bội/phần là số liệu thật của đề ("twice as old" phải xuất hiện như ×2 hoặc ÷2 trong chuỗi).
# Số đếm viết bằng chữ ("four dogs") và số thứ tự ("fifth bounce") không tính.
_MULTIPLIER_WORDS: dict[str, frozenset[Fraction]] = {
    "twice": frozenset({Fraction(2), Fraction(1, 2)}),
    "double": frozenset({Fraction(2), Fraction(1, 2)}),
    "doubled": frozenset({Fraction(2), Fraction(1, 2)}),
    "half": frozenset({Fraction(2), Fraction(1, 2)}),
    "triple": frozenset({Fraction(3), Fraction(1, 3)}),
    "tripled": frozenset({Fraction(3), Fraction(1, 3)}),
    "thrice": frozenset({Fraction(3), Fraction(1, 3)}),
}
_OCCURRENCE_FRACTION = re.compile(r"(?<![\d.])(\d+)\s*/\s*(\d+)(?![\d.])")


def given_quantities(statement: str) -> list[tuple[str, frozenset[Fraction]]]:
    """Số liệu của đề, mỗi số liệu kèm các dạng được coi là "đã dùng" khi xuất hiện trong chuỗi bước.

    - phần trăm `p%`: p hoặc p/100 (40% dùng như 40 hay 0.4);
    - phân số `a/b`: giá trị a/b, và b khi a = 1 (1/4 dùng như "chia 4");
    - số trần: chính nó;
    - từ chỉ bội/phần (`twice`, `double`, `half`, `triple`, `thrice`): ×k hoặc ÷k.
    Số đếm/số thứ tự viết bằng chữ ("ten", "fifth") và số liệu bằng 0 hoặc 1 ("1 lawn", "1 every
    third month": nhân với 1 là bước đồng nhất, đã bị bỏ) không tính là số liệu bắt buộc. Giá
    trị trùng nhau gộp một.
    """
    normalized = normalize_cot_line(statement)
    found: dict[frozenset[Fraction], str] = {}
    for match in _GIVEN_TOKEN.finditer(normalized):
        if match.group("pct") is not None:
            value = Fraction(match.group("pct"))
            forms, label = frozenset({value, value / 100}), f"{match.group('pct')}%"
        elif match.group("num") is not None:
            numerator, denominator = Fraction(match.group("num")), Fraction(match.group("den"))
            if denominator == 0:
                continue
            value = numerator / denominator
            forms = frozenset({value, denominator} if numerator == 1 else {value})
            label = f"{match.group('num')}/{match.group('den')}"
        else:
            value = Fraction(match.group("plain"))
            forms, label = frozenset({value}), match.group("plain")
        if forms <= {Fraction(0), Fraction(1)}:
            continue
        found.setdefault(forms, label)
    for word in re.findall(r"[a-z]+", statement.lower()):
        if word in _MULTIPLIER_WORDS:
            found.setdefault(_MULTIPLIER_WORDS[word], word)
    return [(label, forms) for forms, label in found.items()]


def _occurrences(steps: Sequence[str]) -> list[tuple[int, Fraction]]:
    """(chỉ số bước, giá trị) của mỗi số ở vế trái, kể cả cặp số nguyên `a / b` không chia hết.

    Cặp `a / b` chỉ được hiểu như một phân số (1 / 3, 40 / 100) khi kết quả không nguyên; `12 / 4`
    hay `2 / 0.5` là phép chia, giá trị của nó không được tính là "đã dùng số 3 / số 4 của đề".
    """
    out: list[tuple[int, Fraction]] = []
    for index, step in enumerate(steps):
        lhs = step.rpartition("=")[0]
        out.extend((index, Fraction(token)) for token in _NUMBER_TOKEN.findall(lhs))
        for numerator, denominator in _OCCURRENCE_FRACTION.findall(lhs):
            if int(denominator) != 0 and int(numerator) % int(denominator) != 0:
                out.append((index, Fraction(int(numerator), int(denominator))))
    return out


def _unmatched(options: Sequence[Sequence[int]], n_slots: int) -> list[int]:
    """Ghép cặp cực đại (Kuhn): mỗi vai cần một lần xuất hiện riêng; trả các vai không ghép được."""
    owner = [-1] * n_slots

    def assign(role: int, seen: set[int]) -> bool:
        for slot in options[role]:
            if slot in seen:
                continue
            seen.add(slot)
            if owner[slot] == -1 or assign(owner[slot], seen):
                owner[slot] = role
                return True
        return False

    return [role for role in range(len(options)) if not assign(role, set())]


@dataclass(frozen=True)
class ChainCoverage:
    """Kết quả ghép số liệu của đề và kết quả trung gian với các lần xuất hiện trong chuỗi bước."""

    unused_givens: tuple[str, ...]   # số liệu của đề không có lần xuất hiện nào
    dangling: tuple[str, ...]        # bước có kết quả không được bước sau dùng
    competing: tuple[str, ...]       # đủ riêng từng loại nhưng tranh nhau một lần xuất hiện (con số trùng hợp)


def chain_coverage(statement: str, steps: Sequence[str]) -> ChainCoverage:
    """Mỗi số liệu của đề và mỗi kết quả trung gian (trừ bước cuối) cần MỘT lần xuất hiện riêng ở vế trái.

    Một lần xuất hiện chỉ được tính cho một vai, nên "20 * 0.4" không thể vừa là "20 bạn nữ"
    (kết quả bước trước) vừa là "20%" của đề. Dùng ghép cặp hai phía cực đại để quyết định.
    """
    occurrences = _occurrences(steps)
    givens = given_quantities(statement)
    percent = bool(_PERCENT_CUE_TEXT.search(statement))
    given_options = [[k for k, (_, value) in enumerate(occurrences) if value in forms] for _, forms in givens]
    result_options = []
    for index, step in enumerate(steps[:-1]):
        result = abs(Fraction(step.rpartition("=")[2].strip()))
        forms = {result, result / 100} if percent else {result}
        result_options.append([k for k, (at, value) in enumerate(occurrences) if at > index and value in forms])
    n_slots = len(occurrences)
    unused = tuple(givens[role][0] for role in _unmatched(given_options, n_slots))
    dangling = tuple(steps[role] for role in _unmatched(result_options, n_slots))
    competing: tuple[str, ...] = ()
    if not unused and not dangling:
        joint = _unmatched([*given_options, *result_options], n_slots)
        competing = tuple(givens[role][0] if role < len(givens) else steps[role - len(givens)] for role in joint)
    return ChainCoverage(unused, dangling, competing)


def dangling_steps(steps: Sequence[str]) -> list[str]:
    """Bước (trừ bước cuối) có kết quả không được một lần xuất hiện riêng ở vế trái bước sau dùng tới."""
    return list(chain_coverage("", steps).dangling)


def check_arith_chain(statement: str, steps: Sequence[str], *, grounding: bool = True) -> tuple[str, ...]:
    """Mọi bộ lọc chất lượng của một chuỗi bước số học so với đề; ném SolutionRejected nếu không đạt.

    Thứ tự: bước đồng nhất / quá ít bước (`too_few_steps`), kết quả âm (`negative_quantity`: đại
    lượng của bài lời văn — số đồ vật, tiền, thời gian, vận tốc — không âm; "5 - 8 = -3" rồi dùng
    3 là dấu hiệu đề mâu thuẫn hoặc trừ ngược), số không có nguồn gốc (`incomplete_chain`, chỉ khi
    `grounding`), kết quả bị bỏ rơi (`disconnected_steps`), số liệu của đề không được dùng hoặc bị
    "dùng chung" với một kết quả trùng giá trị (`unused_given`).

    `grounding=False` dành cho bước viết trực tiếp: kiểm `incomplete_chain` nhằm bắt mắt xích mà bộ tách
    CoT bỏ sót, còn bước viết trực tiếp được viết từ chính đề, có thể dùng số mà đề viết bằng chữ
    ("four-fifths", "forty cents", "1 1/4") mà bộ đọc số của đề không nhận ra.
    """
    meaningful = [step for step in steps if not is_identity_step(step)]
    if len(meaningful) != len(steps):
        raise SolutionRejected("too_few_steps", f"có bước đồng nhất: {[s for s in steps if s not in meaningful]}")
    if len(steps) < MIN_STEPS:
        raise SolutionRejected("too_few_steps", f"{len(steps)} bước")
    negative = [step for step in steps if Fraction(step.rpartition("=")[2].strip()) < 0]
    if negative:
        raise SolutionRejected("negative_quantity", "; ".join(negative[:3]))
    missing = ungrounded_numbers(statement, steps) if grounding else []
    if missing:
        raise SolutionRejected("incomplete_chain", "; ".join(missing[:3]))
    coverage = chain_coverage(statement, steps)
    if coverage.dangling:
        raise SolutionRejected("disconnected_steps", "; ".join(coverage.dangling[:3]))
    if coverage.unused_givens:
        raise SolutionRejected("unused_given", "không dùng: " + ", ".join(coverage.unused_givens[:5]))
    if coverage.competing:
        raise SolutionRejected("unused_given", "tranh một lần xuất hiện: " + ", ".join(coverage.competing[:5]))
    return tuple(steps)


def grounded_arith_steps(statement: str, solution: str, answer: str) -> tuple[str, ...]:
    """`extract_arith_steps` + mọi bộ lọc của `check_arith_chain`."""
    return check_arith_chain(statement, extract_arith_steps(solution, answer))


# ---------------------------------------------------------------------------
# Sinh bước giải phương trình bằng SymPy
# ---------------------------------------------------------------------------

_TRANSFORMS = standard_transformations + (implicit_multiplication_application, convert_xor, rationalize)


def _poly(expr: sympy.Expr) -> list[sympy.Rational]:
    """Hệ số [x², x¹, x⁰] (hữu tỉ) của đa thức bậc ≤ 2."""
    poly = sympy.Poly(sympy.expand(expr), X)
    if poly.degree() > 2 or not all(c.is_Rational for c in poly.all_coeffs()):
        raise SolutionRejected("equation_unsupported", f"không phải đa thức hữu tỉ bậc ≤ 2: {expr}")
    coeffs = [sympy.Rational(c) for c in poly.all_coeffs()]
    return [sympy.Rational(0)] * (3 - len(coeffs)) + coeffs


def _is_terminating(value: sympy.Rational) -> bool:
    denominator = int(value.q)
    for prime in (2, 5):
        while denominator % prime == 0:
            denominator //= prime
    return denominator == 1


def _term(coefficient: sympy.Rational, power: int, decimal: bool = False) -> str:
    """Một hạng tử với hệ số dương: `3*x**2`, `x`, `3*x/2`, `5/4`; `decimal` → `1.404*x`."""
    p, q = int(coefficient.p), int(coefficient.q)
    if power == 0:
        return format_number(coefficient)
    base = "x" if power == 1 else "x**2"
    if decimal and q != 1 and _is_terminating(coefficient):
        return f"{format_number(coefficient)}*{base}"
    head = base if p == 1 else f"{p}*{base}"
    return head if q == 1 else f"{head}/{q}"


def format_poly(coeffs: Sequence[sympy.Rational], decimal: bool = False) -> str:
    """[a, b, c] → `a*x**2 + b*x + c` (bỏ hạng tử 0; đa thức 0 → `0`)."""
    return _signed_terms([(coefficient, power) for power, coefficient in zip((2, 1, 0), coeffs)], decimal)


def _signed_terms(terms: Sequence[tuple[sympy.Rational | int, int]], decimal: bool = False) -> str:
    """Dãy (hệ số, bậc) → tổng có dấu, giữ nguyên thứ tự và cả các hạng tử cùng bậc (`x**2 - 3*x - x + 3`)."""
    parts: list[str] = []
    for coefficient, power in terms:
        coefficient = sympy.Rational(coefficient)
        if coefficient == 0:
            continue
        text = _term(abs(coefficient), power, decimal)
        if not parts:
            parts.append(f"-{text}" if coefficient < 0 else text)
        else:
            parts.append(f"{'-' if coefficient < 0 else '+'} {text}")
    return " ".join(parts) if parts else "0"


def _sides(equation: str) -> tuple[sympy.Expr, sympy.Expr]:
    parsed = math_check.parse_equation(equation)  # kiểm an toàn trước mọi phân tích khác
    return parsed.lhs, parsed.rhs


def _unevaluated(side: str) -> sympy.Expr:
    """Cây chưa đánh giá của một vế (đã qua kiểm an toàn của math_check)."""
    math_check.parse_equation(f"{side} = 0")
    return parse_expr(side, local_dict={"x": X}, transformations=_TRANSFORMS, evaluate=False)


def _rebuild(node: sympy.Basic) -> sympy.Expr:
    if not node.args:
        return node
    return node.func(*(_rebuild(arg) for arg in node.args))


def _distributable(node: sympy.Basic) -> sympy.Basic | None:
    """Tích đầu tiên `k*(a + b)` / `(a + b)/k` với k là hằng ≠ ±1 và tổng chứa x."""
    if isinstance(node, sympy.Mul):
        adds = [arg for arg in node.args if isinstance(arg, sympy.Add) and arg.has(X)]
        others = [arg for arg in node.args if arg not in adds]
        if len(adds) == 1 and others and not any(arg.has(X) for arg in others):
            coefficient = _rebuild(sympy.Mul(*others, evaluate=False))
            if coefficient.is_Rational and abs(coefficient) != 1:
                return node
    for arg in node.args:
        found = _distributable(arg)
        if found is not None:
            return found
    return None


def _wrong_distribution(side: str) -> sympy.Expr | None:
    """Vế sau khi phá ngoặc SAI: chỉ nhân hằng số với hạng tử đầu của tổng trong ngoặc."""
    tree = _unevaluated(side)
    node = _distributable(tree)
    if node is None:
        return None
    add = next(arg for arg in node.args if isinstance(arg, sympy.Add))
    others = [arg for arg in node.args if arg is not add]
    coefficient = _rebuild(sympy.Mul(*others, evaluate=False))
    terms = list(add.args)
    wrong = coefficient * _rebuild(terms[0]) + sympy.Add(*(_rebuild(t) for t in terms[1:]))
    return sympy.expand(_rebuild(tree.xreplace({node: wrong})))


def _strip_spaces(text: str) -> str:
    return re.sub(r"\s+", "", text)


def _same_form_key(text: str) -> str:
    """Khóa so hình thức viết: bỏ khoảng trắng, dấu nhân, ngoặc quanh một số/ẩn, dấu + đầu vế; `**` ≡ `^`.

    `x^2 - 5x` ≡ `x**2 - 5*x`, `(x) = 0` ≡ `x = 0`, `+ 10x = 100` ≡ `10*x = 100`.
    """
    key = _strip_spaces(text).replace("**", "^").replace("*", "")
    previous = None
    while previous != key:
        previous = key
        key = re.sub(r"\((x|\d+(?:\.\d+)?)\)", r"\1", key)
    return re.sub(r"(^|=)\+", r"\1", key)


@dataclass(frozen=True)
class _EquationPlan:
    """Bước giải phương trình và các mốc để sinh misconception."""

    steps: tuple[str, ...]
    degree: int
    expand_index: int | None          # bước phá ngoặc / quy đồng (1-based), nếu có
    lcm: int
    move_x_index: int | None          # bước chuyển hạng tử chứa x sang trái
    move_const_index: int | None      # bước chuyển hằng số sang phải
    final_index: int | None           # bước x = v
    linear: tuple[sympy.Rational, sympy.Rational, sympy.Rational, sympy.Rational] | None  # a1, a0, c1, c0
    standard_index: int | None        # bậc hai: dạng chuẩn (hoặc x² = k)
    factored_index: int | None
    factors: tuple[sympy.Expr, ...]
    standard: tuple[int, int, int] | None
    sides: tuple[str, str] = ("", "")  # hai vế của đề theo thứ tự đã giải (vế không có x được đưa sang phải)


class _StepList:
    """Danh sách bước không lặp hình thức: bỏ bước trùng đề hoặc trùng bước ngay trước."""

    def __init__(self, original: str):
        self.items: list[str] = []
        self._last_key = _same_form_key(original)

    def add(self, step: str) -> int | None:
        key = _same_form_key(step)
        if key == self._last_key:
            return None
        self.items.append(step)
        self._last_key = key
        return len(self.items)


def _operand(value: sympy.Rational | Fraction, first: bool = False) -> str:
    """Số làm toán hạng trong một phép tính viết ra: ngoặc quanh số âm (trừ vị trí đầu) và quanh phân số."""
    text = format_number(value)
    if "/" in text or (text.startswith("-") and not first):
        return f"({text})"
    return text


def _difference_text(total: sympy.Rational, constant: sympy.Rational) -> str:
    """`c0 - a0` viết rõ phép tính chuyển vế: `3 - 1`, `3 + 4`."""
    if constant > 0:
        return f"{_operand(total, True)} - {_operand(constant)}"
    return f"{_operand(total, True)} + {_operand(-constant)}"


def _quotient_text(total: sympy.Rational, coefficient: sympy.Rational, decimal: bool) -> str:
    """Bước cô lập x viết rõ: `675 / 25`, `1600 / (-200)`, `351 / 1.404`, `14 * 14`, `4 * 35/2`."""
    if coefficient.q == 1 or (decimal and _is_terminating(coefficient)):
        return f"{_operand(total, True)} / {_operand(coefficient)}"
    reciprocal = 1 / coefficient
    text = format_number(reciprocal) if reciprocal.q == 1 else f"{int(reciprocal.p)}/{int(reciprocal.q)}"
    if reciprocal < 0:
        text = f"({text})"
    return f"{_operand(total, True)} * {text}"


def _clearing_scale(left: Sequence[sympy.Rational], right: Sequence[sympy.Rational], decimal: bool) -> int:
    """Số nhân hai vế để hệ số của x thành số nguyên.

    Đề viết thập phân (hệ số chỉ có mẫu 2^a·5^b) thì nhân 10 hoặc 100; cần nhiều hơn thì giữ
    thập phân (1.404x = 351 tự nhiên hơn 351x = 87750). Còn lại nhân bội chung nhỏ nhất của mẫu.
    """
    lcm = math.lcm(*(int(c.q) for c in (*left[:2], *right[:2])))
    if lcm == 1 or not decimal or not _is_terminating(sympy.Rational(1, lcm)):
        return lcm
    for power in (10, 100):
        if power % lcm == 0:
            return power
    return 1


def _is_bare_x(side: str) -> bool:
    return _same_form_key(side) == "x"


def _evaluation_plan(constant_side: str, answer: str | None) -> _EquationPlan:
    """`x = biểu thức số`: các bước tính dần vế phải (`evaluation_steps`), bước cuối theo dạng đáp án."""
    texts, value = evaluation_steps(constant_side)
    steps = _StepList(f"x = {constant_side}")
    for text in texts[:-1]:
        steps.add(f"x = {text}")
    if texts:
        steps.add(f"x = {_answer_text(value, answer)}")
    rational = sympy.Rational(value.numerator, value.denominator)
    return _EquationPlan(
        steps=tuple(steps.items), degree=1, expand_index=None, lcm=1, move_x_index=None, move_const_index=None,
        final_index=len(steps.items) or None, linear=(sympy.Rational(1), sympy.Rational(0), sympy.Rational(0), rational),
        standard_index=None, factored_index=None, factors=(), standard=None, sides=("x", constant_side),
    )


def _linear_plan(equation: str, lhs: sympy.Expr, rhs: sympy.Expr, answer: str | None = None) -> _EquationPlan:
    left, right = _poly(lhs), _poly(rhs)
    side_l, side_r = (side.strip() for side in equation.split("="))
    if left[1] == 0 and left[0] == 0 and (right[1] != 0 or right[0] != 0):
        left, right = right, left  # "-20 = 16x" → giải như "16x = -20"
        side_l, side_r = side_r, side_l
    if _is_bare_x(side_l) and right[1] == 0:
        try:
            return _evaluation_plan(side_r, answer)
        except ValueError:
            pass  # vế phải ngoài dạng tính dần được: giải như phương trình bậc nhất thường
    decimal = "." in equation
    pure = left[2] == 0 and right[1] == 0  # dạng k·x = c: chỉ cần cô lập x, không quy đồng
    scale = 1 if pure else _clearing_scale(left, right, decimal)
    if scale != 1:
        left = [c * scale for c in left]
        right = [c * scale for c in right]
    original = f"{side_l} = {side_r}"
    steps = _StepList(original)
    expand_index = steps.add(f"{format_poly(left, decimal)} = {format_poly(right, decimal)}")
    a1, a0 = left[1], left[2]
    c1, c0 = right[1], right[2]
    coefficient = a1 - c1
    if coefficient == 0:
        raise SolutionRejected("equation_unsupported", "hệ số của x triệt tiêu")
    move_x_index = move_const_index = None
    if right[0] != 0 or c1 != 0:  # chuyển hạng tử chứa x sang trái (kể cả x² triệt tiêu hai vế)
        move_x_index = steps.add(f"{format_poly([0, coefficient, a0], decimal)} = {format_poly([0, 0, c0], decimal)}")
    if a0 != 0:
        if coefficient == 1 and len(steps.items) + 1 < MIN_STEPS:
            move_const_index = steps.add(f"x = {_difference_text(c0, a0)}")  # bước chuyển hằng chính là bước cuối
        else:
            move_const_index = steps.add(
                f"{format_poly([0, coefficient, 0], decimal)} = {format_poly([0, 0, c0 - a0], decimal)}")
    if coefficient not in (1, -1) and (coefficient.q != 1 or len(steps.items) + 1 < MIN_STEPS):
        steps.add(f"x = {_quotient_text(c0 - a0, coefficient, decimal)}")
    value = (c0 - a0) / coefficient
    steps.add(f"x = {_answer_text(value, answer)}")
    final_index = len(steps.items)
    return _EquationPlan(
        steps=tuple(steps.items), degree=1, expand_index=expand_index, lcm=scale,
        move_x_index=move_x_index, move_const_index=move_const_index, final_index=final_index,
        linear=(a1, a0, c1, c0), standard_index=None, factored_index=None, factors=(), standard=None,
        sides=(side_l, side_r),
    )


def _factor_text(factor: sympy.Expr) -> str:
    coeffs = _poly(factor)
    text = format_poly(coeffs)
    return text if text == "x" else f"({text})"


def _linear_coefficients(factor: sympy.Expr) -> tuple[int, int]:
    _, p, q = _poly(factor)
    return int(p), int(q)


def _grouped_text(first: tuple[int, int], second: tuple[int, int]) -> str:
    """p1·x·(p2·x + q2) + q1·(p2·x + q2): bước nhóm hạng tử để làm xuất hiện nhân tử chung."""
    (p1, q1), (p2, q2) = first, second
    inner = format_poly([sympy.Rational(0), sympy.Rational(p2), sympy.Rational(q2)])
    head = "x" if p1 == 1 else f"{p1}*x"
    tail = f"({inner})" if abs(q1) == 1 else f"{abs(q1)}*({inner})"
    return f"{head}*({inner}) {'+' if q1 > 0 else '-'} {tail}"


def _quadratic_plan(equation: str, lhs: sympy.Expr, rhs: sympy.Expr, answer: str | None = None) -> _EquationPlan:
    coeffs = _poly(lhs - rhs)
    lcm = math.lcm(*(int(c.q) for c in coeffs))
    integers = [int(c * lcm) for c in coeffs]
    divisor = math.gcd(*integers) * (1 if integers[0] > 0 else -1)
    a, b, c = (value // divisor for value in integers)
    _, factor_list = sympy.factor_list(a * X**2 + b * X + c, X)
    factors: list[sympy.Expr] = []
    for factor, multiplicity in factor_list:
        if sympy.Poly(factor, X).degree() != 1:
            raise SolutionRejected("equation_unsupported", f"không phân tích được thành nhân tử bậc nhất: {equation}")
        factors.extend([factor] * multiplicity)
    if len(factors) != 2:
        raise SolutionRejected("equation_unsupported", f"không đủ hai nhân tử: {equation}")
    factors = [f if _linear_coefficients(f)[0] > 0 else -f for f in factors]
    if sympy.expand(factors[0] * factors[1] - (a * X**2 + b * X + c)) != 0:
        raise SolutionRejected("equation_unsupported", f"nhân tử không khớp dạng chuẩn nguyên: {equation}")
    factors.sort(key=lambda f: _root(f))  # nhân tử theo nghiệm tăng dần: (x - 2)*(x - 3)
    double = factors[0] == factors[1]
    steps = _StepList(equation)
    standard_index = None
    if b == 0 and c != 0 and not double:
        if a == 1:
            standard_index = steps.add(f"x**2 = {format_number(sympy.Rational(-c, a))}")
        difference = steps.add(f"{format_poly([sympy.Rational(a), sympy.Rational(0), sympy.Rational(c)])} = 0")
        standard_index = standard_index or difference
    else:
        standard_index = steps.add(f"{format_poly([sympy.Rational(a), sympy.Rational(b), sympy.Rational(c)])} = 0")
        if not double and b != 0 and c != 0:
            first, second = _linear_coefficients(factors[0]), _linear_coefficients(factors[1])
            split_middle = first[0] * second[1], second[0] * first[1]
            steps.add(f"{_signed_terms([(a, 2), (split_middle[0], 1), (split_middle[1], 1), (c, 0)])} = 0")
            steps.add(f"{_grouped_text(first, second)} = 0")
    if double:
        factored_index = steps.add(f"{_factor_text(factors[0])}**2 = 0")
        steps.add(f"{format_poly(_poly(factors[0]))} = 0")
        steps.add(f"x = {_answer_text(_root(factors[0]), answer)}")
    else:
        factored_index = steps.add(f"{_factor_text(factors[0])}*{_factor_text(factors[1])} = 0")
    if factored_index is None:
        raise SolutionRejected("too_few_steps", f"đề đã ở dạng nhân tử: {equation}")
    return _EquationPlan(
        steps=tuple(steps.items), degree=2, expand_index=None, lcm=lcm, move_x_index=None, move_const_index=None,
        final_index=len(steps.items) if double else None, linear=None, standard_index=standard_index,
        factored_index=factored_index, factors=tuple(factors), standard=(a, b, c),
    )


def _plan(equation: str, answer: str | None = None) -> _EquationPlan:
    lhs, rhs = _sides(equation)
    degree = sympy.Poly(sympy.expand(lhs - rhs), X).degree()
    if degree == 1:
        return _linear_plan(equation, lhs, rhs, answer)
    if degree == 2:
        return _quadratic_plan(equation, lhs, rhs, answer)
    raise SolutionRejected("equation_unsupported", f"bậc {degree}")


def equation_steps(equation: str, answer: str | None = None) -> tuple[str, ...]:
    """Chuỗi bước giải phương trình (mỗi bước giữ nguyên tập nghiệm của đề); bước cuối theo dạng `answer`."""
    return _plan(equation, answer).steps


def _root(factor: sympy.Expr) -> sympy.Rational:
    (root,) = sympy.solve(factor, X)
    return sympy.Rational(root)


def _equation_candidates(equation: str, plan: _EquationPlan) -> list[dict[str, Any]]:
    """Misconception ứng viên (chưa kiểm vòng) theo thứ tự ưu tiên."""
    out: list[dict[str, Any]] = []

    def add(kind: str, index: int | None, expression: str) -> None:
        if index is not None:
            out.append({"type": kind, "step_index": index, "expression": expression})

    if plan.degree == 1 and plan.linear is not None:
        a1, a0, c1, c0 = plan.linear
        if plan.expand_index is not None:
            side_l, side_r = plan.sides
            for wrong_side, keep_side, left_first in ((side_l, side_r, True), (side_r, side_l, False)):
                wrong = _wrong_distribution(wrong_side)
                if wrong is None:
                    continue
                kept = sympy.expand(_sides(f"{keep_side} = 0")[0])
                wrong_l, wrong_r = (wrong, kept) if left_first else (kept, wrong)
                try:
                    wl = [v * plan.lcm for v in _poly(wrong_l)]
                    wr = [v * plan.lcm for v in _poly(wrong_r)]
                except SolutionRejected:
                    continue
                if any(v != 0 for v in (*wl[:2], *wr[:2])):
                    add("distribution_error", plan.expand_index, f"{format_poly(wl)} = {format_poly(wr)}")
                break
        coefficient = a1 - c1
        if a0 != 0:
            add("sign_error", plan.move_const_index, f"{format_poly([0, coefficient, 0])} = {format_poly([0, 0, c0 + a0])}")
        if c1 != 0:
            add("sign_error", plan.move_x_index, f"{format_poly([0, a1 + c1, a0])} = {format_poly([0, 0, c0])}")
        value = (c0 - a0) / coefficient
        if value != 0:
            add("sign_error", plan.final_index, f"x = {format_number(-value)}")
        add("arithmetic_slip", plan.final_index, f"x = {format_number((c0 - a0) * coefficient)}")
        add("arithmetic_slip", plan.final_index, f"x = {format_number(value + 1)}")
    elif plan.degree == 2 and plan.standard is not None:
        a, b, c = plan.standard
        first, second = plan.factors
        roots = (_root(first), _root(second))
        if first != second:
            keep = second if roots[1] != 0 else first
            add("missing_root", plan.factored_index, f"{format_poly(_poly(keep))} = 0")
            flip = second if roots[1] != 0 else first
            other = first if flip is second else second
            flipped = sympy.expand(2 * flip.subs(X, 0) * -1 + flip)  # đổi dấu hằng số của một nhân tử
            add("wrong_factor", plan.factored_index, f"{_factor_text(other)}*{_factor_text(flipped)} = 0")
        else:
            flipped = sympy.expand(-2 * first.subs(X, 0) + first)
            if flipped != first:
                add("wrong_factor", plan.factored_index, f"{_factor_text(first)}*{_factor_text(flipped)} = 0")
        if plan.standard_index is not None and c != 0:
            if b == 0:
                add("sign_error", plan.standard_index, f"x**2 = {format_number(sympy.Rational(c, a))}")
            else:
                coeffs = [sympy.Rational(a), sympy.Rational(b), sympy.Rational(-c)]
                add("sign_error", plan.standard_index, f"{format_poly(coeffs)} = 0")
        if first != second:
            negated = sympy.expand(-2 * first.subs(X, 0) + first)
            other_negated = sympy.expand(-2 * second.subs(X, 0) + second)
            add("sign_error", plan.factored_index, f"{_factor_text(negated)}*{_factor_text(other_negated)} = 0")
        elif plan.final_index is not None and roots[0] != 0:
            add("sign_error", plan.final_index, f"x = {format_number(-roots[0])}")
    return out


# ---------------------------------------------------------------------------
# Misconception cho bài số học
# ---------------------------------------------------------------------------

_SWAPS = {"+": ("-", "*"), "-": ("+",), "*": ("+", "/"), "/": ("*", "-")}
_SINGLE_OPERATION = re.compile(r"^(-?[\d.]+) ([+\-*/]) ([\d.]+)$")


def _arith_candidates(steps: Sequence[str]) -> list[dict[str, Any]]:
    """wrong_operation (đổi phép toán, tính đúng), arithmetic_slip (lệch kết quả), sign_error (trừ ngược)."""
    parsed = []
    for index, step in enumerate(steps, start=1):
        lhs, _, rhs = step.rpartition(" = ")
        parsed.append((index, lhs.strip(), rhs.strip()))
    wrong_operation: list[dict[str, Any]] = []
    for index, lhs, _ in parsed:
        operators = [m for m in _BINARY_OPERATOR.finditer(lhs) if m.group(1) in _SWAPS]
        if len(operators) != 1:
            continue
        match = operators[0]
        for swap in _SWAPS[match.group(1)]:
            new_lhs = f"{lhs[: match.start()]} {swap} {lhs[match.end():]}".replace("  ", " ").strip()
            value = _value(new_lhs)
            if value is None:
                continue
            wrong_operation.append({"type": "wrong_operation", "step_index": index,
                                    "expression": f"{new_lhs} = {format_number(value)}"})
    slips: list[dict[str, Any]] = []
    for index, lhs, rhs in reversed(parsed):
        value = _value(rhs)
        if value is None:
            continue
        slips.append({"type": "arithmetic_slip", "step_index": index,
                      "expression": f"{lhs} = {format_number(value + 1)}"})
    signs: list[dict[str, Any]] = []
    for index, lhs, rhs in parsed:
        match = _SINGLE_OPERATION.match(lhs)
        value = _value(rhs)
        if match and match.group(2) == "-" and value not in (None, 0):
            signs.append({"type": "sign_error", "step_index": index,
                          "expression": f"{match.group(3)} - {match.group(1)} = {format_number(-value)}"})
    # Ưu tiên: một wrong_operation + một arithmetic_slip ở bước khác nhau; sign_error làm dự phòng.
    ordered: list[dict[str, Any]] = []
    if wrong_operation:
        ordered.append(wrong_operation[0])
    ordered.extend(slip for slip in slips if not ordered or slip["step_index"] != ordered[0]["step_index"])
    ordered.extend(slips)
    ordered.extend(wrong_operation[1:])
    ordered.extend(signs)
    return ordered


# ---------------------------------------------------------------------------
# Kiểm vòng diagnose và kiểm toàn bài
# ---------------------------------------------------------------------------


def misconception_round_trips(record: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
    """`diagnose(bài, biểu thức, step_index)` trả đúng {equivalent: False, misconception: type}."""
    try:
        result = math_check.diagnose(record, item["expression"], item["step_index"])
    except math_check.MathInputError:
        return False
    return result == {"equivalent": False, "misconception": item["type"]}


def rules_agree(record: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
    """Loại đã khai không mâu thuẫn với quy tắc chung của `diagnose` (chạy trên bài KHÔNG có danh sách lỗi).

    Khứ hồi ở trên gần như luôn đúng vì diagnose so khớp lỗi đã biết của chính bài trước tiên;
    kiểm này bắt nhãn sai (vd. khai arithmetic_slip cho một phép tính đúng nhưng sai phép toán).
    - Số học: quy tắc chung luôn quyết định được → phải trùng loại đã khai.
    - Phương trình: quy tắc chỉ nhận missing_root / sign_error; kết quả None không mâu thuẫn;
      wrong_factor đổi dấu nghiệm trong một nhân tử nên quy tắc gọi là sign_error — chấp nhận.
    """
    bare = {**record, "misconceptions": []}
    try:
        result = math_check.diagnose(bare, item["expression"], item["step_index"])
    except math_check.MathInputError:
        return False
    found = result["misconception"]
    if record.get("kind") == "arithmetic":
        return found == item["type"]
    return found in (None, item["type"]) or (item["type"] == "wrong_factor" and found == "sign_error")


def misconception_ok(record: Mapping[str, Any], item: Mapping[str, Any]) -> bool:
    """Khứ hồi diagnose đúng loại và không mâu thuẫn quy tắc chung."""
    return misconception_round_trips(record, item) and rules_agree(record, item)


def select_misconceptions(record: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]],
                          limit: int = MAX_MISCONCEPTIONS) -> tuple[dict[str, Any], ...]:
    """Chọn tham lam tối đa `limit` misconception mà cả tập vẫn đi vòng diagnose khép kín."""
    chosen: list[dict[str, Any]] = []
    seen: set[str] = set()
    for candidate in candidates:
        if len(chosen) >= limit:
            break
        if candidate["expression"] in seen:
            continue
        trial = [*chosen, dict(candidate)]
        probe = {**record, "misconceptions": trial}
        if all(misconception_ok(probe, item) for item in trial):
            chosen = trial
            seen.add(candidate["expression"])
    return tuple(chosen)


def _last_value_matches(step: str, answer: str) -> bool:
    _, _, rhs = step.rpartition("=")
    return math_check.parse_answer(rhs) == math_check.parse_answer(answer)


def verify_record(record: Mapping[str, Any]) -> list[str]:
    """Kiểm toán học một bản ghi kho bài; trả danh sách lỗi (rỗng = đạt).

    Số học: mọi bước `arith_step` đúng, không có bước đồng nhất, vế phải bước cuối = `answer`.
    Phương trình: `answer` = toàn bộ tập nghiệm, mọi bước giữ tập nghiệm, bước cuối có tập
    nghiệm = `answer`. Cả hai: ≥ 1 misconception và mọi misconception được `diagnose`
    (step_index từ 1) nhận là không tương đương với đúng loại của nó.
    """
    pid = record.get("problem_id", "?")
    errors: list[str] = []
    steps = record.get("steps") or []
    misconceptions = record.get("misconceptions") or []
    if not steps:
        return [f"{pid}: không có bước"]
    if not misconceptions:
        errors.append(f"{pid}: không có misconception")
    try:
        if record.get("kind") == "equation":
            equation = record["equation"]
            if not math_check.answer_matches(equation, record["answer"]):
                errors.append(f"{pid}: đáp án không khớp tập nghiệm")
            for index, step in enumerate(steps, start=1):
                if not math_check.step_is_equivalent(equation, step):
                    errors.append(f"{pid}: bước {index} {step!r} không tương đương đề")
            if math_check.solution_set(steps[-1]) != math_check.parse_answer(record["answer"]):
                errors.append(f"{pid}: bước cuối không cho tập nghiệm = đáp án")
        else:
            for index, step in enumerate(steps, start=1):
                if not math_check.arith_step(step):
                    errors.append(f"{pid}: bước số học {index} {step!r} sai")
                elif is_identity_step(step):
                    errors.append(f"{pid}: bước số học {index} {step!r} là bước đồng nhất")
            if not _last_value_matches(steps[-1], record["answer"]):
                errors.append(f"{pid}: bước cuối không ra đáp án {record['answer']!r}")
    except (math_check.MathInputError, KeyError) as exc:
        errors.append(f"{pid}: {type(exc).__name__}: {exc}")
        return errors
    for item in misconceptions:
        if not misconception_round_trips(record, item):
            errors.append(f"{pid}: misconception {item!r} không được diagnose nhận đúng loại")
        elif not rules_agree(record, item):
            errors.append(f"{pid}: misconception {item!r} mâu thuẫn quy tắc chung của diagnose")
    return errors


# ---------------------------------------------------------------------------
# Gán kỹ năng và độ khó
# ---------------------------------------------------------------------------

_PERCENT_CUE = re.compile(r"%|\bpercent|\bpercentage", re.IGNORECASE)
_RATE_CUE = re.compile(
    r"\bper\b|\bspeed\b|\brate\b|km/h|kmph|\bmph\b|m/s|an hour|each hour|per hour|miles? an?\b|\bvelocity\b|"
    r"\baverage\b|\bwage\b|\bhourly\b",
    re.IGNORECASE,
)
_FRACTION_CUE = re.compile(
    r"\\[dt]?frac|\b\d+\s*/\s*\d+\b|\bhalf\b|\bthird|\bquarter|\bfifth|\bfraction|\btwo-thirds|\bthree-fourths",
    re.IGNORECASE,
)


def arithmetic_skills(statement: str, steps: Sequence[str]) -> tuple[str, ...]:
    """Kỹ năng bài số học: phần trăm > tỉ lệ/vận tốc > phân số (kỹ năng chính đứng đầu), thêm nhiều bước khi ≥ 2 bước."""
    skills: list[str] = []
    non_integer = any((_value(step.rpartition("=")[2]) or sympy.Integer(0)).q != 1 for step in steps)
    if _PERCENT_CUE.search(statement):
        skills.append(SKILL_PERCENT)
    if _RATE_CUE.search(statement):
        skills.append(SKILL_RATES)
    if _FRACTION_CUE.search(statement) or non_integer:
        skills.append(SKILL_FRACTIONS)
    if len(steps) >= 2 or not skills:
        skills.append(SKILL_MULTI_STEP)
    return tuple(skills)


def _has_x(side: str) -> bool:
    return _sides(f"{side} = 0")[0].has(X)


def equation_skills(equation: str, plan: _EquationPlan) -> tuple[str, ...]:
    """Kỹ năng bài phương trình theo cấu trúc đề (phá ngoặc, mẫu số, ẩn hai vế, bậc hai…)."""
    if plan.degree == 2:
        first, second = plan.factors
        a, b, _ = plan.standard or (1, 0, 0)
        skills = [SKILL_SQUARE_ROOT if b == 0 and first != second and _root(first) == -_root(second) else SKILL_FACTORIZATION]
        if first != second:
            skills.append(SKILL_MISSING_ROOT)
        return tuple(skills)
    side_l, side_r = equation.split("=")
    skills = []
    if _distributable(_unevaluated(side_l)) is not None or _distributable(_unevaluated(side_r)) is not None:
        skills.append(SKILL_DISTRIBUTE)
    if plan.lcm != 1:
        skills.append(SKILL_LINEAR_FRACTIONS)
    if _has_x(side_l) and _has_x(side_r):
        skills.append(SKILL_BOTH_SIDES)
    skills.append(SKILL_ISOLATE)
    return tuple(skills)


def arithmetic_difficulty(steps: Sequence[str]) -> int:
    non_integer = any((_value(step.rpartition("=")[2]) or sympy.Integer(0)).q != 1 for step in steps)
    return max(1, min(5, min(len(steps), 4) + int(non_integer)))


def equation_difficulty(plan: _EquationPlan, skills: Sequence[str]) -> int:
    if plan.degree == 2:
        a = plan.standard[0] if plan.standard else 1
        return 3 + int(a != 1)
    extra = sum(skill in skills for skill in (SKILL_DISTRIBUTE, SKILL_LINEAR_FRACTIONS, SKILL_BOTH_SIDES))
    return min(5, 1 + extra)


# ---------------------------------------------------------------------------
# Điểm vào
# ---------------------------------------------------------------------------


def _finish(record: dict[str, Any], candidates: Sequence[Mapping[str, Any]], source: str,
            skills: tuple[str, ...], difficulty: int) -> WrittenSolution:
    chosen = select_misconceptions(record, candidates)
    if not chosen:
        raise SolutionRejected("no_misconception")
    record = {**record, "misconceptions": list(chosen)}
    errors = verify_record(record)
    if errors:
        raise SolutionRejected("verification_failed", "; ".join(errors))
    return WrittenSolution(tuple(record["steps"]), chosen, source, skills, difficulty)


def write_arithmetic(statement: str, solution: str, answer: str) -> WrittenSolution:
    """Bài số học NuminaMath: bước lấy từ CoT (`derived_from_cot`).

    Ngoài các bộ lọc chuỗi, hai kiểm hợp lý rẻ trên chính CoT (không nới kiểm chính xác):
    - CoT không được viết đẳng thức sai giữa hai phép tính số (`cot_false_equalities`): lập luận gốc
      có lỗi thì đáp án gốc không đáng tin, dù từng bước tách ra vẫn đúng;
    - giá trị cuối cùng mà CoT viết ra phải trùng đáp án `\\boxed{}` (`cot_final_value`): lời giải
      kết luận bằng một con số khác (so sánh, làm tròn ngầm, kiểm lại) thì chuỗi tách được không
      diễn đạt đúng lập luận dẫn tới đáp án.
    """
    steps = grounded_arith_steps(statement, solution, answer)
    false_equalities = cot_false_equalities(solution)
    if false_equalities:
        raise SolutionRejected("cot_arithmetic_error", "; ".join(false_equalities[:3]))
    stated = cot_final_value(solution)
    target = _value(answer)
    if stated is not None and target is not None and stated != _fraction(target):
        raise SolutionRejected("cot_answer_conflict", f"CoT kết thúc ở {format_number(stated)} ≠ đáp án {answer}")
    record = {"kind": "arithmetic", "answer": answer, "steps": list(steps)}
    return _finish(record, _arith_candidates(steps), "derived_from_cot",
                   arithmetic_skills(statement, steps), arithmetic_difficulty(steps))


def write_equation(equation: str, answer: str) -> WrittenSolution:
    """Bài phương trình: bước sinh bằng SymPy (`generated_sympy`), tối thiểu MIN_STEPS bước."""
    try:
        plan = _plan(equation, answer)
    except math_check.MathInputError as exc:
        raise SolutionRejected("equation_unsupported", str(exc)) from exc
    if len(plan.steps) < MIN_STEPS:
        raise SolutionRejected("too_few_steps", f"{len(plan.steps)} bước cho {equation}")
    record = {"kind": "equation", "equation": equation, "answer": answer, "steps": list(plan.steps)}
    skills = equation_skills(equation, plan)
    return _finish(record, _equation_candidates(equation, plan), "generated_sympy",
                   skills, equation_difficulty(plan, skills))


def write_solution(kind: str, statement: str, solution: str, answer: str, equation: str | None, *,
                   source: str) -> WrittenSolution:
    """Viết lời giải đã kiểm cho một bài ứng viên; ném SolutionRejected nếu không được.

    Bài số học chỉ nhận từ nguồn bài lời văn (`numina.ARITHMETIC_SOURCES`).
    """
    if kind == "equation":
        if not equation:
            raise SolutionRejected("equation_unsupported", "thiếu phương trình")
        return write_equation(equation, answer)
    if kind == "arithmetic":
        if source not in numina.ARITHMETIC_SOURCES:
            raise SolutionRejected("arithmetic_source_excluded", source)
        return write_arithmetic(statement, solution, answer)
    raise ValueError(f"kind lạ: {kind!r}")


def write_ai_arithmetic(statement: str, answer: str, steps: Sequence[str]) -> WrittenSolution:
    """Bài số học NuminaMath với bước viết trực tiếp (`authored`), dùng khi tách từ CoT thất bại.

    Bước vẫn phải qua bộ kiểm (arith_step, bước cuối = đáp án) và các bộ lọc chuỗi của
    `check_arith_chain` (không bước đồng nhất, ≥ MIN_STEPS bước, kết quả nào cũng được dùng,
    số liệu nào của đề cũng được dùng; trừ `incomplete_chain`, xem docstring hàm đó);
    kỹ năng, độ khó và misconception sinh tự động.
    """
    if not steps or not all(isinstance(step, str) and step.strip() for step in steps):
        raise SolutionRejected("verification_failed", "cần danh sách bước khác rỗng")
    for step in steps:
        try:
            ok = math_check.arith_step(step)
        except math_check.MathInputError as exc:
            raise SolutionRejected("verification_failed", f"{step!r}: {exc}") from exc
        if not ok:
            raise SolutionRejected("verification_failed", f"bước sai: {step!r}")
    check_arith_chain(statement, steps, grounding=False)
    record = {"kind": "arithmetic", "answer": answer, "steps": list(steps)}
    return _finish(record, _arith_candidates(steps), "authored",
                   arithmetic_skills(statement, steps), arithmetic_difficulty(steps))


def complete_authored(record: Mapping[str, Any]) -> WrittenSolution:
    """Bài có lời giải viết trực tiếp (`authored`): giữ nguyên bước/misconception đã viết, chỉ kiểm.

    Nếu bản ghi không ghi misconception thì sinh tự động như các bài khác.
    """
    kind = record["kind"]
    steps = list(record["steps"])
    base = {"kind": kind, "answer": record["answer"], "steps": steps}
    if kind == "equation":
        base["equation"] = record["equation"]
    given = record.get("misconceptions")
    if given:
        candidates: Sequence[Mapping[str, Any]] = given
        limit = len(given)
    elif kind == "equation":
        candidates, limit = _equation_candidates(record["equation"], _plan(record["equation"])), MAX_MISCONCEPTIONS
    else:
        candidates, limit = _arith_candidates(steps), MAX_MISCONCEPTIONS
    chosen = select_misconceptions(base, candidates, limit=limit)
    if given and len(chosen) != len(given):
        raise SolutionRejected("verification_failed", f"misconception viết trực tiếp không đi vòng diagnose: {given}")
    if not chosen:
        raise SolutionRejected("no_misconception")
    full = {**base, "misconceptions": list(chosen)}
    errors = verify_record(full)
    if errors:
        raise SolutionRejected("verification_failed", "; ".join(errors))
    return WrittenSolution(tuple(steps), chosen, "authored", tuple(record["skill_ids"]), int(record["difficulty"]))
