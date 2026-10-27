"""Lọc và chuẩn hóa bài NuminaMath-CoT thành ứng viên cho kho bài.

Ba phần dùng lại được ở các bước sau:
- tách nội dung ``\\boxed{...}`` bằng bộ đếm ngoặc cân bằng (bỏ qua ``\\{``, ``\\}``);
- chuẩn hóa đáp án thành một số hữu tỉ viết chính xác (``24``, ``-3``, ``2.5``, ``3/4``)
  hoặc tập nghiệm số ``{2, 3}`` (cùng định dạng với ``math_check.parse_answer``);
- gán ``kind`` bằng heuristic bảo thủ và lọc từng dòng, trả lý do loại có mã cố định.

An toàn: dữ liệu tải về chỉ được xử lý như chuỗi. Phương trình tách từ đề chỉ được
đưa vào SymPy (qua ``math_check``) sau khi qua danh sách ký tự cho phép, độ dài tối đa
và giới hạn số mũ ≤ 2, nên không thể kích hoạt phép tính lũy thừa khổng lồ.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation
from fractions import Fraction

import sympy

from ai_math_tutor import math_check

ALLOWED_SOURCES = frozenset({"gsm8k", "orca_math", "synthetic_math", "math"})
# Nguồn bài lời văn: chỉ hai nguồn này cho bài số học vào kho. Bài số học của math/synthetic_math
# là bài thi đấu (đếm, đồng dư, tổ hợp) mà chuỗi phép tính không diễn đạt được lập luận và đáp án
# gốc đôi khi sai (rà soát vòng 1); hai nguồn đó chỉ còn đóng góp bài phương trình.
ARITHMETIC_SOURCES = frozenset({"gsm8k", "orca_math"})
KIND_ARITHMETIC = "arithmetic"
KIND_EQUATION = "equation"
MAX_DIGITS = 15
MAX_SET_SIZE = 2
_MAX_EQUATION_LENGTH = 120

# Mã lý do loại → mô tả; manifest ghi nguyên bảng này để số đếm tự giải thích.
DROP_REASONS: dict[str, str] = {
    "source_excluded": "nguồn ngoài {gsm8k, orca_math, synthetic_math, math}",
    "empty_text": "đề hoặc lời giải rỗng",
    "figure": "đề có hình vẽ [asy] — không hiển thị được trong tutor dạng chữ",
    "generation_artifact": "đề lẫn lời thoại của mô hình sinh dữ liệu ('My apologies…', 'Here's the revised problem…')",
    "no_boxed": "lời giải không có \\boxed{...}",
    "malformed_boxed": "\\boxed không có nhóm ngoặc nhọn cân bằng",
    "multiple_distinct_boxes": "nhiều \\boxed với đáp án khác nhau (thường là đề nhiều câu hỏi)",
    "symbolic_problem": "đề dùng ẩn/ký hiệu nhưng không phải một phương trình một ẩn bậc ≤ 2 được nhận diện",
    "equation_answer_mismatch": "đáp án \\boxed không trùng toàn bộ tập nghiệm thực của phương trình tách từ đề",
    "empty_answer": "\\boxed rỗng sau khi bỏ định dạng",
    "multiple_values": "bài số học nhưng đáp án có nhiều giá trị",
    "too_many_values": "phương trình có hơn 2 giá trị trong đáp án",
    "mixed_number": "hỗn số (2\\frac{1}{2}) — mơ hồ giữa phép nhân và phép cộng",
    "number_too_long": f"số có hơn {MAX_DIGITS} chữ số",
    "zero_denominator": "phân số mẫu bằng 0",
    "non_numeric": "đáp án không phải số hữu tỉ (biểu thức, căn, π, khoảng, tọa độ, chữ…)",
    "duplicate_problem": "đề trùng (sau chuẩn hóa khoảng trắng/chữ hoa) với một dòng giữ trước đó",
}

# Quy tắc chuẩn hóa, ghi vào manifest để người đọc data card kiểm lại.
NORMALIZATION_RULES: tuple[str, ...] = (
    "Đáp án = nội dung \\boxed{...} (hoặc \\fbox) CUỐI CÙNG trong lời giải, tách bằng bộ đếm ngoặc cân bằng; "
    "\\{ và \\} là ký tự thường, không tính vào độ sâu.",
    "Nhiều \\boxed: chỉ giữ khi mọi hộp chuẩn hóa ra cùng một đáp án; ngược lại loại (multiple_distinct_boxes).",
    "Bỏ $, \\$, khoảng trắng LaTeX (\\, \\! \\; \\quad ~), \\left/\\right, dấu chấm/phẩy cuối; \\dfrac/\\tfrac → \\frac.",
    "Dấu phẩy hàng nghìn chỉ khi đúng mẫu d{1,3}(,ddd)+ (không có khoảng trắng) hoặc {,} của LaTeX; "
    "mọi dấu phẩy khác là dấu phân cách tập nghiệm.",
    "% và \\% bị bỏ, GIỮ NGUYÊN giá trị (25\\% → 25, vì đề hỏi 'bao nhiêu phần trăm'); ký hiệu độ (^\\circ, °) bị bỏ.",
    "Ký hiệu tiền đứng trước (Rs., ₹, €, £, ¥, USD, INR) bị bỏ.",
    "\\text{}/\\mbox{}/\\mathrm{}... được mở ra thành chữ thường; đơn vị bị bỏ khi là chuỗi từ đứng SAU số, cách "
    "bằng khoảng trắng, từ đầu ≥ 2 chữ cái hoặc đơn vị ghép (24 dollars, 5 \\text{ cm}^2, 9.8 m/s^2, 24 square "
    "feet), chỉ cho mũ ^2/^3; chữ cái đơn hoặc dính liền số (5 m, 2x, 5m) và đuôi có chữ số khác (5 times 3) "
    "không bị coi là đơn vị → loại.",
    "Gán biến một chữ cái được bỏ (x = 4, x_1 = 2); tập nghiệm tách theo ',', ';', 'or', 'and'; "
    "\\pm a hoặc ±a → {-a, a} (chỉ với phương trình).",
    "Số hợp lệ: số nguyên, thập phân, \\frac{p}{q} (kể cả \\frac34), p/q, có dấu âm; hỗn số, căn, π, mũ, "
    "ký hiệu khoa học bị loại.",
    "Dạng chuẩn: số nguyên 'n'; thập phân giữ dạng thập phân rút gọn ('2.50' → '2.5', '12.0' → '12'); phân số "
    "rút gọn 'p/q'; tập nghiệm '{a, b}' sắp tăng, bỏ trùng (nghiệm kép → một số).",
)

_LETTER_VARIABLE_BLOCKLIST = frozenset({"e", "i"})  # hằng số Euler và đơn vị ảo, không phải ẩn


class AnswerRejected(ValueError):
    """Đáp án hoặc dòng dữ liệu bị loại; ``reason`` là một khóa của ``DROP_REASONS``."""

    def __init__(self, reason: str, detail: str = "") -> None:
        if reason not in DROP_REASONS:
            raise ValueError(f"lý do loại không khai báo: {reason!r}")
        super().__init__(f"{reason}: {detail}" if detail else reason)
        self.reason = reason


# ---------------------------------------------------------------------------
# Tách \boxed{...}
# ---------------------------------------------------------------------------

_BOX_COMMAND = re.compile(r"\\(?:boxed|fbox)(?![A-Za-z])")


def _balanced_group(text: str, open_index: int) -> int | None:
    """Vị trí ngoặc đóng khớp với ``text[open_index] == '{'``; None nếu không cân bằng.

    Ký tự sau dấu gạch chéo ngược bị bỏ qua, nên ``\\{``, ``\\}`` và ``\\\\`` không làm
    đổi độ sâu.
    """
    depth = 0
    index = open_index
    while index < len(text):
        char = text[index]
        if char == "\\":
            index += 2
            continue
        if char == "{":
            depth += 1
        elif char == "}":
            depth -= 1
            if depth == 0:
                return index
        index += 1
    return None


def boxed_answers(text: str) -> list[str]:
    """Nội dung mọi ``\\boxed{...}`` ngoài cùng theo thứ tự xuất hiện.

    Hộp lồng (``\\boxed{\\boxed{5}}``) chỉ tính hộp ngoài. Ném ``AnswerRejected``
    (``malformed_boxed``) nếu sau ``\\boxed`` không có nhóm ngoặc nhọn cân bằng.
    """
    found: list[str] = []
    position = 0
    while True:
        match = _BOX_COMMAND.search(text, position)
        if match is None:
            return found
        open_index = match.end()
        while open_index < len(text) and text[open_index].isspace():
            open_index += 1
        if open_index >= len(text) or text[open_index] != "{":
            raise AnswerRejected("malformed_boxed", text[match.start() : match.start() + 40])
        close_index = _balanced_group(text, open_index)
        if close_index is None:
            raise AnswerRejected("malformed_boxed", text[match.start() : match.start() + 40])
        found.append(text[open_index + 1 : close_index])
        position = close_index + 1


def last_boxed(text: str) -> str | None:
    """Nội dung ``\\boxed{...}`` cuối cùng, hoặc None nếu lời giải không có hộp nào."""
    answers = boxed_answers(text)
    return answers[-1] if answers else None


# ---------------------------------------------------------------------------
# Chuẩn hóa đáp án số
# ---------------------------------------------------------------------------

_TEXT_WRAPPER = re.compile(r"\\(?:text|textbf|textit|textrm|mbox|mathrm|mathbf|mathit|operatorname)\s*\{([^{}]*)\}")
_LATEX_SPACING = re.compile(r"\\[,;:> ]|\\q?quad(?![A-Za-z])|~")
# \! (khoảng âm) chỉ dùng để dính chữ số: "1,\!657" là 1657, nên xóa hẳn thay vì thành khoảng trắng.
_NEGATIVE_SPACE = re.compile(r"\\!")
_DEGREE = re.compile(r"\^\s*\{?\s*\\circ\s*\}?|\\circ(?![A-Za-z])|\\degree(?![A-Za-z])|°")
_LATEX_THOUSANDS = re.compile(r"(?<=\d)\{,\}(?=\d{3})")
_PLAIN_THOUSANDS = re.compile(r"(?<![\d.,])\d{1,3}(?:,\d{3})+(?![\d,])")
_LEADING_CURRENCY = re.compile(r"^(?:Rs\.?|INR|USD|₹|€|£|¥)\s*")
_SEPARATORS = re.compile(r"\s*(?:,|;|\bor\b|\band\b)\s*")
_ASSIGNMENT = re.compile(r"^[A-Za-z](?:_\{?[A-Za-z0-9]+\}?)?\s*=\s*")
# Đơn vị: chuỗi từ (từ đầu ≥ 2 chữ cái, hoặc 1 chữ cái của đơn vị ghép m/s) đứng sau số, cách bằng khoảng trắng; nối bằng khoảng trắng,
# '/', '.', '-' và chỉ cho mũ ^2/^3 ("5 cm^2", "9.8 m/s^2", "24 square feet"). Không có chữ số khác,
# nên "5 times 3" không bị cắt thành 5.
_UNIT_POWER = r"(?:\s*\^\s*\{?[23]\}?)?"
_UNIT_TAIL = re.compile(
    rf"(?<=\d)\s+(?:[A-Za-z]{{2,}}|[A-Za-z](?=\s*/\s*[A-Za-z])){_UNIT_POWER}(?:[\s./-]+[A-Za-z]+{_UNIT_POWER})*\.?$"
)

_INTEGER = re.compile(r"^[+-]?\d+$")
_DECIMAL = re.compile(r"^[+-]?(?:\d+\.\d*|\.\d+)$")
_FRAC_BRACED = re.compile(r"^([+-]?)\\frac\{\s*([+-]?\d+)\s*\}\{\s*([+-]?\d+)\s*\}$")
_FRAC_SHORT = re.compile(r"^([+-]?)\\frac\s*(\d|\{\d+\})\s*(\d|\{\d+\})$")
_SLASH_FRACTION = re.compile(r"^([+-]?)\(?([+-]?\d+)\)?\s*/\s*\(?(\d+)\)?$")
_MIXED_NUMBER = re.compile(r"^[+-]?\d+\s*\\frac|^[+-]?\d+\s+\d+\s*/\s*\d+$")


def _strip_wrappers(text: str) -> str:
    """Bỏ các lớp ``\\boxed{}``/``$...$``/``\\{...\\}`` bao trọn đáp án."""
    while True:
        stripped = text.strip()
        if stripped.startswith("$") and stripped.endswith("$") and len(stripped) > 1:
            stripped = stripped.strip("$").strip()
        match = _BOX_COMMAND.match(stripped)
        if match:
            inner = boxed_answers(stripped)
            if len(inner) == 1 and stripped.endswith("}"):
                stripped = inner[0]
        if stripped.startswith("\\{") and stripped.endswith("\\}"):
            stripped = stripped[2:-2]
        if stripped == text:
            return stripped
        text = stripped


def _clean_latex(raw: str) -> str:
    text = raw.replace("\u2212", "-").replace("\u00a0", " ").replace("\u00b1", "\\pm ")
    text = _strip_wrappers(text)
    text = text.replace("\\$", "").replace("$", "")
    text = _NEGATIVE_SPACE.sub("", text)
    text = _LATEX_SPACING.sub(" ", text)
    text = re.sub(r"\\(?:left|right)(?![A-Za-z])", "", text)
    text = re.sub(r"\\[dt]frac(?![A-Za-z])", r"\\frac", text)
    text = _LATEX_THOUSANDS.sub("", text)
    text = text.replace("\\%", "").replace("%", "")
    text = _DEGREE.sub("", text)
    previous = None
    while previous != text:
        previous = text
        text = _TEXT_WRAPPER.sub(lambda m: f" {m.group(1)} ", text)
    text = re.sub(r"\s+", " ", text).strip().rstrip(".,;").strip()
    return _PLAIN_THOUSANDS.sub(lambda m: m.group(0).replace(",", ""), text)


def _canonical_fraction(value: Fraction) -> str:
    return str(value.numerator) if value.denominator == 1 else f"{value.numerator}/{value.denominator}"


def _canonical_decimal(text: str) -> tuple[Fraction, str]:
    try:
        number = Decimal(text)
    except InvalidOperation as error:  # regex đã lọc, phòng hờ
        raise AnswerRejected("non_numeric", text) from error
    canonical = format(number.normalize(), "f")
    if "." in canonical:
        canonical = canonical.rstrip("0").rstrip(".")
    if canonical in {"-0", "+0"}:
        canonical = "0"
    return Fraction(number), canonical


def _check_digits(*parts: str) -> None:
    if sum(char.isdigit() for part in parts for char in part) > MAX_DIGITS:
        raise AnswerRejected("number_too_long", "".join(parts))


def parse_number(item: str) -> tuple[Fraction, str]:
    """Một số hữu tỉ viết chính xác → (giá trị, dạng chuẩn). Ném ``AnswerRejected`` nếu không phải."""
    text = item.strip()
    if not text:
        raise AnswerRejected("empty_answer")
    if _INTEGER.match(text):
        _check_digits(text)
        value = Fraction(int(text))
        return value, _canonical_fraction(value)
    if _DECIMAL.match(text):
        _check_digits(text)
        return _canonical_decimal(text)
    if _MIXED_NUMBER.match(text):
        raise AnswerRejected("mixed_number", text)
    for pattern in (_FRAC_BRACED, _FRAC_SHORT, _SLASH_FRACTION):
        match = pattern.match(text)
        if match:
            sign, numerator, denominator = match.groups()
            numerator, denominator = numerator.strip("{}"), denominator.strip("{}")
            _check_digits(numerator, denominator)
            if int(denominator) == 0:
                raise AnswerRejected("zero_denominator", text)
            value = Fraction(int(numerator), int(denominator))
            return (-value if sign == "-" else value), _canonical_fraction(-value if sign == "-" else value)
    raise AnswerRejected("non_numeric", text)


def _strip_units(item: str) -> str:
    item = _LEADING_CURRENCY.sub("", item.strip())
    return _UNIT_TAIL.sub("", item).strip()


def _item_values(item: str, allow_set: bool) -> list[tuple[Fraction, str]]:
    item = _strip_units(_ASSIGNMENT.sub("", item.strip()))
    if item.startswith("\\pm"):
        if not allow_set:
            raise AnswerRejected("multiple_values", item)
        value, canonical = parse_number(item[3:])
        magnitude = canonical.lstrip("+-")
        return [(-abs(value), "-" + magnitude if value else magnitude), (abs(value), magnitude)]
    return [parse_number(item)]


def normalize_answer(raw: str, *, allow_set: bool = False) -> str:
    """Đáp án ``\\boxed`` thô → chuỗi chuẩn.

    ``allow_set=False`` (bài số học): đúng một số. ``allow_set=True`` (phương trình): một
    số hoặc tập ``{a, b}`` tối đa ``MAX_SET_SIZE`` phần tử, sắp tăng, bỏ trùng. Ném
    ``AnswerRejected`` với lý do cụ thể khi không chuẩn hóa được.
    """
    text = _clean_latex(raw)
    if not text:
        raise AnswerRejected("empty_answer")
    items = [item for item in _SEPARATORS.split(text) if item.strip()]
    if not items:
        raise AnswerRejected("empty_answer")
    if len(items) > 1 and not allow_set:
        raise AnswerRejected("multiple_values", text)
    values: dict[Fraction, str] = {}
    for item in items:
        for value, canonical in _item_values(item, allow_set):
            values.setdefault(value, canonical)
    if len(values) > MAX_SET_SIZE:
        raise AnswerRejected("too_many_values", text)
    ordered = [values[key] for key in sorted(values)]
    return ordered[0] if len(ordered) == 1 else "{" + ", ".join(ordered) + "}"


def answer_value(answer: str) -> Fraction:
    """Giá trị chính xác của một đáp án số đơn đã chuẩn hóa (``'2.5'`` → 5/2)."""
    value, _ = parse_number(answer)
    return value


# ---------------------------------------------------------------------------
# Heuristic loại bài: phương trình một ẩn bậc ≤ 2 hay bài số học
# ---------------------------------------------------------------------------

# Nguồn viết công thức bằng LaTeX ($...$, tiền tệ viết \$). gsm8k/orca_math dùng "$" làm ký hiệu
# tiền ("costs $4") nên đề của chúng luôn được xét như văn bản phẳng.
LATEX_SOURCES = frozenset({"math", "synthetic_math"})
_MATH_SEGMENT = re.compile(r"\$\$(.+?)\$\$|\$(.+?)\$|\\\[(.+?)\\\]|\\\((.+?)\\\)", re.DOTALL)
# Một "đoạn công thức" trong văn bản phẳng: chuỗi liên tiếp gồm số, chữ cái ĐƠN, toán tử, ngoặc.
_PLAIN_FORMULA = re.compile(r"(?:(?<![A-Za-z])[a-z](?![A-Za-z])|\d+(?:\.\d+)?|[-+*/^()=]|[ \t])+")
_SOLVE_CUE = re.compile(
    r"\bsolve\b|\bsolutions?\b|\broots?\b|\bvalue of \$?\(?[a-z]\)?\$?(?![a-z])|"
    r"\bfind \$?[a-z]\$?(?![a-z])|\bfor \$?[a-z]\$?(?![a-z])|\bwhat is \$?[a-z]\$?(?![a-z])",
    re.IGNORECASE,
)
_PLAIN_EQUATION_CHARS = re.compile(r"^[0-9a-z+\-*/^().\s=]+$")
_BAD_EXPONENT = re.compile(r"\^(?![12](?!\d))")
_ALREADY_SOLVED = re.compile(r"^\s*(?:x\s*=\s*[-+]?[\d.]+|[-+]?[\d.]+\s*=\s*x)\s*$")
# Đề lời văn hay viết "12 x 6" với x là dấu nhân.
_LETTER_AS_TIMES = re.compile(r"\d\s+[a-z]\s+\(?\d")
_LATEX_COMMAND = re.compile(r"\\[A-Za-z]+")
# Ẩn trong đề lời văn không dùng $: "3x + 5 = 20", "x = ", "= y".
_PLAIN_TEXT_VARIABLE = re.compile(r"\d\s*[a-z]\b(?=\s*[-+*/=^)])|\b[a-z]\s*[=^]|=\s*-?\s*\d*\s*[a-z]\b")


def math_segments(text: str) -> list[str]:
    """Các đoạn công thức ``$...$``, ``$$...$$``, ``\\[...\\]``, ``\\(...\\)`` trong đề."""
    return [next(group for group in match.groups() if group is not None) for match in _MATH_SEGMENT.finditer(text)]


def _replace_fracs(text: str) -> str | None:
    """``\\frac{A}{B}`` → ``((A)/(B))`` đệ quy; None nếu ngoặc lệch."""
    while True:
        index = text.find("\\frac")
        if index < 0:
            return text
        open_a = index + len("\\frac")
        if open_a >= len(text) or text[open_a] != "{":
            return None
        close_a = _balanced_group(text, open_a)
        if close_a is None or close_a + 1 >= len(text) or text[close_a + 1] != "{":
            return None
        close_b = _balanced_group(text, close_a + 1)
        if close_b is None:
            return None
        numerator = text[open_a + 1 : close_a]
        denominator = text[close_a + 2 : close_b]
        text = f"{text[:index]}(({numerator})/({denominator})){text[close_b + 1:]}"


def latex_to_plain(segment: str) -> str | None:
    """Công thức LaTeX đơn giản → biểu thức phẳng kiểu ``2*x + 3 = 11``; None nếu có lệnh lạ."""
    text = _NEGATIVE_SPACE.sub("", segment.replace("\u2212", "-"))
    text = _LATEX_SPACING.sub(" ", text)
    text = re.sub(r"\\(?:left|right)(?![A-Za-z])", "", text)
    text = re.sub(r"\\[dt]frac(?![A-Za-z])", r"\\frac", text)
    text = re.sub(r"\\frac\s*(\d)\s*(\d)", r"\\frac{\1}{\2}", text)  # dạng tắt \frac 18
    text = re.sub(r"\\displaystyle(?![A-Za-z])", "", text)
    text = re.sub(r"\\(?:cdot|times)(?![A-Za-z])", "*", text)
    text = re.sub(r"\\div(?![A-Za-z])", "/", text)
    replaced = _replace_fracs(text)
    if replaced is None or "\\" in replaced:
        return None
    text = re.sub(r"\^\s*\{\s*(\d+)\s*\}", r"^\1", replaced)
    text = text.replace("{", "(").replace("}", ")")
    text = re.sub(r"\s+", " ", text).strip().rstrip(".,;:").strip()
    return text


def _single_variable_equation(plain: str) -> tuple[str, str] | None:
    """Phương trình phẳng một ẩn bậc 1–2, đa thức → (phương trình theo ẩn ``x``, chữ cái ẩn gốc); None nếu không đạt."""
    if len(plain) > _MAX_EQUATION_LENGTH or plain.count("=") != 1 or not _PLAIN_EQUATION_CHARS.match(plain):
        return None
    letters = set(re.findall(r"[a-z]", plain))
    if len(letters) != 1 or letters & _LETTER_VARIABLE_BLOCKLIST:
        return None
    if re.search(r"[a-z]{2,}", plain) or _BAD_EXPONENT.search(plain):
        return None
    if any(len(token) > MAX_DIGITS for token in re.findall(r"\d+", plain)):
        return None
    (letter,) = letters
    candidate = plain.replace(letter, "x")
    if _ALREADY_SOLVED.match(candidate):  # "x = 0.76" là dữ kiện gán sẵn, không phải đề cần giải
        return None
    try:
        equation = math_check.parse_equation(candidate)
        degree = sympy.Poly(equation.lhs - equation.rhs, math_check.VARIABLE).degree()
    except (math_check.MathInputError, sympy.PolynomialError, sympy.SympifyError, SyntaxError, TypeError,
            ValueError, ZeroDivisionError):
        return None
    if degree not in (1, 2):
        return None
    return candidate, letter


def _formula_segments(problem: str, latex: bool) -> list[str]:
    if latex:
        return math_segments(problem.replace("\\$", " "))
    return [match.group(0).strip() for match in _PLAIN_FORMULA.finditer(problem) if match.group(0).strip()]


def detect_equation(problem: str, *, latex: bool) -> str | None:
    """Phương trình một ẩn bậc ≤ 2 mà đề yêu cầu giải, viết phẳng theo ẩn ``x``; None nếu không chắc.

    Xem ``detect_equation_variable`` để lấy thêm chữ cái ẩn gốc của đề.
    """
    found = detect_equation_variable(problem, latex=latex)
    return found[0] if found is not None else None


def detect_equation_variable(problem: str, *, latex: bool) -> tuple[str, str] | None:
    """(phương trình theo ẩn ``x``, chữ cái ẩn gốc như ``m``/``y``/``x``) hoặc None nếu không chắc.

    ``math_check`` chỉ nhận ẩn ``x`` nên phương trình luôn được đổi ẩn thành ``x``; chữ cái gốc
    được giữ riêng để hiển thị lời giải đúng tên ẩn của đề.

    Bảo thủ: đề phải có từ khóa yêu cầu giải/tìm ẩn và có ĐÚNG MỘT đoạn công thức chứa
    dấu '=' cùng chữ cái (hệ phương trình, điều kiện phụ ``x = 3`` hay hàm ``f(x)`` đều
    làm hỏng điều kiện này). ``latex=False`` cho đề lời văn: đoạn công thức là chuỗi số,
    chữ cái đơn và toán tử liên tiếp (``Solve 4x - 7 = 9.``).
    """
    if not _SOLVE_CUE.search(problem):
        return None
    segments = _formula_segments(problem, latex)
    equation_segments = [seg for seg in segments if "=" in seg and re.search(r"[A-Za-z]", _LATEX_COMMAND.sub("", seg))]
    if len(equation_segments) != 1:
        return None
    plain = latex_to_plain(equation_segments[0]) if latex else equation_segments[0]
    if plain is None or (not latex and _LETTER_AS_TIMES.search(plain)):
        return None
    return _single_variable_equation(plain.strip())


def is_symbolic(problem: str, *, latex: bool) -> bool:
    """Đề có ẩn/ký hiệu chữ trong công thức (``$x$``, ``$f(n)$``, ``$ABC$``, ``3x + 5 = 20``)."""
    if latex:
        text = problem.replace("\\$", " ")
        for segment in math_segments(text):
            stripped = _TEXT_WRAPPER.sub(" ", segment)
            stripped = _LATEX_COMMAND.sub(" ", stripped)
            if re.search(r"[A-Za-z]", stripped):
                return True
        text = _MATH_SEGMENT.sub(" ", text)
    else:
        text = problem
    return bool(_PLAIN_TEXT_VARIABLE.search(text))


def classify_kind(source: str, problem: str) -> str:
    """``equation`` khi nhận diện được phương trình một ẩn bậc ≤ 2 cần giải, ngược lại ``arithmetic``."""
    found = detect_equation(problem, latex=source in LATEX_SOURCES)
    return KIND_EQUATION if found is not None else KIND_ARITHMETIC


# ---------------------------------------------------------------------------
# Lọc một dòng
# ---------------------------------------------------------------------------


# Câu thoại của mô hình sinh đề (orca_math) lọt vào phần đề.
_GENERATION_ARTIFACT = re.compile(
    r"\bmy apologies\b|\bi apologi[sz]e\b|\bi misunderstood\b|\bas an ai\b|\bwithout (?:the|providing the) solution\b|"
    r"\bhere(?:'s| is) (?:the|a) (?:revised|rephrased|harder|modified|new) (?:problem|question|version)\b",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class Candidate:
    """Một dòng NuminaMath đạt mọi bộ lọc."""

    kind: str
    answer_raw: str
    answer: str
    equation: str | None
    variable: str | None = None  # chữ cái ẩn gốc của đề, chỉ có ở bài phương trình


def _final_answer(solution: str, allow_set: bool) -> tuple[str, str]:
    boxes = boxed_answers(solution)
    if not boxes:
        raise AnswerRejected("no_boxed")
    answer_raw = boxes[-1].strip()
    answer = normalize_answer(answer_raw, allow_set=allow_set)
    distinct_raw = {box.strip() for box in boxes}
    if len(distinct_raw) > 1:
        for box in distinct_raw:
            try:
                other = normalize_answer(box, allow_set=allow_set)
            except AnswerRejected as error:
                raise AnswerRejected("multiple_distinct_boxes", box) from error
            if other != answer:
                raise AnswerRejected("multiple_distinct_boxes", f"{box!r} ≠ {answer_raw!r}")
    return answer_raw, answer


def screen_row(source: str, problem: str, solution: str) -> Candidate:
    """Áp mọi bộ lọc lên một dòng; trả ``Candidate`` hoặc ném ``AnswerRejected``."""
    if source not in ALLOWED_SOURCES:
        raise AnswerRejected("source_excluded", source)
    if not problem or not problem.strip() or not solution or not solution.strip():
        raise AnswerRejected("empty_text")
    if "[asy]" in problem:
        raise AnswerRejected("figure")
    if _GENERATION_ARTIFACT.search(problem):
        raise AnswerRejected("generation_artifact")
    if not boxed_answers(solution):
        raise AnswerRejected("no_boxed")
    latex = source in LATEX_SOURCES
    detected = detect_equation_variable(problem, latex=latex)
    if detected is not None:
        equation, variable = detected
        answer_raw, answer = _final_answer(solution, allow_set=True)
        try:
            matches = math_check.answer_matches(equation, answer)
        except math_check.MathInputError as error:
            raise AnswerRejected("equation_answer_mismatch", answer) from error
        if not matches:
            raise AnswerRejected("equation_answer_mismatch", f"{equation} ⇏ {answer}")
        return Candidate(KIND_EQUATION, answer_raw, answer, equation, variable)
    if is_symbolic(problem, latex=latex):
        raise AnswerRejected("symbolic_problem")
    answer_raw, answer = _final_answer(solution, allow_set=False)
    return Candidate(KIND_ARITHMETIC, answer_raw, answer, None)


def problem_key(problem: str) -> str:
    """Khóa khử trùng đề: sha1 của đề viết thường, gộp khoảng trắng."""
    normalized = re.sub(r"\s+", " ", problem.strip().lower())
    return hashlib.sha1(normalized.encode("utf-8")).hexdigest()


def allocate_equal(stratum_sizes: dict[tuple[str, str], int], total: int) -> dict[tuple[str, str], int]:
    """Chia ``total`` đều cho các tầng, tầng nhỏ lấy hết rồi phần dư chia tiếp (water-filling).

    Tất định: phần lẻ chia theo thứ tự khóa tầng đã sắp.
    """
    if total < 0:
        raise ValueError("total phải ≥ 0")
    allocation = {key: 0 for key in stratum_sizes}
    remaining = min(total, sum(stratum_sizes.values()))
    open_keys = sorted(key for key, size in stratum_sizes.items() if size > 0)
    while remaining > 0 and open_keys:
        share, extra = divmod(remaining, len(open_keys))
        next_open = []
        for position, key in enumerate(open_keys):
            want = share + (1 if position < extra else 0)
            take = min(want, stratum_sizes[key] - allocation[key])
            allocation[key] += take
            remaining -= take
            if allocation[key] < stratum_sizes[key]:
                next_open.append(key)
        open_keys = next_open
    return allocation
