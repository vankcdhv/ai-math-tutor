from fractions import Fraction

import pytest

from ai_math_tutor import math_check, numina


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("24", "24"),
        ("-3", "-3"),
        ("2.50", "2.5"),
        ("12.0", "12"),
        (".5", "0.5"),
        (r"\frac{3}{4}", "3/4"),
        (r"\dfrac{6}{8}", "3/4"),  # rút gọn
        (r"-\frac{1}{2}", "-1/2"),
        (r"\frac{-1}{2}", "-1/2"),
        (r"\frac34", "3/4"),
        ("3/4", "3/4"),
        ("18/19", "18/19"),
        (r"\frac{8}{2}", "4"),
        ("1,000", "1000"),
        ("2,000,", "2000"),  # dấu phẩy thừa cuối
        (r"1{,}234", "1234"),
        (r"1,\!657", "1657"),
        (r"\$1,250.50", "1250.5"),
        ("$7$", "7"),
        (r"25\%", "25"),  # giữ giá trị phần trăm
        (r"45^\circ", "45"),
        (r"24 \text{ dollars}", "24"),
        ("24 dollars", "24"),
        (r"5 \text{ cm}^2", "5"),
        (r"9.8 \text{ m/s}^2", "9.8"),
        ("15 km/h", "15"),
        ("24 square feet", "24"),
        ("Rs. 1200", "1200"),
        ("x = 4", "4"),
        (r"\boxed{9}", "9"),
        ("4.", "4"),
        ("−5", "-5"),  # dấu trừ Unicode
    ],
)
def test_normalize_single_number(raw, expected):
    assert numina.normalize_answer(raw) == expected


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("x = 2, 3", "{2, 3}"),
        ("3, -2", "{-2, 3}"),
        (r"x=2 \text{ or } x=3", "{2, 3}"),
        ("x_1 = 2, x_2 = 3", "{2, 3}"),
        (r"\{2, 3\}", "{2, 3}"),
        (r"\pm 3", "{-3, 3}"),
        ("x = ±2.5", "{-2.5, 2.5}"),
        (r"\frac{1}{2}, -1", "{-1, 1/2}"),
        ("x = 5", "5"),
        ("3, 3", "3"),  # nghiệm kép
    ],
)
def test_normalize_solution_set(raw, expected):
    normalized = numina.normalize_answer(raw, allow_set=True)
    assert normalized == expected
    math_check.parse_answer(normalized)  # cùng định dạng với bộ kiểm


@pytest.mark.parametrize(
    ("raw", "allow_set", "reason"),
    [
        ("2x", False, "non_numeric"),
        ("5m", False, "non_numeric"),  # chữ đơn/dính số không phải đơn vị
        ("5 m", False, "non_numeric"),
        ("5 times 3", False, "non_numeric"),  # đuôi có chữ số khác mũ → không phải đơn vị
        (r"\sqrt{2}", False, "non_numeric"),
        (r"32\pi", False, "non_numeric"),
        ("2:1", False, "non_numeric"),
        ("(2, 3)", True, "non_numeric"),
        ("[1, 2]", True, "non_numeric"),
        (r"3 \times 10^5", False, "non_numeric"),
        ("1123_5", False, "non_numeric"),
        (r"2\frac{1}{2}", False, "mixed_number"),
        ("2 1/2", False, "mixed_number"),
        (r"\frac{1}{0}", False, "zero_denominator"),
        ("1234567890123456", False, "number_too_long"),
        ("0.6666666666666666", False, "number_too_long"),
        ("", False, "empty_answer"),
        (r"\text{ }", False, "empty_answer"),
        ("2, 3", False, "multiple_values"),
        (r"\pm 3", False, "multiple_values"),
        ("1, 2, 3", True, "too_many_values"),
    ],
)
def test_normalize_rejects_with_reason(raw, allow_set, reason):
    with pytest.raises(numina.AnswerRejected) as caught:
        numina.normalize_answer(raw, allow_set=allow_set)
    assert caught.value.reason == reason


def test_answer_value_is_exact():
    assert numina.answer_value("2.5") == Fraction(5, 2)
    assert numina.answer_value("-3/4") == Fraction(-3, 4)


@pytest.mark.parametrize(
    ("source", "problem", "equation"),
    [
        ("synthetic_math", "Solve for $x$: $3x + 5 = 20$.", "3x + 5 = 20"),
        ("math", "Find all solutions to $x^2 - 5x + 6 = 0$.", "x^2 - 5x + 6 = 0"),
        ("math", "Solve $2(y - 1) = 10$ for $y$.", "2(x - 1) = 10"),
        ("synthetic_math", r"Solve for \( c \): \[\frac{c-23}{2} = \frac{2c +5}{7}.\]", "((x-23)/(2)) = ((2x +5)/(7))"),
        ("orca_math", "Find the value of x in (x / 5) + 3 = 4.", "(x / 5) + 3 = 4"),
        ("orca_math", "solve for x and check : 25 x = 675", "25 x = 675"),
    ],
)
def test_detect_equation_positive(source, problem, equation):
    assert numina.detect_equation(problem, latex=source in numina.LATEX_SOURCES) == equation
    assert numina.classify_kind(source, problem) == numina.KIND_EQUATION


@pytest.mark.parametrize(
    ("source", "problem"),
    [
        ("math", "Solve $x + y = 3$ and $x - y = 1$."),  # hệ hai ẩn
        ("math", "Solve $x^3 = 8$."),  # bậc 3
        ("math", r"Solve $\frac{1}{x} = 2$."),  # ẩn ở mẫu
        ("math", r"Find $x$ if $\sqrt{x} = 4$."),  # căn
        ("math", "If $f(x) = 2x+1$, what is $f(3)$?"),
        ("synthetic_math", "Evaluate $x^2 + 1$ for $x = 3$."),  # đã gán sẵn, không phải đề giải
        ("math", "If $x = 2$, compute $3x$."),  # không có từ khóa giải ẩn
        ("orca_math", "find the value of y from 12 x 6 = y ?"),  # x là dấu nhân
        ("gsm8k", "Natalia sold clips to 48 of her friends in April. How many clips did she sell?"),
    ],
)
def test_detect_equation_negative(source, problem):
    assert numina.detect_equation(problem, latex=source in numina.LATEX_SOURCES) is None
    assert numina.classify_kind(source, problem) == numina.KIND_ARITHMETIC


@pytest.mark.parametrize(
    ("source", "problem", "symbolic"),
    [
        ("gsm8k", "John pays $20 for each of 3 doors. How much does he pay in total?", False),  # $ là tiền
        ("orca_math", "Ms. Lopez deposits $140 in an account that pays 20% interest.", False),
        ("orca_math", "If 3x + 5 = 20, what is 2 times the number?", True),
        ("math", r"A shirt costs \$15 and pants cost \$25. What is the total cost?", False),
        ("math", "Compute $\\frac{3}{4} \\cdot 12$.", False),
        ("math", "Triangle $ABC$ has sides 3, 4 and 5. What is its area?", True),
        ("synthetic_math", "Let $n$ be the smallest positive integer such that ...", True),
    ],
)
def test_is_symbolic(source, problem, symbolic):
    assert numina.is_symbolic(problem, latex=source in numina.LATEX_SOURCES) is symbolic


def test_screen_row_arithmetic_candidate():
    candidate = numina.screen_row(
        "gsm8k",
        "Tim sells 30 necklaces for $200 each. Each costs $150 to make. What is his profit?",
        r"Profit per necklace is 200 - 150 = 50, so total profit is 30 * 50 = \boxed{1500}.",
    )
    assert candidate == numina.Candidate(numina.KIND_ARITHMETIC, "1500", "1500", None)


def test_screen_row_equation_candidate_requires_full_solution_set():
    problem = "Find all solutions to $x^2 - 5x + 6 = 0$."
    candidate = numina.screen_row("math", problem, r"Factor: $(x-2)(x-3)=0$, so $x = \boxed{2, 3}$.")
    assert candidate.kind == numina.KIND_EQUATION
    assert candidate.answer == "{2, 3}"
    assert math_check.answer_matches(candidate.equation, candidate.answer)
    with pytest.raises(numina.AnswerRejected) as caught:
        numina.screen_row("math", problem, r"The larger root is $\boxed{3}$.")
    assert caught.value.reason == "equation_answer_mismatch"


@pytest.mark.parametrize(
    ("source", "problem", "solution", "reason"),
    [
        ("cn_k12", "Compute 1 + 1.", r"\boxed{2}", "source_excluded"),
        ("math", "Find the area. [asy] draw((0,0)--(1,1)); [/asy]", r"\boxed{2}", "figure"),
        ("gsm8k", "How many apples?", "There are 5 apples.", "no_boxed"),
        ("gsm8k", "How many apples?", r"\boxed{5", "malformed_boxed"),
        ("gsm8k", "How many apples and pears?", r"\boxed{5} apples and \boxed{7} pears", "multiple_distinct_boxes"),
        ("math", "Triangle $ABC$ has sides 3, 4, 5. Find its area.", r"\boxed{6}", "symbolic_problem"),
        ("orca_math", "What is the ratio of boys to girls?", r"\boxed{2:1}", "non_numeric"),
        ("gsm8k", "", r"\boxed{1}", "empty_text"),
        (
            "orca_math",
            "My apologies, I misunderstood the instructions. Here's the harder problem: what is 2 + 3?",
            r"\boxed{5}",
            "generation_artifact",
        ),
    ],
)
def test_screen_row_drop_reasons(source, problem, solution, reason):
    with pytest.raises(numina.AnswerRejected) as caught:
        numina.screen_row(source, problem, solution)
    assert caught.value.reason == reason


def test_repeated_box_with_same_value_is_kept():
    candidate = numina.screen_row(
        "orca_math", "How much does she earn?", r"She earns \boxed{24} dollars. Answer: \boxed{24 \text{ dollars}}"
    )
    assert candidate.answer == "24"


def test_problem_key_ignores_case_and_whitespace():
    assert numina.problem_key("How many  apples?\n") == numina.problem_key("how many apples?")
    assert numina.problem_key("How many apples?") != numina.problem_key("How many pears?")


@pytest.mark.parametrize(
    ("sizes", "total", "expected"),
    [
        ({("a", "x"): 10, ("b", "x"): 100, ("c", "x"): 100}, 90, {("a", "x"): 10, ("b", "x"): 40, ("c", "x"): 40}),
        ({("a", "x"): 5, ("b", "x"): 5}, 100, {("a", "x"): 5, ("b", "x"): 5}),  # thiếu → lấy hết
        ({("a", "x"): 10, ("b", "x"): 10}, 5, {("a", "x"): 3, ("b", "x"): 2}),  # phần lẻ theo thứ tự khóa
        ({("a", "x"): 0, ("b", "x"): 10}, 4, {("a", "x"): 0, ("b", "x"): 4}),
    ],
)
def test_allocate_equal(sizes, total, expected):
    allocation = numina.allocate_equal(sizes, total)
    assert allocation == expected
    assert sum(allocation.values()) == min(total, sum(sizes.values()))


def test_allocate_equal_rejects_negative_total():
    with pytest.raises(ValueError):
        numina.allocate_equal({("a", "x"): 1}, -1)


@pytest.mark.parametrize(
    ("source", "problem", "equation", "variable"),
    [
        ("math", "Solve $2(y - 1) = 10$ for $y$.", "2(x - 1) = 10", "y"),
        ("synthetic_math", r"Solve for \( c \): \[\frac{c-23}{2} = \frac{2c +5}{7}.\]", "((x-23)/(2)) = ((2x +5)/(7))", "c"),
        ("orca_math", "solve for x and check : 25 x = 675", "25 x = 675", "x"),
    ],
)
def test_detect_equation_keeps_the_original_variable(source, problem, equation, variable):
    latex = source in numina.LATEX_SOURCES
    assert numina.detect_equation_variable(problem, latex=latex) == (equation, variable)
    assert numina.detect_equation(problem, latex=latex) == equation


def test_screen_row_records_the_original_variable_for_equations_only():
    candidate = numina.screen_row("math", "What is the value of $m$ in $m + 4 = 9$?", r"$m = \boxed{5}$")
    assert (candidate.kind, candidate.equation, candidate.variable) == (numina.KIND_EQUATION, "x + 4 = 9", "m")
    arithmetic = numina.screen_row("gsm8k", "Tim has 3 boxes of 4 pens. How many pens?", r"3 * 4 = \boxed{12}")
    assert arithmetic.variable is None


def test_arithmetic_sources_are_the_word_problem_sources():
    assert numina.ARITHMETIC_SOURCES == {"gsm8k", "orca_math"}
    assert numina.ARITHMETIC_SOURCES <= numina.ALLOWED_SOURCES
