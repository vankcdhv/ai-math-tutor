import time

import pytest
import sympy

from ai_math_tutor import math_check


@pytest.mark.parametrize(
    ("step", "expected"),
    [
        ("3*x - 3 = 2*x + 5", True),
        ("x = 8", True),
        ("3x - 3 = 2x + 5", True),  # nhân ngầm định
        ("3*x - 1 = 2*x + 5", False),  # lỗi phân phối
        ("x = -8", False),
    ],
)
def test_step_equivalence(step, expected):
    assert math_check.step_is_equivalent("3*(x - 1) = 2*x + 5", step) is expected


def test_answer_requires_complete_solution_set():
    assert math_check.answer_matches("x**2 - 5*x + 6 = 0", "{3, 2}")
    assert not math_check.answer_matches("x**2 - 5*x + 6 = 0", "2")
    assert math_check.answer_matches("x^2 - 5x + 6 = 0", "{2, 3}")


@pytest.mark.parametrize(
    "malicious",
    ["__import__('os').system('id') = 0", "x = 1; import os", "exec('1') = 1", "y = 2", "x = 1 = 2", "x" * 300 + " = 1"],
)
def test_unsafe_or_invalid_input_rejected_before_parsing(malicious):
    with pytest.raises(math_check.MathInputError):
        math_check.solution_set(malicious)


def test_answer_must_be_numeric():
    with pytest.raises(math_check.MathInputError):
        math_check.parse_answer("{x, 3}")


def _rejected_quickly(text: str, limit_s: float = 0.05) -> str:
    math_check.solution_set("x = 1")  # nạp sẵn parser, không tính vào thời gian
    start = time.perf_counter()
    with pytest.raises(math_check.MathInputError) as info:
        math_check.solution_set(text)
    elapsed = time.perf_counter() - start
    assert elapsed < limit_s, f"{text[:40]!r} bị từ chối sau {elapsed * 1000:.1f} ms"
    return str(info.value)


@pytest.mark.parametrize(
    ("attack", "reason"),
    [
        ("9^9^9 = x", "số mũ"),  # tháp lũy thừa: trước đây bị tính ngay lúc parse và treo
        ("9**9**9 = x", "số mũ"),
        ("2^(2^10) = x", "số mũ"),
        ("x^100 = 1", "số mũ"),
        ("x^-11 = 1", "số mũ"),
        ("x^(1/2) = 2", "số mũ"),
        ("(x^2)^5 = 1", "lồng nhau"),
        ("x^3 = 1", "bậc"),
        ("x*x*x = 1", "bậc"),
        ("x^2/(x + 1) = 1", "bậc"),
        ("(" * 30 + "x" + ")" * 30 + " = 1", "ngoặc"),
        ("1+2*(" * 10 + "x" + ")" * 10 + " = 1", "sâu"),
        ("x" + "-1" * 99 + " = 0", "nút"),
        ("1+" * 150 + "1 = x", "không hợp lệ"),
        ("x = 1234567890123456", "chữ số"),
        ("x = 0.0000000000000001", "chữ số"),
    ],
)
def test_expensive_expressions_rejected_before_evaluation(attack, reason):
    assert reason in _rejected_quickly(attack)


@pytest.mark.parametrize(
    "malformed",
    ["x = 1 +", "x = (", "x = )(", "x = 7//2", "x = 1.2.3", "x = 1 000", "x/0 = 1", "1/(x - x) = 1", "(x)(1, 2) = 1"],
)
def test_malformed_input_raises_math_input_error(malformed):
    _rejected_quickly(malformed)


@pytest.mark.parametrize(
    ("equation", "solutions"),
    [
        ("1/x^2 = 4", {sympy.Rational(-1, 2), sympy.Rational(1, 2)}),
        ("(x - 1/2)^2 = 1/4", {0, 1}),  # phép chia trong cơ số không phải lũy thừa lồng nhau
        ("x^-1 = 2", {sympy.Rational(1, 2)}),
        ("2^10*x = 1024", {1}),
        ("0.5*x = 1.25", {sympy.Rational(5, 2)}),
        ("x(x - 5) = -6", {2, 3}),
    ],
)
def test_bounded_expressions_still_solved_exactly(equation, solutions):
    assert math_check.solution_set(equation) == sympy.FiniteSet(*solutions)


def test_decimal_answers_compare_exactly():
    assert math_check.answer_matches("2*x = 1", "0.5")
    assert math_check.answer_matches("10*x = 1", "0.1")
