"""Bước số học của bài lời văn: vế trái phải tính chính xác ra vế phải."""

import pytest

from ai_math_tutor import math_check


@pytest.mark.parametrize(
    "step",
    [
        "48/2 = 24",
        "0.5*8 = 4",
        "0.1 + 0.2 = 0.3",  # số thập phân là phân số chính xác, không có sai số dấu phẩy động
        "1/3 + 1/6 = 0.5",
        "3*(4 + 2) = 18",
        "3(4 + 2) = 18",
        "2^3 = 8",
        "2^-1 = 0.5",
        "-3 - 4 = -7",
        "6*4 = 48/2",
        "1.25 * 4 = 5",
        "(12 - 2) / 4 = 2.5",
    ],
)
def test_correct_arithmetic_steps(step):
    assert math_check.arith_step(step) is True


@pytest.mark.parametrize(
    "step",
    ["48/2 = 25", "1/3 = 0.333", "2^3 = 6", "0.1 + 0.2 = 0.31", "3*(4 + 2) = 14", "7 - 10 = 3"],
)
def test_wrong_arithmetic_steps(step):
    assert math_check.arith_step(step) is False


@pytest.mark.parametrize(
    "invalid",
    ["2*x = 4", "48/0 = 1", "48/2", "48/2 = 24 = 24", "9^9^9 = 1", "2^(2^10) = 4", "1234567890123456 = 1", "abs(-1) = 1"],
)
def test_invalid_arithmetic_steps_rejected(invalid):
    with pytest.raises(math_check.MathInputError):
        math_check.arith_step(invalid)
