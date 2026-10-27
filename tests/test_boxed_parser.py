import pytest

from ai_math_tutor import numina


@pytest.mark.parametrize(
    ("solution", "expected"),
    [
        (r"So the answer is $\boxed{24}$.", "24"),
        (r"Thus $x = \boxed{\frac{3}{4}}$", r"\frac{3}{4}"),
        (r"\boxed{\dfrac{1}{2}}", r"\dfrac{1}{2}"),
        (r"\boxed{x^{2} + \frac{1}{x^{2}}}", r"x^{2} + \frac{1}{x^{2}}"),  # ngoặc lồng nhiều tầng
        (r"\boxed{\{1, 2\}}", r"\{1, 2\}"),  # \{ \} không tính vào độ sâu
        (r"\boxed {7}", "7"),  # khoảng trắng trước ngoặc
        (r"\fbox{9}", "9"),
        (r"\boxed{\boxed{5}}", r"\boxed{5}"),  # hộp lồng: chỉ tính hộp ngoài
        (r"\boxed{\text{5 dollars}}", r"\text{5 dollars}"),
    ],
)
def test_last_boxed_extracts_balanced_content(solution, expected):
    assert numina.last_boxed(solution) == expected


def test_multiple_boxes_returns_last():
    solution = r"First $\boxed{12}$ apples, then the total is $\boxed{\frac{48}{2}}$."
    assert numina.boxed_answers(solution) == ["12", r"\frac{48}{2}"]
    assert numina.last_boxed(solution) == r"\frac{48}{2}"


@pytest.mark.parametrize("solution", ["The answer is 24.", "", r"\boxedfoo{3}", r"\textbf{24}"])
def test_missing_box_returns_none(solution):
    assert numina.last_boxed(solution) is None


@pytest.mark.parametrize("solution", [r"\boxed{\frac{1}{2}", r"answer \boxed 5", r"\boxed\boxed{3}", r"\boxed"])
def test_malformed_box_is_rejected(solution):
    with pytest.raises(numina.AnswerRejected) as caught:
        numina.last_boxed(solution)
    assert caught.value.reason == "malformed_boxed"


def test_unknown_drop_reason_is_a_programming_error():
    with pytest.raises(ValueError):
        numina.AnswerRejected("not_a_reason")
