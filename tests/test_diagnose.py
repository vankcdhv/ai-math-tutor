"""diagnose phải nhận đúng loại lỗi cho cả bài phương trình và bài số học."""

import json
from pathlib import Path

import pytest

from ai_math_tutor import math_check

LINEAR = {
    "problem_id": "T-LIN-001",
    "kind": "equation",
    "equation": "3*(x - 1) = 2*x + 5",
    "answer": "8",
    "steps": ["3*x - 3 = 2*x + 5", "x - 3 = 5", "x = 8"],
    "misconceptions": [
        {"type": "distribution_error", "step_index": 1, "expression": "3*x - 1 = 2*x + 5"},
        {"type": "wrong_operation", "step_index": 2, "expression": "5*x - 3 = 5"},
        {"type": "arithmetic_slip", "step_index": 3, "expression": "x = 9"},
    ],
}
QUADRATIC = {
    "problem_id": "T-QUAD-001",
    "kind": "equation",
    "equation": "x^2 - 5*x + 6 = 0",
    "answer": "{2, 3}",
    "steps": ["(x - 2)*(x - 3) = 0", "x = 2"],
    "misconceptions": [{"type": "wrong_factor", "step_index": 1, "expression": "(x - 1)*(x - 6) = 0"}],
}
ARITHMETIC = {
    "problem_id": "T-ARITH-001",
    "kind": "arithmetic",
    "answer": "80",
    "steps": ["48/2 = 24", "3*(24 + 6) = 90", "90 - 10 = 80"],
    "misconceptions": [
        {"type": "arithmetic_slip", "step_index": 1, "expression": "48/2 = 25"},
        {"type": "wrong_operation", "step_index": 1, "expression": "48*2 = 96"},
        {"type": "distribution_error", "step_index": 2, "expression": "3*24 + 6 = 78"},
        {"type": "wrong_factor", "step_index": 2, "expression": "2*(24 + 6) = 60"},
    ],
}


@pytest.mark.parametrize(
    ("problem", "expression", "step_index", "equivalent", "misconception"),
    [
        # Bài phương trình: tương đương theo tập nghiệm.
        (LINEAR, "3x - 3 = 2x + 5", 1, True, None),
        (LINEAR, "x = 8", None, True, None),
        (LINEAR, "8", None, True, None),  # đáp án trần
        (LINEAR, "3*x - 1 = 2*x + 5", 1, False, "distribution_error"),
        (LINEAR, "3*x = 2*x + 6", 1, False, "distribution_error"),  # cùng tập nghiệm với lỗi đã biết
        (LINEAR, "5*x - 3 = 5", 2, False, "wrong_operation"),
        (LINEAR, "x = 9", 3, False, "arithmetic_slip"),
        (LINEAR, "x = -8", None, False, "sign_error"),  # quy tắc chung: nghiệm đổi dấu
        (LINEAR, "x = 100", None, False, None),
        (QUADRATIC, "(x - 2)(x - 3) = 0", 1, True, None),
        (QUADRATIC, "{3, 2}", None, True, None),
        (QUADRATIC, "(x - 1)*(x - 6) = 0", 1, False, "wrong_factor"),
        (QUADRATIC, "(x - 2)*(x + 3) = 0", 1, False, "sign_error"),  # đổi dấu một nghiệm
        (QUADRATIC, "(x + 2)*(x + 3) = 0", 1, False, "sign_error"),
        (QUADRATIC, "x = 3", 2, False, "missing_root"),
        (QUADRATIC, "2", None, False, "missing_root"),
        (QUADRATIC, "x^2 = -1", None, False, None),  # vô nghiệm không phải "thiếu nghiệm"
        # Bài số học: tính đúng và nằm trên đường giải mẫu.
        (ARITHMETIC, "48/2 = 24", 1, True, None),
        (ARITHMETIC, "3*(24 + 6) = 90", None, True, None),
        (ARITHMETIC, "80", None, True, None),
        (ARITHMETIC, "48/2 = 25", 1, False, "arithmetic_slip"),
        (ARITHMETIC, "48/2 = 26", 1, False, "arithmetic_slip"),  # quy tắc chung: tính sai
        (ARITHMETIC, "48*2 = 96", 1, False, "wrong_operation"),
        (ARITHMETIC, "48 + 2 = 50", 1, False, "wrong_operation"),  # quy tắc chung: tính đúng, sai phép
        (ARITHMETIC, "90 - 10 = 80", 1, False, "wrong_operation"),  # đúng nhưng không phải bước này
        (ARITHMETIC, "3*24 + 6 = 78", 2, False, "distribution_error"),
        (ARITHMETIC, "2*(24 + 6) = 60", 2, False, "wrong_factor"),
        (ARITHMETIC, "10 - 90 = -80", 3, False, "sign_error"),  # quy tắc chung: số đối
        (ARITHMETIC, "-80", None, False, "sign_error"),
        (ARITHMETIC, "81", None, False, None),
    ],
)
def test_diagnose_cases(problem, expression, step_index, equivalent, misconception):
    assert math_check.diagnose(problem, expression, step_index) == {
        "equivalent": equivalent,
        "misconception": misconception,
    }


@pytest.mark.parametrize("misconception_type", math_check.MISCONCEPTION_TYPES)
@pytest.mark.parametrize(
    ("base", "wrong_expression"),
    [
        ({"kind": "equation", "equation": "2*x + 3 = 11", "answer": "4", "steps": ["2*x = 8", "x = 4"]}, "2*x = 14"),
        ({"kind": "arithmetic", "answer": "30", "steps": ["48/2 = 24", "24 + 6 = 30"]}, "24 + 6 = 31"),
    ],
    ids=["equation", "arithmetic"],
)
def test_every_known_misconception_type_is_reported(base, wrong_expression, misconception_type):
    problem = {
        "problem_id": "T-ANY",
        **base,
        "misconceptions": [{"type": misconception_type, "step_index": 2, "expression": wrong_expression}],
    }
    assert math_check.diagnose(problem, wrong_expression, 2) == {"equivalent": False, "misconception": misconception_type}


def test_misconception_on_matching_step_takes_priority():
    problem = {
        "kind": "arithmetic",
        "answer": "30",
        "steps": ["48/2 = 24", "24 + 6 = 30"],
        "misconceptions": [
            {"type": "wrong_operation", "step_index": 1, "expression": "48/2 = 25"},
            {"type": "arithmetic_slip", "step_index": 2, "expression": "48/2 = 25"},
        ],
    }
    assert math_check.diagnose(problem, "48/2 = 25", 2)["misconception"] == "arithmetic_slip"
    assert math_check.diagnose(problem, "48/2 = 25", 1)["misconception"] == "wrong_operation"


def test_week1_problems_without_kind_are_equations():
    path = Path(__file__).resolve().parents[1] / "data_sample" / "problems-week1-sample.jsonl"
    problems = {p["problem_id"]: p for p in map(json.loads, path.read_text().splitlines())}
    assert math_check.diagnose(problems["P-QUAD-001"], "(x - 2)*(x + 3) = 0")["misconception"] == "sign_error"
    assert math_check.diagnose(problems["P-QUAD-001"], "2")["misconception"] == "missing_root"
    assert math_check.diagnose(problems["P-LIN-002"], "x = 8")["equivalent"] is True


@pytest.mark.parametrize(
    ("problem", "expression", "step_index"),
    [
        (ARITHMETIC, "2*x = 48", 1),  # bài số học không nhận bước có ẩn
        (ARITHMETIC, "48/2 = 24", 4),  # step_index ngoài phạm vi (đánh số từ 1)
        (ARITHMETIC, "48/2 = 24", 0),
        (LINEAR, "9^9^9 = x", None),
        ({"kind": "geometry", "answer": "1"}, "x = 1", None),
        ({"kind": "equation", "answer": "1"}, "x = 1", None),  # thiếu equation
        ({**LINEAR, "misconceptions": [{"type": "typo", "expression": "x = 1"}]}, "x = 2", None),
    ],
)
def test_invalid_input_or_problem_raises(problem, expression, step_index):
    with pytest.raises(math_check.MathInputError):
        math_check.diagnose(problem, expression, step_index)
