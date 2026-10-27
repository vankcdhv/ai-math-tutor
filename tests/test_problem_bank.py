"""Lược đồ kho bài và kho mini dùng cho bộ mô phỏng: cấu trúc chặt, mọi bước/misconception đúng toán."""

import json
from pathlib import Path

import pytest
import yaml

from ai_math_tutor.problem_bank import (
    ProblemBank,
    ProblemBankError,
    file_checksum,
    load_problems,
    parse_problem,
    verify_problem,
)

ROOT = Path(__file__).resolve().parents[1]
MINI_BANK = ROOT / "data_sample" / "problem-bank-mini.jsonl"
SKILLS = ROOT / "configs" / "skills.yaml"

VALID = {
    "problem_id": "P-TEST-001",
    "source": "self",
    "statement": "Solve 2x + 3 = 11.",
    "kind": "equation",
    "equation": "2*x + 3 = 11",
    "answer": "4",
    "skill_ids": ["linear_equation.isolate_variable"],
    "steps": ["2*x = 8", "x = 4"],
    "misconceptions": [{"type": "sign_error", "step_index": 1, "expression": "2*x = 14"}],
    "difficulty": 1,
    "split": "dev",
    "solution_source": "authored",
}


def test_mini_bank_covers_linear_and_quadratic_equations():
    problems = load_problems(MINI_BANK)
    assert len(problems) >= 8
    assert all(p.kind == "equation" and p.misconceptions for p in problems)
    families = {s.split(".")[0] for p in problems for s in p.skill_ids}
    assert {"linear_equation", "quadratic_equation"} <= families


@pytest.mark.parametrize("problem", load_problems(MINI_BANK), ids=lambda p: p.problem_id)
def test_mini_bank_problem_is_mathematically_verified(problem):
    assert verify_problem(problem) == []


def test_mini_bank_skills_have_simulator_parameters():
    configured = {s["skill_id"] for s in yaml.safe_load(SKILLS.read_text(encoding="utf-8"))["skills"]}
    used = {s for p in load_problems(MINI_BANK) for s in p.skill_ids}
    assert used <= configured


def test_roundtrip_through_dict():
    problem = parse_problem(VALID)
    assert parse_problem(problem.to_dict()) == problem
    assert problem.primary_skill == "linear_equation.isolate_variable"


@pytest.mark.parametrize(
    "change",
    [
        {"problem_id": "P-TEST-1"},
        {"kind": "geometry"},
        {"equation": "2*x + 3"},
        {"source": "web"},
        {"skill_ids": ["Linear.Isolate"]},
        {"skill_ids": []},
        {"steps": []},
        {"misconceptions": []},
        {"misconceptions": [{"type": "sign_error", "step_index": 3, "expression": "x = 1"}]},
        {"misconceptions": [{"type": "typo", "step_index": 1, "expression": "x = 1"}]},
        {"misconceptions": [{"type": "sign_error", "step_index": 1}]},
        {"difficulty": 0},
        {"difficulty": True},
        {"split": "train"},
        {"solution_source": "llm"},
        {"extra_field": 1},
    ],
)
def test_schema_violations_rejected(change):
    with pytest.raises(ProblemBankError):
        parse_problem({**VALID, **change})


def test_missing_field_and_arithmetic_with_equation_rejected():
    without_answer = {k: v for k, v in VALID.items() if k != "answer"}
    with pytest.raises(ProblemBankError, match="thiếu"):
        parse_problem(without_answer)
    with pytest.raises(ProblemBankError):
        parse_problem({**VALID, "kind": "arithmetic"})


def test_load_reports_line_number_and_duplicates_rejected(tmp_path):
    path = tmp_path / "bank.jsonl"
    path.write_text(json.dumps(VALID) + "\n" + json.dumps({**VALID, "difficulty": 9}) + "\n", encoding="utf-8")
    with pytest.raises(ProblemBankError, match=":2:"):
        load_problems(path)
    with pytest.raises(ProblemBankError, match="trùng"):
        ProblemBank([parse_problem(VALID), parse_problem(VALID)])


def test_split_selection_and_primary_skill_index():
    bank = ProblemBank([
        parse_problem(VALID),
        parse_problem({**VALID, "problem_id": "P-TEST-002", "split": "test",
                       "skill_ids": ["linear_equation.distribute", "linear_equation.isolate_variable"]}),
    ])
    assert [p.problem_id for p in bank.select("dev").problems] == ["P-TEST-001"]
    assert len(bank.select(None)) == 2
    assert bank.primary_skills() == ("linear_equation.isolate_variable", "linear_equation.distribute")
    assert [p.problem_id for p in bank.by_primary_skill("linear_equation.distribute")] == ["P-TEST-002"]
    assert bank.get("P-TEST-002").split == "test"
    with pytest.raises(ProblemBankError):
        bank.select("train")
    with pytest.raises(ProblemBankError):
        bank.get("P-NONE-001")


def test_verify_flags_wrong_steps_answers_and_correct_misconceptions():
    broken = parse_problem({
        **VALID,
        "answer": "5",
        "steps": ["2*x = 9", "x = 4"],
        "misconceptions": [{"type": "sign_error", "step_index": 2, "expression": "x = 8/2"}],
    })
    errors = verify_problem(broken)
    assert any("đáp án" in e for e in errors)
    assert any("bước 1" in e for e in errors)
    assert any("misconception" in e for e in errors)


@pytest.mark.parametrize("source", ["authored", "derived_from_cot", "generated_sympy"])
def test_all_solution_provenance_values_accepted(source):
    assert parse_problem({**VALID, "solution_source": source}).solution_source == source


ARITHMETIC = {
    **{k: v for k, v in VALID.items() if k != "equation"},
    "problem_id": "P-TEST-010",
    "statement": "48 apples are shared equally by 2 children; one child eats 5. How many are left for that child?",
    "kind": "arithmetic",
    "answer": "19",
    "skill_ids": ["arithmetic.multi_step"],
    "steps": ["48 / 2 = 24", "24 - 5 = 19"],
}


def test_arithmetically_valid_wrong_operation_and_reversed_subtraction_are_accepted():
    problem = parse_problem({**ARITHMETIC, "misconceptions": [
        {"type": "wrong_operation", "step_index": 1, "expression": "48 * 2 = 96"},
        {"type": "sign_error", "step_index": 2, "expression": "5 - 24 = -19"},
    ]})
    assert verify_problem(problem) == []


@pytest.mark.parametrize(
    "misconceptions",
    [
        # Biểu thức "lỗi" thực ra là bước đúng.
        [{"type": "wrong_operation", "step_index": 1, "expression": "48 / 2 = 24"}],
        # Cùng giá trị khai hai loại lỗi khác nhau: diagnose chỉ trả một loại ⇒ mục sau sai nhãn.
        [{"type": "wrong_operation", "step_index": 1, "expression": "48 * 2 = 96"},
         {"type": "arithmetic_slip", "step_index": 1, "expression": "2 * 48 = 96"}],
    ],
)
def test_mislabelled_arithmetic_misconception_fails(misconceptions):
    errors = verify_problem(parse_problem({**ARITHMETIC, "misconceptions": misconceptions}))
    assert len(errors) == 1 and "misconception" in errors[0]


def test_mislabelled_equation_misconception_fails():
    problem = parse_problem({**VALID, "misconceptions": [
        {"type": "sign_error", "step_index": 1, "expression": "2*x = 14"},
        {"type": "arithmetic_slip", "step_index": 1, "expression": "x = 7"},
    ]})
    errors = verify_problem(problem)
    assert len(errors) == 1 and "'arithmetic_slip'" in errors[0] and "'sign_error'" in errors[0]


def test_checksum_is_stable_and_content_sensitive(tmp_path):
    a, b = tmp_path / "a.jsonl", tmp_path / "b.jsonl"
    a.write_text(json.dumps(VALID) + "\n", encoding="utf-8")
    b.write_text(json.dumps({**VALID, "difficulty": 2}) + "\n", encoding="utf-8")
    assert file_checksum(a) == file_checksum(a)
    assert file_checksum(a) != file_checksum(b)
    assert file_checksum(MINI_BANK).startswith("sha256:")


def test_variable_field_is_for_equations_and_only_changes_display():
    from ai_math_tutor.problem_bank import display_expression

    row = {**VALID, "variable": "m", "steps": ["2*x = 8", "x = 4"]}
    problem = parse_problem(row)
    assert problem.variable == "m" and problem.display_variable == "m"
    assert problem.display_steps == ("2*m = 8", "m = 4")
    assert problem.steps == ("2*x = 8", "x = 4")  # chuỗi máy giữ ẩn x
    assert verify_problem(problem) == []
    assert parse_problem(problem.to_dict()) == problem
    assert display_expression("3*x**2 - x = 0", "y") == "3*y^2 - y = 0"
    assert display_expression("x = 4", None) == "x = 4"
    assert parse_problem(VALID).display_variable == "x"


@pytest.mark.parametrize("variable", ["e", "i", "xy", "M", 3, ""])
def test_invalid_variable_is_rejected(variable):
    with pytest.raises(ProblemBankError):
        parse_problem({**VALID, "variable": variable})


def test_variable_on_arithmetic_problem_is_rejected():
    arithmetic = {**VALID, "kind": "arithmetic", "answer": "30", "steps": ["4 * 5 = 20", "20 + 10 = 30"],
                  "misconceptions": [{"type": "wrong_operation", "step_index": 1, "expression": "4 + 5 = 9"}],
                  "skill_ids": ["arithmetic.multi_step"], "variable": "m"}
    arithmetic.pop("equation")
    with pytest.raises(ProblemBankError, match="variable"):
        parse_problem(arithmetic)
