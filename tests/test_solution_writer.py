"""Viết lời giải từng bước: tách phép tính từ CoT, sinh bước phương trình, misconception khứ hồi diagnose."""

from __future__ import annotations

from fractions import Fraction
from pathlib import Path

import pytest
import sympy
import yaml

from ai_math_tutor import math_check
from ai_math_tutor import solution_writer as sw

ROOT = Path(__file__).resolve().parents[1]


def _round_trips(record: dict, items) -> bool:
    probe = {**record, "misconceptions": [dict(m) for m in items]}
    return all(sw.misconception_round_trips(probe, m) for m in probe["misconceptions"])


# ---------------------------------------------------------------------------
# Chuẩn hóa văn bản CoT
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("line", "expected"),
    [
        (r"$4 \times 20 = 80$ stamps", "4 * 20 = 80 stamps"),
        (r"\[180 \times \frac{1}{4} = 45 \text{ stamps}\]", "180 * ((1)/(4)) = 45"),
        (r"\$83,200", "83200"),
        (r"$12{,}000 \div 4 = 3{,}000$", "12000 / 4 = 3000"),
        ("20% of 50", "(20/100) of 50"),
        (r"\boxed{135}", "135"),
        ("She earns $.1 per word", "She earns 0.1 per word"),
    ],
)
def test_normalize_cot_line(line: str, expected: str) -> None:
    assert " ".join(sw.normalize_cot_line(line).split()) == expected


def test_unknown_latex_command_poisons_only_its_segment() -> None:
    assert "@" in sw.normalize_cot_line(r"\sqrt{16} = 4")


# ---------------------------------------------------------------------------
# Tách bước số học từ CoT
# ---------------------------------------------------------------------------

GSM_STATEMENT = ("Marie has 4 notebooks with 20 stamps each. She also has two binders with 50 stamps each. "
                 "If she decides to only keep 1/4 of the stamps, how many stamps can she give away?")
GSM_SOLUTION = r"""First, let's calculate the total number of stamps Marie has:

- From notebooks: $4 \times 20 = 80$ stamps
- From binders: $2 \times 50 = 100$ stamps

Adding these together gives the total number of stamps:

\[80 + 100 = 180 \text{ stamps}\]

Marie keeps only $1/4$ of her stamps:

\[180 \times \frac{1}{4} = 45 \text{ stamps}\]

To find out how many stamps she can give away, we subtract:

\[180 - 45 = 135 \text{ stamps}\]

Therefore, Marie can give away $\boxed{135}$ stamps."""


def test_extract_steps_from_latex_cot() -> None:
    steps = sw.extract_arith_steps(GSM_SOLUTION, "135")
    assert steps == ("4 * 20 = 80", "2 * 50 = 100", "80 + 100 = 180", "180 * (1 / 4) = 45", "180 - 45 = 135")
    assert all(math_check.arith_step(step) for step in steps)


def test_plain_text_lines_with_same_label_form_one_chain() -> None:
    solution = ("Relative speed = 75 km/h + 65 km/h\nRelative speed = 140 km/h\n"
                "Time = 280 km / 140 km/h\nTime = 2 hours\nSo the answer is \\boxed{2}.")
    assert sw.extract_arith_steps(solution, "2") == ("75 + 65 = 140", "280 / 140 = 2")


def test_chain_is_cut_after_the_step_that_reaches_the_answer() -> None:
    solution = "$3 \\times 4 = 12$, then $12 + 8 = 20$. Check: $20 - 8 = 12$."
    assert sw.extract_arith_steps(solution, "20") == ("3 * 4 = 12", "12 + 8 = 20")


def test_approximate_answer_is_dropped_as_rounding_dependent() -> None:
    solution = "Speed = 1400 / 36 = 38.89 m/s (approximately). Distance = 38.89 * 7.35 \\approx 285.84 m"
    with pytest.raises(sw.SolutionRejected) as info:
        sw.extract_arith_steps(solution, "285.84")
    assert info.value.reason == "rounding_dependent"


def test_answer_not_reached_and_no_steps() -> None:
    with pytest.raises(sw.SolutionRejected) as info:
        sw.extract_arith_steps("$2 \\times 3 = 6$ so the answer is 7.", "7")
    assert info.value.reason == "answer_not_reached"
    with pytest.raises(sw.SolutionRejected) as info:
        sw.extract_arith_steps("By symmetry the answer is $\\boxed{7}$.", "7")
    assert info.value.reason == "no_verifiable_steps"


def test_number_not_in_statement_or_earlier_step_is_incomplete_chain() -> None:
    statement = "Tom has 30 apples and buys 6 more. He keeps 18. How many did he give away?"
    with pytest.raises(sw.SolutionRejected) as info:
        sw.grounded_arith_steps(statement, "He has $30 + 6 = 36$ and gave $36 - 17 = 19$ apples.", "19")
    assert info.value.reason == "incomplete_chain"
    assert sw.grounded_arith_steps(statement, "He has $30 + 6 = 36$ and gave $36 - 18 = 18$ apples.", "18") == (
        "30 + 6 = 36", "36 - 18 = 18")


def test_single_step_chain_is_too_few_steps() -> None:
    statement = "Tom has 30 apples and gives some away. He keeps 18. How many did he give away?"
    with pytest.raises(sw.SolutionRejected) as info:
        sw.grounded_arith_steps(statement, "He gave $30 - 18 = 12$ apples.", "12")
    assert info.value.reason == "too_few_steps"


def test_context_constants_need_matching_context() -> None:
    assert sw.ungrounded_numbers("He works 3 hours.", ["3 * 60 = 180"]) == []
    assert sw.ungrounded_numbers("He has 3 boxes.", ["3 * 60 = 180"]) == ["3 * 60 = 180: 60"]
    assert sw.ungrounded_numbers("A coat costs 80 and is 25% off.", ["80 * 0.25 = 20"]) == []


def test_dangling_intermediate_result_is_disconnected() -> None:
    statement = "Remove one number from 1, 5, 9, 10 to make pairs that sum to 10."
    with pytest.raises(sw.SolutionRejected) as info:
        sw.grounded_arith_steps(statement, "$10 - 1 = 9$ and $10 - 5 = 5$, so remove $\\boxed{5}$.", "5")
    assert info.value.reason == "disconnected_steps"
    assert sw.dangling_steps(["2 * 3 = 6", "6 + 1 = 7"]) == []
    assert sw.dangling_steps(["1 / 8 + 1 / 8 = 1/4", "1 - (1 / 4) = 3/4"]) == []


# ---------------------------------------------------------------------------
# Định dạng
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("value", "text"),
    [(Fraction(5), "5"), (Fraction(-7, 4), "-1.75"), (Fraction(1, 3), "1/3"), (sympy.Rational(3, 40), "0.075"),
     (Fraction(-1, 20), "-0.05")],
)
def test_format_number(value, text: str) -> None:
    assert sw.format_number(value) == text


def test_format_poly() -> None:
    R = sympy.Rational
    assert sw.format_poly([R(1), R(-5), R(6)]) == "x**2 - 5*x + 6"
    assert sw.format_poly([R(0), R(-1), R(0)]) == "-x"
    assert sw.format_poly([R(0), R(3, 2), R(-1, 4)]) == "3*x/2 - 0.25"
    assert sw.format_poly([R(0), R(0), R(0)]) == "0"


# ---------------------------------------------------------------------------
# Bước phương trình
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("equation", "steps"),
    [
        ("2*x + 3 = 11", ("2*x = 8", "x = 4")),
        ("5*(x - 2) = 3*x + 4", ("5*x - 10 = 3*x + 4", "2*x - 10 = 4", "2*x = 14", "x = 7")),
        ("(x + 1)/2 = 4", ("x + 1 = 8", "x = 7")),
        ("-20 = 16*x", ("x = -20 / 16", "x = -1.25")),
        ("3639 + 11.95 - x = 3054", ("-x + 3650.95 = 3054", "-x = -596.95", "x = 596.95")),
        # k·x = c: viết rõ phép chia/nhân nghịch đảo; hệ số thập phân giữ thập phân
        ("25 x = 675", ("x = 675 / 25", "x = 27")),
        ("- 200 x = 1600", ("x = 1600 / (-200)", "x = -8")),
        ("((1)/(2)) * ((1)/(7)) * x = 14", ("x/14 = 14", "x = 14 * 14", "x = 196")),
        ("((2)/(7)) * ((1)/(5)) * x = 4", ("2*x/35 = 4", "x = 4 * 35/2", "x = 70")),
        ("1.20x * 0.90 * 1.30 = 351", ("1.404*x = 351", "x = 351 / 1.404", "x = 250")),
        ("x + 1 = 3", ("x = 3 - 1", "x = 2")),
        ("+ 10x = 100", ("x = 100 / 10", "x = 10")),
        ("(10 - 2x)^2 = 4x^2", ("4*x**2 - 40*x + 100 = 4*x**2", "-40*x + 100 = 0", "-40*x = -100", "x = 2.5")),
        # x = biểu thức số: tính dần từng lượt
        ("x=((2023^2-2023)/(2023))", ("x = 2023 * (2023 - 1) / 2023", "x = 2023 - 1", "x = 2022")),
        ("289 + 2(17)(4) + 16 = x", ("x = (17 + 4)^2", "x = 21^2", "x = 441")),
        ("x = ((48)/(7 - ((3)/(8)) + ((4)/(9))))",
         ("x = 48 / (504/72 - 27/72 + 32/72)", "x = 48 / (509/72)", "x = 48 * 72/509", "x = 3456/509")),
        ("x = 1 / 3 + 1 / 4", ("x = 4/12 + 3/12", "x = 7/12")),
        ("((2)/(7)) / ((1)/(14)) = x", ("x = 2/7 * 14", "x = 4")),
        # bậc hai: dạng chuẩn → tách hạng tử giữa → nhóm → nhân tử
        ("x**2 - 5*x = -6", ("x**2 - 5*x + 6 = 0", "x**2 - 3*x - 2*x + 6 = 0", "x*(x - 3) - 2*(x - 3) = 0",
                             "(x - 2)*(x - 3) = 0")),
        ("x^2 - 5x + 6 = 0", ("x**2 - 3*x - 2*x + 6 = 0", "x*(x - 3) - 2*(x - 3) = 0", "(x - 2)*(x - 3) = 0")),
        ("x**2 + 6*x + 9 = 0", ("(x + 3)**2 = 0", "x + 3 = 0", "x = -3")),
        ("2*x**2 = 18", ("x**2 = 9", "x**2 - 9 = 0", "(x + 3)*(x - 3) = 0")),
        ("2*x**2 - 3*x - 2 = 0", ("2*x**2 - 4*x + x - 2 = 0", "2*x*(x - 2) + (x - 2) = 0", "(2*x + 1)*(x - 2) = 0")),
    ],
)
def test_equation_steps(equation: str, steps: tuple[str, ...]) -> None:
    assert sw.equation_steps(equation) == steps
    for step in steps:
        assert math_check.step_is_equivalent(equation, step)


def test_final_equation_step_follows_the_answer_form() -> None:
    assert sw.equation_steps("x = ((45)/(7-((3)/(4))))", "36/5")[-1] == "x = 36/5"
    assert sw.equation_steps("x = ((45)/(7-((3)/(4))))")[-1] == "x = 7.2"
    assert sw.equation_steps("x = ((45)/(7-((3)/(4))))", "36/5") == (
        "x = 45 / (28/4 - 3/4)", "x = 45 / (25/4)", "x = 45 * 4/25", "x = 36/5")


@pytest.mark.parametrize(("equation", "answer"), [("x = ((5)/(7))", "5/7"), ("(x) = 0", "0")])
def test_equation_already_solved_is_too_few_steps(equation: str, answer: str) -> None:
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_equation(equation, answer)
    assert info.value.reason == "too_few_steps"


def test_every_generated_equation_has_two_steps_and_round_trip_misconceptions() -> None:
    for equation, answer in [("x + 1 = 3", "2"), ("25 x = 675", "27"), ("x^2 - 2x + 1 = 0", "1"),
                             ("289 + 2(17)(4) + 16 = x", "441"), ("x=((2010^2-2010)/(2010^2))", "2009/2010")]:
        written = sw.write_equation(equation, answer)
        assert len(written.steps) >= sw.MIN_STEPS
        record = {"kind": "equation", "equation": equation, "answer": answer, "steps": list(written.steps)}
        assert _round_trips(record, written.misconceptions)


def test_irrational_roots_are_unsupported() -> None:
    with pytest.raises(sw.SolutionRejected) as info:
        sw.equation_steps("x**2 = 2")
    assert info.value.reason == "equation_unsupported"


def test_linear_equation_misconceptions_round_trip() -> None:
    written = sw.write_equation("3*(2*x - 1) = 4*(x + 2)", "11/2")
    assert written.solution_source == "generated_sympy"
    assert [m["type"] for m in written.misconceptions] == ["distribution_error", "sign_error"]
    record = {"kind": "equation", "equation": "3*(2*x - 1) = 4*(x + 2)", "answer": "11/2", "steps": list(written.steps)}
    assert _round_trips(record, written.misconceptions)
    assert written.skill_ids[0] == "linear_equation.distribute"
    assert "linear_equation.both_sides" in written.skill_ids


def test_quadratic_misconceptions_round_trip() -> None:
    written = sw.write_equation("x**2 - 7*x + 12 = 0", "{3, 4}")
    assert {m["type"] for m in written.misconceptions} == {"missing_root", "wrong_factor"}
    record = {"kind": "equation", "equation": "x**2 - 7*x + 12 = 0", "answer": "{3, 4}", "steps": list(written.steps)}
    assert _round_trips(record, written.misconceptions)
    assert written.skill_ids == ("quadratic_equation.factorization", "quadratic_equation.missing_root")


def test_square_root_form_skill() -> None:
    written = sw.write_equation("x**2 = 16", "{-4, 4}")
    assert written.skill_ids[0] == "quadratic_equation.square_root"


def test_arithmetic_misconceptions_round_trip() -> None:
    written = sw.write_arithmetic(GSM_STATEMENT, GSM_SOLUTION, "135")
    assert written.solution_source == "derived_from_cot"
    types = [m["type"] for m in written.misconceptions]
    assert types == ["wrong_operation", "arithmetic_slip"]
    first = written.misconceptions[0]
    assert first["expression"] == "4 + 20 = 24" and first["step_index"] == 1
    record = {"kind": "arithmetic", "answer": "135", "steps": list(written.steps)}
    assert _round_trips(record, written.misconceptions)
    assert "arithmetic.fractions" in written.skill_ids and written.skill_ids[-1] == "arithmetic.multi_step"


def test_reversed_subtraction_is_sign_error_candidate() -> None:
    candidates = sw._arith_candidates(["12 - 4 = 8"])
    sign = [c for c in candidates if c["type"] == "sign_error"]
    assert sign == [{"type": "sign_error", "step_index": 1, "expression": "4 - 12 = -8"}]
    record = {"kind": "arithmetic", "answer": "8", "steps": ["12 - 4 = 8"]}
    assert _round_trips(record, sign)


# ---------------------------------------------------------------------------
# Bộ kiểm toàn bài
# ---------------------------------------------------------------------------


def _good_arith() -> dict:
    return {"problem_id": "P-T-001", "kind": "arithmetic", "answer": "30", "steps": ["4 * 5 = 20", "20 + 10 = 30"],
            "misconceptions": [{"type": "wrong_operation", "step_index": 1, "expression": "4 + 5 = 9"}]}


def test_verify_record_accepts_good_record() -> None:
    assert sw.verify_record(_good_arith()) == []


@pytest.mark.parametrize(
    ("change", "fragment"),
    [
        ({"steps": ["4 * 5 = 21", "21 + 9 = 30"]}, "sai"),
        ({"answer": "31"}, "bước cuối"),
        ({"misconceptions": [{"type": "arithmetic_slip", "step_index": 1, "expression": "4 + 5 = 9"}]}, "mâu thuẫn"),
        ({"misconceptions": [{"type": "wrong_operation", "step_index": 1, "expression": "4 * 5 = 20"}]}, "diagnose"),
        ({"misconceptions": []}, "không có misconception"),
    ],
)
def test_verify_record_rejects(change: dict, fragment: str) -> None:
    errors = sw.verify_record({**_good_arith(), **change})
    assert errors and any(fragment in e for e in errors)


def test_verify_record_equation_last_step_must_give_answer_set() -> None:
    record = {"problem_id": "P-T-002", "kind": "equation", "equation": "x**2 - 5*x + 6 = 0", "answer": "{2, 3}",
              "steps": ["x - 2 = 0"], "misconceptions": [{"type": "missing_root", "step_index": 1, "expression": "x - 3 = 0"}]}
    assert any("không tương đương" in e for e in sw.verify_record(record))


def test_complete_authored_rejects_misconception_that_does_not_round_trip() -> None:
    record = {"problem_id": "P-SLIN-999", "kind": "equation", "equation": "2*x + 3 = 11", "answer": "4",
              "skill_ids": ["linear_equation.isolate_variable"], "difficulty": 1, "steps": ["2*x = 8", "x = 4"],
              "misconceptions": [{"type": "missing_root", "step_index": 1, "expression": "2*x = 8"}]}
    with pytest.raises(sw.SolutionRejected) as info:
        sw.complete_authored(record)
    assert info.value.reason == "verification_failed"


def test_complete_authored_generates_misconceptions_when_missing() -> None:
    record = {"problem_id": "P-SLIN-998", "kind": "equation", "equation": "2*x + 3 = 11", "answer": "4",
              "skill_ids": ["linear_equation.isolate_variable"], "difficulty": 1, "steps": ["2*x = 8", "x = 4"]}
    written = sw.complete_authored(record)
    assert written.solution_source == "authored" and written.misconceptions


def test_unknown_drop_reason_is_a_programming_error() -> None:
    with pytest.raises(ValueError):
        sw.SolutionRejected("because")


def test_assigned_skills_exist_in_skills_config() -> None:
    config = yaml.safe_load((ROOT / "configs" / "skills.yaml").read_text(encoding="utf-8"))
    known = {skill["skill_id"] for skill in config["skills"]}
    assert set(sw.USED_SKILLS) <= known


def test_constant_two_needs_context() -> None:
    assert sw.ungrounded_numbers("There are 14 cans.", ["14 / 2 = 7"]) == ["14 / 2 = 7: 2"]
    assert sw.ungrounded_numbers("She drinks half of the 14 cans.", ["14 / 2 = 7"]) == []
    assert sw.ungrounded_numbers(r"Compute $101^{4} - 1$.", ["100 ^ 4 = 100000000"]) == ["100 ^ 4 = 100000000: 100"]


def test_hand_written_numina_steps_are_verified_and_labelled() -> None:
    statement = "Ruth goes to school 8 hours a day and 5 days a week. She is in math class 25% of this time."
    written = sw.write_ai_arithmetic(statement, "10", ["8 * 5 = 40", "40 * 25 / 100 = 10"])
    assert written.solution_source == "authored"
    assert written.skill_ids[0] == "arithmetic.percent" and written.misconceptions
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_ai_arithmetic(statement, "10", ["8 * 5 = 45", "45 * 25 / 100 = 11.25"])
    assert info.value.reason == "verification_failed"


def test_hand_written_steps_pass_the_same_chain_filters_except_grounding() -> None:
    statement = "Arthur has $200 in his wallet. He spends four-fifths of that. How much does he have left?"
    # 4 và 5 viết bằng chữ trong đề: bước viết trực tiếp vẫn hợp lệ (không kiểm incomplete_chain)
    assert sw.write_ai_arithmetic(statement, "40", ["200 * 4 / 5 = 160", "200 - 160 = 40"]).steps
    tom = "Tom swims for 2 hours at a speed of 2 miles per hour. He then runs for half the time at 4 times the speed."
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_ai_arithmetic(tom, "12", ["2 * 2 = 4", "2 / 2 = 1", "4 * 2 = 8", "1 * 8 = 8", "4 + 8 = 12"])
    assert info.value.reason == "too_few_steps"  # có bước đồng nhất 1 * 8 = 8


# ---------------------------------------------------------------------------
# Nguồn bài số học, bước đồng nhất, bước tối thiểu
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("source", ["math", "synthetic_math"])
def test_arithmetic_from_competition_sources_is_excluded(source: str) -> None:
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_solution("arithmetic", GSM_STATEMENT, GSM_SOLUTION, "135", None, source=source)
    assert info.value.reason == "arithmetic_source_excluded"
    assert sw.write_solution("arithmetic", GSM_STATEMENT, GSM_SOLUTION, "135", None, source="gsm8k").steps


def test_equations_are_kept_from_every_source() -> None:
    written = sw.write_solution("equation", "Solve $3x - 7 = 11$.", "", "6", "3x - 7 = 11", source="synthetic_math")
    assert written.steps == ("3*x = 18", "x = 6")


@pytest.mark.parametrize(
    ("step", "identity"),
    [
        ("0 + 4458 = 4458", True), ("4458 - 0 = 4458", True), ("1 ^ 5 = 1", True), ("7 ^ 1 = 7", True),
        ("12 * 1 = 12", True), ("1 * 12 = 12", True), ("12 / 1 = 12", True), ("(5) = 5", True),
        ("12 / 4 = 3", False), ("4458 - 1561 = 2897", False), ("10 / 10 = 1", False), ("0 - 5 = -5", False),
        ("3 / 4 = 0.75", False),
    ],
)
def test_identity_steps(step: str, identity: bool) -> None:
    assert sw.is_identity_step(step) is identity


def test_identity_steps_are_dropped_from_cot_chains() -> None:
    solution = ("They had $0 + 4458 = 4458$ items. They sold 1561: $4458 - 1561 = 2897$. "
                "Adding the storeroom: $575 + 2897 = 3472$. So $\\boxed{3472}$.")
    assert sw.extract_arith_steps(solution, "3472") == ("4458 - 1561 = 2897", "575 + 2897 = 3472")


def test_identity_operation_inside_a_real_step_is_removed() -> None:
    assert sw.without_identity_operations("45 * (1000 / 1) * (1 / 3600) = 12.5") == "45 * 1000 * 1/3600 = 12.5"
    assert sw.without_identity_operations("45 * (1000 / 3600) = 12.5") == "45 * (1000 / 3600) = 12.5"
    assert math_check.arith_step("45 * 1000 * 1/3600 = 12.5")


# ---------------------------------------------------------------------------
# Số liệu của đề phải được dùng; kết quả không được dùng hai vai
# ---------------------------------------------------------------------------

GIRLS = ("In a classroom there are 30 students. 1/3 of them are boys. Among the girls, 40% own dogs, 20% own a "
         "cat, and the rest have no pets. How many girls in the class have no pet?")


def test_coincidental_reuse_of_a_result_is_unused_given() -> None:
    # 20 ở bước cuối là số bạn nữ (kết quả bước 2), không thể đồng thời là 20% của đề.
    with pytest.raises(sw.SolutionRejected) as info:
        sw.check_arith_chain(GIRLS, ["1 - (1 / 3) = 2/3", "30 * (2 / 3) = 20", "20 * 0.4 = 8"])
    assert info.value.reason == "unused_given"
    assert sw.check_arith_chain(GIRLS, ["1 - (1 / 3) = 2/3", "30 * (2 / 3) = 20", "100 - 40 - 20 = 40",
                                        "20 * 0.4 = 8"])


def test_percent_only_equation_is_kept_in_percent_units() -> None:
    assert " ".join(sw.normalize_cot_line(r"\[100\% - 40\% - 20\% = 40\%\]").split()) == "100 - 40 - 20 = 40"
    assert "(40/100)" in sw.normalize_cot_line("40% of 50 = 20")


def test_unused_given_quantity_is_rejected() -> None:
    statement = ("A man had 39 cows; 25 died and he sold 6. This year 24 were born and he bought 43 more. "
                 "His friend gave him some. He has 83 cows now. How many did the friend give?")
    with pytest.raises(sw.SolutionRejected) as info:
        sw.check_arith_chain(statement, ["39 - 25 = 14", "14 - 6 = 8"])
    assert info.value.reason == "unused_given" and "24" in info.value.detail


def test_multiplier_words_are_givens_but_counts_in_words_and_ones_are_not() -> None:
    labels = {label for label, _ in sw.given_quantities("He is twice as old as his 3 sisters and owns one dog.")}
    assert labels == {"twice", "3"}
    with pytest.raises(sw.SolutionRejected) as info:
        sw.check_arith_chain("p is 3 times as old as q. Later p will be twice as old as q. p was 30 years old 3 years "
                             "ago.", ["30 + 3 = 33", "33 / 3 = 11"])
    assert "twice" in info.value.detail


def test_integer_division_is_not_read_as_a_given_fraction() -> None:
    statement = "Grass grows 0.5 inches per month from 2 inches to 4 inches. He pays $300 per year."
    with pytest.raises(sw.SolutionRejected) as info:
        sw.check_arith_chain(statement, ["2 / 0.5 = 4", "12 / 4 = 3", "300 / 3 = 100"])
    assert info.value.reason == "unused_given"


def test_negative_intermediate_quantity_is_rejected() -> None:
    statement = "A man rows against the stream at 8 km/h. His rate is 5 km/h. What is his speed with the stream?"
    with pytest.raises(sw.SolutionRejected) as info:
        sw.check_arith_chain(statement, ["5 - 8 = -3", "5 + 3 = 8"])
    assert info.value.reason == "negative_quantity"


# ---------------------------------------------------------------------------
# Kiểm hợp lý trên CoT
# ---------------------------------------------------------------------------


def test_cot_final_value_includes_an_inline_box() -> None:
    assert sw.cot_final_value("So 24 / 6 = 4 doses. In a day: $4 \\times 750 = \\boxed{3000}$ mg.") == 3000
    assert sw.cot_final_value("Selling Price = Rs 60\n\nSo the price is $\\boxed{60}$.") == 60
    assert sw.cot_final_value("It is about 7.5, rounded to $\\boxed{8}$.") is None


def test_cot_that_ends_on_another_value_conflicts_with_the_box() -> None:
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_arithmetic("He buys 3 packs of 4 pens and gets 2 more. How many pens?",
                            "$3 \\times 4 = 12$, $12 + 2 = 14$. Check: $14 - 2 = 12$. So $\\boxed{14}$.", "14")
    assert info.value.reason == "cot_answer_conflict"


def test_false_equality_in_cot_is_an_arithmetic_error() -> None:
    solution = ("His speed back was \\( 60 + 12 = 72 \\) mph.\n\\[ 60t = 72 \\times 5 \\]\n"
                "\\[ t = \\frac{72 \\times 5}{60} \\]\n\\[ t = \\frac{72}{12} \\times 5 \\]\n\\[ t = 30 \\]\n"
                "So it took $\\boxed{30}$ hours.")
    assert sw.cot_false_equalities(solution) == ["(72 * 5) / 60 = (72 / 12) * 5"]
    statement = ("He returned in 5 hours, 12 mph faster than the 60 mph trip there. How many hours did the trip take?")
    with pytest.raises(sw.SolutionRejected) as info:
        sw.write_arithmetic(statement, solution, "30")
    assert info.value.reason == "cot_arithmetic_error"


def test_unit_conversions_and_percent_lines_are_not_false_equalities() -> None:
    assert sw.cot_false_equalities("1 gallon = 3.8 liters\n2 hours = 2 * 60 = 120 minutes") == []
    assert sw.cot_false_equalities("Share = 0.604 \\times 100\\% = 60.4\\%") == []
    assert sw.cot_false_equalities("\\[ \\frac{1}{2}P = \\frac{1}{2} \\times 2 = 1 \\]") == []


# ---------------------------------------------------------------------------
# Tính dần biểu thức số
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    ("expression", "steps", "value"),
    [
        ("((45)/(7-((3)/(4))))", ["45 / (28/4 - 3/4)", "45 / (25/4)", "45 * 4/25", "36/5"], Fraction(36, 5)),
        ("529 + 2(23)(3) + 9", ["(23 + 3)^2", "26^2", "676"], Fraction(676)),
        ("400 - 2(20)(5) + 25", ["(20 - 5)^2", "15^2", "225"], Fraction(225)),
        ("((2011^2 + 2011)/(2011))", ["2011 * (2011 + 1) / 2011", "2011 + 1", "2012"], Fraction(2012)),
        ("((45^2)/(7 - ((3)/(4))))", ["2025 / (28/4 - 3/4)", "2025 / (25/4)", "2025 * 4/25", "324"], Fraction(324)),
        ("5/7", [], Fraction(5, 7)),
    ],
)
def test_evaluation_steps(expression: str, steps: list[str], value: Fraction) -> None:
    assert sw.evaluation_steps(expression) == (steps, value)
    for step in steps:
        assert math_check.arith_step(f"{step} = {sw.format_number(value)}")


def test_evaluation_steps_reject_letters_and_division_by_zero() -> None:
    with pytest.raises(ValueError):
        sw.evaluation_steps("2*y + 1")
    with pytest.raises(ValueError):
        sw.evaluation_steps("5 / (3 - 3)")
