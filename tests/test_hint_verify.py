"""Kiểm chứng gợi ý cá nhân hóa: sự thật đã kiểm của bài, phát hiện lộ đáp án, từ chối bản nháp sai."""

from __future__ import annotations

from fractions import Fraction

import pytest

from ai_math_tutor.hints.verify import (
    CLAIM_EXAMPLE,
    CLAIM_STEP,
    ROLE_EXAMPLE,
    ROLE_FORM,
    ROLE_GENERIC,
    ROLE_OWN,
    Claim,
    HintDraft,
    Part,
    build_facts,
    mask_step,
    number_values,
    verify_draft,
)
from ai_math_tutor.problem_bank import ProblemBank, parse_problem


def _row(problem_id: str, kind: str, split: str, answer: str, steps: list[str], misconceptions: list[dict],
         skill: str, statement: str, equation: str | None = None, variable: str | None = None) -> dict:
    row = {
        "problem_id": problem_id, "source": "self", "statement": statement, "kind": kind, "answer": answer,
        "skill_ids": [skill], "steps": steps, "misconceptions": misconceptions, "difficulty": 2,
        "split": split, "solution_source": "authored",
    }
    if equation is not None:
        row["equation"] = equation
    if variable is not None:
        row["variable"] = variable
    return row


def synthetic_rows() -> list[dict]:
    """Kho bài nhỏ, viết trực tiếp: bài test của người học và bài dev làm bài tương tự."""
    iso = "linear_equation.isolate_variable"
    return [
        _row("P-TLIN-001", "equation", "test", "4", ["2*x = 8", "x = 4"],
             [{"type": "sign_error", "step_index": 1, "expression": "2*x = 14"}],
             iso, "Giải phương trình 2x + 3 = 11.", "2*x + 3 = 11"),
        _row("P-TLIN-002", "equation", "test", "-66", ["5*x - 75 = 6*x - 9", "-x - 75 = -9", "-x = 66", "x = -66"],
             [{"type": "distribution_error", "step_index": 1, "expression": "5*x - 225 = 6*x - 9"}],
             "linear_equation.distribute", "Giải phương trình (x - 15)/3 = (2x - 3)/5.", "(x - 15)/3 = (2*x - 3)/5"),
        _row("P-TQUAD-001", "equation", "test", "{0, 3}", ["x^2 - 3*x = 0", "x*(x - 3) = 0"],
             [{"type": "missing_root", "step_index": 2, "expression": "x = 3"}],
             "quadratic_equation.missing_root", "Giải phương trình x^2 = 3x.", "x^2 = 3*x"),
        _row("P-TARI-001", "arithmetic", "test", "28", ["3 * 12 = 36", "36 - 8 = 28"],
             [{"type": "wrong_operation", "step_index": 1, "expression": "3 + 12 = 15"},
              {"type": "arithmetic_slip", "step_index": 2, "expression": "36 - 8 = 27"}],
             "arithmetic.multi_step", "Mỗi hộp có 12 cái bút, mua 3 hộp rồi tặng bạn 8 cái. Còn lại bao nhiêu cái?"),
        # Đề gốc dùng ẩn y; kho lưu theo x (math_check chỉ kiểm x), `variable` giữ chữ gốc để hiển thị.
        _row("P-TLIN-003", "equation", "test", "3", ["3*x = 9", "x = 3"],
             [{"type": "sign_error", "step_index": 1, "expression": "3*x = 5"}],
             iso, "Giải phương trình 3y - 2 = 7.", "3*x - 2 = 7", variable="y"),
        _row("P-DLIN-001", "equation", "dev", "5", ["3*x = 15", "x = 5"],
             [{"type": "sign_error", "step_index": 1, "expression": "3*x = 25"}],
             iso, "Giải phương trình 3x + 5 = 20.", "3*x + 5 = 20"),
        _row("P-DARI-001", "arithmetic", "dev", "30", ["5 * 6 = 30"],
             [{"type": "wrong_operation", "step_index": 1, "expression": "5 + 6 = 11"}],
             "arithmetic.multi_step", "Mỗi túi có 6 quả táo. Năm túi có bao nhiêu quả?"),
    ]


def synthetic_bank() -> ProblemBank:
    return ProblemBank(parse_problem(row) for row in synthetic_rows())


@pytest.fixture(scope="module")
def facts():
    return {p.problem_id: build_facts(p) for p in synthetic_bank().problems}


def test_synthetic_bank_passes_sympy_verification(facts):
    assert all(f.verified for f in facts.values()), {k: f.errors for k, f in facts.items() if not f.verified}


def test_steps_revealing_the_final_answer_are_flagged(facts):
    lin = facts["P-TLIN-001"]
    assert [s.reveals_answer for s in lin.steps] == [False, True]  # "x = 4" là đáp án
    # "-x = 66" chưa phải dạng x = số nhưng chứa |đáp án| → bị khóa (chặt, fail-closed).
    assert [s.reveals_answer for s in facts["P-TLIN-002"].steps] == [False, False, True, True]
    # Nghiệm 3 có sẵn trong đề, nhưng nghiệm 0 thì không: dạng tích "x*(x - 3) = 0" lộ nghiệm.
    assert [s.reveals_answer for s in facts["P-TQUAD-001"].steps] == [True, True]
    assert [s.reveals_answer for s in facts["P-TARI-001"].steps] == [False, True]


def test_number_values_reads_integers_decimals_and_fractions():
    assert number_values("bước 2*x = 8, rồi 2.5 và 400/19") == {Fraction(2), Fraction(8), Fraction(5, 2),
                                                                  Fraction(400), Fraction(19), Fraction(400, 19)}
    assert number_values("không có số") == set()


def test_mask_keeps_exponents_and_operators():
    assert mask_step("x**2 + 4016*x + 4032064 = x**2") == "x^2 + □*x + □ = x^2"
    assert mask_step("15 - 3.2 = 11.8") == "□ - □ = □"
    assert mask_step("x^2 - 3*x = 0") == "x^2 - □*x = □"


def _draft(*parts: Part, claims: tuple[Claim, ...] = ()) -> HintDraft:
    return HintDraft(level=3, parts=parts, claims=claims)


def test_correct_step_claim_passes(facts):
    draft = _draft(Part("Bước tiếp theo đúng là 2*x = 8.", ROLE_OWN),
                   claims=(Claim("2*x = 8", CLAIM_STEP, 1),))
    assert verify_draft(draft, facts["P-TLIN-001"]) == []


def test_deliberately_wrong_expression_is_rejected(facts):
    draft = _draft(Part("Bước tiếp theo đúng là 2*x = 14.", ROLE_OWN),
                   claims=(Claim("2*x = 14", CLAIM_STEP, 1),))
    errors = verify_draft(draft, facts["P-TLIN-001"])
    assert any("không phải bước đã kiểm" in e for e in errors)


def test_answer_revealing_step_needs_permission(facts):
    draft = _draft(Part("Bước tiếp theo đúng là x = 4.", ROLE_OWN), claims=(Claim("x = 4", CLAIM_STEP, 2),))
    assert any("lộ đáp án" in e for e in verify_draft(draft, facts["P-TLIN-001"]))
    assert verify_draft(draft, facts["P-TLIN-001"], allow_answer=True) == []


def test_sentence_that_leaks_the_answer_is_rejected(facts):
    leaky = _draft(Part("Gần xong rồi, nghiệm là số 4 đó.", ROLE_OWN))
    assert any("giá trị đáp án" in e for e in verify_draft(leaky, facts["P-TLIN-001"]))
    # Số có sẵn trong đề không phải lộ đáp án; số trong câu khái niệm chung không bị xét.
    fine = _draft(Part("Đề có số 11 ở vế phải.", ROLE_OWN), Part("Ví dụ 4 quả táo.", ROLE_GENERIC))
    assert verify_draft(fine, facts["P-TLIN-001"]) == []


def test_unverified_equals_sign_in_text_is_rejected(facts):
    draft = _draft(Part("Thử viết x + 1 = 5 xem.", ROLE_OWN))
    assert any("dấu '='" in e for e in verify_draft(draft, facts["P-TLIN-001"], allow_answer=True))


def test_form_must_match_a_verified_step(facts):
    good = _draft(Part(facts["P-TLIN-001"].steps[0].form, ROLE_FORM, 1))
    assert verify_draft(good, facts["P-TLIN-001"]) == []
    forged = _draft(Part("x = □ + □", ROLE_FORM, 1))
    assert any("dạng" in e for e in verify_draft(forged, facts["P-TLIN-001"]))


def test_example_claim_must_be_a_verified_dev_problem(facts):
    text = "Thử bài tương tự P-DLIN-001: giải 3*x + 5 = 20."
    draft = _draft(Part(text, ROLE_EXAMPLE), claims=(Claim("3*x + 5 = 20", CLAIM_EXAMPLE),))
    assert verify_draft(draft, facts["P-TLIN-001"], {"P-DLIN-001": facts["P-DLIN-001"]}) == []
    # Bài test không bao giờ được dùng làm bài tương tự (tránh rò rỉ tập đánh giá).
    test_example = _draft(Part("Thử giải 2*x + 3 = 11.", ROLE_EXAMPLE), claims=(Claim("2*x + 3 = 11", CLAIM_EXAMPLE),))
    assert verify_draft(test_example, facts["P-TLIN-002"], {"P-TLIN-001": facts["P-TLIN-001"]})


def test_claim_must_appear_in_text(facts):
    draft = _draft(Part("Bước tiếp theo đúng ở trên.", ROLE_OWN), claims=(Claim("2*x = 8", CLAIM_STEP, 1),))
    assert any("không có trong câu" in e for e in verify_draft(draft, facts["P-TLIN-001"]))


def test_unverified_problem_rejects_every_draft():
    row = synthetic_rows()[0] | {"steps": ["2*x = 9", "x = 4"]}  # bước 1 sai cố ý
    facts = build_facts(parse_problem(row))
    assert not facts.verified and facts.errors
    draft = _draft(Part("Hãy thử lại.", ROLE_OWN))
    assert verify_draft(draft, facts) == [f"bài {facts.problem_id} chưa qua kiểm chứng SymPy"]
