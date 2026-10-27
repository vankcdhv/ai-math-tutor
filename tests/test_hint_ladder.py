"""Thang gợi ý: bảng chọn bậc fail-closed, câu mẫu, bộ sinh template|ladder, và quét toàn kho bài."""

from __future__ import annotations

import re

import pytest
import sympy

from ai_math_tutor import math_check
from ai_math_tutor.hints import phrases
from ai_math_tutor.hints.generator import (
    GENERATORS,
    HintEngine,
    default_problem_bank_path,
    render_draft,
    to_machine_expression,
)
from ai_math_tutor.hints.ladder import LadderInput, LevelDecision, select_level
from ai_math_tutor.hints.templates import LEVELS, generic_hint, template_for_skills, verify_template
from ai_math_tutor.hints.verify import ROLE_FORM, ROLE_OWN, HintDraft, Part
from ai_math_tutor.problem_bank import ProblemBank
from test_hint_verify import synthetic_bank

# --- bảng chọn bậc --------------------------------------------------------------------------


def test_missing_state_caps_at_level_one_and_never_allows_answer():
    for hint_count in (1, 2, 5):
        decision = select_level(LadderInput(state_source="default", hint_count=hint_count, consec_wrong=9,
                                            stuck=True, wrong_answers=7, step_wrong_count=4))
        assert (decision.level, decision.answer_allowed, decision.reason) == (1, False, "no_state")


@pytest.mark.parametrize(("hint_count", "expected"), [(1, 1), (2, 2), (3, 3), (9, 3)])
def test_level_follows_hint_count_on_the_same_step(hint_count, expected):
    assert select_level(LadderInput(state_source="redis", hint_count=hint_count)).level == expected


@pytest.mark.parametrize("struggle", [{"consec_wrong": 3}, {"stuck": True}, {"step_wrong_count": 2}])
def test_struggling_learner_gets_one_level_more(struggle):
    decision = select_level(LadderInput(state_source="redis", hint_count=1, **struggle))
    assert decision.level == 2 and "struggling" in decision.reason
    assert select_level(LadderInput(state_source="redis", hint_count=3, **struggle)).level == 3


def test_answer_unlocks_only_after_two_wrong_final_answers():
    assert not select_level(LadderInput(state_source="redis", hint_count=3, wrong_answers=1)).answer_allowed
    assert select_level(LadderInput(state_source="redis", hint_count=3, wrong_answers=2)).answer_allowed


def test_ladder_input_rejects_bad_values():
    with pytest.raises(ValueError):
        LadderInput(state_source="cache")
    with pytest.raises(ValueError):
        LadderInput(state_source="redis", hint_count=-1)
    with pytest.raises(ValueError):
        LadderInput(state_source="redis", consec_wrong=True)


# --- câu mẫu ---------------------------------------------------------------------------------


def test_phrase_table_is_complete_and_has_no_digits_or_equals():
    for kind in phrases.KINDS:
        for level in LEVELS:
            for misconception in phrases.MISCONCEPTIONS:
                assert phrases.phrase(kind, level, misconception)
    for text in phrases.all_phrases():
        assert not re.search(r"[0-9=]", text), text
    with pytest.raises(ValueError):
        phrases.phrase("geometry", 1, None)


def test_generic_template_has_no_math_and_verifies():
    hint = template_for_skills(["unknown.skill"], "equation", 2, "sign_error")
    assert hint.math == () and verify_template(hint) == []
    assert generic_hint("unknown.skill", "arithmetic", 1).misconception is None


# --- bộ sinh trên kho bài tự viết ---------------------------------------------------------------


@pytest.fixture(scope="module")
def engine():
    return HintEngine.from_bank(synthetic_bank())


def _hint(engine, problem_id, step, level, *, status="none", misconception=None, allow=False, generator="ladder"):
    ctx = engine.context(problem_id, step, status, misconception)
    return engine.generator(generator).generate(ctx, LevelDecision(level, allow, "test"))


def test_level_one_has_concept_reminder_and_a_dev_similar_problem(engine):
    hint = _hint(engine, "P-TLIN-001", 1, 1, status="wrong", misconception="sign_error")
    assert hint.source == "ladder" and hint.verified
    assert "P-DLIN-001" in hint.text_vi and hint.math == ("3*x + 5 = 20",)
    assert phrases.phrase("equation", 1, "sign_error") in hint.text_vi


def test_level_two_gives_diagnosis_and_masked_form(engine):
    hint = _hint(engine, "P-TLIN-001", 1, 2, status="wrong", misconception="sign_error")
    assert hint.source == "ladder" and hint.math == ()
    assert "□*x = □" in hint.text_vi and "8" not in hint.text_vi
    assert phrases.phrase("equation", 2, "sign_error") in hint.text_vi


def test_level_three_gives_next_verified_step(engine):
    hint = _hint(engine, "P-TLIN-001", 1, 3, status="wrong", misconception="sign_error")
    assert (hint.source, hint.math, hint.verified) == ("ladder", ("2*x = 8",), True)
    assert "2*x = 8" in hint.text_vi


def test_level_three_on_answer_step_gives_its_masked_form_until_answer_allowed(engine):
    locked = _hint(engine, "P-TLIN-001", 2, 3)
    assert (locked.source, locked.note, locked.verified, locked.hint_level) == ("ladder", "answer_locked", True, 3)
    assert locked.math == () and "x = □" in locked.text_vi and "4" not in locked.text_vi
    assert phrases.LOCKED_FORM_LEAD_VI["equation"] in locked.text_vi
    assert phrases.phrase("equation", 3, None) not in locked.text_vi  # "làm theo bước dưới đây" sai khi bước bị che
    assert locked.text_vi != _hint(engine, "P-TLIN-001", 2, 3, generator="template").text_vi
    unlocked = _hint(engine, "P-TLIN-001", 2, 3, allow=True)
    assert (unlocked.source, unlocked.note, unlocked.math) == ("ladder", "", ("x = 4",))


def test_locked_arithmetic_step_keeps_the_misconception_phrase_and_hides_the_result(engine):
    hint = _hint(engine, "P-TARI-001", 2, 3, status="wrong", misconception="arithmetic_slip")
    assert (hint.source, hint.note, hint.math) == ("ladder", "answer_locked", ())
    assert phrases.phrase("arithmetic", 3, "arithmetic_slip") in hint.text_vi
    assert phrases.LOCKED_FORM_LEAD_VI["arithmetic"] in hint.text_vi
    assert "□ - □ = □" in hint.text_vi and not re.search(r"[0-9]", hint.text_vi)


def test_non_final_level_three_step_is_not_marked_locked(engine):
    hint = _hint(engine, "P-TLIN-001", 1, 3)
    assert (hint.source, hint.note, hint.math) == ("ladder", "", ("2*x = 8",))


def test_correct_expression_moves_target_to_the_next_step(engine):
    hint = _hint(engine, "P-TARI-001", 1, 2, status="correct")
    assert phrases.status_phrase("correct") in hint.text_vi
    assert "□ - □ = □" in hint.text_vi  # dạng của bước 2 (36 - 8 = 28)


def test_template_generator_is_not_personalised(engine):
    hint = _hint(engine, "P-TLIN-001", 1, 3, status="wrong", misconception="sign_error", generator="template")
    assert hint.source == "template" and "2*x = 8" not in hint.text_vi
    assert hint.math == template_for_skills(["linear_equation.isolate_variable"], "equation", 3, "sign_error").math


def test_hint_is_shown_in_the_problems_own_variable(engine):
    hint = _hint(engine, "P-TLIN-003", 1, 3, status="wrong", misconception="sign_error")
    assert hint.source == "ladder" and hint.math == ("3*y = 9",) and hint.machine_math == ("3*x = 9",)
    assert "3*y = 9" in hint.text_vi and not re.search(r"(?<!\w)x(?!\w)", hint.text_vi)
    form = _hint(engine, "P-TLIN-003", 1, 2)
    assert "□*y = □" in form.text_vi
    concept = _hint(engine, "P-TLIN-003", 1, 1)
    assert "hệ số của y" in concept.text_vi  # câu khái niệm nói về ẩn của chính bài
    assert concept.math == ("3*x + 5 = 20",)  # bài tương tự giữ ẩn của nó


def test_variable_mapping_round_trip_and_prose_words_untouched():
    assert to_machine_expression("3*y = 9", "y") == "3*x = 9"
    assert to_machine_expression("2*x = 8", None) == "2*x = 8"
    draft = HintDraft(level=2, parts=(Part("Hãy xét từng nhân tử chứa x: ", ROLE_OWN), Part("x^2 - □*x = □", ROLE_FORM, 1)),
                      claims=())
    text, math = render_draft(draft, "n", None)
    assert text == "Hãy xét từng nhân tử chứa n: n^2 - □*n = □" and math == ()


def test_unknown_generator_name_is_rejected(engine):
    with pytest.raises(ValueError):
        engine.generator("llm")


# --- toàn kho bài × lỗi đã khai × bậc × bộ sinh ----------------------------------------------------


def _answer_values(problem) -> set[sympy.Rational]:
    return {abs(v) for v in math_check.parse_answer(problem.answer).args}


def _reveals_answer(problem, expression: str) -> bool:
    """Kiểm độc lập với `verify`: biểu thức là `x = nghiệm` (phương trình) hoặc có kết quả = đáp án (số học)."""
    answers = _answer_values(problem)
    lhs, _, rhs = expression.partition("=")
    if problem.kind == "equation":
        sides = (lhs.strip(), rhs.strip())
        if "x" in sides and "x" not in (sides[1] if sides[0] == "x" else sides[0]):
            other = sides[1] if sides[0] == "x" else sides[0]
            return abs(math_check.parse_answer(other).args[0]) in answers
        return False
    return abs(math_check.parse_answer(expression.rpartition("=")[2]).args[0]) in answers


@pytest.fixture(scope="module")
def bank_engine():
    return HintEngine.from_bank(ProblemBank.from_jsonl(default_problem_bank_path()))


def test_every_hint_in_the_bank_is_verified_and_never_reveals_the_answer_early(bank_engine):
    assert bank_engine.unverified_problems == []
    assert bank_engine.library.failed == []
    checked = ladder_served = locked = 0
    for problem in bank_engine.bank.problems:
        row = problem.to_dict()
        cases = [(1, "none", None)]
        for item in problem.misconceptions:
            cases.append((item.step_index, "wrong", math_check.diagnose(row, item.expression, item.step_index)["misconception"]))
        cases.append((len(problem.steps) + 1, "none", None))  # đang nộp đáp án cuối
        for step, status, misconception in cases:
            ctx = bank_engine.context(problem.problem_id, step, status, misconception)
            for level in LEVELS:
                for name in GENERATORS:
                    hint = bank_engine.generator(name).generate(ctx, LevelDecision(level, False, "sweep"))
                    checked += 1
                    assert hint.verified, (problem.problem_id, step, level, name)
                    assert hint.hint_level == level
                    if name == "ladder":
                        # Bộ sinh ladder không bao giờ rơi về mẫu trên kho bài hiện tại.
                        assert hint.source == "ladder" and hint.note in ("", "answer_locked"), (problem.problem_id, hint)
                    if hint.note == "answer_locked":
                        locked += 1
                        assert level == 3 and hint.math == () and phrases.FORM_SLOT in hint.text_vi
                        # Chỉ còn số mũ của dạng bước (giữ để thấy bậc), không một con số nào khác.
                        assert not re.search(r"[0-9]", re.sub(r"\^[0-9]+", "", hint.text_vi)), (problem.problem_id, hint)
                    if hint.source == "ladder":
                        ladder_served += 1
                        for expression in hint.machine_math or hint.math:
                            if expression != (ctx.example.equation if ctx.example else None):
                                assert not _reveals_answer(problem, expression), (problem.problem_id, hint)
                        shown = re.escape(problem.display_variable)
                        assert not re.search(rf"(?<![0-9.]){shown}\s*=\s*{re.escape(problem.answer)}(?![0-9.])", hint.text_vi)
    assert checked > 0 and ladder_served > checked // 3 and locked > 0
