"""Gợi ý mẫu: đủ tổ hợp kỹ năng × bậc × loại lỗi, mọi biểu thức toán được SymPy kiểm."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ai_math_tutor import math_check
from ai_math_tutor.hints import templates
from ai_math_tutor.hints.templates import (
    LEVELS,
    MISCONCEPTIONS,
    SKILL_TEMPLATES,
    HintTemplate,
    all_templates,
    template_hint,
    verify_template,
)

SKILLS_CONFIG = Path(__file__).resolve().parents[1] / "configs" / "skills.yaml"


def configured_skills() -> dict[str, str]:
    raw = yaml.safe_load(SKILLS_CONFIG.read_text(encoding="utf-8"))
    return {item["skill_id"]: item["kind"] for item in raw["skills"]}


def test_templates_cover_exactly_the_configured_skills_with_matching_kind():
    assert {skill: t.kind for skill, t in SKILL_TEMPLATES.items()} == configured_skills()


def test_every_skill_level_misconception_combination_exists():
    combos = [(t.skill_id, t.hint_level, t.misconception) for t in all_templates()]
    expected = len(SKILL_TEMPLATES) * len(LEVELS) * len(MISCONCEPTIONS)
    assert len(combos) == len(set(combos)) == expected
    assert set(MISCONCEPTIONS) == {None, *math_check.MISCONCEPTION_TYPES}


@pytest.mark.parametrize("template", list(all_templates()), ids=lambda t: f"{t.skill_id}-L{t.hint_level}-{t.misconception}")
def test_every_math_expression_is_verified_by_math_check(template: HintTemplate):
    assert verify_template(template) == []
    assert template.text_vi.strip()


@pytest.mark.parametrize("skill_id", sorted(SKILL_TEMPLATES))
def test_worked_steps_are_correct_by_sympy(skill_id: str):
    """Kiểm trực tiếp (không qua verify_template) để lỗi trong bộ kiểm cũng bị bắt."""
    skill = SKILL_TEMPLATES[skill_id]
    assert skill.worked_steps
    if skill.kind == "equation":
        assert skill.example is not None
        assert not math_check.solution_set(skill.example).is_empty
        for step in skill.worked_steps:
            assert math_check.step_is_equivalent(skill.example, step), step
    else:
        assert skill.example is None
        for step in skill.worked_steps:
            assert math_check.arith_step(step), step


def test_level_three_shows_the_worked_example_and_level_two_has_no_math():
    hint = template_hint("linear_equation.isolate_variable", 3, "sign_error")
    assert hint.math == ("3x + 5 = 20", "3x = 15", "x = 5")
    assert all(expression in hint.text_vi for expression in hint.math)
    assert template_hint("linear_equation.isolate_variable", 2, "sign_error").math == ()
    assert "sai dấu" in template_hint("linear_equation.isolate_variable", 2, "sign_error").text_vi


def test_misconception_changes_the_diagnosis_but_not_the_skill_content():
    generic = template_hint("quadratic_equation.missing_root", 2)
    specific = template_hint("quadratic_equation.missing_root", 2, "missing_root")
    assert generic.text_vi != specific.text_vi
    assert SKILL_TEMPLATES["quadratic_equation.missing_root"].next_step_vi in generic.text_vi
    assert SKILL_TEMPLATES["quadratic_equation.missing_root"].next_step_vi in specific.text_vi


def test_templates_are_deterministic_and_serialisable():
    first = [t.as_dict() for t in all_templates()]
    assert first == [t.as_dict() for t in all_templates()]
    assert first[0]["source"] == "template"
    assert isinstance(first[0]["math"], list)


@pytest.mark.parametrize(
    ("skill_id", "level", "misconception", "error"),
    [
        ("geometry.angles", 1, None, KeyError),
        ("arithmetic.percent", 0, None, ValueError),
        ("arithmetic.percent", 4, None, ValueError),
        ("arithmetic.percent", True, None, ValueError),
        ("arithmetic.percent", 1, "typo", ValueError),
    ],
)
def test_invalid_requests_are_rejected(skill_id, level, misconception, error):
    with pytest.raises(error):
        template_hint(skill_id, level, misconception)


def test_verifier_rejects_wrong_or_hidden_math():
    wrong_step = HintTemplate("linear_equation.isolate_variable", "equation", 3, None,
                              "Ví dụ 3x + 5 = 20 nên 3x = 25.", ("3x + 5 = 20", "3x = 25"), "3x + 5 = 20")
    assert any("không tương đương" in e for e in verify_template(wrong_step))
    hidden = HintTemplate("arithmetic.percent", "arithmetic", 3, None, "Kết quả 150*20/100 = 31.", (), None)
    assert any("dấu '='" in e for e in verify_template(hidden))
    slip = HintTemplate("arithmetic.percent", "arithmetic", 3, None, "Ta có 150*20/100 = 31.", ("150*20/100 = 31",), None)
    assert any("bước số học sai" in e for e in verify_template(slip))
    unsafe = HintTemplate("arithmetic.percent", "arithmetic", 3, None, "Ta có 9^9^9 = 1.", ("9^9^9 = 1",), None)
    assert any("không hợp lệ" in e for e in verify_template(unsafe))
    assert templates.SOURCE == "template"
