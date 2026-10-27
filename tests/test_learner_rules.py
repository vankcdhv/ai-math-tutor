"""Một định nghĩa bế tắc (10 cơ hội chưa có 3 đúng liên tiếp), thành thạo 3 liên tiếp, cảnh báo sai liên tiếp tách riêng."""

import pytest

from ai_math_tutor import learner_rules as rules

RIGHT, WRONG, HINT = rules.OBSERVATION_CORRECT, rules.OBSERVATION_WRONG, rules.OBSERVATION_HINT


def run(observations, profile=rules.EMPTY_PROFILE):
    states = []
    for obs in observations:
        profile = rules.update(profile, obs)
        states.append(profile)
    return states


def test_three_consecutive_correct_is_mastery():
    states = run([RIGHT, WRONG, RIGHT, RIGHT, RIGHT])
    assert [rules.is_mastered(s) for s in states] == [False, False, False, False, True]
    assert states[-1].streak == 3 and states[-1].correct == 4 and states[-1].opportunities == 5


def test_mastery_is_sticky():
    states = run([RIGHT, RIGHT, RIGHT] + [WRONG] * 12)
    assert all(rules.is_mastered(s) for s in states[2:])
    assert not any(rules.is_wheel_spinning(s) for s in states)


def test_wheel_spinning_turns_on_exactly_at_tenth_opportunity_without_streak():
    pattern = [RIGHT, RIGHT, WRONG] * 4  # không bao giờ 3 đúng liên tiếp
    states = run(pattern)
    flags = [rules.is_wheel_spinning(s) for s in states]
    assert flags[:9] == [False] * 9
    assert all(flags[9:])
    assert states[9].opportunities == rules.WHEEL_SPIN_OPPORTUNITIES


def test_wheel_spinning_episode_ends_when_streak_reached_later():
    states = run([WRONG] * 10 + [RIGHT, RIGHT, RIGHT])
    flags = [rules.is_wheel_spinning(s) for s in states]
    assert flags[9] and flags[10] and flags[11]
    assert not flags[12] and rules.is_mastered(states[12])


def test_consecutive_wrong_alert_is_separate_from_stuck():
    states = run([WRONG, WRONG, WRONG])
    assert rules.consec_wrong_alert(states[-1])
    assert not rules.is_wheel_spinning(states[-1])
    assert not rules.consec_wrong_alert(rules.update(states[-1], RIGHT))


def test_hints_are_counted_but_are_not_opportunities_and_keep_streak():
    states = run([RIGHT, RIGHT, HINT, RIGHT])
    assert states[-1].hints_used == 1
    assert states[-1].opportunities == 3
    assert rules.is_mastered(states[-1])
    stuck_with_hints = run([WRONG] * 9 + [HINT] * 5)
    assert not rules.is_wheel_spinning(stuck_with_hints[-1])


@pytest.mark.parametrize(
    ("event_type", "payload", "expected"),
    [
        ("submit_step", {"step_index": 1, "expression": "x = 4", "is_correct": True}, RIGHT),
        ("submit_step", {"step_index": 1, "expression": "x = 5", "is_correct": False}, WRONG),
        ("complete", {"answer": "4", "attempts": 1, "hints_used": 0}, RIGHT),
        ("wrong_answer", {"answer": "5", "attempt": 1}, WRONG),
        ("request_hint", {"hint_level": 1, "step_index": 1}, HINT),
        ("open_problem", {}, None),
    ],
)
def test_event_to_observation_mapping(event_type, payload, expected):
    assert rules.observation_for(event_type, payload) == expected


def test_submit_step_without_boolean_label_rejected():
    with pytest.raises(ValueError):
        rules.observation_for("submit_step", {"step_index": 1, "expression": "x = 4", "is_correct": "yes"})


def test_apply_event_credits_every_skill_of_the_event():
    event = {
        "event_type": "submit_step",
        "skill_ids": ["linear_equation.distribute", "linear_equation.isolate_variable"],
        "payload": {"step_index": 1, "expression": "3*x - 3 = 2*x + 5", "is_correct": True},
    }
    before = {"linear_equation.isolate_variable": rules.SkillProfile(opportunities=2, correct=2, streak=2)}
    after = rules.apply_event(before, event)
    assert after["linear_equation.distribute"] == rules.SkillProfile(opportunities=1, correct=1, streak=1)
    assert rules.is_mastered(after["linear_equation.isolate_variable"])
    assert before["linear_equation.isolate_variable"].streak == 2  # đầu vào không bị sửa
    assert rules.apply_event(after, {**event, "event_type": "open_problem", "payload": {}}) == after


def test_profile_serialization_roundtrip():
    profile = run([WRONG, RIGHT, HINT, RIGHT])[-1]
    data = profile.to_dict()
    assert data == {"opportunities": 3, "correct": 2, "hints_used": 1, "consec_wrong": 0, "streak": 2, "mastered": False}
    assert rules.SkillProfile.from_dict(data) == profile
