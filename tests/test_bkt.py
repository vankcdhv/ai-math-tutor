"""Công thức BKT so với giá trị tính tay và các ràng buộc biên của G/S."""

import pytest

from ai_math_tutor import bkt


def test_posterior_after_correct_matches_hand_computation():
    # 0.3·0.9 / (0.3·0.9 + 0.7·0.2) = 0.27 / 0.41
    assert bkt.posterior(0.3, True, guess=0.2, slip=0.1) == pytest.approx(0.27 / 0.41)


def test_posterior_after_wrong_matches_hand_computation():
    # 0.3·0.1 / (0.3·0.1 + 0.7·0.8) = 0.03 / 0.59
    assert bkt.posterior(0.3, False, guess=0.2, slip=0.1) == pytest.approx(0.03 / 0.59)


def test_transition_and_full_update_match_hand_computation():
    assert bkt.transition(0.4, 0.25) == pytest.approx(0.4 + 0.6 * 0.25)
    after_correct = 0.27 / 0.41
    expected = after_correct + (1 - after_correct) * 0.15  # ≈ 0.709756
    assert bkt.update(0.3, True, guess=0.2, slip=0.1, learn=0.15) == pytest.approx(expected)
    assert expected == pytest.approx(0.709756, abs=1e-6)


def test_predicted_correctness():
    assert bkt.p_correct(0.3, guess=0.2, slip=0.1) == pytest.approx(0.3 * 0.9 + 0.7 * 0.2)


@pytest.mark.parametrize("p", [0.01, 0.3, 0.5, 0.99])
def test_correct_raises_and_wrong_lowers_belief_when_model_not_degenerate(p):
    assert bkt.posterior(p, False, 0.25, 0.08) < p < bkt.posterior(p, True, 0.25, 0.08)


def test_boundary_beliefs_stay_finite_and_in_range():
    # G, S > 0 ⇒ mẫu số > 0 kể cả khi P(L) chạm biên do làm tròn.
    assert bkt.posterior(0.0, True, 0.2, 0.1) == 0.0
    assert bkt.posterior(1.0, False, 0.2, 0.1) == 1.0
    assert bkt.transition(1.0, 0.3) == 1.0
    assert bkt.transition(0.0, 0.0) == 0.0


def test_tiny_guess_makes_a_correct_answer_strong_evidence():
    assert bkt.posterior(0.1, True, guess=1e-6, slip=0.05) > 0.9999


@pytest.mark.parametrize(
    ("guess", "slip"),
    [(0.0, 0.1), (0.2, 0.0), (1.0, 0.0), (0.6, 0.4), (0.7, 0.5), (-0.1, 0.1), (0.2, 1.0)],
)
def test_degenerate_or_invalid_guess_slip_rejected(guess, slip):
    with pytest.raises(ValueError):
        bkt.posterior(0.5, True, guess, slip)
    with pytest.raises(ValueError):
        bkt.BktParams(0.1, 0.1, guess, slip)


@pytest.mark.parametrize("p", [-0.01, 1.01])
def test_probability_out_of_range_rejected(p):
    with pytest.raises(ValueError):
        bkt.posterior(p, True, 0.2, 0.1)
    with pytest.raises(ValueError):
        bkt.transition(0.5, p)


def test_params_object_uses_same_equations():
    params = bkt.BktParams(p_init=0.3, p_learn=0.15, p_guess=0.2, p_slip=0.1)
    assert params.update(0.3, True) == pytest.approx(bkt.update(0.3, True, 0.2, 0.1, 0.15))
    assert params.p_correct(0.3) == pytest.approx(bkt.p_correct(0.3, 0.2, 0.1))
    assert params.to_dict() == {"p_init": 0.3, "p_learn": 0.15, "p_guess": 0.2, "p_slip": 0.1}


def test_repeated_correct_answers_cross_mastery_threshold():
    p = 0.1
    for _ in range(5):
        p = bkt.update(p, True, guess=0.2, slip=0.1, learn=0.1)
    assert p > bkt.DEFAULT_MASTERY_THRESHOLD
