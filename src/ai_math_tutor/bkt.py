"""Bayesian Knowledge Tracing (Corbett & Anderson, 1995) dạng hàm thuần.

Dùng chung cho bộ mô phỏng, batch baseline và pipeline streaming để cả ba tính
cùng một công thức:

    đúng: P(L | đúng) = P(L)(1 − S) / [P(L)(1 − S) + (1 − P(L))G]
    sai:  P(L | sai)  = P(L)S / [P(L)S + (1 − P(L))(1 − G)]
    học:  P(L_{t+1})  = P(L | quan sát) + (1 − P(L | quan sát))·T

Ràng buộc tham số: 0 < G < 1, 0 < S < 1, G + S < 1 (ngoài vùng này một câu trả
lời đúng không còn là bằng chứng của thành thạo — mô hình suy biến). Với G, S > 0
mẫu số không bao giờ bằng 0 nên mọi P(L) ∈ [0, 1] đều hợp lệ, kể cả khi làm tròn
số thực đẩy P(L) chạm biên.
"""

from __future__ import annotations

from dataclasses import dataclass

# Ngưỡng "thành thạo theo BKT" là quy ước phổ biến, không phải chuẩn có nguồn gốc;
# cấu hình được và không lẫn với tiêu chí "3 lần đúng liên tiếp" của learner_rules.
DEFAULT_MASTERY_THRESHOLD = 0.95


def _check_probability(name: str, value: float) -> None:
    if not 0.0 <= value <= 1.0:
        raise ValueError(f"{name} phải nằm trong [0, 1], nhận {value!r}")


def _check_observation_params(guess: float, slip: float) -> None:
    if not 0.0 < guess < 1.0:
        raise ValueError(f"p_guess phải nằm trong (0, 1), nhận {guess!r}")
    if not 0.0 < slip < 1.0:
        raise ValueError(f"p_slip phải nằm trong (0, 1), nhận {slip!r}")
    if guess + slip >= 1.0:
        raise ValueError(f"p_guess + p_slip phải < 1 (mô hình suy biến), nhận {guess + slip!r}")


def posterior(p_mastery: float, correct: bool, guess: float, slip: float) -> float:
    """P(L | quan sát) sau một cơ hội làm bài."""
    _check_probability("p_mastery", p_mastery)
    _check_observation_params(guess, slip)
    if correct:
        evidence = p_mastery * (1.0 - slip)
        return evidence / (evidence + (1.0 - p_mastery) * guess)
    evidence = p_mastery * slip
    return evidence / (evidence + (1.0 - p_mastery) * (1.0 - guess))


def transition(p_mastery: float, learn: float) -> float:
    """P(L_{t+1}) sau bước học: chưa thành thạo chuyển sang thành thạo với xác suất T (không quên)."""
    _check_probability("p_mastery", p_mastery)
    _check_probability("p_learn", learn)
    return p_mastery + (1.0 - p_mastery) * learn


def update(p_mastery: float, correct: bool, guess: float, slip: float, learn: float) -> float:
    """Một bước BKT đầy đủ: hậu nghiệm theo quan sát rồi chuyển trạng thái."""
    return transition(posterior(p_mastery, correct, guess, slip), learn)


def p_correct(p_mastery: float, guess: float, slip: float) -> float:
    """Xác suất dự đoán trả lời đúng ở cơ hội kế tiếp."""
    _check_probability("p_mastery", p_mastery)
    _check_observation_params(guess, slip)
    return p_mastery * (1.0 - slip) + (1.0 - p_mastery) * guess


@dataclass(frozen=True)
class BktParams:
    """Bốn tham số BKT của một (người học, kỹ năng); kiểm tra ràng buộc khi tạo."""

    p_init: float
    p_learn: float
    p_guess: float
    p_slip: float

    def __post_init__(self) -> None:
        _check_probability("p_init", self.p_init)
        _check_probability("p_learn", self.p_learn)
        _check_observation_params(self.p_guess, self.p_slip)

    def update(self, p_mastery: float, correct: bool) -> float:
        return update(p_mastery, correct, self.p_guess, self.p_slip, self.p_learn)

    def p_correct(self, p_mastery: float) -> float:
        return p_correct(p_mastery, self.p_guess, self.p_slip)

    def to_dict(self) -> dict[str, float]:
        return {"p_init": self.p_init, "p_learn": self.p_learn, "p_guess": self.p_guess, "p_slip": self.p_slip}
