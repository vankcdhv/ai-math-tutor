"""Hành vi phiên học của một người học mô phỏng (sinh sự kiện theo thời gian sự kiện).

Mô hình sinh (giả định, có tham số):
- Trạng thái ẩn mỗi kỹ năng: đã/chưa thành thạo (khởi tạo Bernoulli(L0)).
- Mỗi cơ hội (nộp bước, nộp đáp án cuối): P(đúng) = Π_k (1 − S_k nếu thành thạo, G_k nếu chưa)
  trên mọi kỹ năng của bài (mô hình kết hợp); sau cơ hội, kỹ năng chưa thành thạo chuyển sang
  thành thạo với xác suất T_k (+ `hint_learn_bonus` nếu vừa xin gợi ý). Không quên.
- Bước đúng = `steps[i]` của bài; bước sai = một `misconception` của bài (ưu tiên đúng bước),
  nên nhãn `is_correct` luôn đúng về toán học nếu kho bài đã được kiểm.
- Đáp án sai lấy từ tập ứng viên tất định (thiếu nghiệm, nghiệm từ misconception dạng x = c,
  lệch ±1, đổi dấu) — luôn khác tập nghiệm đúng.
- Xin gợi ý sau `hint_after_wrong` lần sai ở một bước, hoặc với xác suất `p_hint` sau mỗi lần sai;
  bậc gợi ý tăng 1 → 3 trong một bước. Bỏ bài sau `max_step_attempts` lần sai ở một bước.
- Chọn kỹ năng theo chương trình (xoay vòng từ vị trí ngẫu nhiên), chuyển kỹ năng khi đã
  thành thạo theo luật (3 đúng liên tiếp) hoặc sau `max_opportunities_per_skill` cơ hội.
- Think-time log-normal với trung bình cho trước (chế độ tốc độ: learners / rate giây).
"""

from __future__ import annotations

import math
import random
import re
from collections.abc import Iterator
from dataclasses import asdict, dataclass
from fractions import Fraction

from ai_math_tutor import learner_rules
from ai_math_tutor.problem_bank import Problem
from ai_math_tutor.simulator.population import LearnerProfile, SimulationConfigError

DAY_MS = 86_400_000
_STUDY_WINDOW_MS = (6 * 3_600_000, 23 * 3_600_000)
_MIN_SESSION_GAP_MS = 5 * 60_000
_SIMPLE_ROOT_RE = re.compile(r"^\s*x\s*=\s*(-?[0-9]+(?:/[0-9]+)?)\s*$")


@dataclass(frozen=True)
class BehaviorSettings:
    think_mean_s: float = 20.0
    think_sigma: float = 0.6
    sessions_per_day_weights: tuple[float, ...] = (0.1, 0.4, 0.35, 0.15)  # P(0, 1, 2, 3 phiên/ngày)
    problems_per_session: tuple[int, int] = (4, 12)
    max_step_attempts: int = 4
    max_answer_attempts: int = 3
    hint_after_wrong: int = 2
    p_hint: float = 0.2
    hint_learn_bonus: float = 0.05
    max_opportunities_per_skill: int = 25

    def __post_init__(self) -> None:
        if self.think_mean_s <= 0 or self.think_sigma < 0:
            raise SimulationConfigError("think_mean_s phải > 0 và think_sigma ≥ 0")
        low, high = self.problems_per_session
        if not 1 <= low <= high:
            raise SimulationConfigError("problems_per_session cần 1 ≤ thấp ≤ cao")
        if min(self.max_step_attempts, self.max_answer_attempts, self.hint_after_wrong, self.max_opportunities_per_skill) < 1:
            raise SimulationConfigError("số lần thử/ngưỡng gợi ý phải ≥ 1")
        if not (0.0 <= self.p_hint <= 1.0 and 0.0 <= self.hint_learn_bonus < 1.0):
            raise SimulationConfigError("p_hint ∈ [0, 1], hint_learn_bonus ∈ [0, 1)")
        if not self.sessions_per_day_weights or any(w < 0 for w in self.sessions_per_day_weights) or sum(self.sessions_per_day_weights) <= 0:
            raise SimulationConfigError("sessions_per_day_weights phải không âm và có tổng > 0")

    def to_dict(self) -> dict:
        return asdict(self)


def _format_value(value: Fraction) -> str:
    return str(value.numerator) if value.denominator == 1 else f"{value.numerator}/{value.denominator}"


def format_answer(values: frozenset[Fraction] | set[Fraction]) -> str:
    """Định dạng đáp án như kho bài: '4' hoặc '{-4, 3}' (tăng dần)."""
    ordered = sorted(values)
    if len(ordered) == 1:
        return _format_value(ordered[0])
    return "{" + ", ".join(_format_value(v) for v in ordered) + "}"


def _answer_values(answer: str) -> frozenset[Fraction]:
    items = [item.strip() for item in answer.strip().strip("{}").split(",") if item.strip()]
    try:
        return frozenset(Fraction(item) for item in items)
    except (ValueError, ZeroDivisionError) as exc:
        raise SimulationConfigError(f"đáp án {answer!r} không phải số hữu tỉ dạng a hoặc a/b") from exc


def wrong_answer_candidates(problem: Problem) -> tuple[str, ...]:
    """Đáp án sai tất định cho bài; mỗi ứng viên là một tập số khác tập nghiệm đúng."""
    correct = _answer_values(problem.answer)
    candidates: list[frozenset[Fraction]] = []
    if len(correct) > 1:
        candidates.extend(correct - {v} for v in sorted(correct))
    for item in problem.misconceptions:
        match = _SIMPLE_ROOT_RE.match(item.expression)
        if match:
            candidates.append(frozenset({Fraction(match.group(1))}))
    if len(correct) == 1:
        (value,) = correct
        candidates.extend(frozenset({v}) for v in (value + 1, -value, value - 1))
    result: list[str] = []
    for values in candidates:
        if values and values != correct:
            text = format_answer(values)
            if text not in result:
                result.append(text)
    if not result:
        raise SimulationConfigError(f"{problem.problem_id}: không tạo được đáp án sai")
    return tuple(result)


@dataclass(frozen=True)
class PreparedProblem:
    """Bài đã chuẩn bị sẵn cho vòng sinh nóng (không gọi SymPy khi sinh)."""

    problem: Problem
    wrong_steps: tuple[tuple[str, ...], ...]  # chỉ số bước − 1 → biểu thức sai dùng được
    wrong_answers: tuple[str, ...]

    @classmethod
    def from_problem(cls, problem: Problem) -> PreparedProblem:
        everything = tuple(m.expression for m in problem.misconceptions)
        per_step = []
        for index in range(1, len(problem.steps) + 1):
            at_step = tuple(m.expression for m in problem.misconceptions if m.step_index == index)
            per_step.append(at_step or everything)
        return cls(problem, tuple(per_step), wrong_answer_candidates(problem))


# (event_offset_ms, session_no, session_seq, event_type, problem, payload, learned_skills)
SimEvent = tuple[int, int, int, str, Problem, dict, tuple[str, ...]]


class LearnerSim:
    """Sinh chuỗi sự kiện đúng thứ tự thời gian của một người học trong [0, horizon_ms]."""

    def __init__(
        self,
        profile: LearnerProfile,
        curriculum: dict[str, tuple[PreparedProblem, ...]],
        settings: BehaviorSettings,
        rng: random.Random,
        horizon_ms: int,
        think_mean_s: float,
        continuous: bool,
    ):
        if not curriculum:
            raise SimulationConfigError("chương trình học rỗng: kho bài không có kỹ năng nào")
        self.profile = profile
        self.settings = settings
        self.rng = rng
        self.horizon_ms = horizon_ms
        self.continuous = continuous
        self.latent: dict[str, bool] = dict(profile.mastered_at_start)
        self.observed: dict[str, learner_rules.SkillProfile] = {}
        sigma = settings.think_sigma
        self._think_mu = math.log(think_mean_s * 1000.0) - sigma * sigma / 2.0
        self._think_sigma = sigma
        self._think_mean_ms = think_mean_s * 1000.0
        skills = list(curriculum)
        start = rng.randrange(len(skills))
        self._order = skills[start:] + skills[:start]
        self._curriculum = curriculum
        self._focus = 0
        self._focus_opportunities = 0
        self._hint_pending = False
        self._t = 0
        self._session_no = 0
        self._seq = 0

    # --- thời gian --------------------------------------------------------
    def _think(self) -> int:
        if self._think_sigma == 0:
            return max(1, round(self._think_mean_ms))
        return max(1, round(self.rng.lognormvariate(self._think_mu, self._think_sigma)))

    def _session_starts(self) -> Iterator[int]:
        if self.continuous:
            # Chế độ tốc độ: bắt đầu so le trong một think-time đầu, các phiên nối tiếp không nghỉ.
            yield int(self.rng.random() * self._think_mean_ms)
            while True:
                yield self._t
        weights = self.settings.sessions_per_day_weights
        days = max(1, -(-self.horizon_ms // DAY_MS))
        for day in range(days):
            if self.horizon_ms >= DAY_MS:
                low, high = day * DAY_MS + _STUDY_WINDOW_MS[0], day * DAY_MS + _STUDY_WINDOW_MS[1]
            else:
                low, high = 0, self.horizon_ms
            count = self.rng.choices(range(len(weights)), weights=weights, k=1)[0]
            for start in sorted(self.rng.randrange(low, high) for _ in range(count)):
                yield start

    # --- mô hình học ------------------------------------------------------
    def _attempt(self, problem: Problem) -> tuple[bool, tuple[str, ...]]:
        rng, params, latent = self.rng, self.profile.params, self.latent
        p = 1.0
        for skill in problem.skill_ids:
            bkt = params[skill]
            p *= (1.0 - bkt.p_slip) if latent[skill] else bkt.p_guess
        correct = rng.random() < p
        bonus = self.settings.hint_learn_bonus if self._hint_pending else 0.0
        self._hint_pending = False
        learned = []
        for skill in problem.skill_ids:
            if not latent[skill] and rng.random() < min(params[skill].p_learn + bonus, 1.0):
                latent[skill] = True
                learned.append(skill)
        return correct, tuple(learned)

    def _observe(self, problem: Problem, event_type: str, payload: dict) -> None:
        observation = learner_rules.observation_for(event_type, payload)
        if observation is None:
            return
        for skill in problem.skill_ids:
            self.observed[skill] = learner_rules.update(self.observed.get(skill, learner_rules.EMPTY_PROFILE), observation)
        if not observation.is_hint and problem.primary_skill == self._order[self._focus]:
            self._focus_opportunities += 1

    def _emit(self, event_type: str, prepared: PreparedProblem, payload: dict, learned: tuple[str, ...] = ()) -> SimEvent:
        self._seq += 1
        self._observe(prepared.problem, event_type, payload)
        return (self._t, self._session_no, self._seq, event_type, prepared.problem, payload, learned)

    def _advance_focus(self) -> None:
        current = self._order[self._focus]
        profile = self.observed.get(current, learner_rules.EMPTY_PROFILE)
        if not (learner_rules.is_mastered(profile) or self._focus_opportunities >= self.settings.max_opportunities_per_skill):
            return
        self._focus_opportunities = 0
        n = len(self._order)
        for step in range(1, n + 1):
            candidate = (self._focus + step) % n
            if not learner_rules.is_mastered(self.observed.get(self._order[candidate], learner_rules.EMPTY_PROFILE)):
                self._focus = candidate
                return
        self._focus = (self._focus + 1) % n  # đã thành thạo hết: ôn lại theo vòng

    # --- sinh sự kiện -----------------------------------------------------
    def _tick(self) -> bool:
        """Tiến đồng hồ một think-time; False nếu vượt khỏi cửa sổ mô phỏng."""
        self._t += self._think()
        return self._t <= self.horizon_ms

    def _problem(self, prepared: PreparedProblem) -> Iterator[SimEvent]:
        problem, rng, settings = prepared.problem, self.rng, self.settings
        self._hint_pending = False
        yield self._emit("open_problem", prepared, {})
        hints = 0
        for step_index, expression in enumerate(problem.steps, start=1):
            wrong, level = 0, 0
            while True:
                if not self._tick():
                    return
                correct, learned = self._attempt(problem)
                if correct:
                    yield self._emit("submit_step", prepared, {"step_index": step_index, "expression": expression, "is_correct": True}, learned)
                    break
                options = prepared.wrong_steps[step_index - 1]
                wrong_expression = options[rng.randrange(len(options))]
                yield self._emit("submit_step", prepared, {"step_index": step_index, "expression": wrong_expression, "is_correct": False}, learned)
                wrong += 1
                if wrong >= settings.max_step_attempts:
                    return  # bỏ bài
                if level < 3 and (wrong >= settings.hint_after_wrong or rng.random() < settings.p_hint):
                    if not self._tick():
                        return
                    level += 1
                    hints += 1
                    self._hint_pending = True
                    yield self._emit("request_hint", prepared, {"hint_level": level, "step_index": step_index})
        last_step = len(problem.steps)
        level = 0
        for attempt in range(1, settings.max_answer_attempts + 1):
            if not self._tick():
                return
            correct, learned = self._attempt(problem)
            if correct:
                yield self._emit("complete", prepared, {"answer": problem.answer, "attempts": attempt, "hints_used": hints}, learned)
                return
            answer = prepared.wrong_answers[rng.randrange(len(prepared.wrong_answers))]
            yield self._emit("wrong_answer", prepared, {"answer": answer, "attempt": attempt}, learned)
            if level < 3 and rng.random() < settings.p_hint:
                if not self._tick():
                    return
                level += 1
                hints += 1
                self._hint_pending = True
                yield self._emit("request_hint", prepared, {"hint_level": level, "step_index": last_step})

    def events(self) -> Iterator[SimEvent]:
        low, high = self.settings.problems_per_session
        for start in self._session_starts():
            if start >= self.horizon_ms:
                if self.continuous:
                    return
                continue
            # Phiên không chồng lên phiên trước: chế độ tự nhiên cách ≥ 5 phút, chế độ tốc độ cách
            # một think-time. Thời gian sự kiện của một người học luôn tăng ngặt.
            if self._session_no == 0:
                gap = 0
            else:
                gap = self._think() if self.continuous else _MIN_SESSION_GAP_MS
            self._t = max(start, self._t + gap)
            if self._t > self.horizon_ms:
                return
            self._session_no += 1
            self._seq = 0
            for index in range(self.rng.randint(low, high)):
                if index > 0 and not self._tick():
                    return
                problems = self._curriculum[self._order[self._focus]]
                yield from self._problem(problems[self.rng.randrange(len(problems))])
                if self._t > self.horizon_ms:
                    return
                self._advance_focus()
