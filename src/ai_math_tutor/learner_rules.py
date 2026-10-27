"""Luật hồ sơ người học dùng chung cho bộ mô phỏng (ground truth), batch và streaming.

Một định nghĩa bế tắc duy nhất (wheel spinning, Beck & Gong 2013 theo cách vận hành
của Zhang et al.): người học đã có ≥ 10 cơ hội trên một kỹ năng mà chưa từng đúng
3 lần liên tiếp. "Thành thạo" theo luật = đã đạt 3 lần đúng liên tiếp (dính, không
mất lại); khác với ngưỡng xác suất của BKT (`bkt.DEFAULT_MASTERY_THRESHOLD`).
`consec_wrong_alert` (≥ 3 lần sai liên tiếp) chỉ là cảnh báo phụ, không bao giờ
được chấm như bế tắc.

Ánh xạ sự kiện → quan sát (cùng một hàm cho mọi nơi, áp cho **mọi** kỹ năng trong
`skill_ids` của sự kiện):
- `submit_step`: một cơ hội, đúng = `payload.is_correct`;
- `wrong_answer`: một cơ hội sai (nộp đáp án cuối sai);
- `complete`: một cơ hội đúng (nộp đáp án cuối đúng);
- `request_hint`: chỉ tăng `hints_used`, không phải cơ hội, không đổi chuỗi;
- `open_problem` (và loại khác): bỏ qua.

Luật phụ thuộc thứ tự: người gọi phải áp sự kiện theo `event_time` tăng dần
(hòa thì theo `session_seq`) trên tập đã khử trùng `event_id`.
Thuần Python, không phụ thuộc Spark/pandas.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass, replace
from typing import Any

WHEEL_SPIN_OPPORTUNITIES = 10
MASTERY_STREAK = 3
CONSEC_WRONG_ALERT = 3

OPPORTUNITY_EVENT_TYPES = frozenset({"submit_step", "wrong_answer", "complete"})
HINT_EVENT_TYPE = "request_hint"


@dataclass(frozen=True, slots=True)
class Observation:
    """Một quan sát trên một kỹ năng: cơ hội làm bài (`correct` là bool) hoặc xin gợi ý (`correct` là None)."""

    correct: bool | None

    @property
    def is_hint(self) -> bool:
        return self.correct is None


OBSERVATION_CORRECT = Observation(True)
OBSERVATION_WRONG = Observation(False)
OBSERVATION_HINT = Observation(None)


def observation_for(event_type: str, payload: Mapping[str, Any]) -> Observation | None:
    """Quan sát mà một sự kiện tạo ra (None nếu sự kiện không ảnh hưởng hồ sơ)."""
    if event_type == "submit_step":
        correct = payload.get("is_correct")
        if not isinstance(correct, bool):
            raise ValueError("submit_step thiếu payload.is_correct kiểu bool")
        return OBSERVATION_CORRECT if correct else OBSERVATION_WRONG
    if event_type == "complete":
        return OBSERVATION_CORRECT
    if event_type == "wrong_answer":
        return OBSERVATION_WRONG
    if event_type == HINT_EVENT_TYPE:
        return OBSERVATION_HINT
    return None


@dataclass(frozen=True, slots=True)
class SkillProfile:
    """Hồ sơ luật của một (người học, kỹ năng). Bất biến: `update` trả bản mới."""

    opportunities: int = 0
    correct: int = 0
    hints_used: int = 0
    consec_wrong: int = 0
    streak: int = 0
    mastered: bool = False

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> SkillProfile:
        return cls(
            opportunities=int(data["opportunities"]),
            correct=int(data["correct"]),
            hints_used=int(data["hints_used"]),
            consec_wrong=int(data["consec_wrong"]),
            streak=int(data["streak"]),
            mastered=bool(data["mastered"]),
        )


EMPTY_PROFILE = SkillProfile()


def update(profile: SkillProfile, observation: Observation) -> SkillProfile:
    """Áp một quan sát lên hồ sơ (hàm thuần)."""
    if observation.correct is None:
        return replace(profile, hints_used=profile.hints_used + 1)
    if observation.correct:
        streak = profile.streak + 1
        return SkillProfile(
            opportunities=profile.opportunities + 1,
            correct=profile.correct + 1,
            hints_used=profile.hints_used,
            consec_wrong=0,
            streak=streak,
            mastered=profile.mastered or streak >= MASTERY_STREAK,
        )
    return SkillProfile(
        opportunities=profile.opportunities + 1,
        correct=profile.correct,
        hints_used=profile.hints_used,
        consec_wrong=profile.consec_wrong + 1,
        streak=0,
        mastered=profile.mastered,
    )


def is_mastered(profile: SkillProfile) -> bool:
    """Đã từng đúng 3 lần liên tiếp trên kỹ năng."""
    return profile.mastered


def is_wheel_spinning(profile: SkillProfile) -> bool:
    """Bế tắc: ≥ 10 cơ hội mà chưa thành thạo. Trở về False khi đạt 3 đúng liên tiếp (kết thúc episode)."""
    return profile.opportunities >= WHEEL_SPIN_OPPORTUNITIES and not profile.mastered


def consec_wrong_alert(profile: SkillProfile) -> bool:
    """Cảnh báo phụ: ≥ 3 lần sai liên tiếp. Không phải tín hiệu bế tắc."""
    return profile.consec_wrong >= CONSEC_WRONG_ALERT


def apply_event(profiles: Mapping[str, SkillProfile], event: Mapping[str, Any]) -> dict[str, SkillProfile]:
    """Áp một sự kiện hợp đồng (dict) lên hồ sơ các kỹ năng của người học; trả dict mới."""
    result = dict(profiles)
    observation = observation_for(event["event_type"], event["payload"])
    if observation is None:
        return result
    for skill in event["skill_ids"]:
        result[skill] = update(result.get(skill, EMPTY_PROFILE), observation)
    return result
