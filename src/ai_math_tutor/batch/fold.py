"""Gấp (fold) chuỗi quan sát của một (người học, kỹ năng) thành hồ sơ — dùng chung cho đường DataFrame và RDD.

Luật duy nhất là `learner_rules` (không viết lại): mỗi quan sát đi qua `learner_rules.update`,
bế tắc = `is_wheel_spinning`, cảnh báo phụ = `consec_wrong_alert` (không bao giờ chấm như bế tắc).
Thứ tự áp: `(event_time_ms, session_seq, event_id)` tăng dần trên tập đã khử trùng — `event_id`
chỉ để phá hòa cho tất định.

Ngoài trạng thái cuối, fold ghi **episode bế tắc** mà nó thấy trong lịch sử: sự kiện làm vị từ
chuyển sang True (onset, "sự kiện kích hoạt") và sự kiện làm nó về False (kết thúc). Vì
`mastered` dính (không mất lại), mỗi (người học, kỹ năng) có tối đa một episode; fold kiểm bất
biến này và báo lỗi nếu luật đổi khiến có episode thứ hai.
Thuần Python, không phụ thuộc Spark/pandas.
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping
from typing import Any

from ai_math_tutor import learner_rules

# Loại sự kiện có ảnh hưởng tới hồ sơ (lấy từ learner_rules, không liệt kê lại).
PROFILE_EVENT_TYPES = frozenset(learner_rules.OPPORTUNITY_EVENT_TYPES | {learner_rules.HINT_EVENT_TYPE})

# (tên cột, kiểu Spark DDL) của hồ sơ — thứ tự cột của mọi đầu ra hồ sơ.
PROFILE_FIELDS: tuple[tuple[str, str], ...] = (
    ("learner_id", "string"),
    ("skill_id", "string"),
    ("opportunities", "int"),
    ("correct", "int"),
    ("hints_used", "int"),
    ("consec_wrong", "int"),
    ("streak", "int"),
    ("mastered", "boolean"),
    ("stuck", "boolean"),
    ("consec_wrong_alert", "boolean"),
    ("last_event_time_ms", "bigint"),
    ("onset_event_id", "string"),
    ("onset_event_time_ms", "bigint"),
    ("onset_sent_time_ms", "bigint"),
    ("end_event_id", "string"),
    ("end_event_time_ms", "bigint"),
)
PROFILE_COLUMNS: tuple[str, ...] = tuple(name for name, _ in PROFILE_FIELDS)
PROFILE_DDL = ", ".join(f"{name} {kind}" for name, kind in PROFILE_FIELDS)
# Cột đếm: combiner-safe (cộng được theo bất kỳ cách chia nào).
COUNT_COLUMNS = ("opportunities", "correct", "hints_used")
# Khóa sắp xếp trong một (người học, kỹ năng).
ORDER_COLUMNS = ("event_time_ms", "session_seq", "event_id")


class FoldInvariantError(RuntimeError):
    """Chuỗi quan sát vi phạm giả định của fold (ví dụ hai episode bế tắc cho cùng một kỹ năng)."""


def observation_of(event_type: str, is_correct: Any) -> learner_rules.Observation | None:
    """Quan sát của một sự kiện qua `learner_rules.observation_for` (ValueError nếu submit_step thiếu is_correct bool)."""
    return learner_rules.observation_for(event_type, {"is_correct": is_correct})


def fold_skill(
    learner_id: str,
    skill_id: str,
    observations: Iterable[tuple[int, int, str, learner_rules.Observation]],
) -> dict[str, Any]:
    """Áp lần lượt các quan sát `(event_time_ms, sent_time_ms, event_id, observation)` đã sắp theo ORDER_COLUMNS."""
    profile = learner_rules.EMPTY_PROFILE
    last_event_time: int | None = None
    onset: tuple[str, int, int] | None = None
    end: tuple[str, int] | None = None
    for event_time_ms, sent_time_ms, event_id, observation in observations:
        was_stuck = learner_rules.is_wheel_spinning(profile)
        profile = learner_rules.update(profile, observation)
        now_stuck = learner_rules.is_wheel_spinning(profile)
        last_event_time = event_time_ms
        if now_stuck and not was_stuck:
            if onset is not None:
                raise FoldInvariantError(f"episode bế tắc thứ hai cho ({learner_id}, {skill_id}) tại {event_id}")
            onset = (event_id, event_time_ms, sent_time_ms)
        elif was_stuck and not now_stuck:
            end = (event_id, event_time_ms)
    if last_event_time is None:
        raise FoldInvariantError(f"không có quan sát nào cho ({learner_id}, {skill_id})")
    return {
        "learner_id": learner_id,
        "skill_id": skill_id,
        "opportunities": profile.opportunities,
        "correct": profile.correct,
        "hints_used": profile.hints_used,
        "consec_wrong": profile.consec_wrong,
        "streak": profile.streak,
        "mastered": learner_rules.is_mastered(profile),
        "stuck": learner_rules.is_wheel_spinning(profile),
        "consec_wrong_alert": learner_rules.consec_wrong_alert(profile),
        "last_event_time_ms": last_event_time,
        "onset_event_id": onset[0] if onset else None,
        "onset_event_time_ms": onset[1] if onset else None,
        "onset_sent_time_ms": onset[2] if onset else None,
        "end_event_id": end[0] if end else None,
        "end_event_time_ms": end[1] if end else None,
    }


def count_contribution(observation: learner_rules.Observation) -> tuple[int, int, int]:
    """Phần đóng góp (opportunities, correct, hints_used) của một quan sát — phép cộng nên combiner-safe."""
    if observation.is_hint:
        return (0, 0, 1)
    return (1, 1 if observation.correct else 0, 0)


def profile_row(profile: Mapping[str, Any]) -> tuple[Any, ...]:
    """Hồ sơ dạng tuple theo PROFILE_COLUMNS (để tạo DataFrame từ RDD)."""
    return tuple(profile[name] for name in PROFILE_COLUMNS)
