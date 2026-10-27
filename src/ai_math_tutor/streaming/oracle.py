"""Oracle ngoại tuyến: trạng thái cuối *đáng lẽ phải có* từ các sự kiện mà pipeline thực sự nhận.

Đầu vào là tập sự kiện **đã khử trùng** (mỗi `event_id` một lần), **không bị loại** (chỉ những sự
kiện có trong `lake/<run>/applied`), áp **đúng thứ tự** `event_time` (hòa thì `session_seq`, rồi
`event_id`) cho từng người học bằng chính `learner_rules` và `bkt` mà processor dùng. So trạng thái
Redis với oracle tách được hai nguồn sai khác:
- sự kiện bị loại vì muộn hơn watermark → đã bị loại khỏi oracle, không tính là sai trạng thái;
- áp sai thứ tự (chế độ eager) → còn lại trong độ lệch so với oracle.
Chế độ `ordered` vì vậy phải khớp oracle 100% khi mọi sự kiện đến trong ngưỡng watermark.

Tham số BKT lấy từ `ai_math_tutor.streaming.processor.bkt_params_for` (cùng nguồn với processor);
không có bản sao dự phòng để hai bên không lệch nhau âm thầm.
"""

from __future__ import annotations

import json
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from ai_math_tutor import learner_rules
from ai_math_tutor.bkt import BktParams

MODELS = ("rule", "bkt")
EVENT_COLUMNS = ("replay_offset_ms", "event_offset_ms", "sent_offset_ms", "event_id", "learner_id", "class_id",
                 "event_type", "connection", "session_seq", "value")

ParamsFor = Callable[[str], BktParams]


class OracleError(RuntimeError):
    """Không tính được oracle (thiếu tham số BKT, sự kiện sai dạng)."""


@dataclass(frozen=True)
class OracleEvent:
    """Một sự kiện duy nhất của cửa sổ phát lại (thời gian là offset ms từ t0 của mô phỏng)."""

    event_id: str
    learner_id: str
    class_id: str
    event_type: str
    connection: str
    session_seq: int
    event_offset_ms: int
    sent_offset_ms: int
    replay_offset_ms: int
    skill_ids: tuple[str, ...]
    payload: Mapping[str, Any]

    def order_key(self) -> tuple[int, int, str]:
        return (self.event_offset_ms, self.session_seq, self.event_id)


def load_window_events(events_path: Path, cutoff_ms: int | None = None) -> dict[str, OracleEvent]:
    """Mọi `event_id` duy nhất có lần giao đầu với `replay_offset_ms < cutoff` (cả tệp nếu None).

    Bản trùng có chủ đích (cùng `event_id`, cùng byte) chỉ giữ lần đầu; cùng id khác nội dung là lỗi
    hợp đồng của bộ mô phỏng.
    """
    import pyarrow.parquet as pq

    parquet = pq.ParquetFile(str(events_path))
    events: dict[str, OracleEvent] = {}
    values: dict[str, str] = {}
    for batch in parquet.iter_batches(batch_size=65_536, columns=list(EVENT_COLUMNS)):
        columns = {name: batch.column(name).to_pylist() for name in EVENT_COLUMNS}
        for i, replay_ms in enumerate(columns["replay_offset_ms"]):
            if cutoff_ms is not None and replay_ms >= cutoff_ms:
                continue
            event_id = columns["event_id"][i]
            value = columns["value"][i]
            if event_id in events:
                if values[event_id] != value:
                    raise OracleError(f"event_id {event_id} xuất hiện với nội dung khác nhau")
                continue
            body = json.loads(value)
            values[event_id] = value
            events[event_id] = OracleEvent(
                event_id=event_id,
                learner_id=columns["learner_id"][i],
                class_id=columns["class_id"][i],
                event_type=columns["event_type"][i],
                connection=columns["connection"][i],
                session_seq=int(columns["session_seq"][i]),
                event_offset_ms=int(columns["event_offset_ms"][i]),
                sent_offset_ms=int(columns["sent_offset_ms"][i]),
                replay_offset_ms=int(replay_ms),
                skill_ids=tuple(body.get("skill_ids") or ()),
                payload=body.get("payload") or {},
            )
    return events


@dataclass
class ExpectedSkill:
    learner_id: str
    skill_id: str
    class_id: str
    profile: learner_rules.SkillProfile = learner_rules.EMPTY_PROFILE
    p_mastery: float | None = None
    stuck_onset_event_id: str | None = None
    last_event_offset_ms: int | None = None

    @property
    def stuck(self) -> bool:
        return learner_rules.is_wheel_spinning(self.profile)


@dataclass(frozen=True)
class StuckOnset:
    learner_id: str
    class_id: str
    skill_id: str
    onset_event_id: str
    onset_offset_ms: int


@dataclass
class OracleResult:
    model: str
    states: dict[tuple[str, str], ExpectedSkill] = field(default_factory=dict)
    stuck_onsets: list[StuckOnset] = field(default_factory=list)
    events_applied: int = 0


def default_params_for() -> ParamsFor:
    """`bkt_params_for` của processor; thiếu thì báo lỗi rõ ràng thay vì đoán tham số."""
    try:
        from ai_math_tutor.streaming.processor import bkt_params_for
    except ImportError as exc:
        raise OracleError("model=bkt cần ai_math_tutor.streaming.processor.bkt_params_for") from exc
    return bkt_params_for


def compute_oracle(events: Iterable[OracleEvent], *, model: str, params_for: ParamsFor | None = None) -> OracleResult:
    """Áp sự kiện theo thứ tự đúng cho từng người học; trả trạng thái cuối và các onset bế tắc."""
    if model not in MODELS:
        raise ValueError(f"model phải thuộc {MODELS}, nhận {model!r}")
    if model == "bkt" and params_for is None:
        params_for = default_params_for()
    params_cache: dict[str, BktParams] = {}
    result = OracleResult(model=model)
    for event in sorted(events, key=lambda e: (e.learner_id, *e.order_key())):
        result.events_applied += 1
        # Sự kiện không tạo quan sát (open_problem) vẫn "chạm" kỹ năng: trạng thái được tạo với P(L0)
        # như processor làm, chỉ hồ sơ luật không đổi.
        observation = learner_rules.observation_for(event.event_type, event.payload)
        for skill in event.skill_ids:
            key = (event.learner_id, skill)
            expected = result.states.get(key)
            if expected is None:
                expected = ExpectedSkill(event.learner_id, skill, event.class_id)
                if model == "bkt":
                    assert params_for is not None
                    params = params_cache.get(skill)
                    if params is None:
                        params = params_cache[skill] = params_for(skill)
                    expected.p_mastery = params.p_init
                result.states[key] = expected
            was_stuck = expected.stuck
            if observation is not None:
                expected.profile = learner_rules.update(expected.profile, observation)
                if model == "bkt" and observation.correct is not None:
                    expected.p_mastery = params_cache[skill].update(float(expected.p_mastery), observation.correct)
            previous = expected.last_event_offset_ms
            expected.last_event_offset_ms = event.event_offset_ms if previous is None else max(previous,
                                                                                                event.event_offset_ms)
            if expected.stuck and not was_stuck:
                # Onset giữ nguyên sau khi episode kết thúc (cùng quy ước với processor).
                expected.stuck_onset_event_id = event.event_id
                result.stuck_onsets.append(StuckOnset(event.learner_id, event.class_id, skill, event.event_id,
                                                      event.event_offset_ms))
    return result


STATE_FIELDS = ("opportunities", "correct", "consec_wrong", "streak", "mastered", "stuck")


def compare_state(expected: ExpectedSkill, actual: Mapping[str, Any], *, p_tolerance: float = 1e-9) -> list[str]:
    """Tên các trường lệch giữa oracle và trạng thái Redis của một (người học, kỹ năng)."""
    wanted = {
        "opportunities": expected.profile.opportunities,
        "correct": expected.profile.correct,
        "consec_wrong": expected.profile.consec_wrong,
        "streak": expected.profile.streak,
        "mastered": expected.profile.mastered,
        "stuck": expected.stuck,
    }
    mismatched = []
    for name in STATE_FIELDS:
        value = actual.get(name)
        if isinstance(wanted[name], bool):
            if bool(value) != wanted[name]:
                mismatched.append(name)
        elif value is None or int(value) != wanted[name]:
            mismatched.append(name)
    if expected.p_mastery is not None:
        p_actual = actual.get("p_mastery")
        if p_actual is None or abs(float(p_actual) - expected.p_mastery) > p_tolerance:
            mismatched.append("p_mastery")
    return mismatched
