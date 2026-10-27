"""Trạng thái người học trong streaming: logic thuần `apply_events` + `LearnerStateProcessor` bọc cho Spark.

Luật hồ sơ chỉ có một nguồn: `learner_rules` (cơ hội, chuỗi đúng/sai, thành thạo 3 đúng liên tiếp,
bế tắc = `is_wheel_spinning`, cảnh báo phụ = `consec_wrong_alert`) và `bkt` (P(L) khi model = bkt).
Mô-đun này chỉ quyết định **thứ tự áp** và **khi nào áp**, theo ba chế độ:

- `eager`: sắp các sự kiện trong micro-batch theo `(event_time, session_seq, event_id)` rồi áp ngay.
  Sự kiện có `event_time` cũ hơn sự kiện mới nhất đã áp của người học vẫn được áp (sai thứ tự)
  và được gắn cờ `applied_out_of_order`.
- `ordered`: đệm sự kiện (ListState) và chỉ áp khi watermark ≥ `event_time`; timer event-time đặt
  tại `event_time` nhỏ nhất đang đệm. Đúng thứ tự cho mọi sự kiện không bị loại theo watermark,
  đổi lại độ trễ thêm ≈ W.
- `none`: không watermark (`timeMode="None"`), áp ngay như eager; khử trùng trong state bằng MapState
  `seen_event_ids` khi `dedup` bật, mục cũ hơn sự kiện mới nhất của người học quá TTL bị tỉa theo
  event time. Không có timer nên không có cảnh báo "không hoạt động" và không hết hạn state.

Mỗi lần gọi trả các dòng theo `schema.OUTPUT_SCHEMA`:
- `update`: một dòng cho mỗi (người học, kỹ năng) bị chạm. Mọi sự kiện đã áp (kể cả `open_problem`,
  `request_hint`) được liệt kê **đúng một lần** trong `applied_event_ids` của dòng update ứng với
  `skill_ids[0]` của nó, kèm `event_time`, `sent_time`, cờ sai thứ tự — đủ để đối soát mọi loại sự
  kiện và đo độ trễ cho từng sự kiện.
- `alert`: `stuck` (chuyển sang bế tắc, một lần mỗi episode), `consec_wrong` (lần sai thứ 3 liên
  tiếp), `inactivity` (timer: không có sự kiện trong `inactivity_ms` khi bài cuối chưa xong).
  `alert_id` = sha256(learner, skill, loại, sự kiện kích hoạt)[:24] — tất định, nên batch chạy lại
  sinh đúng id cũ và sinks khử trùng được.
- `progress`: một dòng cho mỗi (người học, bài) bị chạm (số lần sai đáp án cuối, bước đúng cao nhất,
  đã hoàn thành).

`state_version` tăng 1 mỗi sự kiện chạm vào bản ghi (kỹ năng hoặc bài), tất định theo dữ liệu vào.
Nó chỉ đơn điệu trong một "đời" trạng thái: khi state hết hạn (TTL theo event time) rồi người học
quay lại, version bắt đầu lại từ 1. Vì vậy sinks so cặp (batch_id, state_version) chứ không so
version một mình (xem `redis_upsert.lua`).
"""

from __future__ import annotations

import functools
import hashlib
import math
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import pandas as pd
from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle

from ai_math_tutor import learner_rules
from ai_math_tutor.bkt import BktParams
from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.streaming.schema import ALERT_TYPES, OUTPUT_COLUMNS, PROCESSOR_INPUT_COLUMNS

MODES = ("eager", "ordered", "none")
MODELS = ("rule", "bkt")
DEFAULT_STATE_TTL_MS = 24 * 3600 * 1000
DEFAULT_INACTIVITY_MS = 300_000
# Chế độ none tỉa seen_event_ids mỗi TTL/24 event time của người học (≈ 1 h với TTL 24 h).
SEEN_PRUNE_FRACTION = 24
ALERT_ID_LENGTH = 24
SKILLS_CONFIG_PATH = REPO_ROOT / "configs" / "skills.yaml"


class ProcessorError(ValueError):
    """Tham số processor sai hoặc sự kiện vi phạm bất biến (đáng lẽ đã bị lọc vào dead-letter)."""


# ---------------------------------------------------------------- tham số BKT của bộ theo dõi

@functools.lru_cache(maxsize=4)
def _bkt_table(path: str) -> tuple[dict[str, BktParams], BktParams]:
    from ai_math_tutor.simulator.population import load_skills_config

    config = load_skills_config(Path(path))
    table: dict[str, BktParams] = {}
    for skill in config.skills:
        table[skill.skill_id] = BktParams(
            p_init=_mid(skill.p_init.low, skill.p_init.high),
            p_learn=_mid(skill.p_learn.low, skill.p_learn.high),
            p_guess=_mid(skill.p_guess.low, skill.p_guess.high),
            p_slip=_mid(skill.p_slip.low, skill.p_slip.high),
        )
    count = len(table)
    fallback = BktParams(
        p_init=sum(p.p_init for p in table.values()) / count,
        p_learn=sum(p.p_learn for p in table.values()) / count,
        p_guess=sum(p.p_guess for p in table.values()) / count,
        p_slip=sum(p.p_slip for p in table.values()) / count,
    )
    return table, fallback


def _mid(low: float, high: float) -> float:
    return (low + high) / 2.0


def bkt_params_for(skill_id: str, skills_path: Path | str = SKILLS_CONFIG_PATH) -> BktParams:
    """Tham số BKT của **bộ theo dõi** cho một kỹ năng — một nguồn duy nhất cho processor và oracle.

    Quy tắc: mỗi tham số (L0, T, G, S) = trung điểm khoảng `[thấp, cao]` của kỹ năng trong
    `configs/skills.yaml`. Kỹ năng không có trong cấu hình nhận trung bình các trung điểm của mọi
    kỹ năng. Không dùng tham số ẩn theo người học của bộ mô phỏng (đó là ground truth).
    """
    table, fallback = _bkt_table(str(skills_path))
    return table.get(skill_id, fallback)


@dataclass(frozen=True)
class LearnerModel:
    """Mô hình cập nhật: `rule` chỉ bộ đếm theo luật; `bkt` thêm P(L) theo `bkt_params_for`."""

    name: str
    params: Mapping[str, BktParams] = field(default_factory=dict)
    fallback: BktParams | None = None

    def __post_init__(self) -> None:
        if self.name not in MODELS:
            raise ProcessorError(f"model phải thuộc {MODELS}, nhận {self.name!r}")
        if self.name == "bkt" and self.fallback is None:
            raise ProcessorError("model bkt cần bảng tham số")

    @property
    def uses_bkt(self) -> bool:
        return self.name == "bkt"

    def params_for(self, skill_id: str) -> BktParams:
        assert self.fallback is not None
        return self.params.get(skill_id, self.fallback)


def resolve_model(model: LearnerModel | str, skills_path: Path | str = SKILLS_CONFIG_PATH) -> LearnerModel:
    """Chuỗi `rule`/`bkt` → `LearnerModel` (bảng tham số chụp một lần để gửi kèm processor tới worker)."""
    if isinstance(model, LearnerModel):
        return model
    if model == "bkt":
        table, fallback = _bkt_table(str(skills_path))
        return LearnerModel("bkt", dict(table), fallback)
    return LearnerModel(model)


# ---------------------------------------------------------------- sự kiện và trạng thái

@dataclass(frozen=True, slots=True)
class Event:
    """Một sự kiện học tập đã qua kiểm hợp đồng (heartbeat không bao giờ tới đây)."""

    event_id: str
    event_type: str
    event_time_ms: int
    sent_time_ms: int
    session_seq: int
    class_id: str
    problem_id: str
    skill_ids: tuple[str, ...]
    is_correct: bool | None = None
    step_index: int | None = None

    @property
    def order_key(self) -> tuple[int, int, str]:
        """Thứ tự áp: event_time, rồi session_seq, rồi event_id (chỉ để phá hòa tất định)."""
        return (self.event_time_ms, self.session_seq, self.event_id)

    def to_tuple(self) -> tuple[Any, ...]:
        return (self.event_id, self.event_type, self.event_time_ms, self.sent_time_ms, self.session_seq,
                self.class_id, self.problem_id, list(self.skill_ids), self.is_correct, self.step_index)

    @classmethod
    def from_tuple(cls, row: tuple[Any, ...]) -> Event:
        (event_id, event_type, event_time_ms, sent_time_ms, session_seq, class_id, problem_id, skill_ids,
         is_correct, step_index) = row
        return cls(str(event_id), str(event_type), int(event_time_ms), int(sent_time_ms), int(session_seq),
                   str(class_id), str(problem_id), tuple(str(s) for s in skill_ids),
                   None if _missing(is_correct) else bool(is_correct),
                   None if _missing(step_index) else int(step_index))


EVENT_STATE_DDL = (
    "event_id STRING, event_type STRING, event_time_ms BIGINT, sent_time_ms BIGINT, session_seq BIGINT, "
    "class_id STRING, problem_id STRING, skill_ids ARRAY<STRING>, is_correct BOOLEAN, step_index BIGINT"
)


@dataclass(frozen=True, slots=True)
class SkillState:
    """Trạng thái một (người học, kỹ năng): hồ sơ luật + P(L) + episode bế tắc + phiên bản."""

    profile: learner_rules.SkillProfile = learner_rules.EMPTY_PROFILE
    p_mastery: float | None = None
    stuck_onset_event_id: str | None = None
    last_event_time_ms: int | None = None
    state_version: int = 0

    @property
    def stuck(self) -> bool:
        return learner_rules.is_wheel_spinning(self.profile)

    def to_tuple(self) -> tuple[Any, ...]:
        p = self.profile
        return (self.p_mastery, p.opportunities, p.correct, p.hints_used, p.consec_wrong, p.streak, p.mastered,
                self.stuck, self.stuck_onset_event_id, self.last_event_time_ms, self.state_version)

    @classmethod
    def from_tuple(cls, row: tuple[Any, ...]) -> SkillState:
        (p_mastery, opportunities, correct, hints_used, consec_wrong, streak, mastered, _stuck, onset,
         last_event_time_ms, state_version) = row
        profile = learner_rules.SkillProfile(int(opportunities), int(correct), int(hints_used), int(consec_wrong),
                                             int(streak), bool(mastered))
        return cls(profile, None if _missing(p_mastery) else float(p_mastery), None if _missing(onset) else str(onset),
                   None if _missing(last_event_time_ms) else int(last_event_time_ms), int(state_version))


SKILL_STATE_DDL = (
    "p_mastery DOUBLE, opportunities INT, correct INT, hints_used INT, consec_wrong INT, streak INT, "
    "mastered BOOLEAN, stuck BOOLEAN, stuck_onset_event_id STRING, last_event_time_ms BIGINT, "
    "state_version BIGINT"
)


@dataclass(frozen=True, slots=True)
class ProblemState:
    """Tiến độ một (người học, bài) cho thang gợi ý: số lần sai đáp án cuối, bước đúng cao nhất, xong chưa."""

    wrong_answers: int = 0
    last_step: int = 0
    completed: bool = False
    last_event_time_ms: int | None = None
    state_version: int = 0

    def to_tuple(self) -> tuple[Any, ...]:
        return (self.wrong_answers, self.last_step, self.completed, self.last_event_time_ms, self.state_version)

    @classmethod
    def from_tuple(cls, row: tuple[Any, ...]) -> ProblemState:
        wrong_answers, last_step, completed, last_event_time_ms, state_version = row
        return cls(int(wrong_answers), int(last_step), bool(completed),
                   None if _missing(last_event_time_ms) else int(last_event_time_ms), int(state_version))


PROBLEM_STATE_DDL = (
    "wrong_answers INT, last_step INT, completed BOOLEAN, last_event_time_ms BIGINT, state_version BIGINT"
)


@dataclass(frozen=True, slots=True)
class LearnerMeta:
    """Phần trạng thái theo người học (một ValueState): lớp, mốc thứ tự, sự kiện mới nhất, timer đang đặt."""

    class_id: str | None = None
    max_applied_ms: int | None = None
    last_seen_ms: int | None = None
    last_seen_event_id: str | None = None
    last_seen_skill_id: str | None = None
    last_seen_problem_id: str | None = None
    inactivity_due_ms: int | None = None
    buffer_due_ms: int | None = None
    buffer_size: int = 0
    expire_due_ms: int | None = None
    seen_prune_due_ms: int | None = None

    def to_tuple(self) -> tuple[Any, ...]:
        return (self.class_id, self.max_applied_ms, self.last_seen_ms, self.last_seen_event_id,
                self.last_seen_skill_id, self.last_seen_problem_id, self.inactivity_due_ms, self.buffer_due_ms,
                self.buffer_size, self.expire_due_ms, self.seen_prune_due_ms)

    @classmethod
    def from_tuple(cls, row: tuple[Any, ...] | None) -> LearnerMeta:
        if row is None:
            return cls()
        values = [None if _missing(v) else v for v in row]
        (class_id, max_applied, last_seen, last_id, last_skill, last_problem, inactivity_due, buffer_due,
         buffer_size, expire_due, seen_prune_due) = values
        return cls(class_id, _int_or_none(max_applied), _int_or_none(last_seen), last_id, last_skill, last_problem,
                   _int_or_none(inactivity_due), _int_or_none(buffer_due), int(buffer_size or 0),
                   _int_or_none(expire_due), _int_or_none(seen_prune_due))

    @property
    def timers(self) -> frozenset[int]:
        """Các timer event-time mà trạng thái này cần (gộp khi trùng thời điểm)."""
        return frozenset(t for t in (self.inactivity_due_ms, self.buffer_due_ms, self.expire_due_ms)
                         if t is not None)


META_STATE_DDL = (
    "class_id STRING, max_applied_ms BIGINT, last_seen_ms BIGINT, last_seen_event_id STRING, "
    "last_seen_skill_id STRING, last_seen_problem_id STRING, inactivity_due_ms BIGINT, buffer_due_ms BIGINT, "
    "buffer_size INT, expire_due_ms BIGINT, seen_prune_due_ms BIGINT"
)


def _missing(value: Any) -> bool:
    """None, NaN (số nguyên null qua pandas) hoặc pandas.NA."""
    return value is None or value is pd.NA or (isinstance(value, float) and math.isnan(value))


def _int_or_none(value: Any) -> int | None:
    return None if value is None else int(value)


class LearnerState:
    """Trạng thái của một người học trong một lần gọi: đọc lười từng mục, ghi nhận mục bẩn để lưu lại.

    `load_skill`, `load_problem`, `load_seen` đọc từ kho (MapState trên Spark, dict trong test);
    mỗi khóa đọc tối đa một lần. Sau `apply_events`, người gọi lưu `dirty_skills`, `dirty_problems`,
    `new_seen`, `buffer` (nếu `buffer_dirty`) và `meta`.
    """

    def __init__(
        self,
        learner_id: str,
        meta: LearnerMeta | None = None,
        *,
        load_skill: Callable[[str], SkillState | None] | None = None,
        load_problem: Callable[[str], ProblemState | None] | None = None,
        load_seen: Callable[[str], bool] | None = None,
        buffer: Iterable[Event] = (),
    ) -> None:
        self.learner_id = learner_id
        self.meta = meta or LearnerMeta()
        self._load_skill = load_skill or (lambda _sid: None)
        self._load_problem = load_problem or (lambda _pid: None)
        self._load_seen = load_seen or (lambda _eid: False)
        self.skills: dict[str, SkillState] = {}
        self.problems: dict[str, ProblemState] = {}
        self.dirty_skills: set[str] = set()
        self.dirty_problems: set[str] = set()
        self.new_seen: dict[str, int] = {}
        self.buffer: list[Event] = list(buffer)
        self.buffer_dirty = False
        self.expired = False
        # Chế độ none: xóa các mục seen_event_ids có event_time < mốc này khi lưu.
        self.prune_seen_before: int | None = None

    def skill(self, skill_id: str) -> SkillState:
        if skill_id not in self.skills:
            self.skills[skill_id] = self._load_skill(skill_id) or SkillState()
        return self.skills[skill_id]

    def set_skill(self, skill_id: str, value: SkillState) -> None:
        self.skills[skill_id] = value
        self.dirty_skills.add(skill_id)

    def problem(self, problem_id: str) -> ProblemState:
        if problem_id not in self.problems:
            self.problems[problem_id] = self._load_problem(problem_id) or ProblemState()
        return self.problems[problem_id]

    def set_problem(self, problem_id: str, value: ProblemState) -> None:
        self.problems[problem_id] = value
        self.dirty_problems.add(problem_id)

    def seen(self, event_id: str) -> bool:
        return event_id in self.new_seen or self._load_seen(event_id)

    def mark_seen(self, event_id: str, event_time_ms: int) -> None:
        self.new_seen[event_id] = event_time_ms

    def set_buffer(self, events: list[Event]) -> None:
        self.buffer = events
        self.buffer_dirty = True


# ---------------------------------------------------------------- logic thuần

def alert_id(learner_id: str, skill_id: str | None, alert_type: str, onset_event_id: str) -> str:
    """Mã cảnh báo tất định: sha256("learner|skill|loại|sự kiện kích hoạt") lấy 24 ký tự hex đầu."""
    text = "|".join((learner_id, skill_id or "", alert_type, onset_event_id))
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:ALERT_ID_LENGTH]


@dataclass
class _Applied:
    ids: list[str] = field(default_factory=list)
    event_times: list[int] = field(default_factory=list)
    sent_times: list[int] = field(default_factory=list)
    out_of_order: list[bool] = field(default_factory=list)


class _Output:
    """Gom các dòng đầu ra của một lần gọi (một dòng update cho mỗi kỹ năng, mỗi bài một dòng progress)."""

    def __init__(self) -> None:
        self.skills: dict[str, _Applied] = {}
        self.problems: list[str] = []
        self.alerts: list[dict[str, Any]] = []

    def touch_skill(self, skill_id: str) -> _Applied:
        return self.skills.setdefault(skill_id, _Applied())

    def touch_problem(self, problem_id: str) -> None:
        if problem_id not in self.problems:
            self.problems.append(problem_id)

    def rows(self, state: LearnerState) -> list[dict[str, Any]]:
        rows: list[dict[str, Any]] = []
        for skill_id, applied in self.skills.items():
            s = state.skill(skill_id)
            rows.append(_row(
                kind="update", learner_id=state.learner_id, class_id=state.meta.class_id, skill_id=skill_id,
                p_mastery=s.p_mastery, opportunities=s.profile.opportunities, correct=s.profile.correct,
                consec_wrong=s.profile.consec_wrong, streak=s.profile.streak, mastered=s.profile.mastered,
                stuck=s.stuck, stuck_onset_event_id=s.stuck_onset_event_id, state_version=s.state_version,
                last_event_time_ms=s.last_event_time_ms, applied_event_ids=applied.ids,
                applied_event_time_ms=applied.event_times, applied_sent_time_ms=applied.sent_times,
                applied_out_of_order=applied.out_of_order,
            ))
        rows.extend(self.alerts)
        for problem_id in self.problems:
            p = state.problem(problem_id)
            rows.append(_row(
                kind="progress", learner_id=state.learner_id, class_id=state.meta.class_id, problem_id=problem_id,
                wrong_answers=p.wrong_answers, last_step=p.last_step, completed=p.completed,
                state_version=p.state_version, last_event_time_ms=p.last_event_time_ms,
            ))
        return rows


def _row(**values: Any) -> dict[str, Any]:
    unknown = set(values) - set(OUTPUT_COLUMNS)
    if unknown:  # lỗi lập trình: cột ngoài hợp đồng đầu ra
        raise ProcessorError(f"cột không có trong OUTPUT_SCHEMA: {sorted(unknown)}")
    return {column: values.get(column) for column in OUTPUT_COLUMNS}


def _alert(state: LearnerState, skill_id: str | None, alert_type: str, onset_event_id: str,
           event_time_ms: int) -> dict[str, Any]:
    return _row(kind="alert", learner_id=state.learner_id, class_id=state.meta.class_id, skill_id=skill_id,
                stuck_onset_event_id=onset_event_id, alert_type=alert_type,
                alert_id=alert_id(state.learner_id, skill_id, alert_type, onset_event_id),
                alert_event_time_ms=event_time_ms)


def _observation(event: Event) -> learner_rules.Observation | None:
    try:
        return learner_rules.observation_for(event.event_type, {"is_correct": event.is_correct})
    except ValueError as exc:  # submit_step không có is_correct bool: đáng lẽ đã vào dead-letter
        raise ProcessorError(f"sự kiện {event.event_id}: {exc}") from exc


def _apply_one(state: LearnerState, event: Event, model: LearnerModel, out: _Output) -> None:
    if not event.skill_ids:
        raise ProcessorError(f"sự kiện {event.event_id} không có skill_ids")
    meta = state.meta
    out_of_order = meta.max_applied_ms is not None and event.event_time_ms < meta.max_applied_ms
    state.meta = replace(
        meta,
        class_id=event.class_id or meta.class_id,
        max_applied_ms=event.event_time_ms if meta.max_applied_ms is None else max(meta.max_applied_ms,
                                                                                    event.event_time_ms),
    )
    observation = _observation(event)
    for skill_id in event.skill_ids:
        current = state.skill(skill_id)
        profile = current.profile
        p_mastery = current.p_mastery
        if model.uses_bkt and p_mastery is None:
            p_mastery = model.params_for(skill_id).p_init
        if observation is not None:
            profile = learner_rules.update(profile, observation)
            if model.uses_bkt and not observation.is_hint:
                assert p_mastery is not None and observation.correct is not None
                p_mastery = model.params_for(skill_id).update(p_mastery, observation.correct)
        onset = current.stuck_onset_event_id
        if learner_rules.is_wheel_spinning(profile) and not current.stuck:
            onset = event.event_id
            out.alerts.append(_alert(state, skill_id, "stuck", event.event_id, event.event_time_ms))
        if learner_rules.consec_wrong_alert(profile) and not learner_rules.consec_wrong_alert(current.profile):
            out.alerts.append(_alert(state, skill_id, "consec_wrong", event.event_id, event.event_time_ms))
        last = event.event_time_ms if current.last_event_time_ms is None else max(current.last_event_time_ms,
                                                                                   event.event_time_ms)
        state.set_skill(skill_id, SkillState(profile, p_mastery, onset, last, current.state_version + 1))
        out.touch_skill(skill_id)
    applied = out.touch_skill(event.skill_ids[0])
    applied.ids.append(event.event_id)
    applied.event_times.append(event.event_time_ms)
    applied.sent_times.append(event.sent_time_ms)
    applied.out_of_order.append(out_of_order)

    problem = state.problem(event.problem_id)
    wrong_answers, last_step, completed = problem.wrong_answers, problem.last_step, problem.completed
    if event.event_type == "wrong_answer":
        wrong_answers += 1
    elif event.event_type == "complete":
        completed = True
    elif event.event_type == "submit_step" and event.is_correct and event.step_index is not None:
        last_step = max(last_step, event.step_index)
    last_problem_ms = event.event_time_ms if problem.last_event_time_ms is None else max(
        problem.last_event_time_ms, event.event_time_ms)
    state.set_problem(event.problem_id, ProblemState(wrong_answers, last_step, completed, last_problem_ms,
                                                     problem.state_version + 1))
    out.touch_problem(event.problem_id)


def _note_seen(state: LearnerState, events: list[Event], inactivity_ms: int | None, expire_ms: int | None) -> None:
    """Cập nhật sự kiện mới nhất (theo event_time); hẹn lại timer không hoạt động và hết hạn khi nó đổi."""
    if not events:
        return
    newest = max(events, key=lambda e: e.order_key)
    meta = state.meta
    if meta.last_seen_ms is not None and newest.event_time_ms <= meta.last_seen_ms:
        return  # chỉ sự kiện cũ hơn đến muộn: không đổi mốc hoạt động cuối
    state.meta = replace(
        meta,
        class_id=newest.class_id or meta.class_id,
        last_seen_ms=newest.event_time_ms,
        last_seen_event_id=newest.event_id,
        last_seen_skill_id=newest.skill_ids[0] if newest.skill_ids else None,
        last_seen_problem_id=newest.problem_id,
        inactivity_due_ms=None if not inactivity_ms else newest.event_time_ms + inactivity_ms,
        expire_due_ms=None if not expire_ms else newest.event_time_ms + expire_ms,
    )


def _schedule_seen_prune(state: LearnerState, ttl_ms: int) -> None:
    """Chế độ none: tỉa seen_event_ids theo event time, tối đa một lần mỗi ttl/24 của người học."""
    meta = state.meta
    newest = meta.last_seen_ms
    if newest is None:
        return
    interval = max(1, ttl_ms // SEEN_PRUNE_FRACTION)
    if meta.seen_prune_due_ms is None:
        state.meta = replace(meta, seen_prune_due_ms=newest + interval)
    elif newest >= meta.seen_prune_due_ms:
        state.prune_seen_before = newest - ttl_ms
        state.meta = replace(meta, seen_prune_due_ms=newest + interval)


def apply_events(
    state: LearnerState,
    events: Iterable[Event],
    mode: str,
    model: LearnerModel | str,
    *,
    watermark_ms: int | None = None,
    dedup: bool = True,
    inactivity_ms: int | None = DEFAULT_INACTIVITY_MS,
    expire_ms: int | None = DEFAULT_STATE_TTL_MS,
) -> list[dict[str, Any]]:
    """Áp các sự kiện mới (có thể rỗng — lần gọi do timer) lên `state`; trả các dòng đầu ra.

    `watermark_ms`: watermark hiện tại của batch (bỏ qua ở chế độ none). Với ordered, sự kiện có
    `event_time ≤ watermark` (đang đệm hoặc mới tới) được áp theo thứ tự; còn lại vào đệm.
    Việc đến hạn (đệm, không hoạt động) được xử lý ngay trong lần gọi này khi watermark đã qua hạn.
    `dedup` chỉ có tác dụng ở chế độ none (eager/ordered khử trùng trước processor bằng
    `dropDuplicatesWithinWatermark`).
    `expire_ms` (eager/ordered): khi watermark vượt sự kiện mới nhất + `expire_ms` mà không có sự kiện
    mới, trạng thái người học hết hạn — `state.expired = True`, người gọi xóa toàn bộ state. Đây là
    TTL theo event time (Spark chỉ cho TTL state với timeMode ProcessingTime). Chế độ none không có
    watermark nên không hết hạn state; `expire_ms` chỉ dùng để tỉa `seen_event_ids` (giữ các id có
    event_time ≥ sự kiện mới nhất của người học − `expire_ms`).
    """
    if mode not in MODES:
        raise ProcessorError(f"mode phải thuộc {MODES}, nhận {mode!r}")
    resolved = resolve_model(model)
    incoming = list(events)
    timed = mode != "none"
    wm = watermark_ms if timed and watermark_ms is not None and watermark_ms >= 0 else None
    inactivity = inactivity_ms if timed and inactivity_ms and inactivity_ms > 0 else None
    expire = expire_ms if timed and expire_ms and expire_ms > 0 else None
    out = _Output()

    if mode == "none" and dedup:
        unique: list[Event] = []
        for event in incoming:
            if not state.seen(event.event_id):
                state.mark_seen(event.event_id, event.event_time_ms)
                unique.append(event)
        incoming = unique

    _note_seen(state, incoming, inactivity, expire)
    if mode == "none" and dedup and expire_ms and expire_ms > 0:
        _schedule_seen_prune(state, expire_ms)

    if mode == "ordered":
        pool = state.buffer + incoming
        ready = [e for e in pool if wm is not None and e.event_time_ms <= wm]
        waiting = sorted((e for e in pool if wm is None or e.event_time_ms > wm), key=lambda e: e.order_key)
        if incoming or ready:
            state.set_buffer(waiting)
        to_apply = sorted(ready, key=lambda e: e.order_key)
        state.meta = replace(state.meta, buffer_size=len(state.buffer),
                             buffer_due_ms=state.buffer[0].event_time_ms if state.buffer else None)
    else:
        to_apply = sorted(incoming, key=lambda e: e.order_key)

    for event in to_apply:
        _apply_one(state, event, resolved, out)

    meta = state.meta
    if inactivity is None and meta.inactivity_due_ms is not None:
        state.meta = meta = replace(meta, inactivity_due_ms=None)
    if wm is not None and meta.inactivity_due_ms is not None and meta.inactivity_due_ms <= wm:
        # Đã quá hạn mà không có sự kiện mới hơn: cảnh báo nếu người học bỏ dở bài cuối.
        problem_id = meta.last_seen_problem_id
        if meta.last_seen_event_id and problem_id and not state.problem(problem_id).completed:
            out.alerts.append(_alert(state, meta.last_seen_skill_id, "inactivity", meta.last_seen_event_id,
                                     meta.inactivity_due_ms))
        state.meta = meta = replace(meta, inactivity_due_ms=None)
    if expire is None and meta.expire_due_ms is not None:
        state.meta = meta = replace(meta, expire_due_ms=None)
    rows = out.rows(state)
    if wm is not None and meta.expire_due_ms is not None and meta.expire_due_ms <= wm and not state.buffer:
        # Hết hạn: mọi sự kiện ≤ watermark đã áp (đệm rỗng); người gọi xóa toàn bộ state của người học.
        state.expired = True
        state.meta = replace(meta, inactivity_due_ms=None, buffer_due_ms=None, expire_due_ms=None)
    return rows


def plan_timers(before: frozenset[int], after: frozenset[int], watermark_ms: int | None) -> tuple[list[int], list[int]]:
    """(xóa, đăng ký) để chuyển tập timer `before` → `after`.

    Timer ≤ watermark đang/sắp bắn trong batch này và Spark tự xóa sau khi bắn, nên không xóa tay.
    Timer mới luôn ở tương lai (> watermark) vì việc đến hạn đã được xử lý ngay.
    """
    wm = -1 if watermark_ms is None else watermark_ms
    delete = sorted(t for t in before - after if t > wm)
    register = sorted(t for t in after - before)
    stale = [t for t in register if t <= wm]
    if stale:
        raise ProcessorError(f"timer {stale} không ở tương lai (watermark {wm})")
    return delete, register


# ---------------------------------------------------------------- kho trong bộ nhớ (test, oracle)

@dataclass
class MemoryLearnerStore:
    """Kho trạng thái trong bộ nhớ cho một người học: cùng giao kèo đọc/ghi như state Spark.

    Dùng trong test để chạy nhiều "batch" liên tiếp bằng đúng `apply_events` mà processor dùng.
    """

    learner_id: str
    meta: LearnerMeta = field(default_factory=LearnerMeta)
    skills: dict[str, SkillState] = field(default_factory=dict)
    problems: dict[str, ProblemState] = field(default_factory=dict)
    seen: dict[str, int] = field(default_factory=dict)
    buffer: list[Event] = field(default_factory=list)
    timers: set[int] = field(default_factory=set)

    def open(self) -> LearnerState:
        return LearnerState(self.learner_id, self.meta, load_skill=self.skills.get, load_problem=self.problems.get,
                            load_seen=self.seen.__contains__, buffer=list(self.buffer))

    def commit(self, state: LearnerState, watermark_ms: int | None, *, timed: bool = True) -> None:
        if state.expired:
            if timed:
                delete, _ = plan_timers(self.meta.timers, frozenset(), watermark_ms)
                self.timers.difference_update(delete)
            self.meta, self.skills, self.problems, self.seen, self.buffer = LearnerMeta(), {}, {}, {}, []
            return
        for skill_id in state.dirty_skills:
            self.skills[skill_id] = state.skills[skill_id]
        for problem_id in state.dirty_problems:
            self.problems[problem_id] = state.problems[problem_id]
        self.seen.update(state.new_seen)
        if state.prune_seen_before is not None:
            self.seen = {k: v for k, v in self.seen.items() if v >= state.prune_seen_before}
        if state.buffer_dirty:
            self.buffer = list(state.buffer)
        if timed:
            delete, register = plan_timers(self.meta.timers, state.meta.timers, watermark_ms)
            self.timers.difference_update(delete)
            self.timers.update(register)
        self.meta = state.meta

    def run(self, events: Iterable[Event], mode: str, model: LearnerModel | str, *, watermark_ms: int | None = None,
            dedup: bool = True, inactivity_ms: int | None = DEFAULT_INACTIVITY_MS,
            expire_ms: int | None = DEFAULT_STATE_TTL_MS) -> list[dict[str, Any]]:
        """Một batch: mở, áp, lưu. Timer đến hạn (≤ watermark) coi như đã bắn và bị xóa."""
        state = self.open()
        rows = apply_events(state, events, mode, model, watermark_ms=watermark_ms, dedup=dedup,
                            inactivity_ms=inactivity_ms, expire_ms=expire_ms)
        self.commit(state, watermark_ms, timed=mode != "none")
        if watermark_ms is not None:
            self.timers = {t for t in self.timers if t > watermark_ms}
        return rows


# ---------------------------------------------------------------- bọc cho Spark

def events_from_pandas(frame: pd.DataFrame) -> list[Event]:
    """Các dòng đầu vào processor (pandas, cột theo PROCESSOR_INPUT_COLUMNS) → Event."""
    columns = [frame[name].tolist() for name in PROCESSOR_INPUT_COLUMNS]
    events: list[Event] = []
    for (_learner, class_id, event_id, event_type, event_time_ms, sent_time_ms, session_seq, problem_id, skill_ids,
         is_correct, step_index) in zip(*columns):
        events.append(Event(
            event_id=str(event_id), event_type=str(event_type), event_time_ms=int(event_time_ms),
            sent_time_ms=int(sent_time_ms), session_seq=int(session_seq), class_id=str(class_id),
            problem_id=str(problem_id), skill_ids=tuple(str(s) for s in skill_ids),
            is_correct=None if _missing(is_correct) else bool(is_correct),
            step_index=None if _missing(step_index) else int(step_index),
        ))
    return events


def rows_to_pandas(rows: list[dict[str, Any]]) -> pd.DataFrame:
    """Dòng đầu ra → pandas với cột object (giữ None cho số nguyên, list cho mảng) để Arrow ép đúng kiểu."""
    return pd.DataFrame({column: pd.Series([row[column] for row in rows], dtype="object")
                         for column in OUTPUT_COLUMNS})


def time_mode_for(mode: str) -> str:
    """timeMode của transformWithStateInPandas: `EventTime` (eager/ordered) hoặc `None` (none).

    Không dùng TTL state của Spark: nó chỉ được phép với `ProcessingTime`
    (STATEFUL_PROCESSOR_INCORRECT_TIME_MODE_TO_ASSIGN_TTL), mà `ProcessingTime` khiến Spark chạy
    micro-batch rỗng liên tục khi trigger ASAP. Hết hạn được làm theo event time trong `apply_events`.
    """
    if mode not in MODES:
        raise ProcessorError(f"mode phải thuộc {MODES}, nhận {mode!r}")
    return "EventTime" if mode != "none" else "None"


class LearnerStateProcessor(StatefulProcessor):
    """Bọc `apply_events` cho `transformWithStateInPandas` (khóa nhóm = learner_id).

    State của mỗi người học:
    - ValueState `meta` (LearnerMeta): lớp, mốc thứ tự, sự kiện mới nhất, timer đang đặt;
    - MapState `skills` skill_id → SkillState; MapState `problems` problem_id → ProblemState;
    - ListState `buffer` (chỉ ordered); MapState `seen_event_ids` (chỉ none + dedup).
    Hết hạn sau `state_ttl_ms` (mặc định 24 h) theo event time: eager/ordered có timer tại sự kiện mới
    nhất + TTL, xóa toàn bộ state khi watermark vượt; none (không timer) chỉ tỉa `seen_event_ids`.
    Đọc lười từng khóa (mỗi lời gọi state là một vòng Python↔JVM), chỉ ghi mục đã đổi, timer
    lưu trong `meta` nên không cần `listTimers()`.
    """

    def __init__(self, mode: str, model: LearnerModel | str, *, dedup: bool = True,
                 inactivity_ms: int | None = DEFAULT_INACTIVITY_MS,
                 state_ttl_ms: int = DEFAULT_STATE_TTL_MS) -> None:
        self.time_mode = time_mode_for(mode)
        if state_ttl_ms <= 0:
            raise ProcessorError("state_ttl_ms phải > 0")
        self.mode = mode
        self.model = resolve_model(model)
        self.dedup = dedup
        self.inactivity_ms = inactivity_ms
        self.state_ttl_ms = state_ttl_ms

    @property
    def timed(self) -> bool:
        return self.mode != "none"

    def init(self, handle: StatefulProcessorHandle) -> None:
        self.handle = handle
        self.meta_state = handle.getValueState("meta", META_STATE_DDL)
        self.skill_state = handle.getMapState("skills", "skill_id STRING", SKILL_STATE_DDL)
        self.problem_state = handle.getMapState("problems", "problem_id STRING", PROBLEM_STATE_DDL)
        self.buffer_state = (handle.getListState("buffer", EVENT_STATE_DDL) if self.mode == "ordered" else None)
        self.seen_state = (handle.getMapState("seen_event_ids", "event_id STRING", "event_time_ms BIGINT")
                           if self.mode == "none" and self.dedup else None)

    def _open(self, learner_id: str) -> LearnerState:
        meta = LearnerMeta.from_tuple(self.meta_state.get())
        buffer: list[Event] = []
        if self.buffer_state is not None and meta.buffer_size > 0:
            buffer = [Event.from_tuple(tuple(row)) for row in self.buffer_state.get()]

        def load_skill(skill_id: str) -> SkillState | None:
            value = self.skill_state.getValue((skill_id,))
            return None if value is None else SkillState.from_tuple(tuple(value))

        def load_problem(problem_id: str) -> ProblemState | None:
            value = self.problem_state.getValue((problem_id,))
            return None if value is None else ProblemState.from_tuple(tuple(value))

        seen_state = self.seen_state

        def load_seen(event_id: str) -> bool:
            return seen_state is not None and seen_state.containsKey((event_id,))

        return LearnerState(learner_id, meta, load_skill=load_skill, load_problem=load_problem,
                            load_seen=load_seen, buffer=buffer)

    def _clear(self, before: LearnerMeta, watermark_ms: int | None) -> None:
        """Xóa toàn bộ state của người học (hết hạn theo event time) và các timer còn ở tương lai."""
        delete, _ = plan_timers(before.timers, frozenset(), watermark_ms)
        for expiry in delete:
            self.handle.deleteTimer(expiry)
        for variable in (self.meta_state, self.skill_state, self.problem_state, self.buffer_state, self.seen_state):
            if variable is not None:
                variable.clear()

    def _commit(self, state: LearnerState, before: LearnerMeta, watermark_ms: int | None) -> None:
        if state.expired:
            self._clear(before, watermark_ms)
            return
        for skill_id in state.dirty_skills:
            self.skill_state.updateValue((skill_id,), state.skills[skill_id].to_tuple())
        for problem_id in state.dirty_problems:
            self.problem_state.updateValue((problem_id,), state.problems[problem_id].to_tuple())
        if self.seen_state is not None:
            for event_id, event_time_ms in state.new_seen.items():
                self.seen_state.updateValue((event_id,), (event_time_ms,))
            if state.prune_seen_before is not None:
                stale = [key for key, value in self.seen_state.iterator() if value[0] < state.prune_seen_before]
                for key in stale:
                    self.seen_state.removeKey(tuple(key))
        if self.buffer_state is not None and state.buffer_dirty:
            if state.buffer:
                self.buffer_state.put([e.to_tuple() for e in state.buffer])
            else:
                self.buffer_state.clear()
        if self.timed:
            delete, register = plan_timers(before.timers, state.meta.timers, watermark_ms)
            for expiry in delete:
                self.handle.deleteTimer(expiry)
            for expiry in register:
                self.handle.registerTimer(expiry)
        if state.meta != before:
            self.meta_state.update(state.meta.to_tuple())

    def _run(self, learner_id: str, events: list[Event], timer_values: Any) -> Iterator[Any]:
        watermark_ms = timer_values.getCurrentWatermarkInMs() if self.timed else None
        state = self._open(learner_id)
        before = state.meta
        rows = apply_events(state, events, self.mode, self.model, watermark_ms=watermark_ms, dedup=self.dedup,
                            inactivity_ms=self.inactivity_ms, expire_ms=self.state_ttl_ms)
        self._commit(state, before, watermark_ms)
        if rows:
            yield rows_to_pandas(rows)

    def handleInputRows(self, key: Any, rows: Iterator[Any], timerValues: Any) -> Iterator[Any]:
        events = [event for frame in rows for event in events_from_pandas(frame)]
        yield from self._run(str(key[0]), events, timerValues)

    def handleExpiredTimer(self, key: Any, timerValues: Any, expiredTimerInfo: Any) -> Iterator[Any]:
        yield from self._run(str(key[0]), [], timerValues)

    def close(self) -> None:
        pass


__all__ = [
    "ALERT_TYPES", "DEFAULT_INACTIVITY_MS", "DEFAULT_STATE_TTL_MS", "MODELS", "MODES", "Event", "LearnerMeta",
    "LearnerModel", "LearnerState", "LearnerStateProcessor", "MemoryLearnerStore", "ProblemState", "ProcessorError",
    "SkillState", "alert_id", "apply_events", "bkt_params_for", "events_from_pandas", "plan_timers",
    "resolve_model", "rows_to_pandas", "time_mode_for",
]
