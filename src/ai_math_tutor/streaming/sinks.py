"""Ghi kết quả mỗi micro-batch: Redis (Lua idempotent), cảnh báo, tổng hợp lớp, Parquet theo batch.

foreachBatch của Spark chỉ bảo đảm **at-least-once**: sau lỗi hay restart, batch cuối chưa commit
được chạy lại với cùng offset và cùng trạng thái ⇒ cùng các dòng đầu ra. Vì vậy mọi side effect
ở đây đều idempotent:

- trạng thái `state:{learner}` (mỗi kỹ năng một trường JSON) và `progress:{learner}:{problem}`
  ghi qua `redis_upsert.lua`: chỉ ghi khi `(batch_id, state_version)` mới lớn hơn bản đang lưu theo
  thứ tự từ điển (batch chạy lại giữ batch_id ⇒ không đổi gì; version quay về 1 sau khi state hết
  hạn vẫn được ghi vì thuộc batch sau);
- cảnh báo qua `redis_alert.lua`: kiểm `alert:{id}`, `XADD alerts:stream` rồi mới `SET` trong cùng
  script, sau đó gửi topic Kafka `alerts` (consumer khử trùng theo `alert_id`);
- tổng hợp lớp `agg:class:{class}:skill:{skill}`, `stuck:set:{class}`, `trend:{class}:{bucket5m}`
  tính lại hoàn toàn từ trạng thái hiện có (ghi đè), chỉ cho lớp bị chạm trong batch. Trạng thái của
  thành viên roster lấy từ bộ đệm trên driver (`LearnerStateCache`): nạp từ Redis lần đầu một người
  học cần tới sau khi (re)start, sau đó cập nhật bằng chính các bản ghi batch vừa ghi;
- Parquet `lake/<run_id>/{applied,latency,alerts}/batch=<batch_id>/part-00000.parquet` ghi đè
  (batch chạy lại thay đúng tệp cũ, không nhân đôi).

Thời điểm "visible" của batch lấy **sau khi** mọi lệnh Redis xong; mỗi sự kiện đã áp có hai số đo:
độ trễ hệ thống = visible − sent_time, độ cũ trạng thái = visible − event_time.
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections import deque
from collections.abc import Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ai_math_tutor.config import RuntimeConfig, validate_run_id

LOGGER = logging.getLogger(__name__)

MODES = ("eager", "ordered", "none")
MODELS = ("rule", "bkt")
ROW_KINDS = ("update", "alert", "progress")
ALERT_TYPES = ("stuck", "consec_wrong", "inactivity")
LAKE_KINDS = ("applied", "latency", "alerts")

UPSERT_LUA_PATH = Path(__file__).with_name("redis_upsert.lua")
ALERT_LUA_PATH = Path(__file__).with_name("redis_alert.lua")

# Giới hạn xấp xỉ số mục giữ trong alerts:stream (XADD MAXLEN ~).
ALERT_STREAM_MAXLEN = 100_000
TREND_BUCKET_MS = 5 * 60 * 1000
TREND_TTL_S = 2 * 60 * 60
LATENCY_WINDOW_MS = 60_000
# Số bản ghi tối đa mỗi lần gọi Lua: giữ mỗi script ngắn (Redis đơn luồng bị chặn khi script chạy).
UPSERT_CHUNK = 256
ALERT_FLUSH_TIMEOUT_S = 30.0
LAKE_FILE = "part-00000.parquet"


class SinkError(RuntimeError):
    """Batch không ghi xong được (dòng sai hợp đồng, Kafka không nhận cảnh báo...): để query lỗi và chạy lại."""


# ---------------------------------------------------------------------------------------------
# Khóa Redis: một nơi duy nhất sinh tên khóa (bảng "Khóa Redis" trong docs/event-schema.md).
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RedisKeys:
    """Tên khóa Redis của pipeline; `prefix` rỗng khi chạy thật, có giá trị khi test dùng chung Redis."""

    prefix: str = ""

    def state(self, learner_id: str) -> str:
        return f"{self.prefix}state:{learner_id}"

    def progress(self, learner_id: str, problem_id: str) -> str:
        return f"{self.prefix}progress:{learner_id}:{problem_id}"

    def roster(self, class_id: str) -> str:
        return f"{self.prefix}roster:{class_id}"

    def agg(self, class_id: str, skill_id: str) -> str:
        return f"{self.prefix}agg:class:{class_id}:skill:{skill_id}"

    def stuck_set(self, class_id: str) -> str:
        return f"{self.prefix}stuck:set:{class_id}"

    def alert(self, alert_id: str) -> str:
        return f"{self.prefix}alert:{alert_id}"

    @property
    def alerts_stream(self) -> str:
        return f"{self.prefix}alerts:stream"

    def trend(self, class_id: str, bucket_ms: int) -> str:
        return f"{self.prefix}trend:{class_id}:{bucket_ms}"

    @property
    def metrics_pipeline(self) -> str:
        return f"{self.prefix}metrics:pipeline"

    def hintctx(self, learner_id: str, problem_id: str, step_index: int) -> str:
        """Tập `event_id` yêu cầu gợi ý của một bước (dịch vụ gợi ý ghi và đọc)."""
        return f"{self.prefix}hintctx:{learner_id}:{problem_id}:{int(step_index)}"

    def lasthint(self, learner_id: str) -> str:
        """Gợi ý gần nhất của người học (dịch vụ gợi ý ghi, dashboard đọc)."""
        return f"{self.prefix}lasthint:{learner_id}"


def load_roster(client: Any, roster: Mapping[str, Sequence[str]], keys: RedisKeys = RedisKeys()) -> int:
    """Nạp `roster.json` ({class_id: [learner_id, ...]}) vào `roster:{class}`; trả số người học.

    Thay hẳn tập cũ của mỗi lớp (DEL + SADD trong một transaction) nên gọi lại không đổi kết quả.
    """
    total = 0
    pipe = client.pipeline(transaction=True)
    for class_id, learners in roster.items():
        if not isinstance(class_id, str) or not class_id:
            raise ValueError(f"class_id không hợp lệ trong roster: {class_id!r}")
        members = [str(m) for m in learners]
        pipe.delete(keys.roster(class_id))
        if members:
            pipe.sadd(keys.roster(class_id), *members)
        total += len(members)
    pipe.execute()
    return total


# ---------------------------------------------------------------------------------------------
# Kế hoạch batch: hàm thuần trên các dòng OUTPUT_SCHEMA (dict), không gọi Redis/Kafka.
# ---------------------------------------------------------------------------------------------


def _decode(value: Any) -> str:
    return value.decode() if isinstance(value, bytes) else str(value)


def _require(row: Mapping[str, Any], name: str, kind: str) -> Any:
    value = row.get(name)
    if value is None:
        raise SinkError(f"dòng {kind} thiếu cột {name}: {dict(row)!r}")
    return value


def _int(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, np.integer)):
        raise SinkError(f"{name} phải là số nguyên, nhận {value!r}")
    return int(value)


@dataclass
class BatchPlan:
    """Những gì một batch cần ghi, đã gom và khử lặp trong batch."""

    # Bản mới nhất cho mỗi (learner, skill) / (learner, problem), theo `_row_order`.
    updates: list[dict[str, Any]] = field(default_factory=list)
    progress: list[dict[str, Any]] = field(default_factory=list)
    # Một dòng cho mỗi alert_id.
    alerts: list[dict[str, Any]] = field(default_factory=list)
    # Một dòng cho mỗi event_id đã áp (thứ tự xuất hiện).
    applied: list[dict[str, Any]] = field(default_factory=list)
    # class_id → event_time lớn nhất của các dòng update trong batch (-1: chỉ có cảnh báo ⇒ không ghi trend).
    touched_classes: dict[str, int] = field(default_factory=dict)
    duplicate_applied_in_batch: int = 0
    rows: int = 0


def _row_order(row: Mapping[str, Any]) -> tuple[int, int]:
    """Thứ tự "mới hơn" giữa hai dòng cùng khóa trong một batch: (last_event_time_ms, state_version).

    Không hết hạn: version tăng ⇒ last_event_time_ms (max event time đã áp) không giảm, nên thứ tự
    này trùng thứ tự version. Hết hạn rồi người học quay lại trong cùng batch: version bắt đầu lại từ
    1 nhưng sự kiện mới hơn bản trước quá TTL ⇒ bản sau hết hạn vẫn thắng.
    """
    last = row.get("last_event_time_ms")
    return (-1 if last is None else int(last), int(row["state_version"]))


def plan_batch(rows: Iterable[Mapping[str, Any]]) -> BatchPlan:
    """Kiểm từng dòng theo hợp đồng đầu ra của processor rồi gom thành kế hoạch ghi."""
    plan = BatchPlan()
    latest_update: dict[tuple[str, str], dict[str, Any]] = {}
    latest_progress: dict[tuple[str, str], dict[str, Any]] = {}
    alerts: dict[str, dict[str, Any]] = {}
    seen_events: set[str] = set()

    def touch(class_id: str, event_ms: int | None) -> None:
        current = plan.touched_classes.get(class_id)
        if event_ms is not None and (current is None or event_ms > current):
            plan.touched_classes[class_id] = event_ms
        elif current is None:
            plan.touched_classes[class_id] = -1

    for raw in rows:
        plan.rows += 1
        row = dict(raw)
        kind = row.get("kind")
        if kind not in ROW_KINDS:
            raise SinkError(f"kind không hợp lệ: {kind!r}")
        learner = str(_require(row, "learner_id", kind))
        class_id = str(_require(row, "class_id", kind))
        if kind == "update":
            skill = str(_require(row, "skill_id", kind))
            version = _int(_require(row, "state_version", kind), "state_version")
            last_ms = row.get("last_event_time_ms")
            ids = list(row.get("applied_event_ids") or [])
            event_ms = list(row.get("applied_event_time_ms") or [])
            sent_ms = list(row.get("applied_sent_time_ms") or [])
            out_of_order = list(row.get("applied_out_of_order") or [False] * len(ids))
            if not (len(ids) == len(event_ms) == len(sent_ms) == len(out_of_order)):
                raise SinkError(f"mảng applied_* lệch độ dài cho ({learner}, {skill})")
            if last_ms is not None:
                _int(last_ms, "last_event_time_ms")
            key = (learner, skill)
            previous = latest_update.get(key)
            if previous is None or _row_order(row) > _row_order(previous):
                latest_update[key] = row
            for event_id, e_ms, s_ms, ooo in zip(ids, event_ms, sent_ms, out_of_order):
                if event_id in seen_events:
                    plan.duplicate_applied_in_batch += 1
                    continue
                seen_events.add(event_id)
                plan.applied.append({
                    "event_id": str(event_id),
                    "learner_id": learner,
                    "class_id": class_id,
                    "skill_id": skill,
                    "event_time_ms": _int(e_ms, "applied_event_time_ms"),
                    "sent_time_ms": _int(s_ms, "applied_sent_time_ms"),
                    "out_of_order": bool(ooo),
                    "state_version": version,
                })
            touch(class_id, None if last_ms is None else _int(last_ms, "last_event_time_ms"))
        elif kind == "progress":
            problem = str(_require(row, "problem_id", kind))
            _int(_require(row, "state_version", kind), "state_version")
            if row.get("last_event_time_ms") is not None:
                _int(row["last_event_time_ms"], "last_event_time_ms")
            key = (learner, problem)
            previous = latest_progress.get(key)
            if previous is None or _row_order(row) > _row_order(previous):
                latest_progress[key] = row
        else:
            alert_id = str(_require(row, "alert_id", kind))
            alert_type = _require(row, "alert_type", kind)
            if alert_type not in ALERT_TYPES:
                raise SinkError(f"alert_type không hợp lệ: {alert_type!r}")
            _require(row, "skill_id", kind)
            alerts.setdefault(alert_id, row)
            touch(class_id, None)
    plan.updates = list(latest_update.values())
    plan.progress = list(latest_progress.values())
    plan.alerts = list(alerts.values())
    return plan


def state_value(row: Mapping[str, Any], batch_id: int) -> str:
    """JSON lưu trong trường `<skill>` của `state:{learner}` (đọc bởi dịch vụ gợi ý và dashboard)."""
    p_mastery = row.get("p_mastery")
    return json.dumps({
        "class_id": row["class_id"],
        "p_mastery": None if p_mastery is None else float(p_mastery),
        "opportunities": int(row.get("opportunities") or 0),
        "correct": int(row.get("correct") or 0),
        "consec_wrong": int(row.get("consec_wrong") or 0),
        "streak": int(row.get("streak") or 0),
        "mastered": bool(row.get("mastered")),
        "stuck": bool(row.get("stuck")),
        "stuck_onset_event_id": row.get("stuck_onset_event_id"),
        "state_version": int(row["state_version"]),
        "last_event_time_ms": None if row.get("last_event_time_ms") is None else int(row["last_event_time_ms"]),
        "batch_id": int(batch_id),
    }, separators=(",", ":"), sort_keys=True)


def progress_pairs(row: Mapping[str, Any], batch_id: int) -> list[str]:
    """Cặp trường/giá trị của hash `progress:{learner}:{problem}` (gồm `state_version`)."""
    values = {
        "class_id": row["class_id"],
        "wrong_answers": int(row.get("wrong_answers") or 0),
        "last_step": int(row.get("last_step") or 0),
        "completed": 1 if row.get("completed") else 0,
        "state_version": int(row["state_version"]),
        "last_event_time_ms": "" if row.get("last_event_time_ms") is None else int(row["last_event_time_ms"]),
        "batch_id": int(batch_id),
    }
    pairs: list[str] = []
    for name, value in values.items():
        pairs.extend([name, str(value)])
    return pairs


def upsert_calls(plan: BatchPlan, keys: RedisKeys, batch_id: int,
                 chunk: int = UPSERT_CHUNK) -> list[tuple[list[str], list[str]]]:
    """Chia bản ghi trạng thái/tiến độ thành các lời gọi `redis_upsert.lua` (KEYS, ARGV).

    Mỗi bản ghi mang `batch_id` của batch đang ghi: Lua so (batch_id, state_version) với bản đang lưu.
    """
    batch = str(int(batch_id))
    records: list[tuple[str, list[str]]] = []
    for row in plan.updates:
        records.append((keys.state(row["learner_id"]),
                        ["f", str(row["skill_id"]), batch, str(int(row["state_version"])),
                         state_value(row, batch_id)]))
    for row in plan.progress:
        pairs = progress_pairs(row, batch_id)
        records.append((keys.progress(row["learner_id"], row["problem_id"]),
                        ["h", batch, str(int(row["state_version"])), str(len(pairs) // 2), *pairs]))
    calls: list[tuple[list[str], list[str]]] = []
    for start in range(0, len(records), chunk):
        part = records[start:start + chunk]
        calls.append(([key for key, _ in part], [arg for _, args in part for arg in args]))
    return calls


def alert_fields(row: Mapping[str, Any], run_id: str, batch_id: int) -> dict[str, str]:
    """Trường của mục `alerts:stream` (cũng là nội dung tin Kafka `alerts`, cộng `visible_ms`)."""
    return {
        "alert_id": str(row["alert_id"]),
        "alert_type": str(row["alert_type"]),
        "learner_id": str(row["learner_id"]),
        "class_id": str(row["class_id"]),
        "skill_id": str(row["skill_id"]),
        "onset_event_id": "" if row.get("stuck_onset_event_id") is None else str(row["stuck_onset_event_id"]),
        "alert_event_time_ms": "" if row.get("alert_event_time_ms") is None else str(int(row["alert_event_time_ms"])),
        "run_id": run_id,
        "batch_id": str(int(batch_id)),
    }


def parse_alert_state(value: Any) -> tuple[str, str]:
    """`pending:<stream id>` / `produced:<stream id>` → (trạng thái, stream id).

    Giá trị khác dạng (ví dụ `pending` trần do bản script cũ để lại) không trỏ tới mục stream nào ⇒
    SinkError. Sinks không bao giờ nhận giá trị như vậy: `redis_alert.lua` coi nó là chưa có cảnh báo,
    XADD lại và ghi đè, nên batch chạy lại không bị kẹt; dashboard báo nó như một vấn đề dữ liệu.
    """
    text = value.decode() if isinstance(value, bytes) else str(value)
    status, sep, stream_id = text.partition(":")
    if not sep or status not in ("pending", "produced") or not stream_id:
        raise SinkError(f"giá trị alert:* không đúng dạng: {text!r}")
    return status, stream_id


def stream_id_ms(stream_id: str) -> int:
    """Phần mili giây của stream id Redis (`<ms>-<seq>`) = lúc XADD lần đầu."""
    return int(stream_id.split("-", 1)[0])


def class_aggregates(
    class_id: str,
    roster: Sequence[str],
    states: Mapping[str, Mapping[str, Mapping[str, Any]]],
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Tổng hợp theo kỹ năng cho một lớp từ trạng thái hiện có của các thành viên roster.

    `states`: learner → skill → dict trạng thái (JSON đã giải mã). Trả (skill → hash agg, danh sách
    "learner|skill" đang bế tắc). Hàm thuần: cùng trạng thái ⇒ cùng kết quả (ghi đè an toàn).
    """
    per_skill: dict[str, dict[str, Any]] = {}
    stuck: list[str] = []
    for learner in sorted(set(roster)):
        for skill, state in (states.get(learner) or {}).items():
            acc = per_skill.setdefault(skill, {"observed": 0, "mastered": 0, "stuck": 0, "opportunities": 0,
                                                "correct": 0, "p_sum": 0.0, "p_n": 0, "as_of": None})
            acc["observed"] += 1
            acc["mastered"] += 1 if state.get("mastered") else 0
            if state.get("stuck"):
                acc["stuck"] += 1
                stuck.append(f"{learner}|{skill}")
            acc["opportunities"] += int(state.get("opportunities") or 0)
            acc["correct"] += int(state.get("correct") or 0)
            if state.get("p_mastery") is not None:
                acc["p_sum"] += float(state["p_mastery"])
                acc["p_n"] += 1
            last = state.get("last_event_time_ms")
            if last is not None and (acc["as_of"] is None or int(last) > acc["as_of"]):
                acc["as_of"] = int(last)
    result: dict[str, dict[str, str]] = {}
    for skill, acc in sorted(per_skill.items()):
        result[skill] = {
            "class_id": class_id,
            "skill_id": skill,
            "learners": str(len(set(roster))),
            "observed": str(acc["observed"]),
            "mastered": str(acc["mastered"]),
            "stuck": str(acc["stuck"]),
            "mean_p_mastery": "" if acc["p_n"] == 0 else repr(acc["p_sum"] / acc["p_n"]),
            "mean_opportunities": repr(acc["opportunities"] / acc["observed"]),
            "accuracy": "" if acc["opportunities"] == 0 else repr(acc["correct"] / acc["opportunities"]),
            "as_of_event_ms": "" if acc["as_of"] is None else str(acc["as_of"]),
        }
    return result, sorted(stuck)


def trend_bucket(event_ms: int) -> int:
    return (event_ms // TREND_BUCKET_MS) * TREND_BUCKET_MS


def snapshot_event_ms(aggregates: Mapping[str, Mapping[str, str]], fallback_ms: int) -> int:
    """Event time mà snapshot tổng hợp của lớp phản ánh: max `as_of_event_ms` trên mọi kỹ năng của lớp.

    Ô trend được chọn theo giá trị này chứ không theo sự kiện của batch: snapshot chứa trạng thái của
    cả lớp, nên một batch chỉ chạm lớp qua sự kiện cũ hơn không được ghi đè ô 5 phút cũ bằng dữ liệu
    mới hơn ô đó. Giá trị không giảm giữa các batch vì trạng thái trong Redis chỉ tiến lên.
    """
    times = [int(values["as_of_event_ms"]) for values in aggregates.values() if values.get("as_of_event_ms")]
    return max(times, default=fallback_ms)


# Trường của trạng thái kỹ năng mà `class_aggregates` đọc; bộ đệm chỉ giữ các trường này.
AGG_STATE_FIELDS = ("mastered", "stuck", "opportunities", "correct", "p_mastery", "last_event_time_ms")


def _slim(state: Mapping[str, Any]) -> dict[str, Any]:
    return {name: state.get(name) for name in AGG_STATE_FIELDS}


class LearnerStateCache:
    """Bản sao trên driver của `state:{learner}` (learner → skill → trường tổng hợp) cho tổng hợp lớp.

    Bất biến: người học có trong bộ đệm ⇔ bộ đệm bằng đúng `state:{learner}` trong Redis (các trường
    `AGG_STATE_FIELDS`). Giữ được vì foreachBatch là nơi duy nhất ghi `state:*` trong một run:
    - người học chưa có ⇒ nạp từ Redis (HGETALL) khi lớp của họ cần tổng hợp lần đầu sau (re)start;
    - upsert ghi được mọi bản ghi của batch ⇒ áp đúng chuỗi JSON đã ghi cho người học đã có;
    - có bản ghi bị Lua bỏ (batch chạy lại, bản lưu mới hơn) hoặc upsert lỗi ⇒ bỏ người học của batch
      khỏi bộ đệm, lần sau nạp lại từ Redis.
    Không lưu xuống đĩa: restart bắt đầu rỗng và nạp lại từ Redis, nên batch chạy lại vẫn idempotent.
    """

    def __init__(self) -> None:
        self._states: dict[str, dict[str, dict[str, Any]]] = {}

    def __len__(self) -> int:
        return len(self._states)

    @property
    def states(self) -> Mapping[str, Mapping[str, Mapping[str, Any]]]:
        return self._states

    def missing(self, learners: Iterable[str]) -> list[str]:
        return [learner for learner in learners if learner not in self._states]

    def load(self, learner: str, raw: Mapping[Any, Any]) -> None:
        """Nạp kết quả HGETALL `state:{learner}` (rỗng ⇒ người học chưa có trạng thái)."""
        self._states[learner] = {_decode(k): _slim(json.loads(_decode(v))) for k, v in raw.items()}

    def apply(self, learner: str, skill: str, value: str) -> None:
        """Áp chuỗi JSON vừa ghi vào `state:{learner}`[skill]; người học chưa nạp thì bỏ qua (nạp sau)."""
        skills = self._states.get(learner)
        if skills is not None:
            skills[skill] = _slim(json.loads(value))

    def invalidate(self, learners: Iterable[str]) -> None:
        for learner in learners:
            self._states.pop(learner, None)


# ---------------------------------------------------------------------------------------------
# Độ trễ: cửa sổ trượt 1 phút (theo visible time) dùng chung với listener trong cùng driver.
# ---------------------------------------------------------------------------------------------


def nearest_rank(values: np.ndarray, q: float) -> float | None:
    """Phân vị kiểu nearest-rank (giá trị thứ ceil(q·n) sau khi sắp); None khi rỗng."""
    if values.size == 0:
        return None
    return float(np.percentile(values, q * 100.0, method="inverted_cdf"))


def latency_summary(system_ms: np.ndarray, staleness_ms: np.ndarray) -> dict[str, Any]:
    def stats(values: np.ndarray) -> dict[str, float | None]:
        return {
            "p50": nearest_rank(values, 0.50),
            "p95": nearest_rank(values, 0.95),
            "p99": nearest_rank(values, 0.99),
            "max": float(values.max()) if values.size else None,
        }

    return {"count": int(system_ms.size), "system_ms": stats(system_ms), "staleness_ms": stats(staleness_ms)}


class LatencyWindow:
    """Mẫu độ trễ của các batch trong 1 phút gần nhất + tóm tắt từng batch cho listener.

    foreachBatch (luồng stream execution) ghi, listener (luồng listener bus) đọc ⇒ có khóa.
    """

    def __init__(self, window_ms: int = LATENCY_WINDOW_MS, clock_ms: Callable[[], int] | None = None) -> None:
        self.window_ms = window_ms
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._lock = threading.Lock()
        self._samples: deque[tuple[int, np.ndarray, np.ndarray]] = deque()
        self._batches: dict[int, dict[str, Any]] = {}

    def add(self, visible_ms: int, system_ms: np.ndarray, staleness_ms: np.ndarray) -> None:
        with self._lock:
            if system_ms.size:
                self._samples.append((visible_ms, system_ms, staleness_ms))
            self._prune(self._clock_ms())

    def _prune(self, now_ms: int) -> None:
        while self._samples and self._samples[0][0] < now_ms - self.window_ms:
            self._samples.popleft()

    def snapshot(self, now_ms: int | None = None) -> dict[str, Any]:
        with self._lock:
            self._prune(self._clock_ms() if now_ms is None else now_ms)
            if not self._samples:
                empty = np.empty(0)
                return {"window_ms": self.window_ms, **latency_summary(empty, empty)}
            system = np.concatenate([s for _, s, _ in self._samples])
            staleness = np.concatenate([s for _, _, s in self._samples])
        return {"window_ms": self.window_ms, **latency_summary(system, staleness)}

    def record_batch(self, batch_id: int, summary: Mapping[str, Any]) -> None:
        with self._lock:
            self._batches[int(batch_id)] = dict(summary)
            # Listener lấy theo batchId; giữ tối đa vài batch phòng khi listener chậm/không gắn.
            for stale in sorted(self._batches)[:-32]:
                del self._batches[stale]

    def pop_batch(self, batch_id: int) -> dict[str, Any] | None:
        with self._lock:
            return self._batches.pop(int(batch_id), None)


_WINDOWS: dict[str, LatencyWindow] = {}
_WINDOWS_LOCK = threading.Lock()


def latency_window(run_id: str) -> LatencyWindow:
    """Cửa sổ độ trễ của một run trong tiến trình driver (sinks ghi, listener đọc)."""
    with _WINDOWS_LOCK:
        window = _WINDOWS.get(run_id)
        if window is None:
            window = _WINDOWS[run_id] = LatencyWindow()
        return window


# ---------------------------------------------------------------------------------------------
# Parquet theo batch (ghi đè).
# ---------------------------------------------------------------------------------------------

APPLIED_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("learner_id", pa.string()), ("class_id", pa.string()), ("skill_id", pa.string()),
    ("event_time_ms", pa.int64()), ("sent_time_ms", pa.int64()), ("out_of_order", pa.bool_()),
    ("state_version", pa.int64()), ("batch_id", pa.int64()),
])
LATENCY_SCHEMA = pa.schema([
    ("event_id", pa.string()), ("learner_id", pa.string()), ("class_id", pa.string()),
    ("event_time_ms", pa.int64()), ("sent_time_ms", pa.int64()), ("visible_ms", pa.int64()),
    ("system_latency_ms", pa.int64()), ("staleness_ms", pa.int64()), ("out_of_order", pa.bool_()),
    ("batch_id", pa.int64()),
])
ALERTS_SCHEMA = pa.schema([
    ("alert_id", pa.string()), ("alert_type", pa.string()), ("learner_id", pa.string()), ("class_id", pa.string()),
    ("skill_id", pa.string()), ("onset_event_id", pa.string()), ("alert_event_time_ms", pa.int64()),
    ("visible_ms", pa.int64()), ("stream_id", pa.string()), ("batch_id", pa.int64()),
])
LAKE_SCHEMAS = {"applied": APPLIED_SCHEMA, "latency": LATENCY_SCHEMA, "alerts": ALERTS_SCHEMA}


def batch_dir(lake_root: Path, kind: str, batch_id: int) -> Path:
    if kind not in LAKE_KINDS:
        raise ValueError(f"kind phải thuộc {LAKE_KINDS}, nhận {kind!r}")
    return Path(lake_root) / kind / f"batch={int(batch_id)}"


def write_batch_parquet(lake_root: Path, kind: str, batch_id: int, rows: Sequence[Mapping[str, Any]]) -> Path | None:
    """Ghi đè `<lake>/<kind>/batch=<id>/part-00000.parquet`; batch không có dòng ⇒ không có thư mục.

    Ghi vào tệp tạm cùng thư mục rồi `os.replace` (nguyên tử trên cùng filesystem): người đọc không
    bao giờ thấy tệp dở; tệp lạ còn sót (tệp tạm của lần chạy bị giết) bị xóa.
    """
    directory = batch_dir(lake_root, kind, batch_id)
    target = directory / LAKE_FILE
    if not rows:
        if directory.exists():
            for leftover in directory.iterdir():
                leftover.unlink()
            directory.rmdir()
        return None
    directory.mkdir(parents=True, exist_ok=True)
    schema = LAKE_SCHEMAS[kind]
    table = pa.Table.from_pylist([dict(r) for r in rows], schema=schema)
    tmp = directory / f".{LAKE_FILE}.{os.getpid()}.{threading.get_ident()}.tmp"
    pq.write_table(table, tmp, compression="zstd")
    os.replace(tmp, target)
    for leftover in directory.iterdir():
        if leftover.name != LAKE_FILE:
            leftover.unlink()
    return target


# ---------------------------------------------------------------------------------------------
# Ghi Redis + Kafka.
# ---------------------------------------------------------------------------------------------


class AlertProducer(Protocol):
    def send(self, topic: str, value: bytes, key: bytes | None = None, partition: int | None = None) -> float: ...
    def flush(self, timeout_s: float) -> int: ...


def rows_from_dataframe(batch_df: Any) -> list[dict[str, Any]]:
    """Dòng của micro-batch về driver (mảng → list Python). Batch chỉ vài nghìn dòng nên collect là đủ."""
    return [row.asDict(recursive=False) for row in batch_df.collect()]


@dataclass
class BatchResult:
    batch_id: int
    rows: int
    updates: int
    updates_written: int
    progress: int
    alerts: int
    alerts_created: int
    alerts_produced: int
    applied_events: int
    classes_touched: int
    classes_without_roster: int
    duplicate_applied_in_batch: int
    visible_ms: int | None
    redis_ms: float
    foreach_ms: float
    latency: dict[str, Any]

    def as_dict(self) -> dict[str, Any]:
        return dict(self.__dict__)


class ForeachBatchSink:
    """Hàm foreachBatch(batch_df, batch_id) có trạng thái (client Redis, producer Kafka) dùng lại giữa batch."""

    def __init__(
        self,
        cfg: RuntimeConfig,
        run_id: str,
        *,
        mode: str,
        model: str,
        key_prefix: str = "",
        alerts_topic: str | None = None,
        redis_client: Any = None,
        alert_producer: AlertProducer | None = None,
        lake_root: Path | None = None,
        window: LatencyWindow | None = None,
        clock_ms: Callable[[], int] | None = None,
    ) -> None:
        if mode not in MODES:
            raise ValueError(f"mode phải thuộc {MODES}, nhận {mode!r}")
        if model not in MODELS:
            raise ValueError(f"model phải thuộc {MODELS}, nhận {model!r}")
        self.cfg = cfg
        self.run_id = validate_run_id(run_id)
        self.mode = mode
        self.model = model
        self.keys = RedisKeys(key_prefix)
        self.alerts_topic = alerts_topic or cfg.kafka.topics["alerts"]
        self.lake_root = Path(lake_root) if lake_root is not None else cfg.paths.lake_dir(run_id)
        self.window = window or latency_window(run_id)
        self._clock_ms = clock_ms or (lambda: time.time_ns() // 1_000_000)
        self._redis = redis_client
        self._producer = alert_producer
        self._upsert: Any = None
        self._alert: Any = None
        self._cache = LearnerStateCache()
        self.last_result: BatchResult | None = None

    # Kết nối tạo lười: foreachBatch chạy trên driver, một client cho cả run.
    @property
    def redis(self) -> Any:
        if self._redis is None:
            import redis

            self._redis = redis.Redis.from_url(self.cfg.redis.url, socket_timeout=30, health_check_interval=30)
        return self._redis

    @property
    def producer(self) -> AlertProducer:
        if self._producer is None:
            from ai_math_tutor.ingestion.producer import IdempotentProducer, ProducerSettings

            self._producer = IdempotentProducer(ProducerSettings(
                self.cfg.kafka.bootstrap_servers, linger_ms=0, client_id=f"nhom8-alerts-{self.run_id}"))
        return self._producer

    def _scripts(self) -> tuple[Any, Any]:
        if self._upsert is None:
            self._upsert = self.redis.register_script(UPSERT_LUA_PATH.read_text(encoding="utf-8"))
            self._alert = self.redis.register_script(ALERT_LUA_PATH.read_text(encoding="utf-8"))
        return self._upsert, self._alert

    def __call__(self, batch_df: Any, batch_id: int) -> None:
        self.process_rows(rows_from_dataframe(batch_df), int(batch_id))

    def process_rows(self, rows: Sequence[Mapping[str, Any]], batch_id: int) -> BatchResult:
        """Toàn bộ side effect của một batch (tách khỏi Spark để test bằng dòng tự dựng)."""
        started = time.perf_counter()
        plan = plan_batch(rows)
        upsert, alert_script = self._scripts() if (plan.updates or plan.progress or plan.alerts) else (None, None)

        redis_started = time.perf_counter()
        updates_written = 0
        if upsert is not None:
            calls = upsert_calls(plan, self.keys, batch_id)
            if calls:
                pipe = self.redis.pipeline(transaction=False)
                for call_keys, call_args in calls:
                    upsert(keys=call_keys, args=call_args, client=pipe)
                try:
                    updates_written = sum(int(n) for n in pipe.execute())
                except BaseException:
                    # Không biết phần nào đã ghi: người học của batch phải nạp lại từ Redis.
                    self._cache.invalidate(row["learner_id"] for row in plan.updates)
                    raise
        self._note_written(plan, batch_id, updates_written)

        alert_states: list[tuple[dict[str, Any], bool, str, str]] = []
        if plan.alerts:
            pipe = self.redis.pipeline(transaction=False)
            prepared = []
            for row in plan.alerts:
                fields = alert_fields(row, self.run_id, batch_id)
                prepared.append((row, fields))
                args: list[str] = [str(ALERT_STREAM_MAXLEN)]
                for name, value in fields.items():
                    args.extend([name, value])
                alert_script(keys=[self.keys.alert(fields["alert_id"]), self.keys.alerts_stream], args=args,
                             client=pipe)
            for (row, fields), reply in zip(prepared, pipe.execute()):
                created = int(reply[0]) == 1
                status, stream_id = parse_alert_state(reply[1])
                alert_states.append((fields, created, status, stream_id))

        classes_without_roster = self._refresh_classes(plan, batch_id, cache=self._cache)
        visible_ms = self._clock_ms()
        redis_ms = (time.perf_counter() - redis_started) * 1000.0

        produced = self._produce_alerts(alert_states, visible_ms)
        latency_rows, system, staleness = self._latency_rows(plan, batch_id, visible_ms)
        self._write_lake(plan, batch_id, latency_rows, alert_states)
        self.window.add(visible_ms, system, staleness)
        summary = latency_summary(system, staleness)
        result = BatchResult(
            batch_id=batch_id,
            rows=plan.rows,
            updates=len(plan.updates),
            updates_written=updates_written,
            progress=len(plan.progress),
            alerts=len(plan.alerts),
            alerts_created=sum(1 for _, created, _, _ in alert_states if created),
            alerts_produced=produced,
            applied_events=len(plan.applied),
            classes_touched=len(plan.touched_classes),
            classes_without_roster=classes_without_roster,
            duplicate_applied_in_batch=plan.duplicate_applied_in_batch,
            visible_ms=visible_ms if plan.rows else None,
            redis_ms=round(redis_ms, 3),
            foreach_ms=round((time.perf_counter() - started) * 1000.0, 3),
            latency=summary,
        )
        self.window.record_batch(batch_id, result.as_dict())
        self.last_result = result
        if classes_without_roster:
            LOGGER.warning("batch %s: %s lớp bị chạm không có roster:{class} — bỏ qua tổng hợp lớp",
                           batch_id, classes_without_roster)
        return result

    def _note_written(self, plan: BatchPlan, batch_id: int, updates_written: int) -> None:
        """Cập nhật bộ đệm trạng thái theo kết quả upsert (xem bất biến của `LearnerStateCache`)."""
        if not plan.updates:
            return
        if updates_written == len(plan.updates) + len(plan.progress):
            for row in plan.updates:
                self._cache.apply(row["learner_id"], str(row["skill_id"]), state_value(row, batch_id))
        else:
            self._cache.invalidate(row["learner_id"] for row in plan.updates)

    def _read_states(self, learners: Sequence[str]) -> list[dict[Any, Any]]:
        if not learners:
            return []
        pipe = self.redis.pipeline(transaction=False)
        for learner in learners:
            pipe.hgetall(self.keys.state(learner))
        return list(pipe.execute())

    def _refresh_classes(self, plan: BatchPlan, batch_id: int, cache: LearnerStateCache | None = None) -> int:
        """Tính lại agg/stuck/trend cho lớp bị chạm, từ trạng thái hiện có của roster.

        `cache` None ⇒ đọc lại `state:*` của mọi thành viên roster từ Redis; có `cache` ⇒ chỉ đọc
        người học chưa có trong bộ đệm. Hai cách cho cùng kết quả (bất biến của `LearnerStateCache`).
        """
        if not plan.touched_classes:
            return 0
        classes = sorted(plan.touched_classes)
        pipe = self.redis.pipeline(transaction=False)
        for class_id in classes:
            pipe.smembers(self.keys.roster(class_id))
        rosters = {c: sorted(_decode(m) for m in members) for c, members in zip(classes, pipe.execute())}
        missing = [c for c in classes if not rosters[c]]
        learners = sorted({learner for c in classes for learner in rosters[c]})
        states: Mapping[str, Mapping[str, Mapping[str, Any]]]
        if cache is None:
            states = {learner: {_decode(k): json.loads(_decode(v)) for k, v in raw.items()}
                      for learner, raw in zip(learners, self._read_states(learners))}
        else:
            absent = cache.missing(learners)
            for learner, raw in zip(absent, self._read_states(absent)):
                cache.load(learner, raw)
            states = cache.states
        pipe = self.redis.pipeline(transaction=True)
        for class_id in classes:
            if not rosters[class_id]:
                continue
            aggregates, stuck = class_aggregates(class_id, rosters[class_id], states)
            for skill, values in aggregates.items():
                key = self.keys.agg(class_id, skill)
                pipe.delete(key)
                pipe.hset(key, mapping={**values, "batch_id": str(batch_id)})
            pipe.delete(self.keys.stuck_set(class_id))
            if stuck:
                pipe.sadd(self.keys.stuck_set(class_id), *stuck)
            event_ms = plan.touched_classes[class_id]
            if event_ms >= 0 and aggregates:
                trend_key = self.keys.trend(class_id, trend_bucket(snapshot_event_ms(aggregates, event_ms)))
                pipe.delete(trend_key)
                pipe.hset(trend_key, mapping={
                    skill: json.dumps({k: values[k] for k in ("observed", "mastered", "stuck", "mean_p_mastery",
                                                              "accuracy", "as_of_event_ms")},
                                      separators=(",", ":"), sort_keys=True)
                    for skill, values in aggregates.items()
                })
                pipe.expire(trend_key, TREND_TTL_S)
        pipe.execute()
        return len(missing)

    def _produce_alerts(self, alert_states: Sequence[tuple[dict[str, Any], bool, str, str]], visible_ms: int) -> int:
        """Gửi Kafka các cảnh báo còn 'pending' (vừa tạo, hoặc lần trước chết trước khi gửi xong)."""
        pending = [(fields, stream_id) for fields, _, status, stream_id in alert_states if status == "pending"]
        if not pending:
            return 0
        producer = self.producer
        stats = getattr(producer, "stats", None)
        errors_before = int(getattr(stats, "delivery_errors", 0) or 0)
        for fields, stream_id in pending:
            message = {**fields, "visible_ms": stream_id_ms(stream_id), "stream_id": stream_id}
            producer.send(self.alerts_topic, json.dumps(message, separators=(",", ":"), sort_keys=True).encode(),
                          fields["learner_id"].encode())
        remaining = producer.flush(ALERT_FLUSH_TIMEOUT_S)
        errors = int(getattr(stats, "delivery_errors", 0) or 0) - errors_before
        if remaining or errors:
            raise SinkError(f"topic {self.alerts_topic}: {remaining} cảnh báo chưa giao, {errors} lỗi giao "
                            "— batch sẽ chạy lại, cảnh báo còn 'pending' được gửi lại")
        pipe = self.redis.pipeline(transaction=False)
        for fields, stream_id in pending:
            pipe.set(self.keys.alert(fields["alert_id"]), f"produced:{stream_id}", xx=True)
        pipe.execute()
        return len(pending)

    @staticmethod
    def _latency_rows(plan: BatchPlan, batch_id: int, visible_ms: int) -> tuple[list[dict[str, Any]], np.ndarray, np.ndarray]:
        rows = []
        for applied in plan.applied:
            rows.append({
                "event_id": applied["event_id"],
                "learner_id": applied["learner_id"],
                "class_id": applied["class_id"],
                "event_time_ms": applied["event_time_ms"],
                "sent_time_ms": applied["sent_time_ms"],
                "visible_ms": visible_ms,
                "system_latency_ms": visible_ms - applied["sent_time_ms"],
                "staleness_ms": visible_ms - applied["event_time_ms"],
                "out_of_order": applied["out_of_order"],
                "batch_id": batch_id,
            })
        system = np.fromiter((r["system_latency_ms"] for r in rows), dtype=np.int64, count=len(rows))
        staleness = np.fromiter((r["staleness_ms"] for r in rows), dtype=np.int64, count=len(rows))
        return rows, system, staleness

    def _write_lake(self, plan: BatchPlan, batch_id: int, latency_rows: Sequence[Mapping[str, Any]],
                    alert_states: Sequence[tuple[dict[str, Any], bool, str, str]]) -> None:
        applied_rows = [{**row, "batch_id": batch_id} for row in plan.applied]
        alert_rows = [{
            "alert_id": fields["alert_id"],
            "alert_type": fields["alert_type"],
            "learner_id": fields["learner_id"],
            "class_id": fields["class_id"],
            "skill_id": fields["skill_id"],
            "onset_event_id": fields["onset_event_id"] or None,
            "alert_event_time_ms": int(fields["alert_event_time_ms"]) if fields["alert_event_time_ms"] else None,
            "visible_ms": stream_id_ms(stream_id),
            "stream_id": stream_id,
            "batch_id": batch_id,
        } for fields, _, _, stream_id in alert_states]
        write_batch_parquet(self.lake_root, "applied", batch_id, applied_rows)
        write_batch_parquet(self.lake_root, "latency", batch_id, latency_rows)
        write_batch_parquet(self.lake_root, "alerts", batch_id, alert_rows)

    def close(self) -> None:
        """Đóng kết nối (gọi sau khi query dừng); producer đã flush ở từng batch."""
        if self._producer is not None:
            self._producer.flush(ALERT_FLUSH_TIMEOUT_S)
        if self._redis is not None:
            try:
                self._redis.close()
            except Exception:  # đóng kết nối không được không làm hỏng kết quả đã ghi
                LOGGER.debug("không đóng được client Redis", exc_info=True)


def make_foreach_batch(
    cfg: RuntimeConfig,
    run_id: str,
    *,
    mode: str,
    model: str,
    key_prefix: str = "",
    alerts_topic: str | None = None,
) -> ForeachBatchSink:
    """Trả về hàm foreachBatch(batch_df, batch_id): ghi Redis (Lua idempotent), alerts (SET NX → XADD +
    produce), latency/applied/alerts theo batch=<batch_id> (ghi đè), cập nhật tổng hợp lớp bị chạm.
    Ghi visible_time sau khi Redis xong.

    `key_prefix`: tiền tố mọi khóa Redis (rỗng khi chạy thật; test dùng chung Redis đặt tiền tố riêng).
    `alerts_topic`: topic Kafka cảnh báo, mặc định `cfg.kafka.topics["alerts"]`.
    """
    return ForeachBatchSink(cfg, run_id, mode=mode, model=model, key_prefix=key_prefix, alerts_topic=alerts_topic)
