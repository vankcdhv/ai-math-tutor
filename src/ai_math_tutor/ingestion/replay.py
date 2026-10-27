"""Bộ phát lại open-loop: đưa `events.parquet` của bộ mô phỏng vào Kafka đúng lịch.

Bộ phát lại không tính toán gì về người học. Nó chỉ:
1. Chọn `replay_start` (epoch ms, chung cho mọi tiến trình) và gửi dòng có `replay_offset_ms = o`
   đúng lúc `replay_start + o` (hệ số thời gian 1). Open-loop: lịch không phụ thuộc broker trả lời
   nhanh hay chậm; nếu bị chậm (broker dừng, hàng đợi đầy) thì gửi bù ngay khi được và ghi
   **độ trượt lịch** = lúc tin thực sự vào producer − lúc đáng lẽ gửi.
2. Đóng dấu lại `event_time`/`sent_time` = `replay_start + offset` **một lần cho mỗi `event_id`**;
   bản trùng có chủ đích gửi lại đúng các byte đã đóng dấu của bản gốc (hợp đồng: retry = cùng byte).
3. Chia `P` tiến trình theo `crc32(learner_id) % P` (hàm băm ổn định giữa các tiến trình, khác
   `hash()` của Python bị ngẫu nhiên hóa) ⇒ mọi sự kiện của một người học đi qua một tiến trình,
   theo đúng thứ tự trong tệp; key Kafka = `learner_id` ⇒ cùng partition ⇒ thứ tự người học được giữ.
4. Hết log: gửi heartbeat (hợp đồng v0.2) vào **từng** partition (chỉ định partition tường minh),
   vòng k có `event_time = cuối log + W + 1 s + k s`, mỗi giây một vòng tới khi hết thời lượng
   hoặc hàm `until` của runner trả True. Watermark = max event_time − W nên vượt cuối log.

Kết quả: `results/raw/replay-<run_id>.json` + một dòng run record (experiment "ingestion").

CLI: python -m ai_math_tutor.ingestion.replay --events EVENTS.parquet [--processes P] [--duration S]
     [--rate-check] [--watermark-s W] [--heartbeat-s N | --no-heartbeat] [--linger-ms ...]
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import queue
import signal
import sys
import time
import traceback
import uuid
import zlib
from array import array
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, NamedTuple, Protocol

from ai_math_tutor.config import REPO_ROOT, ConfigError, load_config, validate_run_id
from ai_math_tutor.event_validation import HEARTBEAT_EVENT_TYPE, HEARTBEAT_LEARNER_ID
from ai_math_tutor.ingestion.producer import (
    DEFAULT_BATCH_SIZE,
    DEFAULT_COMPRESSION,
    DEFAULT_DELIVERY_TIMEOUT_MS,
    DEFAULT_LINGER_MS,
    DEFAULT_QUEUE_MAX_MESSAGES,
    COMPRESSION_TYPES,
    IdempotentProducer,
    ProducerSettings,
    ProducerStopped,
)
from ai_math_tutor.ingestion.topics import TopicError, partition_count
from ai_math_tutor.run_record import append_run_record, build_run_record
from ai_math_tutor.simulator.output import iso_from_epoch_ms, restamp

EXPERIMENT = "ingestion"
HEARTBEAT_SCHEMA_VERSION = "0.2"
HEARTBEAT_KEY = HEARTBEAT_LEARNER_ID.encode()
DEFAULT_WATERMARK_S = 120.0
DEFAULT_HEARTBEAT_S = 5.0
HEARTBEAT_INTERVAL_S = 1.0
# Tốc độ gửi thực dưới ngưỡng này ⇒ run không hợp lệ (đo nhầm giới hạn của bộ sinh tải).
RATE_OK_RATIO = 0.95
DEFAULT_LEAD_S = 1.0
DEFAULT_FLUSH_TIMEOUT_S = 120.0
DEFAULT_READY_TIMEOUT_S = 120.0
READ_COLUMNS = ("replay_offset_ms", "event_offset_ms", "sent_offset_ms", "event_id", "learner_id", "value")
_STOP_CHECK_EVERY = 256
_MAX_WAIT_S = 0.05
# Histogram độ trượt lịch: ô 0,1 ms tới 10 s; giá trị lớn hơn giữ nguyên trong danh sách tràn.
SLIP_BIN_MS = 0.1
SLIP_BINS = 100_000


class ReplayError(RuntimeError):
    """Không phát lại được (đầu vào rỗng/sai, topic chưa tạo, tiến trình con lỗi)."""


class ReplayRow(NamedTuple):
    replay_offset_ms: int
    event_offset_ms: int
    sent_offset_ms: int
    event_id: str
    learner_id: str
    value: str


class Clock(Protocol):
    def monotonic(self) -> float: ...
    def time_ms(self) -> int: ...
    def sleep(self, seconds: float) -> None: ...


class SystemClock:
    def monotonic(self) -> float:
        return time.monotonic()

    def time_ms(self) -> int:
        return time.time_ns() // 1_000_000

    def sleep(self, seconds: float) -> None:
        time.sleep(seconds)


class Sender(Protocol):
    """Đích gửi tối thiểu (producer thật bọc topic, hoặc bộ ghi trong bộ nhớ khi test)."""

    def send(self, value: bytes, key: bytes | None, partition: int | None = None) -> float: ...
    def poll(self, timeout_s: float) -> Any: ...


class TopicSender:
    def __init__(self, producer: IdempotentProducer, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    def send(self, value: bytes, key: bytes | None, partition: int | None = None) -> float:
        return self._producer.send(self._topic, value, key, partition)

    def poll(self, timeout_s: float) -> int:
        return self._producer.poll(timeout_s)


def shard_of(learner_id: str, processes: int) -> int:
    """Tiến trình phụ trách người học: crc32 ổn định giữa các tiến trình và các lần chạy."""
    if processes < 1:
        raise ValueError("processes phải ≥ 1")
    return zlib.crc32(learner_id.encode()) % processes


class SlipHistogram:
    """Phân bố độ trượt lịch (ms): đếm theo ô 0,1 ms để gộp giữa tiến trình mà không giữ từng giá trị."""

    def __init__(self) -> None:
        self.counts = array("q", bytes(8 * SLIP_BINS))
        self.overflow: list[float] = []
        self.count = 0
        self.max_ms = 0.0
        self.total_ms = 0.0

    def add(self, slip_ms: float) -> None:
        if slip_ms < 0:
            slip_ms = 0.0
        self.count += 1
        self.total_ms += slip_ms
        if slip_ms > self.max_ms:
            self.max_ms = slip_ms
        index = int(slip_ms / SLIP_BIN_MS)
        if index < SLIP_BINS:
            self.counts[index] += 1
        else:
            self.overflow.append(slip_ms)

    def merge(self, other: SlipHistogram) -> None:
        for index, value in enumerate(other.counts):
            if value:
                self.counts[index] += value
        self.overflow.extend(other.overflow)
        self.count += other.count
        self.total_ms += other.total_ms
        self.max_ms = max(self.max_ms, other.max_ms)

    def quantile(self, q: float) -> float | None:
        """Phân vị q (0..1), sai số ≤ 0,1 ms trong vùng ô; chính xác trong vùng tràn."""
        if not 0 <= q <= 1:
            raise ValueError("q phải trong [0, 1]")
        if self.count == 0:
            return None
        rank = max(1, int(-(-q * self.count // 1)))  # ceil(q·n), tối thiểu 1
        seen = 0
        for index, value in enumerate(self.counts):
            seen += value
            if seen >= rank:
                return min((index + 1) * SLIP_BIN_MS, self.max_ms)
        overflow = sorted(self.overflow)
        return overflow[rank - seen - 1]

    def summary(self) -> dict[str, float | int | None]:
        def rounded(value: float | None) -> float | None:
            return None if value is None else round(value, 3)

        return {
            "count": self.count,
            "mean_ms": rounded(self.total_ms / self.count) if self.count else None,
            "p50_ms": rounded(self.quantile(0.50)),
            "p95_ms": rounded(self.quantile(0.95)),
            "p99_ms": rounded(self.quantile(0.99)),
            "max_ms": rounded(self.max_ms) if self.count else None,
        }

    def to_state(self) -> tuple[bytes, list[float], int, float, float]:
        return self.counts.tobytes(), self.overflow, self.count, self.max_ms, self.total_ms

    @classmethod
    def from_state(cls, state: tuple[bytes, list[float], int, float, float]) -> SlipHistogram:
        histogram = cls()
        counts, histogram.overflow, histogram.count, histogram.max_ms, histogram.total_ms = state
        histogram.counts = array("q")
        histogram.counts.frombytes(counts)
        return histogram


@dataclass
class WindowSummary:
    """Các dòng sẽ phát trong cửa sổ `replay_offset_ms < cutoff` (cả tệp nếu không cắt)."""

    rows: int
    file_rows: int
    unique_events: int
    duplicate_deliveries: int
    first_offset_ms: int
    last_offset_ms: int
    max_event_offset_ms: int
    shard_rows: list[int]
    # event_id → số lần giao thêm (bản trùng), theo tiến trình phụ trách.
    shard_duplicates: list[dict[str, int]]

    @property
    def complete(self) -> bool:
        return self.rows == self.file_rows


def summarize_window(events_path: Path, cutoff_ms: int | None, processes: int) -> WindowSummary:
    """Đếm trước (pyarrow, không qua Python từng dòng): số dòng, số duy nhất, bản trùng theo shard."""
    import pyarrow as pa
    import pyarrow.compute as pc
    import pyarrow.parquet as pq

    file_rows = pq.ParquetFile(str(events_path)).metadata.num_rows
    filters = [("replay_offset_ms", "<", int(cutoff_ms))] if cutoff_ms is not None else None
    table = pq.read_table(str(events_path), columns=["replay_offset_ms", "event_offset_ms", "event_id", "learner_id"],
                          filters=filters)
    rows = table.num_rows
    if rows == 0:
        return WindowSummary(0, file_rows, 0, 0, 0, 0, 0, [0] * processes, [{} for _ in range(processes)])
    offsets = table.column("replay_offset_ms")
    by_learner = table.group_by("learner_id").aggregate([("event_id", "count")])
    shard_rows = [0] * processes
    for learner, count in zip(by_learner.column("learner_id").to_pylist(), by_learner.column("event_id_count").to_pylist()):
        shard_rows[shard_of(learner, processes)] += count
    by_event = table.group_by(["event_id", "learner_id"]).aggregate([("replay_offset_ms", "count")])
    unique = by_event.num_rows
    repeated = by_event.filter(pc.greater(by_event.column("replay_offset_ms_count"), pa.scalar(1, pa.int64())))
    shard_duplicates: list[dict[str, int]] = [{} for _ in range(processes)]
    for event_id, learner, count in zip(repeated.column("event_id").to_pylist(),
                                        repeated.column("learner_id").to_pylist(),
                                        repeated.column("replay_offset_ms_count").to_pylist()):
        shard_duplicates[shard_of(learner, processes)][event_id] = count - 1
    if unique != pc.count_distinct(table.column("event_id")).as_py():
        raise ReplayError("một event_id xuất hiện với nhiều learner_id — vi phạm hợp đồng")
    return WindowSummary(
        rows=rows,
        file_rows=file_rows,
        unique_events=unique,
        duplicate_deliveries=rows - unique,
        first_offset_ms=int(pc.min(offsets).as_py()),
        last_offset_ms=int(pc.max(offsets).as_py()),
        max_event_offset_ms=int(pc.max(table.column("event_offset_ms")).as_py()),
        shard_rows=shard_rows,
        shard_duplicates=shard_duplicates,
    )


def iter_window_rows(
    events_path: Path,
    *,
    cutoff_ms: int | None = None,
    shard: int = 0,
    processes: int = 1,
    batch_size: int = 16_384,
) -> Iterator[ReplayRow]:
    """Các dòng của một shard theo đúng thứ tự tệp (đã sắp theo `replay_offset_ms`), đọc theo lô."""
    import pyarrow as pa
    import pyarrow.parquet as pq

    if not 0 <= shard < processes:
        raise ValueError(f"shard {shard} ngoài [0, {processes})")
    parquet = pq.ParquetFile(str(events_path))
    shard_cache: dict[str, bool] = {}
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(READ_COLUMNS)):
        offsets = batch.column(0).to_pylist()
        if cutoff_ms is not None and offsets and offsets[0] >= cutoff_ms:
            return
        if processes == 1 and (cutoff_ms is None or offsets[-1] < cutoff_ms):
            selected = batch
        else:
            learners = batch.column(4).to_pylist()
            keep: list[int] = []
            for index, (offset, learner) in enumerate(zip(offsets, learners)):
                if cutoff_ms is not None and offset >= cutoff_ms:
                    break
                mine = shard_cache.get(learner)
                if mine is None:
                    mine = shard_cache[learner] = shard_of(learner, processes) == shard
                if mine:
                    keep.append(index)
            if not keep:
                continue
            selected = batch.take(pa.array(keep, type=pa.int32()))
        columns = [selected.column(i).to_pylist() for i in range(len(READ_COLUMNS))]
        for values in zip(*columns):
            yield ReplayRow(*values)


class Stamper:
    """Đóng dấu một lần cho mỗi `event_id`; byte của bản gốc được giữ tới khi bản trùng cuối cùng đã gửi.

    `duplicate_counts`: event_id → số lần giao thêm dự kiến (đếm trước trong `summarize_window`),
    nên bộ nhớ đệm chỉ chứa sự kiện còn bản trùng chưa gửi.
    """

    def __init__(self, replay_start_ms: int, duplicate_counts: Mapping[str, int]) -> None:
        self.replay_start_ms = replay_start_ms
        self._pending = dict(duplicate_counts)
        self._cache: dict[str, bytes] = {}
        self.stamped = 0
        self.from_cache = 0
        self.max_cached = 0

    def value_for(self, row: ReplayRow) -> bytes:
        cached = self._cache.get(row.event_id)
        if cached is not None:
            self.from_cache += 1
            left = self._pending[row.event_id] - 1
            if left <= 0:
                del self._cache[row.event_id]
                del self._pending[row.event_id]
            else:
                self._pending[row.event_id] = left
            return cached
        stamped = restamp(row.value, self.replay_start_ms, row.event_offset_ms, row.sent_offset_ms).encode()
        self.stamped += 1
        if self._pending.get(row.event_id, 0) > 0:
            self._cache[row.event_id] = stamped
            if len(self._cache) > self.max_cached:
                self.max_cached = len(self._cache)
        return stamped

    @property
    def cached(self) -> int:
        return len(self._cache)


@dataclass
class ShardProgress:
    scheduled_sent: int = 0
    first_send_ms: int | None = None
    last_send_ms: int | None = None
    backpressure_s: float = 0.0
    stopped_early: bool = False
    slip: SlipHistogram = field(default_factory=SlipHistogram)


def replay_rows(
    rows: Iterable[ReplayRow],
    *,
    sender: Sender,
    stamper: Stamper,
    replay_start_ms: int,
    clock: Clock | None = None,
    should_stop: Callable[[], bool] | None = None,
) -> ShardProgress:
    """Vòng lặp open-loop của một shard: chờ tới `replay_start + offset`, gửi, ghi độ trượt lịch."""
    clock = clock or SystemClock()
    progress = ShardProgress()
    # Ánh xạ epoch → monotonic một lần, sau đó chỉ dùng đồng hồ đơn điệu (không bị NTP chỉnh giữa run).
    mono_start = clock.monotonic() + (replay_start_ms - clock.time_ms()) / 1000.0
    stop_check = 0
    for row in rows:
        stop_check += 1
        if should_stop is not None and stop_check % _STOP_CHECK_EVERY == 0 and should_stop():
            progress.stopped_early = True
            break
        value = stamper.value_for(row)
        target = mono_start + row.replay_offset_ms / 1000.0
        now = clock.monotonic()
        while now < target:
            wait = target - now
            if wait < 0.001:
                clock.sleep(wait)  # poll() chỉ phân giải ms; khoảng ngắn hơn ngủ trực tiếp
            else:
                sender.poll(min(wait, _MAX_WAIT_S))  # vừa chờ vừa phục vụ callback giao
                if should_stop is not None and should_stop():
                    progress.stopped_early = True
                    return progress
            now = clock.monotonic()
        key = row.learner_id.encode()
        try:
            progress.backpressure_s += sender.send(value, key)
        except ProducerStopped:
            progress.stopped_early = True
            break
        sent_at = clock.monotonic()
        progress.slip.add((sent_at - target) * 1000.0)
        sent_ms = replay_start_ms + round((sent_at - mono_start) * 1000.0)
        if progress.first_send_ms is None:
            progress.first_send_ms = sent_ms
        progress.last_send_ms = sent_ms
        progress.scheduled_sent += 1
    return progress


def heartbeat_event_ms(end_of_log_ms: int, watermark_s: float, round_index: int) -> int:
    """event_time của vòng heartbeat thứ k: cuối log + W + 1 s + k giây."""
    return end_of_log_ms + round(watermark_s * 1000) + 1000 + round_index * 1000


def build_heartbeat(event_ms: int, event_id: str | None = None) -> bytes:
    """Heartbeat hợp đồng v0.2: learner dành riêng, payload rỗng, sent_time = event_time (tổng hợp)."""
    stamp = iso_from_epoch_ms(event_ms)
    return json.dumps({
        "schema_version": HEARTBEAT_SCHEMA_VERSION,
        "event_id": event_id or str(uuid.uuid4()),
        "event_type": HEARTBEAT_EVENT_TYPE,
        "learner_id": HEARTBEAT_LEARNER_ID,
        "event_time": stamp,
        "sent_time": stamp,
        "payload": {},
    }).encode()


@dataclass
class HeartbeatResult:
    rounds: int = 0
    sent: int = 0
    partitions: int = 0
    first_event_ms: int | None = None
    last_event_ms: int | None = None
    until_satisfied: bool | None = None


def send_heartbeats(
    sender: Sender,
    *,
    partitions: int,
    end_of_log_ms: int,
    watermark_s: float,
    duration_s: float,
    until: Callable[[], bool] | None = None,
    should_stop: Callable[[], bool] | None = None,
    clock: Clock | None = None,
    interval_s: float = HEARTBEAT_INTERVAL_S,
) -> HeartbeatResult:
    """Mỗi `interval_s` một vòng heartbeat tới mọi partition, tới khi `until()` đúng hoặc hết `duration_s`.

    Luôn gửi ít nhất một vòng. Có `until`: `duration_s` là giới hạn trên (quá hạn ⇒ `until_satisfied=False`).
    """
    if partitions < 1:
        raise ValueError("partitions phải ≥ 1")
    clock = clock or SystemClock()
    result = HeartbeatResult(partitions=partitions)
    started = clock.monotonic()
    while True:
        event_ms = heartbeat_event_ms(end_of_log_ms, watermark_s, result.rounds)
        for partition in range(partitions):
            sender.send(build_heartbeat(event_ms), HEARTBEAT_KEY, partition)
            result.sent += 1
        if result.first_event_ms is None:
            result.first_event_ms = event_ms
        result.last_event_ms = event_ms
        result.rounds += 1
        if until is not None and until():
            result.until_satisfied = True
            return result
        next_round = started + result.rounds * interval_s
        if next_round - started >= duration_s or (should_stop is not None and should_stop()):
            if until is not None:
                result.until_satisfied = False
            return result
        while (now := clock.monotonic()) < next_round:
            sender.poll(min(next_round - now, _MAX_WAIT_S))


@dataclass(frozen=True)
class ReplaySettings:
    events_path: Path
    topic: str
    producer: ProducerSettings
    processes: int = 1
    duration_s: float | None = None
    watermark_s: float = DEFAULT_WATERMARK_S
    heartbeat_s: float | None = DEFAULT_HEARTBEAT_S  # None ⇒ không gửi heartbeat
    lead_s: float = DEFAULT_LEAD_S
    flush_timeout_s: float = DEFAULT_FLUSH_TIMEOUT_S
    ready_timeout_s: float = DEFAULT_READY_TIMEOUT_S

    def __post_init__(self) -> None:
        if isinstance(self.processes, bool) or not isinstance(self.processes, int) or not 1 <= self.processes <= 64:
            raise ValueError(f"processes phải trong [1, 64], nhận {self.processes!r}")
        if self.duration_s is not None and not self.duration_s > 0:
            raise ValueError("duration_s phải > 0")
        if not self.watermark_s >= 0:
            raise ValueError("watermark_s phải ≥ 0")
        if self.heartbeat_s is not None and not self.heartbeat_s >= 0:
            raise ValueError("heartbeat_s phải ≥ 0")
        for name in ("lead_s", "flush_timeout_s", "ready_timeout_s"):
            if not getattr(self, name) > 0:
                raise ValueError(f"{name} phải > 0")

    @property
    def cutoff_ms(self) -> int | None:
        return None if self.duration_s is None else round(self.duration_s * 1000)


@dataclass(frozen=True)
class _ShardJob:
    events_path: str
    topic: str
    producer: ProducerSettings
    shard: int
    processes: int
    cutoff_ms: int | None
    duplicate_counts: dict[str, int]
    flush_timeout_s: float
    parent_pid: int


def _shard_main(job: _ShardJob, start_event: Any, start_ms: Any, stop_event: Any, results: Any) -> None:
    """Thân tiến trình con: một producer idempotent cho một shard người học."""
    signal.signal(signal.SIGINT, signal.SIG_IGN)  # Ctrl-C do tiến trình cha xử lý rồi báo qua stop_event

    def stopped() -> bool:
        return stop_event.is_set() or os.getppid() != job.parent_pid

    try:
        producer = IdempotentProducer(job.producer, should_stop=stopped)
        if producer.partition_count(job.topic) is None:
            raise ReplayError(f"topic {job.topic} không tồn tại")
        rows = iter_window_rows(Path(job.events_path), cutoff_ms=job.cutoff_ms, shard=job.shard,
                                processes=job.processes)
        results.put(("ready", job.shard, None))
        while not start_event.wait(0.2):
            if stopped():
                return
        replay_start_ms = int(start_ms.value)
        stamper = Stamper(replay_start_ms, job.duplicate_counts)
        progress = replay_rows(rows, sender=TopicSender(producer, job.topic), stamper=stamper,
                               replay_start_ms=replay_start_ms, should_stop=stopped)
        remaining = producer.flush(job.flush_timeout_s)
        results.put(("result", job.shard, {
            "shard": job.shard,
            "sent": progress.scheduled_sent,
            "first_send_ms": progress.first_send_ms,
            "last_send_ms": progress.last_send_ms,
            "stopped_early": progress.stopped_early,
            "stamped": stamper.stamped,
            "duplicates_from_cache": stamper.from_cache,
            "max_cached": stamper.max_cached,
            "undelivered_after_flush": remaining,
            "producer": producer.stats.as_dict(),
            "slip_state": progress.slip.to_state(),
        }))
    except BaseException:  # noqa: BLE001 — mọi lỗi phải về được tiến trình cha, kể cả KeyboardInterrupt
        results.put(("error", job.shard, traceback.format_exc()))


def _rate(count: int, start_ms: int | None, end_ms: int | None) -> float | None:
    if start_ms is None or end_ms is None or end_ms <= start_ms:
        return None
    return round(count * 1000.0 / (end_ms - start_ms), 3)


def _collect(results: Any, workers: list[Any], kind: str, deadline: float, stop_requested: Callable[[], bool],
             on_stop: Callable[[], None]) -> dict[int, Any]:
    """Chờ mỗi shard gửi một thông báo `kind`; lỗi/chết bất thường của tiến trình con ⇒ `ReplayError`."""
    collected: dict[int, Any] = {}
    stop_sent = False
    while len(collected) < len(workers):
        if not stop_sent and stop_requested():
            on_stop()
            stop_sent = True
        if time.monotonic() > deadline:
            raise ReplayError(f"quá hạn chờ tiến trình con ({kind}: {len(collected)}/{len(workers)})")
        try:
            message_kind, shard, payload = results.get(timeout=0.5)
        except queue.Empty:
            # Tiến trình con xả hàng đợi trước khi thoát ⇒ đã chết mà không có thông báo là chết bất thường.
            dead = [shard for shard, worker in enumerate(workers)
                    if shard not in collected and not worker.is_alive()]
            if dead and not stop_sent:
                codes = {shard: workers[shard].exitcode for shard in dead}
                raise ReplayError(f"tiến trình shard chết không báo kết quả (exitcode {codes})") from None
            if dead and stop_sent:
                return collected
            continue
        if message_kind == "error":
            raise ReplayError(f"tiến trình shard {shard} lỗi:\n{payload}")
        if message_kind == kind:
            collected[shard] = payload
    return collected


def run_replay(
    settings: ReplaySettings,
    *,
    heartbeat_until: Callable[[], bool] | None = None,
    on_started: Callable[[int], None] | None = None,
    stop_requested: Callable[[], bool] | None = None,
) -> dict[str, Any]:
    """Phát lại một lần; trả dict kết quả (cũng là nội dung `replay-<run>.json`).

    `on_started(replay_start_ms)`: gọi ngay khi lịch bắt đầu (runner dịch ground truth bằng giá trị này).
    `heartbeat_until`: runner trả True khi watermark đã qua cuối log ⇒ dừng heartbeat sớm.
    `stop_requested`: hỏi định kỳ; True ⇒ các shard dừng gửi, flush, rồi báo kết quả một phần.
    """
    wall_started = time.monotonic()
    started_at = datetime.now(timezone.utc)
    events_path = Path(settings.events_path)
    if not events_path.is_file():
        raise ReplayError(f"không thấy tệp sự kiện: {events_path}")
    window = summarize_window(events_path, settings.cutoff_ms, settings.processes)
    if window.rows == 0:
        raise ReplayError("cửa sổ phát lại không có dòng nào")
    try:
        partitions = partition_count(settings.producer.bootstrap_servers, settings.topic)
    except TopicError as exc:
        raise ReplayError(str(exc)) from exc
    stop_requested = stop_requested or (lambda: False)

    ctx = mp.get_context("spawn")  # không fork tiến trình đã có luồng librdkafka/pyarrow
    start_event, stop_event = ctx.Event(), ctx.Event()
    start_ms = ctx.Value("q", 0)
    results = ctx.Queue()
    processes = [
        ctx.Process(
            target=_shard_main,
            name=f"replay-shard-{shard}",
            args=(_ShardJob(str(events_path), settings.topic, settings.producer, shard, settings.processes,
                            settings.cutoff_ms, window.shard_duplicates[shard], settings.flush_timeout_s,
                            os.getpid()),
                  start_event, start_ms, stop_event, results),
            daemon=True,
        )
        for shard in range(settings.processes)
    ]
    shard_results: dict[int, Any] = {}
    try:
        for process in processes:
            process.start()
        _collect(results, processes, "ready", time.monotonic() + settings.ready_timeout_s,
                 stop_requested, stop_event.set)
        if stop_requested():
            raise ReplayError("dừng theo yêu cầu trước khi bắt đầu phát lại")
        replay_start_ms = time.time_ns() // 1_000_000 + round(settings.lead_s * 1000)
        start_ms.value = replay_start_ms
        start_event.set()
        if on_started is not None:
            on_started(replay_start_ms)
        # Lưới an toàn, không phải giới hạn đo: lịch + thời hạn giao tối đa của librdkafka + flush.
        deadline = (time.monotonic() + settings.lead_s + max(window.last_offset_ms, 0) / 1000.0
                    + settings.producer.delivery_timeout_ms / 1000.0 + settings.flush_timeout_s + 120.0)
        shard_results = _collect(results, processes, "result", deadline, stop_requested, stop_event.set)
        if len(shard_results) < settings.processes:
            raise ReplayError(f"chỉ {len(shard_results)}/{settings.processes} shard báo kết quả sau khi dừng")
    finally:
        if len(shard_results) < settings.processes:
            stop_event.set()
        for process in processes:
            process.join(timeout=10)
        for process in processes:
            if process.is_alive():
                process.terminate()
                process.join(timeout=5)

    slip = SlipHistogram()
    totals = {"sent": 0, "delivered": 0, "delivery_errors": 0, "undelivered_after_flush": 0,
              "buffer_full_events": 0, "backpressure_s": 0.0, "stamped": 0, "duplicates_from_cache": 0}
    shards_out: list[dict[str, Any]] = []
    error_counts: dict[str, int] = {}
    client_errors: dict[str, int] = {}
    for shard in range(settings.processes):
        data = dict(shard_results[shard])
        slip_shard = SlipHistogram.from_state(data.pop("slip_state"))
        slip.merge(slip_shard)
        stats = data["producer"]
        totals["sent"] += data["sent"]
        totals["stamped"] += data["stamped"]
        totals["duplicates_from_cache"] += data["duplicates_from_cache"]
        totals["undelivered_after_flush"] += data["undelivered_after_flush"]
        totals["delivered"] += stats["delivered"]
        totals["delivery_errors"] += stats["delivery_errors"]
        totals["buffer_full_events"] += stats["buffer_full_events"]
        totals["backpressure_s"] += stats["backpressure_s"]
        for name, count in stats["error_counts"].items():
            error_counts[name] = error_counts.get(name, 0) + count
        for name, count in stats["client_errors"].items():
            client_errors[name] = client_errors.get(name, 0) + count
        data["scheduled"] = window.shard_rows[shard]
        data["slip_ms"] = slip_shard.summary()
        shards_out.append(data)

    first_scheduled_ms = replay_start_ms + window.first_offset_ms
    last_send_ms = max((s["last_send_ms"] for s in shards_out if s["last_send_ms"] is not None), default=None)
    last_ack_ms = max((s["producer"]["last_ack_epoch_ms"] for s in shards_out
                       if s["producer"]["last_ack_epoch_ms"] is not None), default=None)
    target_rate = _rate(window.rows, window.first_offset_ms, window.last_offset_ms)
    achieved_rate = _rate(totals["sent"], first_scheduled_ms, last_send_ms)
    ratio = round(achieved_rate / target_rate, 4) if achieved_rate and target_rate else None
    stopped_early = any(s["stopped_early"] for s in shards_out)
    end_of_log_ms = replay_start_ms + window.max_event_offset_ms

    heartbeat: HeartbeatResult | None = None
    if settings.heartbeat_s is not None and not stopped_early and not stop_requested():
        producer = IdempotentProducer(settings.producer, should_stop=stop_requested)
        try:
            heartbeat = send_heartbeats(TopicSender(producer, settings.topic), partitions=partitions,
                                        end_of_log_ms=end_of_log_ms, watermark_s=settings.watermark_s,
                                        duration_s=settings.heartbeat_s, until=heartbeat_until,
                                        should_stop=stop_requested)
        except ProducerStopped:
            heartbeat = HeartbeatResult(partitions=partitions, until_satisfied=False)
        heartbeat_undelivered = producer.flush(settings.flush_timeout_s)
        heartbeat_errors = producer.stats.delivery_errors
    else:
        heartbeat_undelivered = heartbeat_errors = 0

    delivered_all = (totals["delivered"] == totals["sent"] and totals["delivery_errors"] == 0
                     and totals["undelivered_after_flush"] == 0)
    return {
        "run_id": None,
        "experiment": EXPERIMENT,
        "started_at": started_at.isoformat(),
        "events_path": str(events_path),
        "topic": settings.topic,
        "partitions": partitions,
        "processes": settings.processes,
        "replay_start_ms": replay_start_ms,
        "replay_start": iso_from_epoch_ms(replay_start_ms),
        "time_scale": 1.0,
        "cutoff_ms": settings.cutoff_ms,
        "window": {
            "rows": window.rows,
            "file_rows": window.file_rows,
            "complete": window.complete,
            "unique_events": window.unique_events,
            "duplicate_deliveries": window.duplicate_deliveries,
            "first_offset_ms": window.first_offset_ms,
            "last_offset_ms": window.last_offset_ms,
            "max_event_offset_ms": window.max_event_offset_ms,
        },
        "producer_config": settings.producer.to_config(),
        "scheduled": window.rows,
        **{key: (round(value, 3) if isinstance(value, float) else value) for key, value in totals.items()},
        "delivery_error_counts": error_counts,
        "client_errors": client_errors,
        "delivered_all": delivered_all,
        "stopped_early": stopped_early,
        "target_rate_eps": target_rate,
        "achieved_rate_eps": achieved_rate,
        "acked_rate_eps": _rate(totals["delivered"], first_scheduled_ms, last_ack_ms),
        "rate_ratio": ratio,
        "rate_ok": ratio is not None and ratio >= RATE_OK_RATIO,
        "slip_ms": slip.summary(),
        "end_of_log_ms": end_of_log_ms,
        "end_of_log": iso_from_epoch_ms(end_of_log_ms),
        "watermark_s": settings.watermark_s,
        "heartbeat": None if heartbeat is None else {
            **asdict(heartbeat),
            "first_event_time": iso_from_epoch_ms(heartbeat.first_event_ms) if heartbeat.first_event_ms else None,
            "undelivered_after_flush": heartbeat_undelivered,
            "delivery_errors": heartbeat_errors,
        },
        "shards": shards_out,
        "wall_s": round(time.monotonic() - wall_started, 3),
    }


def _load_manifest(path: Path | None) -> dict[str, Any] | None:
    if path is None or not path.is_file():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise ReplayError(f"không đọc được manifest {path}: {exc}") from exc


def replay_status(result: Mapping[str, Any], rate_check: bool) -> str:
    """success khi mọi tin đã giao, không dừng sớm và (nếu kiểm) tốc độ thực ≥ 95% mục tiêu."""
    if result["stopped_early"]:
        return "aborted"
    heartbeat = result.get("heartbeat") or {}
    if not result["delivered_all"] or heartbeat.get("delivery_errors") or heartbeat.get("undelivered_after_flush"):
        return "failed"
    if rate_check and not result["rate_ok"]:
        return "failed"
    return "success"


def _display_path(path: Path) -> str:
    """Đường dẫn tương đối với gốc repo khi nằm trong repo (run record không phụ thuộc máy)."""
    try:
        return str(path.resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def write_results(
    result: dict[str, Any],
    *,
    results_dir: Path,
    run_id: str,
    manifest: Mapping[str, Any] | None,
    rate_check: bool,
    run_record: bool = True,
) -> Path:
    """Ghi `replay-<run_id>.json` và (tùy chọn) nối một run record vào `runs.jsonl`."""
    validate_run_id(run_id)
    result["run_id"] = run_id
    result["status"] = replay_status(result, rate_check)
    result["rate_check"] = rate_check
    if manifest is not None:
        result["manifest"] = {
            "simulator_version": manifest.get("simulator_version"),
            "seed": manifest.get("seed"),
            "params": manifest.get("params"),
            "counts": {k: manifest.get("counts", {}).get(k) for k in ("unique_events", "deliveries",
                                                                     "duplicate_deliveries", "learners")},
            "events_checksum": manifest.get("outputs", {}).get("events.parquet", {}).get("checksum"),
        }
    results_dir.mkdir(parents=True, exist_ok=True)
    path = results_dir / f"replay-{run_id}.json"
    path.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if run_record:
        record = build_run_record(
            experiment=EXPERIMENT,
            data_version=(result.get("manifest") or {}).get("events_checksum") or f"file:{result['events_path']}",
            parameters={
                "events_path": result["events_path"], "topic": result["topic"], "partitions": result["partitions"],
                "processes": result["processes"], "cutoff_ms": result["cutoff_ms"], "time_scale": 1.0,
                "watermark_s": result["watermark_s"], "producer_config": result["producer_config"],
                "target_rate_param": ((result.get("manifest") or {}).get("params") or {}).get("target_rate"),
            },
            seed=int((manifest or {}).get("seed") or 0),
            timeout_s=float(result["wall_s"]),
            metrics={key: result[key] for key in (
                "replay_start_ms", "replay_start", "scheduled", "sent", "delivered", "delivery_errors",
                "undelivered_after_flush", "buffer_full_events", "backpressure_s", "target_rate_eps",
                "achieved_rate_eps", "acked_rate_eps", "rate_ratio", "rate_ok", "slip_ms", "end_of_log_ms")},
            status=result["status"],
            log_path=_display_path(path),
            started_at=datetime.fromisoformat(result["started_at"]),
            repo_dir=REPO_ROOT,
            run_id=run_id,
            runtime_s=float(result["wall_s"]),
        )
        append_run_record(record, results_dir / "runs.jsonl")
    return path


def default_run_id() -> str:
    return f"{EXPERIMENT}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--events", type=Path, required=True, help="events.parquet của bộ mô phỏng")
    parser.add_argument("--manifest", type=Path, default=None, help="manifest.json (mặc định cạnh --events)")
    parser.add_argument("--topic", default=None, help="mặc định learner-events theo cấu hình")
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--processes", "-P", type=int, default=1, help="số tiến trình gửi (shard theo learner)")
    parser.add_argument("--duration", type=float, default=None,
                        help="chỉ phát các dòng có replay_offset < S giây (mặc định cả log)")
    parser.add_argument("--rate-check", action="store_true",
                        help=f"run thất bại (mã 1) nếu tốc độ gửi thực < {RATE_OK_RATIO:.0%} mục tiêu")
    parser.add_argument("--watermark-s", type=float, default=DEFAULT_WATERMARK_S, help="W của pipeline (heartbeat)")
    beat = parser.add_mutually_exclusive_group()
    beat.add_argument("--heartbeat-s", type=float, default=DEFAULT_HEARTBEAT_S,
                      help="gửi heartbeat mỗi giây trong N giây sau khi hết log")
    beat.add_argument("--no-heartbeat", action="store_true")
    parser.add_argument("--linger-ms", type=float, default=DEFAULT_LINGER_MS)
    parser.add_argument("--batch-size", type=int, default=DEFAULT_BATCH_SIZE)
    parser.add_argument("--compression", choices=COMPRESSION_TYPES, default=DEFAULT_COMPRESSION)
    parser.add_argument("--queue-max-messages", type=int, default=DEFAULT_QUEUE_MAX_MESSAGES)
    parser.add_argument("--delivery-timeout-ms", type=int, default=DEFAULT_DELIVERY_TIMEOUT_MS)
    parser.add_argument("--flush-timeout-s", type=float, default=DEFAULT_FLUSH_TIMEOUT_S)
    parser.add_argument("--lead-s", type=float, default=DEFAULT_LEAD_S, help="khoảng chờ từ lúc sẵn sàng tới replay_start")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--results-dir", type=Path, default=None, help="mặc định results/raw của repo")
    parser.add_argument("--no-run-record", action="store_true", help="không nối run record vào runs.jsonl")
    args = parser.parse_args(argv)

    stop_flag = {"stop": False}

    def request_stop(signum: int, _frame: Any) -> None:
        stop_flag["stop"] = True

    signal.signal(signal.SIGINT, request_stop)
    signal.signal(signal.SIGTERM, request_stop)
    try:
        cfg = load_config()
        run_id = validate_run_id(args.run_id) if args.run_id else default_run_id()
        settings = ReplaySettings(
            events_path=args.events,
            topic=args.topic or cfg.kafka.topics["learner_events"],
            producer=ProducerSettings(
                bootstrap_servers=args.bootstrap or cfg.kafka.bootstrap_servers,
                linger_ms=args.linger_ms, batch_size=args.batch_size, compression=args.compression,
                queue_max_messages=args.queue_max_messages, delivery_timeout_ms=args.delivery_timeout_ms,
            ),
            processes=args.processes,
            duration_s=args.duration,
            watermark_s=args.watermark_s,
            heartbeat_s=None if args.no_heartbeat else args.heartbeat_s,
            lead_s=args.lead_s,
            flush_timeout_s=args.flush_timeout_s,
        )
        manifest = _load_manifest(args.manifest or args.events.with_name("manifest.json"))
        result = run_replay(settings, stop_requested=lambda: stop_flag["stop"])
        path = write_results(result, results_dir=args.results_dir or cfg.paths.results, run_id=run_id,
                             manifest=manifest, rate_check=args.rate_check, run_record=not args.no_run_record)
    except (ReplayError, ConfigError, ValueError) as exc:
        print(f"lỗi: {exc}", file=sys.stderr)
        return 1
    brief = {key: result[key] for key in (
        "run_id", "status", "replay_start", "processes", "partitions", "scheduled", "sent", "delivered",
        "delivery_errors", "buffer_full_events", "target_rate_eps", "achieved_rate_eps", "rate_ratio", "slip_ms")}
    brief["result_path"] = str(path)
    print(json.dumps(brief, ensure_ascii=False, indent=2))
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
