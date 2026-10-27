"""StreamingQueryListener: ghi tiến độ từng micro-batch ra `results/raw/<run_id>/progress.jsonl` và Redis.

Mỗi lần Spark báo `QueryProgressEvent` (sau khi một batch commit), listener ghi một dòng JSON gồm:
- thông lượng (`inputRowsPerSecond`, `processedRowsPerSecond`), thời lượng batch, `durationMs`;
- watermark (`eventTime.watermark`), số dòng trạng thái, số dòng bị loại vì muộn hơn watermark
  (`numRowsDroppedByWatermark`) và số bản trùng bị bỏ (`numDroppedDuplicateRows`);
- **processing lag** = end offset hiện tại của topic (AdminClient) − `endOffset` batch vừa commit, theo
  partition. Không dùng `avgOffsetsBehindLatest`: chỉ số này so với `latestOffset` mà chính batch đã
  chọn nên luôn = 0 khi trigger ASAP (đã quan sát trong spike Spark TWS + Kafka);
- độ trễ hai loại (hệ thống, độ cũ trạng thái): tóm tắt của batch và P95 cửa sổ trượt 1 phút, lấy từ
  `sinks.latency_window(run_id)` (foreachBatch ghi trong cùng driver);
- RSS của driver Python + JVM con (theo dõi rò bộ nhớ khi chạy dài).

Listener chạy trên luồng listener bus của Spark: lỗi ở đây chỉ được ghi log và đếm, không bao giờ ném
ra ngoài (không được làm hỏng query).
"""

from __future__ import annotations

import json
import logging
import os
import threading
import time
from collections.abc import Callable, Mapping
from datetime import datetime
from pathlib import Path
from typing import Any

from pyspark.sql.streaming import StreamingQueryListener

from ai_math_tutor.config import RuntimeConfig, validate_run_id
from ai_math_tutor.streaming.sinks import LatencyWindow, RedisKeys, latency_window

LOGGER = logging.getLogger(__name__)

PROGRESS_FILE = "progress.jsonl"
OFFSETS_TIMEOUT_S = 5.0
# Tên query trạng thái của một run (khớp `job.JobSettings.query_name`). Listener của Spark là toàn
# cục cho mọi query của session (job còn có query dead-letter) nên phải lọc theo tên.
STATE_QUERY_PREFIX = "learner_state_"
_DEFAULT_QUERY = object()
# Mốc 0 của Spark khi chưa có watermark.
_EPOCH_ISO = "1970-01-01T00:00:00.000Z"

EndOffsets = dict[str, dict[int, int]]
LatestOffsetsFn = Callable[[Mapping[str, list[int]]], EndOffsets]


def iso_to_ms(text: str | None) -> int | None:
    """ISO 8601 UTC của Spark (`...Z`) → epoch ms; None khi rỗng."""
    if not text:
        return None
    moment = datetime.fromisoformat(text.replace("Z", "+00:00"))
    return int(moment.timestamp() * 1000)


def parse_end_offsets(sources: Any) -> EndOffsets:
    """`sources[].endOffset` của nguồn Kafka → {topic: {partition: offset}}.

    `endOffset` là chuỗi JSON trong đối tượng Python, là object trong `progress.json`; nguồn khác
    Kafka (không có dạng topic → partition → offset) bị bỏ qua.
    """
    result: EndOffsets = {}
    for source in sources or []:
        raw = source.get("endOffset") if isinstance(source, Mapping) else None
        if isinstance(raw, str):
            try:
                raw = json.loads(raw)
            except json.JSONDecodeError:
                continue
        if not isinstance(raw, Mapping):
            continue
        for topic, partitions in raw.items():
            if not isinstance(partitions, Mapping):
                continue
            for partition, offset in partitions.items():
                try:
                    result.setdefault(str(topic), {})[int(partition)] = int(offset)
                except (TypeError, ValueError):
                    continue
    return result


def compute_lag(end_offsets: Mapping[str, Mapping[int, int]],
                latest: Mapping[str, Mapping[int, int]]) -> dict[str, Any]:
    """Lag theo partition = end offset mới nhất trên broker − offset batch đã đọc tới (không âm)."""
    per_partition: dict[str, int] = {}
    for topic, partitions in sorted(end_offsets.items()):
        for partition, committed in sorted(partitions.items()):
            newest = (latest.get(topic) or {}).get(partition)
            if newest is None:
                raise ValueError(f"thiếu end offset mới nhất cho {topic}[{partition}]")
            per_partition[f"{topic}[{partition}]"] = max(0, int(newest) - int(committed))
    return {
        "total": sum(per_partition.values()),
        "max_partition": max(per_partition.values(), default=0),
        "per_partition": per_partition,
    }


def _float(value: Any) -> float | None:
    try:
        return None if value is None else float(value)
    except (TypeError, ValueError):
        return None


def state_summary(operators: Any) -> dict[str, Any]:
    """Gom `stateOperators`: tổng số dòng trạng thái, dòng bị loại vì watermark, bản trùng bị bỏ."""
    rows: list[dict[str, Any]] = []
    dropped_watermark = dropped_duplicates = 0
    for op in operators or []:
        custom = op.get("customMetrics") or {}
        entry = {
            "operator": op.get("operatorName"),
            "num_rows_total": int(op.get("numRowsTotal") or 0),
            "num_rows_updated": int(op.get("numRowsUpdated") or 0),
            "num_rows_removed": int(op.get("numRowsRemoved") or 0),
            "num_rows_dropped_by_watermark": int(op.get("numRowsDroppedByWatermark") or 0),
            "memory_used_bytes": int(op.get("memoryUsedBytes") or 0),
            "commit_time_ms": int(op.get("commitTimeMs") or 0),
            "all_updates_time_ms": int(op.get("allUpdatesTimeMs") or 0),
        }
        if "numDroppedDuplicateRows" in custom:
            entry["num_dropped_duplicate_rows"] = int(custom["numDroppedDuplicateRows"])
            dropped_duplicates += entry["num_dropped_duplicate_rows"]
        dropped_watermark += entry["num_rows_dropped_by_watermark"]
        rows.append(entry)
    return {"operators": rows, "dropped_by_watermark": dropped_watermark, "dropped_duplicates": dropped_duplicates}


def progress_record(
    run_id: str,
    progress: Mapping[str, Any],
    *,
    latest_offsets: Mapping[str, Mapping[int, int]] | None,
    lag_error: str | None = None,
    batch: Mapping[str, Any] | None = None,
    latency_1m: Mapping[str, Any] | None = None,
    rss_bytes: int | None = None,
) -> dict[str, Any]:
    """Một dòng progress.jsonl từ dict `StreamingQueryProgress.json` (hàm thuần)."""
    event_time = progress.get("eventTime") or {}
    watermark = event_time.get("watermark")
    watermark_ms = iso_to_ms(watermark) if watermark and watermark != _EPOCH_ISO else None
    end_offsets = parse_end_offsets(progress.get("sources"))
    lag: dict[str, Any] | None = None
    if latest_offsets is not None and end_offsets:
        try:
            lag = compute_lag(end_offsets, latest_offsets)
        except ValueError as exc:
            lag_error = str(exc)
    state = state_summary(progress.get("stateOperators"))
    return {
        "event": "progress",
        "run_id": run_id,
        "query_id": progress.get("id"),
        "query_run_id": progress.get("runId"),
        "batch_id": progress.get("batchId"),
        "timestamp": progress.get("timestamp"),
        "timestamp_ms": iso_to_ms(progress.get("timestamp")),
        "num_input_rows": int(progress.get("numInputRows") or 0),
        "input_rows_per_s": _float(progress.get("inputRowsPerSecond")),
        "processed_rows_per_s": _float(progress.get("processedRowsPerSecond")),
        "batch_duration_ms": int(progress.get("batchDuration") or 0),
        "duration_ms": dict(progress.get("durationMs") or {}),
        "event_time": dict(event_time),
        "watermark_ms": watermark_ms,
        "state": state["operators"],
        "dropped_by_watermark": state["dropped_by_watermark"],
        "dropped_duplicates": state["dropped_duplicates"],
        "end_offsets": {t: {str(p): o for p, o in parts.items()} for t, parts in end_offsets.items()},
        "lag": lag,
        "lag_error": lag_error,
        "batch": dict(batch) if batch is not None else None,
        "latency_1m": dict(latency_1m) if latency_1m is not None else None,
        "rss_bytes": rss_bytes,
    }


def metrics_fields(record: Mapping[str, Any]) -> dict[str, str]:
    """Trường của hash `metrics:pipeline` (dashboard/runner đọc); giá trị thiếu ghi chuỗi rỗng."""
    def text(value: Any) -> str:
        return "" if value is None else str(value)

    lag = record.get("lag") or {}
    window = record.get("latency_1m") or {}
    batch = record.get("batch") or {}
    return {
        "run_id": text(record.get("run_id")),
        "batch_id": text(record.get("batch_id")),
        "updated_ms": text(record.get("timestamp_ms")),
        "input_rows_per_s": text(record.get("input_rows_per_s")),
        "processed_rows_per_s": text(record.get("processed_rows_per_s")),
        "batch_duration_ms": text(record.get("batch_duration_ms")),
        "foreach_ms": text(batch.get("foreach_ms")),
        "watermark_ms": text(record.get("watermark_ms")),
        "lag_total": text(lag.get("total")),
        "lag_max_partition": text(lag.get("max_partition")),
        "p95_system_ms_1m": text((window.get("system_ms") or {}).get("p95")),
        "p95_staleness_ms_1m": text((window.get("staleness_ms") or {}).get("p95")),
        "latency_samples_1m": text(window.get("count")),
        "dropped_by_watermark": text(record.get("dropped_by_watermark")),
        "rss_bytes": text(record.get("rss_bytes")),
    }


class KafkaLatestOffsets:
    """End offset mới nhất của các partition qua AdminClient (`list_offsets` với `OffsetSpec.latest()`)."""

    def __init__(self, bootstrap_servers: str, timeout_s: float = OFFSETS_TIMEOUT_S) -> None:
        from confluent_kafka.admin import AdminClient

        self._admin: Any = AdminClient({"bootstrap.servers": bootstrap_servers, "client.id": "nhom8-lag"})
        self._timeout_s = timeout_s

    def close(self) -> None:
        """Giải phóng AdminClient: confluent-kafka không có `close()` cho AdminClient, handle librdkafka
        (kết nối broker, luồng nền) bị hủy khi tham chiếu cuối biến mất."""
        self._admin = None

    def __call__(self, partitions: Mapping[str, list[int]]) -> EndOffsets:
        from confluent_kafka import TopicPartition
        from confluent_kafka.admin import OffsetSpec

        if self._admin is None:
            raise RuntimeError("KafkaLatestOffsets đã đóng")
        request = {TopicPartition(topic, int(p)): OffsetSpec.latest() for topic, ps in partitions.items() for p in ps}
        if not request:
            return {}
        futures = self._admin.list_offsets(request, request_timeout=self._timeout_s)
        result: EndOffsets = {}
        for tp, future in futures.items():
            result.setdefault(tp.topic, {})[tp.partition] = int(future.result(timeout=self._timeout_s).offset)
        return result


def process_rss_bytes() -> int | None:
    """RSS của tiến trình driver + mọi tiến trình con (JVM của Spark do PySpark khởi động)."""
    try:
        import psutil

        process = psutil.Process(os.getpid())
        total = process.memory_info().rss
        for child in process.children(recursive=True):
            try:
                total += child.memory_info().rss
            except psutil.Error:
                continue
        return int(total)
    except Exception:  # psutil thiếu hoặc tiến trình biến mất giữa chừng: chỉ mất một số đo
        return None


def state_query_name(run_id: str) -> str:
    return f"{STATE_QUERY_PREFIX}{run_id}"


class ProgressListener(StreamingQueryListener):
    """Ghi progress.jsonl + `metrics:pipeline` cho query trạng thái của một run.

    `query_name`: chỉ ghi sự kiện của query có tên này (mặc định `learner_state_<run_id>`);
    None ⇒ ghi mọi query của session.
    """

    def __init__(
        self,
        cfg: RuntimeConfig,
        run_id: str,
        *,
        redis_client: Any = None,
        key_prefix: str = "",
        latest_offsets: LatestOffsetsFn | None = None,
        out_path: Path | None = None,
        window: LatencyWindow | None = None,
        rss: Callable[[], int | None] = process_rss_bytes,
        query_name: Any = _DEFAULT_QUERY,
    ) -> None:
        super().__init__()
        self.cfg = cfg
        self.run_id = validate_run_id(run_id)
        self.query_name: str | None = state_query_name(run_id) if query_name is _DEFAULT_QUERY else query_name
        self._query_ids: set[str] = set()
        self.keys = RedisKeys(key_prefix)
        self.out_path = Path(out_path) if out_path is not None else cfg.paths.results / run_id / PROGRESS_FILE
        self.out_path.parent.mkdir(parents=True, exist_ok=True)
        self.window = window or latency_window(run_id)
        self._redis = redis_client
        self._latest = latest_offsets
        # Chỉ đóng client do listener tự tạo; client truyền vào thuộc về người gọi.
        self._owns_redis = redis_client is None
        self._owns_latest = latest_offsets is None
        self._closed = False
        self._rss = rss
        self._lock = threading.Lock()
        self.errors = 0
        self.records = 0
        self.last_record: dict[str, Any] | None = None

    @property
    def redis(self) -> Any:
        if self._redis is None:
            import redis

            self._redis = redis.Redis.from_url(self.cfg.redis.url, socket_timeout=10)
        return self._redis

    def _latest_offsets(self, partitions: Mapping[str, list[int]]) -> EndOffsets:
        if self._latest is None:
            self._latest = KafkaLatestOffsets(self.cfg.kafka.bootstrap_servers)
        return self._latest(partitions)

    def _append(self, record: Mapping[str, Any]) -> None:
        line = json.dumps(record, ensure_ascii=False, sort_keys=True, default=str)
        with self._lock, self.out_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def accepts(self, name: Any) -> bool:
        return self.query_name is None or name == self.query_name

    def close(self) -> None:
        """Đóng client Redis và Kafka admin do listener tạo (gọi sau `spark.streams.removeListener`).

        Sự kiện còn tới muộn trên listener bus sau khi đóng bị bỏ qua, không tạo lại kết nối.
        """
        self._closed = True
        if self._owns_redis and self._redis is not None:
            try:
                self._redis.close()
            except Exception:  # đóng không được không làm hỏng số đo đã ghi
                LOGGER.debug("không đóng được client Redis của listener", exc_info=True)
            self._redis = None
        if self._owns_latest and self._latest is not None:
            close = getattr(self._latest, "close", None)
            if close is not None:
                close()
            self._latest = None

    def handle_progress(self, progress: Mapping[str, Any]) -> dict[str, Any] | None:
        """Xử lý một dict progress (tách khỏi sự kiện Spark để test được); None nếu là query khác
        hoặc listener đã đóng."""
        if self._closed or not self.accepts(progress.get("name")):
            return None
        end_offsets = parse_end_offsets(progress.get("sources"))
        latest: EndOffsets | None = None
        lag_error: str | None = None
        if end_offsets:
            try:
                latest = self._latest_offsets({t: sorted(ps) for t, ps in end_offsets.items()})
            except Exception as exc:  # broker tạm mất: ghi lỗi, batch sau đo lại
                lag_error = f"{type(exc).__name__}: {exc}"
        batch_id = progress.get("batchId")
        record = progress_record(
            self.run_id, progress,
            latest_offsets=latest, lag_error=lag_error,
            batch=self.window.pop_batch(int(batch_id)) if batch_id is not None else None,
            latency_1m=self.window.snapshot(),
            rss_bytes=self._rss(),
        )
        self._append(record)
        self.records += 1
        self.last_record = record
        try:
            self.redis.hset(self.keys.metrics_pipeline, mapping=metrics_fields(record))
        except Exception:  # Redis lỗi không được làm hỏng query; dòng jsonl vẫn còn
            self.errors += 1
            LOGGER.warning("không ghi được %s", self.keys.metrics_pipeline, exc_info=True)
        return record

    def onQueryStarted(self, event: Any) -> None:
        try:
            if not self.accepts(event.name):
                return
            self._query_ids.add(str(event.id))
            self._append({"event": "started", "run_id": self.run_id, "query_id": str(event.id),
                          "query_run_id": str(event.runId), "name": event.name, "timestamp": event.timestamp,
                          "logged_ms": time.time_ns() // 1_000_000})
        except Exception:
            self.errors += 1
            LOGGER.exception("listener: lỗi khi ghi onQueryStarted")

    def onQueryProgress(self, event: Any) -> None:
        try:
            self.handle_progress(json.loads(event.progress.json))
        except Exception:
            self.errors += 1
            LOGGER.exception("listener: lỗi khi ghi onQueryProgress")

    def onQueryIdle(self, event: Any) -> None:
        # Không có batch mới ⇒ không có số đo mới; dòng progress kế tiếp sẽ có lag cập nhật.
        return None

    def onQueryTerminated(self, event: Any) -> None:
        try:
            if self.query_name is not None and str(event.id) not in self._query_ids:
                return
            self._append({"event": "terminated", "run_id": self.run_id, "query_id": str(event.id),
                          "query_run_id": str(event.runId), "exception": event.exception,
                          "logged_ms": time.time_ns() // 1_000_000})
        except Exception:
            self.errors += 1
            LOGGER.exception("listener: lỗi khi ghi onQueryTerminated")


def attach_listener(spark: Any, cfg: RuntimeConfig, run_id: str, **kwargs: Any) -> ProgressListener:
    """Tạo listener của run và đăng ký vào `spark.streams`; trả listener (gỡ bằng `removeListener` rồi
    `close()` — `job.detach_listener` làm cả hai)."""
    listener = ProgressListener(cfg, run_id, **kwargs)
    spark.streams.addListener(listener)
    return listener
