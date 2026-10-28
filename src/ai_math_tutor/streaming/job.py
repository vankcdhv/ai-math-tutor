"""Job streaming trạng thái người học: Kafka → kiểm hợp đồng → watermark/khử trùng → trạng thái → foreachBatch.

Hai query trên cùng topic vào:
1. **Trạng thái** (query chính, checkpoint `checkpoints/<run_id>`): sự kiện hợp lệ →
   `withWatermark(event_time, W)` (heartbeat vẫn có mặt nên đẩy được watermark) →
   `dropDuplicatesWithinWatermark(event_id)` (chế độ eager/ordered, `--dedup on`) → lọc heartbeat →
   `groupBy(learner_id).transformWithStateInPandas(LearnerStateProcessor)` → `foreachBatch` của sinks.
   Chế độ `none`: không watermark, `timeMode="None"`, khử trùng trong state.
2. **Dead-letter** (checkpoint `checkpoints/<run_id>/_dead_letter`): bản ghi vi phạm hợp đồng →
   topic dead-letter, giá trị là JSON `{reason, source_topic, partition, offset, kafka_timestamp, raw}`.

Bản ghi lỗi bị tách **trước** watermark: một bản ghi rác có `event_time` ở tương lai xa sẽ đẩy
watermark vọt lên và làm mọi sự kiện thật thành "muộn". Cùng lý do, sự kiện có `event_time` đi trước
**timestamp Kafka của chính bản ghi đó** quá `--max-future-s` (mặc định 120 s) cũng vào dead-letter;
heartbeat được phép đi trước tới W + `--heartbeat-allowance-s` (mặc định W + 60 s, tối đa W + 1 h).
Mốc so sánh là timestamp Kafka (không phải giờ xử lý) nên hai query cho cùng kết luận với mọi bản ghi:
mỗi bản ghi vào đúng một nơi (trạng thái hoặc dead-letter).

Checkpoint ghi lại tham số ngữ nghĩa của run (`job-settings.json`) ở lần chạy đầu; chạy lại cùng
checkpoint với tham số khác bị từ chối (xem `ensure_checkpoint_settings`).

CLI (chạy trong WSL, sau `make up` và `make topics`):
  python -m ai_math_tutor.streaming.job --run-id R [--mode eager|ordered|none] [--watermark 30s]
      [--dedup on|off] [--trigger asap|"N seconds"] [--learner-model rule|bkt]
      [--starting-offsets earliest|latest|JSON] [--max-offsets-per-trigger N] [--run-seconds S]
      [--max-future-s 120] [--heartbeat-allowance-s 60]
Dừng bằng Ctrl-C/SIGTERM: job chờ hết micro-batch đang chạy rồi mới dừng query (cố gắng tốt nhất,
xem `stop_at_batch_boundary`; dừng giữa batch vẫn an toàn vì mọi sink idempotent).
"""

from __future__ import annotations

import argparse
import json
import os
import re
import signal
import sys
import threading
import time
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ai_math_tutor.config import ConfigError, RuntimeConfig, load_config, validate_run_id
from ai_math_tutor.streaming import schema as S
from ai_math_tutor.streaming.processor import (
    DEFAULT_INACTIVITY_MS,
    DEFAULT_STATE_TTL_MS,
    MODELS,
    MODES,
    LearnerStateProcessor,
)

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession
    from pyspark.sql.streaming import StreamingQuery

ForeachBatch = Callable[["DataFrame", int], None]

DEFAULT_WATERMARK = "30 seconds"
DEFAULT_WATERMARK_MS = 30_000
# Sự kiện học tập: event_time được đi trước timestamp Kafka của bản ghi tối đa chừng này.
DEFAULT_MAX_FUTURE_S = 120
# Heartbeat của bộ phát lại đi trước đúng W + 1 s (event_time = cuối log + W + 1 s + k, gửi lúc ≈ cuối
# log + k); phần dư cho phép trên W mặc định 60 s, không quá 1 h để một heartbeat giả không đẩy được
# watermark đi xa.
DEFAULT_HEARTBEAT_ALLOWANCE_S = 60
MAX_HEARTBEAT_ALLOWANCE_S = 3600
CHECKPOINT_SETTINGS_FILE = "job-settings.json"
DEFAULT_STOP_TIMEOUT_S = 120.0
DEAD_LETTER_SUBDIR = "_dead_letter"
_KEY_PREFIX_RE = re.compile(r"^[A-Za-z0-9._:-]{1,64}$")
_INTERVAL_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes)?\s*$",
                          re.IGNORECASE)


class JobError(RuntimeError):
    """Tham số job sai hoặc môi trường chưa sẵn sàng (topic chưa tạo, sinks thiếu)."""


def parse_interval_ms(text: str) -> int:
    """"30", "30s", "30 seconds", "2 minutes" → mili giây (> 0)."""
    match = _INTERVAL_RE.match(str(text))
    if not match:
        raise JobError(f"khoảng thời gian không hợp lệ: {text!r} (ví dụ 30s, \"2 minutes\")")
    value = float(match.group(1))
    unit = (match.group(2) or "s").lower()
    ms = round(value * (60_000 if unit.startswith("m") else 1000))
    if ms <= 0:
        raise JobError(f"khoảng thời gian phải > 0: {text!r}")
    return ms


def spark_interval(ms: int) -> str:
    """Mili giây → chuỗi khoảng của Spark (`withWatermark`, `processingTime`)."""
    return f"{ms} milliseconds" if ms % 1000 else f"{ms // 1000} seconds"


@dataclass(frozen=True)
class JobSettings:
    run_id: str
    mode: str = "eager"
    watermark_ms: int = DEFAULT_WATERMARK_MS
    dedup: bool = True
    trigger_ms: int | None = None  # None = ASAP (micro-batch kế tiếp ngay khi batch trước xong)
    learner_model: str = "rule"
    starting_offsets: str = "earliest"
    max_offsets_per_trigger: int | None = None
    input_topic: str | None = None
    dead_letter_topic: str | None = None
    alerts_topic: str | None = None
    inactivity_ms: int | None = DEFAULT_INACTIVITY_MS
    state_ttl_ms: int = DEFAULT_STATE_TTL_MS
    max_future_s: int = DEFAULT_MAX_FUTURE_S
    heartbeat_allowance_s: int = DEFAULT_HEARTBEAT_ALLOWANCE_S
    shuffle_partitions: int | None = None
    run_seconds: float | None = None
    redis_key_prefix: str = ""  # rỗng khi chạy thật (runner FLUSHDB đầu run); tách khóa khi dùng chung Redis

    def __post_init__(self) -> None:
        try:
            validate_run_id(self.run_id)
        except ConfigError as exc:
            raise JobError(str(exc)) from exc
        if self.mode not in MODES:
            raise JobError(f"--mode phải thuộc {MODES}")
        if self.learner_model not in MODELS:
            raise JobError(f"--learner-model phải thuộc {MODELS}")
        if self.watermark_ms <= 0:
            raise JobError("--watermark phải > 0")
        if self.trigger_ms is not None and self.trigger_ms <= 0:
            raise JobError("--trigger phải > 0")
        if self.max_offsets_per_trigger is not None and self.max_offsets_per_trigger < 1:
            raise JobError("--max-offsets-per-trigger phải ≥ 1")
        if self.inactivity_ms is not None and self.inactivity_ms < 0:
            raise JobError("--inactivity-s phải ≥ 0")
        if self.state_ttl_ms <= 0 or self.max_future_s <= 0:
            raise JobError("--state-ttl-h và --max-future-s phải > 0")
        if not 0 <= self.heartbeat_allowance_s <= MAX_HEARTBEAT_ALLOWANCE_S:
            raise JobError(f"--heartbeat-allowance-s phải trong [0, {MAX_HEARTBEAT_ALLOWANCE_S}]")
        if self.shuffle_partitions is not None and self.shuffle_partitions < 1:
            raise JobError("--shuffle-partitions phải ≥ 1")
        if self.run_seconds is not None and self.run_seconds <= 0:
            raise JobError("--run-seconds phải > 0")
        if self.redis_key_prefix and not _KEY_PREFIX_RE.match(self.redis_key_prefix):
            raise JobError("--redis-key-prefix chỉ gồm [A-Za-z0-9._:-], tối đa 64 ký tự")
        offsets = self.starting_offsets.strip()
        if offsets not in ("earliest", "latest"):
            try:
                json.loads(offsets)
            except json.JSONDecodeError as exc:
                raise JobError("--starting-offsets phải là earliest, latest hoặc JSON offset theo partition") from exc

    @property
    def timed(self) -> bool:
        return self.mode != "none"

    @property
    def query_name(self) -> str:
        return f"learner_state_{self.run_id}"

    @property
    def heartbeat_max_future_ms(self) -> int:
        """Heartbeat được đi trước timestamp Kafka tối đa W + phần dư."""
        return self.watermark_ms + self.heartbeat_allowance_s * 1000


def job_config(cfg: RuntimeConfig, settings: JobSettings) -> RuntimeConfig:
    """Cấu hình của run: thay tên topic theo tham số (topic riêng mỗi run/test) để sinks dùng đúng topic."""
    topics = dict(cfg.kafka.topics)
    for key, value in (("learner_events", settings.input_topic), ("dead_letter", settings.dead_letter_topic),
                       ("alerts", settings.alerts_topic)):
        if value:
            topics[key] = value
    return replace(cfg, kafka=replace(cfg.kafka, topics=topics))


def spark_conf(cfg: RuntimeConfig, settings: JobSettings) -> dict[str, str]:
    """Spark conf của job: conf chung (RocksDB, Kafka connector) + shuffle partitions của run + thư mục tạm."""
    conf = cfg.spark.conf()
    if settings.shuffle_partitions is not None:
        conf["spark.sql.shuffle.partitions"] = str(settings.shuffle_partitions)
    local_dir = cfg.paths.data_root / "spark-tmp" / "streaming"
    conf.update({
        "spark.app.name": f"nhom8-learner-state-{settings.run_id}",
        "spark.local.dir": str(local_dir),
        "spark.driver.extraJavaOptions": f"-Djava.io.tmpdir={local_dir}",
        "spark.sql.session.timeZone": "UTC",
    })
    return conf


def build_spark(conf: dict[str, str]) -> SparkSession:
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    Path(conf["spark.local.dir"]).mkdir(parents=True, exist_ok=True)
    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


# ---------------------------------------------------------------- parse + kiểm hợp đồng

def _ok(condition: Column) -> Column:
    """Điều kiện hợp lệ an toàn với null: null (không xác định) coi là không hợp lệ."""
    from pyspark.sql import functions as F

    return F.coalesce(condition, F.lit(False))


def _in(column: Column, values: Sequence[str]) -> Column:
    return column.isin(*values)


def parse_events(raw: DataFrame, max_future_s: int = DEFAULT_MAX_FUTURE_S,
                 heartbeat_max_future_ms: int | None = None) -> DataFrame:
    """Bản ghi Kafka (`key`, `value`, `topic`, `partition`, `offset`, `timestamp`) → sự kiện đã parse + `invalid_reason`.

    `invalid_reason` null ⇔ bản ghi qua mọi kiểm tra (heartbeat hợp lệ cũng vậy). Kiểm theo thứ tự,
    lấy lý do đầu tiên: malformed_json, unknown_field, missing_field, schema_version, event_type,
    event_id, learner_id, event_time, sent_time, heartbeat_contract, learning_fields, payload,
    future_event_time. Những gì JSON Schema kiểm mà ở đây không kiểm (trường thừa trong payload,
    `uniqueItems` theo nghĩa đầy đủ, định dạng uuid ngoài regex) do `event_validation` kiểm ngoại tuyến.

    future_event_time: `event_time` − timestamp Kafka của bản ghi > `max_future_s` (sự kiện học tập)
    hoặc > `heartbeat_max_future_ms` (heartbeat; None ⇒ W mặc định + phần dư mặc định). Chỉ phụ thuộc
    nội dung bản ghi nên query trạng thái và query dead-letter luôn cho cùng kết quả.
    """
    from pyspark.sql import functions as F

    value = F.col("value").cast("string")
    frame = raw.select(
        F.col("key").cast("string").alias("kafka_key"),
        value.alias("raw_value"),
        F.col("topic").alias("kafka_topic"),
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(value, S.EVENT_SCHEMA).alias("e"),
        F.json_object_keys(value).alias("json_keys"),
    ).select("kafka_key", "raw_value", "kafka_topic", "kafka_partition", "kafka_offset", "kafka_timestamp",
             "json_keys", "e.*")
    fmt = F.lit(S.SPARK_DATETIME_FORMAT)
    frame = (frame
             .withColumn("event_ts", F.try_to_timestamp(F.col("event_time"), fmt))
             .withColumn("sent_ts", F.try_to_timestamp(F.col("sent_time"), fmt)))

    c, p = F.col, F.col("payload")
    keys = c("json_keys")
    is_heartbeat = c("event_type") == F.lit(S.HEARTBEAT_EVENT_TYPE)
    allowed = F.array(*[F.lit(name) for name in S.EVENT_FIELD_NAMES])
    heartbeat_allowed = F.array(*[F.lit(name) for name in S.HEARTBEAT_FIELDS])
    payload_keys = F.json_object_keys(F.get_json_object(c("raw_value"), "$.payload"))
    skills = c("skill_ids")
    learning_ok = (
        (c("learner_id") != F.lit(S.HEARTBEAT_LEARNER_ID))
        & F.forall(F.array(*[c(name).isNotNull() for name in S.LEARNING_REQUIRED_FIELDS]), lambda x: x)
        & c("class_id").rlike(S.CLASS_ID_PATTERN)
        & c("session_id").rlike(S.SESSION_ID_PATTERN)
        & (c("session_seq") >= 1)
        & c("problem_id").rlike(S.PROBLEM_ID_PATTERN)
        & (F.size(skills) >= 1)
        & (F.size(F.array_distinct(skills)) == F.size(skills))
        & F.forall(skills, lambda s: s.isNotNull() & s.rlike(S.SKILL_ID_PATTERN))
        & _in(c("client.platform"), S.PLATFORMS)
        & c("client.app_version").rlike(S.APP_VERSION_PATTERN)
        & _in(c("client.connection"), S.CONNECTIONS)
    )
    heartbeat_ok = (
        (c("schema_version") == F.lit("0.2"))
        & (c("learner_id") == F.lit(S.HEARTBEAT_LEARNER_ID))
        & (F.size(F.array_except(keys, heartbeat_allowed)) == 0)
        & (F.size(payload_keys) == 0)
    )
    et = c("event_type")
    payload_ok = (
        F.when(et == "submit_step", (p.step_index >= 1) & (F.length(p.expression).between(1, 500))
               & p.is_correct.isNotNull())
        .when(et == "wrong_answer", F.length(p.answer).between(1, 200) & (p.attempt >= 1))
        .when(et == "request_hint", p.hint_level.between(1, 3) & (p.step_index >= 1))
        .when(et == "complete", F.length(p.answer).between(1, 200) & (p.attempts >= 1) & (p.hints_used >= 0))
        .when(et == "open_problem", F.size(payload_keys) == 0)
        .otherwise(F.lit(True))
    )
    if heartbeat_max_future_ms is None:
        heartbeat_max_future_ms = DEFAULT_WATERMARK_MS + DEFAULT_HEARTBEAT_ALLOWANCE_S * 1000
    lead_ms = F.unix_millis(c("event_ts")) - F.unix_millis(c("kafka_timestamp"))
    future_limit_ms = F.when(is_heartbeat, F.lit(int(heartbeat_max_future_ms))).otherwise(
        F.lit(int(max_future_s) * 1000))
    checks: list[tuple[str, Column]] = [
        ("malformed_json", keys.isNotNull()),
        ("unknown_field", F.size(F.array_except(keys, allowed)) == 0),
        ("missing_field", F.forall(F.array(*[c(name).isNotNull() for name in S.REQUIRED_FIELDS]), lambda x: x)),
        ("schema_version", _in(c("schema_version"), S.SCHEMA_VERSIONS)),
        ("event_type", _in(et, S.EVENT_TYPES)),
        ("event_id", c("event_id").rlike(S.EVENT_ID_PATTERN)),
        ("learner_id", c("learner_id").rlike(S.LEARNER_ID_PATTERN)),
        ("event_time", c("event_time").rlike(S.DATETIME_PATTERN) & c("event_ts").isNotNull()),
        ("sent_time", c("sent_time").rlike(S.DATETIME_PATTERN) & c("sent_ts").isNotNull()),
        ("heartbeat_contract", F.when(is_heartbeat, heartbeat_ok).otherwise(F.lit(True))),
        ("learning_fields", F.when(is_heartbeat, F.lit(True)).otherwise(learning_ok)),
        ("payload", payload_ok),
        ("future_event_time", lead_ms <= future_limit_ms),
    ]
    reason = F.coalesce(*[F.when(~_ok(condition), F.lit(name)) for name, condition in checks])
    return (frame
            .withColumn("invalid_reason", reason)
            .withColumn("event_time_ms", F.unix_millis(c("event_ts")))
            .withColumn("sent_time_ms", F.unix_millis(c("sent_ts"))))


def dead_letter_records(parsed: DataFrame) -> DataFrame:
    """Bản ghi vi phạm hợp đồng → (key, value) cho topic dead-letter; giữ nguyên chuỗi gốc để tra cứu."""
    from pyspark.sql import functions as F

    return parsed.filter(F.col("invalid_reason").isNotNull()).select(
        F.col("kafka_key").alias("key"),
        F.to_json(F.struct(
            F.col("invalid_reason").alias("reason"),
            F.col("kafka_topic").alias("source_topic"),
            F.col("kafka_partition").alias("partition"),
            F.col("kafka_offset").alias("offset"),
            F.unix_millis(F.col("kafka_timestamp")).alias("kafka_timestamp_ms"),
            F.col("raw_value").alias("raw"),
        )).alias("value"),
    )


def processor_input(parsed: DataFrame, settings: JobSettings) -> DataFrame:
    """Sự kiện hợp lệ → (watermark, khử trùng) → bỏ heartbeat → cột đầu vào processor."""
    from pyspark.sql import functions as F

    valid = parsed.filter(F.col("invalid_reason").isNull())
    if settings.timed:
        valid = valid.withWatermark("event_ts", spark_interval(settings.watermark_ms))
        if settings.dedup:
            valid = valid.dropDuplicatesWithinWatermark(["event_id"])
    # Vế `event_ts IS NULL` (luôn sai với bản ghi hợp lệ) giữ bộ lọc ở trên EventTimeWatermark: một điều kiện
    # không nhắc tới cột event-time được optimizer đẩy xuống dưới watermark, heartbeat bị lọc trước khi đẩy
    # watermark và job không bao giờ drain khi tắt khử trùng (không còn toán tử nào chặn việc đẩy xuống).
    learning = valid.filter((F.col("event_type") != F.lit(S.HEARTBEAT_EVENT_TYPE)) | F.col("event_ts").isNull())
    columns = {
        "is_correct": F.col("payload.is_correct"),
        "step_index": F.col("payload.step_index"),
    }
    return learning.select(*[columns.get(name, F.col(name)).cast(kind).alias(name)
                             for name, kind in S.PROCESSOR_INPUT_FIELDS])


def learner_state(parsed: DataFrame, settings: JobSettings) -> DataFrame:
    """Đầu ra trạng thái (OUTPUT_SCHEMA) của `transformWithStateInPandas` theo learner_id."""
    processor = LearnerStateProcessor(settings.mode, settings.learner_model, dedup=settings.dedup,
                                      inactivity_ms=settings.inactivity_ms or None,
                                      state_ttl_ms=settings.state_ttl_ms)
    return (processor_input(parsed, settings)
            .groupBy("learner_id")
            .transformWithStateInPandas(
                statefulProcessor=processor,
                outputStructType=S.OUTPUT_SCHEMA,
                outputMode="Update",
                timeMode=processor.time_mode,
            ))


def read_kafka(spark: SparkSession, cfg: RuntimeConfig, settings: JobSettings) -> DataFrame:
    reader = (spark.readStream.format("kafka")
              .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
              .option("subscribe", cfg.kafka.topics["learner_events"])
              .option("startingOffsets", settings.starting_offsets.strip()))
    if settings.max_offsets_per_trigger is not None:
        reader = reader.option("maxOffsetsPerTrigger", str(settings.max_offsets_per_trigger))
    return reader.load()


@dataclass
class JobStreams:
    state: StreamingQuery
    dead_letter: StreamingQuery

    @property
    def queries(self) -> tuple[StreamingQuery, StreamingQuery]:
        return (self.state, self.dead_letter)


def parsed_events(spark: SparkSession, cfg: RuntimeConfig, settings: JobSettings) -> DataFrame:
    """Kafka → `parse_events` với ngưỡng tương lai của run (cùng biểu thức cho cả hai query)."""
    return parse_events(read_kafka(spark, cfg, settings), settings.max_future_s, settings.heartbeat_max_future_ms)


# Tham số đổi ngữ nghĩa của state/đầu ra: chạy lại checkpoint với giá trị khác sẽ trộn hai ngữ nghĩa
# (Spark còn giữ shuffle partitions của checkpoint và bỏ qua giá trị mới mà không báo).
RESUME_FIELDS = ("mode", "learner_model", "dedup", "watermark_ms", "inactivity_ms", "state_ttl_ms",
                 "shuffle_partitions")


def checkpoint_settings(settings: JobSettings, shuffle_partitions: int) -> dict[str, Any]:
    values = {name: getattr(settings, name) for name in RESUME_FIELDS if name != "shuffle_partitions"}
    values["shuffle_partitions"] = int(shuffle_partitions)
    return values


def ensure_checkpoint_settings(checkpoint: Path, settings: JobSettings, shuffle_partitions: int) -> dict[str, Any]:
    """Ghi `job-settings.json` trong checkpoint ở lần chạy đầu; lần sau so và từ chối nếu khác.

    Checkpoint cũ đã có offset nhưng chưa có tệp (tạo trước khi có kiểm tra này): ghi tham số hiện
    tại kèm `adopted_on_resume=true` vì không còn biết tham số gốc. Trả nội dung tệp.
    """
    path = Path(checkpoint) / CHECKPOINT_SETTINGS_FILE
    current = checkpoint_settings(settings, shuffle_partitions)
    if path.is_file():
        try:
            stored = json.loads(path.read_text(encoding="utf-8"))
            original = stored["settings"]
        except (OSError, ValueError, KeyError, TypeError) as exc:
            raise JobError(f"không đọc được {path}: {exc}") from exc
        diffs = {name: (original.get(name), current[name]) for name in RESUME_FIELDS
                 if original.get(name) != current[name]}
        if diffs:
            detail = ", ".join(f"{name}: checkpoint={old!r} ≠ hiện tại={new!r}" for name, (old, new) in diffs.items())
            raise JobError(f"không chạy tiếp checkpoint {checkpoint} với tham số khác ({detail}); "
                           "dùng --run-id mới hoặc đúng tham số cũ")
        return stored
    Path(checkpoint).mkdir(parents=True, exist_ok=True)
    record = {
        "run_id": settings.run_id,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "adopted_on_resume": (Path(checkpoint) / "offsets").is_dir(),
        "settings": current,
    }
    tmp = path.with_name(f".{path.name}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    os.replace(tmp, path)
    return record


def start_streams(spark: SparkSession, cfg: RuntimeConfig, settings: JobSettings,
                  foreach_batch: ForeachBatch) -> JobStreams:
    """Khởi động query trạng thái (foreachBatch đã cho) và query dead-letter; `cfg` đã qua `job_config`.

    Từ chối (JobError) chạy tiếp checkpoint có tham số ngữ nghĩa khác (`ensure_checkpoint_settings`).
    """
    checkpoint = cfg.paths.checkpoint_dir(settings.run_id)
    checkpoint.mkdir(parents=True, exist_ok=True)
    ensure_checkpoint_settings(checkpoint, settings, int(spark.conf.get("spark.sql.shuffle.partitions")))
    state_writer = (learner_state(parsed_events(spark, cfg, settings), settings)
                    .writeStream.queryName(settings.query_name)
                    .outputMode("update")
                    .option("checkpointLocation", str(checkpoint))
                    .foreachBatch(foreach_batch))
    dead_writer = (dead_letter_records(parsed_events(spark, cfg, settings))
                   .writeStream.queryName(f"dead_letter_{settings.run_id}")
                   .format("kafka")
                   .option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
                   .option("topic", cfg.kafka.topics["dead_letter"])
                   .option("checkpointLocation", str(checkpoint / DEAD_LETTER_SUBDIR)))
    if settings.trigger_ms is not None:
        interval = spark_interval(settings.trigger_ms)
        state_writer = state_writer.trigger(processingTime=interval)
        dead_writer = dead_writer.trigger(processingTime=interval)
    state_query = state_writer.start()
    try:
        dead_query = dead_writer.start()
    except Exception:
        stop_at_batch_boundary(state_query)
        raise
    return JobStreams(state_query, dead_query)


def stop_at_batch_boundary(query: StreamingQuery, timeout_s: float = DEFAULT_STOP_TIMEOUT_S) -> bool:
    """Chờ không còn micro-batch đang chạy rồi mới `stop()` (dừng giữa batch làm hủy job giữa chừng).

    Cố gắng tốt nhất, không bảo đảm: giữa lúc thấy `isTriggerActive == False` và lúc `stop()`, trigger
    ASAP có thể đã bắt đầu batch kế tiếp, và `stop()` hủy batch đó giữa chừng. An toàn vì mọi sink
    idempotent: batch chưa commit được chạy lại nguyên vẹn ở lần khởi động sau.
    Trả False nếu hết hạn chờ mà batch vẫn chạy (vẫn dừng, chấp nhận hủy giữa batch).
    """
    deadline = time.monotonic() + timeout_s
    clean = True
    while query.isActive and query.status.get("isTriggerActive"):
        if time.monotonic() >= deadline:
            clean = False
            break
        time.sleep(0.05)
    query.stop()
    return clean


def stop_streams(streams: JobStreams, timeout_s: float = DEFAULT_STOP_TIMEOUT_S) -> dict[str, bool]:
    return {query.name: stop_at_batch_boundary(query, timeout_s) for query in streams.queries if query.isActive}


def check_topics(cfg: RuntimeConfig, settings: JobSettings) -> dict[str, int]:
    """Topic vào và dead-letter phải có sẵn (broker tắt tự tạo topic); trả tên → số partition."""
    from ai_math_tutor.ingestion.topics import TopicError, partition_count

    names = [cfg.kafka.topics["learner_events"], cfg.kafka.topics["dead_letter"]]
    try:
        return {name: partition_count(cfg.kafka.bootstrap_servers, name) for name in names}
    except TopicError as exc:
        raise JobError(str(exc)) from exc


def job_record_path(out: Path) -> Path:
    """`job.json` cho lần chạy đầu; mỗi lần chạy sau (restart) một tệp mới `job-<n>.json`, n = 1, 2, …"""
    path = out / "job.json"
    n = 0
    while path.exists():
        n += 1
        path = out / f"job-{n}.json"
    return path


def write_job_record(cfg: RuntimeConfig, settings: JobSettings, conf: dict[str, str], *, topics: dict[str, int],
                     resumed: bool, extra: dict[str, Any] | None = None) -> Path:
    """`results/raw/<run_id>/job.json` (lần đầu) hoặc `job-<n>.json` (mỗi lần khởi động lại): tham số,
    Spark conf, số partition thật (đưa vào run record). Không ghi đè bản ghi của lần chạy trước."""
    out = cfg.paths.results / settings.run_id
    out.mkdir(parents=True, exist_ok=True)
    path = job_record_path(out)
    record = {
        "run_id": settings.run_id,
        "written_at": datetime.now(timezone.utc).isoformat(),
        "settings": asdict(settings),
        "topics": topics,
        "kafka_partitions": topics.get(cfg.kafka.topics["learner_events"]),
        "shuffle_partitions": int(conf["spark.sql.shuffle.partitions"]),
        "checkpoint": str(cfg.paths.checkpoint_dir(settings.run_id)),
        "resumed_from_checkpoint": resumed,
        "spark_conf": conf,
        **(extra or {}),
    }
    path.write_text(json.dumps(record, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


# ---------------------------------------------------------------- CLI

def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Job streaming trạng thái người học (Kafka → Spark → sinks)")
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--mode", choices=MODES, default="eager")
    parser.add_argument("--watermark", default=DEFAULT_WATERMARK, help='ví dụ 30s, "2 minutes" (mặc định 30 s)')
    parser.add_argument("--dedup", choices=("on", "off"), default="on")
    parser.add_argument("--trigger", default="asap", help='asap hoặc "N seconds"')
    parser.add_argument("--learner-model", choices=MODELS, default="rule")
    parser.add_argument("--starting-offsets", default="earliest")
    parser.add_argument("--max-offsets-per-trigger", type=int, default=None)
    parser.add_argument("--input-topic", default=None, help="mặc định theo configs/runtime.yaml")
    parser.add_argument("--dead-letter-topic", default=None)
    parser.add_argument("--alerts-topic", default=None)
    parser.add_argument("--inactivity-s", type=float, default=DEFAULT_INACTIVITY_MS / 1000,
                        help="cảnh báo không hoạt động sau S giây event time (0 = tắt)")
    parser.add_argument("--state-ttl-h", type=float, default=DEFAULT_STATE_TTL_MS / 3_600_000)
    parser.add_argument("--max-future-s", type=int, default=DEFAULT_MAX_FUTURE_S,
                        help="sự kiện học tập có event_time đi trước timestamp Kafka quá S giây ⇒ dead-letter")
    parser.add_argument("--heartbeat-allowance-s", type=int, default=DEFAULT_HEARTBEAT_ALLOWANCE_S,
                        help=f"heartbeat được đi trước timestamp Kafka tới W + S giây (0–{MAX_HEARTBEAT_ALLOWANCE_S})")
    parser.add_argument("--shuffle-partitions", type=int, default=None,
                        help="mặc định theo config (= số Kafka partition)")
    parser.add_argument("--run-seconds", type=float, default=None, help="tự dừng sau S giây (mặc định chạy tới khi bị dừng)")
    parser.add_argument("--redis-key-prefix", default="", help="tiền tố mọi khóa Redis (mặc định rỗng)")
    return parser


def settings_from_args(args: argparse.Namespace) -> JobSettings:
    trigger = str(args.trigger).strip()
    return JobSettings(
        run_id=args.run_id,
        mode=args.mode,
        watermark_ms=parse_interval_ms(args.watermark),
        dedup=args.dedup == "on",
        trigger_ms=None if trigger.lower() == "asap" else parse_interval_ms(trigger),
        learner_model=args.learner_model,
        starting_offsets=args.starting_offsets,
        max_offsets_per_trigger=args.max_offsets_per_trigger,
        input_topic=args.input_topic,
        dead_letter_topic=args.dead_letter_topic,
        alerts_topic=args.alerts_topic,
        inactivity_ms=round(args.inactivity_s * 1000) if args.inactivity_s else None,
        state_ttl_ms=round(args.state_ttl_h * 3_600_000),
        max_future_s=args.max_future_s,
        heartbeat_allowance_s=args.heartbeat_allowance_s,
        shuffle_partitions=args.shuffle_partitions,
        run_seconds=args.run_seconds,
        redis_key_prefix=args.redis_key_prefix,
    )


def _log(message: str) -> None:
    print(f"JOB {datetime.now(timezone.utc):%H:%M:%S} {message}", file=sys.stderr, flush=True)


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        settings = settings_from_args(args)
        cfg = job_config(load_config(), settings)
        topics = check_topics(cfg, settings)
    except (JobError, ConfigError) as exc:
        parser.error(str(exc))
    kafka_partitions = topics[cfg.kafka.topics["learner_events"]]
    conf = spark_conf(cfg, settings)
    if int(conf["spark.sql.shuffle.partitions"]) != kafka_partitions:
        _log(f"CẢNH BÁO: shuffle partitions {conf['spark.sql.shuffle.partitions']} ≠ {kafka_partitions} "
             "Kafka partition (đặt NHOM8_KAFKA_PARTITIONS hoặc --shuffle-partitions)")

    # Sinks và listener là phần riêng của pipeline; thiếu thì job không chạy (không có sink giả).
    from ai_math_tutor.streaming.listener import attach_listener
    from ai_math_tutor.streaming.sinks import make_foreach_batch

    resumed = (cfg.paths.checkpoint_dir(settings.run_id) / "offsets").is_dir()
    stop_requested = threading.Event()

    def on_signal(signum: int, _frame: Any) -> None:
        _log(f"nhận tín hiệu {signum}: dừng ở ranh giới batch")
        stop_requested.set()

    for sig in (signal.SIGINT, signal.SIGTERM):
        signal.signal(sig, on_signal)

    spark = build_spark(conf)
    streams: JobStreams | None = None
    foreach_batch: Any = None
    listener: Any = None
    try:
        # Kiểm trước khi ghi bản ghi job: checkpoint có tham số khác ⇒ không ghi gì, thoát với lỗi.
        ensure_checkpoint_settings(cfg.paths.checkpoint_dir(settings.run_id), settings,
                                   int(conf["spark.sql.shuffle.partitions"]))
        listener = attach_listener(spark, cfg, settings.run_id, key_prefix=settings.redis_key_prefix)
        foreach_batch = make_foreach_batch(cfg, settings.run_id, mode=settings.mode, model=settings.learner_model,
                                           key_prefix=settings.redis_key_prefix)
        write_job_record(cfg, settings, conf, topics=topics, resumed=resumed,
                         extra={"spark_jars": spark.sparkContext.getConf().get("spark.jars", "")})
        streams = start_streams(spark, cfg, settings, foreach_batch)
        _log(f"run {settings.run_id}: mode={settings.mode} watermark={settings.watermark_ms} ms "
             f"dedup={settings.dedup} model={settings.learner_model} resumed={resumed}")
        deadline = None if settings.run_seconds is None else time.monotonic() + settings.run_seconds
        while not stop_requested.is_set():
            for query in streams.queries:
                error = query.exception()
                if error is not None:
                    raise JobError(f"query {query.name} lỗi: {error}")
                if not query.isActive:
                    raise JobError(f"query {query.name} dừng bất ngờ")
            if deadline is not None and time.monotonic() >= deadline:
                break
            stop_requested.wait(1.0)
        return 0
    except JobError as exc:
        _log(str(exc))
        return 1
    finally:
        if streams is not None:
            _log(f"dừng query: {stop_streams(streams)}")
        if listener is not None:
            detach_listener(spark, listener)
        if foreach_batch is not None and hasattr(foreach_batch, "close"):
            foreach_batch.close()  # flush producer cảnh báo, đóng Redis — sau khi query đã dừng
        spark.stop()


def detach_listener(spark: SparkSession, listener: Any) -> None:
    """Gỡ listener khỏi session rồi đóng client Redis/Kafka admin của nó (sau khi query đã dừng)."""
    try:
        spark.streams.removeListener(listener)
    except Exception as exc:  # session đang tắt: vẫn đóng client bên dưới
        _log(f"không gỡ được listener: {type(exc).__name__}: {exc}")
    listener.close()


if __name__ == "__main__":
    sys.exit(main())
