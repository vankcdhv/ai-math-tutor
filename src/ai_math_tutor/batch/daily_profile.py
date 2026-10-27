"""Đường chính của batch baseline: Spark DataFrame + `applyInPandas` gọi `learner_rules`.

Một job tại cutoff c:
1. đọc log (`read_values`; Parquet có thể được đóng dấu lại bằng `simulator.output.restamp`);
2. parse JSON sự kiện bằng `from_json`, lấy `event_time`/`sent_time` từ chính JSON (`parse_deliveries`);
3. bỏ dòng hỏng và heartbeat (hợp đồng v0.2), giữ lần giao có `sent_time ≤ c` (batch chỉ thấy sự
   kiện **đã đến** trước cutoff);
4. khử trùng theo `event_id` (`deduplicate`: giữ min theo struct — tất định, bản trùng giống từng byte);
5. tách mỗi sự kiện thành một quan sát cho từng kỹ năng trong `skill_ids` (`observations`);
6. `groupBy(learner_id, skill_id).applyInPandas`: trong mỗi nhóm sắp theo
   (event_time, session_seq, event_id) rồi fold bằng `fold.fold_skill` — luật không viết lại.

Bước 6 là một "reducer có thứ tự": chuỗi sai/đúng liên tiếp phụ thuộc thứ tự nên không thể gộp
từng phần như phép đếm; phải gom đủ nhóm rồi mới duyệt.
"""

from __future__ import annotations

import math
from typing import Any

import pandas as pd
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F

from ai_math_tutor.batch.fold import (
    ORDER_COLUMNS,
    PROFILE_COLUMNS,
    PROFILE_DDL,
    PROFILE_EVENT_TYPES,
    fold_skill,
    observation_of,
    profile_row,
)
from ai_math_tutor.batch.source import EventSource
from ai_math_tutor.event_validation import HEARTBEAT_EVENT_TYPE
from ai_math_tutor.simulator.output import restamp

VALUE_SCHEMA = (
    "event_id string, event_type string, learner_id string, session_seq int, skill_ids array<string>, "
    "event_time string, sent_time string, payload struct<is_correct: boolean>"
)
STEP_EVENT_TYPE = "submit_step"  # loại sự kiện duy nhất cần payload.is_correct (learner_rules.observation_for)
DEDUP_COLUMNS = ("sent_time_ms", "event_time_ms", "learner_id", "session_seq", "event_type", "skill_ids", "is_correct")


def _restamp_series(value: pd.Series, event_offset_ms: pd.Series, sent_offset_ms: pd.Series, start_ms: pd.Series) -> pd.Series:
    return pd.Series(
        [restamp(v, int(s), int(e), int(t)) for v, e, t, s in zip(value, event_offset_ms, sent_offset_ms, start_ms)],
        dtype=object,
    )


def read_values(spark: SparkSession, source: EventSource) -> DataFrame:
    """Một cột `value` (JSON sự kiện), mỗi dòng một lần giao."""
    if source.format == "jsonl":
        lines = spark.read.text(source.spark_path)
        return lines.filter(F.length(F.trim(F.col("value"))) > 0)
    frame = spark.read.parquet(source.spark_path)
    if not source.restamps:
        return frame.select("value")
    restamp_udf = F.pandas_udf(_restamp_series, "string")
    return frame.select(
        restamp_udf(
            F.col("value"), F.col("event_offset_ms"), F.col("sent_offset_ms"), F.lit(source.replay_start_ms).cast("long")
        ).alias("value")
    )


def _epoch_ms(column: Column) -> Column:
    # Chỉ chấp nhận UTC kết thúc bằng 'Z' như hợp đồng (giống simulator.output.parse_utc ở đường RDD).
    return F.when(column.endswith("Z"), F.unix_millis(F.try_to_timestamp(column)))


def parse_deliveries(values: DataFrame) -> DataFrame:
    """Parse JSON; thêm `event_time_ms`, `sent_time_ms` và cờ `valid` (đủ trường để vào luật)."""
    event = F.from_json(F.col("value"), VALUE_SCHEMA).alias("e")
    parsed = values.select(event).select(
        F.col("e.event_id").alias("event_id"),
        F.col("e.learner_id").alias("learner_id"),
        F.col("e.session_seq").alias("session_seq"),
        F.col("e.event_type").alias("event_type"),
        F.col("e.skill_ids").alias("skill_ids"),
        F.col("e.payload.is_correct").alias("is_correct"),
        _epoch_ms(F.col("e.event_time")).alias("event_time_ms"),
        _epoch_ms(F.col("e.sent_time")).alias("sent_time_ms"),
    )
    required = ("event_id", "learner_id", "session_seq", "event_type", "skill_ids", "event_time_ms", "sent_time_ms")
    heartbeat = F.coalesce(F.col("event_type") == F.lit(HEARTBEAT_EVENT_TYPE), F.lit(False))
    valid = ~heartbeat
    for name in required:
        valid = valid & F.col(name).isNotNull()
    return parsed.withColumn("heartbeat", heartbeat).withColumn("valid", valid)


def deduplicate(deliveries: DataFrame) -> DataFrame:
    """Một dòng mỗi `event_id` (min theo struct DEDUP_COLUMNS ⇒ tất định kể cả khi bản trùng khác nhau)."""
    return (
        deliveries.groupBy("event_id")
        .agg(F.min(F.struct(*DEDUP_COLUMNS)).alias("d"))
        .select("event_id", "d.*")
    )


def observations(events: DataFrame) -> DataFrame:
    """Sự kiện có ảnh hưởng hồ sơ → một dòng mỗi kỹ năng; submit_step thiếu is_correct bị bỏ (không hợp lệ)."""
    relevant = F.col("event_type").isin(sorted(PROFILE_EVENT_TYPES)) & (
        (F.col("event_type") != STEP_EVENT_TYPE) | F.col("is_correct").isNotNull()
    )
    return events.filter(relevant).select(
        "learner_id",
        F.explode("skill_ids").alias("skill_id"),
        "event_time_ms",
        "sent_time_ms",
        "session_seq",
        "event_id",
        "event_type",
        "is_correct",
    )


def arrived_events(spark: SparkSession, source: EventSource, cutoff_ms: int) -> DataFrame:
    """Sự kiện duy nhất, hợp lệ, đã đến trước hoặc đúng cutoff."""
    deliveries = parse_deliveries(read_values(spark, source))
    arrived = deliveries.filter(F.col("valid") & (F.col("sent_time_ms") <= F.lit(cutoff_ms)))
    return deduplicate(arrived)


def _as_bool(value: Any) -> bool | None:
    """Giá trị từ pandas (numpy.bool_, None, NaN) → bool thuần hoặc None; learner_rules đòi đúng kiểu bool."""
    if value is None or (isinstance(value, float) and math.isnan(value)) or value is pd.NA:
        return None
    return bool(value)


def _fold_group(key: tuple[Any, ...], group: pd.DataFrame) -> pd.DataFrame:
    # Sắp bằng tuple Python theo ORDER_COLUMNS: event_id duy nhất trong nhóm (đã khử trùng) nên
    # phép so sánh không bao giờ chạm tới các phần tử sau nó. Nhanh hơn sort_values/astype trên
    # DataFrame nhỏ — chi phí mỗi nhóm là phần lớn runtime của applyInPandas.
    ordered = sorted(zip(*(group[name].tolist() for name in (*ORDER_COLUMNS, "sent_time_ms", "event_type", "is_correct"))))
    rows = (
        (int(event_time), int(sent_time), str(event_id), observation_of(event_type, _as_bool(is_correct)))
        for event_time, _seq, event_id, sent_time, event_type, is_correct in ordered
    )
    profile = fold_skill(str(key[0]), str(key[1]), rows)
    # Cột có thể null (onset/end) để kiểu object chứa int/None; Arrow ép theo PROFILE_DDL.
    return pd.DataFrame.from_records([profile_row(profile)], columns=list(PROFILE_COLUMNS))


def profiles_dataframe(spark: SparkSession, source: EventSource, cutoff_ms: int) -> DataFrame:
    """Hồ sơ (PROFILE_COLUMNS) của mọi (người học, kỹ năng) tại cutoff, bằng DataFrame + applyInPandas."""
    obs = observations(arrived_events(spark, source, cutoff_ms))
    return obs.groupBy("learner_id", "skill_id").applyInPandas(_fold_group, schema=PROFILE_DDL)


def delivery_inventory(spark: SparkSession, source: EventSource) -> dict[str, Any]:
    """Kiểm kê toàn log (ngoài phần đo runtime): số lần giao, dòng hỏng, sự kiện duy nhất, mảng thời gian.

    Trả `delivery_sent_ms` (mọi lần giao hợp lệ) và `unique_event_ms`/`unique_sent_ms`/`unique_affects_profile`
    (mỗi event_id một phần tử) dạng numpy để tính số dòng vào từng job và độ cũ.
    """
    deliveries = parse_deliveries(read_values(spark, source)).cache()
    try:
        totals = deliveries.agg(
            F.count(F.lit(1)).alias("deliveries"),
            F.sum(F.col("heartbeat").cast("int")).alias("heartbeats"),
            F.sum((~F.col("valid") & ~F.col("heartbeat")).cast("int")).alias("invalid"),
        ).first()
        valid = deliveries.filter("valid")
        sent = valid.select("sent_time_ms").toPandas()["sent_time_ms"].to_numpy(dtype="int64")
        unique = deduplicate(valid).select(
            "event_time_ms", "sent_time_ms", F.col("event_type").isin(sorted(PROFILE_EVENT_TYPES)).alias("affects_profile")
        ).toPandas()
    finally:
        deliveries.unpersist()
    return {
        "deliveries": int(totals["deliveries"]),
        "heartbeats": int(totals["heartbeats"] or 0),
        "invalid": int(totals["invalid"] or 0),
        "delivery_sent_ms": sent,
        "unique_event_ms": unique["event_time_ms"].to_numpy(dtype="int64"),
        "unique_sent_ms": unique["sent_time_ms"].to_numpy(dtype="int64"),
        "unique_affects_profile": unique["affects_profile"].to_numpy(dtype=bool),
    }
