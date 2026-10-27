"""Snapshot hồ sơ tại một cutoff: thêm cột cutoff và gợi ý mẫu, ghi Parquet, đọc lại để đánh giá.

Gợi ý baseline không cá nhân hóa: chỉ (người học, kỹ năng) đang bế tắc mới nhận gợi ý, bậc 1
(nhắc khái niệm + bài tương tự), lên bậc 2 khi đồng thời có cảnh báo sai liên tiếp; loại lỗi
không chẩn đoán (None). Snapshot không chứa ground truth.
"""

from __future__ import annotations

import hashlib
from pathlib import Path
from typing import Any

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

from ai_math_tutor.batch.fold import PROFILE_COLUMNS
from ai_math_tutor.hints.templates import SKILL_TEMPLATES, template_hint

BASELINE_HINT_LEVELS = (1, 2)
SNAPSHOT_COLUMNS = (*PROFILE_COLUMNS, "cutoff_ms", "hint_level", "hint_text_vi")
DETECTION_COLUMNS = ("learner_id", "skill_id", "onset_event_id", "onset_event_time_ms", "onset_sent_time_ms", "end_event_time_ms")


def hint_table(spark: SparkSession) -> DataFrame:
    rows = [
        (skill_id, level, template_hint(skill_id, level).text_vi)
        for skill_id in SKILL_TEMPLATES
        for level in BASELINE_HINT_LEVELS
    ]
    return spark.createDataFrame(rows, schema="skill_id string, hint_level int, hint_text_vi string")


def finalize_snapshot(profiles: DataFrame, cutoff_ms: int, hints: DataFrame) -> DataFrame:
    level = F.when(F.col("stuck"), F.when(F.col("consec_wrong_alert"), F.lit(2)).otherwise(F.lit(1))).cast("int")
    with_level = profiles.withColumn("cutoff_ms", F.lit(cutoff_ms).cast("bigint")).withColumn("hint_level", level)
    joined = with_level.join(F.broadcast(hints), on=["skill_id", "hint_level"], how="left")
    return joined.select(*SNAPSHOT_COLUMNS)


def snapshot_path(root: Path, cutoff_ms: int) -> Path:
    return root / f"cutoff_ms={cutoff_ms}"


def write_snapshot(snapshot: DataFrame, path: Path) -> None:
    """Ghi Parquet; thư mục đã tồn tại là lỗi (kết quả không bao giờ bị ghi đè)."""
    snapshot.write.mode("errorifexists").parquet(str(path))


def read_snapshot(spark: SparkSession, path: Path) -> dict[str, Any]:
    """Đếm và lấy các onset (để tính phát hiện) từ snapshot đã ghi."""
    frame = spark.read.parquet(str(path))
    totals = frame.agg(
        F.count(F.lit(1)).alias("rows_out"),
        F.sum(F.col("stuck").cast("int")).alias("stuck"),
        F.sum(F.col("consec_wrong_alert").cast("int")).alias("consec_wrong_alert"),
        F.sum(F.col("mastered").cast("int")).alias("mastered"),
        F.sum(F.col("onset_event_id").isNotNull().cast("int")).alias("with_onset"),
        F.sum(F.col("hint_text_vi").isNotNull().cast("int")).alias("hints"),
        F.sum((F.col("stuck") & F.col("hint_text_vi").isNull()).cast("int")).alias("stuck_without_template"),
    ).first()
    detections = [
        row.asDict()
        for row in frame.filter(F.col("onset_event_id").isNotNull()).select(*DETECTION_COLUMNS).collect()
    ]
    counts = {key: int(totals[key] or 0) for key in totals.asDict()}
    return {**counts, "detections": detections}


def profile_rows(profiles: DataFrame) -> list[tuple[Any, ...]]:
    """Các dòng hồ sơ (PROFILE_COLUMNS) đã sắp theo (learner, skill) — khóa duy nhất nên không so tới None."""
    return sorted(tuple(row) for row in profiles.select(*PROFILE_COLUMNS).collect())


def compare_profiles(left: DataFrame, right: DataFrame) -> dict[str, Any]:
    """So hai bảng hồ sơ: số dòng mỗi bên, số dòng chỉ có ở một bên, có giống hệt không."""
    a, b = profile_rows(left), profile_rows(right)
    differing = len(set(a) ^ set(b))
    return {"rows_left": len(a), "rows_right": len(b), "differing_rows": differing, "identical": a == b}


def profile_digest(spark: SparkSession, path: Path) -> str:
    """SHA-256 của nội dung hồ sơ (sắp theo learner, skill; không gồm cutoff/hint) — kiểm tính tất định."""
    frame = spark.read.parquet(str(path)).select(*PROFILE_COLUMNS).toPandas()
    frame = frame.sort_values(["learner_id", "skill_id"], kind="mergesort").reset_index(drop=True)
    text = frame.to_csv(index=False, lineterminator="\n")
    return "sha256:" + hashlib.sha256(text.encode("utf-8")).hexdigest()
