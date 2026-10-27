"""Cùng luật hồ sơ, viết theo kiểu MapReduce trên Spark RDD (để minh họa và đối chiếu với đường DataFrame).

Luồng (mỗi mũi tên là một bước map hoặc một lần shuffle):

    value ─map(parse_delivery)─▶ (event_id, bản ghi) ─filter(sent ≤ cutoff)─▶ reduceByKey(min)      # khử trùng
      ─flatMap(to_observations)─▶ ((learner, skill, event_time, session_seq, event_id), (sent, mã quan sát))
        ├─ map → ((learner, skill), (cơ hội, đúng, gợi ý)) ─reduceByKey(cộng)─▶ số đếm               # combiner-safe
        └─ repartitionAndSortWithinPartitions(partition = crc32(learner))                           # secondary sort
             ─mapPartitions(fold theo nhóm liên tiếp)─▶ ((learner, skill), hồ sơ có thứ tự)
      ─join─▶ hồ sơ; số đếm của hai nhánh phải khớp (FoldInvariantError nếu không)

- `reduceByKey` gộp cục bộ trong từng partition trước khi shuffle (combiner). Chỉ an toàn khi phép gộp
  kết hợp và giao hoán: `min` (khử trùng) và cộng bộ ba số đếm thỏa điều đó, nên kết quả không đổi
  dù dữ liệu chia 1, 3 hay 7 partition.
- Chuỗi đúng/sai liên tiếp không gộp được như vậy. Khóa ghép (learner, skill, event_time, session_seq,
  event_id) + partitioner chỉ theo learner ⇒ mọi sự kiện của một người học vào cùng partition và
  được Spark sắp sẵn theo thời gian; reducer chỉ cần duyệt một lượt (secondary sort).
- Partitioner dùng crc32 (tất định giữa các tiến trình), không dùng `hash()` của Python (bị ngẫu
  nhiên hóa theo PYTHONHASHSEED).
"""

from __future__ import annotations

import itertools
import json
import zlib
from collections.abc import Iterable, Iterator
from typing import Any

from pyspark import RDD
from pyspark.sql import DataFrame, SparkSession

from ai_math_tutor import learner_rules
from ai_math_tutor.batch.fold import (
    COUNT_COLUMNS,
    PROFILE_DDL,
    PROFILE_EVENT_TYPES,
    FoldInvariantError,
    count_contribution,
    fold_skill,
    observation_of,
    profile_row,
)
from ai_math_tutor.batch.source import EventSource
from ai_math_tutor.event_validation import HEARTBEAT_EVENT_TYPE
from ai_math_tutor.simulator.output import epoch_ms, parse_utc, restamp

# Mã hóa quan sát thành số nhỏ để shuffle rẻ; giải mã về đúng hằng số của learner_rules.
_OBSERVATION_BY_CODE = {
    1: learner_rules.OBSERVATION_CORRECT,
    0: learner_rules.OBSERVATION_WRONG,
    2: learner_rules.OBSERVATION_HINT,
}
# is_correct trong bản ghi đã parse: 1 / 0 / -1 (thiếu hoặc không phải bool) — so sánh được trong min().
_MISSING = -1

Record = tuple[int, int, str, int, str, tuple[str, ...], int]  # sent, event, learner, seq, type, skills, is_correct


def _observation_code(observation: learner_rules.Observation) -> int:
    if observation.is_hint:
        return 2
    return 1 if observation.correct else 0


def _time_ms(value: Any) -> int | None:
    if not isinstance(value, str):
        return None
    try:
        return epoch_ms(parse_utc(value))
    except ValueError:
        return None


def parse_delivery(value: str) -> tuple[str, Record] | None:
    """JSON sự kiện → (event_id, bản ghi); None nếu hỏng hoặc thiếu trường mà luật cần."""
    try:
        event = json.loads(value)
    except (TypeError, ValueError):
        return None
    if not isinstance(event, dict):
        return None
    event_id, learner_id, event_type = event.get("event_id"), event.get("learner_id"), event.get("event_type")
    session_seq, skills = event.get("session_seq"), event.get("skill_ids")
    if not (isinstance(event_id, str) and isinstance(learner_id, str) and isinstance(event_type, str)):
        return None
    if event_type == HEARTBEAT_EVENT_TYPE:  # bản ghi kỹ thuật của streaming, không phải sự kiện học tập
        return None
    if isinstance(session_seq, bool) or not isinstance(session_seq, int):
        return None
    if not isinstance(skills, list) or not all(isinstance(skill, str) for skill in skills):
        return None
    event_ms, sent_ms = _time_ms(event.get("event_time")), _time_ms(event.get("sent_time"))
    if event_ms is None or sent_ms is None:
        return None
    payload = event.get("payload")
    correct = payload.get("is_correct") if isinstance(payload, dict) else None
    correct_code = int(correct) if isinstance(correct, bool) else _MISSING
    return event_id, (sent_ms, event_ms, learner_id, session_seq, event_type, tuple(skills), correct_code)


def to_observations(item: tuple[str, Record]) -> list[tuple[tuple[str, str, int, int, str], tuple[int, int]]]:
    """Một sự kiện đã khử trùng → các cặp (khóa ghép, (sent_ms, mã quan sát)), một cặp mỗi kỹ năng."""
    event_id, (sent_ms, event_ms, learner_id, session_seq, event_type, skills, correct_code) = item
    if event_type not in PROFILE_EVENT_TYPES:
        return []
    try:
        observation = observation_of(event_type, None if correct_code == _MISSING else bool(correct_code))
    except ValueError:  # submit_step thiếu is_correct: không hợp lệ, bỏ như đường DataFrame
        return []
    if observation is None:
        return []
    code = _observation_code(observation)
    return [((learner_id, skill, event_ms, session_seq, event_id), (sent_ms, code)) for skill in skills]


def learner_partition(key: tuple[Any, ...]) -> int:
    """Partitioner theo learner_id (phần tử đầu của khóa ghép); Spark lấy kết quả modulo số partition."""
    return zlib.crc32(key[0].encode("utf-8"))


def fold_sorted_partition(
    items: Iterable[tuple[tuple[str, str, int, int, str], tuple[int, int]]],
) -> Iterator[tuple[tuple[str, str], dict[str, Any]]]:
    """Reducer của secondary sort: các khóa đã sắp ⇒ mỗi (learner, skill) là một đoạn liên tiếp."""
    for (learner_id, skill_id), group in itertools.groupby(items, key=lambda kv: (kv[0][0], kv[0][1])):
        observations = (
            (key[2], value[0], key[4], _OBSERVATION_BY_CODE[value[1]]) for key, value in group
        )
        yield (learner_id, skill_id), fold_skill(learner_id, skill_id, observations)


def _add_counts(a: tuple[int, int, int], b: tuple[int, int, int]) -> tuple[int, int, int]:
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2])


def _merge(item: tuple[tuple[str, str], tuple[dict[str, Any], tuple[int, int, int]]]) -> tuple[Any, ...]:
    key, (profile, counts) = item
    ordered = tuple(profile[name] for name in COUNT_COLUMNS)
    if ordered != counts:
        raise FoldInvariantError(f"số đếm reduceByKey {counts} khác fold có thứ tự {ordered} tại {key}")
    return profile_row(profile)


def source_values(spark: SparkSession, source: EventSource) -> RDD:
    """RDD các chuỗi JSON sự kiện (đã đóng dấu lại nếu nguồn yêu cầu)."""
    if source.format == "jsonl":
        return spark.sparkContext.textFile(source.spark_path).filter(lambda line: bool(line.strip()))
    frame = spark.read.parquet(source.spark_path)
    if not source.restamps:
        return frame.select("value").rdd.map(lambda row: row[0])
    start = int(source.replay_start_ms)  # type: ignore[arg-type]  # restamps ⇒ không None
    return frame.select("value", "event_offset_ms", "sent_offset_ms").rdd.map(
        lambda row: restamp(row[0], start, row[1], row[2])
    )


def profile_rdd(values: RDD, cutoff_ms: int, num_partitions: int) -> RDD:
    """Các tuple hồ sơ (PROFILE_COLUMNS) từ RDD chuỗi JSON."""
    if num_partitions < 1:
        raise ValueError(f"num_partitions phải ≥ 1, nhận {num_partitions}")
    unique = (
        values.map(parse_delivery)
        .filter(lambda item: item is not None and item[1][0] <= cutoff_ms)
        .reduceByKey(min, numPartitions=num_partitions)
    )
    keyed = unique.flatMap(to_observations)
    counts = keyed.map(lambda kv: ((kv[0][0], kv[0][1]), count_contribution(_OBSERVATION_BY_CODE[kv[1][1]]))).reduceByKey(
        _add_counts, numPartitions=num_partitions
    )
    ordered = keyed.repartitionAndSortWithinPartitions(
        numPartitions=num_partitions, partitionFunc=learner_partition
    ).mapPartitions(fold_sorted_partition)
    return ordered.join(counts, numPartitions=num_partitions).map(_merge)


def profiles_mapreduce(
    spark: SparkSession, source: EventSource, cutoff_ms: int, num_partitions: int, input_partitions: int | None = None
) -> DataFrame:
    """Hồ sơ tại cutoff bằng đường RDD; `input_partitions` chia lại đầu vào (kiểm combiner-safety)."""
    values = source_values(spark, source)
    if input_partitions is not None:
        values = values.repartition(input_partitions)
    return spark.createDataFrame(profile_rdd(values, cutoff_ms, num_partitions), schema=PROFILE_DDL)
