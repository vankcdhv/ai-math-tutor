"""Đường RDD MapReduce ≡ đường DataFrame; kết quả không đổi khi chia 1/3/7 partition (combiner-safe)."""

from __future__ import annotations

import json
import zlib
from pathlib import Path

import pytest

from ai_math_tutor import learner_rules
from ai_math_tutor.batch.mapreduce_profile import (
    fold_sorted_partition,
    learner_partition,
    parse_delivery,
    to_observations,
)
from ai_math_tutor.batch.source import open_source
from ai_math_tutor.simulator import SimulationConfig, run_simulation
from ai_math_tutor.simulator.output import iter_rows, parquet_metadata, write_events_jsonl


def _event(**overrides):
    event = {
        "event_id": "e1", "event_type": "submit_step", "learner_id": "L-1", "session_seq": 3,
        "skill_ids": ["a", "b"], "event_time": "2026-11-02T00:00:01.250Z", "sent_time": "2026-11-02T00:00:01.300Z",
        "payload": {"is_correct": False},
    }
    event.update(overrides)
    return json.dumps(event)


# ---------------------------------------------------------------- các bước map/reduce (thuần Python)

def test_parse_delivery_reads_times_from_the_event_json():
    event_id, record = parse_delivery(_event())
    assert event_id == "e1"
    assert record == (1_793_577_601_300, 1_793_577_601_250, "L-1", 3, "submit_step", ("a", "b"), 0)


@pytest.mark.parametrize(
    "value",
    ["{not json", "[]", _event(event_id=None), _event(session_seq=True), _event(skill_ids="a"),
     _event(event_time="2026-11-02T00:00:01"), _event(sent_time=5)],
)
def test_parse_delivery_rejects_malformed_events(value):
    assert parse_delivery(value) is None


def test_observations_follow_learner_rules_mapping_per_skill():
    pairs = to_observations(parse_delivery(_event()))
    assert [key[:2] for key, _ in pairs] == [("L-1", "a"), ("L-1", "b")]
    assert pairs[0] == (("L-1", "a", 1_793_577_601_250, 3, "e1"), (1_793_577_601_300, 0))
    assert to_observations(parse_delivery(_event(event_type="open_problem"))) == []
    assert to_observations(parse_delivery(_event(payload={}))) == []  # submit_step thiếu is_correct: bỏ
    hint = to_observations(parse_delivery(_event(event_type="request_hint", payload={"hint_level": 1})))
    assert hint[0][1][1] == 2


def test_partitioner_is_stable_and_depends_only_on_learner():
    assert learner_partition(("L-1", "a", 1, 1, "x")) == zlib.crc32(b"L-1") == learner_partition(("L-1", "z", 9, 9, "y"))


def test_sorted_partition_fold_groups_consecutive_keys():
    items = sorted(
        [(("L-1", "a", t, 1, f"e{t}"), (t, 0)) for t in range(10)]
        + [(("L-1", "b", 5, 1, "x"), (5, 1)), (("L-2", "a", 1, 1, "y"), (1, 2))]
    )
    folded = dict(fold_sorted_partition(items))
    assert set(folded) == {("L-1", "a"), ("L-1", "b"), ("L-2", "a")}
    assert folded[("L-1", "a")]["stuck"] is True and folded[("L-1", "a")]["onset_event_id"] == "e9"
    assert folded[("L-2", "a")]["hints_used"] == 1 and folded[("L-2", "a")]["opportunities"] == 0
    assert learner_rules.WHEEL_SPIN_OPPORTUNITIES == 10


# ---------------------------------------------------------------- Spark

@pytest.fixture(scope="module")
def tiny_log(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("parity-log")
    run_simulation(SimulationConfig(learners=20, horizon_s=600, seed=9, out_dir=out, target_rate=5.0,
                                    late_profile="high", duplicate_rate=0.05))
    return out


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(master="local[2]", shuffle_partitions=2, data_root=tmp_path_factory.mktemp("spark"))
    with spark_session(conf) as session:
        yield session


def _cutoffs(log_dir: Path) -> tuple[int, int]:
    t0 = int(parquet_metadata(log_dir / "events.parquet")["nhom8.t0_epoch_ms"])
    sent = sorted(r["sent_offset_ms"] for r in iter_rows(log_dir / "events.parquet", columns=["sent_offset_ms"]))
    return t0 + sent[len(sent) // 2], t0 + sent[-1]


def test_dataframe_and_rdd_paths_give_identical_profiles(spark, tiny_log):
    from ai_math_tutor.batch.daily_profile import profiles_dataframe
    from ai_math_tutor.batch.mapreduce_profile import profiles_mapreduce
    from ai_math_tutor.batch.snapshot import compare_profiles

    source = open_source(tiny_log / "events.parquet")
    for cutoff in _cutoffs(tiny_log):
        result = compare_profiles(profiles_dataframe(spark, source, cutoff), profiles_mapreduce(spark, source, cutoff, 2))
        assert result["identical"], result
        assert result["rows_left"] > 0


def test_rdd_result_is_the_same_for_1_3_7_partitions(spark, tiny_log):
    from ai_math_tutor.batch.mapreduce_profile import profiles_mapreduce
    from ai_math_tutor.batch.snapshot import profile_rows

    source = open_source(tiny_log / "events.parquet")
    _, end = _cutoffs(tiny_log)
    results = [profile_rows(profiles_mapreduce(spark, source, end, n, input_partitions=n)) for n in (1, 3, 7)]
    assert results[0] and results[0] == results[1] == results[2]


def test_dataframe_result_is_the_same_for_1_3_7_shuffle_partitions(spark, tiny_log):
    from ai_math_tutor.batch.daily_profile import profiles_dataframe
    from ai_math_tutor.batch.snapshot import profile_rows

    source = open_source(tiny_log / "events.parquet")
    _, end = _cutoffs(tiny_log)
    previous = spark.conf.get("spark.sql.shuffle.partitions")
    results = []
    try:
        for n in (1, 3, 7):
            spark.conf.set("spark.sql.shuffle.partitions", str(n))
            results.append(profile_rows(profiles_dataframe(spark, source, end)))
    finally:
        spark.conf.set("spark.sql.shuffle.partitions", previous)
    assert results[0] and results[0] == results[1] == results[2]


def test_jsonl_log_gives_the_same_profiles_as_parquet(spark, tiny_log, tmp_path):
    from ai_math_tutor.batch.daily_profile import profiles_dataframe
    from ai_math_tutor.batch.mapreduce_profile import profiles_mapreduce
    from ai_math_tutor.batch.snapshot import profile_rows

    from ai_math_tutor.batch.daily_profile import delivery_inventory

    jsonl = tmp_path / "events.jsonl"
    deliveries = write_events_jsonl(tiny_log / "events.parquet", jsonl)
    # Log đã phát lại có thể chứa heartbeat (hợp đồng v0.2) và dòng hỏng: cả hai không được thành hồ sơ.
    heartbeat = {"schema_version": "0.2", "event_id": "00000000-0000-4000-8000-000000000000", "event_type": "heartbeat",
                 "learner_id": "L-0000000000000000", "event_time": "2026-11-02T00:00:00.000Z",
                 "sent_time": "2026-11-02T00:00:00.000Z", "payload": {}}
    with jsonl.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(heartbeat) + "\n{not json\n")
    mid, end = _cutoffs(tiny_log)
    from_parquet = profile_rows(profiles_dataframe(spark, open_source(tiny_log / "events.parquet"), mid))
    assert profile_rows(profiles_dataframe(spark, open_source(jsonl), mid)) == from_parquet
    assert profile_rows(profiles_mapreduce(spark, open_source(jsonl), mid, 3)) == from_parquet
    inventory = delivery_inventory(spark, open_source(jsonl))
    assert (inventory["deliveries"], inventory["heartbeats"], inventory["invalid"]) == (deliveries + 2, 1, 1)
    assert inventory["delivery_sent_ms"].size == deliveries and inventory["delivery_sent_ms"].max() == end
