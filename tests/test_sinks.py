"""Sinks không cần Lua: gom batch, tổng hợp lớp (fakeredis), Parquet ghi đè theo batch, cửa sổ độ trễ.

Phần Lua (upsert có version, SET NX → XADD) cần Redis thật vì fakeredis không có EVAL khi thiếu
`lupa`: xem tests/test_redis_lua.py (integration).
"""

from __future__ import annotations

import json
import random

import fakeredis
import numpy as np
import pyarrow.parquet as pq
import pytest

from ai_math_tutor.config import load_config
from ai_math_tutor.streaming.processor import Event, MemoryLearnerStore
from ai_math_tutor.streaming.schema import OUTPUT_COLUMNS, OUTPUT_SCHEMA
from ai_math_tutor.streaming.sinks import (
    LAKE_FILE,
    BatchPlan,
    ForeachBatchSink,
    LatencyWindow,
    LearnerStateCache,
    RedisKeys,
    SinkError,
    batch_dir,
    class_aggregates,
    load_roster,
    nearest_rank,
    parse_alert_state,
    plan_batch,
    rows_from_dataframe,
    snapshot_event_ms,
    state_value,
    trend_bucket,
    upsert_calls,
    write_batch_parquet,
)

T0 = 1_793_577_600_000  # 2026-11-02T00:00:00Z
SKILL = "linear_equation.isolate_variable"


def _event(i: int, kind: str = "submit_step", correct: bool | None = True, *, t: int | None = None,
           skills: tuple[str, ...] = (SKILL,), problem: str = "P-LIN-001") -> Event:
    event_time = T0 + (t if t is not None else i * 1000)
    return Event(event_id=f"e{i:03d}", event_type=kind, event_time_ms=event_time, sent_time_ms=event_time + 40,
                 session_seq=i, class_id="C-TEST-1", problem_id=problem, skill_ids=skills,
                 is_correct=correct if kind == "submit_step" else None,
                 step_index=1 if kind == "submit_step" else None)


def _rows(events, learner="L-00000000000000a1", mode="eager", model="rule", store=None):
    store = store or MemoryLearnerStore(learner)
    return store, store.run(events, mode, model, watermark_ms=None)


def _cfg(tmp_path):
    return load_config(env={"REDIS_PASSWORD": "unit-test", "NHOM8_DATA_ROOT": str(tmp_path)}, dotenv_path=None)


# ---------------------------------------------------------------- plan_batch


def test_plan_batch_keeps_latest_version_and_lists_each_event_once():
    store, first = _rows([_event(1, "open_problem"), _event(2, correct=False), _event(3, correct=True)])
    _, second = _rows([_event(4, correct=True)], store=store)
    plan = plan_batch(first + second)
    assert [u["skill_id"] for u in plan.updates] == [SKILL]
    assert plan.updates[0]["state_version"] == 4  # bản của "batch" sau thắng
    assert [a["event_id"] for a in plan.applied] == ["e001", "e002", "e003", "e004"]
    assert plan.applied[0]["sent_time_ms"] - plan.applied[0]["event_time_ms"] == 40
    assert len(plan.progress) == 1 and plan.progress[0]["state_version"] == 4
    assert plan.touched_classes == {"C-TEST-1": T0 + 4000}
    assert plan.duplicate_applied_in_batch == 0


def test_plan_batch_counts_event_listed_twice_and_rejects_contract_violations():
    _, rows = _rows([_event(1)])
    update = next(r for r in rows if r["kind"] == "update")
    plan = plan_batch([update, dict(update)])
    assert len(plan.applied) == 1 and plan.duplicate_applied_in_batch == 1

    with pytest.raises(SinkError, match="kind"):
        plan_batch([{**update, "kind": "snapshot"}])
    with pytest.raises(SinkError, match="lệch độ dài"):
        plan_batch([{**update, "applied_sent_time_ms": []}])
    with pytest.raises(SinkError, match="state_version"):
        plan_batch([{**update, "state_version": None}])
    with pytest.raises(SinkError, match="alert_type"):
        plan_batch([{"kind": "alert", "learner_id": "L-1", "class_id": "C-1", "skill_id": SKILL,
                     "alert_id": "a1", "alert_type": "bored"}])


def test_alert_rows_are_deduplicated_by_alert_id():
    # 10 lần sai liên tiếp ⇒ một cảnh báo consec_wrong (lần 3) và một cảnh báo stuck (cơ hội 10).
    _, rows = _rows([_event(i, correct=False) for i in range(1, 11)])
    alerts = [r for r in rows if r["kind"] == "alert"]
    assert sorted(a["alert_type"] for a in alerts) == ["consec_wrong", "stuck"]
    plan = plan_batch(rows + alerts)
    assert len(plan.alerts) == 2
    stuck = next(a for a in plan.alerts if a["alert_type"] == "stuck")
    assert stuck["stuck_onset_event_id"] == "e010"


def test_upsert_calls_encode_field_and_hash_records_in_chunks():
    _, rows = _rows([_event(1), _event(2, problem="P-LIN-002")])
    plan = plan_batch(rows)
    keys = RedisKeys("it:")
    calls = upsert_calls(plan, keys, batch_id=7, chunk=2)
    flat_keys = [k for call_keys, _ in calls for k in call_keys]
    assert flat_keys == ["it:state:L-00000000000000a1", "it:progress:L-00000000000000a1:P-LIN-001",
                         "it:progress:L-00000000000000a1:P-LIN-002"]
    assert [len(call_keys) for call_keys, _ in calls] == [2, 1]
    state_args = calls[0][1][:5]
    assert state_args[:4] == ["f", SKILL, "7", "2"]  # loại, trường, batch_id, state_version
    value = json.loads(state_args[4])
    assert value["state_version"] == 2 and value["opportunities"] == 2 and value["batch_id"] == 7
    hash_args = calls[0][1][5:]
    assert hash_args[:3] == ["h", "7", "1"]
    pairs = dict(zip(hash_args[4::2], hash_args[5::2]))
    assert int(hash_args[3]) == len(pairs) and pairs["state_version"] == "1" and pairs["last_step"] == "1"
    assert pairs["batch_id"] == "7"


def test_plan_batch_prefers_post_expiry_rows_over_higher_pre_expiry_versions():
    """Đệm ordered được xả rồi state hết hạn, người học quay lại trong cùng batch: version bắt đầu lại từ 1
    nhưng bản sau hết hạn (event time mới hơn) phải là bản được ghi."""
    ttl = 60_000
    store = MemoryLearnerStore("L-00000000000000a1")
    first = store.run([_event(i, correct=False) for i in range(1, 6)], "ordered", "rule", watermark_ms=0,
                      expire_ms=ttl)
    flushed = store.run([], "ordered", "rule", watermark_ms=T0 + 5_000 + ttl, expire_ms=ttl)
    assert store.skills == {}  # đã hết hạn
    returned = store.run([_event(100, correct=True, t=10 * ttl)], "ordered", "rule",
                         watermark_ms=T0 + 10 * ttl, expire_ms=ttl)
    rows = first + flushed + returned
    versions = sorted(r["state_version"] for r in rows if r["kind"] == "update")
    assert versions == [1, 5]  # bản trước hết hạn có version cao hơn
    plan = plan_batch(rows)
    assert [(u["state_version"], u["opportunities"]) for u in plan.updates] == [(1, 1)]
    assert [(p["state_version"], p["last_event_time_ms"]) for p in plan.progress] == [(1, T0 + 10 * ttl)]


# ---------------------------------------------------------------- tổng hợp lớp


def _state(**values):
    base = {"class_id": "C-1", "p_mastery": None, "opportunities": 0, "correct": 0, "consec_wrong": 0, "streak": 0,
            "mastered": False, "stuck": False, "stuck_onset_event_id": None, "state_version": 1,
            "last_event_time_ms": T0, "batch_id": 0}
    base.update(values)
    return base


def test_class_aggregates_from_roster_states():
    states = {
        "L-a": {"s1": _state(opportunities=10, correct=3, stuck=True, p_mastery=0.2, last_event_time_ms=T0 + 5)},
        "L-b": {"s1": _state(opportunities=4, correct=4, mastered=True, p_mastery=0.9),
                "s2": _state(opportunities=2, correct=0)},
        "L-outside": {"s1": _state(opportunities=99, correct=99)},
    }
    aggregates, stuck = class_aggregates("C-1", ["L-a", "L-b", "L-c"], states)
    s1 = aggregates["s1"]
    assert (s1["learners"], s1["observed"], s1["mastered"], s1["stuck"]) == ("3", "2", "1", "1")
    assert float(s1["mean_p_mastery"]) == pytest.approx(0.55)
    assert float(s1["accuracy"]) == pytest.approx(7 / 14)
    assert float(s1["mean_opportunities"]) == pytest.approx(7.0)
    assert s1["as_of_event_ms"] == str(T0 + 5)
    assert aggregates["s2"]["mean_p_mastery"] == "" and aggregates["s2"]["observed"] == "1"
    assert stuck == ["L-a|s1"]


def test_refresh_touched_classes_writes_agg_stuck_set_and_trend(tmp_path):
    client = fakeredis.FakeRedis()
    keys = RedisKeys("u:")
    load_roster(client, {"C-TEST-1": ["L-00000000000000a1", "L-00000000000000a2"], "C-OTHER": ["L-x"]}, keys)
    assert load_roster(client, {"C-TEST-1": ["L-00000000000000a1", "L-00000000000000a2"]}, keys) == 2
    _, rows = _rows([_event(i, correct=False) for i in range(1, 11)])
    plan = plan_batch(rows)
    client.hset(keys.state("L-00000000000000a1"), SKILL, state_value(plan.updates[0], 3))
    sink = ForeachBatchSink(_cfg(tmp_path), "unit-run", mode="eager", model="rule", key_prefix="u:",
                            redis_client=client, window=LatencyWindow())
    plan.touched_classes["C-NO-ROSTER"] = -1
    assert sink._refresh_classes(plan, batch_id=3) == 1

    agg = {k.decode(): v.decode() for k, v in client.hgetall(keys.agg("C-TEST-1", SKILL)).items()}
    assert agg["learners"] == "2" and agg["observed"] == "1" and agg["stuck"] == "1" and agg["batch_id"] == "3"
    assert client.smembers(keys.stuck_set("C-TEST-1")) == {f"L-00000000000000a1|{SKILL}".encode()}
    trend_key = keys.trend("C-TEST-1", trend_bucket(T0 + 10_000))
    assert json.loads(client.hget(trend_key, SKILL))["stuck"] == "1"
    assert 0 < client.ttl(trend_key) <= 7200
    assert not client.exists(keys.agg("C-OTHER", SKILL))

    # Gọi lại cùng batch ⇒ cùng nội dung (ghi đè, không cộng dồn).
    def snapshot():
        out = {}
        for key in client.scan_iter("u:*"):
            kind = client.type(key)
            out[key] = client.hgetall(key) if kind == b"hash" else client.smembers(key)
        return out

    before = snapshot()
    sink._refresh_classes(plan, batch_id=3)
    assert snapshot() == before


def test_trend_bucket_is_five_minutes():
    assert trend_bucket(T0 + 299_999) == T0 and trend_bucket(T0 + 300_000) == T0 + 300_000


def test_trend_snapshot_uses_class_event_time_not_the_batch_event(tmp_path):
    """Batch N ghi ô 10:05 (A có sự kiện lúc 10:05:00.5); batch N+1 chỉ chạm lớp qua B với sự kiện cũ hơn
    (10:04:59.8). Snapshot của cả lớp chứa dữ liệu 10:05 của A nên phải vào ô 10:05, không ghi đè ô 10:00."""
    client = fakeredis.FakeRedis()
    keys = RedisKeys("u:")
    load_roster(client, {"C-1": ["L-a", "L-b"]}, keys)
    sink = ForeachBatchSink(_cfg(tmp_path), "unit-run", mode="eager", model="rule", key_prefix="u:",
                            redis_client=client, window=LatencyWindow())
    bucket = T0 + 10 * 300_000
    a_time, b_time = bucket + 500, bucket - 200
    client.hset(keys.state("L-a"), "s1", state_value(_update_row("L-a", "C-1", "s1", 1, a_time), 1))
    sink._refresh_classes(BatchPlan(touched_classes={"C-1": a_time}), 1)
    assert client.exists(keys.trend("C-1", bucket))
    client.hset(keys.state("L-b"), "s1", state_value(_update_row("L-b", "C-1", "s1", 1, b_time), 2))
    sink._refresh_classes(BatchPlan(touched_classes={"C-1": b_time}), 2)
    assert not client.exists(keys.trend("C-1", trend_bucket(b_time)))
    snapshot = json.loads(client.hget(keys.trend("C-1", bucket), "s1"))
    assert snapshot["observed"] == "2" and snapshot["as_of_event_ms"] == str(a_time)
    assert snapshot_event_ms({}, 42) == 42
    assert snapshot_event_ms({"s1": {"as_of_event_ms": ""}, "s2": {"as_of_event_ms": "7"}}, 42) == 7


# ---------------------------------------------------------------- bộ đệm trạng thái cho tổng hợp lớp


def _update_row(learner: str, class_id: str, skill: str, version: int, last_ms: int, *,
                rng: random.Random | None = None) -> dict:
    rng = rng or random.Random(version)
    opportunities = rng.randint(0, 12)
    return {"kind": "update", "learner_id": learner, "class_id": class_id, "skill_id": skill,
            "p_mastery": None if rng.random() < 0.3 else rng.random(), "opportunities": opportunities,
            "correct": rng.randint(0, opportunities), "consec_wrong": rng.randint(0, 3), "streak": rng.randint(0, 3),
            "mastered": rng.random() < 0.4, "stuck": rng.random() < 0.2,
            "stuck_onset_event_id": f"e-{learner}-{version}", "state_version": version, "last_event_time_ms": last_ms,
            "applied_event_ids": [], "applied_event_time_ms": [], "applied_sent_time_ms": [],
            "applied_out_of_order": []}


def _class_keys_snapshot(client, prefix: str) -> dict:
    out = {}
    for key in sorted(client.scan_iter(f"{prefix}*")):
        name = key.decode()
        if name.startswith((f"{prefix}agg:", f"{prefix}trend:")):
            out[key] = client.hgetall(key)
        elif name.startswith(f"{prefix}stuck:set:"):
            out[key] = client.smembers(key)
    return out


@pytest.mark.parametrize("seed", [3, 11, 29])
def test_cached_class_aggregates_equal_full_recompute_on_random_states(tmp_path, seed):
    """Tổng hợp lớp qua bộ đệm = tính lại toàn bộ từ Redis (byte-đối-byte), kể cả sau một lần restart giả
    lập (bộ đệm mới rỗng) và sau batch có bản ghi bị Lua bỏ (bộ đệm phải nạp lại người học đó)."""
    rng = random.Random(seed)
    client = fakeredis.FakeRedis()
    prefix = "u:"
    keys = RedisKeys(prefix)
    skills = ["s1", "s2", "s3"]
    roster = {f"C-{c}": [f"L-{c}{i:015x}" for i in range(6)] for c in range(4)}
    roster["C-0"].append(roster["C-1"][0])  # một người học thuộc hai lớp
    learners = sorted({learner for members in roster.values() for learner in members}) + ["L-no-roster"]
    home = {learner: class_id for class_id, members in roster.items() for learner in members}
    load_roster(client, roster, keys)

    def new_sink():
        return ForeachBatchSink(_cfg(tmp_path), "unit-run", mode="eager", model="bkt", key_prefix=prefix,
                                redis_client=client, window=LatencyWindow())

    sink = new_sink()
    versions: dict[tuple[str, str], int] = {}
    for batch in range(40):
        if batch == 20:
            sink = new_sink()  # restart: bộ đệm rỗng, nạp lại từ Redis
        rows = []
        for learner in rng.sample(learners, rng.randint(1, 8)):
            for skill in rng.sample(skills, rng.randint(1, 2)):
                versions[learner, skill] = versions.get((learner, skill), 0) + 1
                rows.append(_update_row(learner, home.get(learner, "C-0"), skill, versions[learner, skill],
                                        T0 + batch * 7_000 + rng.randint(0, 6_000), rng=rng))
        plan = plan_batch(rows)
        rejected = batch % 9 == 4  # bản lưu được coi là mới hơn: Lua không ghi gì
        if not rejected:
            for row in plan.updates:  # đúng giá trị mà redis_upsert.lua ghi khi chấp nhận
                client.hset(keys.state(row["learner_id"]), row["skill_id"], state_value(row, batch))
        sink._note_written(plan, batch, 0 if rejected else len(plan.updates) + len(plan.progress))
        if rejected:
            assert not any(row["learner_id"] in sink._cache.states for row in plan.updates)
        sink._refresh_classes(plan, batch, cache=sink._cache)
        cached = _class_keys_snapshot(client, prefix)
        sink._refresh_classes(plan, batch)  # đường cũ: đọc lại toàn bộ roster từ Redis
        assert _class_keys_snapshot(client, prefix) == cached, f"batch {batch}"
    # Bộ đệm chỉ đọc Redis cho người học chưa có: sau batch cuối, mọi người học trong roster đã nạp.
    assert len(sink._cache) >= len(roster["C-1"])


def test_learner_state_cache_keeps_only_aggregate_fields_and_skips_unloaded_learners():
    cache = LearnerStateCache()
    value = state_value(_update_row("L-a", "C-1", "s1", 3, T0), 5)
    cache.apply("L-a", "s1", value)  # chưa nạp ⇒ bỏ qua (sẽ nạp đủ từ Redis)
    assert cache.missing(["L-a"]) == ["L-a"]
    cache.load("L-a", {b"s1": value.encode()})
    assert set(cache.states["L-a"]["s1"]) == {"mastered", "stuck", "opportunities", "correct", "p_mastery",
                                              "last_event_time_ms"}
    full = {"L-a": {"s1": json.loads(value)}}
    assert class_aggregates("C-1", ["L-a"], cache.states) == class_aggregates("C-1", ["L-a"], full)
    cache.invalidate(["L-a", "L-unknown"])
    assert len(cache) == 0


def test_parse_alert_state_rejects_bare_or_unknown_values():
    assert parse_alert_state(b"pending:1-0") == ("pending", "1-0")
    assert parse_alert_state("produced:17-3") == ("produced", "17-3")
    for bad in (b"pending", "pending:", "done:1-0", ""):
        with pytest.raises(SinkError):
            parse_alert_state(bad)


# ---------------------------------------------------------------- Parquet theo batch


def test_batch_parquet_overwrites_same_batch_and_removes_empty(tmp_path):
    rows = [{"event_id": "e1", "learner_id": "L", "class_id": "C", "skill_id": SKILL, "event_time_ms": 1,
             "sent_time_ms": 2, "out_of_order": False, "state_version": 1, "batch_id": 5}]
    first = write_batch_parquet(tmp_path, "applied", 5, rows)
    (first.parent / ".part-00000.parquet.999.tmp").write_bytes(b"left over by a killed attempt")
    second = write_batch_parquet(tmp_path, "applied", 5, rows + [{**rows[0], "event_id": "e2"}])
    assert first == second == batch_dir(tmp_path, "applied", 5) / LAKE_FILE
    assert [p.name for p in second.parent.iterdir()] == [LAKE_FILE]
    assert pq.read_table(second).column("event_id").to_pylist() == ["e1", "e2"]
    assert write_batch_parquet(tmp_path, "applied", 5, []) is None
    assert not batch_dir(tmp_path, "applied", 5).exists()
    with pytest.raises(ValueError):
        batch_dir(tmp_path, "state", 1)


# ---------------------------------------------------------------- độ trễ


def test_nearest_rank_and_latency_window_prunes_after_one_minute():
    values = np.arange(1, 101)
    assert nearest_rank(values, 0.95) == 95 and nearest_rank(values, 0.5) == 50
    assert nearest_rank(np.empty(0), 0.95) is None
    now = {"ms": 0}
    window = LatencyWindow(window_ms=60_000, clock_ms=lambda: now["ms"])
    window.add(0, np.array([5000] * 10), np.array([9000] * 10))
    now["ms"] = 30_000
    window.add(30_000, np.array([1000] * 10), np.array([2000] * 10))
    snap = window.snapshot()
    assert snap["count"] == 20 and snap["system_ms"]["p95"] == 5000 and snap["staleness_ms"]["max"] == 9000
    snap = window.snapshot(now_ms=61_000)  # mẫu lúc 0 đã ra khỏi cửa sổ
    assert snap["count"] == 10 and snap["system_ms"]["p95"] == 1000
    window.record_batch(4, {"batch_id": 4})
    assert window.pop_batch(4) == {"batch_id": 4} and window.pop_batch(4) is None


def test_sink_rejects_unknown_mode_and_model(tmp_path):
    with pytest.raises(ValueError):
        ForeachBatchSink(_cfg(tmp_path), "unit-run", mode="fast", model="rule")
    with pytest.raises(ValueError):
        ForeachBatchSink(_cfg(tmp_path), "unit-run", mode="eager", model="dkt")
    with pytest.raises(Exception):
        ForeachBatchSink(_cfg(tmp_path), "../escape", mode="eager", model="rule")


# ---------------------------------------------------------------- DataFrame OUTPUT_SCHEMA → dòng


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(master="local[1]", shuffle_partitions=1, data_root=tmp_path_factory.mktemp("spark"))
    with spark_session(conf) as session:
        yield session


def test_rows_from_output_schema_dataframe_feed_plan_batch(spark):
    _, rows = _rows([_event(i, correct=False) for i in range(1, 11)], model="bkt")
    frame = spark.createDataFrame([tuple(r[c] for c in OUTPUT_COLUMNS) for r in rows], OUTPUT_SCHEMA)
    collected = rows_from_dataframe(frame)
    assert isinstance(collected[0]["applied_event_ids"], list)
    plan = plan_batch(collected)
    expected = plan_batch(rows)
    assert isinstance(plan, BatchPlan)
    assert plan.applied == expected.applied
    assert [a["alert_id"] for a in plan.alerts] == [a["alert_id"] for a in expected.alerts]
    assert plan.updates[0]["p_mastery"] == pytest.approx(expected.updates[0]["p_mastery"])


def test_hint_keys_match_the_hint_service_key_builder():
    from ai_math_tutor.hints.state_client import HintKeys

    for prefix in ("", "it-1:"):
        keys = RedisKeys(prefix)
        hint = HintKeys(keys)
        assert keys.hintctx("L-00000000000000a1", "P-LIN-001", 2) == hint.hintctx("L-00000000000000a1", "P-LIN-001", 2)
        assert keys.hintctx("L-1", "P-1", 3) == f"{prefix}hintctx:L-1:P-1:3"
        assert keys.lasthint("L-00000000000000a1") == hint.lasthint("L-00000000000000a1")
        assert keys.lasthint("L-1") == f"{prefix}lasthint:L-1"
