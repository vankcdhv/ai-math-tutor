"""Lua idempotent trên Redis thật (make up): upsert theo (batch_id, state_version), cảnh báo đúng một lần,
foreachBatch chạy lại, bộ đệm tổng hợp lớp.

fakeredis không chạy được EVAL khi thiếu `lupa`, nên các test này dùng Redis của Compose (integration).
Redis dùng chung với các thành phần khác: mỗi test đặt tiền tố khóa riêng `it-<uuid>:` và chỉ xóa
khóa của mình (không FLUSHDB). Test foreachBatch cũng tạo topic Kafka `alerts` riêng rồi xóa.
"""

from __future__ import annotations

import json
import random
import uuid
from dataclasses import replace

import pyarrow.parquet as pq
import pytest

from ai_math_tutor.config import load_config
from ai_math_tutor.streaming.processor import Event, MemoryLearnerStore
from ai_math_tutor.streaming.sinks import (
    ALERT_LUA_PATH,
    LAKE_FILE,
    UPSERT_LUA_PATH,
    ForeachBatchSink,
    LatencyWindow,
    RedisKeys,
    batch_dir,
    load_roster,
    plan_batch,
)

pytestmark = pytest.mark.integration

T0 = 1_793_577_600_000
SKILL = "linear_equation.isolate_variable"
LEARNER = "L-00000000000000b1"


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def client(cfg):
    import redis

    try:
        connection = redis.Redis.from_url(cfg.redis.url, socket_timeout=10)
        connection.ping()
    except Exception as exc:  # stack chưa up hoặc thiếu REDIS_PASSWORD
        pytest.skip(f"Redis không sẵn sàng ({cfg.redis.redacted_url}): {exc}")
    yield connection
    connection.close()


@pytest.fixture
def prefix(client):
    value = f"it-{uuid.uuid4().hex[:10]}:"
    yield value
    keys = list(client.scan_iter(match=f"{value}*", count=1000))
    if keys:
        client.delete(*keys)


def _snapshot(client, prefix):
    """Nội dung mọi khóa của test (theo kiểu dữ liệu) để so trước/sau khi chạy lại."""
    out = {}
    for key in sorted(client.scan_iter(match=f"{prefix}*", count=1000)):
        kind = client.type(key)
        if kind == b"hash":
            out[key] = client.hgetall(key)
        elif kind == b"set":
            out[key] = client.smembers(key)
        elif kind == b"stream":
            out[key] = client.xrange(key)
        else:
            out[key] = client.get(key)
    return out


# ---------------------------------------------------------------- redis_upsert.lua


def _value(batch_id, version, opportunities):
    return json.dumps({"batch_id": batch_id, "state_version": version, "opportunities": opportunities})


def _f(field, batch_id, version, opportunities):
    return ["f", field, str(batch_id), str(version), _value(batch_id, version, opportunities)]


def _h(batch_id, version, wrong_answers):
    return ["h", str(batch_id), str(version), "3", "batch_id", str(batch_id), "state_version", str(version),
            "wrong_answers", str(wrong_answers)]


def test_upsert_field_writes_only_newer_batch_and_version(client, prefix):
    upsert = client.register_script(UPSERT_LUA_PATH.read_text(encoding="utf-8"))
    key = f"{prefix}state:{LEARNER}"

    assert upsert(keys=[key, key], args=[*_f("s1", 3, 2, 2), *_f("s2", 3, 1, 1)]) == 2
    # Chạy lại đúng lời gọi đó (batch replay) ⇒ không ghi gì.
    assert upsert(keys=[key, key], args=[*_f("s1", 3, 2, 2), *_f("s2", 3, 1, 1)]) == 0
    # Cùng batch, version cũ hơn ⇒ bỏ; batch cũ hơn (dù version lớn) ⇒ bỏ.
    assert upsert(keys=[key], args=_f("s1", 3, 1, 99)) == 0
    assert upsert(keys=[key], args=_f("s1", 2, 50, 99)) == 0
    assert json.loads(client.hget(key, "s1"))["opportunities"] == 2
    # Cùng batch version mới hơn, hoặc batch sau ⇒ ghi.
    assert upsert(keys=[key], args=_f("s1", 3, 3, 3)) == 1
    assert upsert(keys=[key], args=_f("s1", 4, 1, 4)) == 1
    assert json.loads(client.hget(key, "s1"))["opportunities"] == 4
    # Giá trị hỏng (không phải JSON) coi như (-1, -1) ⇒ được thay.
    client.hset(key, "s3", "not-json")
    assert upsert(keys=[key], args=_f("s3", 0, 0, 0)) == 1


def test_post_expiry_versions_restarting_at_one_are_written_in_later_batches(client, prefix):
    """State hết hạn theo event time ⇒ version của người học quay về 1. Bản đang lưu có version 40
    (batch 5); các bản 1..15 sau hết hạn thuộc batch sau nên phải được ghi, còn chạy lại batch thì không."""
    upsert = client.register_script(UPSERT_LUA_PATH.read_text(encoding="utf-8"))
    state_key = f"{prefix}state:{LEARNER}"
    progress_key = f"{prefix}progress:{LEARNER}:P-LIN-001"
    assert upsert(keys=[state_key, progress_key], args=[*_f(SKILL, 5, 40, 40), *_h(5, 40, 7)]) == 2

    # Người học quay lại: các bản 1..15 rải trên các batch 9, 10, ... (mỗi batch vài sự kiện).
    for version in range(1, 16):
        batch_id = 9 + (version - 1) // 4
        assert upsert(keys=[state_key, progress_key],
                      args=[*_f(SKILL, batch_id, version, version), *_h(batch_id, version, 0)]) == 2, version
        assert json.loads(client.hget(state_key, SKILL))["state_version"] == version
    last_batch = 9 + 14 // 4
    assert json.loads(client.hget(state_key, SKILL))["opportunities"] == 15
    assert client.hget(progress_key, "state_version") == b"15"
    assert client.hget(progress_key, "batch_id") == str(last_batch).encode()

    # Batch cuối chạy lại (cùng batch_id, cùng version) ⇒ không đổi gì.
    assert upsert(keys=[state_key, progress_key],
                  args=[*_f(SKILL, last_batch, 15, 15), *_h(last_batch, 15, 0)]) == 0
    # Bản trước hết hạn (version cao nhưng batch cũ) tới muộn ⇒ bị bỏ.
    assert upsert(keys=[state_key, progress_key], args=[*_f(SKILL, 5, 40, 40), *_h(5, 40, 7)]) == 0
    assert json.loads(client.hget(state_key, SKILL))["opportunities"] == 15


def test_upsert_hash_compares_batch_and_state_version_fields(client, prefix):
    upsert = client.register_script(UPSERT_LUA_PATH.read_text(encoding="utf-8"))
    key = f"{prefix}progress:{LEARNER}:P-LIN-001"
    assert upsert(keys=[key], args=_h(2, 4, 1)) == 1
    assert upsert(keys=[key], args=_h(2, 4, 1)) == 0
    assert upsert(keys=[key], args=_h(2, 3, 0)) == 0
    assert upsert(keys=[key], args=_h(1, 9, 0)) == 0
    assert client.hgetall(key) == {b"batch_id": b"2", b"state_version": b"4", b"wrong_answers": b"1"}
    assert upsert(keys=[key], args=_h(3, 1, 2)) == 1
    assert client.hget(key, "wrong_answers") == b"2"


def test_upsert_rejects_malformed_arguments_before_writing_anything(client, prefix):
    import redis

    upsert = client.register_script(UPSERT_LUA_PATH.read_text(encoding="utf-8"))
    key = f"{prefix}state:{LEARNER}"
    hkey = f"{prefix}progress:{LEARNER}:P-LIN-001"
    good = _f("s1", 1, 1, 1)
    bad_calls = [
        ("loại bản ghi", [key], ["x", "s1", "1", "1", "{}"]),
        ("không khớp", [key], [*good, "extra"]),
        ("thiếu field/batch_id", [key], ["f", "s1", "one", "1", "{}"]),
        ("thiếu field/batch_id", [key], ["f", "s1", "1", "-2", "{}"]),
        ("thiếu batch_id/version", [hkey], ["h", "1", "1.5", "1", "state_version", "1"]),
        ("khớp tham số", [hkey], ["h", "1", "1", "1", "state_version", "1"]),  # thiếu batch_id
        ("khớp tham số", [hkey], ["h", "1", "1", "2", "batch_id", "2", "state_version", "1"]),
    ]
    for message, keys, args in bad_calls:
        # Bản ghi hợp lệ đứng trước bản ghi sai cũng không được ghi.
        with pytest.raises(redis.exceptions.ResponseError, match=message):
            upsert(keys=[key, *keys], args=[*good, *args])
    assert not client.exists(key) and not client.exists(hkey)


# ---------------------------------------------------------------- redis_alert.lua


def test_alert_xadd_exactly_once(client, prefix):
    alert = client.register_script(ALERT_LUA_PATH.read_text(encoding="utf-8"))
    keys = [f"{prefix}alert:abc", f"{prefix}alerts:stream"]
    created, value = alert(keys=keys, args=["1000", "alert_id", "abc", "alert_type", "stuck"])
    assert created == 1 and value.startswith(b"pending:")
    created_again, value_again = alert(keys=keys, args=["1000", "alert_id", "abc", "alert_type", "stuck"])
    assert created_again == 0 and value_again == value
    entries = client.xrange(keys[1])
    assert len(entries) == 1 and entries[0][1][b"alert_id"] == b"abc"
    assert value == b"pending:" + entries[0][0]
    client.set(keys[0], b"produced:" + entries[0][0])
    assert alert(keys=keys, args=["1000", "alert_id", "abc", "alert_type", "stuck"])[0] == 0
    assert len(client.xrange(keys[1])) == 1


def test_failed_xadd_leaves_no_alert_key_and_replay_recovers(client, prefix):
    import redis

    alert = client.register_script(ALERT_LUA_PATH.read_text(encoding="utf-8"))
    keys = [f"{prefix}alert:xyz", f"{prefix}alerts:stream"]
    args = ["1000", "alert_id", "xyz", "alert_type", "stuck"]
    for maxlen in ("many", "0", "1.5"):
        with pytest.raises(redis.exceptions.ResponseError, match="MAXLEN"):
            alert(keys=keys, args=[maxlen, *args[1:]])
    client.set(keys[1], "not a stream")  # XADD sẽ lỗi WRONGTYPE
    with pytest.raises(redis.exceptions.ResponseError):
        alert(keys=keys, args=args)
    assert not client.exists(keys[0])  # không có giá trị 'pending' mồ côi
    client.delete(keys[1])
    created, value = alert(keys=keys, args=args)  # batch chạy lại: phát đúng một lần
    assert created == 1 and len(client.xrange(keys[1])) == 1


def test_legacy_bare_pending_value_is_repaired_instead_of_crash_looping(client, prefix, alerts_topic, cfg, tmp_path):
    """Bản script cũ ghi `SET alert:{id} pending NX` trước XADD; XADD lỗi để lại `pending` trần không có mục
    stream. Batch chạy lại phải phát cảnh báo (một mục stream, một tin Kafka) thay vì lỗi mãi."""
    keys = RedisKeys(prefix)
    rows = _batch_rows()
    stuck_id = next(r["alert_id"] for r in rows if r["kind"] == "alert" and r["alert_type"] == "stuck")
    client.set(keys.alert(stuck_id), "pending")
    run_cfg = replace(cfg, paths=replace(cfg.paths, lake=tmp_path / "lake"))
    sink = ForeachBatchSink(run_cfg, "it-legacy", mode="eager", model="bkt", key_prefix=prefix,
                            alerts_topic=alerts_topic, redis_client=client, window=LatencyWindow())
    try:
        result = sink.process_rows(rows, batch_id=0)
    finally:
        sink.close()
    assert result.alerts_created == 2 and result.alerts_produced == 2
    status, stream_id = client.get(keys.alert(stuck_id)).decode().split(":", 1)
    assert status == "produced"
    entries = client.xrange(keys.alerts_stream)
    assert len(entries) == 2 and stream_id in {e[0].decode() for e in entries}


# ---------------------------------------------------------------- foreachBatch chạy lại


@pytest.fixture
def alerts_topic(cfg):
    from ai_math_tutor.ingestion.topics import TopicError, TopicSpec, delete_topics, ensure_topics

    name = f"it-alerts-{uuid.uuid4().hex[:10]}"
    try:
        ensure_topics(cfg.kafka.bootstrap_servers, [TopicSpec(name, 1)], timeout_s=30)
    except TopicError as exc:
        pytest.skip(f"Kafka không sẵn sàng ({cfg.kafka.bootstrap_servers}): {exc}")
    yield name
    delete_topics(cfg.kafka.bootstrap_servers, [name])


def _topic_messages(cfg, topic):
    from ai_math_tutor.ingestion.verify_topic import consume_topic

    return [json.loads(value) for _, _, _, value in consume_topic(cfg.kafka.bootstrap_servers, topic, timeout_s=30)]


def _batch_rows():
    store = MemoryLearnerStore(LEARNER)
    events = [Event(f"e{i:03d}", "submit_step", T0 + i * 1000, T0 + i * 1000 + 50, i, "C-IT-1", "P-LIN-001",
                    (SKILL,), is_correct=False, step_index=1) for i in range(1, 11)]
    return store.run(events, "eager", "bkt", watermark_ms=None)


def test_foreach_batch_replay_changes_nothing(cfg, client, prefix, alerts_topic, tmp_path):
    keys = RedisKeys(prefix)
    load_roster(client, {"C-IT-1": [LEARNER, "L-00000000000000b2"]}, keys)
    run_cfg = replace(cfg, paths=replace(cfg.paths, lake=tmp_path / "lake"))
    sink = ForeachBatchSink(run_cfg, "it-sink", mode="eager", model="bkt", key_prefix=prefix,
                            alerts_topic=alerts_topic, redis_client=client, window=LatencyWindow())
    rows = _batch_rows()
    try:
        first = sink.process_rows(rows, batch_id=0)
        assert first.updates_written == 2  # state (1 kỹ năng) + progress (1 bài)
        assert first.alerts == 2 and first.alerts_created == 2 and first.alerts_produced == 2
        assert first.applied_events == 10 and first.classes_without_roster == 0
        state = json.loads(client.hget(keys.state(LEARNER), SKILL))
        assert state["opportunities"] == 10 and state["stuck"] is True and state["state_version"] == 10
        assert 0 < state["p_mastery"] < 1
        assert client.hget(keys.agg("C-IT-1", SKILL), "stuck") == b"1"
        before = _snapshot(client, prefix)

        replayed = sink.process_rows(rows, batch_id=0)
        assert replayed.updates_written == 0 and replayed.alerts_created == 0 and replayed.alerts_produced == 0
        assert _snapshot(client, prefix) == before
    finally:
        sink.close()

    assert len(client.xrange(keys.alerts_stream)) == 2
    messages = _topic_messages(cfg, alerts_topic)
    assert sorted(m["alert_type"] for m in messages) == ["consec_wrong", "stuck"]
    for kind, expected in (("applied", 10), ("latency", 10), ("alerts", 2)):
        directory = batch_dir(run_cfg.paths.lake_dir("it-sink"), kind, 0)
        assert [p.name for p in directory.iterdir()] == [LAKE_FILE]
        assert pq.read_table(directory / LAKE_FILE).num_rows == expected
    latency = pq.read_table(batch_dir(run_cfg.paths.lake_dir("it-sink"), "latency", 0) / LAKE_FILE).to_pylist()
    assert all(r["system_latency_ms"] == r["staleness_ms"] - 50 for r in latency)
    assert all(r["visible_ms"] == replayed.visible_ms for r in latency)


def test_pending_alert_is_produced_again_after_crash(cfg, client, prefix, alerts_topic, tmp_path):
    """Chết sau XADD nhưng trước khi Kafka nhận ⇒ khóa còn 'pending' ⇒ batch chạy lại gửi lại (cùng alert_id)."""
    keys = RedisKeys(prefix)
    run_cfg = replace(cfg, paths=replace(cfg.paths, lake=tmp_path / "lake"))
    sink = ForeachBatchSink(run_cfg, "it-pending", mode="eager", model="bkt", key_prefix=prefix,
                            alerts_topic=alerts_topic, redis_client=client, window=LatencyWindow())
    rows = _batch_rows()
    try:
        sink.process_rows(rows, batch_id=0)
        stuck_id = next(r["alert_id"] for r in rows if r["kind"] == "alert" and r["alert_type"] == "stuck")
        status, stream_id = client.get(keys.alert(stuck_id)).decode().split(":", 1)
        assert status == "produced"
        client.set(keys.alert(stuck_id), f"pending:{stream_id}")
        result = sink.process_rows(rows, batch_id=0)
        assert result.alerts_created == 0 and result.alerts_produced == 1
        assert client.get(keys.alert(stuck_id)).decode() == f"produced:{stream_id}"
    finally:
        sink.close()
    assert len(client.xrange(keys.alerts_stream)) == 2
    messages = _topic_messages(cfg, alerts_topic)
    assert [m["alert_id"] for m in messages].count(stuck_id) == 2
    assert len({m["alert_id"] for m in messages}) == 2
    assert {m["stream_id"] for m in messages if m["alert_id"] == stuck_id} == {stream_id}


# ---------------------------------------------------------------- bộ đệm tổng hợp lớp với Lua thật


def _class_keys_snapshot(client, prefix):
    out = {}
    for key in sorted(client.scan_iter(match=f"{prefix}*", count=1000)):
        name = key.decode()
        if name.startswith((f"{prefix}agg:", f"{prefix}trend:")):
            out[key] = client.hgetall(key)
        elif name.startswith(f"{prefix}stuck:set:"):
            out[key] = client.smembers(key)
    return out


def test_cached_class_aggregates_match_full_recompute_through_real_upserts(cfg, client, prefix, tmp_path):
    """Dòng processor thật (nhiều lớp, sự kiện ngẫu nhiên) → process_rows với Lua thật: sau mỗi batch, tổng
    hợp lớp qua bộ đệm bằng byte-đối-byte kết quả đọc lại toàn bộ roster từ Redis. Giữa chừng tạo sink mới
    (restart: bộ đệm rỗng) và chạy lại một batch (Lua bỏ mọi bản ghi)."""
    rng = random.Random(5)
    keys = RedisKeys(prefix)
    roster = {f"C-IT-{c}": [f"L-{c:02d}{i:014x}" for i in range(5)] for c in range(3)}
    load_roster(client, roster, keys)
    stores = {learner: (class_id, MemoryLearnerStore(learner)) for class_id, members in roster.items()
              for learner in members}
    skills = [SKILL, "linear_equation.distribute", "quadratic_equation.factorization"]
    run_cfg = replace(cfg, paths=replace(cfg.paths, lake=tmp_path / "lake"))

    def new_sink():
        return ForeachBatchSink(run_cfg, "it-cache", mode="eager", model="bkt", key_prefix=prefix,
                                redis_client=client, window=LatencyWindow())

    sink = new_sink()
    seq = 0
    try:
        for batch in range(12):
            if batch == 6:
                sink.close()
                sink = new_sink()
            rows = []
            for learner in rng.sample(sorted(stores), rng.randint(1, 6)):
                class_id, store = stores[learner]
                events = []
                for _ in range(rng.randint(1, 4)):
                    seq += 1
                    events.append(Event(f"e{seq:05d}", "submit_step", T0 + seq * 500, T0 + seq * 500 + 30, seq,
                                        class_id, "P-LIN-001", (rng.choice(skills),),
                                        is_correct=rng.random() < 0.5, step_index=1))
                rows += [r for r in store.run(events, "eager", "bkt", watermark_ms=None) if r["kind"] != "alert"]
            result = sink.process_rows(rows, batch_id=batch)
            assert result.updates_written == len(plan_batch(rows).updates) + len(plan_batch(rows).progress)
            cached = _class_keys_snapshot(client, prefix)
            sink._refresh_classes(plan_batch(rows), batch)  # đường cũ: đọc lại toàn bộ roster
            assert _class_keys_snapshot(client, prefix) == cached, f"batch {batch}"
            if batch == 8:  # chạy lại batch vừa xong: Lua bỏ mọi bản ghi, bộ đệm nạp lại người học đó
                replayed = sink.process_rows(rows, batch_id=batch)
                assert replayed.updates_written == 0
                assert _class_keys_snapshot(client, prefix) == cached
    finally:
        sink.close()
