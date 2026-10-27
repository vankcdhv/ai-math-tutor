"""Pipeline streaming thật: Kafka (make up) → job (parse, dead-letter, watermark, dedup, TWS) → sinks (Redis, lake).

Mỗi test dùng run_id riêng: topic `<run_id>-events|-dl|-alerts`, checkpoint/lake dưới thư mục tạm,
khóa Redis có tiền tố `<run_id>:` (xóa khi xong — không FLUSHDB vì Redis dùng chung). Query dừng ở
ranh giới micro-batch. Kiểm:
- mẫu tuần 1: 19 sự kiện được áp đúng một lần, bản trùng bị bỏ, heartbeat không vào trạng thái,
  bản ghi lỗi vào dead-letter (kể cả sự kiện/heartbeat có event_time đi trước timestamp Kafka quá
  ngưỡng — không đẩy được watermark), drain đưa watermark qua cuối log, trạng thái Redis = oracle;
- sự kiện muộn theo chế độ: eager áp sai thứ tự (gắn cờ), ordered áp đúng thứ tự, W nhỏ thì bị loại,
  none áp sai thứ tự và tự khử trùng;
- một run mô phỏng nhỏ qua bộ phát lại + heartbeat drain: ordered khớp oracle 100% trên các sự kiện
  không bị loại, không sự kiện nào áp hai lần.
"""

from __future__ import annotations

import json
import time
import uuid
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from ai_math_tutor import learner_rules
from ai_math_tutor.config import REPO_ROOT, load_config
from ai_math_tutor.ingestion.producer import ProducerSettings
from ai_math_tutor.ingestion.replay import ReplaySettings, build_heartbeat, run_replay
from ai_math_tutor.ingestion.topics import TopicError, TopicSpec, admin_client, delete_topics, describe_topics, ensure_topics
from ai_math_tutor.simulator import SimulationConfig, run_simulation
from ai_math_tutor.simulator.output import iter_rows
from ai_math_tutor.streaming import job
from ai_math_tutor.streaming.processor import bkt_params_for

pytestmark = pytest.mark.integration

SAMPLE = REPO_ROOT / "data_sample" / "events-week1-sample.jsonl"
PARTITIONS = 3
LATE_HINT_INDEX = 18  # dòng 19: gợi ý của người học B, event 08:01:10, gửi 08:02:45 (muộn 95 s)
WAIT_S = 120


def _ms(text: str) -> int:
    return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)


def _iso(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def _sample_lines() -> list[bytes]:
    return SAMPLE.read_bytes().splitlines()


# ---------------------------------------------------------------- fixtures

@pytest.fixture(scope="module")
def bootstrap() -> str:
    servers = load_config().kafka.bootstrap_servers
    try:
        describe_topics(admin_client(servers), timeout_s=5)
    except TopicError as exc:
        pytest.skip(f"Kafka không chạy ở {servers} (make up): {exc}")
    return servers


@pytest.fixture(scope="module")
def redis_client():
    import redis

    client = redis.Redis.from_url(load_config().redis.url, socket_timeout=10)
    try:
        client.ping()
    except redis.RedisError as exc:
        pytest.skip(f"Redis không sẵn sàng (make up): {exc}")
    yield client
    client.close()


@pytest.fixture(scope="module")
def base_cfg(tmp_path_factory):
    """Cấu hình thật (Kafka/Redis), nhưng checkpoint/lake/results trong thư mục tạm của test."""
    root = tmp_path_factory.mktemp("stream-data")
    cfg = load_config()
    paths = replace(cfg.paths, data_root=root, checkpoints=root / "checkpoints", lake=root / "lake",
                    results=root / "results")
    return replace(cfg, paths=paths)


@pytest.fixture(scope="module")
def spark(base_cfg, bootstrap):
    conf = job.spark_conf(base_cfg, job.JobSettings(run_id="it-stream-session", shuffle_partitions=PARTITIONS))
    # Test cộng numInputRows trên mọi batch: giữ toàn bộ lịch sử progress (mặc định chỉ 100).
    conf["spark.sql.streaming.numRecentProgressUpdates"] = "100000"
    session = job.build_spark(conf)
    yield session
    session.stop()


# ---------------------------------------------------------------- một run pipeline

@dataclass
class Run:
    settings: job.JobSettings
    cfg: Any
    streams: job.JobStreams
    sink: Any
    prefix: str
    rows: list[tuple[int, dict[str, Any]]] = field(default_factory=list)

    @property
    def topic(self) -> str:
        return self.cfg.kafka.topics["learner_events"]

    def check(self) -> None:
        for query in self.streams.queries:
            if query.exception() is not None:
                raise AssertionError(f"query {query.name} lỗi: {query.exception()}")

    def wait(self, predicate: Callable[[], bool], what: str, timeout_s: float = WAIT_S) -> None:
        deadline = time.monotonic() + timeout_s
        while time.monotonic() < deadline:
            self.check()
            if predicate():
                return
            time.sleep(0.25)
        raise AssertionError(f"hết {timeout_s}s khi chờ {what}; progress cuối: {self.streams.state.lastProgress}")

    def progress(self) -> list[dict[str, Any]]:
        return list(self.streams.state.recentProgress)

    def input_rows(self) -> int:
        return sum(p.get("numInputRows", 0) for p in self.progress())

    def watermark_ms(self) -> int:
        marks = [_ms(p["eventTime"]["watermark"]) for p in self.progress() if (p.get("eventTime") or {}).get("watermark")]
        return max(marks, default=0)

    def dropped_by_watermark(self) -> int:
        return sum(op.get("numRowsDroppedByWatermark", 0) for p in self.progress()
                   for op in p.get("stateOperators", []))

    def of_kind(self, kind: str) -> list[dict[str, Any]]:
        return [r for _, r in self.rows if r["kind"] == kind]

    def applied(self) -> list[tuple[str, bool]]:
        return [(i, bool(f)) for r in self.of_kind("update")
                for i, f in zip(r["applied_event_ids"], r["applied_out_of_order"])]

    def redis_state(self, client, learner_id: str) -> dict[str, dict[str, Any]]:
        raw = client.hgetall(f"{self.prefix}state:{learner_id}")
        return {k.decode(): json.loads(v) for k, v in raw.items()}


def _produce(bootstrap: str, topic: str, records: list[tuple[bytes | None, bytes, int | None]]) -> None:
    from confluent_kafka import Producer

    errors: list[str] = []
    producer = Producer({"bootstrap.servers": bootstrap, "enable.idempotence": True, "acks": "all", "linger.ms": 5})
    for key, value, partition in records:
        kwargs: dict[str, Any] = {"key": key, "value": value,
                                  "on_delivery": lambda err, _m: errors.append(str(err)) if err else None}
        if partition is not None:
            kwargs["partition"] = partition
        producer.produce(topic, **kwargs)
    remaining = producer.flush(30)
    assert remaining == 0 and not errors, (remaining, errors)


def _events(lines: list[bytes]) -> list[tuple[bytes, bytes, None]]:
    return [(json.loads(line)["learner_id"].encode(), line, None) for line in lines]


def _heartbeats(event_ms: int) -> list[tuple[bytes, bytes, int]]:
    return [(b"L-0000000000000000", build_heartbeat(event_ms), p) for p in range(PARTITIONS)]


def _delete_prefixed_keys(client, prefix: str) -> int:
    keys = list(client.scan_iter(match=f"{prefix}*", count=500))
    if keys:
        client.delete(*keys)
    return len(keys)


def _wait_listener_quiet(listener, quiet_s: float = 1.5, timeout_s: float = 15.0) -> None:
    """Chờ tới khi listener không ghi thêm bản ghi nào trong `quiet_s` (trước khi xóa khóa Redis của run)."""
    deadline = time.monotonic() + timeout_s
    last, since = listener.records, time.monotonic()
    while time.monotonic() < deadline:
        time.sleep(0.25)
        if listener.records != last:
            last, since = listener.records, time.monotonic()
        elif time.monotonic() - since >= quiet_s:
            return


@contextmanager
def pipeline(spark, base_cfg, bootstrap, redis_client, **settings_kwargs) -> Iterator[Run]:
    from ai_math_tutor.streaming.listener import attach_listener
    from ai_math_tutor.streaming.sinks import make_foreach_batch, rows_from_dataframe

    run_id = f"it-stream-{uuid.uuid4().hex[:8]}"
    topics = {"in": f"{run_id}-events", "dl": f"{run_id}-dl", "alerts": f"{run_id}-alerts"}
    ensure_topics(bootstrap, [TopicSpec(topics["in"], PARTITIONS), TopicSpec(topics["dl"], 1),
                              TopicSpec(topics["alerts"], 1)])
    settings = job.JobSettings(run_id=run_id, input_topic=topics["in"], dead_letter_topic=topics["dl"],
                               alerts_topic=topics["alerts"], **settings_kwargs)
    cfg = job.job_config(base_cfg, settings)
    prefix = f"{run_id}:"
    sink = make_foreach_batch(cfg, run_id, mode=settings.mode, model=settings.learner_model, key_prefix=prefix)
    listener = attach_listener(spark, cfg, run_id, key_prefix=prefix)
    collected: list[tuple[int, dict[str, Any]]] = []

    def foreach_batch(batch_df, batch_id: int) -> None:
        rows = rows_from_dataframe(batch_df)
        collected.extend((batch_id, row) for row in rows)
        sink.process_rows(rows, int(batch_id))

    streams = job.start_streams(spark, cfg, settings, foreach_batch)
    run = Run(settings, cfg, streams, sink, prefix, collected)
    try:
        yield run
    finally:
        clean = job.stop_streams(streams)
        spark.streams.removeListener(listener)
        _wait_listener_quiet(listener)  # bus sự kiện bất đồng bộ: progress cuối có thể tới sau stop()
        listener.close()  # đóng client Redis/Kafka admin của listener; sự kiện muộn hơn bị bỏ qua
        sink.close()
        delete_topics(bootstrap, list(topics.values()))
        _delete_prefixed_keys(redis_client, prefix)
        assert all(clean.values()), f"query không dừng được ở ranh giới batch: {clean}"


def _oracle(events: list[dict[str, Any]], model: str = "rule") -> dict[tuple[str, str], dict[str, Any]]:
    """Áp learner_rules (+ BKT theo `bkt_params_for`) đúng thứ tự trên tập đã khử trùng."""
    unique = {e["event_id"]: e for e in events}
    ordered = sorted(unique.values(), key=lambda e: (_ms(e["event_time"]), e["session_seq"], e["event_id"]))
    profiles: dict[tuple[str, str], learner_rules.SkillProfile] = {}
    p_mastery: dict[tuple[str, str], float] = {}
    for e in ordered:
        observation = learner_rules.observation_for(e["event_type"], e["payload"])
        for skill in e["skill_ids"]:
            key = (e["learner_id"], skill)
            profile = profiles.get(key, learner_rules.EMPTY_PROFILE)
            params = bkt_params_for(skill)
            p_mastery.setdefault(key, params.p_init)
            if observation is not None:
                profile = learner_rules.update(profile, observation)
                if not observation.is_hint:
                    p_mastery[key] = params.update(p_mastery[key], bool(observation.correct))
            profiles[key] = profile
    return {key: {"opportunities": p.opportunities, "correct": p.correct, "consec_wrong": p.consec_wrong,
                  "streak": p.streak, "mastered": p.mastered, "stuck": learner_rules.is_wheel_spinning(p),
                  **({"p_mastery": p_mastery[key]} if model == "bkt" else {})}
            for key, p in profiles.items()}


def _final_states(run: Run) -> dict[tuple[str, str], dict[str, Any]]:
    """Trạng thái cuối theo đầu ra processor (dòng update có state_version lớn nhất)."""
    latest: dict[tuple[str, str], dict[str, Any]] = {}
    for row in run.of_kind("update"):
        key = (row["learner_id"], row["skill_id"])
        if key not in latest or row["state_version"] > latest[key]["state_version"]:
            latest[key] = row
    return latest


def _matches_oracle(run: Run, oracle: dict, model: str = "rule") -> list[tuple]:
    fields = ("opportunities", "correct", "consec_wrong", "streak", "mastered", "stuck")
    final = _final_states(run)
    diffs = []
    for key in sorted(set(final) | set(oracle)):
        got, want = final.get(key), oracle.get(key)
        if got is None or want is None or any(got[f] != want[f] for f in fields):
            diffs.append((key, got and {f: got[f] for f in fields}, want))
        elif model == "bkt" and abs(got["p_mastery"] - want["p_mastery"]) > 1e-9:
            diffs.append((key, got["p_mastery"], want["p_mastery"]))
    return diffs


# ---------------------------------------------------------------- mẫu tuần 1

def test_week1_sample_through_kafka_with_real_sinks(spark, base_cfg, bootstrap, redis_client):
    lines = _sample_lines()
    events = [json.loads(line) for line in lines]
    end_of_log = max(_ms(e["event_time"]) for e in events)
    # event_time đi trước timestamp Kafka (≈ lúc gửi) 1 h: quá 120 s (sự kiện học tập) và quá W + 60 s
    # (heartbeat) ⇒ dead-letter; nếu lọt vào, heartbeat này đẩy watermark lên tương lai và drain sai.
    far_future = _iso(int(time.time() * 1000) + 3_600_000)
    invalid = [(None, b"{not json", None),
               (b"L-15c23bacf714b792", json.dumps({**events[0], "event_type": "logout"}).encode(), None),
               (b"L-15c23bacf714b792", json.dumps({**events[0], "event_id": str(uuid.uuid4()),
                                                    "event_time": far_future, "sent_time": far_future}).encode(), None),
               (b"L-0000000000000000", build_heartbeat(_ms(far_future)), 0)]
    with pipeline(spark, base_cfg, bootstrap, redis_client, mode="eager", learner_model="bkt") as run:
        from ai_math_tutor.streaming.sinks import RedisKeys, load_roster

        load_roster(redis_client, {"C-10A1-2026": sorted({e["learner_id"] for e in events})}, RedisKeys(run.prefix))
        _produce(bootstrap, run.topic, _events(lines) + invalid)
        run.wait(lambda: run.input_rows() >= len(lines) + len(invalid), "24 bản ghi vào")
        run.wait(lambda: len(run.applied()) >= 19, "19 sự kiện được áp")
        heartbeat_ms = end_of_log + run.settings.watermark_ms + 1000
        _produce(bootstrap, run.topic, _heartbeats(heartbeat_ms))
        run.wait(lambda: run.watermark_ms() >= end_of_log, "watermark qua cuối log (drain)")
        run.wait(lambda: run.streams.dead_letter.lastProgress is not None
                 and sum(p["numInputRows"] for p in run.streams.dead_letter.recentProgress)
                 >= len(lines) + len(invalid),
                 "query dead-letter đọc hết")

        applied = run.applied()
        assert len(applied) == 19 == len({i for i, _ in applied})
        assert {i for i, _ in applied} == {e["event_id"] for e in events}
        assert run.dropped_by_watermark() == 0
        dedup_ops = [op for p in run.progress() for op in p.get("stateOperators", [])
                     if op.get("operatorName") == "dedupeWithinWatermark"]
        assert sum(int(op["customMetrics"]["numDroppedDuplicateRows"]) for op in dedup_ops) == 1
        learners = {r["learner_id"] for _, r in run.rows}
        assert learners == {e["learner_id"] for e in events}  # heartbeat không thành người học
        assert run.watermark_ms() == heartbeat_ms - run.settings.watermark_ms
        assert not _matches_oracle(run, _oracle(events, "bkt"), "bkt")
        # Redis (Lua upsert theo state_version) giữ đúng trạng thái cuối.
        final = _final_states(run)
        for (learner, skill), row in final.items():
            stored = run.redis_state(redis_client, learner)[skill]
            assert (stored["state_version"], stored["opportunities"], stored["mastered"]) == (
                row["state_version"], row["opportunities"], row["mastered"])
        assert not redis_client.exists(f"{run.prefix}state:L-0000000000000000")
        progress = redis_client.hgetall(f"{run.prefix}progress:L-3dc9002e320b29be:P-QUAD-001")
        assert progress[b"wrong_answers"] == b"1" and progress[b"completed"] == b"1"
        assert redis_client.exists(f"{run.prefix}agg:class:C-10A1-2026:skill:quadratic_equation.factorization")
        # Lake: applied theo batch, đủ 19 sự kiện, không trùng.
        lake = run.cfg.paths.lake_dir(run.settings.run_id) / "applied"
        lake_ids = [i for f in sorted(lake.rglob("*.parquet")) for i in pq.read_table(f)["event_id"].to_pylist()]
        assert sorted(lake_ids) == sorted(i for i, _ in applied)
        progress_log = run.cfg.paths.results / run.settings.run_id / "progress.jsonl"
        assert progress_log.is_file() and progress_log.stat().st_size > 0

        from confluent_kafka import Consumer

        consumer = Consumer({"bootstrap.servers": bootstrap, "group.id": f"{run.settings.run_id}-check",
                             "auto.offset.reset": "earliest", "enable.auto.commit": False})
        consumer.subscribe([run.cfg.kafka.topics["dead_letter"]])
        reasons: list[str] = []
        deadline = time.monotonic() + 30
        while len(reasons) < len(invalid) and time.monotonic() < deadline:
            message = consumer.poll(1.0)
            if message is not None and message.error() is None:
                reasons.append(json.loads(message.value())["reason"])
        consumer.close()
        assert sorted(reasons) == ["event_type", "future_event_time", "future_event_time", "malformed_json"]


@pytest.mark.parametrize(("mode", "watermark_s", "late_applied", "late_out_of_order"), [
    ("eager", 120, True, True),     # trong ngưỡng W, đến sau sự kiện mới hơn ⇒ áp sai thứ tự, gắn cờ
    ("ordered", 120, True, False),  # trong ngưỡng W ⇒ đệm tới khi watermark qua ⇒ áp đúng thứ tự
    ("eager", 30, False, None),     # cũ hơn watermark ⇒ bị loại trước processor
    ("none", 30, True, True),       # không watermark ⇒ luôn áp ngay; bản trùng bị khử trong state
])
def test_late_event_handling_by_mode(spark, base_cfg, bootstrap, redis_client, mode, watermark_s, late_applied,
                                     late_out_of_order):
    lines = _sample_lines()
    events = [json.loads(line) for line in lines]
    late = events[LATE_HINT_INDEX]
    first = [line for i, line in enumerate(lines) if i < LATE_HINT_INDEX]  # max event_time 08:02:40
    second = [lines[LATE_HINT_INDEX], lines[-1]]
    first_max = max(_ms(json.loads(line)["event_time"]) for line in first)
    with pipeline(spark, base_cfg, bootstrap, redis_client, mode=mode, watermark_ms=watermark_s * 1000) as run:
        _produce(bootstrap, run.topic, _events(first))
        run.wait(lambda: run.input_rows() >= len(first), "phần đầu được đọc")
        if mode != "none":
            run.wait(lambda: run.watermark_ms() >= first_max - watermark_s * 1000, "watermark sau phần đầu")
        else:
            run.wait(lambda: len(run.applied()) >= 17, "phần đầu được áp")
        _produce(bootstrap, run.topic, _events(second))
        run.wait(lambda: run.input_rows() >= len(lines), "phần sau được đọc")
        end_of_log = max(_ms(e["event_time"]) for e in events)
        if mode == "none":
            run.wait(lambda: len(run.applied()) >= 19, "19 sự kiện được áp")
        else:
            _produce(bootstrap, run.topic, _heartbeats(end_of_log + watermark_s * 1000 + 1000))
            run.wait(lambda: run.watermark_ms() >= end_of_log, "drain")
            run.wait(lambda: len(run.applied()) >= (19 if late_applied else 18), "sự kiện được áp sau drain")

        applied = dict(run.applied())
        assert len(run.applied()) == len(applied)  # không sự kiện nào áp hai lần
        assert (late["event_id"] in applied) is late_applied
        if late_applied:
            assert applied[late["event_id"]] is late_out_of_order
        else:
            assert run.dropped_by_watermark() >= 1
        assert len(applied) == (19 if late_applied else 18)
        kept = [e for e in events if e["event_id"] in applied]
        diffs = _matches_oracle(run, _oracle(kept))
        # Gợi ý không phải cơ hội nên áp sai thứ tự không đổi bộ đếm của mẫu; mọi chế độ khớp oracle.
        assert not diffs, diffs
        if mode == "ordered":
            assert not any(flag for flag in applied.values())


# ---------------------------------------------------------------- run mô phỏng nhỏ + bộ phát lại + drain

@pytest.mark.parametrize("mode", ["ordered", "eager"])
def test_small_simulation_replay_with_heartbeat_drain(spark, base_cfg, bootstrap, redis_client, tmp_path, mode):
    watermark_s = 60
    manifest = run_simulation(SimulationConfig(learners=60, horizon_s=40, seed=17, out_dir=tmp_path / "sim",
                                               target_rate=30.0, late_profile="high", duplicate_rate=0.05))
    events_path = tmp_path / "sim" / "events.parquet"
    duration_s = 45
    rows = [r for r in iter_rows(events_path) if r["replay_offset_ms"] < duration_s * 1000]
    with pipeline(spark, base_cfg, bootstrap, redis_client, mode=mode, watermark_ms=watermark_s * 1000) as run:
        started: list[int] = []

        def drained() -> bool:
            return bool(started) and run.watermark_ms() >= started[0] + max(r["event_offset_ms"] for r in rows)

        settings = ReplaySettings(events_path, run.topic, ProducerSettings(bootstrap), processes=1,
                                  duration_s=duration_s, watermark_s=watermark_s, heartbeat_s=90)
        result = run_replay(settings, heartbeat_until=drained, on_started=started.append)
        assert result["delivered_all"], result
        assert result["heartbeat"]["until_satisfied"], result["heartbeat"]
        run.wait(lambda: run.input_rows() >= result["sent"] + result["heartbeat"]["sent"], "đọc hết topic")

        sent_unique = {json.loads(r["value"])["event_id"]: json.loads(r["value"]) for r in rows}
        applied = run.applied()
        applied_ids = {i for i, _ in applied}
        assert len(applied) == len(applied_ids), "có sự kiện bị áp hai lần"
        assert applied_ids <= set(sent_unique)
        assert result["window"]["duplicate_deliveries"] > 0
        dropped = len(sent_unique) - len(applied_ids)
        # Sự kiện thiếu chỉ có thể là bị loại theo watermark (đếm ở dedupeWithinWatermark, gồm cả bản trùng muộn).
        assert dropped <= run.dropped_by_watermark()
        assert len(applied_ids) >= 0.9 * len(sent_unique)
        kept = [e for event_id, e in sent_unique.items() if event_id in applied_ids]
        diffs = _matches_oracle(run, _oracle(kept))
        out_of_order = sum(1 for _, flag in applied if flag)
        print(json.dumps({"mode": mode, "unique_sent": len(sent_unique), "applied": len(applied_ids),
                          "dropped": dropped, "dropped_by_watermark_rows": run.dropped_by_watermark(),
                          "out_of_order": out_of_order, "oracle_mismatch": len(diffs),
                          "state_keys": len(_final_states(run)), "batches": len(run.progress()),
                          "manifest_unique": manifest["counts"]["unique_events"]}))
        if mode == "ordered":
            assert out_of_order == 0
            assert not diffs, diffs[:5]  # ordered = 100% đúng so oracle trên sự kiện không bị loại
        else:
            assert out_of_order > 0  # hồ sơ trễ high ⇒ có sự kiện đến sau sự kiện mới hơn
