"""Dashboard trên Redis/Kafka/Spark thật (make up), mỗi test một tiền tố khóa và topic riêng, chỉ xóa của mình.

- Trạng thái đổi qua đúng đường ghi của pipeline (processor `apply_events` → `ForeachBatchSink.process_rows`
  với Lua thật, cảnh báo vào topic riêng) → lần đọc kế tiếp của dashboard thấy ngay; ngưỡng k giữ nguyên.
- 10.000 người học ghi qua mã tổng hợp của pipeline: một lần làm mới chế độ lớp đọc ≤ 100 ms.
- Bảng Pipeline khớp `query.lastProgress` của một query Spark thật có listener của pipeline.
"""

from __future__ import annotations

import json
import random
import statistics
import time
import uuid

import pytest
import redis

from ai_math_tutor.config import load_config
from ai_math_tutor.dashboard import queries
from ai_math_tutor.ingestion.topics import TopicError, TopicSpec, admin_client, delete_topics, describe_topics, ensure_topics
from ai_math_tutor.streaming.listener import iso_to_ms
from ai_math_tutor.streaming.processor import Event, MemoryLearnerStore
from ai_math_tutor.streaming.sinks import BatchPlan, ForeachBatchSink, RedisKeys, load_roster, state_value

pytestmark = pytest.mark.integration

SKILL = "linear_equation.isolate_variable"
REFRESH_BUDGET_MS = 100.0


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def client(cfg):
    conn = redis.Redis.from_url(cfg.redis.url, socket_timeout=10)
    try:
        conn.ping()
    except redis.RedisError as exc:
        pytest.skip(f"Redis không chạy ở {cfg.redis.redacted_url} (make up): {exc}")
    yield conn
    conn.close()


@pytest.fixture
def keys(client):
    prefix = f"it-dash-{uuid.uuid4().hex[:10]}:"
    yield RedisKeys(prefix)
    stale = list(client.scan_iter(match=queries.glob_escape(prefix) + "*", count=1000))
    for start in range(0, len(stale), 1000):
        client.delete(*stale[start:start + 1000])
    assert not list(client.scan_iter(match=queries.glob_escape(prefix) + "*", count=1000))


@pytest.fixture(scope="module")
def bootstrap(cfg):
    servers = cfg.kafka.bootstrap_servers
    try:
        describe_topics(admin_client(servers), timeout_s=5)
    except TopicError as exc:
        pytest.skip(f"Kafka không chạy ở {servers} (make up): {exc}")
    return servers


@pytest.fixture
def private_topic(bootstrap):
    name = f"it-dash-{uuid.uuid4().hex[:10]}"
    ensure_topics(bootstrap, [TopicSpec(name, 1)])
    yield name
    delete_topics(bootstrap, [name])


class LiveClass:
    """Một lớp người học chạy qua processor thật, kết quả ghi bằng foreachBatch sink thật."""

    def __init__(self, cfg, keys: RedisKeys, client, topic: str, tmp_path, size: int) -> None:
        self.class_id = f"C-IT-{uuid.uuid4().hex[:6]}"
        self.learners = [f"L-{uuid.uuid4().hex[:16]}" for _ in range(size)]
        self.stores = {learner: MemoryLearnerStore(learner) for learner in self.learners}
        self.seq = {learner: 0 for learner in self.learners}
        self.sink = ForeachBatchSink(cfg, "it-dash", mode="eager", model="rule", key_prefix=keys.prefix,
                                     alerts_topic=topic, redis_client=client, lake_root=tmp_path / "lake")
        self.batch = 0
        load_roster(client, {self.class_id: self.learners}, keys)

    def answers(self, learner: str, results: list[bool]) -> list[Event]:
        now = int(time.time() * 1000)
        events = []
        for correct in results:
            self.seq[learner] += 1
            n = self.seq[learner]
            events.append(Event(event_id=f"{learner}-{n}", event_type="submit_step", event_time_ms=now + n,
                                sent_time_ms=now + n + 20, session_seq=n, class_id=self.class_id,
                                problem_id="P-LIN-001", skill_ids=(SKILL,), is_correct=correct, step_index=1))
        return events

    def run_batch(self, plan: dict[str, list[bool]]):
        rows = []
        for learner, results in plan.items():
            rows += self.stores[learner].run(self.answers(learner, results), "eager", "rule")
        result = self.sink.process_rows(rows, self.batch)
        self.batch += 1
        return result


def _detail(client, keys, class_id: str) -> dict:
    snapshot = queries.read_class_snapshot(client, keys, [class_id], [SKILL])
    return {r["skill_id"]: r for r in queries.class_detail_rows(snapshot, class_id)}[SKILL]


def test_state_change_is_visible_on_next_refresh_with_k_threshold(cfg, client, keys, private_topic, tmp_path):
    live = LiveClass(cfg, keys, client, private_topic, tmp_path, size=12)
    try:
        live.run_batch({learner: [False] for learner in live.learners})
        assert queries.discover_classes(client, keys) == [live.class_id]
        before = _detail(client, keys, live.class_id)
        assert (before["observed"], before["mastered"], before["stuck"]) == ("≈10", "<5", "<5")

        masters, spinners = live.learners[:6], live.learners[-2:]
        result = live.run_batch({**{m: [True, True, True] for m in masters}, **{s: [False] * 9 for s in spinners}})
        written = time.perf_counter()
        after = _detail(client, keys, live.class_id)
        visible_ms = (time.perf_counter() - written) * 1000.0
        assert result.alerts_created >= 2 and result.classes_without_roster == 0
        assert (after["observed"], after["mastered"], after["stuck"], after["mastery_rate"]) == (
            "≈10", "≈5", "<5", "≈50%")

        stuck, problems = queries.read_stuck_learners(client, keys, live.class_id)
        assert not problems
        assert sorted(s.learner_id for s in stuck) == sorted(spinners)
        assert all(s.onset_event_ms is not None and s.stuck_for_ms is not None and s.stuck_for_ms >= 0 for s in stuck)
        assert all(s.last_hint is None for s in stuck)  # chưa có lasthint:{learner} ⇒ để trống
        alerts, problems = queries.read_recent_alerts(client, keys)
        assert not problems and {a.learner_id for a in alerts if a.alert_type == "stuck"} == set(spinners)
        assert len({a.alert_id for a in alerts}) == len(alerts)

        snapshot = queries.read_class_snapshot(client, keys, [live.class_id], [SKILL])
        points, problems = queries.read_trend(client, keys, live.class_id, snapshot.as_of_event_ms(), [SKILL])
        assert points and not problems
        print(f"\nđọc sau khi foreachBatch ghi xong: {visible_ms:.2f} ms; refresh cần ≤ 2 s (chu kỳ fragment) + đọc")
        assert visible_ms < REFRESH_BUDGET_MS
    finally:
        live.sink.close()


def test_class_refresh_reads_within_budget_at_10k_learners(cfg, client, keys, tmp_path):
    rng = random.Random(8)
    skills = queries_skills()
    roster = {f"C-SIM-{c + 1:04d}": [f"L-{c * 35 + i:016x}" for i in range(35)] for c in range(286)}
    roster["C-SIM-0286"] = roster["C-SIM-0286"][:25]
    load_roster(client, roster, keys)
    now = int(time.time() * 1000)
    pipe = client.pipeline(transaction=False)
    for class_id, learners in roster.items():
        for learner in learners:
            pipe.hset(keys.state(learner), mapping={s: state_value({
                "class_id": class_id, "p_mastery": None, "opportunities": 6, "correct": 3, "consec_wrong": 0,
                "streak": 0, "mastered": rng.random() < 0.4, "stuck": rng.random() < 0.05,
                "stuck_onset_event_id": "e", "state_version": 1, "last_event_time_ms": now}, 1) for s in skills})
    pipe.execute()
    sink = ForeachBatchSink(cfg, "it-dash-10k", mode="eager", model="rule", key_prefix=keys.prefix,
                            redis_client=client, lake_root=tmp_path / "lake")
    sink._refresh_classes(BatchPlan(touched_classes={c: now for c in roster}), 1)

    started = time.perf_counter()
    classes = queries.discover_classes(client, keys)
    discover_ms = (time.perf_counter() - started) * 1000.0
    assert len(classes) == 286
    timings = []
    for _ in range(20):
        started = time.perf_counter()
        snapshot = queries.read_class_snapshot(client, keys, classes, skills)
        queries.read_trend(client, keys, classes[0], snapshot.as_of_event_ms(classes[0]), skills)
        queries.read_pipeline_metrics(client, keys)
        timings.append((time.perf_counter() - started) * 1000.0)
    assert snapshot.commands == 286 * (1 + len(skills)) and len(snapshot.counts) == 286 * len(skills)
    timings.sort()
    p50, p95 = statistics.median(timings), timings[int(0.95 * (len(timings) - 1))]
    print(f"\n10K learners, Redis thật: refresh P50 {p50:.1f} ms, P95 {p95:.1f} ms, max {timings[-1]:.1f} ms; "
          f"SCAN lớp (cache 30 s) {discover_ms:.1f} ms; keys in DB {client.dbsize()}")
    assert p95 <= REFRESH_BUDGET_MS


def queries_skills() -> tuple[str, ...]:
    from ai_math_tutor.simulator.population import load_skills_config
    from ai_math_tutor.streaming.processor import SKILLS_CONFIG_PATH

    return load_skills_config(SKILLS_CONFIG_PATH).skill_ids


# ---------------------------------------------------------------- Pipeline tab ↔ lastProgress


def _as_dict(progress) -> dict:
    return json.loads(progress.json) if hasattr(progress, "json") else dict(progress)


def test_pipeline_metrics_match_spark_last_progress(cfg, client, keys, bootstrap, private_topic, tmp_path):
    from confluent_kafka import Producer

    from ai_math_tutor.streaming.job import build_spark
    from ai_math_tutor.streaming.listener import attach_listener, state_query_name

    total = 400
    producer = Producer({"bootstrap.servers": bootstrap})
    for i in range(total):
        producer.produce(private_topic, value=f"m{i}".encode(), key=b"k")
    assert producer.flush(30) == 0

    conf = cfg.spark.conf()
    local_dir = cfg.paths.data_root / "spark-tmp" / "dashboard-it"
    conf.update({"spark.app.name": "nhom8-dashboard-it", "spark.local.dir": str(local_dir),
                 "spark.sql.shuffle.partitions": "1",
                 "spark.driver.extraJavaOptions": f"-Djava.io.tmpdir={local_dir}"})
    spark = build_spark(conf)
    run_id = f"it-dash-{uuid.uuid4().hex[:8]}"
    listener = attach_listener(spark, cfg, run_id, redis_client=client, key_prefix=keys.prefix,
                               out_path=tmp_path / "progress.jsonl")
    query = None
    try:
        frame = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", bootstrap)
                 .option("subscribe", private_topic).option("startingOffsets", "earliest")
                 .option("maxOffsetsPerTrigger", 100).load()
                 .withWatermark("timestamp", "5 seconds").dropDuplicatesWithinWatermark(["partition", "offset"]))
        # Tên query trạng thái của run: listener của pipeline chỉ ghi sự kiện của query này.
        query = (frame.writeStream.queryName(state_query_name(run_id)).foreachBatch(lambda df, _batch_id: df.count())
                 .option("checkpointLocation", str(tmp_path / "checkpoint")).trigger(processingTime="1 second")
                 .start())
        deadline = time.time() + 120
        while time.time() < deadline:
            last = query.lastProgress
            if last is not None and _as_dict(last).get("batchId", -1) >= 1:
                break
            time.sleep(0.5)
        query.stop()
        last = _as_dict(query.lastProgress)
        deadline = time.time() + 15
        while time.time() < deadline and (listener.last_record or {}).get("batch_id") != last["batchId"]:
            time.sleep(0.2)
        log = (tmp_path / "progress.jsonl").read_text(encoding="utf-8") if (tmp_path / "progress.jsonl").exists() else ""
        assert (listener.last_record or {}).get("batch_id") == last["batchId"], (
            f"listener chưa nhận batch cuối: records={listener.records} errors={listener.errors} log={log[-2000:]!r}")

        metrics = queries.read_pipeline_metrics(client, keys)
        assert metrics is not None and metrics.run_id == run_id
        end_offset = json.loads(last["sources"][0]["endOffset"]) if isinstance(last["sources"][0]["endOffset"], str) \
            else last["sources"][0]["endOffset"]
        consumed = sum(int(v) for parts in end_offset.values() for v in parts.values())
        watermark = (last.get("eventTime") or {}).get("watermark")
        expected = {
            "batch_id": last["batchId"],
            "updated_ms": iso_to_ms(last["timestamp"]),
            "input_rows_per_s": float(last["inputRowsPerSecond"]),
            "processed_rows_per_s": float(last["processedRowsPerSecond"]),
            "batch_duration_ms": int(last["batchDuration"]),
            "watermark_ms": None if watermark in (None, "1970-01-01T00:00:00.000Z") else iso_to_ms(watermark),
            "lag_total": total - consumed,
        }
        actual = {name: getattr(metrics, name) for name in expected}
        print(f"\nlastProgress batch {last['batchId']}: {json.dumps(expected)}\nmetrics:pipeline: {json.dumps(actual)}")
        assert actual == expected
        assert 0 < consumed <= total
    finally:
        if query is not None and query.isActive:
            query.stop()
        spark.streams.removeListener(listener)
        spark.stop()
