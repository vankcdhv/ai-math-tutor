"""Spike kiểm chứng API trạng thái PySpark 4.1.3 + Kafka trước khi viết pipeline thật.

Pipeline giống thiết kế đích: Kafka learner-events → from_json → withWatermark(event_time)
→ dropDuplicatesWithinWatermark(["event_id"]) → groupBy(learner_id).transformWithStateInPandas
(timeMode="EventTime", timer bế tắc theo event time) → foreachBatch (thu kết quả về driver).

Kịch bản (--scenarios, phân tách bằng dấu phẩy):
  functional   20 sự kiện mẫu (1 trùng, 1 muộn) + heartbeat vượt end+W để drain timer,
               rồi gửi sự kiện cũ hơn watermark để xem bị loại; chạy cả hai biến thể
               eventTimeColumnName "" và "event_time".
  provider     cùng pipeline với HDFSBackedStateStoreProvider — TWS có bắt buộc RocksDB không.
  latency      tải tổng hợp open-loop ở từng --rates × --shuffle-partitions, trigger ASAP;
               độ trễ = thời điểm foreachBatch nhận dòng − sent_time.
  no-connector phiên Spark KHÔNG có spark.jars.packages (phải chạy riêng một mình).
Kết quả JSON ghi vào $NHOM8_DATA_ROOT/spike/<stamp>/results.json; log Spark trên stderr.
"""

from __future__ import annotations

import argparse
import json
import multiprocessing as mp
import os
import sys
import time
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(REPO_ROOT / "src"))

from ai_math_tutor.config import load_config  # noqa: E402

SAMPLE = REPO_ROOT / "data_sample" / "events-week1-sample.jsonl"
ROCKSDB = "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider"
HDFS = "org.apache.spark.sql.execution.streaming.state.HDFSBackedStateStoreProvider"
ISO_FMT = "yyyy-MM-dd'T'HH:mm:ss.SSSX"
HEARTBEAT_LEARNER = "__heartbeat__"
OUTPUT_DDL = (
    "learner_id STRING, kind STRING, sent_ms LONG, event_ms LONG, count LONG, "
    "watermark_ms LONG, expiry_ms LONG, event_time TIMESTAMP"
)


def log(message: str) -> None:
    print(f"SPIKE {datetime.now(timezone.utc):%H:%M:%S} {message}", flush=True)


def iso_ms(epoch_ms: int) -> str:
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def parse_iso_ms(value: str) -> int:
    return int(datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp() * 1000)


# ---------------------------------------------------------------- Kafka helpers

def kafka_admin(bootstrap: str):
    from confluent_kafka.admin import AdminClient
    return AdminClient({"bootstrap.servers": bootstrap})


def create_topic(bootstrap: str, name: str, partitions: int) -> None:
    from confluent_kafka.admin import NewTopic
    # Giữ tham chiếu admin tới khi future xong: client bị thu hồi sẽ hủy yêu cầu (_DESTROY).
    admin = kafka_admin(bootstrap)
    futures = admin.create_topics([NewTopic(name, num_partitions=partitions, replication_factor=1)])
    futures[name].result(timeout=30)


def delete_topic(bootstrap: str, name: str) -> None:
    try:
        admin = kafka_admin(bootstrap)
        admin.delete_topics([name])[name].result(timeout=30)
    except Exception as exc:  # dọn dẹp: không che lỗi chính của kịch bản
        log(f"không xóa được topic {name}: {exc}")


def make_producer(bootstrap: str):
    from confluent_kafka import Producer
    return Producer({
        "bootstrap.servers": bootstrap,
        "enable.idempotence": True,   # mặc định false trong librdkafka
        "acks": "all",
        "linger.ms": 5,
    })


def produce_all(producer, topic: str, records: list[tuple[str | None, bytes, int | None]]) -> None:
    errors: list[str] = []

    def on_delivery(err, _msg):
        if err is not None:
            errors.append(str(err))

    for key, value, partition in records:
        kwargs: dict[str, Any] = {"key": key, "value": value, "on_delivery": on_delivery}
        if partition is not None:
            kwargs["partition"] = partition
        producer.produce(topic, **kwargs)
    remaining = producer.flush(30)
    if remaining or errors:
        raise RuntimeError(f"gửi Kafka lỗi: còn {remaining} chưa gửi, lỗi {errors[:3]}")


def open_loop_producer(bootstrap: str, topic: str, rate: int, duration_s: float, learners: int,
                       result_queue) -> None:
    """Gửi đúng lịch t0 + i/rate (không chờ phản hồi); sent_time đóng dấu ngay trước produce()."""
    try:
        producer = make_producer(bootstrap)
        learner_ids = [f"L-{i:016x}" for i in range(learners)]
        delivery_errors = [0]

        def on_delivery(err, _msg):
            if err is not None:
                delivery_errors[0] += 1

        total = int(rate * duration_s)
        start = time.time()
        sent = 0
        while sent < total:
            due = min(total, int((time.time() - start) * rate) + 1)
            while sent < due:
                now_ms = int(time.time() * 1000)
                learner = learner_ids[sent % learners]
                event = {
                    "schema_version": "0.1", "event_id": str(uuid.uuid4()), "event_type": "open_problem",
                    "learner_id": learner, "event_time": iso_ms(now_ms), "sent_time": iso_ms(now_ms),
                    "payload": {},
                }
                try:
                    producer.produce(topic, key=learner, value=json.dumps(event).encode(), on_delivery=on_delivery)
                except BufferError:
                    producer.poll(0.01)
                    continue
                sent += 1
            producer.poll(0)
            time.sleep(0.002)
        elapsed = time.time() - start
        remaining = producer.flush(60)
        result_queue.put({"sent": sent, "elapsed_s": elapsed, "actual_rate": sent / elapsed if elapsed else None,
                          "unflushed": remaining, "delivery_errors": delivery_errors[0], "start_ms": int(start * 1000)})
    except Exception as exc:  # báo về tiến trình cha thay vì chết im lặng
        result_queue.put({"error": repr(exc)})


# ---------------------------------------------------------------- Spark

def build_spark(app: str, with_connector: bool = True):
    from pyspark.sql import SparkSession

    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    cfg = load_config()
    builder = SparkSession.builder.appName(app)
    for key, value in cfg.spark.conf().items():
        if key == "spark.jars.packages" and not with_connector:
            continue
        builder = builder.config(key, value)
    builder = builder.config("spark.sql.streaming.numRecentProgressUpdates", "1000")
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark, cfg


class ProgressCollector:
    """Giữ mọi QueryProgress (recentProgress chỉ giữ số có hạn)."""

    def __init__(self):
        from pyspark.sql.streaming import StreamingQueryListener

        collector = self

        class _Listener(StreamingQueryListener):
            def onQueryStarted(self, event):
                pass

            def onQueryProgress(self, event):
                collector.by_query.setdefault(str(event.progress.id), []).append(json.loads(event.progress.json))

            def onQueryIdle(self, event):
                pass

            def onQueryTerminated(self, event):
                pass

        self.by_query: dict[str, list[dict]] = {}
        self.listener = _Listener()

    def progress(self, query) -> list[dict]:
        return list(self.by_query.get(str(query.id), []))


def make_processor_class(idle_timeout_ms: int, lean: bool = False):
    from pyspark.sql.streaming import StatefulProcessor, StatefulProcessorHandle

    class StuckTimerProcessor(StatefulProcessor):
        """Đếm sự kiện theo người học và giữ một timer event-time = event mới nhất + idle_timeout."""

        def init(self, handle: StatefulProcessorHandle) -> None:
            self.handle = handle
            self.count = handle.getValueState("count", "value LONG")
            self.last_event = handle.getValueState("last_event_ms", "value LONG")

        def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
            watermark = timerValues.getCurrentWatermarkInMs()
            count = self.count.get()[0] if self.count.exists() else 0
            last = self.last_event.get()[0] if self.last_event.exists() else None
            frames = []
            for pdf in rows:
                pdf = pdf.sort_values("event_ms")
                counts = count + np.arange(1, len(pdf) + 1)
                count += len(pdf)
                frames.append(pd.DataFrame({
                    "learner_id": key[0], "kind": "event", "sent_ms": pdf["sent_ms"].to_numpy(),
                    "event_ms": pdf["event_ms"].to_numpy(), "count": counts, "watermark_ms": watermark,
                    "expiry_ms": None, "event_time": pd.to_datetime(pdf["event_ms"], unit="ms").to_numpy(),
                }))
                newest = int(pdf["event_ms"].max())
                last = newest if last is None else max(last, newest)
            self.count.update((count,))
            self.last_event.update((last,))
            for timer in self.handle.listTimers():
                self.handle.deleteTimer(timer)
            self.handle.registerTimer(last + idle_timeout_ms)
            if frames:
                yield pd.concat(frames, ignore_index=True)

        def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
            expiry = expiredTimerInfo.getExpiryTimeInMs()
            yield pd.DataFrame({
                "learner_id": [key[0]], "kind": ["timer"], "sent_ms": [None], "event_ms": [None],
                "count": [self.count.get()[0] if self.count.exists() else 0],
                "watermark_ms": [timerValues.getCurrentWatermarkInMs()], "expiry_ms": [expiry],
                "event_time": [pd.to_datetime(expiry, unit="ms")],
            })

        def close(self) -> None:
            pass

    class LeanStuckTimerProcessor(StatefulProcessor):
        """Cùng hành vi, ít lời gọi state API hơn: mỗi lời gọi là một vòng protobuf qua socket tới JVM.

        Một ValueState dạng struct (count, last_event_ms, timer_ms) thay cho hai biến; timer hiện tại
        lưu trong state nên không cần listTimers(). Mỗi key/batch: 1 get + 1 update + (delete + register).
        """

        def init(self, handle: StatefulProcessorHandle) -> None:
            self.handle = handle
            self.state = handle.getValueState("learner", "count LONG, last_event_ms LONG, timer_ms LONG")

        def handleInputRows(self, key, rows, timerValues) -> Iterator[pd.DataFrame]:
            watermark = timerValues.getCurrentWatermarkInMs()
            current = self.state.get()
            count, last, timer = current if current is not None else (0, None, None)
            frames = []
            for pdf in rows:
                pdf = pdf.sort_values("event_ms")
                counts = count + np.arange(1, len(pdf) + 1)
                count += len(pdf)
                frames.append(pd.DataFrame({
                    "learner_id": key[0], "kind": "event", "sent_ms": pdf["sent_ms"].to_numpy(),
                    "event_ms": pdf["event_ms"].to_numpy(), "count": counts, "watermark_ms": watermark,
                    "expiry_ms": None, "event_time": pd.to_datetime(pdf["event_ms"], unit="ms").to_numpy(),
                }))
                newest = int(pdf["event_ms"].max())
                last = newest if last is None else max(last, newest)
            new_timer = last + idle_timeout_ms
            if timer != new_timer:
                if timer is not None:
                    self.handle.deleteTimer(timer)
                self.handle.registerTimer(new_timer)
            self.state.update((count, last, new_timer))
            if frames:
                yield pd.concat(frames, ignore_index=True)

        def handleExpiredTimer(self, key, timerValues, expiredTimerInfo) -> Iterator[pd.DataFrame]:
            expiry = expiredTimerInfo.getExpiryTimeInMs()
            current = self.state.get()
            count = current[0] if current is not None else 0
            if current is not None:
                self.state.update((current[0], current[1], None))  # timer đã hết hạn tự bị xóa
            yield pd.DataFrame({
                "learner_id": [key[0]], "kind": ["timer"], "sent_ms": [None], "event_ms": [None],
                "count": [count], "watermark_ms": [timerValues.getCurrentWatermarkInMs()], "expiry_ms": [expiry],
                "event_time": [pd.to_datetime(expiry, unit="ms")],
            })

        def close(self) -> None:
            pass

    return LeanStuckTimerProcessor if lean else StuckTimerProcessor


def build_query(spark, bootstrap: str, topic: str, checkpoint: Path, watermark: str, idle_timeout_ms: int,
                sink, event_time_col: str = "", name: str = "spike", lean: bool = False):
    from pyspark.sql import functions as F
    from pyspark.sql.types import StringType, StructField, StructType

    schema = StructType([StructField(n, StringType()) for n in
                         ("event_id", "event_type", "learner_id", "event_time", "sent_time")])
    raw = (spark.readStream.format("kafka")
           .option("kafka.bootstrap.servers", bootstrap)
           .option("subscribe", topic)
           .option("startingOffsets", "earliest")
           .load())
    events = (raw.select(F.from_json(F.col("value").cast("string"), schema).alias("e")).select("e.*")
              .withColumn("event_time", F.to_timestamp("event_time", ISO_FMT))
              .withColumn("sent_ms", F.unix_millis(F.to_timestamp("sent_time", ISO_FMT)))
              .withColumn("event_ms", F.unix_millis("event_time"))
              .drop("sent_time"))
    stateful = (events.withWatermark("event_time", watermark)
                .dropDuplicatesWithinWatermark(["event_id"])
                .filter(F.col("event_type") != "heartbeat")
                .select("learner_id", "event_time", "event_ms", "sent_ms")
                .groupBy("learner_id")
                .transformWithStateInPandas(
                    statefulProcessor=make_processor_class(idle_timeout_ms, lean)(),
                    outputStructType=OUTPUT_DDL,
                    outputMode="Update",
                    timeMode="EventTime",
                    eventTimeColumnName=event_time_col,
                ))
    return (stateful.writeStream.queryName(name)
            .outputMode("update")
            .option("checkpointLocation", str(checkpoint))
            .foreachBatch(sink)
            .start())


def wait_for(predicate, query, timeout_s: float, what: str) -> bool:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        if query.exception() is not None:
            raise RuntimeError(f"query lỗi khi chờ {what}: {query.exception()}")
        if predicate():
            return True
        time.sleep(0.25)
    log(f"hết giờ chờ {what} ({timeout_s}s)")
    return False


def stop_at_batch_boundary(query, timeout_s: float = 60) -> None:
    """Dừng query khi không có batch đang chạy; stop() giữa batch làm hủy job giữa chừng."""
    deadline = time.time() + timeout_s
    while time.time() < deadline and query.isActive and query.status.get("isTriggerActive"):
        time.sleep(0.05)
    query.stop()


def total_input(progress: list[dict]) -> int:
    return sum(p.get("numInputRows", 0) for p in progress)


def summarize_progress(p: dict) -> dict:
    """Các trường lastProgress mà pipeline/runner sẽ dùng."""
    return {
        "batchId": p.get("batchId"),
        "numInputRows": p.get("numInputRows"),
        "durationMs": p.get("durationMs"),
        "eventTime": p.get("eventTime"),
        "stateOperators": p.get("stateOperators"),
        "sources": [{"description": s.get("description"), "endOffset": s.get("endOffset"),
                     "latestOffset": s.get("latestOffset"), "metrics": s.get("metrics")}
                    for s in p.get("sources", [])],
    }


# ---------------------------------------------------------------- scenarios

def run_functional(spark, cfg, collector, work: Path, partitions: int, event_time_col: str, lean: bool,
                   provider: str = ROCKSDB) -> dict[str, Any]:
    bootstrap = cfg.kafka.bootstrap_servers
    tag = "et" if event_time_col else "noet"
    label = f"functional-{tag}" + ("-hdfs" if provider == HDFS else "") + ("-lean" if lean else "")
    topic = f"spike-{label}-{uuid.uuid4().hex[:6]}"
    watermark_s, idle_timeout_ms = 30, 60_000
    rows: list[dict] = []

    def sink(batch_df, batch_id):
        for r in batch_df.collect():
            rows.append({**r.asDict(), "batch_id": batch_id, "event_time": str(r["event_time"])})

    spark.conf.set("spark.sql.streaming.stateStore.providerClass", provider)
    spark.conf.set("spark.sql.shuffle.partitions", str(partitions))
    result: dict[str, Any] = {"scenario": label, "topic": topic, "provider": provider,
                              "probe": provider != ROCKSDB,
                              "eventTimeColumnName": event_time_col, "watermark_s": watermark_s,
                              "idle_timeout_ms": idle_timeout_ms}
    query = None
    try:
        create_topic(bootstrap, topic, partitions)
        producer = make_producer(bootstrap)
        query = build_query(spark, bootstrap, topic, work / label / "checkpoint", f"{watermark_s} seconds",
                            idle_timeout_ms, sink, event_time_col, name=label.replace("-", "_"), lean=lean)
        lines = SAMPLE.read_bytes().splitlines()
        events = [json.loads(line) for line in lines]
        produce_all(producer, topic, [(e["learner_id"], line, None) for e, line in zip(events, lines)])
        wait_for(lambda: total_input(collector.progress(query)) >= len(lines), query, 120, "20 sự kiện mẫu")
        wait_for(lambda: any(r["kind"] == "event" for r in rows), query, 60, "đầu ra sự kiện")
        after_sample = summarize_progress(collector.progress(query)[-1])

        max_et = max(parse_iso_ms(e["event_time"]) for e in events)
        heartbeat_ms = max_et + watermark_s * 1000 + idle_timeout_ms + 5_000
        beats = []
        for p in range(partitions):
            beat = {"schema_version": "0.1", "event_id": str(uuid.uuid4()), "event_type": "heartbeat",
                    "learner_id": HEARTBEAT_LEARNER, "event_time": iso_ms(heartbeat_ms),
                    "sent_time": iso_ms(int(time.time() * 1000))}
            beats.append((HEARTBEAT_LEARNER, json.dumps(beat).encode(), p))
        sent_before = total_input(collector.progress(query))
        produce_all(producer, topic, beats)
        learners = {e["learner_id"] for e in events}
        wait_for(lambda: {r["learner_id"] for r in rows if r["kind"] == "timer"} >= learners, query, 90,
                 "timer của cả 3 người học")
        progress = collector.progress(query)
        after_drain = summarize_progress(progress[-1])

        # Sau drain: sự kiện mới (event_id mới) nhưng event_time cũ, và bản trùng nguyên byte của dòng 1.
        stale = dict(events[0], event_id=str(uuid.uuid4()))
        rows_before_stale = len(rows)
        produce_all(producer, topic, [(stale["learner_id"], json.dumps(stale).encode(), None),
                                      (events[0]["learner_id"], lines[0], None)])
        wait_for(lambda: total_input(collector.progress(query)) >= sent_before + partitions + 2, query, 60,
                 "2 sự kiện cũ")
        time.sleep(3)
        progress = collector.progress(query)
        after_stale = summarize_progress(progress[-1])

        final_counts: dict[str, int] = {}
        for r in rows:
            if r["kind"] == "event":
                final_counts[r["learner_id"]] = max(final_counts.get(r["learner_id"], 0), r["count"])
        expected: dict[str, int] = {}
        seen_ids: set[str] = set()
        for e in events:
            if e["event_id"] not in seen_ids:
                seen_ids.add(e["event_id"])
                expected[e["learner_id"]] = expected.get(e["learner_id"], 0) + 1
        timer_learners = {r["learner_id"] for r in rows if r["kind"] == "timer"}
        checks = {
            "dedup_correct": final_counts == expected,
            "timers_fired_for_all_learners": timer_learners == learners,
            "stale_events_produced_no_output": not rows[rows_before_stale:],
        }
        result.update({
            # Đạt khi cả ba bất biến đúng: khử trùng, timer event-time khi drain, loại sự kiện cũ.
            "ok": all(checks.values()),
            "checks": checks,
            "event_rows": sum(r["kind"] == "event" for r in rows),
            "final_counts": final_counts,
            "expected_unique_counts": expected,
            "timer_rows": [r for r in rows if r["kind"] == "timer"],
            "rows_from_stale_events": rows[rows_before_stale:],
            "heartbeat_event_time": iso_ms(heartbeat_ms),
            "max_sample_event_time": iso_ms(max_et),
            "progress_after_sample": after_sample,
            "progress_after_drain": after_drain,
            "progress_after_stale": after_stale,
            "watermark_trace": [(p.get("batchId"), p.get("numInputRows"), (p.get("eventTime") or {}).get("watermark"))
                                for p in progress],
            "all_progress_last": progress[-1],
        })
    except Exception as exc:
        result.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"[:4000]})
        log(f"{label} LỖI: {result['error'][:600]}")
    finally:
        if query is not None:
            stop_at_batch_boundary(query)
        spark.conf.set("spark.sql.streaming.stateStore.providerClass", cfg.spark.state_store_provider)
        delete_topic(bootstrap, topic)
    return result


def percentile(values, q: float) -> float | None:
    return float(np.percentile(values, q)) if len(values) else None


def run_latency(spark, cfg, collector, work: Path, rate: int, shuffle_partitions: int, kafka_partitions: int,
                lean: bool,
                duration_s: float, warmup_s: float, learners: int) -> dict[str, Any]:
    bootstrap = cfg.kafka.bootstrap_servers
    label = f"latency-r{rate}-sp{shuffle_partitions}" + ("-lean" if lean else "")
    topic = f"spike-{label}-{uuid.uuid4().hex[:6]}"
    batches: list[dict] = []

    def sink(batch_df, batch_id):
        data = batch_df.select("kind", "sent_ms").collect()
        visible_ms = time.time() * 1000
        sent = np.array([r["sent_ms"] for r in data if r["kind"] == "event"], dtype="float64")
        batches.append({"batch_id": batch_id, "visible_ms": visible_ms, "sent_ms": sent,
                        "timers": sum(r["kind"] == "timer" for r in data)})

    spark.conf.set("spark.sql.shuffle.partitions", str(shuffle_partitions))
    result: dict[str, Any] = {"scenario": label, "rate_target": rate, "shuffle_partitions": shuffle_partitions,
                              "kafka_partitions": kafka_partitions, "duration_s": duration_s,
                              "warmup_s": warmup_s, "learners": learners}
    query = None
    proc = None
    ctx = mp.get_context("spawn")
    try:
        create_topic(bootstrap, topic, kafka_partitions)
        query = build_query(spark, bootstrap, topic, work / label / "checkpoint", "2 minutes", 60_000, sink,
                            name=label.replace("-", "_"), lean=lean)
        queue = ctx.Queue()
        proc = ctx.Process(
            target=open_loop_producer, args=(bootstrap, topic, rate, duration_s, learners, queue), daemon=True)
        proc.start()
        proc.join(duration_s + 120)
        if proc.is_alive():
            raise RuntimeError("producer quá giờ")
        produced = queue.get(timeout=10)
        if "error" in produced:
            raise RuntimeError(f"producer lỗi: {produced['error']}")
        result["producer"] = produced
        wait_for(lambda: sum(len(b["sent_ms"]) for b in batches) >= produced["sent"], query, 180,
                 "pipeline xử lý hết")
        progress = collector.progress(query)

        window_start = produced["start_ms"] + warmup_s * 1000
        window_end = produced["start_ms"] + duration_s * 1000
        latencies = np.concatenate([b["visible_ms"] - b["sent_ms"][(b["sent_ms"] >= window_start)
                                                                   & (b["sent_ms"] <= window_end)]
                                    for b in batches]) if batches else np.array([])
        steady = [p for p in progress
                  if window_start <= parse_iso_ms(p["timestamp"]) <= window_end and p.get("numInputRows", 0) > 0]
        trig = [p["durationMs"]["triggerExecution"] for p in steady]
        behind_max = [s["metrics"].get("maxOffsetsBehindLatest") for p in steady for s in p.get("sources", [])
                      if s.get("metrics") and s["metrics"].get("maxOffsetsBehindLatest") is not None]
        behind_avg = [s["metrics"].get("avgOffsetsBehindLatest") for p in steady for s in p.get("sources", [])
                      if s.get("metrics") and s["metrics"].get("avgOffsetsBehindLatest") is not None]
        result.update({
            "ok": True,
            "events_seen": int(sum(len(b["sent_ms"]) for b in batches)),
            "events_measured": int(len(latencies)),
            "latency_ms": {"p50": percentile(latencies, 50), "p95": percentile(latencies, 95),
                           "p99": percentile(latencies, 99), "max": float(latencies.max()) if len(latencies) else None,
                           "mean": float(latencies.mean()) if len(latencies) else None},
            "batches_steady": len(steady),
            "trigger_execution_ms": {"p50": percentile(trig, 50), "p95": percentile(trig, 95),
                                     "max": max(trig) if trig else None},
            "rows_per_batch_mean": float(np.mean([p["numInputRows"] for p in steady])) if steady else None,
            "processed_rows_per_s_mean": float(np.mean([p.get("processedRowsPerSecond") or 0 for p in steady]))
            if steady else None,
            "offsets_behind_latest": {"avg_p95": percentile([float(x) for x in behind_avg], 95),
                                      "max_max": max(float(x) for x in behind_max) if behind_max else None},
            "duration_ms_keys": sorted(steady[-1]["durationMs"].keys()) if steady else None,
            "last_progress": summarize_progress(progress[-1]) if progress else None,
        })
        log(f"{label}: p50={result['latency_ms']['p50']:.0f} ms p95={result['latency_ms']['p95']:.0f} ms "
            f"trigger p50={result['trigger_execution_ms']['p50']} ms rate={produced['actual_rate']:.0f} ev/s")
    except Exception as exc:
        result.update({"ok": False, "error": f"{type(exc).__name__}: {exc}"[:4000]})
        log(f"{label} LỖI: {result['error'][:600]}")
    finally:
        if proc is not None and proc.is_alive():
            proc.terminate()
            proc.join(10)
        if query is not None:
            stop_at_batch_boundary(query)
        delete_topic(bootstrap, topic)
    return result


def run_no_connector() -> dict[str, Any]:
    spark, cfg = build_spark("spike-no-connector", with_connector=False)
    try:
        (spark.readStream.format("kafka").option("kafka.bootstrap.servers", cfg.kafka.bootstrap_servers)
         .option("subscribe", "learner-events").load())
        return {"scenario": "no-connector", "probe": True, "ok": True,
                "note": "nạp được nguồn kafka khi không có package"}
    except Exception as exc:
        return {"scenario": "no-connector", "probe": True, "ok": False,
                "error": f"{type(exc).__name__}: {exc}"[:1500]}
    finally:
        spark.stop()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--scenarios", default="functional,provider,latency")
    parser.add_argument("--rates", default="100,1000")
    parser.add_argument("--shuffle-partitions", default="3,200")
    parser.add_argument("--kafka-partitions", type=int, default=3)
    parser.add_argument("--duration-s", type=float, default=60)
    parser.add_argument("--warmup-s", type=float, default=15)
    parser.add_argument("--learners", type=int, default=1000)
    parser.add_argument("--processor", choices=("full", "lean"), default="full",
                        help="full: 2 ValueState + listTimers; lean: 1 ValueState struct, timer giữ trong state")
    parser.add_argument("--out", type=Path, default=None)
    args = parser.parse_args(argv)

    scenarios = [s.strip() for s in args.scenarios.split(",") if s.strip()]
    unknown = set(scenarios) - {"functional", "provider", "latency", "no-connector"}
    if unknown:
        parser.error(f"kịch bản lạ: {sorted(unknown)}")
    if "no-connector" in scenarios and len(scenarios) > 1:
        parser.error("no-connector cần JVM riêng: chạy một mình")
    rates = [int(x) for x in args.rates.split(",")]
    shuffles = [int(x) for x in args.shuffle_partitions.split(",")]
    if min(rates + shuffles + [args.kafka_partitions, args.learners]) < 1 or args.warmup_s >= args.duration_s:
        parser.error("rates/partitions/learners phải ≥ 1 và warmup < duration")

    cfg = load_config()
    lean = args.processor == "lean"
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    work = cfg.paths.data_root / "spike" / stamp
    work.mkdir(parents=True, exist_ok=True)
    out = args.out or work / "results.json"
    results: dict[str, Any] = {"stamp": stamp, "args": vars(args) | {"out": str(out)}, "scenarios": []}

    if scenarios == ["no-connector"]:
        results["scenarios"].append(run_no_connector())
    else:
        started = time.time()
        spark, cfg = build_spark("spike-tws")
        collector = ProgressCollector()
        spark.streams.addListener(collector.listener)
        try:
            spark_conf = spark.sparkContext.getConf()
            results["spark"] = {
                "version": spark.version,
                "spark.jars.packages": spark_conf.get("spark.jars.packages", None),
                # Danh sách jar SparkSubmit đã resolve từ spark.jars.packages (Ivy).
                "spark.jars": spark_conf.get("spark.jars", ""),
                "conf": {k: spark_conf.get(k, None) for k in cfg.spark.conf()},
                "noDataMicroBatches": spark.conf.get("spark.sql.streaming.noDataMicroBatches.enabled", None),
            }
            try:
                results["spark"]["listJars"] = spark.sparkContext._jsc.sc().listJars().mkString(",")
            except Exception as exc:  # API nội bộ JVM; chỉ để đối chiếu
                results["spark"]["listJars"] = f"không đọc được: {exc}"
            log(f"Spark {spark.version}; jars: {results['spark']['spark.jars']}")
            if "functional" in scenarios:
                for col in ("", "event_time"):
                    results["scenarios"].append(run_functional(spark, cfg, collector, work, args.kafka_partitions, col,
                                                               lean))
            if "provider" in scenarios:
                results["scenarios"].append(run_functional(spark, cfg, collector, work, args.kafka_partitions, "",
                                                           lean, provider=HDFS))
            if "latency" in scenarios:
                for rate in rates:
                    for sp in shuffles:
                        results["scenarios"].append(run_latency(spark, cfg, collector, work, rate, sp,
                                                                args.kafka_partitions, lean, args.duration_s,
                                                                args.warmup_s, args.learners))
        finally:
            results["runtime_s"] = time.time() - started
            spark.stop()

    out.write_text(json.dumps(results, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    for s in results["scenarios"]:
        log(f"{s['scenario']}: ok={s.get('ok')} {s.get('error', '')[:300]}")
    log(f"kết quả: {out}")
    # Kịch bản "probe" ghi nhận một hành vi (có thể là lỗi mong đợi), không tính là spike thất bại.
    return 0 if all(s.get("ok") for s in results["scenarios"] if not s.get("probe")) else 1


if __name__ == "__main__":
    sys.exit(main())
