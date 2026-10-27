"""Listener tiến độ: tách endOffset, lag theo AdminClient, dòng progress.jsonl và hash metrics:pipeline."""

from __future__ import annotations

import json

import fakeredis
import numpy as np
import pytest

from ai_math_tutor.config import load_config
from ai_math_tutor.streaming.listener import (
    ProgressListener,
    compute_lag,
    iso_to_ms,
    metrics_fields,
    parse_end_offsets,
    progress_record,
    state_summary,
)
from ai_math_tutor.streaming.sinks import LatencyWindow, RedisKeys

# Dạng `StreamingQueryProgress.json` đã quan sát trong spike Spark TWS + Kafka (rút gọn).
PROGRESS = {
    "id": "q-1", "runId": "r-1", "name": "learner_state_run-1", "timestamp": "2026-11-02T00:01:00.000Z", "batchId": 7,
    "batchDuration": 1650, "numInputRows": 1659, "inputRowsPerSecond": 1001.2, "processedRowsPerSecond": 1005.4,
    "durationMs": {"addBatch": 1400, "triggerExecution": 1650, "latestOffset": 3},
    "eventTime": {"avg": "2026-11-02T00:00:59.000Z", "max": "2026-11-02T00:01:00.000Z",
                  "min": "2026-11-02T00:00:58.000Z", "watermark": "2026-11-02T00:00:30.000Z"},
    "stateOperators": [
        {"operatorName": "dedupeWithinWatermark", "numRowsTotal": 5000, "numRowsUpdated": 1659,
         "numRowsRemoved": 0, "numRowsDroppedByWatermark": 2, "memoryUsedBytes": 1024,
         "customMetrics": {"numDroppedDuplicateRows": 31}},
        {"operatorName": "transformWithStateInPandasExec", "numRowsTotal": 900, "numRowsUpdated": 400,
         "numRowsDroppedByWatermark": 0, "memoryUsedBytes": 2048, "customMetrics": {"numRegisteredTimers": 3}},
    ],
    "sources": [{"description": "KafkaV2[Subscribe[learner-events]]",
                 "startOffset": {"learner-events": {"0": 100, "1": 90, "2": 80}},
                 "endOffset": {"learner-events": {"0": 650, "1": 640, "2": 630}},
                 "metrics": {"avgOffsetsBehindLatest": "0.0", "maxOffsetsBehindLatest": "0"}}],
    "sink": {"description": "ForeachBatchSink", "numOutputRows": -1},
}
LATEST = {"learner-events": {0: 700, 1: 640, 2: 631}}


def test_parse_end_offsets_accepts_object_or_json_string_and_skips_other_sources():
    as_string = [{"endOffset": json.dumps({"t": {"0": 5, "1": 6}})}, {"endOffset": "not json"},
                 {"endOffset": None}, {"endOffset": {"t2": "bad"}}]
    assert parse_end_offsets(as_string) == {"t": {0: 5, 1: 6}}
    assert parse_end_offsets(PROGRESS["sources"]) == {"learner-events": {0: 650, 1: 640, 2: 630}}


def test_lag_is_latest_minus_committed_end_offset_per_partition():
    lag = compute_lag(parse_end_offsets(PROGRESS["sources"]), LATEST)
    assert lag == {"total": 51, "max_partition": 50,
                   "per_partition": {"learner-events[0]": 50, "learner-events[1]": 0, "learner-events[2]": 1}}
    # Offset trên broker nhỏ hơn (topic vừa bị xóa/tạo lại) không cho lag âm.
    assert compute_lag({"t": {0: 10}}, {"t": {0: 3}})["total"] == 0
    with pytest.raises(ValueError, match="thiếu end offset"):
        compute_lag({"t": {0: 10}}, {"t": {}})


def test_progress_record_extracts_watermark_drops_and_lag():
    window = {"window_ms": 60000, "count": 3, "system_ms": {"p95": 900.0}, "staleness_ms": {"p95": 1200.0}}
    record = progress_record("run-1", PROGRESS, latest_offsets=LATEST, batch={"foreach_ms": 120.5},
                             latency_1m=window, rss_bytes=123)
    assert record["batch_id"] == 7 and record["num_input_rows"] == 1659
    assert record["watermark_ms"] == iso_to_ms("2026-11-02T00:00:30.000Z") == 1_793_577_630_000
    assert record["dropped_by_watermark"] == 2 and record["dropped_duplicates"] == 31
    assert record["lag"]["total"] == 51 and record["lag_error"] is None
    assert record["end_offsets"] == {"learner-events": {"0": 650, "1": 640, "2": 630}}
    fields = metrics_fields(record)
    assert fields["lag_total"] == "51" and fields["p95_system_ms_1m"] == "900.0"
    assert fields["watermark_ms"] == "1793577630000" and fields["foreach_ms"] == "120.5"
    assert all(isinstance(v, str) for v in fields.values())


def test_no_watermark_yet_and_missing_partition_become_explicit_nulls():
    progress = {**PROGRESS, "eventTime": {"watermark": "1970-01-01T00:00:00.000Z"}}
    record = progress_record("run-1", progress, latest_offsets={"learner-events": {0: 700}})
    assert record["watermark_ms"] is None
    assert record["lag"] is None and "thiếu end offset" in record["lag_error"]
    assert metrics_fields(record)["lag_total"] == ""


def test_state_summary_tolerates_missing_fields():
    summary = state_summary([{"operatorName": "x"}])
    assert summary["dropped_by_watermark"] == 0 and summary["operators"][0]["num_rows_total"] == 0


def _cfg(tmp_path):
    return load_config(env={"REDIS_PASSWORD": "unit-test", "NHOM8_DATA_ROOT": str(tmp_path)}, dotenv_path=None)


def test_listener_writes_jsonl_and_metrics_hash(tmp_path):
    client = fakeredis.FakeRedis()
    window = LatencyWindow(clock_ms=lambda: 1_000)
    window.add(1_000, np.array([100, 200, 300]), np.array([150, 250, 350]))
    window.record_batch(7, {"batch_id": 7, "foreach_ms": 80.0})
    asked = []

    def latest(partitions):
        asked.append(partitions)
        return LATEST

    out = tmp_path / "results" / "progress.jsonl"
    listener = ProgressListener(_cfg(tmp_path), "run-1", redis_client=client, key_prefix="u:",
                                latest_offsets=latest, out_path=out, window=window, rss=lambda: 42)
    record = listener.handle_progress(PROGRESS)
    assert asked == [{"learner-events": [0, 1, 2]}]
    assert record["batch"] == {"batch_id": 7, "foreach_ms": 80.0}
    assert record["latency_1m"]["system_ms"]["p95"] == 300.0 and record["rss_bytes"] == 42
    lines = [json.loads(line) for line in out.read_text(encoding="utf-8").splitlines()]
    assert len(lines) == 1 and lines[0]["lag"]["total"] == 51
    metrics = client.hgetall(RedisKeys("u:").metrics_pipeline)
    assert metrics[b"lag_total"] == b"51" and metrics[b"batch_id"] == b"7" and metrics[b"rss_bytes"] == b"42"


def test_listener_survives_broker_and_redis_errors(tmp_path):
    def broken_latest(_partitions):
        raise ConnectionError("broker down")

    class BrokenRedis:
        def hset(self, *args, **kwargs):
            raise ConnectionError("redis down")

    out = tmp_path / "p.jsonl"
    listener = ProgressListener(_cfg(tmp_path), "run-1", redis_client=BrokenRedis(), latest_offsets=broken_latest,
                                out_path=out, window=LatencyWindow(), rss=lambda: None)
    record = listener.handle_progress(PROGRESS)
    assert record["lag"] is None and record["lag_error"].startswith("ConnectionError")
    assert listener.errors == 1 and out.exists()

    class Event:
        progress = None  # thiếu .json ⇒ lỗi bên trong callback, không ném ra Spark

    listener.onQueryProgress(Event())
    assert listener.errors == 2


def test_listener_follows_only_the_state_query_of_its_run(tmp_path):
    from ai_math_tutor.streaming.job import JobSettings

    out = tmp_path / "p.jsonl"
    listener = ProgressListener(_cfg(tmp_path), "run-1", redis_client=fakeredis.FakeRedis(),
                                latest_offsets=lambda _p: LATEST, out_path=out, window=LatencyWindow(),
                                rss=lambda: None)
    assert listener.query_name == JobSettings(run_id="run-1").query_name
    assert listener.handle_progress({**PROGRESS, "name": "dead_letter_run-1", "batchId": 99}) is None
    assert not out.exists()

    class Started:
        id, runId, timestamp = "q-dead", "r", "2026-11-02T00:00:00Z"
        name = "dead_letter_run-1"

    class Terminated:
        id, runId, exception = "q-dead", "r", None

    listener.onQueryStarted(Started())
    listener.onQueryTerminated(Terminated())
    assert not out.exists() and listener.errors == 0
    Started.id, Started.name, Terminated.id = "q-1", "learner_state_run-1", "q-1"
    listener.onQueryStarted(Started())
    listener.onQueryTerminated(Terminated())
    assert [json.loads(line)["event"] for line in out.read_text(encoding="utf-8").splitlines()] == [
        "started", "terminated"]
    every = ProgressListener(_cfg(tmp_path), "run-1", redis_client=fakeredis.FakeRedis(),
                             latest_offsets=lambda _p: LATEST, out_path=tmp_path / "all.jsonl",
                             window=LatencyWindow(), rss=lambda: None, query_name=None)
    assert every.handle_progress({**PROGRESS, "name": "anything"}) is not None


class _Closable:
    def __init__(self) -> None:
        self.closed = 0

    def close(self) -> None:
        self.closed += 1


def test_close_releases_only_clients_the_listener_created_and_ignores_late_events(tmp_path):
    out = tmp_path / "p.jsonl"
    injected = fakeredis.FakeRedis()
    borrowed = ProgressListener(_cfg(tmp_path), "run-1", redis_client=injected, latest_offsets=lambda _p: LATEST,
                                out_path=out, window=LatencyWindow(), rss=lambda: None)
    borrowed.close()
    injected.ping()  # client của người gọi vẫn dùng được
    assert borrowed.handle_progress(PROGRESS) is None and not out.exists()  # sự kiện tới muộn sau khi đóng

    owned = ProgressListener(_cfg(tmp_path), "run-1", out_path=out, window=LatencyWindow(), rss=lambda: None)
    redis_client, offsets = _Closable(), _Closable()
    owned._redis, owned._latest = redis_client, offsets  # như sau lần tạo lười đầu tiên
    owned.close()
    owned.close()  # đóng hai lần không lỗi, không đóng lại
    assert (redis_client.closed, offsets.closed) == (1, 1)
    assert owned._redis is None and owned._latest is None


def test_kafka_latest_offsets_refuses_calls_after_close():
    from ai_math_tutor.streaming.listener import KafkaLatestOffsets

    offsets = KafkaLatestOffsets.__new__(KafkaLatestOffsets)  # không tạo AdminClient thật (không cần broker)
    offsets._admin, offsets._timeout_s = object(), 1.0
    offsets.close()
    with pytest.raises(RuntimeError, match="đã đóng"):
        offsets({"learner-events": [0]})


def test_job_detaches_listener_then_closes_it_even_if_removal_fails(tmp_path):
    from ai_math_tutor.streaming.job import detach_listener

    calls = []

    class Listener:
        def close(self) -> None:
            calls.append("close")

    class Streams:
        def __init__(self, fail: bool) -> None:
            self.fail = fail

        def removeListener(self, listener) -> None:
            calls.append("remove")
            if self.fail:
                raise RuntimeError("session đã dừng")

    class Spark:
        def __init__(self, fail: bool) -> None:
            self.streams = Streams(fail)

    detach_listener(Spark(False), Listener())
    detach_listener(Spark(True), Listener())
    assert calls == ["remove", "close", "remove", "close"]
