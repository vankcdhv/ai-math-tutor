"""Chỉ số của run: P95 bỏ warm-up, hai định nghĩa thời gian, lag bền vững vs tăng dần, RSS, drain, trạng thái run."""

from __future__ import annotations

import pyarrow as pa
import pytest

from ai_math_tutor.experiments import metrics as X
from ai_math_tutor.streaming.sinks import LATENCY_SCHEMA

START = 1_800_000_000_000


def _latency(rows: list[tuple[int, int, int]]) -> pa.Table:
    """(event_time_ms, sent_time_ms, visible_ms) → bảng đúng schema lake `latency/`."""
    columns = {name: [] for name in LATENCY_SCHEMA.names}
    for i, (event_ms, sent_ms, visible_ms) in enumerate(rows):
        values = {"event_id": f"e{i}", "learner_id": "L-0000000000000001", "class_id": "C-A",
                  "event_time_ms": event_ms, "sent_time_ms": sent_ms, "visible_ms": visible_ms,
                  "system_latency_ms": visible_ms - sent_ms, "staleness_ms": visible_ms - event_ms,
                  "out_of_order": False, "batch_id": i}
        for name in LATENCY_SCHEMA.names:
            columns[name].append(values[name])
    return pa.table(columns, schema=LATENCY_SCHEMA)


def test_p95_excludes_warmup_and_reports_both_time_definitions():
    rows = []
    # 60 s warm-up: độ trễ hệ thống 30 s (JVM nguội) — phải bị loại.
    for i in range(50):
        sent = START + i * 1000
        rows.append((sent - 200, sent, sent + 30_000))
    # sau warm-up: hệ thống 100..199 ms; độ cũ = hệ thống + 2 s độ trễ mạng mô phỏng.
    for i in range(100):
        sent = START + 60_000 + i * 100
        rows.append((sent - 2000, sent, sent + 100 + i))
    out = X.latency_after_warmup(_latency(rows), replay_start_ms=START, warmup_s=60)
    assert out["events_excluded_warmup"] == 50 and out["system_ms"]["n"] == 100
    assert out["system_ms"]["p95"] == 194 and out["system_ms"]["max"] == 199
    assert out["staleness_ms"]["p95"] == 2194
    no_warmup = X.latency_after_warmup(_latency(rows), replay_start_ms=START, warmup_s=0)
    assert no_warmup["system_ms"]["p95"] == 30_000


def test_empty_latency_table_gives_empty_distribution():
    out = X.latency_after_warmup(LATENCY_SCHEMA.empty_table(), replay_start_ms=START, warmup_s=60)
    assert out["system_ms"] == {"n": 0, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}


def _progress(points: list[tuple[int, int]], rows: int = 100, **extra) -> list[dict]:
    """(t_ms, lag) → bản ghi listener tối thiểu (batch 0 ms ⇒ lag đo tại t)."""
    return [{"event": "progress", "timestamp_ms": t, "batch_duration_ms": 0, "lag": {"total": lag},
             "num_input_rows": rows, "processed_rows_per_s": 1000.0, **extra} for t, lag in points]


def test_lag_flat_is_sustainable_and_growing_is_not():
    flat = _progress([(START + i * 1000, 500 + (i % 3) * 10) for i in range(600)])
    out = X.lag_trend(flat, start_ms=START, end_ms=START + 600_000, target_rate=1000, window_s=300)
    assert out["sustainable"] is True and abs(out["slope_events_per_s"]) < 1
    growing = _progress([(START + i * 1000, 50 * i) for i in range(600)])  # +50 sự kiện/s = 5% của 1K ev/s
    out = X.lag_trend(growing, start_ms=START, end_ms=START + 600_000, target_rate=1000, window_s=300)
    assert out["sustainable"] is False and out["slope_events_per_s"] == pytest.approx(50, rel=1e-6)
    assert out["max"] == 50 * 599 and out["final"] == 50 * 599
    few = X.lag_trend(_progress([(START, 0), (START + 1000, 0)]), start_ms=START, end_ms=START + 2000,
                      target_rate=1000)
    assert few["sustainable"] is None


def test_lag_window_ignores_batches_outside_the_measurement_window():
    progress = _progress([(START - 5000, 10**6)] + [(START + i * 1000, 0) for i in range(10)])
    out = X.lag_trend(progress, start_ms=START, end_ms=START + 10_000, target_rate=100, window_s=300)
    assert out["max"] == 0 and out["sustainable"] is True


def test_throughput_counts_visible_events_in_window():
    rows = [(START + i * 10, START + i * 10, START + i * 10 + 50) for i in range(1000)]  # 100 ev/s, 10 s
    progress = _progress([(START + i * 1000, 0) for i in range(10)], rows=100)
    out = X.throughput(_latency(rows), progress, start_ms=START, end_ms=START + 10_000)
    assert out["applied_events_per_s"] == pytest.approx(100, rel=0.01)
    assert out["input_rows_per_s"] == pytest.approx(100, rel=0.01)


def test_rss_trend_detects_growth_and_soak_windows_over_slo():
    progress = [{"event": "progress", "timestamp_ms": START + i * 60_000, "rss_bytes": (2000 + 10 * i) * 1_048_576,
                 "latency_1m": {"system_ms": {"p95": 6000 if i % 10 == 0 else 2000}}} for i in range(30)]
    rss = X.rss_trend(progress, start_ms=START, end_ms=START + 29 * 60_000, window_s=300)
    assert rss["slope_mb_per_min"] == pytest.approx(10, rel=1e-6) and rss["growth_ratio"] > 1.1
    soak = X.latency_1m_windows(progress, start_ms=START, end_ms=START + 29 * 60_000, slo_ms=5000)
    assert soak["windows_over_slo"] == 3 and soak["share_over_slo"] == pytest.approx(0.1)


def test_state_rows_sum_operators_of_last_batch():
    progress = [{"event": "progress", "timestamp_ms": START, "state": [{"num_rows_total": 5}, {"num_rows_total": 7}]},
                {"event": "progress", "timestamp_ms": START + 1, "state": [{"num_rows_total": 9}]}]
    assert X.state_rows(progress) == {"final": 9, "max": 12}


def test_drained_requires_zero_lag_measured_after_replay_and_watermark_past_end():
    end = START + 60_000
    finished = START + 61_000
    ok = {"event": "progress", "timestamp_ms": finished + 10, "batch_duration_ms": 5, "lag": {"total": 0},
          "watermark_ms": end + 1000}
    assert X.drained(ok, end_of_log_ms=end, timed=True, replay_finished_ms=finished)
    assert not X.drained({**ok, "lag": {"total": 3}}, end_of_log_ms=end, timed=True, replay_finished_ms=finished)
    assert not X.drained({**ok, "watermark_ms": end - 1}, end_of_log_ms=end, timed=True, replay_finished_ms=finished)
    early = {**ok, "timestamp_ms": finished - 500, "batch_duration_ms": 100}
    assert not X.drained(early, end_of_log_ms=end, timed=True, replay_finished_ms=finished)
    # Chế độ none: không có watermark, chỉ cần đọc hết topic.
    assert X.drained({**ok, "watermark_ms": None}, end_of_log_ms=end, timed=False, replay_finished_ms=finished)
    assert not X.drained(None, end_of_log_ms=end, timed=False, replay_finished_ms=finished)


def test_rate_validity_threshold():
    assert X.rate_validity({"rate_ratio": 0.95})["ok"] is True
    assert X.rate_validity({"rate_ratio": 0.9499})["ok"] is False
    assert X.rate_validity({})["ok"] is False


@pytest.mark.parametrize("kwargs, status", [
    ({}, "success"),
    ({"interrupted": True}, "aborted"),
    ({"timed_out": True}, "timeout"),
    ({"rate_ok": False}, "failed"),
    ({"replay_status": "failed"}, "failed"),
    ({"job_exit": 1}, "failed"),
    ({"error": "RunnerError: x"}, "failed"),
    ({"replay_status": None}, "failed"),
    ({"rate_ok": False, "require_rate": False}, "success"),
])
def test_decide_status(kwargs, status):
    base = {"interrupted": False, "error": None, "timed_out": False, "replay_status": "success", "rate_ok": True,
            "job_exit": 0}
    got, reasons = X.decide_status(**{**base, **kwargs})
    assert got == status
    assert (reasons == []) == (status == "success")
