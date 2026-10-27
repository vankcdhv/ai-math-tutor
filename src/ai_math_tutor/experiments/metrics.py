"""Chỉ số của một run thí nghiệm, tính từ lake (`latency/`), `progress.jsonl`, kết quả phát lại và đối soát.

Quy ước (docs/experiment-protocol.md):
- **Warm-up:** sự kiện có `sent_time` trong `warmup_s` giây đầu kể từ `replay_start` bị loại khỏi mọi
  chỉ số độ trễ/throughput; cửa sổ đo là `[replay_start + warmup, cuối phần phát lại]`.
- **Hai định nghĩa độ trễ**, cùng đo tại foreachBatch sau khi mọi lệnh Redis của batch xong:
  độ trễ hệ thống = `visible − sent_time` (SLO); độ cũ trạng thái = `visible − event_time`.
- **Processing lag** = end offset mới nhất trên broker − offset batch vừa đọc tới (listener). Throughput
  là **bền vững** khi độ dốc hồi quy tuyến tính của lag trong `window_s` giây cuối phần phát lại
  ≤ `slope_ratio` × tốc độ mục tiêu (lag không tăng; định nghĩa của Karimov et al.).
- Phân vị dùng nearest-rank như listener và reconcile (`sinks.nearest_rank`).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.compute as pc

from ai_math_tutor.streaming.sinks import nearest_rank

LATENCY_COLUMNS = ("system_latency_ms", "staleness_ms")


def distribution(values: np.ndarray) -> dict[str, Any]:
    array = np.asarray(values, dtype=np.float64)
    if array.size == 0:
        return {"n": 0, "p50": None, "p95": None, "p99": None, "max": None, "mean": None}
    return {"n": int(array.size), "p50": nearest_rank(array, 0.50), "p95": nearest_rank(array, 0.95),
            "p99": nearest_rank(array, 0.99), "max": float(array.max()), "mean": float(array.mean())}


def latency_after_warmup(latency: pa.Table, *, replay_start_ms: int, warmup_s: float) -> dict[str, Any]:
    """P50/P95/P99 của hai loại độ trễ trên sự kiện gửi sau warm-up (một dòng lake = một sự kiện đã áp)."""
    cutoff = replay_start_ms + round(warmup_s * 1000)
    if latency.num_rows:
        kept = latency.filter(pc.greater_equal(latency.column("sent_time_ms"), cutoff))
    else:
        kept = latency
    return {
        "warmup_s": warmup_s,
        "events_total": latency.num_rows,
        "events_excluded_warmup": latency.num_rows - kept.num_rows,
        "system_ms": distribution(kept.column("system_latency_ms").to_numpy(zero_copy_only=False)
                                  if kept.num_rows else np.empty(0)),
        "staleness_ms": distribution(kept.column("staleness_ms").to_numpy(zero_copy_only=False)
                                     if kept.num_rows else np.empty(0)),
    }


def progress_batches(progress: Sequence[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return [p for p in progress if p.get("event") == "progress" and p.get("timestamp_ms") is not None]


def measured_ms(record: Mapping[str, Any]) -> int:
    """Lúc listener đo lag của batch: bắt đầu batch + thời lượng batch."""
    return int(record["timestamp_ms"]) + int(record.get("batch_duration_ms") or 0)


def throughput(latency: pa.Table, progress: Sequence[Mapping[str, Any]], *, start_ms: int,
               end_ms: int) -> dict[str, Any]:
    """Sự kiện áp/giây (lake, theo `visible_ms`) và bản ghi vào/giây (Spark) trong cửa sổ đo."""
    span_s = (end_ms - start_ms) / 1000
    if span_s <= 0:
        return {"window_s": span_s, "applied_events_per_s": None, "input_rows_per_s": None,
                "processed_rows_per_s_p50": None}
    applied = 0
    if latency.num_rows:
        visible = latency.column("visible_ms")
        mask = pc.and_(pc.greater_equal(visible, start_ms), pc.less_equal(visible, end_ms))
        applied = int(pc.sum(pc.cast(mask, pa.int64())).as_py() or 0)
    batches = [p for p in progress_batches(progress) if start_ms <= int(p["timestamp_ms"]) <= end_ms]
    rows = sum(int(p.get("num_input_rows") or 0) for p in batches)
    processed = np.array([p["processed_rows_per_s"] for p in batches
                          if p.get("processed_rows_per_s") is not None and int(p.get("num_input_rows") or 0) > 0],
                         dtype=np.float64)
    return {
        "window_s": round(span_s, 3),
        "applied_events_per_s": round(applied / span_s, 3),
        "input_rows_per_s": round(rows / span_s, 3),
        "processed_rows_per_s_p50": nearest_rank(processed, 0.5) if processed.size else None,
        "batches": len(batches),
    }


def lag_trend(progress: Sequence[Mapping[str, Any]], *, start_ms: int, end_ms: int, target_rate: float,
              window_s: float = 300, slope_ratio: float = 0.01) -> dict[str, Any]:
    """Lag trong cửa sổ đo + độ dốc ở `window_s` giây cuối ⇒ throughput có bền vững không."""
    points = [(measured_ms(p), int(p["lag"]["total"])) for p in progress_batches(progress)
              if isinstance(p.get("lag"), Mapping) and p["lag"].get("total") is not None
              and start_ms <= measured_ms(p) <= end_ms]
    if not points:
        return {"samples": 0, "max": None, "final": None, "window_s": window_s, "slope_events_per_s": None,
                "threshold_events_per_s": slope_ratio * target_rate, "sustainable": None}
    tail_from = end_ms - window_s * 1000
    tail = [(t, lag) for t, lag in points if t >= tail_from]
    slope = None
    if len(tail) >= 3 and tail[-1][0] > tail[0][0]:
        times = np.array([t for t, _ in tail], dtype=np.float64) / 1000
        lags = np.array([lag for _, lag in tail], dtype=np.float64)
        slope = float(np.polyfit(times - times[0], lags, 1)[0])
    threshold = slope_ratio * target_rate
    return {
        "samples": len(points),
        "max": max(lag for _, lag in points),
        "final": points[-1][1],
        "mean": float(np.mean([lag for _, lag in points])),
        "window_s": window_s,
        "tail_samples": len(tail),
        "slope_events_per_s": None if slope is None else round(slope, 4),
        "threshold_events_per_s": threshold,
        "sustainable": None if slope is None else slope <= threshold,
    }


def state_rows(progress: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Số dòng trạng thái (tổng `numRowsTotal` các operator) ở batch cuối và lớn nhất trong run."""
    totals = [sum(int(op.get("num_rows_total") or 0) for op in (p.get("state") or []))
              for p in progress_batches(progress)]
    return {"final": totals[-1] if totals else None, "max": max(totals) if totals else None}


def _window_median(points: Sequence[tuple[int, float]], lo: float, hi: float) -> float | None:
    values = [v for t, v in points if lo <= t <= hi]
    return float(np.median(values)) if values else None


def rss_trend(progress: Sequence[Mapping[str, Any]], *, start_ms: int, end_ms: int,
              window_s: float = 300) -> dict[str, Any]:
    """RSS driver + JVM (listener ghi mỗi batch): trung vị cửa sổ đầu/cuối, độ dốc MB/phút."""
    points = [(int(p["timestamp_ms"]), float(p["rss_bytes"])) for p in progress_batches(progress)
              if p.get("rss_bytes") and start_ms <= int(p["timestamp_ms"]) <= end_ms]
    if len(points) < 2:
        return {"samples": len(points), "first_window_median_bytes": None, "last_window_median_bytes": None,
                "growth_ratio": None, "slope_mb_per_min": None, "peak_bytes": max((v for _, v in points), default=None)}
    first = _window_median(points, start_ms, start_ms + window_s * 1000)
    last = _window_median(points, end_ms - window_s * 1000, end_ms)
    times = np.array([t for t, _ in points], dtype=np.float64) / 60_000
    values = np.array([v for _, v in points], dtype=np.float64) / 1_048_576
    slope = float(np.polyfit(times - times[0], values, 1)[0])
    return {
        "samples": len(points),
        "first_window_median_bytes": first,
        "last_window_median_bytes": last,
        "growth_ratio": round(last / first, 4) if first and last else None,
        "slope_mb_per_min": round(slope, 3),
        "peak_bytes": max(v for _, v in points),
    }


def latency_1m_windows(progress: Sequence[Mapping[str, Any]], *, start_ms: int, end_ms: int,
                       slo_ms: float) -> dict[str, Any]:
    """P95 trượt 1 phút (listener) theo thời gian: lớn nhất, trung vị, tỉ lệ cửa sổ vượt SLO."""
    values = []
    for p in progress_batches(progress):
        window = p.get("latency_1m") or {}
        p95 = (window.get("system_ms") or {}).get("p95")
        if p95 is not None and start_ms <= int(p["timestamp_ms"]) <= end_ms:
            values.append(float(p95))
    if not values:
        return {"samples": 0, "p95_system_ms_max": None, "p95_system_ms_median": None, "share_over_slo": None,
                "slo_ms": slo_ms}
    over = sum(1 for v in values if v > slo_ms)
    return {"samples": len(values), "p95_system_ms_max": max(values), "p95_system_ms_median": float(np.median(values)),
            "windows_over_slo": over, "share_over_slo": round(over / len(values), 4), "slo_ms": slo_ms}


def drained(record: Mapping[str, Any] | None, *, end_of_log_ms: int | None, timed: bool,
            replay_finished_ms: int) -> bool:
    """Job đã đọc hết topic (lag 0, đo sau khi bộ phát lại xong) và — nếu có watermark — watermark ≥ cuối log."""
    if not record or record.get("event") != "progress":
        return False
    lag = record.get("lag") or {}
    if lag.get("total") != 0 or measured_ms(record) < replay_finished_ms:
        return False
    if not timed:
        return True
    watermark = record.get("watermark_ms")
    return end_of_log_ms is not None and watermark is not None and int(watermark) >= end_of_log_ms


def rate_validity(replay: Mapping[str, Any], ok_ratio: float = 0.95) -> dict[str, Any]:
    """Tốc độ gửi thực so với lịch của cửa sổ phát lại; dưới `ok_ratio` ⇒ run không hợp lệ."""
    ratio = replay.get("rate_ratio")
    return {"target_eps": replay.get("target_rate_eps"), "achieved_eps": replay.get("achieved_rate_eps"),
            "acked_eps": replay.get("acked_rate_eps"), "ratio": ratio,
            "ok": ratio is not None and ratio >= ok_ratio, "ok_ratio": ok_ratio}


def reconcile_summary(report: Mapping[str, Any]) -> dict[str, Any]:
    """Phần của reconcile.json đưa vào run record (bản đầy đủ nằm cạnh run record)."""
    dropped = report.get("dropped") or {}
    alerts = report.get("alerts") or {}
    truth = alerts.get("vs_ground_truth") or {}
    state = report.get("state_vs_oracle") or {}
    return {
        "status": report.get("status"),
        "checks": report.get("checks"),
        "dropped": {key: dropped.get(key) for key in (
            "deliveries", "unique_events", "injected_duplicates", "applied_unique", "total", "rate", "by_connection",
            "late_events_by_connection", "spark_dropped_by_watermark_rows", "spark_dropped_duplicate_rows")},
        "state_vs_oracle": {key: state.get(key) for key in (
            "pairs_expected", "matched", "accuracy", "missing_in_redis", "extra_in_redis")},
        "state_vs_latent": report.get("state_vs_latent"),
        "alerts": {"unique_alert_ids": alerts.get("unique_alert_ids"),
                   "duplicate_stream_entries": alerts.get("duplicate_stream_entries"),
                   "by_type": alerts.get("by_type"), "vs_oracle": alerts.get("vs_oracle"),
                   "episodes_in_window": truth.get("episodes_in_window"), "recall": truth.get("recall"),
                   "time_to_detect_system_ms": truth.get("time_to_detect_system_ms"),
                   "time_to_detect_staleness_ms": truth.get("time_to_detect_staleness_ms")},
        "latency_all_events": report.get("latency"),
        "final_watermark_ms": report.get("final_watermark_ms"),
        "end_of_log_ms": report.get("end_of_log_ms"),
    }


def decide_status(*, interrupted: bool, error: str | None, timed_out: bool, replay_status: str | None,
                  rate_ok: bool | None, job_exit: int | None, require_rate: bool = True) -> tuple[str, list[str]]:
    """Trạng thái run record + lý do không hợp lệ (run không hợp lệ vẫn được ghi, không bị xóa)."""
    if interrupted:
        return "aborted", ["runner bị dừng giữa run"]
    reasons: list[str] = []
    if error:
        reasons.append(error)
    if timed_out:
        return "timeout", reasons or ["không drain được trước hạn"]
    if replay_status not in (None, "success"):
        reasons.append(f"bộ phát lại: {replay_status}")
    if require_rate and rate_ok is False:
        reasons.append("tốc độ gửi thực < 95% mục tiêu")
    if job_exit not in (None, 0):
        reasons.append(f"job thoát mã {job_exit}")
    if replay_status is None and not reasons:
        reasons.append("bộ phát lại không có kết quả")
    return ("failed", reasons) if reasons else ("success", [])
