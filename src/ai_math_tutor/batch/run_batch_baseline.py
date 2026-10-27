"""CLI batch baseline: chạy job hồ sơ tại mọi cutoff của một lịch, đánh giá so với ground truth, ghi run record.

Ví dụ (trong WSL):
    uv run python -m ai_math_tutor.batch.run_batch_baseline --schedule daily \\
        --events "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/events.parquet" \\
        --ground-truth "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/ground_truth.jsonl" --parity-check

Đầu ra:
- snapshot Parquet mỗi cutoff: `<out-root>/<run_id>/cutoff_ms=<c>/` (mặc định `$NHOM8_DATA_ROOT/batch`);
- kết quả thô nhỏ: `<results>/batch/<run_id>/{jobs.csv, episodes.csv, false_positives.csv, summary.json}`;
- log: `<results>/batch/logs/<run_id>.log`; một dòng run record (Phụ lục B) nối vào `<results>/runs.jsonl`.

Run lỗi hoặc quá `--timeout-s` vẫn ghi run record với `status` tương ứng (exit 1 / 2).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np

from ai_math_tutor.batch.evaluate import (
    EpisodeEvaluation,
    JobResult,
    evaluate_episodes,
    first_detections,
    load_ground_truth,
    read_header,
    summarize,
    visibility_latency,
    write_csv,
)
from ai_math_tutor.batch.schedule import SCHEDULES, Schedule, make_schedule
from ai_math_tutor.batch.source import FORMATS, EventSource, open_source
from ai_math_tutor.config import REPO_ROOT, load_config, validate_run_id
from ai_math_tutor.problem_bank import file_checksum
from ai_math_tutor.run_record import append_run_record, build_run_record
from ai_math_tutor.simulator.output import epoch_ms, iso_from_epoch_ms, parse_utc

IMPLEMENTATIONS = ("dataframe", "rdd")
DEFAULT_TIMEOUT_S = 600.0
JOB_COLUMNS = (
    "job_index", "cutoff_ms", "cutoff_iso", "runtime_s", "visible_ms", "rows_in", "rows_unique", "rows_out",
    "stuck", "consec_wrong_alert", "mastered", "with_onset", "hints", "new_detections",
)
EPISODE_COLUMNS = (
    "learner_id", "skill_id", "connection", "ability_group", "latent_mastered_at_onset", "true_onset_event_id",
    "true_onset_ms", "true_end_ms", "detected", "detect_cutoff_ms", "visible_ms", "time_to_detect_s",
    "detected_while_open", "onset_event_match", "trigger_rule_holds",
)
FALSE_POSITIVE_COLUMNS = ("learner_id", "skill_id", "detect_cutoff_ms", "visible_ms", "onset_event_id", "onset_event_time_ms")

log = logging.getLogger("ai_math_tutor.batch")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Batch baseline daily/hourly trên event log mô phỏng hoặc đã phát lại.")
    parser.add_argument("--schedule", choices=SCHEDULES, required=True)
    parser.add_argument("--events", type=Path, required=True, help="events.parquet (mô phỏng) hoặc JSONL hợp đồng")
    parser.add_argument("--ground-truth", type=Path, required=True)
    parser.add_argument("--format", choices=("auto", *FORMATS), default="auto")
    parser.add_argument("--replay-start-ms", type=int, default=None,
                        help="mốc phát lại (epoch ms): đóng dấu lại Parquet như bộ phát lại và dịch ground truth")
    parser.add_argument("--seed", type=int, default=None, help="seed cho pha lịch (mặc định: seed của ground truth)")
    parser.add_argument("--phase-s", type=float, default=None, help="ghi đè pha lịch (giây); mặc định lấy từ seed")
    parser.add_argument("--impl", choices=IMPLEMENTATIONS, default="dataframe")
    parser.add_argument("--partitions", type=int, default=None,
                        help="spark.sql.shuffle.partitions và số partition RDD (mặc định theo runtime.yaml)")
    parser.add_argument("--master", default=None, help="Spark master (mặc định theo runtime.yaml, local[*])")
    parser.add_argument("--parity-check", action="store_true", help="so DataFrame và RDD tại cutoff cuối")
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--out-root", type=Path, default=None, help="thư mục snapshot (mặc định $NHOM8_DATA_ROOT/batch)")
    parser.add_argument("--results", type=Path, default=None, help="thư mục kết quả thô (mặc định results/raw)")
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    return parser


def _setup_logging(log_path: Path) -> logging.Handler:
    log_path.parent.mkdir(parents=True, exist_ok=True)
    handler = logging.FileHandler(log_path, encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    if not any(isinstance(h, logging.StreamHandler) and not isinstance(h, logging.FileHandler) for h in log.handlers):
        log.addHandler(logging.StreamHandler(sys.stderr))
    log.setLevel(logging.INFO)
    return handler


def _data_version(source: EventSource) -> str:
    if source.path.is_file():
        return f"{source.path.name} {file_checksum(source.path)}"
    return f"dir:{source.path}"


def _time_base(source: EventSource, header: dict[str, Any]) -> tuple[int, int]:
    """(t0_ms, base_ms). t0 của ground truth phải khớp metadata Parquet; base = replay_start nếu có."""
    t0_ms = epoch_ms(parse_utc(str(header["t0"])))
    meta_t0 = source.t0_epoch_ms()
    if meta_t0 is not None and meta_t0 != t0_ms:
        raise ValueError(f"t0 của events ({meta_t0}) khác t0 của ground truth ({t0_ms}) — hai tệp không cùng một run")
    return t0_ms, source.replay_start_ms if source.replay_start_ms is not None else t0_ms


def _compute_profiles(spark: Any, impl: str, source: EventSource, cutoff_ms: int, partitions: int) -> Any:
    if impl == "dataframe":
        from ai_math_tutor.batch.daily_profile import profiles_dataframe

        return profiles_dataframe(spark, source, cutoff_ms)
    from ai_math_tutor.batch.mapreduce_profile import profiles_mapreduce

    return profiles_mapreduce(spark, source, cutoff_ms, partitions)


def run_jobs(
    spark: Any,
    *,
    source: EventSource,
    schedule: Schedule,
    impl: str,
    partitions: int,
    snapshot_root: Path,
    deadline: float,
    inventory: dict[str, Any],
) -> tuple[list[JobResult], list[dict[str, Any]], str]:
    """Chạy job tại từng cutoff (đo runtime đọc → tính → ghi); dừng với status timeout khi quá hạn."""
    from ai_math_tutor.batch.snapshot import finalize_snapshot, hint_table, read_snapshot, snapshot_path, write_snapshot

    sent = inventory["delivery_sent_ms"]
    unique_sent = np.sort(inventory["unique_sent_ms"])
    delivery_sent = np.sort(sent)
    cutoffs = schedule.cutoffs_covering(int(delivery_sent[0]), int(delivery_sent[-1]))
    log.info("lịch %s: %d cutoff, pha %.0f s (%s)", schedule.name, len(cutoffs), schedule.phase_ms / 1000, schedule.phase_source)
    hints = hint_table(spark).cache()
    jobs: list[JobResult] = []
    rows: list[dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    status = "success"
    for index, cutoff in enumerate(cutoffs):
        if time.monotonic() > deadline:
            status = "timeout"
            log.warning("quá timeout trước job %d/%d", index + 1, len(cutoffs))
            break
        path = snapshot_path(snapshot_root, cutoff)
        started = time.perf_counter()
        profiles = _compute_profiles(spark, impl, source, cutoff, partitions)
        write_snapshot(finalize_snapshot(profiles, cutoff, hints), path)
        runtime = time.perf_counter() - started
        info = read_snapshot(spark, path)
        job = JobResult(cutoff_ms=cutoff, runtime_s=runtime, detections=tuple(info["detections"]))
        jobs.append(job)
        keys = {(d["learner_id"], d["skill_id"]) for d in info["detections"]}
        rows.append({
            "job_index": index,
            "cutoff_ms": cutoff,
            "cutoff_iso": iso_from_epoch_ms(cutoff),
            "runtime_s": round(runtime, 3),
            "visible_ms": job.visible_ms,
            "rows_in": int(np.searchsorted(delivery_sent, cutoff, side="right")),
            "rows_unique": int(np.searchsorted(unique_sent, cutoff, side="right")),
            "rows_out": info["rows_out"],
            "stuck": info["stuck"],
            "consec_wrong_alert": info["consec_wrong_alert"],
            "mastered": info["mastered"],
            "with_onset": info["with_onset"],
            "hints": info["hints"],
            "new_detections": len(keys - seen),
        })
        seen |= keys
        log.info("job %d/%d cutoff %s: %.2f s, %d hồ sơ, %d bế tắc", index + 1, len(cutoffs),
                 iso_from_epoch_ms(cutoff), runtime, info["rows_out"], info["stuck"])
    hints.unpersist()
    return jobs, rows, status


def _parity(spark: Any, source: EventSource, cutoff_ms: int, partitions: int) -> dict[str, Any]:
    from ai_math_tutor.batch.daily_profile import profiles_dataframe
    from ai_math_tutor.batch.mapreduce_profile import profiles_mapreduce
    from ai_math_tutor.batch.snapshot import compare_profiles

    started = time.perf_counter()
    result = compare_profiles(
        profiles_dataframe(spark, source, cutoff_ms), profiles_mapreduce(spark, source, cutoff_ms, partitions)
    )
    return {**result, "cutoff_ms": cutoff_ms, "seconds": round(time.perf_counter() - started, 3)}


def _evaluate(
    jobs: list[JobResult], schedule: Schedule, gt_path: Path, base_ms: int, inventory: dict[str, Any]
) -> tuple[EpisodeEvaluation, dict[str, Any]]:
    truth = load_ground_truth(gt_path, base_ms)
    evaluation = evaluate_episodes(truth.episodes, first_detections(jobs), schedule)
    affects = inventory["unique_affects_profile"]
    staleness = visibility_latency(
        inventory["unique_event_ms"][affects],
        inventory["unique_sent_ms"][affects],
        [job.cutoff_ms for job in jobs],
        [job.runtime_s for job in jobs],
    )
    return evaluation, staleness


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    wall_start = time.perf_counter()
    started_at = datetime.now(timezone.utc)
    cfg = load_config()
    source = open_source(args.events, args.format, args.replay_start_ms)
    gt_path = args.ground_truth.expanduser().resolve()
    header = read_header(gt_path)
    t0_ms, base_ms = _time_base(source, header)
    seed = args.seed if args.seed is not None else int(header["seed"])
    schedule = make_schedule(args.schedule, base_ms, seed, args.phase_s)
    experiment = f"batch-{schedule.name}"
    run_id = validate_run_id(args.run_id or f"{experiment}-{started_at:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}")
    results_root = (args.results or cfg.paths.results).expanduser().resolve()
    raw_dir = results_root / "batch" / run_id
    snapshot_root = (args.out_root or cfg.paths.data_root / "batch").expanduser().resolve() / run_id
    log_path = results_root / "batch" / "logs" / f"{run_id}.log"
    if raw_dir.exists() or snapshot_root.exists():
        raise SystemExit(f"run_id đã tồn tại: {run_id}")
    handler = _setup_logging(log_path)

    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(args.master, args.partitions)
    partitions = int(conf["spark.sql.shuffle.partitions"])
    status, error = "success", None
    jobs: list[JobResult] = []
    job_rows: list[dict[str, Any]] = []
    metrics: dict[str, Any] = {}
    parity: dict[str, Any] | None = None
    digest: str | None = None
    inventory: dict[str, Any] | None = None
    timings: dict[str, float] = {}
    deadline = time.monotonic() + args.timeout_s
    log.info("run %s: %s, impl %s, %d partition, events %s", run_id, experiment, args.impl, partitions, source.path)
    try:
        with spark_session(conf) as spark:
            timings["spark_startup_s"] = time.perf_counter() - wall_start
            mark = time.perf_counter()
            from ai_math_tutor.batch.daily_profile import delivery_inventory
            from ai_math_tutor.batch.snapshot import profile_digest

            inventory = delivery_inventory(spark, source)
            timings["inventory_s"] = time.perf_counter() - mark
            if inventory["delivery_sent_ms"].size == 0:
                raise ValueError(f"không có lần giao hợp lệ nào trong {source.path}")
            jobs, job_rows, status = run_jobs(
                spark, source=source, schedule=schedule, impl=args.impl, partitions=partitions,
                snapshot_root=snapshot_root, deadline=deadline, inventory=inventory,
            )
            if jobs:
                digest = profile_digest(spark, snapshot_root / f"cutoff_ms={jobs[-1].cutoff_ms}")
                if args.parity_check:
                    parity = _parity(spark, source, jobs[-1].cutoff_ms, partitions)
                    log.info("parity DataFrame/RDD tại cutoff cuối: %s", parity)
                    if not parity["identical"]:
                        status, error = "failed", "DataFrame và RDD cho hồ sơ khác nhau"
    except Exception as exc:  # run lỗi vẫn phải có run record
        status, error = "failed", repr(exc)
        log.exception("run %s thất bại", run_id)

    evaluation: EpisodeEvaluation | None = None
    staleness: dict[str, Any] | None = None
    if jobs and inventory is not None:
        try:
            evaluation, staleness = _evaluate(jobs, schedule, gt_path, base_ms, inventory)
        except Exception as exc:
            status, error = "failed", repr(exc)
            log.exception("đánh giá thất bại")
    runtime_s = time.perf_counter() - wall_start
    last = job_rows[-1] if job_rows else None
    metrics = {
        "runtime_s_total": round(runtime_s, 3),
        **{k: round(v, 3) for k, v in timings.items()},
        "jobs": len(jobs),
        "cutoffs_first_iso": job_rows[0]["cutoff_iso"] if job_rows else None,
        "cutoffs_last_iso": last["cutoff_iso"] if last else None,
        "job_runtime_s": summarize([job.runtime_s for job in jobs]),
        "job_runtime_total_s": round(sum(job.runtime_s for job in jobs), 3),
        "rows_in": inventory["deliveries"] if inventory else None,
        "rows_invalid": inventory["invalid"] if inventory else None,
        "rows_heartbeat": inventory["heartbeats"] if inventory else None,
        "rows_unique": int(inventory["unique_sent_ms"].size) if inventory else None,
        "rows_out_last": last["rows_out"] if last else None,
        "final_snapshot": {k: last[k] for k in ("stuck", "consec_wrong_alert", "mastered", "hints")} if last else None,
        "episodes": evaluation.summary if evaluation else None,
        "staleness": staleness,
        "profile_digest": digest,
        "parity": parity,
        "error": error,
    }
    raw_dir.mkdir(parents=True, exist_ok=True)
    write_csv(raw_dir / "jobs.csv", job_rows, JOB_COLUMNS)
    if evaluation is not None:
        write_csv(raw_dir / "episodes.csv", evaluation.episodes, EPISODE_COLUMNS)
        write_csv(raw_dir / "false_positives.csv", evaluation.false_positives, FALSE_POSITIVE_COLUMNS)
    parameters = {
        "schedule": schedule.describe(),
        "impl": args.impl,
        "partitions": partitions,
        "events": str(source.path),
        "format": source.format,
        "ground_truth": str(gt_path),
        "t0_ms": t0_ms,
        "time_base_ms": base_ms,
        "replay_start_ms": source.replay_start_ms,
        "restamped": source.restamps,
        "parity_check": args.parity_check,
        "snapshot_root": str(snapshot_root),
        "raw_dir": str(raw_dir),
    }
    (raw_dir / "summary.json").write_text(
        json.dumps({"run_id": run_id, "status": status, "parameters": parameters, "metrics": metrics},
                   ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )
    record = build_run_record(
        experiment=experiment,
        data_version=_data_version(source),
        parameters=parameters,
        seed=seed,
        timeout_s=args.timeout_s,
        metrics=metrics,
        status=status,
        log_path=str(log_path),
        started_at=started_at,
        repo_dir=REPO_ROOT,
        run_id=run_id,
        runtime_s=runtime_s,
        spark_conf=conf,
        images={},
    )
    append_run_record(record, results_root / "runs.jsonl")
    episodes = metrics["episodes"] or {}
    log.info("run %s %s sau %.1f s: recall %s, precision %s, TTD P50 %s s", run_id, status, runtime_s,
             episodes.get("recall"), episodes.get("precision"), (episodes.get("time_to_detect_s") or {}).get("p50"))
    log.removeHandler(handler)
    handler.close()
    print(json.dumps({"run_id": run_id, "status": status, "raw_dir": str(raw_dir), "metrics": metrics}, ensure_ascii=False))
    return {"success": 0, "timeout": 2}.get(status, 1)


if __name__ == "__main__":
    raise SystemExit(main())
