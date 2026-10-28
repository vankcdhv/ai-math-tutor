"""Thí nghiệm khả năng mở rộng của job MapReduce: 3 quy mô dữ liệu × 2 cấu hình tài nguyên × 3 lần lặp.

    uv run python -m ai_math_tutor.experiments.mapreduce_matrix --dry-run     # kế hoạch, cỡ dữ liệu, ước lượng phút
    uv run python -m ai_math_tutor.experiments.mapreduce_matrix               # chạy (resume: bỏ run đã success)
    uv run python -m ai_math_tutor.experiments.mapreduce_matrix --scales 10000 --configs B   # một kịch bản
    uv run python -m ai_math_tutor.experiments.mapreduce_matrix --summarize-only

**Dữ liệu.** D1/D2/D3 = 100/1.000/10.000 người học, cùng tốc độ mỗi người học (mặc định 0,1 sự kiện/s, tức
`rate = 0,1 × learners`) và cùng thời lượng, cùng hồ sơ muộn (`low`) và tỉ lệ gửi trùng (2%), cùng seed. Bộ mô phỏng
sinh một lần vào cache `$NHOM8_DATA_ROOT/exp-sim/<khóa>/` (`experiments.datasets.ensure_dataset`). Đầu vào của job
là `deliveries.jsonl` (một lần giao mỗi dòng, đúng thứ tự truyền) dưới `$NHOM8_DATA_ROOT/mapreduce/input/<khóa>/`,
có thêm hai loại bản ghi tất định theo seed để kiểm đường lỗi: (1) với xác suất `--invalid-rate` sau mỗi lần giao,
một bản sao bị làm hỏng theo một trong các lý do của `INVALID_KINDS` (giữ nguyên event_id gốc, trừ khi JSON bị cắt);
(2) `--heartbeats` heartbeat v0.2 ở cuối log như bộ phát lại. Bản ghi chèn thêm không đổi tập sự kiện hợp lệ, nên
bộ đếm vẫn so được với manifest của bộ mô phỏng. Bản tham chiếu thuần Python tính một lần mỗi bộ dữ liệu và lưu cache.

**Cấu hình.** A = 1 worker × (2 lõi, 4 GB), B = 2 worker × (2 lõi, 4 GB), cùng số partition và cùng job
(`batch.mapreduce_run`). Mỗi cấu hình chạy trong một tiến trình con với một SparkSession dùng lại cho mọi run; run
đầu tiên là warm-up trên bộ dữ liệu nhỏ nhất, ghi ở `metrics/warmup/` và không vào bảng tổng hợp.

**Minh chứng** dưới `results/mapreduce/`: `configs/` (kịch bản, Spark conf thật), `raw_logs/` (log từng run, log
session và executor, dump REST, mẫu tài nguyên; nén gzip), `metrics/` (một JSON mỗi run, `summary.csv`), `figures/`,
`error_analysis/`. Mỗi run đo ghi thêm một run record (`experiment = batch-mapreduce`) vào `results/raw/runs.jsonl`.
"""

from __future__ import annotations

import argparse
import gzip
import hashlib
import json
import math
import os
import random
import shutil
import signal
import statistics
import subprocess
import sys
import threading
import time
import uuid
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ai_math_tutor.batch.mapreduce_problems import DEFAULT_TOP_K, reference_counts_from_file
from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.event_validation import DEFAULT_LATE_THRESHOLD_S, HEARTBEAT_EVENT_TYPE, HEARTBEAT_LEARNER_ID
from ai_math_tutor.experiments.matrix import PROBLEM_DEFAULTS, dataset_key

SCALES = (100, 1000, 10000)
CONFIG_NAMES = ("A", "B")
PER_LEARNER_RATE = 0.1
DEFAULT_DURATION_S = 1200
DATASET_SEED = 11
LATE_PROFILE = "low"
DUPLICATE_RATE = 0.02
DEFAULT_INVALID_RATE = 0.001
DEFAULT_HEARTBEATS = 3
DEFAULT_REPEATS = 3
DEFAULT_PARTITIONS = 8
DEFAULT_JOB_TIMEOUT_S = 900.0
HEARTBEAT_LEAD_S = 121  # như bộ phát lại với W = 120 s: cuối log + W + 1 s + k s
HEARTBEAT_NAMESPACE = uuid.UUID("5d0c6c1e-2f4b-4b8e-9a51-7f3e0c9d2a64")
RESULTS_REL = Path("results") / "mapreduce"

# Ước lượng thời gian máy cho --dry-run. Tốc độ bộ mô phỏng lấy từ experiments.matrix (số đo); các hằng còn lại
# là giả định đặt trước khi đo (chưa có số đo của job này), có hệ số bi quan riêng.
SIMULATOR_EVENTS_PER_S = 40_000.0
PREPARE_LINES_PER_S = 200_000.0
REFERENCE_LINES_PER_S = 60_000.0
SPARK_LINES_PER_CORE_S = 30_000.0
SESSION_STARTUP_S = 30.0
JOB_OVERHEAD_S = 5.0
PESSIMISTIC_FACTOR = 3.0

INVALID_KINDS = ("malformed_json", "unknown_field", "missing_field", "event_type", "learner_id", "event_time",
                 "learning_fields", "payload", "sent_before_event")


class MatrixError(RuntimeError):
    """Kế hoạch hoặc dữ liệu của thí nghiệm MapReduce không dùng được."""


# ---------------------------------------------------------------- kịch bản

@dataclass(frozen=True)
class Scenario:
    number: int          # số kịch bản như bảng ở mục 5 của thiết kế (1–6)
    dataset_index: int   # D1/D2/D3
    learners: int
    config: str

    @property
    def scenario_id(self) -> str:
        return f"s{self.number}-d{self.dataset_index}-l{self.learners}-{self.config}"

    def run_key(self, repeat: int) -> str:
        return f"{self.scenario_id}-r{repeat}"


def scenarios(scales: Sequence[int], configs: Sequence[str]) -> list[Scenario]:
    """Kịch bản theo thứ tự của thiết kế: D1/A=1, D1/B=2, D2/A=3, … (chỉ những quy mô, cấu hình được chọn)."""
    out = []
    for d_index, learners in enumerate(SCALES, start=1):
        for c_index, config in enumerate(CONFIG_NAMES):
            if learners in scales and config in configs:
                out.append(Scenario(2 * (d_index - 1) + c_index + 1, d_index, learners, config))
    return out


def dataset_params(learners: int, duration_s: int, rate_per_learner: float) -> dict[str, Any]:
    return {"learners": learners, "rate_events_per_s": round(learners * rate_per_learner, 6),
            "late_profile": LATE_PROFILE, "duplicate_rate": DUPLICATE_RATE, "duration_s": duration_s,
            **PROBLEM_DEFAULTS}


def expected_deliveries(params: Mapping[str, Any], invalid_rate: float, heartbeats: int) -> int:
    """Số dòng đầu vào dự kiến trước khi sinh: sự kiện × (1 + trùng) × (1 + hỏng) + heartbeat."""
    from ai_math_tutor.experiments.matrix import DATASET_MARGIN_S

    events = params["rate_events_per_s"] * (params["duration_s"] + DATASET_MARGIN_S)
    return round(events * (1 + params["duplicate_rate"]) * (1 + invalid_rate)) + heartbeats


# ---------------------------------------------------------------- chuẩn bị đầu vào

def corrupt(value: str, kind: str) -> str:
    """Bản sao bị làm hỏng của một lần giao hợp lệ; `kind` là lý do mà job phải trả cho bản ghi này."""
    if kind == "malformed_json":
        return value[: len(value) // 2]
    event = json.loads(value)
    if kind == "unknown_field":
        event["device_name"] = "tablet"
    elif kind == "missing_field":
        del event["learner_id"]
    elif kind == "event_type":
        event["event_type"] = str(event["event_type"]).upper()
    elif kind == "learner_id":
        event["learner_id"] = "L-" + "Z" * 16
    elif kind == "event_time":
        event["event_time"] = str(event["event_time"]).replace("T", " ")
    elif kind == "learning_fields":
        event["session_seq"] = 0
    elif kind == "payload":
        event["payload"] = {"step_index": 0}
    elif kind == "sent_before_event":
        from ai_math_tutor.simulator.output import epoch_ms, iso_from_epoch_ms, parse_utc

        event["sent_time"] = iso_from_epoch_ms(epoch_ms(parse_utc(event["event_time"])) - 5000)
    else:
        raise ValueError(f"loại hỏng không biết: {kind!r}")
    return json.dumps(event)


def heartbeat_line(seed: int, index: int, event_ms: int) -> str:
    from ai_math_tutor.simulator.output import iso_from_epoch_ms

    moment = iso_from_epoch_ms(event_ms)
    return json.dumps({"schema_version": "0.2", "event_id": str(uuid.uuid5(HEARTBEAT_NAMESPACE, f"{seed}/{index}")),
                       "event_type": HEARTBEAT_EVENT_TYPE, "learner_id": HEARTBEAT_LEARNER_ID,
                       "event_time": moment, "sent_time": moment, "payload": {}})


def write_deliveries(values: Iterable[tuple[str, int]], out_path: Path, *, t0_ms: int, invalid_rate: float,
                     heartbeats: int, seed: int) -> dict[str, Any]:
    """Ghi các lần giao (value, event_offset_ms) + bản ghi hỏng chèn thêm + heartbeat; trả số đếm đã chèn."""
    if not 0 <= invalid_rate < 1:
        raise MatrixError(f"invalid_rate phải trong [0, 1), nhận {invalid_rate}")
    rng = random.Random(seed * 1_000_003 + 17)
    injected = {kind: 0 for kind in INVALID_KINDS}
    lines = source_rows = 0
    max_offset = 0
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp = out_path.with_name(out_path.name + ".tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as handle:
        for value, offset in values:
            handle.write(value + "\n")
            source_rows += 1
            lines += 1
            max_offset = max(max_offset, int(offset))
            if invalid_rate and rng.random() < invalid_rate:
                kind = INVALID_KINDS[rng.randrange(len(INVALID_KINDS))]
                handle.write(corrupt(value, kind) + "\n")
                injected[kind] += 1
                lines += 1
        for index in range(heartbeats):
            handle.write(heartbeat_line(seed, index, t0_ms + max_offset + (HEARTBEAT_LEAD_S + index) * 1000) + "\n")
            lines += 1
    tmp.replace(out_path)
    return {"lines": lines, "source_rows": source_rows, "injected": {
        "invalid_by_reason": injected, "invalid_total": sum(injected.values()), "heartbeats": heartbeats}}


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with Path(path).open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def prepare_dataset(data_root: Path, learners: int, *, duration_s: int, rate_per_learner: float, seed: int,
                    invalid_rate: float, heartbeats: int, k: int, late_threshold_s: float) -> dict[str, Any]:
    """Bộ mô phỏng (cache) → deliveries.jsonl (cache) → bản tham chiếu (cache); trả mô tả bộ dữ liệu."""
    from ai_math_tutor.experiments.datasets import ensure_dataset, read_manifest
    from ai_math_tutor.simulator.output import iter_rows, parquet_metadata

    params = dataset_params(learners, duration_s, rate_per_learner)
    started = time.perf_counter()
    sim_dir, sim_cached = ensure_dataset(data_root / "exp-sim", params, seed)
    simulate_s = time.perf_counter() - started
    manifest = read_manifest(sim_dir)
    source_checksum = manifest["outputs"]["events.parquet"]["checksum"]
    key = dataset_key(params, seed)
    input_dir = data_root / "mapreduce" / "input" / f"{key}-inv{invalid_rate:g}-hb{heartbeats}"
    input_path = input_dir / "deliveries.jsonl"
    meta_path = input_dir / "prepared.json"
    meta: dict[str, Any] | None = None
    if meta_path.is_file() and input_path.is_file():
        meta = json.loads(meta_path.read_text(encoding="utf-8"))
        if meta.get("source_checksum") != source_checksum or meta.get("input_size") != input_path.stat().st_size:
            meta = None
    prepared_cached = meta is not None
    if meta is None:
        mark = time.perf_counter()
        t0_ms = int(parquet_metadata(sim_dir / "events.parquet")["nhom8.t0_epoch_ms"])
        rows = ((row["value"], row["event_offset_ms"])
                for row in iter_rows(sim_dir / "events.parquet", columns=["value", "event_offset_ms"]))
        written = write_deliveries(rows, input_path, t0_ms=t0_ms, invalid_rate=invalid_rate, heartbeats=heartbeats,
                                   seed=seed)
        meta = {"source": str(sim_dir / "events.parquet"), "source_checksum": source_checksum,
                "invalid_rate": invalid_rate, "seed": seed, **written, "input_size": input_path.stat().st_size,
                "input_checksum": file_sha256(input_path), "prepare_s": round(time.perf_counter() - mark, 3)}
        meta_path.write_text(json.dumps(meta, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    reference_path = input_dir / f"reference-k{k}-late{late_threshold_s:g}.json.gz"
    reference_meta = input_dir / f"reference-k{k}-late{late_threshold_s:g}.json"
    reference_cached = False
    if reference_path.is_file() and reference_meta.is_file():
        reference_cached = json.loads(reference_meta.read_text(encoding="utf-8")).get("input_checksum") == meta["input_checksum"]
    if not reference_cached:
        mark = time.perf_counter()
        reference = reference_counts_from_file(input_path, k, late_threshold_s)
        reference.save(reference_path)
        reference_meta.write_text(json.dumps({"input_checksum": meta["input_checksum"], "k": k,
                                              "late_threshold_s": late_threshold_s, "stats": reference.stats,
                                              "seconds": round(time.perf_counter() - mark, 3)}, indent=2) + "\n",
                                  encoding="utf-8")
    counts = manifest["counts"]
    return {
        "learners": learners, "dataset_key": key, "seed": seed, "params": params, "sim_dir": str(sim_dir),
        "input": str(input_path), "input_checksum": meta["input_checksum"], "input_size": meta["input_size"],
        "lines": meta["lines"], "invalid_rate": invalid_rate, "injected": meta["injected"],
        "reference": str(reference_path),
        "manifest_counts": {name: counts[name] for name in ("unique_events", "deliveries", "duplicate_deliveries",
                                                            "late_events", "sessions", "learners")},
        "by_event_type": counts.get("by_event_type"),
        "cache": {"simulation": sim_cached, "input": prepared_cached, "reference": reference_cached},
        "simulate_s": round(simulate_s, 3),
    }


# ---------------------------------------------------------------- kế hoạch và ước lượng

def estimate_minutes(sizes: Mapping[int, int], plan_scenarios: Sequence[Scenario], repeats: int,
                     cached: Mapping[int, bool], warmup: bool) -> dict[str, Any]:
    """Phút máy ước lượng (giả định ở đầu mô-đun); `sizes` = learners → số dòng đầu vào."""
    from ai_math_tutor.batch.mapreduce_run import CONFIGS

    prep = sum(0.0 if cached.get(n) else lines / SIMULATOR_EVENTS_PER_S + lines / PREPARE_LINES_PER_S
               + lines / REFERENCE_LINES_PER_S for n, lines in sizes.items())
    per_config: dict[str, float] = {}
    smallest = min(sizes.values()) if sizes else 0
    for scenario in plan_scenarios:
        cores = CONFIGS[scenario.config].workers * CONFIGS[scenario.config].cores
        job = JOB_OVERHEAD_S + sizes[scenario.learners] / (cores * SPARK_LINES_PER_CORE_S)
        per_config.setdefault(scenario.config, SESSION_STARTUP_S + (
            (JOB_OVERHEAD_S + smallest / (cores * SPARK_LINES_PER_CORE_S)) if warmup else 0.0))
        per_config[scenario.config] += repeats * job
    total = prep + sum(per_config.values())
    return {
        "datasets_s": round(prep, 1), "per_config_s": {k: round(v, 1) for k, v in per_config.items()},
        "total_minutes": round(total / 60, 1), "pessimistic_minutes": round(total * PESSIMISTIC_FACTOR / 60, 1),
        "assumptions": {"simulator_events_per_s": SIMULATOR_EVENTS_PER_S, "prepare_lines_per_s": PREPARE_LINES_PER_S,
                        "reference_lines_per_s": REFERENCE_LINES_PER_S, "spark_lines_per_core_s": SPARK_LINES_PER_CORE_S,
                        "session_startup_s": SESSION_STARTUP_S, "job_overhead_s": JOB_OVERHEAD_S,
                        "pessimistic_factor": PESSIMISTIC_FACTOR},
    }


def _cached_lines(data_root: Path, params: Mapping[str, Any], seed: int, invalid_rate: float, heartbeats: int) -> int | None:
    meta = data_root / "mapreduce" / "input" / f"{dataset_key(params, seed)}-inv{invalid_rate:g}-hb{heartbeats}" / "prepared.json"
    try:
        return int(json.loads(meta.read_text(encoding="utf-8"))["lines"])
    except (OSError, ValueError, KeyError):
        return None


def build_plan(args: argparse.Namespace, data_root: Path) -> dict[str, Any]:
    plan_scenarios = scenarios(args.scales, args.configs)
    if not plan_scenarios:
        raise MatrixError("không có kịch bản nào với --scales/--configs đã chọn")
    learners_needed = sorted({s.learners for s in plan_scenarios})
    datasets, sizes, cached = [], {}, {}
    for learners in learners_needed:
        params = dataset_params(learners, args.duration_s, args.rate_per_learner)
        lines = _cached_lines(data_root, params, args.seed, args.invalid_rate, args.heartbeats)
        cached[learners] = lines is not None
        sizes[learners] = lines if lines is not None else expected_deliveries(params, args.invalid_rate, args.heartbeats)
        datasets.append({"learners": learners, "dataset_key": dataset_key(params, args.seed), "params": params,
                         "input_lines": sizes[learners], "input_lines_source": "cache" if lines is not None else "estimate",
                         "events_per_learner": round(args.rate_per_learner * args.duration_s, 1)})
    return {
        "scenarios": [{"scenario": s.scenario_id, "number": s.number, "learners": s.learners, "config": s.config,
                       "runs": [s.run_key(r) for r in range(1, args.repeats + 1)]} for s in plan_scenarios],
        "datasets": datasets,
        "spark_mode": args.spark_mode, "partitions": args.partitions, "input_partitions": args.input_partitions,
        "repeats": args.repeats, "warmup": not args.no_warmup, "k": args.k,
        "estimate": estimate_minutes(sizes, plan_scenarios, args.repeats, cached, not args.no_warmup),
    }


def completed(metrics_dir: Path, run_key: str) -> bool:
    try:
        return json.loads((metrics_dir / f"{run_key}.json").read_text(encoding="utf-8")).get("status") == "success"
    except (OSError, ValueError):
        return False


# ---------------------------------------------------------------- chạy

def run_config(config: str, runs: list[dict[str, Any]], args: argparse.Namespace, data_root: Path, results: Path,
               runs_jsonl: Path) -> int:
    """Tiến trình con cho một cấu hình; log session vào raw_logs/ (nén). Trả mã thoát của tiến trình con."""
    from ai_math_tutor.batch.mapreduce_run import CONFIGS

    plan = {
        "config": config, "resources": CONFIGS[config].describe(), "spark_mode": args.spark_mode,
        "driver_memory": args.driver_memory, "input_partitions": args.input_partitions, "partitions": args.partitions,
        "k": args.k, "late_threshold_s": args.late_threshold_s, "job_timeout_s": args.job_timeout_s,
        "results_dir": str(results), "runs_jsonl": str(runs_jsonl), "data_root": str(data_root),
        "work_dir": str(data_root / "mapreduce" / "out"), "runs": runs,
    }
    plan_path = results / "configs" / f"session-plan-{config}.json"
    plan_path.parent.mkdir(parents=True, exist_ok=True)
    plan_path.write_text(json.dumps(plan, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    log_path = results / "raw_logs" / f"session-{config}-{stamp}.log"
    log_path.parent.mkdir(parents=True, exist_ok=True)
    timeout = SESSION_STARTUP_S * 4 + len(runs) * (args.job_timeout_s + 120)
    command = [sys.executable, "-m", "ai_math_tutor.batch.mapreduce_run", "--plan", str(plan_path)]
    print(f"[mapreduce] cấu hình {config}: {len(runs)} run, log {log_path}", flush=True)
    timed_out = threading.Event()
    with log_path.open("w", encoding="utf-8") as log_handle:
        proc = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=log_handle, text=True, start_new_session=True)

        def expire() -> None:
            timed_out.set()
            print(f"[mapreduce] {config}: quá {timeout:.0f} s, dừng tiến trình con", flush=True)
            _stop_group(proc)

        watchdog = threading.Timer(timeout, expire)
        watchdog.daemon = True
        watchdog.start()
        try:
            assert proc.stdout is not None
            for line in proc.stdout:  # mỗi run in một dòng JSON tiến độ
                log_handle.write(line)
                log_handle.flush()
                print(f"[mapreduce] {config}: {line.strip()}", flush=True)
            code = proc.wait()
        except BaseException:
            _stop_group(proc)  # Ctrl-C hay lỗi ở đây: không để lại JVM/executor mồ côi
            raise
        finally:
            watchdog.cancel()
        if timed_out.is_set():
            code = 124
    with log_path.open("rb") as src, gzip.open(log_path.with_name(log_path.name + ".gz"), "wb") as dst:
        shutil.copyfileobj(src, dst)
    log_path.unlink()
    return code


def _stop_group(proc: subprocess.Popen[str]) -> None:
    """SIGTERM cho cả nhóm tiến trình (Python con, JVM, executor), SIGKILL nếu sau 30 s chưa dừng."""
    for sig, wait_s in ((signal.SIGTERM, 30), (signal.SIGKILL, 10)):
        try:
            os.killpg(proc.pid, sig)
        except (ProcessLookupError, PermissionError):
            return
        try:
            proc.wait(timeout=wait_s)
            return
        except subprocess.TimeoutExpired:
            continue


def write_scenario_configs(results: Path, plan_scenarios: Sequence[Scenario], datasets: Mapping[int, Mapping[str, Any]],
                           args: argparse.Namespace) -> None:
    from ai_math_tutor.batch.mapreduce_run import CONFIGS

    for scenario in plan_scenarios:
        path = results / "configs" / f"{scenario.scenario_id}.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps({
            "scenario": scenario.scenario_id, "design_number": scenario.number, "dataset": f"D{scenario.dataset_index}",
            "config": CONFIGS[scenario.config].describe(), "spark_mode": args.spark_mode,
            "spark_conf_file": f"spark-{scenario.config}.json", "repeats": args.repeats,
            "input_partitions": args.input_partitions, "partitions": args.partitions, "k": args.k,
            "late_threshold_s": args.late_threshold_s, "dataset_info": datasets[scenario.learners],
        }, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


# ---------------------------------------------------------------- tổng hợp

def _dig(data: Any, *path: str) -> Any:
    for name in path:
        if not isinstance(data, Mapping):
            return None
        data = data.get(name)
    return data


SUMMARY_METRICS: dict[str, tuple[str, ...]] = {
    "job_wall_s": ("job_wall_s",),
    "throughput_deliveries_per_s": ("throughput_deliveries_per_s",),
    "driver_cpu_percent_mean": ("resources", "driver_cpu_percent_mean"),
    "executors_cpu_percent_mean": ("resources", "executors_cpu_percent_mean"),
    "driver_rss_bytes_peak": ("resources", "driver_rss_bytes_peak"),
    "executors_rss_bytes_peak_sum": ("resources", "executors_rss_bytes_peak_sum"),
    "shuffle_read_bytes": ("spark", "totals", "shuffle_read_bytes"),
    "shuffle_write_bytes": ("spark", "totals", "shuffle_write_bytes"),
    "shuffle_remote_bytes_read": ("spark", "totals", "shuffle_remote_bytes_read"),
    "shuffle_fetch_wait_ms": ("spark", "totals", "shuffle_fetch_wait_ms"),
    "memory_spilled_bytes": ("spark", "totals", "memory_spilled_bytes"),
    "disk_spilled_bytes": ("spark", "totals", "disk_spilled_bytes"),
    "executor_run_time_ms": ("spark", "totals", "executor_run_time_ms"),
    "tasks": ("spark", "totals", "num_tasks"),
    "failed_tasks": ("spark", "totals", "failed_tasks"),
    "task_attempts_retried": ("spark", "totals", "task_attempts_retried"),
    "reduce_records_max_over_median": ("spark", "skew_by_learner_reduce", "records", "max_over_median"),
    "reduce_run_time_max_over_median": ("spark", "skew_by_learner_reduce", "run_time_ms", "max_over_median"),
    "disk_read_bytes": ("io_delta_machine", "disk_read_bytes"),
    "disk_write_bytes": ("io_delta_machine", "disk_write_bytes"),
    "loopback_bytes_sent": ("io_delta_machine", "loopback_bytes_sent"),
}
RUN_COLUMNS = ("row_type", "scenario", "config", "learners", "repeat", "run_id", "status", "correct", "n_runs",
               "deliveries", "unique_events", "duplicate_deliveries", "invalid", "late_events", "output_keys",
               *SUMMARY_METRICS)


def load_metrics(metrics_dir: Path) -> list[dict[str, Any]]:
    rows = []
    for path in sorted(metrics_dir.glob("*.json")):
        try:
            rows.append(json.loads(path.read_text(encoding="utf-8")))
        except (OSError, ValueError):
            continue
    return rows


def run_row(metrics: Mapping[str, Any]) -> dict[str, Any]:
    counts = metrics.get("counts") or {}
    return {
        "row_type": "run", "scenario": metrics.get("scenario"), "config": metrics.get("config"),
        "learners": metrics.get("scale_learners"), "repeat": metrics.get("repeat"), "run_id": metrics.get("run_id"),
        "status": metrics.get("status"), "correct": int(bool(_dig(metrics, "correctness", "match"))), "n_runs": 1,
        **{name: counts.get(name) for name in ("deliveries", "unique_events", "duplicate_deliveries", "invalid",
                                                "late_events", "output_keys")},
        **{name: _dig(metrics, *path) for name, path in SUMMARY_METRICS.items()},
    }


def aggregate_rows(runs: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Mỗi kịch bản: trung bình, trung vị, min, max của từng metric trên các run success."""
    out: list[dict[str, Any]] = []
    scenario_ids = sorted({r["scenario"] for r in runs}, key=lambda s: int(str(s).split("-")[0][1:]))
    for scenario in scenario_ids:
        group = [r for r in runs if r["scenario"] == scenario and r["status"] == "success"]
        if not group:
            continue
        base = {"scenario": scenario, "config": group[0]["config"], "learners": group[0]["learners"],
                "n_runs": len(group), "correct": int(all(r["correct"] for r in group))}
        for stat, fn in (("mean", statistics.fmean), ("median", statistics.median), ("min", min), ("max", max)):
            row: dict[str, Any] = {"row_type": stat, **base}
            for name in ("deliveries", "unique_events", "duplicate_deliveries", "invalid", "late_events",
                         "output_keys", *SUMMARY_METRICS):
                values = [r[name] for r in group if isinstance(r.get(name), (int, float))]
                row[name] = round(fn(values), 3) if values else None
            out.append(row)
    return out


def write_figures(rows: Sequence[Mapping[str, Any]], figures: Path) -> list[str]:
    import matplotlib

    matplotlib.use("Agg")
    import matplotlib.pyplot as plt

    figures.mkdir(parents=True, exist_ok=True)
    medians = [r for r in rows if r["row_type"] == "median"]
    lows = {r["scenario"]: r for r in rows if r["row_type"] == "min"}
    highs = {r["scenario"]: r for r in rows if r["row_type"] == "max"}
    written: list[str] = []
    if not medians:
        return written

    def by_config(metric: str) -> dict[str, list[tuple[int, float, float, float]]]:
        series: dict[str, list[tuple[int, float, float, float]]] = {}
        for row in medians:
            if row.get(metric) is None:
                continue
            low = lows[row["scenario"]].get(metric) or row[metric]
            high = highs[row["scenario"]].get(metric) or row[metric]
            series.setdefault(row["config"], []).append((int(row["learners"]), row[metric], low, high))
        return {k: sorted(v) for k, v in series.items()}

    for metric, ylabel, name in (("job_wall_s", "Thời gian hoàn thành job (s)", "completion-time"),
                                 ("throughput_deliveries_per_s", "Throughput (lần giao/s)", "throughput")):
        fig, ax = plt.subplots(figsize=(5.5, 3.6))
        for config, points in sorted(by_config(metric).items()):
            xs = [p[0] for p in points]
            ys = [p[1] for p in points]
            err = [[p[1] - p[2] for p in points], [p[3] - p[1] for p in points]]
            ax.errorbar(xs, ys, yerr=err, marker="o", capsize=3, label=f"Config {config}")
        ax.set_xscale("log")
        ax.set_xlabel("Số người học (thang log)")
        ax.set_ylabel(ylabel)
        ax.set_title("Trung vị qua các lần lặp, vạch = min–max")
        ax.legend()
        ax.grid(alpha=0.3)
        fig.tight_layout()
        fig.savefig(figures / f"{name}.png", dpi=150)
        plt.close(fig)
        written.append(f"{name}.png")

    labels = [r["scenario"] for r in medians]
    for metrics, ylabel, name, scale in (
        (("shuffle_write_bytes", "shuffle_read_bytes", "shuffle_remote_bytes_read"), "MB (trung vị)", "shuffle", 1e6),
        (("reduce_records_max_over_median", "reduce_run_time_max_over_median"), "max / trung vị", "skew", 1.0),
    ):
        fig, ax = plt.subplots(figsize=(7, 3.6))
        width = 0.8 / len(metrics)
        for offset, metric in enumerate(metrics):
            values = [(r.get(metric) or 0) / scale for r in medians]
            ax.bar([i + offset * width for i in range(len(labels))], values, width=width, label=metric)
        ax.set_xticks([i + width * (len(metrics) - 1) / 2 for i in range(len(labels))], labels, rotation=20, fontsize=8)
        ax.set_ylabel(ylabel)
        ax.legend(fontsize=7)
        ax.grid(alpha=0.3, axis="y")
        fig.tight_layout()
        fig.savefig(figures / f"{name}.png", dpi=150)
        plt.close(fig)
        written.append(f"{name}.png")
    return written


def write_error_analysis(metrics_rows: Sequence[Mapping[str, Any]], out: Path) -> dict[str, Any]:
    from ai_math_tutor.batch.evaluate import write_csv

    out.mkdir(parents=True, exist_ok=True)
    measured = [m for m in metrics_rows if not m.get("warmup")]
    quality, skew, retries, mismatches, reasons = [], [], [], [], []
    seen_datasets: set[Any] = set()
    with (out / "invalid_samples.jsonl").open("w", encoding="utf-8", newline="\n") as samples:
        for m in measured:
            counts = m.get("counts") or {}
            checks = _dig(m, "data_checks", "checks") or {}
            quality.append({
                "run_key": m.get("run_key"), "scenario": m.get("scenario"), "status": m.get("status"),
                "deliveries": counts.get("deliveries"), "unique_events": counts.get("unique_events"),
                "unique_events_manifest": _dig(checks, "unique_events", "expected"),
                "duplicate_deliveries": counts.get("duplicate_deliveries"),
                "duplicate_deliveries_manifest": _dig(checks, "duplicate_deliveries", "expected"),
                "conflicting_event_ids": counts.get("conflicting_event_ids"),
                "late_events": counts.get("late_events"), "late_events_manifest": _dig(checks, "late_events", "expected"),
                "heartbeats": counts.get("heartbeats"), "invalid": counts.get("invalid"),
                "invalid_injected": _dig(checks, "invalid", "expected"),
                "invalid_with_event_id": counts.get("invalid_with_event_id"), "output_keys": counts.get("output_keys"),
                "data_checks_ok": _dig(m, "data_checks", "ok"), "correct": _dig(m, "correctness", "match"),
            })
            for stage in _dig(m, "spark", "stages") or []:
                if stage.get("skew"):
                    s = stage["skew"]
                    skew.append({"run_key": m.get("run_key"), "stage_id": stage["stage_id"], "label": stage["label"],
                                 "tasks": s["tasks"], "records_max": s["records"]["max"],
                                 "records_median": s["records"]["median"],
                                 "records_max_over_median": s["records"]["max_over_median"],
                                 "run_time_ms_max": s["run_time_ms"]["max"],
                                 "run_time_ms_median": s["run_time_ms"]["median"],
                                 "run_time_max_over_median": s["run_time_ms"]["max_over_median"],
                                 "slowest_executor": (s.get("slowest_task") or {}).get("executor"),
                                 "tasks_by_executor": json.dumps(s.get("tasks_by_executor"))})
            totals = _dig(m, "spark", "totals") or {}
            retries.append({"run_key": m.get("run_key"), "status": m.get("status"),
                            **{name: totals.get(name) for name in ("failed_tasks", "killed_tasks", "task_attempts_retried",
                                                                   "stage_attempts_retried", "failed_stages", "failed_jobs")},
                            "error": m.get("error")})
            if m.get("status") != "success" or not _dig(m, "correctness", "match"):
                mismatches.append({"run_key": m.get("run_key"), "status": m.get("status"), "error": m.get("error"),
                                   "correctness": m.get("correctness"), "data_checks": m.get("data_checks")})
            dataset = m.get("scale_learners")
            if dataset not in seen_datasets and m.get("status") == "success":
                seen_datasets.add(dataset)
                injected = _dig(checks, "invalid", "expected")
                for reason, count in sorted((m.get("invalid_by_reason") or {}).items()):
                    reasons.append({"learners": dataset, "reason": reason, "count": count})
                reasons.append({"learners": dataset, "reason": "TOTAL", "count": counts.get("invalid"),
                                "injected_total": injected})
                for sample in m.get("invalid_samples") or []:
                    samples.write(json.dumps({"learners": dataset, **sample}, ensure_ascii=False) + "\n")
    write_csv(out / "data_quality.csv", quality, list(quality[0]) if quality else ["run_key"])
    write_csv(out / "skew.csv", skew, list(skew[0]) if skew else ["run_key"])
    write_csv(out / "retries.csv", retries, list(retries[0]) if retries else ["run_key"])
    write_csv(out / "invalid_reasons.csv", reasons, ["learners", "reason", "count", "injected_total"])
    (out / "mismatches.json").write_text(json.dumps(mismatches, ensure_ascii=False, indent=2, default=str) + "\n",
                                         encoding="utf-8")
    worst = max(skew, key=lambda r: r["run_time_max_over_median"] or 0, default=None)
    overview = {
        "runs": len(measured), "success": sum(1 for m in measured if m.get("status") == "success"),
        "mismatches_or_failures": len(mismatches),
        "failed_tasks_total": sum(r.get("failed_tasks") or 0 for r in retries),
        "task_attempts_retried_total": sum(r.get("task_attempts_retried") or 0 for r in retries),
        "worst_shuffle_stage_run_time_skew": worst,
    }
    (out / "overview.json").write_text(json.dumps(overview, ensure_ascii=False, indent=2, default=str) + "\n",
                                       encoding="utf-8")
    return overview


def summarize(results: Path) -> dict[str, Any]:
    from ai_math_tutor.batch.evaluate import write_csv

    metrics_rows = load_metrics(results / "metrics")
    runs = [run_row(m) for m in metrics_rows if not m.get("warmup")]
    runs.sort(key=lambda r: (int(str(r["scenario"]).split("-")[0][1:]), r.get("repeat") or 0))
    rows = [*runs, *aggregate_rows(runs)]
    write_csv(results / "metrics" / "summary.csv", rows, RUN_COLUMNS)
    figures = write_figures(rows, results / "figures") if runs else []
    overview = write_error_analysis(metrics_rows, results / "error_analysis")
    return {"runs": len(runs), "figures": figures, "error_analysis": overview}


# ---------------------------------------------------------------- CLI

def _int_list(text: str) -> list[int]:
    try:
        values = [int(part) for part in text.split(",") if part.strip()]
    except ValueError as exc:
        raise argparse.ArgumentTypeError(f"danh sách số nguyên không hợp lệ: {text!r}") from exc
    bad = [v for v in values if v not in SCALES]
    if bad or not values:
        raise argparse.ArgumentTypeError(f"quy mô phải thuộc {SCALES}, nhận {text!r}")
    return values


def _config_list(text: str) -> list[str]:
    values = [part.strip().upper() for part in text.split(",") if part.strip()]
    if not values or any(v not in CONFIG_NAMES for v in values):
        raise argparse.ArgumentTypeError(f"cấu hình phải thuộc {CONFIG_NAMES}, nhận {text!r}")
    return values


def build_parser() -> argparse.ArgumentParser:
    from ai_math_tutor.batch.mapreduce_run import DEFAULT_DRIVER_MEMORY, SPARK_MODES

    parser = argparse.ArgumentParser(prog="ai_math_tutor.experiments.mapreduce_matrix",
                                     description=__doc__.splitlines()[0])
    parser.add_argument("--scales", type=_int_list, default=list(SCALES), help="số người học, vd. 100,1000,10000")
    parser.add_argument("--configs", type=_config_list, default=list(CONFIG_NAMES), help="A,B")
    parser.add_argument("--repeats", type=int, default=DEFAULT_REPEATS)
    parser.add_argument("--duration-s", type=int, default=DEFAULT_DURATION_S, help="thời lượng mô phỏng (giây)")
    parser.add_argument("--rate-per-learner", type=float, default=PER_LEARNER_RATE, help="sự kiện/giây mỗi người học")
    parser.add_argument("--seed", type=int, default=DATASET_SEED)
    parser.add_argument("--invalid-rate", type=float, default=DEFAULT_INVALID_RATE)
    parser.add_argument("--heartbeats", type=int, default=DEFAULT_HEARTBEATS)
    parser.add_argument("--partitions", type=int, default=DEFAULT_PARTITIONS, help="số partition của mỗi shuffle")
    parser.add_argument("--input-partitions", type=int, default=DEFAULT_PARTITIONS, help="minPartitions của textFile")
    parser.add_argument("--k", type=int, default=DEFAULT_TOP_K)
    parser.add_argument("--late-threshold-s", type=float, default=DEFAULT_LATE_THRESHOLD_S)
    parser.add_argument("--spark-mode", choices=SPARK_MODES, default="local-cluster")
    parser.add_argument("--driver-memory", default=DEFAULT_DRIVER_MEMORY, help="driver khi dùng local-cluster")
    parser.add_argument("--job-timeout-s", type=float, default=DEFAULT_JOB_TIMEOUT_S)
    parser.add_argument("--results-dir", type=Path, default=REPO_ROOT / RESULTS_REL)
    parser.add_argument("--runs-jsonl", type=Path, default=None, help="mặc định <results của runtime.yaml>/runs.jsonl")
    parser.add_argument("--data-root", type=Path, default=None, help="mặc định NHOM8_DATA_ROOT")
    parser.add_argument("--no-warmup", action="store_true")
    parser.add_argument("--force", action="store_true", help="chạy lại cả run đã success")
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--dry-run", action="store_true", help="in kế hoạch, cỡ dữ liệu, ước lượng; không sinh, không Spark")
    mode.add_argument("--prepare-only", action="store_true", help="chỉ sinh/kiểm dữ liệu và bản tham chiếu")
    mode.add_argument("--summarize-only", action="store_true", help="chỉ tổng hợp metrics/ thành CSV, hình, phân tích lỗi")
    return parser


def main(argv: list[str] | None = None) -> int:
    from ai_math_tutor.config import load_config

    args = build_parser().parse_args(argv)
    if args.repeats < 1 or args.partitions < 1 or args.input_partitions < 1 or args.k < 1 or args.duration_s < 1:
        raise SystemExit("--repeats, --partitions, --input-partitions, --k, --duration-s phải ≥ 1")
    if not args.rate_per_learner > 0 or not math.isfinite(args.rate_per_learner):
        raise SystemExit("--rate-per-learner phải > 0")
    cfg = load_config()
    data_root = (args.data_root or cfg.paths.data_root).expanduser().resolve()
    results = args.results_dir.expanduser().resolve()
    runs_jsonl = (args.runs_jsonl or cfg.paths.results / "runs.jsonl").expanduser().resolve()

    if args.summarize_only:
        print(json.dumps(summarize(results), ensure_ascii=False, indent=2, default=str))
        return 0
    plan = build_plan(args, data_root)
    if args.dry_run:
        print(json.dumps({"data_root": str(data_root), "results": str(results), **plan}, ensure_ascii=False, indent=2))
        return 0

    plan_scenarios = scenarios(args.scales, args.configs)
    datasets: dict[int, dict[str, Any]] = {}
    for learners in sorted({s.learners for s in plan_scenarios}):
        started = time.perf_counter()
        datasets[learners] = prepare_dataset(
            data_root, learners, duration_s=args.duration_s, rate_per_learner=args.rate_per_learner, seed=args.seed,
            invalid_rate=args.invalid_rate, heartbeats=args.heartbeats, k=args.k, late_threshold_s=args.late_threshold_s)
        info = datasets[learners]
        print(f"[mapreduce] dữ liệu l{learners}: {info['lines']} dòng, cache {info['cache']}, "
              f"{time.perf_counter() - started:.1f} s", flush=True)
    write_scenario_configs(results, plan_scenarios, datasets, args)
    if args.prepare_only:
        return 0

    metrics_dir = results / "metrics"
    failures = 0
    for config in args.configs:
        pending = [(s, r) for s in plan_scenarios if s.config == config for r in range(1, args.repeats + 1)
                   if args.force or not completed(metrics_dir, s.run_key(r))]
        if not pending:
            print(f"[mapreduce] cấu hình {config}: mọi run đã success, bỏ qua", flush=True)
            continue
        runs = []
        if not args.no_warmup:
            smallest = min(datasets)
            runs.append({"run_key": f"warmup-{config}", "warmup": True, "scenario": f"warmup-{config}", "repeat": 0,
                         "dataset": datasets[smallest]})
        runs += [{"run_key": s.run_key(r), "warmup": False, "scenario": s.scenario_id, "repeat": r,
                  "dataset": datasets[s.learners]} for s, r in pending]
        code = run_config(config, runs, args, data_root, results, runs_jsonl)
        failures += code != 0
    summary = summarize(results)
    print(json.dumps(summary, ensure_ascii=False, indent=2, default=str))
    return 1 if failures else 0


if __name__ == "__main__":
    raise SystemExit(main())
