"""Một cấu hình tài nguyên của thí nghiệm MapReduce: một SparkSession, một job khởi động, rồi các run đo.

Chạy như tiến trình con của `ai_math_tutor.experiments.mapreduce_matrix` với một tệp kế hoạch JSON:

    python -m ai_math_tutor.batch.mapreduce_run --plan <plan.json>

**Cấu hình.** Config A = 1 worker × (2 lõi, 4 GB), Config B = 2 worker × (2 lõi, 4 GB), trên cùng một máy.
Mặc định dùng master `local-cluster[W,C,M]` của Spark: Master và W Worker chạy trong JVM driver, mỗi Worker
khởi động một executor là một JVM riêng (`CoarseGrainedExecutorBackend`, heap = M MB, C lõi), executor có các
tiến trình Python worker riêng. Shuffle giữa hai executor đi qua block manager (netty) trên loopback, như giữa hai
máy nhưng không qua mạng vật lý. `--spark-mode local` là phương án dự phòng: `local[W·C]`, driver W·M MB (mọi
task trong một JVM).

**Dùng lại session.** Mỗi cấu hình mở một SparkSession và chạy mọi run của nó trong session đó: chi phí khởi
động JVM và đăng ký executor không lặp lại ở mỗi run. Run đầu tiên là job khởi động (warm-up) trên bộ dữ liệu nhỏ
nhất, được ghi riêng và không vào bảng tổng hợp. Mỗi run đọc lại đầu vào từ đĩa, cache của run được giải phóng khi
xong, và mỗi run có job group riêng nên số liệu REST tách theo run.

**Số đo mỗi run.** Thời gian job (đồng hồ tường của các action), throughput (lần giao/s), CPU và RSS của driver và
từng executor (`experiments.monitor`, mẫu 0,5 s), I/O đĩa và loopback của cả máy trong lúc job chạy, và từ REST API
của Spark UI (chỉ nghe 127.0.0.1, cổng trống ngẫu nhiên): theo stage — shuffle read/write, spill bộ nhớ/đĩa,
executor run time, số task, task lỗi/thử lại; theo task của stage đọc shuffle — số bản ghi và run time
(max, trung vị, max/trung vị). Kết quả được so với bản tham chiếu thuần Python và với số đếm của bộ mô phỏng.
"""

from __future__ import annotations

import argparse
import gzip
import json
import logging
import os
import shutil
import socket
import statistics
import sys
import threading
import time
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import psutil

from ai_math_tutor.batch.mapreduce_problems import ProblemCounts, compare_counts, read_job_outputs, run_problem_job
from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.experiments.monitor import ProcessTree, ResourceMonitor
from ai_math_tutor.run_record import append_run_record, build_run_record

log = logging.getLogger("ai_math_tutor.batch.mapreduce")

SPARK_MODES = ("local-cluster", "local")
EXPERIMENT = "batch-mapreduce"
EXECUTOR_MAIN = "org.apache.spark.executor.CoarseGrainedExecutorBackend"
DEFAULT_DRIVER_MEMORY = "2g"
REGISTER_TIMEOUT_S = 120.0
SETTLE_TIMEOUT_S = 30.0


@dataclass(frozen=True)
class ResourceConfig:
    name: str
    workers: int
    cores: int
    memory_mb: int

    def describe(self) -> dict[str, Any]:
        return {"name": self.name, "workers": self.workers, "cores_per_worker": self.cores,
                "memory_mb_per_worker": self.memory_mb, "total_cores": self.workers * self.cores}


CONFIGS = {
    "A": ResourceConfig("A", workers=1, cores=2, memory_mb=4096),
    "B": ResourceConfig("B", workers=2, cores=2, memory_mb=4096),
}


def free_port(host: str = "127.0.0.1") -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.bind((host, 0))
        return int(sock.getsockname()[1])


def mapreduce_spark_conf(
    config: ResourceConfig,
    mode: str,
    *,
    partitions: int,
    ui_port: int,
    driver_memory: str = DEFAULT_DRIVER_MEMORY,
    data_root: Path | None = None,
) -> dict[str, str]:
    """Conf đầy đủ của một cấu hình (để đặt vào builder và ghi vào configs/ cùng run record)."""
    from ai_math_tutor.batch.session import batch_spark_conf

    if mode not in SPARK_MODES:
        raise ValueError(f"spark mode phải thuộc {SPARK_MODES}, nhận {mode!r}")
    if mode == "local-cluster":
        master = f"local-cluster[{config.workers},{config.cores},{config.memory_mb}]"
    else:
        master = f"local[{config.workers * config.cores}]"
        driver_memory = f"{config.workers * config.memory_mb}m"
    conf = batch_spark_conf(master=master, shuffle_partitions=partitions, data_root=data_root)
    tmp_option = conf["spark.driver.extraJavaOptions"]
    conf.update({
        "spark.app.name": f"nhom8-mapreduce-problems-{config.name}",
        "spark.driver.memory": driver_memory,
        "spark.default.parallelism": str(partitions),
        # UI chỉ để đọc REST API; SPARK_LOCAL_IP=127.0.0.1 giữ mọi cổng (UI, Master/Worker, block manager) trên loopback.
        "spark.ui.enabled": "true",
        "spark.ui.port": str(ui_port),
        "spark.ui.showConsoleProgress": "false",
        "spark.executor.processTreeMetrics.enabled": "true",
    })
    if mode == "local-cluster":
        conf.update({
            "spark.executor.memory": f"{config.memory_mb}m",
            "spark.executor.cores": str(config.cores),
            "spark.cores.max": str(config.workers * config.cores),
            "spark.executor.extraJavaOptions": tmp_option,
        })
    return conf


# ---------------------------------------------------------------- REST API của Spark UI

class SparkRest:
    """Client tối giản cho `/api/v1` (không đi qua proxy của môi trường)."""

    def __init__(self, base_url: str, timeout_s: float = 10.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_s = timeout_s
        self._opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))

    def get(self, path: str, **params: Any) -> Any:
        query = f"?{urllib.parse.urlencode(params)}" if params else ""
        with self._opener.open(f"{self.base_url}/api/v1{path}{query}", timeout=self.timeout_s) as response:
            return json.loads(response.read().decode("utf-8"))

    def app_id(self) -> str:
        apps = self.get("/applications")
        if not apps:
            raise RuntimeError("REST API chưa có application nào")
        return str(apps[0]["id"])


def wait_for_executors(rest: SparkRest, app_id: str, expected: int, timeout_s: float = REGISTER_TIMEOUT_S) -> list[dict[str, Any]]:
    """Chờ đủ `expected` executor (không tính driver) đăng ký; trả danh sách executor."""
    deadline = time.monotonic() + timeout_s
    executors: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        executors = [e for e in rest.get(f"/applications/{app_id}/executors") if e.get("id") != "driver"]
        if len(executors) >= expected:
            return executors
        time.sleep(0.5)
    raise RuntimeError(f"chỉ {len(executors)}/{expected} executor đăng ký sau {timeout_s:.0f} s; "
                       "chạy lại với --spark-mode local nếu local-cluster không dùng được")


def collect_rest(rest: SparkRest, app_id: str, job_group: str, settle_timeout_s: float = SETTLE_TIMEOUT_S) -> dict[str, Any]:
    """Job, stage (mọi attempt) và task của một job group, sau khi listener đã cập nhật xong."""
    deadline = time.monotonic() + settle_timeout_s
    previous: list[tuple[Any, ...]] | None = None
    jobs: list[dict[str, Any]] = []
    while time.monotonic() < deadline:
        jobs = [j for j in rest.get(f"/applications/{app_id}/jobs") if j.get("jobGroup") == job_group]
        state = sorted((j["jobId"], j.get("status"), j.get("numCompletedTasks"), j.get("numFailedTasks")) for j in jobs)
        if jobs and all(j.get("status") != "RUNNING" for j in jobs) and state == previous:
            break
        previous = state
        time.sleep(0.5)
    stage_ids = sorted({sid for job in jobs for sid in job.get("stageIds", [])})
    stages: list[dict[str, Any]] = []
    tasks: dict[str, list[dict[str, Any]]] = {}
    for sid in stage_ids:
        try:
            attempts = rest.get(f"/applications/{app_id}/stages/{sid}", details="false")  # task lấy riêng ở dưới
        except OSError:  # stage không còn trong bộ nhớ của UI (HTTP 404) hoặc lỗi đọc
            continue
        for attempt in attempts:
            stages.append(attempt)
            if attempt.get("status") in ("COMPLETE", "FAILED"):
                key = f"{sid}.{attempt.get('attemptId', 0)}"
                tasks[key] = rest.get(f"/applications/{app_id}/stages/{sid}/{attempt.get('attemptId', 0)}/taskList",
                                      offset=0, length=1_000_000)
    return {"jobs": jobs, "stages": stages, "tasks": tasks}


def executor_snapshot(rest: SparkRest, app_id: str) -> list[dict[str, Any]]:
    return list(rest.get(f"/applications/{app_id}/allexecutors"))


# ---------------------------------------------------------------- tóm tắt số đo (thuần Python)

def _ratio(top: float, mid: float) -> float | None:
    return round(top / mid, 3) if mid else None


def _spread(values: Sequence[float]) -> dict[str, Any]:
    if not values:
        return {"max": None, "median": None, "max_over_median": None}
    top, mid = max(values), statistics.median(values)
    return {"max": top, "median": mid, "max_over_median": _ratio(top, mid)}


def _metric(task: Mapping[str, Any], *path: str) -> float:
    node: Any = task.get("taskMetrics") or {}
    for name in path:
        node = node.get(name) if isinstance(node, Mapping) else None
    return float(node or 0)


def task_skew(tasks: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Độ lệch giữa các task thành công của một stage: bản ghi đọc (input + shuffle) và executor run time."""
    done = [t for t in tasks if t.get("status") == "SUCCESS"]
    records = [_metric(t, "inputMetrics", "recordsRead") + _metric(t, "shuffleReadMetrics", "recordsRead") for t in done]
    run_ms = [_metric(t, "executorRunTime") for t in done]
    by_executor: dict[str, int] = {}
    for task in done:
        by_executor[str(task.get("executorId"))] = by_executor.get(str(task.get("executorId")), 0) + 1
    slowest = max(done, key=lambda t: _metric(t, "executorRunTime"), default=None)
    return {
        "tasks": len(done),
        "records": _spread(records),
        "run_time_ms": _spread(run_ms),
        "tasks_by_executor": dict(sorted(by_executor.items())),
        "slowest_task": None if slowest is None else {
            "index": slowest.get("index"), "executor": slowest.get("executorId"),
            "run_time_ms": _metric(slowest, "executorRunTime"),
            "records": _metric(slowest, "inputMetrics", "recordsRead") + _metric(slowest, "shuffleReadMetrics", "recordsRead"),
        },
    }


def _parse_rest_time(text: Any) -> float | None:
    """'2026-10-28T10:00:00.123GMT' → epoch giây."""
    if not isinstance(text, str):
        return None
    try:
        return datetime.strptime(text.replace("GMT", "+0000"), "%Y-%m-%dT%H:%M:%S.%f%z").timestamp()
    except ValueError:
        return None


STAGE_SUM_FIELDS = {
    "num_tasks": "numTasks", "failed_tasks": "numFailedTasks", "killed_tasks": "numKilledTasks",
    "executor_run_time_ms": "executorRunTime", "executor_cpu_time_ns": "executorCpuTime", "jvm_gc_time_ms": "jvmGcTime",
    "input_bytes": "inputBytes", "input_records": "inputRecords", "output_bytes": "outputBytes",
    "output_records": "outputRecords", "shuffle_read_bytes": "shuffleReadBytes",
    "shuffle_read_records": "shuffleReadRecords", "shuffle_remote_bytes_read": "shuffleRemoteBytesRead",
    "shuffle_local_bytes_read": "shuffleLocalBytesRead", "shuffle_fetch_wait_ms": "shuffleFetchWaitTime",
    "shuffle_write_bytes": "shuffleWriteBytes", "shuffle_write_records": "shuffleWriteRecords",
    "shuffle_write_time_ns": "shuffleWriteTime", "memory_spilled_bytes": "memoryBytesSpilled",
    "disk_spilled_bytes": "diskBytesSpilled",
}


def summarize_rest(raw: Mapping[str, Any]) -> dict[str, Any]:
    """Tóm tắt `collect_rest`: từng stage đã chạy, tổng của run, độ lệch của các stage đọc shuffle."""
    jobs = sorted(raw.get("jobs") or [], key=lambda j: j["jobId"])
    label_of: dict[int, str] = {}
    for job in jobs:
        for sid in job.get("stageIds", []):
            label_of.setdefault(int(sid), str(job.get("description") or job.get("name") or ""))
    stage_rows: list[dict[str, Any]] = []
    skipped = 0
    for stage in sorted(raw.get("stages") or [], key=lambda s: (s["stageId"], s.get("attemptId", 0))):
        if stage.get("status") == "SKIPPED":
            skipped += 1
            continue
        if stage.get("status") not in ("COMPLETE", "FAILED"):
            continue
        sid, attempt = int(stage["stageId"]), int(stage.get("attemptId", 0))
        started, ended = _parse_rest_time(stage.get("submissionTime")), _parse_rest_time(stage.get("completionTime"))
        tasks = (raw.get("tasks") or {}).get(f"{sid}.{attempt}") or []
        row: dict[str, Any] = {
            "stage_id": sid, "attempt": attempt, "label": label_of.get(sid, ""), "name": stage.get("name"),
            "status": stage.get("status"),
            "duration_ms": round((ended - started) * 1000) if started is not None and ended is not None else None,
            **{name: stage.get(key, 0) or 0 for name, key in STAGE_SUM_FIELDS.items()},
            "task_attempts_retried": sum(1 for t in tasks if int(t.get("attempt", 0) or 0) > 0),
            "speculative_tasks": sum(1 for t in tasks if t.get("speculative")),
        }
        row["skew"] = task_skew(tasks) if row["shuffle_read_records"] else None
        stage_rows.append(row)
    totals = {name: sum(row[name] for row in stage_rows) for name in STAGE_SUM_FIELDS}
    totals["task_attempts_retried"] = sum(row["task_attempts_retried"] for row in stage_rows)
    totals["stage_attempts_retried"] = sum(1 for row in stage_rows if row["attempt"] > 0)
    totals["failed_stages"] = sum(1 for row in stage_rows if row["status"] == "FAILED")
    totals["stages_run"] = len(stage_rows)
    totals["stages_skipped"] = skipped
    totals["jobs"] = len(jobs)
    totals["failed_jobs"] = sum(1 for job in jobs if job.get("status") == "FAILED")
    shuffle_stages = [row for row in stage_rows if row["skew"] is not None]
    # Stage reduce theo learner_id: stage cuối của action ghi counters (đọc shuffle, ghi part-*).
    reducers = [row for row in shuffle_stages if row["label"] == "counters_output"]
    by_learner = max(reducers, key=lambda r: r["stage_id"]) if reducers else None
    return {
        "stages": stage_rows,
        "totals": totals,
        "skew_by_learner_reduce": None if by_learner is None else {"stage_id": by_learner["stage_id"], **by_learner["skew"]},
        "worst_shuffle_skew": max((row["skew"]["run_time_ms"]["max_over_median"] or 0 for row in shuffle_stages), default=None),
    }


EXECUTOR_DELTA_FIELDS = ("totalTasks", "failedTasks", "completedTasks", "totalDuration", "totalGCTime",
                         "totalInputBytes", "totalShuffleRead", "totalShuffleWrite")


def executor_deltas(before: Sequence[Mapping[str, Any]], after: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Phần của run trong các tổng cộng dồn theo executor (session dùng lại giữa các run)."""
    old = {str(e.get("id")): e for e in before}
    out: dict[str, Any] = {}
    for executor in after:
        eid = str(executor.get("id"))
        prev = old.get(eid, {})
        out[eid] = {
            **{name: (executor.get(name) or 0) - (prev.get(name) or 0) for name in EXECUTOR_DELTA_FIELDS},
            "is_active": executor.get("isActive"),
            "max_memory_bytes": executor.get("maxMemory"),
            "peak_memory_metrics_session": executor.get("peakMemoryMetrics"),
        }
    return out


# ---------------------------------------------------------------- tiến trình: driver và executor

def executor_processes(root_pid: int) -> dict[str, int]:
    """executor id → pid của các JVM executor là hậu duệ của `root_pid` (local-cluster)."""
    found: dict[str, int] = {}
    try:
        children = psutil.Process(root_pid).children(recursive=True)
    except psutil.Error:
        return found
    for proc in children:
        try:
            cmdline = proc.cmdline()
        except psutil.Error:
            continue
        if EXECUTOR_MAIN in cmdline:
            eid = cmdline[cmdline.index("--executor-id") + 1] if "--executor-id" in cmdline else str(proc.pid)
            found[eid] = proc.pid
    return found


class DriverTree(ProcessTree):
    """Cây tiến trình của driver (Python + JVM driver) trừ các cây con executor."""

    def __init__(self, pid: int, excluded_roots: Sequence[int]) -> None:
        super().__init__(pid)
        self.excluded_roots = tuple(excluded_roots)

    def sample(self) -> tuple[float, int] | None:
        try:
            root = psutil.Process(self.pid)
            members = [root, *root.children(recursive=True)]
        except psutil.Error:
            return None
        excluded: set[int] = set()
        for pid in self.excluded_roots:
            try:
                excluded |= {pid, *(p.pid for p in psutil.Process(pid).children(recursive=True))}
            except psutil.Error:
                continue
        cpu, rss = 0.0, 0
        alive: dict[int, psutil.Process] = {}
        for proc in members:
            if proc.pid in excluded:
                continue
            cached = self._procs.get(proc.pid, proc)
            try:
                cpu += cached.cpu_percent(None)
                rss += cached.memory_info().rss
            except psutil.Error:
                continue
            alive[proc.pid] = cached
        self._procs = alive
        return cpu, rss


def start_monitor(out_path: Path, interval_s: float = 0.5) -> tuple[ResourceMonitor, dict[str, int]]:
    executors = executor_processes(os.getpid())
    monitor = ResourceMonitor({}, out_path, interval_s=interval_s, write_every=2, containers=[])
    monitor.add_role("driver", os.getpid())
    monitor.trees["driver"] = DriverTree(os.getpid(), list(executors.values()))
    for eid, pid in sorted(executors.items()):
        monitor.add_role(f"executor-{eid}", pid)
    monitor.start()
    return monitor, executors


def io_counters() -> dict[str, int | None]:
    disk = psutil.disk_io_counters()
    loopback = (psutil.net_io_counters(pernic=True) or {}).get("lo")
    return {
        "disk_read_bytes": disk.read_bytes if disk else None, "disk_write_bytes": disk.write_bytes if disk else None,
        "loopback_bytes_sent": loopback.bytes_sent if loopback else None,
    }


def io_delta(before: Mapping[str, int | None], after: Mapping[str, int | None]) -> dict[str, int | None]:
    return {name: (after[name] - before[name]) if after.get(name) is not None and before.get(name) is not None else None
            for name in before}


# ---------------------------------------------------------------- kiểm dữ liệu so với bộ mô phỏng

def data_checks(stats: Mapping[str, int], dataset: Mapping[str, Any]) -> dict[str, Any]:
    """So số đếm của job với manifest bộ mô phỏng và số bản ghi đã chèn khi chuẩn bị đầu vào."""
    manifest = dataset.get("manifest_counts") or {}
    injected = dataset.get("injected") or {}
    expected = {
        "unique_events": manifest.get("unique_events"),
        "duplicate_deliveries": manifest.get("duplicate_deliveries"),
        "late_events": manifest.get("late_events"),
        "invalid": injected.get("invalid_total"),
        "heartbeats": injected.get("heartbeats"),
        "deliveries": dataset.get("lines"),
    }
    checks = {name: {"expected": value, "actual": stats.get(name), "ok": stats.get(name) == value}
              for name, value in expected.items() if value is not None}
    return {"ok": all(c["ok"] for c in checks.values()), "checks": checks}


# ---------------------------------------------------------------- một run

def _gzip_file(path: Path) -> Path:
    target = path.with_name(path.name + ".gz")
    with path.open("rb") as src, gzip.open(target, "wb") as dst:
        shutil.copyfileobj(src, dst)
    path.unlink()
    return target


def _write_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(path.name + ".tmp")
    tmp.write_text(json.dumps(data, ensure_ascii=False, indent=2, default=str) + "\n", encoding="utf-8")
    tmp.replace(path)


def _resources(summary: Mapping[str, Any]) -> dict[str, Any]:
    processes = summary.get("processes") or {}
    executors = {role: data for role, data in processes.items() if role.startswith("executor-")}

    def total(field: str, stat: str, roles: Mapping[str, Any]) -> float | None:
        values = [(data.get(field) or {}).get(stat) for data in roles.values()]
        values = [v for v in values if v is not None]
        return sum(values) if values else None

    driver = processes.get("driver") or {}
    return {
        "driver_cpu_percent_mean": (driver.get("cpu_percent") or {}).get("mean"),
        "driver_cpu_percent_peak": (driver.get("cpu_percent") or {}).get("peak"),
        "driver_rss_bytes_peak": (driver.get("rss_bytes") or {}).get("peak"),
        "executors_cpu_percent_mean": total("cpu_percent", "mean", executors),
        "executors_rss_bytes_peak_sum": total("rss_bytes", "peak", executors),
        "per_role": processes,
        "samples_interval_s": summary.get("interval_s"),
        "monitor_errors": summary.get("errors"),
    }


def run_one(
    spark: Any,
    rest: SparkRest,
    app_id: str,
    plan: Mapping[str, Any],
    run: Mapping[str, Any],
    session_info: Mapping[str, Any],
) -> dict[str, Any]:
    """Một run: job + số đo + đối chiếu; ghi metrics/<run_key>.json, raw_logs/, và run record (trừ warm-up)."""
    results = Path(plan["results_dir"])
    run_key, dataset = str(run["run_key"]), run["dataset"]
    started_at = datetime.now(timezone.utc)
    run_id = f"mr-{run_key}-{started_at:%Y%m%dT%H%M%SZ}"
    raw_logs = results / "raw_logs"
    raw_logs.mkdir(parents=True, exist_ok=True)
    log_path = raw_logs / f"{run_key}.log"
    handler = logging.FileHandler(log_path, mode="w", encoding="utf-8")
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    log.addHandler(handler)
    wall_start = time.perf_counter()
    sc = spark.sparkContext
    out_dir = Path(plan["work_dir"]) / run_key
    status, error = "success", None
    job: dict[str, Any] | None = None
    comparison: dict[str, Any] | None = None
    checks: dict[str, Any] | None = None
    rest_summary: dict[str, Any] | None = None
    executors_delta: dict[str, Any] | None = None
    resources: dict[str, Any] | None = None
    io: dict[str, Any] | None = None
    actual: ProblemCounts | None = None
    cancelled = threading.Event()
    log.info("run %s (%s): đầu vào %s, %s dòng", run_id, run_key, dataset["input"], dataset.get("lines"))
    monitor_path = raw_logs / f"{run_key}-resources.jsonl"
    monitor_path.unlink(missing_ok=True)
    try:
        before_exec = executor_snapshot(rest, app_id)
        monitor, executor_pids = start_monitor(monitor_path)
        io_before = io_counters()

        def cancel() -> None:
            cancelled.set()
            log.warning("quá %s s: hủy job group %s", plan["job_timeout_s"], run_id)
            sc.cancelJobGroup(run_id)

        timer = threading.Timer(float(plan["job_timeout_s"]), cancel)
        timer.daemon = True
        timer.start()
        try:
            job = run_problem_job(
                sc, dataset["input"], out_dir, input_partitions=int(plan["input_partitions"]),
                partitions=int(plan["partitions"]), k=int(plan["k"]), late_threshold_s=float(plan["late_threshold_s"]),
                job_group=run_id, on_action=lambda label: log.info("action %s", label),
            )
        finally:
            timer.cancel()
            io = io_delta(io_before, io_counters())
            resources = _resources(monitor.stop())
            resources["executor_pids"] = executor_pids
        log.info("job xong sau %.2f s: %s", job["wall_s"], job["stats"])
        raw = collect_rest(rest, app_id, run_id)
        rest_summary = summarize_rest(raw)
        executors_delta = executor_deltas(before_exec, executor_snapshot(rest, app_id))
        with gzip.open(raw_logs / f"{run_key}-spark-rest.json.gz", "wt", encoding="utf-8") as handle:
            json.dump(raw, handle)
        actual = read_job_outputs(out_dir)
        expected = ProblemCounts.load(Path(dataset["reference"]))
        comparison = compare_counts(actual, expected)
        checks = data_checks(actual.stats, dataset)
        log.info("đối chiếu tham chiếu: %s; kiểm theo bộ mô phỏng: %s", comparison["checks"], checks["ok"])
        if not comparison["match"]:
            status, error = "failed", "kết quả job khác bản tham chiếu"
        elif not checks["ok"]:
            status, error = "failed", "số đếm khác manifest bộ mô phỏng hoặc số bản ghi đã chèn"
    except Exception as exc:  # run lỗi vẫn có metrics và run record
        status = "timeout" if cancelled.is_set() else "failed"
        error = repr(exc)
        log.exception("run %s thất bại", run_id)
    runtime_s = time.perf_counter() - wall_start

    deliveries = (job or {}).get("stats", {}).get("deliveries")
    wall_s = (job or {}).get("wall_s")
    metrics: dict[str, Any] = {
        "run_key": run_key, "run_id": run_id, "status": status, "error": error, "warmup": bool(run.get("warmup")),
        "scenario": run["scenario"], "config": plan["config"], "scale_learners": dataset["learners"],
        "repeat": run.get("repeat"), "started_at": started_at.isoformat(),
        "job_wall_s": wall_s,
        "throughput_deliveries_per_s": round(deliveries / wall_s, 1) if deliveries and wall_s else None,
        "actions_s": (job or {}).get("actions"),
        "counts": actual.stats if actual else (job or {}).get("stats"),
        "invalid_by_reason": actual.invalid_by_reason if actual else (job or {}).get("invalid_by_reason"),
        "top_k": (job or {}).get("top_k"),
        "correctness": comparison,
        "data_checks": checks,
        "resources": resources,
        "io_delta_machine": io,
        "spark": rest_summary,
        "executors_delta": executors_delta,
        "invalid_samples": actual.invalid_samples if actual else [],
        "runtime_s": round(runtime_s, 3),
        "session": session_info,
    }
    metrics_dir = results / "metrics" / ("warmup" if run.get("warmup") else "")
    _write_json(metrics_dir / f"{run_key}.json", metrics)
    log.info("run %s %s sau %.1f s", run_id, status, runtime_s)
    log.removeHandler(handler)
    handler.close()
    log_gz = _gzip_file(log_path)
    if monitor_path.exists():
        _gzip_file(monitor_path)
    if not run.get("warmup"):
        totals = (rest_summary or {}).get("totals") or {}
        record = build_run_record(
            experiment=EXPERIMENT,
            data_version=f"{Path(dataset['input']).name} {dataset.get('input_checksum')}",
            parameters={
                "scenario": run["scenario"], "config": plan["config"], "resources": plan["resources"],
                "spark_mode": plan["spark_mode"], "master": session_info.get("master"), "repeat": run.get("repeat"),
                "learners": dataset["learners"], "dataset_key": dataset["dataset_key"], "dataset_params": dataset["params"],
                "invalid_rate": dataset.get("invalid_rate"), "input": dataset["input"],
                "input_partitions": plan["input_partitions"], "partitions": plan["partitions"], "k": plan["k"],
                "late_threshold_s": plan["late_threshold_s"], "session_reused": True,
                "metrics_path": str(metrics_dir / f"{run_key}.json"),
            },
            seed=int(dataset["seed"]),
            timeout_s=float(plan["job_timeout_s"]),
            metrics={
                "job_wall_s": wall_s, "throughput_deliveries_per_s": metrics["throughput_deliveries_per_s"],
                "counts": metrics["counts"], "invalid_by_reason": metrics["invalid_by_reason"],
                "correct": bool(comparison and comparison["match"]), "correctness_checks": (comparison or {}).get("checks"),
                "data_checks_ok": bool(checks and checks["ok"]),
                "resources": {k: v for k, v in (resources or {}).items() if k not in ("per_role", "executor_pids")},
                "io_delta_machine": io,
                "spark_totals": totals,
                "skew_by_learner_reduce": (rest_summary or {}).get("skew_by_learner_reduce"),
                "error": error,
            },
            status=status,
            log_path=str(log_gz),
            started_at=started_at,
            repo_dir=REPO_ROOT,
            run_id=run_id,
            runtime_s=runtime_s,
            spark_conf=session_info.get("spark_conf"),
            images={},
        )
        append_run_record(record, Path(plan["runs_jsonl"]))
    return metrics


# ---------------------------------------------------------------- session của một cấu hình

def _harvest_worker_logs(app_id: str, raw_logs: Path, config: str) -> list[str]:
    """Chép stdout/stderr của executor (thư mục work/ của local-cluster dưới SPARK_HOME) vào raw_logs, rồi xóa."""
    try:
        from pyspark.find_spark_home import _find_spark_home

        work = Path(_find_spark_home()) / "work" / app_id
    except Exception:
        return []
    copied: list[str] = []
    if not work.is_dir():
        return copied
    for path in sorted(work.glob("*/std*")):
        target = raw_logs / f"session-{config}-executor-{path.parent.name}-{path.name}.log.gz"
        with path.open("rb") as src, gzip.open(target, "wb") as dst:
            shutil.copyfileobj(src, dst)
        copied.append(target.name)
    shutil.rmtree(work, ignore_errors=True)
    return copied


def run_session(plan: Mapping[str, Any]) -> int:
    """Mở session của một cấu hình, chạy lần lượt các run của kế hoạch; trả số run không success."""
    from pyspark.sql import SparkSession

    config = CONFIGS[str(plan["config"])]
    results = Path(plan["results_dir"])
    port = free_port()
    conf = mapreduce_spark_conf(config, str(plan["spark_mode"]), partitions=int(plan["partitions"]), ui_port=port,
                                driver_memory=str(plan.get("driver_memory") or DEFAULT_DRIVER_MEMORY),
                                data_root=Path(plan["data_root"]))
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    started = time.perf_counter()
    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    failures = 0
    app_id = ""
    try:
        ui = spark.sparkContext.uiWebUrl or f"http://127.0.0.1:{port}"
        actual_port = urllib.parse.urlparse(ui).port or port
        rest = SparkRest(f"http://127.0.0.1:{actual_port}")
        app_id = rest.app_id()
        expected = config.workers if plan["spark_mode"] == "local-cluster" else 0
        executors = wait_for_executors(rest, app_id, expected) if expected else []
        startup_s = time.perf_counter() - started
        session_info = {
            "app_id": app_id, "master": conf["spark.master"], "spark_mode": plan["spark_mode"],
            "startup_s": round(startup_s, 3), "spark_version": spark.version, "ui_port": actual_port,
            "executors": [{k: e.get(k) for k in ("id", "hostPort", "totalCores", "maxMemory")} for e in executors],
            "spark_conf": conf,
        }
        _write_json(results / "configs" / f"spark-{config.name}.json", {
            "config": config.describe(), "spark_mode": plan["spark_mode"], **session_info,
            "input_partitions": plan["input_partitions"], "partitions": plan["partitions"],
        })
        print(json.dumps({"event": "session", "config": config.name, "startup_s": session_info["startup_s"],
                          "executors": len(executors)}), flush=True)
        for run in plan["runs"]:
            metrics = run_one(spark, rest, app_id, plan, run, session_info)
            failures += metrics["status"] != "success"
            print(json.dumps({"event": "run", "run_key": metrics["run_key"], "status": metrics["status"],
                              "job_wall_s": metrics["job_wall_s"],
                              "match": (metrics["correctness"] or {}).get("match")}), flush=True)
    finally:
        spark.stop()
        if app_id:
            _harvest_worker_logs(app_id, results / "raw_logs", config.name)
    return failures


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Chạy các run MapReduce của một cấu hình tài nguyên trong một SparkSession.")
    parser.add_argument("--plan", type=Path, required=True, help="kế hoạch JSON do mapreduce_matrix ghi")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s", stream=sys.stderr)
    plan = json.loads(args.plan.read_text(encoding="utf-8"))
    if str(plan.get("config")) not in CONFIGS:
        parser.error(f"config phải thuộc {sorted(CONFIGS)}")
    return 1 if run_session(plan) else 0


if __name__ == "__main__":
    raise SystemExit(main())
