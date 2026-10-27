"""Runner thí nghiệm: mở rộng ma trận rồi chạy từng run với vòng đời đầy đủ, ghi một run record cho mỗi lần thử.

Vòng đời một run streaming (`kind: stream`):
1. kiểm dung lượng (`scripts/disk-check.sh`, dừng runner nếu SSD trống < ngưỡng) và stack (Kafka + Redis
   trả lời; nếu không thì `docker compose up -d --wait`, không bao giờ dừng container);
2. dữ liệu: cache bộ mô phỏng theo (learners, rate, late_profile, trùng, thời lượng, seed);
3. reset: topic riêng của run (`exp-<run_id>`, `-dl`, `-al`, tạo lại rỗng với số partition cấu hình),
   checkpoint/lake mới theo run_id, Redis DB riêng cho thí nghiệm (FLUSHDB — từ chối DB 0 và DB có
   khóa lạ) rồi nạp `roster:{class}` từ `roster.json`;
4. job Spark (`stream_job` → `streaming.job.main`, shuffle partitions = số Kafka partition), chờ
   query trạng thái khởi động (dòng `started` của listener);
5. phát lại open-loop đúng tốc độ (`replay_child`), heartbeat tới khi watermark của job ≥ cuối log;
6. drain: chờ batch có lag 0 đo sau khi phát lại xong (và watermark ≥ cuối log nếu có watermark),
   hạn 2·W (+ 2 chu kỳ trigger) — không drain được ⇒ `timeout`; giới hạn cứng duration + 2·W + 5 phút;
7. dừng job bằng SIGTERM (job dừng ở ranh giới micro-batch), đối soát (`streaming.reconcile`) ra
   `results/raw/<run_id>/reconcile.json`, tính chỉ số (`metrics`), nén tiến độ/giám sát/log vào
   `results/raw/<run_id>/`;
8. dọn: xóa topic, checkpoint, Redis của run; lake chuyển sang `--archive-dir` hoặc xóa;
9. nối đúng một run record (Phụ lục B + `runtime_s`, `spark_conf`, `replay_start`, môi trường) — kể cả
   khi lỗi/timeout/bị dừng. Không bao giờ xóa hay sửa bản ghi cũ.

Loại khác: `batch` (batch baseline daily/hourly trên cửa sổ log đã phát lại của run streaming cùng
seed, CLI `batch.run_batch_baseline` tự ghi run record), `hint` (`scripts/hint-load-test.py` một mức
user, tự ghi run record), `ingest` (chỉ bộ phát lại, đo trần tốc độ).

Kỷ luật tiến trình: mọi tiến trình con chạy session riêng và được ghi pidfile; tín hiệu
SIGTERM/SIGINT/SIGHUP ⇒ dừng con, ghi bản ghi `aborted`, dọn rồi thoát; runner khởi động sau một lần
bị SIGKILL dọn các nhóm tiến trình mồ côi (`reaper`). Khóa flock: một runner mỗi máy.

CLI: python -m ai_math_tutor.experiments.runner --plan configs/experiments/plan-v1.0.yaml
       --exp A|B|C|D|E|ablations|stress|soak|ceiling|all[,…] [--repeats 3] [--dry-run] [--only SLUG]
       [--redis-db 9] [--archive-dir DIR] [--keep-lake] [--max-attempts 3]
"""

from __future__ import annotations

import argparse
import contextlib
import gzip
import json
import os
import platform
import re
import shutil
import signal
import subprocess
import sys
import time
import traceback
from collections.abc import Callable, Iterator, Mapping, Sequence
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ai_math_tutor.config import REPO_ROOT, ConfigError, RuntimeConfig, load_config
from ai_math_tutor.experiments import matrix as M
from ai_math_tutor.experiments import metrics as X
from ai_math_tutor.experiments.datasets import ensure_dataset, materialize_window, read_manifest
from ai_math_tutor.experiments.monitor import ResourceMonitor, compose_containers, has_event, tail_record
from ai_math_tutor.experiments.reaper import LockError, PidRegistry, RunnerLock, kill_group, reap_stale
from ai_math_tutor.run_record import append_run_record, build_run_record

DEFAULT_REDIS_DB = 9
DEFAULT_HINT_PORT = 8765
STARTUP_TIMEOUT_S = 300.0
STOP_TIMEOUT_S = 180.0
CHILD_GRACE_S = 15.0
BATCH_TIMEOUT_S = 900.0
HINT_SCRIPT = REPO_ROOT / "scripts" / "hint-load-test.py"
DISK_CHECK = REPO_ROOT / "scripts" / "disk-check.sh"
ABANDONED_FILE = "experiment-abandoned.jsonl"
CONTEXT_FILE = "context.json"
# Khóa pipeline (docs/event-schema.md, "Khóa Redis"); khóa khác trong DB thí nghiệm ⇒ DB không thuộc runner.
PIPELINE_KEY_PREFIXES = ("state:", "progress:", "roster:", "agg:class:", "stuck:set:", "alert:", "alerts:stream",
                         "trend:", "metrics:pipeline", "hintctx:", "lasthint:")
EXIT_OK, EXIT_FAILED_RUNS, EXIT_DISK, EXIT_LOCK, EXIT_USAGE, EXIT_INTERRUPTED = 0, 1, 3, 4, 2, 130


class RunnerError(RuntimeError):
    """Lỗi của runner (môi trường, tham số an toàn, tài nguyên)."""


class DiskLow(RunnerError):
    """SSD trống dưới ngưỡng: dừng cả runner (không bắt đầu run mới)."""


class RunnerInterrupted(BaseException):
    """Runner nhận SIGTERM/SIGINT/SIGHUP. BaseException để `except Exception` không nuốt mất."""


# ---------------------------------------------------------------------------------------------
# Tín hiệu và tiến trình con.
# ---------------------------------------------------------------------------------------------


class SignalGuard:
    """Tín hiệu chỉ được ghi nhận; vòng chờ của runner gọi `check()` và dừng run ở điểm an toàn.

    Không ném ngoại lệ từ trình xử lý tín hiệu ở lần đầu: nếu ném đúng lúc giữa thân run và phần dọn
    dẹp thì tiến trình con và bản ghi run sẽ bị bỏ lại. Tín hiệu thứ hai (ngoài khối dọn dẹp) ném ngay
    để thoát được cả khi đang ở bước dài không có điểm kiểm (vd. sinh dữ liệu lớn).
    """

    SIGNALS = (signal.SIGTERM, signal.SIGINT, signal.SIGHUP)

    def __init__(self) -> None:
        self.received: int | None = None
        self.count = 0
        self.raised = False
        self._depth = 0

    def install(self) -> None:
        for sig in self.SIGNALS:
            signal.signal(sig, self._handle)

    def _handle(self, signum: int, _frame: Any) -> None:
        print(f"RUNNER nhận tín hiệu {signum}", file=sys.stderr, flush=True)
        self.count += 1
        if self.received is None:
            self.received = signum
        if self.count >= 2 and self._depth == 0 and not self.raised:
            self.raised = True
            raise RunnerInterrupted(signum)

    def check(self) -> None:
        """Ném RunnerInterrupted (một lần) nếu đã nhận tín hiệu và không ở trong khối dọn dẹp."""
        if self.received is not None and self._depth == 0 and not self.raised:
            self.raised = True
            raise RunnerInterrupted(self.received)

    @contextlib.contextmanager
    def deferred(self) -> Iterator[None]:
        """Khối dọn dẹp + ghi run record: tín hiệu trong khối này không cắt ngang."""
        self._depth += 1
        try:
            yield
        finally:
            self._depth -= 1


class Children:
    """Tiến trình con của runner: session riêng (nhóm tiến trình), pidfile, dừng có kiểm soát."""

    def __init__(self, registry: PidRegistry) -> None:
        self.registry = registry
        self.active: list[subprocess.Popen[bytes]] = []

    def spawn(self, run_id: str, role: str, command: Sequence[str], log_path: Path,
              env: Mapping[str, str] | None = None) -> subprocess.Popen[bytes]:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        with log_path.open("ab") as handle:
            process = subprocess.Popen(list(command), stdout=handle, stderr=subprocess.STDOUT, cwd=REPO_ROOT,
                                       env=dict(env) if env is not None else None, start_new_session=True)
        self.active.append(process)
        self.registry.register(run_id, role, process, " ".join(str(c) for c in command))
        return process

    def stop(self, process: subprocess.Popen[bytes], grace_s: float = CHILD_GRACE_S) -> int | None:
        """SIGTERM tới tiến trình trưởng (job/bộ phát lại tự dừng gọn), chờ, rồi dọn cả nhóm còn sót."""
        if process.poll() is None:
            with contextlib.suppress(ProcessLookupError):
                os.kill(process.pid, signal.SIGTERM)
            with contextlib.suppress(subprocess.TimeoutExpired):
                process.wait(timeout=grace_s)
        kill_group(process.pid, grace_s=10.0)  # JVM/shard còn sót sau khi trưởng nhóm thoát
        with contextlib.suppress(subprocess.TimeoutExpired):
            process.wait(timeout=10)
        if process in self.active:
            self.active.remove(process)
        return process.returncode

    def stop_all(self, grace_s: float = CHILD_GRACE_S) -> None:
        for process in list(self.active):
            self.stop(process, grace_s)

    def release(self, run_id: str) -> None:
        if not self.active:
            self.registry.forget(run_id)


# ---------------------------------------------------------------------------------------------
# Redis của thí nghiệm.
# ---------------------------------------------------------------------------------------------


def foreign_keys(client: Any, prefix: str = "", limit: int = 5) -> list[str]:
    """Khóa trong DB không phải khóa pipeline (có tiền tố `prefix`) — tối đa `limit` ví dụ."""
    known = tuple(f"{prefix}{p}" for p in PIPELINE_KEY_PREFIXES)
    found: list[str] = []
    for raw in client.scan_iter(count=1000):
        key = raw.decode() if isinstance(raw, bytes) else str(raw)
        if not key.startswith(known):
            found.append(key)
            if len(found) >= limit:
                break
    return found


def delete_prefix(client: Any, prefix: str) -> int:
    keys = list(client.scan_iter(match=f"{prefix}*", count=1000))
    for start in range(0, len(keys), 1000):
        client.delete(*keys[start:start + 1000])
    return len(keys)


def reset_redis(client: Any, db: int, prefix: str) -> dict[str, Any]:
    """Đưa trạng thái Redis của run về rỗng. Không bao giờ FLUSHALL; FLUSHDB chỉ trên DB ≠ 0 chỉ chứa khóa pipeline."""
    if prefix:
        return {"mode": "prefix", "db": db, "deleted": delete_prefix(client, prefix)}
    if db == 0:
        raise RunnerError("từ chối FLUSHDB trên DB 0 (stack dùng chung); dùng --redis-db ≠ 0 hoặc --redis-key-prefix")
    before = int(client.dbsize())
    if before:
        strange = foreign_keys(client)
        if strange:
            raise RunnerError(f"DB {db} có khóa không thuộc pipeline (vd. {strange}); không FLUSHDB — chọn DB khác")
    client.flushdb()
    return {"mode": "flushdb", "db": db, "keys_before": before}


# ---------------------------------------------------------------------------------------------
# Môi trường, đĩa, tiện ích.
# ---------------------------------------------------------------------------------------------


def _wslconfig() -> dict[str, Any]:
    """Cấu hình .wslconfig của người dùng Windows (bộ nhớ/CPU cấp cho WSL2) — chỉ dòng khóa=giá trị."""
    for path in sorted(Path("/mnt/c/Users").glob("*/.wslconfig")) if Path("/mnt/c/Users").is_dir() else []:
        values: dict[str, str] = {}
        section = ""
        try:
            lines = path.read_text(encoding="utf-8", errors="replace").splitlines()
        except OSError:
            continue
        for raw in lines:
            line = raw.split("#", 1)[0].strip()
            if line.startswith("[") and line.endswith("]"):
                section = line[1:-1].strip()
            elif "=" in line:
                key, value = (part.strip() for part in line.split("=", 1))
                values[f"{section}.{key}" if section else key] = value
        return {"path": str(path), "values": values}
    return {}


def environment_info() -> dict[str, Any]:
    """CPU (model; i9-14900K = 8 P-core + 16 E-core), số CPU logic WSL thấy, kernel WSL, .wslconfig."""
    cpu_model = None
    with contextlib.suppress(OSError):
        for line in Path("/proc/cpuinfo").read_text(encoding="utf-8").splitlines():
            if line.startswith("model name"):
                cpu_model = line.split(":", 1)[1].strip()
                break
    return {"cpu_model": cpu_model, "logical_cpus": os.cpu_count(), "kernel": platform.release(),
            "wsl_distro": os.environ.get("WSL_DISTRO_NAME"), "wslconfig": _wslconfig()}


def disk_check(data_root: Path, threshold_gb: float) -> str:
    env = {**os.environ, "NHOM8_DATA_ROOT": str(data_root)}
    try:
        out = subprocess.run(["bash", str(DISK_CHECK), str(threshold_gb)], capture_output=True, text=True,
                             timeout=30, env=env)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError(f"không chạy được disk-check: {exc}") from exc
    line = (out.stdout or "").strip()
    if out.returncode == 1:
        raise DiskLow(f"{line} {(out.stderr or '').strip()}")
    if out.returncode != 0:
        raise RunnerError(f"disk-check lỗi (mã {out.returncode}): {(out.stderr or '').strip()}")
    return line


def stack_problems(cfg: RuntimeConfig, redis_url: str) -> list[str]:
    """Kafka (metadata qua AdminClient) và Redis (PING) có trả lời không; trả danh sách lỗi (rỗng = khỏe)."""
    from ai_math_tutor.ingestion.topics import admin_client, describe_topics

    problems: list[str] = []
    try:
        describe_topics(admin_client(cfg.kafka.bootstrap_servers), timeout_s=10)
    except Exception as exc:  # broker tắt: TopicError hoặc KafkaException tùy chỗ hỏng
        problems.append(f"kafka: {type(exc).__name__}: {exc}")
    try:
        import redis

        client = redis.Redis.from_url(redis_url, socket_timeout=5, socket_connect_timeout=5)
        try:
            client.ping()
        finally:
            client.close()
    except Exception as exc:
        problems.append(f"redis: {type(exc).__name__}")
    return problems


def ensure_stack(cfg: RuntimeConfig, redis_url: str, *, up_timeout_s: float = 300.0) -> dict[str, Any]:
    """Stack khỏe thì thôi; không thì `docker compose up -d --wait` (chỉ khởi động container đang dừng,
    không bao giờ dừng/tạo lại container đang chạy) rồi kiểm lại. Vẫn hỏng ⇒ RunnerError (không đốt lượt thử)."""
    problems = stack_problems(cfg, redis_url)
    if not problems:
        return {"healthy": True, "started": False}
    print(f"RUNNER stack chưa sẵn sàng ({problems}); chạy docker compose up -d --wait", file=sys.stderr, flush=True)
    try:
        out = subprocess.run(["docker", "compose", "up", "-d", "--wait"], cwd=REPO_ROOT, capture_output=True,
                             text=True, timeout=up_timeout_s)
    except (OSError, subprocess.SubprocessError) as exc:
        raise RunnerError(f"không khởi động được stack: {exc}") from exc
    for _ in range(30):
        problems = stack_problems(cfg, redis_url)
        if not problems:
            return {"healthy": True, "started": True, "compose_exit": out.returncode}
        time.sleep(2.0)
    raise RunnerError(f"stack vẫn chưa khỏe sau docker compose up (mã {out.returncode}): {problems}")


def utc_stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")


def now_ms() -> int:
    return time.time_ns() // 1_000_000


def gzip_to(src: Path, dst: Path) -> bool:
    if not src.is_file():
        return False
    with src.open("rb") as fin, gzip.open(dst, "wb", compresslevel=6) as fout:
        shutil.copyfileobj(fin, fout)
    return True


def repo_relative(path: Path) -> str:
    try:
        return str(Path(path).resolve().relative_to(REPO_ROOT))
    except ValueError:
        return str(path)


def wait_until(predicate: Callable[[], bool], timeout_s: float, *, alive: Callable[[], bool] | None = None,
               what: str = "điều kiện", poll_s: float = 1.0, check: Callable[[], None] | None = None) -> None:
    deadline = time.monotonic() + timeout_s
    while not predicate():
        if check is not None:
            check()
        if alive is not None and not alive():
            raise RunnerError(f"tiến trình thoát trước khi {what}")
        if time.monotonic() >= deadline:
            raise TimeoutError(f"quá {timeout_s:.0f} s chờ {what}")
        time.sleep(poll_s)


# ---------------------------------------------------------------------------------------------
# Runner.
# ---------------------------------------------------------------------------------------------


@dataclass
class RunnerOptions:
    plan_path: Path
    results_dir: Path
    data_root: Path
    redis_db: int = DEFAULT_REDIS_DB
    redis_key_prefix: str = ""
    max_attempts: int = 3
    archive_dir: Path | None = None
    keep_lake: bool = False
    disk_threshold_gb: float = 10.0
    hint_port: int = DEFAULT_HINT_PORT
    startup_timeout_s: float = STARTUP_TIMEOUT_S
    stop_timeout_s: float = STOP_TIMEOUT_S

    @property
    def runner_dir(self) -> Path:
        return self.data_root / "exp-runner"

    @property
    def work_root(self) -> Path:
        return self.data_root / "exp-work"


@dataclass
class RunContext:
    """Những gì một lần thử thu được — đủ để ghi run record dù dừng ở bước nào."""

    spec: M.RunSpec
    attempt: int
    run_id: str
    work: Path
    out: Path
    started_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    wall0: float = field(default_factory=time.monotonic)
    phases: dict[str, float] = field(default_factory=dict)
    params: dict[str, Any] = field(default_factory=dict)
    metrics: dict[str, Any] = field(default_factory=dict)
    error: str | None = None
    interrupted: bool = False
    timed_out: bool = False
    data_version: str = ""
    spark_conf: dict[str, Any] = field(default_factory=dict)
    timeout_s: float = 0.0
    cleanup: dict[str, Any] = field(default_factory=dict)

    def log(self, message: str) -> None:
        line = f"{datetime.now(timezone.utc):%H:%M:%S} {self.run_id} {message}"
        print(f"RUNNER {line}", file=sys.stderr, flush=True)
        with contextlib.suppress(OSError):
            with (self.work / "runner.log").open("a", encoding="utf-8") as handle:
                handle.write(line + "\n")

    @contextlib.contextmanager
    def phase(self, name: str) -> Iterator[None]:
        started = time.monotonic()
        self.log(f"bắt đầu {name}")
        try:
            yield
        finally:
            self.phases[name] = round(time.monotonic() - started, 3)


class Runner:
    def __init__(self, opts: RunnerOptions, plan: M.Plan, cfg: RuntimeConfig, guard: SignalGuard | None = None) -> None:
        self.opts = opts
        self.plan = plan
        self.cfg = cfg
        self.guard = guard or SignalGuard()
        self.registry = PidRegistry(opts.runner_dir / "pids")
        self.children = Children(self.registry)
        self.environment = environment_info()
        self.records_path = opts.results_dir / "runs.jsonl"
        # Client Redis của runner dùng cùng DB thí nghiệm mà job con nhận qua NHOM8_REDIS_DB.
        self.experiment_cfg = replace(cfg, redis=replace(cfg.redis, db=opts.redis_db))
        self.stack: dict[str, Any] = {}

    # ----------------------------------------------------------------- vòng ngoài

    def records(self) -> list[dict[str, Any]]:
        return M.load_records(self.records_path)

    def execute(self, specs: Sequence[M.RunSpec]) -> dict[str, Any]:
        summary: dict[str, Any] = {"done_before": 0, "success": 0, "failed_attempts": 0, "abandoned": [],
                                   "blocked": [], "runs": []}
        for spec in specs:
            state = M.spec_state(spec, self.records(), self.opts.max_attempts)
            if state.status == "done":
                summary["done_before"] += 1
                continue
            if state.status == "abandoned":
                self.note_abandoned(spec, state)
                summary["abandoned"].append(spec.run_prefix)
                continue
            source = None
            if spec.kind == "batch":
                source = self.batch_source(spec)
                if source is None:
                    print(f"RUNNER bỏ qua {spec.run_prefix}: run streaming nguồn chưa success", file=sys.stderr)
                    summary["blocked"].append(spec.run_prefix)
                    continue
            attempts = state.attempts
            while attempts < self.opts.max_attempts:
                self.guard.check()
                disk = disk_check(self.opts.data_root, self.opts.disk_threshold_gb)
                print(f"RUNNER {disk}", file=sys.stderr, flush=True)
                self.stack = ensure_stack(self.cfg, self.experiment_cfg.redis.url)
                record = self.run_once(spec, attempts + 1, source)
                summary["runs"].append({"run_id": record["run_id"], "status": record["status"]})
                if record["status"] == "success":
                    summary["success"] += 1
                    break
                attempts += 1
                summary["failed_attempts"] += 1
            else:
                state = M.spec_state(spec, self.records(), self.opts.max_attempts)
                self.note_abandoned(spec, state)
                summary["abandoned"].append(spec.run_prefix)
        return summary

    def run_once(self, spec: M.RunSpec, attempt: int, source: Mapping[str, Any] | None = None) -> dict[str, Any]:
        if spec.kind == "batch":
            return self.run_batch(spec, attempt, source or {})
        if spec.kind == "stream":
            return self.run_stream(spec, attempt)
        if spec.kind == "ingest":
            return self.run_ingest(spec, attempt)
        return self.run_hint(spec, attempt)

    def wait_process(self, proc: subprocess.Popen[bytes], timeout_s: float) -> int | None:
        """Chờ tiến trình con, kiểm tín hiệu mỗi giây; None khi quá hạn."""
        deadline = time.monotonic() + timeout_s
        while proc.poll() is None:
            self.guard.check()
            if time.monotonic() >= deadline:
                return None
            time.sleep(1.0)
        return proc.returncode

    def note_abandoned(self, spec: M.RunSpec, state: M.SpecState) -> None:
        path = self.opts.results_dir / ABANDONED_FILE
        if any(r.get("run_prefix") == spec.run_prefix for r in M.load_records(path)):
            return
        entry = {"run_prefix": spec.run_prefix, "config": spec.describe(), "attempts": state.attempts,
                 "last_run_id": state.last_run_id, "reason": state.last_error,
                 "at": datetime.now(timezone.utc).isoformat()}
        with path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(entry, ensure_ascii=False, sort_keys=True) + "\n")
        print(f"RUNNER abandoned {spec.run_prefix} sau {state.attempts} lần thử: {state.last_error}", file=sys.stderr)

    def batch_source(self, spec: M.RunSpec) -> dict[str, Any] | None:
        prefix = f"{M.RUN_PREFIX['stream']}-{spec.source_slug}-s{spec.seed}-"
        return next((r for r in reversed(self.records())
                     if str(r.get("run_id", "")).startswith(prefix) and r.get("status") == "success"), None)

    # ----------------------------------------------------------------- tiện ích của một lần thử

    def new_context(self, spec: M.RunSpec, attempt: int) -> RunContext:
        run_id = spec.run_id(utc_stamp())
        # Lần thử lại ngay trong cùng giây sẽ trùng run_id: chờ sang giây mới.
        while (self.opts.work_root / run_id).exists() or (self.opts.results_dir / run_id).exists():
            time.sleep(1.0)
            run_id = spec.run_id(utc_stamp())
        work = self.opts.work_root / run_id
        out = self.opts.results_dir / run_id
        work.mkdir(parents=True, exist_ok=False)
        out.mkdir(parents=True, exist_ok=False)
        ctx = RunContext(spec=spec, attempt=attempt, run_id=run_id, work=work, out=out)
        self.registry.open_run(run_id)
        ctx.params = {**dict(spec.params), "config_key": spec.slug, "groups": list(spec.groups), "seed": spec.seed,
                      "attempt": attempt, "plan": repo_relative(self.plan.path), "plan_version": self.plan.version,
                      "harness": "ai_math_tutor.experiments.runner"}
        ctx.metrics["loadavg_at_start"] = list(os.getloadavg())
        ctx.metrics["stack_at_start"] = dict(self.stack)
        (work / CONTEXT_FILE).write_text(json.dumps({
            "run_id": run_id, "kind": spec.kind, "experiment": spec.experiment, "seed": spec.seed,
            "params": ctx.params, "started_at": ctx.started_at.isoformat(), "out": str(out),
        }, ensure_ascii=False, indent=2), encoding="utf-8")
        return ctx

    def recover_orphans(self, reaped: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
        """Run dở của một runner đã chết (SIGKILL, mất điện): dọn topic/checkpoint/lake/Redis/thư mục làm
        việc và ghi bản ghi `aborted` (không tính là lần thử) — run nào bắt đầu cũng có đúng một bản ghi."""
        from ai_math_tutor.ingestion.topics import delete_topics

        recovered = []
        for run_id in dict.fromkeys(str(a["run_id"]) for a in reaped):
            work = self.opts.work_root / run_id
            try:
                context = json.loads((work / CONTEXT_FILE).read_text(encoding="utf-8"))
            except (OSError, json.JSONDecodeError):
                context = {}
            cleanup: dict[str, Any] = {}

            def step(name: str, action: Callable[[], Any]) -> None:
                try:
                    cleanup[name] = action()
                except Exception as exc:
                    cleanup[name] = f"error: {type(exc).__name__}: {exc}"

            out = Path(context.get("out") or self.opts.results_dir / run_id)
            out.mkdir(parents=True, exist_ok=True)
            if work.is_dir():
                step("artifacts", lambda: self.copy_work_artifacts(work, out))
            topics = [f"exp-{run_id}", f"exp-{run_id}-dl", f"exp-{run_id}-al"]
            step("topics", lambda: delete_topics(self.cfg.kafka.bootstrap_servers, topics, timeout_s=120))
            step("checkpoint", lambda: self.archive_or_remove(self.cfg.paths.checkpoint_dir(run_id), "checkpoints"))
            step("lake", lambda: self.archive_or_remove(self.cfg.paths.lake_dir(run_id), "lake", keep=self.opts.keep_lake))
            step("batch_snapshots", lambda: self.archive_or_remove(self.opts.data_root / "batch" / run_id, "batch"))
            if context.get("kind") == "stream":
                step("redis", lambda: reset_redis(self.redis_client(), self.opts.redis_db, self.opts.redis_key_prefix))
            exists = any(r.get("run_id") == run_id for r in self.records())
            if context and not exists:
                started = datetime.fromisoformat(context["started_at"])
                record = build_run_record(
                    experiment=str(context.get("experiment", "unknown")), data_version="unknown",
                    parameters=context.get("params") or {}, seed=int(context.get("seed", 0)), timeout_s=0.0,
                    metrics={"interrupted": True, "valid": False, "cleanup": cleanup,
                             "error": "runner chết giữa run; tiến trình con và tài nguyên được dọn khi runner khởi động lại",
                             "invalid_reasons": ["runner chết giữa run"]},
                    status="aborted", log_path=repo_relative(out), started_at=started, repo_dir=REPO_ROOT,
                    run_id=run_id, runtime_s=None)
                record["environment"] = self.environment
                append_run_record(record, self.records_path)
            step("work", lambda: self.archive_or_remove(work, "work"))
            recovered.append({"run_id": run_id, "record_written": bool(context) and not exists, "cleanup": cleanup})
            print(f"RUNNER đã khôi phục run dở {run_id}: {cleanup}", file=sys.stderr, flush=True)
        return recovered

    def child_env(self) -> dict[str, str]:
        return {**os.environ, "NHOM8_DATA_ROOT": str(self.opts.data_root), "PYTHONUNBUFFERED": "1"}

    def redis_client(self) -> Any:
        import redis

        return redis.Redis.from_url(self.experiment_cfg.redis.url, socket_timeout=30)

    def finish(self, ctx: RunContext, status: str, reasons: Sequence[str], *, write_record: bool = True,
               experiment: str | None = None) -> dict[str, Any]:
        ctx.metrics.update({"error": ctx.error, "invalid_reasons": list(reasons), "valid": status == "success",
                            "interrupted": ctx.interrupted, "phases_s": ctx.phases, "cleanup": ctx.cleanup})
        record = build_run_record(
            experiment=experiment or ctx.spec.experiment, data_version=ctx.data_version or "unknown",
            parameters=ctx.params, seed=ctx.spec.seed, timeout_s=float(ctx.timeout_s), metrics=ctx.metrics,
            status=status, log_path=repo_relative(ctx.out), started_at=ctx.started_at, repo_dir=REPO_ROOT,
            run_id=ctx.run_id, runtime_s=round(time.monotonic() - ctx.wall0, 3), spark_conf=ctx.spark_conf,
        )
        record["environment"] = self.environment
        if write_record:
            append_run_record(record, self.records_path)
        ctx.log(f"run record: {status} {list(reasons)}")
        return record

    def archive_or_remove(self, path: Path, kind: str, keep: bool = False) -> str:
        if not path.exists():
            return "absent"
        if self.opts.archive_dir is not None:
            target = self.opts.archive_dir / kind / path.name
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.move(str(path), str(target))
            return f"archived:{target}"
        if keep:
            return "kept"
        shutil.rmtree(path, ignore_errors=True)
        return "removed"

    def cleanup_step(self, ctx: RunContext, name: str, action: Callable[[], Any]) -> None:
        try:
            ctx.cleanup[name] = action()
        except Exception as exc:  # dọn lỗi không được che bản ghi run
            ctx.cleanup[name] = f"error: {type(exc).__name__}: {exc}"
            ctx.log(f"dọn {name} lỗi: {exc}")

    # ----------------------------------------------------------------- streaming

    def stream_commands(self, ctx: RunContext, sim_dir: Path, topics: Mapping[str, str]) -> tuple[list[str], list[str]]:
        p = ctx.spec.params
        watermark_s = p["watermark_s"] or 120  # chế độ none bỏ qua watermark; job vẫn cần giá trị > 0
        trigger = "asap" if p["trigger_s"] is None else f"{p['trigger_s']} seconds"
        job = [sys.executable, "-m", "ai_math_tutor.experiments.stream_job", "--redis-db", str(self.opts.redis_db),
               "--work-dir", str(self.opts.work_root), "--",
               "--run-id", ctx.run_id, "--mode", p["mode"], "--watermark", f"{watermark_s}s",
               "--dedup", "on" if p["dedup"] else "off", "--trigger", trigger, "--learner-model", p["learner_model"],
               "--input-topic", topics["events"], "--dead-letter-topic", topics["dead"],
               "--alerts-topic", topics["alerts"], "--shuffle-partitions", str(p["kafka_partitions"]),
               "--run-seconds", str(int(self.opts.startup_timeout_s + ctx.timeout_s + self.opts.stop_timeout_s))]
        if self.opts.redis_key_prefix:
            job += ["--redis-key-prefix", self.opts.redis_key_prefix]
        timed = p["mode"] != "none"
        heartbeat_s = 2 * (p["watermark_s"] or 0) + 2 * (p["trigger_s"] or 0) + 30 if timed else 0
        replay = [sys.executable, "-m", "ai_math_tutor.experiments.replay_child", "--run-id", ctx.run_id,
                  "--events", str(sim_dir / "events.parquet"), "--topic", topics["events"],
                  "--processes", str(p["replay_processes"]), "--duration-s", str(p["duration_s"]),
                  "--watermark-s", str(p["watermark_s"] or 0), "--heartbeat-s", str(heartbeat_s),
                  "--progress", str(ctx.work / "progress.jsonl"), "--out-dir", str(ctx.work),
                  "--start-file", str(ctx.work / "replay_start_ms"), "--rate-check"]
        return job, replay

    def run_stream(self, spec: M.RunSpec, attempt: int) -> dict[str, Any]:
        from ai_math_tutor.ingestion.topics import TopicSpec, delete_topics, ensure_topics
        from ai_math_tutor.streaming.reconcile import RECONCILE_FILE, read_lake, read_progress, reconcile, write_report
        from ai_math_tutor.streaming.sinks import RedisKeys, load_roster

        ctx = self.new_context(spec, attempt)
        p = spec.params
        watermark_s = p["watermark_s"] or 0
        trigger_s = p["trigger_s"] or 0
        timed = p["mode"] != "none"
        ctx.timeout_s = p["duration_s"] + 2 * watermark_s + 300 + 2 * trigger_s
        drain_budget_s = 2 * watermark_s + 2 * trigger_s + 60
        topics = {"events": f"exp-{ctx.run_id}", "dead": f"exp-{ctx.run_id}-dl", "alerts": f"exp-{ctx.run_id}-al"}
        progress_path = ctx.work / "progress.jsonl"
        lake_root = self.cfg.paths.lake_dir(ctx.run_id)
        ctx.params.update({"topics": topics, "redis_db": self.opts.redis_db, "redis_key_prefix": self.opts.redis_key_prefix,
                           "shuffle_partitions": p["kafka_partitions"], "cutoff_ms": p["duration_s"] * 1000,
                           "dataset_key": M.dataset_key(p, spec.seed)})
        bootstrap = self.cfg.kafka.bootstrap_servers
        redis = None
        job = replay_proc = None
        monitor: ResourceMonitor | None = None
        replay: dict[str, Any] | None = None
        report: dict[str, Any] | None = None
        job_exit: int | None = None
        sim_dir: Path | None = None
        replay_finished_ms: int | None = None
        try:
            with ctx.phase("dataset"):
                sim_dir, cached = ensure_dataset(self.opts.data_root / "exp-sim", p, spec.seed)
                manifest = read_manifest(sim_dir)
                checksum = manifest["outputs"]["events.parquet"]["checksum"]
                ctx.data_version = f"events.parquet {checksum}"
                ctx.metrics["dataset"] = {"dir": str(sim_dir), "cached": cached, "counts": manifest.get("counts"),
                                          "rates": manifest.get("rates"),
                                          "stuck_episodes": (manifest.get("ground_truth") or {}).get("stuck_episodes")}
            with ctx.phase("reset"):
                ensure_topics(bootstrap, [TopicSpec(topics["events"], p["kafka_partitions"]),
                                          TopicSpec(topics["dead"], 1), TopicSpec(topics["alerts"], 1)],
                              recreate=list(topics.values()), timeout_s=120)
                redis = self.redis_client()
                ctx.metrics["redis_reset"] = reset_redis(redis, self.opts.redis_db, self.opts.redis_key_prefix)
                roster = json.loads((sim_dir / "roster.json").read_text(encoding="utf-8"))
                ctx.metrics["roster_learners"] = load_roster(redis, roster, RedisKeys(self.opts.redis_key_prefix))
                for stale in (self.cfg.paths.checkpoint_dir(ctx.run_id), lake_root):
                    if stale.exists():
                        raise RunnerError(f"{stale} đã tồn tại — run_id phải mới")
            job_cmd, replay_cmd = self.stream_commands(ctx, sim_dir, topics)
            with ctx.phase("job_start"):
                job = self.children.spawn(ctx.run_id, "spark_job", job_cmd, ctx.work / "job.log", self.child_env())
                wait_until(lambda: has_event(progress_path, "started"), self.opts.startup_timeout_s,
                           alive=lambda: job.poll() is None, what="query trạng thái khởi động",
                           check=self.guard.check)
            monitor = ResourceMonitor({"spark_job": job.pid}, ctx.work / "monitor.jsonl",
                                      containers=compose_containers())
            monitor.start()
            with ctx.phase("replay"):
                replay_proc = self.children.spawn(ctx.run_id, "replayer", replay_cmd, ctx.work / "replay.log",
                                                  self.child_env())
                monitor.add_role("replayer", replay_proc.pid)
                deadline = time.monotonic() + ctx.timeout_s + 60  # +60 s: đếm trước cửa sổ, khởi động shard
                while replay_proc.poll() is None:
                    self.guard.check()
                    if job.poll() is not None:
                        raise RunnerError(f"job thoát giữa lúc phát lại (mã {job.returncode})")
                    if time.monotonic() >= deadline:
                        ctx.timed_out = True
                        ctx.error = "bộ phát lại chưa xong khi hết giới hạn cứng"
                        break
                    time.sleep(1.0)
                replay_finished_ms = now_ms()
                replay_path = ctx.work / f"replay-{ctx.run_id}.json"
                if not ctx.timed_out:
                    if not replay_path.is_file():
                        raise RunnerError(f"bộ phát lại thoát mã {replay_proc.returncode} mà không có kết quả")
                    replay = json.loads(replay_path.read_text(encoding="utf-8"))
                    ctx.params["replay_start_ms"] = replay["replay_start_ms"]
                    ctx.params["replay_start"] = replay["replay_start"]
            if replay is not None:
                with ctx.phase("drain"):
                    end_of_log = int(replay["end_of_log_ms"])
                    finished = replay_finished_ms or now_ms()
                    try:
                        wait_until(lambda: X.drained(tail_record(progress_path), end_of_log_ms=end_of_log, timed=timed,
                                                     replay_finished_ms=finished),
                                   drain_budget_s, alive=lambda: job.poll() is None, what="drain", poll_s=0.5,
                                   check=self.guard.check)
                        ctx.metrics["drained"] = True
                    except TimeoutError:
                        ctx.metrics["drained"] = False
                        ctx.timed_out = True
                        ctx.error = f"không drain được trong {drain_budget_s} s sau khi phát lại xong"
            with ctx.phase("stop_job"):
                job_exit = self.children.stop(job, grace_s=self.opts.stop_timeout_s)
            ctx.metrics["resources"] = monitor.stop()
            if replay is not None and lake_root.is_dir():
                with ctx.phase("reconcile"):
                    report = reconcile(run_id=ctx.run_id, replay=replay, events_path=sim_dir / "events.parquet",
                                       ground_truth_path=sim_dir / "ground_truth.jsonl", lake_root=lake_root,
                                       model=p["learner_model"], redis_client=redis,
                                       keys=RedisKeys(self.opts.redis_key_prefix), progress_path=progress_path)
                    write_report(report, ctx.out / RECONCILE_FILE)
                with ctx.phase("metrics"):
                    self.stream_metrics(ctx, replay, report, read_progress(progress_path), read_lake(lake_root, "latency"))
        except RunnerInterrupted:
            ctx.interrupted = True
            ctx.error = "runner bị dừng"
        except Exception as exc:
            ctx.error = f"{type(exc).__name__}: {exc}"
            ctx.log(traceback.format_exc())
        with self.guard.deferred():
            self.children.stop_all()
            if monitor is not None and "resources" not in ctx.metrics:
                ctx.metrics["resources"] = monitor.stop()
            if job is not None and job_exit is None:
                job_exit = job.returncode
            ctx.metrics["job_exit_code"] = job_exit
            ctx.spark_conf = self.job_spark_conf(ctx.work / "job.json")
            if replay is not None:
                ctx.metrics.setdefault("rate", X.rate_validity(replay))
            status, reasons = X.decide_status(
                interrupted=ctx.interrupted, error=ctx.error, timed_out=ctx.timed_out,
                replay_status=None if replay is None else replay.get("status"),
                rate_ok=None if replay is None else bool(replay.get("rate_ok")),
                job_exit=job_exit)
            self.copy_artifacts(ctx, extra=[f"replay-{ctx.run_id}.json"])
            self.cleanup_step(ctx, "topics", lambda: delete_topics(bootstrap, list(topics.values()), timeout_s=120))
            self.cleanup_step(ctx, "checkpoint", lambda: self.archive_or_remove(
                self.cfg.paths.checkpoint_dir(ctx.run_id), "checkpoints"))
            self.cleanup_step(ctx, "lake", lambda: self.archive_or_remove(lake_root, "lake", keep=self.opts.keep_lake))
            if redis is not None:
                self.cleanup_step(ctx, "redis", lambda: reset_redis(redis, self.opts.redis_db, self.opts.redis_key_prefix))
                with contextlib.suppress(Exception):
                    redis.close()
            record = self.finish(ctx, status, reasons)
            self.cleanup_step(ctx, "work", lambda: self.archive_or_remove(ctx.work, "work"))
            self.children.release(ctx.run_id)
        if ctx.interrupted:
            raise RunnerInterrupted(self.guard.received or signal.SIGTERM)
        return record

    def stream_metrics(self, ctx: RunContext, replay: Mapping[str, Any], report: Mapping[str, Any],
                       progress: Sequence[Mapping[str, Any]], latency: Any) -> None:
        p = ctx.spec.params
        start_ms = int(replay["replay_start_ms"])
        window = replay.get("window") or {}
        end_ms = start_ms + int(window.get("last_offset_ms") or p["duration_s"] * 1000)
        measure_from = start_ms + p["warmup_s"] * 1000
        slo_ms = float(self.plan.slo.get("state_update_p95_ms", 5000))
        sustainable = self.plan.sustainable
        latency_metrics = X.latency_after_warmup(latency, replay_start_ms=start_ms, warmup_s=p["warmup_s"])
        p95 = latency_metrics["system_ms"]["p95"]
        ctx.metrics.update({
            "replay_start_ms": start_ms,
            "replay_start": replay.get("replay_start"),
            "end_of_log_ms": replay.get("end_of_log_ms"),
            "measure_window_ms": [measure_from, end_ms],
            "rate": X.rate_validity(replay),
            "replay": {k: replay.get(k) for k in ("scheduled", "sent", "delivered", "delivered_all", "stopped_early",
                                                  "buffer_full_events", "backpressure_s", "slip_ms", "heartbeat",
                                                  "wall_s")},
            "latency": latency_metrics,
            "slo": {"state_update_p95_ms": slo_ms, "system_p95_ms": p95,
                    "met": None if p95 is None else p95 <= slo_ms, "applies": p["mode"] == "eager"},
            "throughput": X.throughput(latency, progress, start_ms=measure_from, end_ms=end_ms),
            "processing_lag": X.lag_trend(progress, start_ms=measure_from, end_ms=end_ms,
                                          target_rate=float(p["rate_events_per_s"]),
                                          window_s=float(sustainable["window_s"]),
                                          slope_ratio=float(sustainable["lag_slope_ratio"])),
            "state_rows": X.state_rows(progress),
            "driver_rss": X.rss_trend(progress, start_ms=measure_from, end_ms=end_ms,
                                      window_s=float(sustainable["window_s"])),
            "latency_1m": X.latency_1m_windows(progress, start_ms=measure_from, end_ms=end_ms, slo_ms=slo_ms),
            "progress_batches": len(X.progress_batches(progress)),
            "reconcile": X.reconcile_summary(report),
        })

    @staticmethod
    def job_spark_conf(path: Path) -> dict[str, Any]:
        """Spark conf thật của job (job.json) + jar Kafka connector đã resolve."""
        try:
            job = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return {}
        conf = dict(job.get("spark_conf") or {})
        conf["resolved_jars"] = job.get("spark_jars")
        conf["kafka_partitions"] = job.get("kafka_partitions")
        return conf

    def copy_artifacts(self, ctx: RunContext, extra: Sequence[str] = ()) -> None:
        ctx.cleanup["artifacts"] = self.copy_work_artifacts(ctx.work, ctx.out, extra)

    @staticmethod
    def copy_work_artifacts(work: Path, out: Path, extra: Sequence[str] = ()) -> list[str]:
        """Kết quả nhỏ vào results/raw/<run_id>/: JSON nguyên bản, JSONL/log nén gzip."""
        copied = []
        names = [*extra, *(p.name for p in work.glob("replay-*.json"))]
        for name in dict.fromkeys(("job.json", *names)):
            src = work / name
            if src.is_file():
                shutil.copy2(src, out / name)
                copied.append(name)
        for name in ("progress.jsonl", "monitor.jsonl", "job.log", "replay.log", "runner.log", "child.log"):
            if gzip_to(work / name, out / f"{name}.gz"):
                copied.append(f"{name}.gz")
        return copied

    # ----------------------------------------------------------------- trần ingestion

    def run_ingest(self, spec: M.RunSpec, attempt: int) -> dict[str, Any]:
        from ai_math_tutor.ingestion.topics import TopicSpec, delete_topics, ensure_topics

        ctx = self.new_context(spec, attempt)
        p = spec.params
        ctx.timeout_s = p["duration_s"] + 600
        topic = f"exp-{ctx.run_id}"
        ctx.params.update({"topics": {"events": topic}, "dataset_key": M.dataset_key(p, spec.seed)})
        bootstrap = self.cfg.kafka.bootstrap_servers
        replay: dict[str, Any] | None = None
        monitor: ResourceMonitor | None = None
        try:
            with ctx.phase("dataset"):
                sim_dir, cached = ensure_dataset(self.opts.data_root / "exp-sim", p, spec.seed)
                manifest = read_manifest(sim_dir)
                ctx.data_version = f"events.parquet {manifest['outputs']['events.parquet']['checksum']}"
                ctx.metrics["dataset"] = {"dir": str(sim_dir), "cached": cached, "counts": manifest.get("counts")}
            with ctx.phase("reset"):
                ensure_topics(bootstrap, [TopicSpec(topic, p["kafka_partitions"])], recreate=[topic], timeout_s=120)
            cmd = [sys.executable, "-m", "ai_math_tutor.experiments.replay_child", "--run-id", ctx.run_id,
                   "--events", str(sim_dir / "events.parquet"), "--topic", topic,
                   "--processes", str(p["replay_processes"]), "--duration-s", str(p["duration_s"]),
                   "--heartbeat-s", "0", "--out-dir", str(ctx.work)]
            with ctx.phase("replay"):
                proc = self.children.spawn(ctx.run_id, "replayer", cmd, ctx.work / "replay.log", self.child_env())
                monitor = ResourceMonitor({"replayer": proc.pid}, ctx.work / "monitor.jsonl",
                                          containers=compose_containers())
                monitor.start()
                if self.wait_process(proc, ctx.timeout_s) is None:
                    ctx.timed_out = True
                    ctx.error = "bộ phát lại quá hạn"
                ctx.metrics["resources"] = monitor.stop()
                path = ctx.work / f"replay-{ctx.run_id}.json"
                if path.is_file():
                    replay = json.loads(path.read_text(encoding="utf-8"))
                elif not ctx.timed_out:
                    raise RunnerError(f"bộ phát lại thoát mã {proc.returncode} mà không có kết quả")
            if replay is not None:
                rate = X.rate_validity(replay)
                ctx.params["replay_start_ms"] = replay["replay_start_ms"]
                ctx.metrics.update({
                    "replay_start_ms": replay["replay_start_ms"], "rate": rate, "sustained": rate["ok"],
                    "replay": {k: replay.get(k) for k in ("scheduled", "sent", "delivered", "delivered_all",
                                                          "delivery_errors", "buffer_full_events", "backpressure_s",
                                                          "slip_ms", "client_errors", "wall_s", "partitions",
                                                          "processes")},
                })
        except RunnerInterrupted:
            ctx.interrupted = True
            ctx.error = "runner bị dừng"
        except Exception as exc:
            ctx.error = f"{type(exc).__name__}: {exc}"
            ctx.log(traceback.format_exc())
        with self.guard.deferred():
            self.children.stop_all()
            if monitor is not None and "resources" not in ctx.metrics:
                ctx.metrics["resources"] = monitor.stop()
            # Trần tốc độ là phép đo: tốc độ thực < mục tiêu là kết quả (sustained=false), không làm run hỏng.
            status, reasons = X.decide_status(interrupted=ctx.interrupted, error=ctx.error, timed_out=ctx.timed_out,
                                              replay_status=None if replay is None else replay.get("status"),
                                              rate_ok=None, job_exit=None, require_rate=False)
            self.copy_artifacts(ctx, extra=[f"replay-{ctx.run_id}.json"])
            self.cleanup_step(ctx, "topics", lambda: delete_topics(bootstrap, [topic], timeout_s=120))
            record = self.finish(ctx, status, reasons)
            self.cleanup_step(ctx, "work", lambda: self.archive_or_remove(ctx.work, "work"))
            self.children.release(ctx.run_id)
        if ctx.interrupted:
            raise RunnerInterrupted(self.guard.received or signal.SIGTERM)
        return record

    # ----------------------------------------------------------------- tiến trình tự ghi run record

    def _external_run(self, ctx: RunContext, role: str, cmd: Sequence[str], timeout_s: float) -> int | None:
        """Chạy một CLI tự ghi run record; trả mã thoát (None khi quá hạn hoặc bị dừng)."""
        proc = self.children.spawn(ctx.run_id, role, cmd, ctx.work / "child.log", self.child_env())
        code = self.wait_process(proc, timeout_s)
        if code is None:
            ctx.timed_out = True
            ctx.error = f"{role} quá hạn {timeout_s:.0f} s"
        return code

    def _external_finish(self, ctx: RunContext, code: int | None, records_path: Path, experiment: str) -> dict[str, Any]:
        """Bản ghi do CLI con ghi; thiếu (con chết/quá hạn/bị dừng) thì runner ghi thay với status tương ứng."""
        record = next((r for r in reversed(M.load_records(records_path)) if r.get("run_id") == ctx.run_id), None)
        if record is not None:
            return record
        if ctx.error is None and not ctx.interrupted:
            ctx.error = f"tiến trình con thoát mã {code} mà không ghi run record"
        status, reasons = X.decide_status(interrupted=ctx.interrupted, error=ctx.error, timed_out=ctx.timed_out,
                                          replay_status="n/a", rate_ok=None, job_exit=None, require_rate=False)
        return self.finish(ctx, status, reasons, experiment=experiment)

    def run_hint(self, spec: M.RunSpec, attempt: int) -> dict[str, Any]:
        """Exp E: một mức user của scripts/hint-load-test.py (fixture trạng thái xác định theo seed, Locust headless)."""
        if self.opts.results_dir.resolve() != self.cfg.paths.results.resolve():
            raise RunnerError("thí nghiệm hint ghi run record vào results/raw của cấu hình; không dùng --results-dir khác")
        ctx = self.new_context(spec, attempt)
        p = spec.params
        ctx.timeout_s = p["duration_s"] + 300
        cmd = [sys.executable, str(HINT_SCRIPT), "--users", str(p["concurrent_users"]),
               "--duration-s", str(p["duration_s"]), "--pacing-s", str(p["pacing_s"]),
               "--hint-generator", p["hint_generator"], "--seed", str(spec.seed), "--tag", spec.hint_tag(ctx.run_id),
               "--port", str(self.opts.hint_port)]
        code: int | None = None
        try:
            if not HINT_SCRIPT.is_file():
                raise RunnerError(f"thiếu {repo_relative(HINT_SCRIPT)}")
            with ctx.phase("hint_load"):
                code = self._external_run(ctx, "hint_load", cmd, ctx.timeout_s)
        except RunnerInterrupted:
            ctx.interrupted = True
            ctx.error = "runner bị dừng"
        except Exception as exc:
            ctx.error = f"{type(exc).__name__}: {exc}"
        with self.guard.deferred():
            self.children.stop_all()
            self.copy_artifacts(ctx)
            record = self._external_finish(ctx, code, self.records_path, "hint-api")
            if not any(ctx.out.iterdir()):
                ctx.out.rmdir()
            self.cleanup_step(ctx, "work", lambda: self.archive_or_remove(ctx.work, "work"))
            self.children.release(ctx.run_id)
        if ctx.interrupted:
            raise RunnerInterrupted(self.guard.received or signal.SIGTERM)
        return record

    def run_batch(self, spec: M.RunSpec, attempt: int, source: Mapping[str, Any]) -> dict[str, Any]:
        """Exp C: batch baseline trên cửa sổ log mà run streaming nguồn đã phát lại (dịch theo replay_start của nó)."""
        ctx = self.new_context(spec, attempt)
        ctx.timeout_s = BATCH_TIMEOUT_S
        src = spec.params["source"]
        code: int | None = None
        try:
            replay_start_ms = int((source.get("metrics") or {})["replay_start_ms"])
            with ctx.phase("window"):
                sim_dir, _ = ensure_dataset(self.opts.data_root / "exp-sim", src, spec.seed)
                window = materialize_window(sim_dir, ctx.work / "window", src["duration_s"] * 1000)
            harness = {"run_id": ctx.run_id, "source_run_id": source.get("run_id"), "config_key": spec.slug,
                       "groups": list(spec.groups), "replay_start_ms": replay_start_ms, "window": window,
                       "dataset": str(sim_dir), "attempt": attempt}
            (ctx.out / "harness.json").write_text(json.dumps(harness, ensure_ascii=False, indent=2) + "\n",
                                                  encoding="utf-8")
            cmd = [sys.executable, "-m", "ai_math_tutor.batch.run_batch_baseline", "--schedule", spec.params["schedule"],
                   "--impl", spec.params["impl"], "--events", str(ctx.work / "window" / "events.parquet"),
                   "--ground-truth", str(ctx.work / "window" / "ground_truth.jsonl"),
                   "--replay-start-ms", str(replay_start_ms), "--run-id", ctx.run_id,
                   "--results", str(self.opts.results_dir), "--timeout-s", str(int(BATCH_TIMEOUT_S))]
            with ctx.phase("batch"):
                code = self._external_run(ctx, "batch", cmd, BATCH_TIMEOUT_S + 300)
        except RunnerInterrupted:
            ctx.interrupted = True
            ctx.error = "runner bị dừng"
        except Exception as exc:
            ctx.error = f"{type(exc).__name__}: {exc}"
        with self.guard.deferred():
            self.children.stop_all()
            self.copy_artifacts(ctx)
            record = self._external_finish(ctx, code, self.records_path, f"batch-{spec.params['schedule']}")
            self.cleanup_step(ctx, "snapshots", lambda: self.archive_or_remove(
                self.opts.data_root / "batch" / ctx.run_id, "batch", keep=self.opts.keep_lake))
            self.cleanup_step(ctx, "work", lambda: self.archive_or_remove(ctx.work, "work"))
            self.children.release(ctx.run_id)
        if ctx.interrupted:
            raise RunnerInterrupted(self.guard.received or signal.SIGTERM)
        return record


# ---------------------------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------------------------


def format_plan(specs: Sequence[M.RunSpec], records: Sequence[Mapping[str, Any]], max_attempts: int) -> str:
    lines = [f"{'thí nghiệm':<26} {'loại':<6} {'seed':>4} {'trạng thái':<10} {'thử':>3} {'phút':>6}  cấu hình"]
    for spec in specs:
        state = M.spec_state(spec, records, max_attempts)
        lines.append(f"{spec.experiment:<26} {spec.kind:<6} {spec.seed:>4} {state.status:<10} {state.attempts:>3} "
                     f"{M.estimate_seconds(spec) / 60:>6.1f}  {spec.slug}"
                     + (f"  (cũng thuộc {', '.join(spec.groups[1:])})" if len(spec.groups) > 1 else ""))
    return "\n".join(lines)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--plan", type=Path, default=REPO_ROOT / "configs" / "experiments" / "plan-v1.0.yaml")
    parser.add_argument("--exp", default="all", help="A|B|C|D|E|ablations|stress|soak|ceiling|all, phân tách bằng dấu phẩy")
    parser.add_argument("--repeats", type=int, default=None, help="số seed mỗi cấu hình (khối có repeats riêng giữ nguyên)")
    parser.add_argument("--only", default=None, help="chỉ cấu hình có slug chứa chuỗi này")
    parser.add_argument("--dry-run", action="store_true", help="in kế hoạch, trạng thái resume và ước lượng giờ máy")
    parser.add_argument("--max-attempts", type=int, default=3, help="số lần thử tối đa (1 + 2 lần thử lại)")
    parser.add_argument("--redis-db", type=int, default=DEFAULT_REDIS_DB, help="DB Redis riêng cho thí nghiệm (≠ 0)")
    parser.add_argument("--redis-key-prefix", default="", help="tiền tố khóa (khi phải dùng chung DB)")
    parser.add_argument("--results-dir", type=Path, default=None, help="mặc định results/raw")
    parser.add_argument("--archive-dir", type=Path, default=None, help="chuyển lake/log thô sang đây (HDD) thay vì xóa")
    parser.add_argument("--keep-lake", action="store_true", help="giữ lake/<run_id> trên SSD (mặc định xóa sau đối soát)")
    parser.add_argument("--disk-threshold-gb", type=float, default=10.0)
    parser.add_argument("--hint-port", type=int, default=DEFAULT_HINT_PORT)
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        cfg = load_config()
        plan = M.load_plan(args.plan, args.repeats)
        labels = M.resolve_labels(plan, args.exp.split(","))
        specs = M.select_specs(plan, labels, args.only)
    except (M.PlanError, ConfigError) as exc:
        parser.error(str(exc))
    if args.max_attempts < 1:
        parser.error("--max-attempts phải ≥ 1")
    if args.redis_db < 0 or (args.redis_db == 0 and not args.redis_key_prefix):
        parser.error("--redis-db 0 là DB của stack dùng chung: chọn DB khác hoặc đặt --redis-key-prefix")
    if args.redis_key_prefix and not re.fullmatch(r"[A-Za-z0-9._:-]{1,64}", args.redis_key_prefix):
        parser.error("--redis-key-prefix chỉ gồm [A-Za-z0-9._:-], tối đa 64 ký tự")
    opts = RunnerOptions(plan_path=args.plan, results_dir=(args.results_dir or cfg.paths.results).resolve(),
                         data_root=cfg.paths.data_root, redis_db=args.redis_db, redis_key_prefix=args.redis_key_prefix,
                         max_attempts=args.max_attempts, archive_dir=args.archive_dir, keep_lake=args.keep_lake,
                         disk_threshold_gb=args.disk_threshold_gb, hint_port=args.hint_port)
    if any(s.kind == "hint" for s in specs) and opts.results_dir != cfg.paths.results.resolve():
        parser.error("thí nghiệm hint (scripts/hint-load-test.py) luôn ghi vào results/raw: bỏ --results-dir")
    records = M.load_records(opts.results_dir / "runs.jsonl")
    if args.dry_run:
        print(format_plan(specs, records, opts.max_attempts))
        print(json.dumps({"plan": str(args.plan), "labels": labels, "estimate": M.estimate_plan(specs)},
                         ensure_ascii=False, indent=2))
        return EXIT_OK
    opts.results_dir.mkdir(parents=True, exist_ok=True)
    guard = SignalGuard()
    try:
        with RunnerLock(opts.runner_dir / "runner.lock"):
            reaped = reap_stale(opts.runner_dir / "pids")
            if reaped:
                print(f"RUNNER đã dọn tiến trình mồ côi: {reaped}", file=sys.stderr, flush=True)
            guard.install()
            runner = Runner(opts, plan, cfg, guard)
            runner.recover_orphans(reaped)
            summary = runner.execute(specs)
    except LockError as exc:
        print(f"RUNNER {exc}", file=sys.stderr)
        return EXIT_LOCK
    except DiskLow as exc:
        print(f"RUNNER dừng: {exc}", file=sys.stderr)
        return EXIT_DISK
    except RunnerInterrupted:
        print("RUNNER đã dừng theo tín hiệu (run dở được ghi aborted, tiến trình con đã dừng)", file=sys.stderr)
        return EXIT_INTERRUPTED
    except RunnerError as exc:
        print(f"RUNNER lỗi: {exc}", file=sys.stderr)
        return EXIT_FAILED_RUNS
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return EXIT_OK if not summary["abandoned"] else EXIT_FAILED_RUNS


if __name__ == "__main__":
    sys.exit(main())
