"""Theo dõi và dọn tiến trình con của runner: pidfile theo run, dọn nhóm tiến trình mồ côi, khóa runner.

Mỗi tiến trình con (job Spark, bộ phát lại, batch, kiểm tải gợi ý) được khởi động trong **session
riêng** (`start_new_session=True`) nên là trưởng một nhóm tiến trình (pgid = pid): giết cả nhóm là
giết luôn JVM của Spark và các shard của bộ phát lại. Runner ghi mỗi tiến trình con một dòng vào
`<pid_dir>/<run_id>.jsonl` (pid, pgid, thời điểm tạo, pid + thời điểm tạo của runner chủ).

Khi runner khởi động, `reap_stale` đọc mọi pidfile: nếu runner chủ đã chết (kể cả bị SIGKILL) thì
nhóm tiến trình còn sống bị dừng (SIGTERM, chờ, rồi SIGKILL). Chỉ giết khi pid còn đúng là tiến
trình đã ghi (so `create_time`) — pid bị hệ điều hành cấp lại cho tiến trình khác thì không động tới.

`RunnerLock` (flock) bảo đảm mỗi máy chỉ một runner chạy: hai runner cùng lúc sẽ FLUSHDB và xóa topic
của nhau.
"""

from __future__ import annotations

import fcntl
import json
import os
import signal
import subprocess
import time
from collections.abc import Callable
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Any

import psutil

CREATE_TIME_TOLERANCE_S = 0.05
DEFAULT_GRACE_S = 15.0


class LockError(RuntimeError):
    """Một runner khác đang giữ khóa."""


@dataclass(frozen=True)
class ProcessEntry:
    run_id: str
    role: str
    pid: int
    pgid: int
    create_time: float
    owner_pid: int
    owner_create_time: float
    command: str


def _create_time(pid: int) -> float | None:
    try:
        return psutil.Process(pid).create_time()
    except (psutil.NoSuchProcess, psutil.AccessDenied, psutil.ZombieProcess):
        return None


def is_same_process(pid: int, create_time: float) -> bool:
    """pid còn sống và vẫn là tiến trình đã ghi (không phải pid được cấp lại)."""
    actual = _create_time(pid)
    if actual is None:
        return False
    try:
        if psutil.Process(pid).status() == psutil.STATUS_ZOMBIE:
            return False
    except psutil.Error:
        return False
    return abs(actual - create_time) <= CREATE_TIME_TOLERANCE_S


def group_members(pgid: int) -> list[int]:
    """pid còn chạy (không tính zombie chờ cha thu) của nhóm tiến trình `pgid`."""
    members = []
    for process in psutil.process_iter(["pid", "status"]):
        try:
            if process.info["status"] == psutil.STATUS_ZOMBIE:
                continue
            if os.getpgid(process.info["pid"]) == pgid:
                members.append(process.info["pid"])
        except (ProcessLookupError, psutil.Error):
            continue
    return members


def _group_alive(pgid: int) -> bool:
    try:
        os.killpg(pgid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    # killpg(…, 0) vẫn thành công khi nhóm chỉ còn zombie: đếm thành viên thật.
    return bool(group_members(pgid))


def kill_group(pgid: int, grace_s: float = DEFAULT_GRACE_S, sleep: Callable[[float], None] = time.sleep) -> str:
    """SIGTERM cả nhóm, chờ tới `grace_s`, còn sống thì SIGKILL; trả `gone` | `terminated` | `killed`."""
    if not _group_alive(pgid):
        return "gone"
    try:
        os.killpg(pgid, signal.SIGTERM)
    except ProcessLookupError:
        return "gone"
    deadline = time.monotonic() + grace_s
    while time.monotonic() < deadline:
        if not _group_alive(pgid):
            return "terminated"
        sleep(0.2)
    try:
        os.killpg(pgid, signal.SIGKILL)
    except ProcessLookupError:
        return "terminated"
    for _ in range(50):
        if not _group_alive(pgid):
            break
        sleep(0.1)
    return "killed"


class PidRegistry:
    """Pidfile `<pid_dir>/<run_id>.jsonl` của các tiến trình con do runner hiện tại khởi động."""

    def __init__(self, pid_dir: Path) -> None:
        self.pid_dir = Path(pid_dir)
        self.pid_dir.mkdir(parents=True, exist_ok=True)
        self.owner_pid = os.getpid()
        self.owner_create_time = _create_time(self.owner_pid) or 0.0

    def path(self, run_id: str) -> Path:
        return self.pid_dir / f"{run_id}.jsonl"

    def open_run(self, run_id: str) -> ProcessEntry:
        """Đánh dấu run đang chạy (mục `runner` = chính runner) trước khi có tiến trình con nào: runner
        chết ở bất kỳ bước nào thì lần khởi động sau vẫn biết run này dở để dọn và ghi bản ghi."""
        return self._append(ProcessEntry(run_id=run_id, role="runner", pid=self.owner_pid, pgid=os.getpgid(0),
                                         create_time=self.owner_create_time, owner_pid=self.owner_pid,
                                         owner_create_time=self.owner_create_time, command="runner"))

    def register(self, run_id: str, role: str, process: subprocess.Popen[Any], command: str) -> ProcessEntry:
        return self._append(ProcessEntry(run_id=run_id, role=role, pid=process.pid, pgid=os.getpgid(process.pid),
                                         create_time=_create_time(process.pid) or time.time(), owner_pid=self.owner_pid,
                                         owner_create_time=self.owner_create_time, command=command[:300]))

    def _append(self, entry: ProcessEntry) -> ProcessEntry:
        with self.path(entry.run_id).open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(entry), ensure_ascii=False) + "\n")
            handle.flush()
            os.fsync(handle.fileno())
        return entry

    def forget(self, run_id: str) -> None:
        self.path(run_id).unlink(missing_ok=True)


def read_entries(path: Path) -> list[ProcessEntry]:
    entries = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        try:
            entries.append(ProcessEntry(**json.loads(line)))
        except (json.JSONDecodeError, TypeError):
            continue  # dòng ghi dở khi runner bị giết: bỏ qua
    return entries


def reap_stale(pid_dir: Path, grace_s: float = DEFAULT_GRACE_S) -> list[dict[str, Any]]:
    """Dừng nhóm tiến trình của các runner đã chết; xóa pidfile đã xử lý. Trả nhật ký từng mục."""
    actions: list[dict[str, Any]] = []
    directory = Path(pid_dir)
    if not directory.is_dir():
        return actions
    for path in sorted(directory.glob("*.jsonl")):
        entries = read_entries(path)
        if any(is_same_process(e.owner_pid, e.owner_create_time) for e in entries):
            continue  # runner chủ còn sống: không phải mồ côi
        for entry in entries:
            if entry.role == "runner":  # mục đánh dấu run; runner chủ đã chết (kiểm ở trên)
                actions.append({"run_id": entry.run_id, "role": entry.role, "pid": entry.pid, "pgid": entry.pgid,
                                "result": "owner_dead"})
                continue
            if is_same_process(entry.pid, entry.create_time):
                result = kill_group(entry.pgid, grace_s)
            else:
                result = "gone" if _create_time(entry.pid) is None else "pid_reused_skipped"
            actions.append({"run_id": entry.run_id, "role": entry.role, "pid": entry.pid, "pgid": entry.pgid,
                            "result": result})
        path.unlink(missing_ok=True)
    return actions


class RunnerLock:
    """Khóa độc quyền (flock) cho một runner trên máy; tự nhả khi tiến trình chết."""

    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self._handle: Any = None

    def acquire(self) -> RunnerLock:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        handle = self.path.open("a+", encoding="utf-8")
        try:
            fcntl.flock(handle.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as exc:
            handle.seek(0)
            holder = handle.read().strip()
            handle.close()
            raise LockError(f"runner khác đang chạy (khóa {self.path}: {holder or 'không rõ'})") from exc
        handle.seek(0)
        handle.truncate()
        handle.write(f"pid={os.getpid()}\n")
        handle.flush()
        self._handle = handle
        return self

    def release(self) -> None:
        if self._handle is not None:
            fcntl.flock(self._handle.fileno(), fcntl.LOCK_UN)
            self._handle.close()
            self._handle = None

    def __enter__(self) -> RunnerLock:
        return self.acquire()

    def __exit__(self, *_exc: Any) -> None:
        self.release()
