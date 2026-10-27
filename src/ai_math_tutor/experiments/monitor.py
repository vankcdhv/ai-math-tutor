"""Giám sát tài nguyên trong một run: CPU/RAM cây tiến trình con (psutil) và container Kafka/Redis (`docker stats`).

`ResourceMonitor` là một luồng nền: mỗi `interval_s` đo mỗi vai trò (vd. `spark_job` = driver Python
+ JVM, `replayer` = tiến trình phát lại + các shard) bằng tổng `cpu_percent` (100% = một lõi) và tổng
RSS của cả cây; mỗi `docker_every_s` gọi `docker stats --no-stream` cho container của stack. Đỉnh và
trung bình giữ trong bộ nhớ; mẫu được ghi thưa (`write_every`) ra JSONL để vẽ chuỗi thời gian.

Đọc tiến độ: `tail_record` lấy dòng cuối của `progress.jsonl` (listener của job ghi mỗi micro-batch)
mà không đọc lại cả tệp — runner và bộ phát lại hỏi mỗi giây trong lúc drain.
"""

from __future__ import annotations

import json
import logging
import re
import subprocess
import threading
import time
from collections.abc import Callable, Mapping, Sequence
from pathlib import Path
from typing import Any

import psutil

LOGGER = logging.getLogger(__name__)

COMPOSE_PROJECT = "nhom8"
TAIL_BYTES = 256 * 1024
_SIZE_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*([KMGTP]?i?B)\s*$", re.IGNORECASE)
_UNITS = {"b": 1, "kb": 1000, "mb": 1000**2, "gb": 1000**3, "tb": 1000**4,
          "kib": 1024, "mib": 1024**2, "gib": 1024**3, "tib": 1024**4}


def tail_record(path: Path, event: str | None = "progress", tail_bytes: int = TAIL_BYTES) -> dict[str, Any] | None:
    """Bản ghi JSON cuối cùng (có `event` khớp, nếu cho) trong phần đuôi tệp JSONL; None nếu chưa có."""
    try:
        with Path(path).open("rb") as handle:
            handle.seek(0, 2)
            size = handle.tell()
            handle.seek(max(0, size - tail_bytes))
            chunk = handle.read()
    except OSError:
        return None
    for line in reversed(chunk.splitlines()):
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue  # dòng đang ghi dở hoặc dòng đầu bị cắt
        if isinstance(record, dict) and (event is None or record.get("event") == event):
            return record
    return None


def has_event(path: Path, event: str) -> bool:
    """Tệp JSONL có ít nhất một bản ghi `event` (vd. `started` của listener)."""
    try:
        with Path(path).open(encoding="utf-8") as handle:
            for line in handle:
                if f'"event": "{event}"' in line or f'"event":"{event}"' in line:
                    return True
    except OSError:
        return False
    return False


def parse_size(text: str) -> int | None:
    """"123.4MiB" → byte (docker stats dùng cả đơn vị thập phân và nhị phân)."""
    match = _SIZE_RE.match(text or "")
    if not match:
        return None
    return int(float(match.group(1)) * _UNITS[match.group(2).lower()])


def parse_docker_stats(line: str) -> dict[str, Any] | None:
    """Một dòng `docker stats --format '{{json .}}'` → {name, cpu_percent, mem_bytes}."""
    try:
        row = json.loads(line)
    except json.JSONDecodeError:
        return None
    if not isinstance(row, Mapping) or "Name" not in row:
        return None
    cpu_text = str(row.get("CPUPerc", "")).strip().rstrip("%")
    try:
        cpu = float(cpu_text)
    except ValueError:
        cpu = None
    usage = str(row.get("MemUsage", "")).split("/")[0]
    return {"name": str(row["Name"]), "cpu_percent": cpu, "mem_bytes": parse_size(usage)}


def compose_containers(project: str = COMPOSE_PROJECT) -> list[str]:
    try:
        out = subprocess.run(["docker", "ps", "--filter", f"label=com.docker.compose.project={project}",
                              "--format", "{{.Names}}"], capture_output=True, text=True, timeout=15, check=True)
    except (OSError, subprocess.SubprocessError):
        return []
    return sorted(name for name in out.stdout.split() if name)


def docker_stats(names: Sequence[str]) -> list[dict[str, Any]]:
    if not names:
        return []
    try:
        out = subprocess.run(["docker", "stats", "--no-stream", "--format", "{{json .}}", *names],
                             capture_output=True, text=True, timeout=30, check=True)
    except (OSError, subprocess.SubprocessError):
        return []
    rows = [parse_docker_stats(line) for line in out.stdout.splitlines() if line.strip()]
    return [row for row in rows if row is not None]


class _Series:
    """Đỉnh / trung bình / số mẫu của một đại lượng."""

    def __init__(self) -> None:
        self.n = 0
        self.total = 0.0
        self.peak: float | None = None

    def add(self, value: float | None) -> None:
        if value is None:
            return
        self.n += 1
        self.total += value
        self.peak = value if self.peak is None else max(self.peak, value)

    def summary(self) -> dict[str, Any]:
        return {"samples": self.n, "peak": self.peak, "mean": self.total / self.n if self.n else None}


class ProcessTree:
    """CPU% và RSS tổng của một tiến trình gốc và mọi hậu duệ (giữ đối tượng psutil để cpu_percent có mốc)."""

    def __init__(self, pid: int) -> None:
        self.pid = pid
        self._procs: dict[int, psutil.Process] = {}

    def sample(self) -> tuple[float, int] | None:
        try:
            root = psutil.Process(self.pid)
            members = [root, *root.children(recursive=True)]
        except psutil.Error:
            return None
        cpu = 0.0
        rss = 0
        alive: dict[int, psutil.Process] = {}
        for proc in members:
            cached = self._procs.get(proc.pid, proc)
            try:
                cpu += cached.cpu_percent(None)  # lần đầu của tiến trình mới trả 0 (chưa có mốc)
                rss += cached.memory_info().rss
            except psutil.Error:
                continue
            alive[proc.pid] = cached
        self._procs = alive
        return cpu, rss


class ResourceMonitor(threading.Thread):
    def __init__(
        self,
        roles: Mapping[str, int],
        out_path: Path | None = None,
        *,
        interval_s: float = 1.0,
        write_every: int = 5,
        containers: Sequence[str] | None = None,
        docker_every_s: float = 5.0,
        docker_fn: Callable[[Sequence[str]], list[dict[str, Any]]] = docker_stats,
    ) -> None:
        super().__init__(name="experiment-monitor", daemon=True)
        self.trees = {role: ProcessTree(pid) for role, pid in roles.items()}
        self.out_path = Path(out_path) if out_path is not None else None
        self.interval_s = interval_s
        self.write_every = max(1, write_every)
        self.containers = list(containers) if containers is not None else compose_containers()
        self.docker_every_s = docker_every_s
        self._docker_fn = docker_fn
        self._stop_event = threading.Event()
        self.cpu = {role: _Series() for role in self.trees}
        self.rss = {role: _Series() for role in self.trees}
        self.docker_cpu: dict[str, _Series] = {}
        self.docker_mem: dict[str, _Series] = {}
        self.errors = 0
        self._last_docker: dict[str, Any] = {}

    def add_role(self, role: str, pid: int) -> None:
        """Thêm vai trò khi tiến trình khởi động muộn hơn monitor (vd. bộ phát lại sau job)."""
        self.trees[role] = ProcessTree(pid)
        self.cpu.setdefault(role, _Series())
        self.rss.setdefault(role, _Series())

    def _write(self, record: Mapping[str, Any]) -> None:
        if self.out_path is None:
            return
        with self.out_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")

    def run(self) -> None:
        tick = 0
        next_docker = 0.0
        while not self._stop_event.is_set():
            try:
                now_ms = time.time_ns() // 1_000_000
                record: dict[str, Any] = {"t_ms": now_ms, "processes": {}}
                for role, tree in list(self.trees.items()):
                    sample = tree.sample()
                    if sample is None:
                        continue
                    cpu, rss = sample
                    self.cpu[role].add(cpu)
                    self.rss[role].add(float(rss))
                    record["processes"][role] = {"cpu_percent": round(cpu, 1), "rss_bytes": rss}
                if self.containers and time.monotonic() >= next_docker:
                    next_docker = time.monotonic() + self.docker_every_s
                    for row in self._docker_fn(self.containers):
                        self.docker_cpu.setdefault(row["name"], _Series()).add(row["cpu_percent"])
                        mem = row["mem_bytes"]
                        self.docker_mem.setdefault(row["name"], _Series()).add(None if mem is None else float(mem))
                        self._last_docker[row["name"]] = row
                    record["docker"] = dict(self._last_docker)
                if tick % self.write_every == 0 or "docker" in record:
                    self._write(record)
            except Exception:  # monitor không được làm hỏng run; chỉ mất một mẫu
                self.errors += 1
                LOGGER.warning("monitor: lỗi khi lấy mẫu", exc_info=True)
            tick += 1
            self._stop_event.wait(self.interval_s)

    def stop(self) -> dict[str, Any]:
        self._stop_event.set()
        if self.is_alive():
            self.join(timeout=60)
        return self.summary()

    def summary(self) -> dict[str, Any]:
        processes = {role: {"cpu_percent": self.cpu[role].summary(), "rss_bytes": self.rss[role].summary()}
                     for role in self.trees}
        docker = {name: {"cpu_percent": self.docker_cpu[name].summary(),
                         "mem_bytes": self.docker_mem.get(name, _Series()).summary()}
                  for name in sorted(self.docker_cpu)}
        return {"interval_s": self.interval_s, "processes": processes, "docker": docker, "errors": self.errors}
