"""Dọn tiến trình: pidfile của runner đã chết được dọn (cả nhóm), runner còn sống/pid cấp lại không bị đụng,
SIGTERM bị bỏ qua thì SIGKILL, khóa runner độc quyền; giám sát tài nguyên và đọc đuôi progress.jsonl."""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import asdict
from pathlib import Path

import psutil
import pytest

from ai_math_tutor.experiments import monitor as MON
from ai_math_tutor.experiments import reaper as R

pytestmark = pytest.mark.skipif(sys.platform == "win32", reason="nhóm tiến trình POSIX")


def _spawn(code: str) -> subprocess.Popen[bytes]:
    return subprocess.Popen([sys.executable, "-c", code], start_new_session=True)


def _dead_owner() -> tuple[int, float]:
    """pid + create_time của một tiến trình đã kết thúc (runner chủ bị SIGKILL)."""
    proc = subprocess.Popen([sys.executable, "-c", "pass"])
    create = psutil.Process(proc.pid).create_time()
    proc.wait()
    return proc.pid, create


def _write(pid_dir: Path, entry: R.ProcessEntry) -> None:
    pid_dir.mkdir(parents=True, exist_ok=True)
    with (pid_dir / f"{entry.run_id}.jsonl").open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(asdict(entry)) + "\n")


def _entry(proc: subprocess.Popen[bytes], owner: tuple[int, float], run_id: str = "r1",
           create_time: float | None = None) -> R.ProcessEntry:
    return R.ProcessEntry(run_id=run_id, role="spark_job", pid=proc.pid, pgid=os.getpgid(proc.pid),
                          create_time=create_time if create_time is not None else psutil.Process(proc.pid).create_time(),
                          owner_pid=owner[0], owner_create_time=owner[1], command="test")


def test_stale_pidfile_of_dead_runner_kills_whole_group(tmp_path):
    # Trưởng nhóm có một tiến trình con cùng nhóm (như driver Python + JVM).
    leader = _spawn("import subprocess, sys, time; subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(120)']);"
                    " time.sleep(120)")
    time.sleep(0.5)
    members = R.group_members(leader.pid)
    assert len(members) == 2
    _write(tmp_path, _entry(leader, _dead_owner()))
    actions = R.reap_stale(tmp_path, grace_s=5)
    assert [a["result"] for a in actions] == ["terminated"]
    leader.wait(timeout=10)
    assert R.group_members(leader.pid) == []
    assert not list(tmp_path.glob("*.jsonl"))


def test_live_owner_and_reused_pid_are_left_alone(tmp_path):
    proc = _spawn("import time; time.sleep(120)")
    try:
        me = (os.getpid(), psutil.Process().create_time())
        _write(tmp_path, _entry(proc, me, run_id="live"))
        assert R.reap_stale(tmp_path) == []
        assert proc.poll() is None and (tmp_path / "live.jsonl").exists()
        (tmp_path / "live.jsonl").unlink()
        _write(tmp_path, _entry(proc, _dead_owner(), run_id="reused", create_time=1.0))
        actions = R.reap_stale(tmp_path)
        assert [a["result"] for a in actions] == ["pid_reused_skipped"]
        assert proc.poll() is None
    finally:
        R.kill_group(proc.pid, grace_s=2)
        proc.wait(timeout=10)


def test_sigterm_ignored_escalates_to_sigkill():
    proc = _spawn("import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(120)")
    time.sleep(0.5)
    assert R.kill_group(proc.pid, grace_s=1) == "killed"
    proc.wait(timeout=10)
    assert R.kill_group(proc.pid, grace_s=1) == "gone"


def test_runner_marker_entry_reports_dead_owner(tmp_path):
    registry = R.PidRegistry(tmp_path)
    marker = registry.open_run("run-x")
    assert marker.role == "runner" and marker.pid == os.getpid()
    # Runner hiện tại còn sống ⇒ không phải mồ côi.
    assert R.reap_stale(tmp_path) == []
    owner = _dead_owner()
    entries = R.read_entries(tmp_path / "run-x.jsonl")
    (tmp_path / "run-x.jsonl").write_text(json.dumps({**asdict(entries[0]), "owner_pid": owner[0], "pid": owner[0],
                                                      "owner_create_time": owner[1]}) + "\n", encoding="utf-8")
    assert R.reap_stale(tmp_path) == [{"run_id": "run-x", "role": "runner", "pid": owner[0],
                                       "pgid": entries[0].pgid, "result": "owner_dead"}]
    registry.forget("run-x")


def test_partial_pidfile_line_is_ignored(tmp_path):
    proc = _spawn("import time; time.sleep(120)")
    _write(tmp_path, _entry(proc, _dead_owner()))
    with (tmp_path / "r1.jsonl").open("a", encoding="utf-8") as handle:
        handle.write('{"run_id": "r1", "pid": ')  # runner chết giữa lúc ghi
    assert [a["result"] for a in R.reap_stale(tmp_path, grace_s=5)] == ["terminated"]
    proc.wait(timeout=10)


def test_runner_lock_is_exclusive(tmp_path):
    with R.RunnerLock(tmp_path / "runner.lock"):
        with pytest.raises(R.LockError, match="runner khác"):
            R.RunnerLock(tmp_path / "runner.lock").acquire()
    R.RunnerLock(tmp_path / "runner.lock").acquire().release()


def test_tail_record_skips_partial_line_and_filters_event(tmp_path):
    path = tmp_path / "progress.jsonl"
    lines = [json.dumps({"event": "started"}), json.dumps({"event": "progress", "batch_id": 1}),
             json.dumps({"event": "progress", "batch_id": 2}), '{"event": "progr']
    path.write_text("\n".join(lines), encoding="utf-8")
    assert MON.tail_record(path)["batch_id"] == 2
    assert MON.tail_record(path, event="started") == {"event": "started"}
    assert MON.has_event(path, "started") and not MON.has_event(path, "terminated")
    assert MON.tail_record(tmp_path / "missing.jsonl") is None


def test_docker_stats_parsing():
    row = MON.parse_docker_stats(json.dumps({"Name": "nhom8-kafka-1", "CPUPerc": "47.25%",
                                             "MemUsage": "668.2MiB / 23.44GiB"}))
    assert row == {"name": "nhom8-kafka-1", "cpu_percent": 47.25, "mem_bytes": int(668.2 * 1024**2)}
    assert MON.parse_size("1.5GB") == 1_500_000_000 and MON.parse_size("??") is None
    assert MON.parse_docker_stats("not json") is None


def test_resource_monitor_measures_process_tree(tmp_path):
    proc = _spawn("import time\nbuf = bytearray(64 * 1024 * 1024)\nend = time.time() + 3\nwhile time.time() < end: pass")
    try:
        monitor = MON.ResourceMonitor({"busy": proc.pid}, tmp_path / "monitor.jsonl", interval_s=0.2, write_every=2,
                                      containers=[])
        monitor.start()
        time.sleep(2.0)
        summary = monitor.stop()
    finally:
        proc.wait(timeout=10)
    busy = summary["processes"]["busy"]
    assert busy["rss_bytes"]["peak"] > 64 * 1024 * 1024
    assert busy["cpu_percent"]["peak"] > 50  # vòng bận ≈ 100% một lõi
    assert summary["errors"] == 0 and (tmp_path / "monitor.jsonl").read_text(encoding="utf-8").strip()
