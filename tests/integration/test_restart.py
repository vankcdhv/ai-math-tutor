"""Chịu lỗi checkpoint/restart và xử lý sự kiện muộn, chạy pipeline thật (Kafka + Spark + Redis của `make up`).

Restart **xác định**: topic được nạp sẵn (bộ phát lại thật, kể cả heartbeat drain) trước khi Spark
chạy, `maxOffsetsPerTrigger` cố định ⇒ ranh giới micro-batch chỉ phụ thuộc offset. Hai lần chạy:
1. không gián đoạn tới khi drain xong;
2. SIGKILL cả nhóm tiến trình (driver Python + JVM) giữa run, rồi chạy lại **cùng checkpoint**.
Mỗi lần chạy được so với oracle (chỉ sự kiện không bị loại, đúng thứ tự) chứ không so hai lần với
nhau: chế độ `ordered` phải khớp 100%, không sự kiện nào áp hai lần, mỗi episode đúng một alert_id
trong `alerts:stream`, mỗi batch đúng một tệp latency/applied.

Sự kiện muộn: hồ sơ trễ low/high × chế độ eager/ordered trên cùng topic, ghi số bị loại theo kết nối,
độ đúng trạng thái so oracle và hai loại độ trễ (minh chứng cho docs/reports/late-events.md).

Cách ly với các thành phần khác dùng chung stack: topic, run_id, checkpoint, lake riêng mỗi test; mọi
khóa Redis có tiền tố `it-<run_id>:` và chỉ khóa đó bị xóa. Job thật được điều khiển từ tiến trình con
chạy chính tệp này (`python tests/integration/test_restart.py child ...`) bằng các hàm công khai của
`ai_math_tutor.streaming.job` — CLI của job chưa có tùy chọn tiền tố khóa Redis.
Số đo của mỗi lần chạy ghi vào `$NHOM8_DATA_ROOT/it-evidence/`.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import signal
import subprocess
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pytest

from ai_math_tutor.config import load_config

RESTART_WATERMARK_S = 10
LATE_WATERMARK_S = 30
MAX_OFFSETS_PER_TRIGGER = 400
KILL_AFTER_BATCH = 5
CHILD_TIMEOUT_S = 600
SHUFFLE_PARTITIONS = 3
KAFKA_PARTITIONS = 3


# ---------------------------------------------------------------------------------------------
# Tiến trình con: chạy job tới khi drain xong (watermark qua cuối log, hết lag, batch không dữ liệu).
# ---------------------------------------------------------------------------------------------


def _child(argv: list[str]) -> int:
    from ai_math_tutor.streaming.job import (
        JobSettings,
        build_spark,
        detach_listener,
        job_config,
        spark_conf,
        start_streams,
        stop_streams,
    )
    from ai_math_tutor.streaming.listener import attach_listener
    from ai_math_tutor.streaming.sinks import make_foreach_batch

    parser = argparse.ArgumentParser()
    for name in ("--run-id", "--prefix", "--input-topic", "--dead-letter-topic", "--alerts-topic", "--mode",
                 "--model", "--progress-out"):
        parser.add_argument(name, required=True)
    for name in ("--watermark-ms", "--max-offsets"):
        parser.add_argument(name, type=int, required=True)
    # Cuối log: biết trước (topic nạp sẵn) hoặc đọc từ tệp do test ghi khi bộ phát lại xong (chạy đồng thời).
    end_of_log = parser.add_mutually_exclusive_group(required=True)
    end_of_log.add_argument("--end-of-log-ms", type=int)
    end_of_log.add_argument("--end-of-log-file", type=Path)
    args = parser.parse_args(argv)

    def end_of_log_ms() -> int | None:
        if args.end_of_log_ms is not None:
            return args.end_of_log_ms
        try:
            return int(args.end_of_log_file.read_text(encoding="utf-8").strip())
        except (OSError, ValueError):
            return None

    settings = JobSettings(run_id=args.run_id, mode=args.mode, watermark_ms=args.watermark_ms,
                           learner_model=args.model, starting_offsets="earliest",
                           max_offsets_per_trigger=args.max_offsets, input_topic=args.input_topic,
                           dead_letter_topic=args.dead_letter_topic, alerts_topic=args.alerts_topic,
                           shuffle_partitions=SHUFFLE_PARTITIONS)
    cfg = job_config(load_config(), settings)
    spark = build_spark(spark_conf(cfg, settings))
    streams = None
    sink = make_foreach_batch(cfg, args.run_id, mode=args.mode, model=args.model, key_prefix=args.prefix,
                              alerts_topic=args.alerts_topic)
    started = time.monotonic()
    listener = None
    try:
        listener = attach_listener(spark, cfg, args.run_id, key_prefix=args.prefix, out_path=Path(args.progress_out))
        streams = start_streams(spark, cfg, settings, sink)
        while True:
            for query in streams.queries:
                if query.exception() is not None:
                    print(json.dumps({"drained": False, "error": str(query.exception())}), flush=True)
                    return 2
            record = listener.last_record
            end = end_of_log_ms()
            if (end is not None and record and record["num_input_rows"] == 0 and record.get("lag")
                    and record["lag"]["total"] == 0 and record.get("watermark_ms") and record["watermark_ms"] > end):
                break
            if time.monotonic() - started > CHILD_TIMEOUT_S:
                print(json.dumps({"drained": False, "error": "timeout"}), flush=True)
                return 3
            time.sleep(0.5)
        stopped = stop_streams(streams)
        streams = None
        print(json.dumps({"drained": True, "batches": listener.records, "listener_errors": listener.errors,
                          "stopped_cleanly": stopped, "wall_s": round(time.monotonic() - started, 1)}), flush=True)
        return 0
    finally:
        if streams is not None:
            stop_streams(streams)
        if listener is not None:
            detach_listener(spark, listener)
        sink.close()
        spark.stop()


# ---------------------------------------------------------------------------------------------
# Phía test.
# ---------------------------------------------------------------------------------------------

pytestmark = pytest.mark.integration


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def redis_client(cfg):
    import redis

    try:
        client = redis.Redis.from_url(cfg.redis.url, socket_timeout=30)
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis không sẵn sàng ({cfg.redis.redacted_url}): {exc}")
    yield client
    client.close()


@pytest.fixture(scope="module")
def job_module():
    try:
        import ai_math_tutor.streaming.job as job
    except ImportError as exc:
        pytest.skip(f"ai_math_tutor.streaming.job chưa có: {exc}")
    return job


class Workspace:
    """Tài nguyên của một test (topic, run_id, tiền tố khóa, checkpoint, lake) và dọn sạch khi xong."""

    def __init__(self, cfg: Any, redis_client: Any, base: Path) -> None:
        self.cfg = cfg
        self.redis = redis_client
        self.base = base
        self.tag = uuid.uuid4().hex[:8]
        self.topics: list[str] = []
        self.run_ids: list[str] = []
        self.processes: list[subprocess.Popen[bytes]] = []

    def topic(self, kind: str, partitions: int) -> str:
        from ai_math_tutor.ingestion.topics import TopicError, TopicSpec, ensure_topics

        name = f"it-{kind}-{self.tag}"
        try:
            ensure_topics(self.cfg.kafka.bootstrap_servers, [TopicSpec(name, partitions)], timeout_s=60)
        except TopicError as exc:
            pytest.skip(f"Kafka không sẵn sàng: {exc}")
        self.topics.append(name)
        return name

    def run_id(self, label: str) -> str:
        run_id = f"it-{label}-{self.tag}"
        self.run_ids.append(run_id)
        return run_id

    @staticmethod
    def prefix(run_id: str) -> str:
        return f"{run_id}:"

    def cleanup(self) -> None:
        from ai_math_tutor.ingestion.topics import delete_topics

        for process in self.processes:
            if process.poll() is None:
                os.killpg(process.pid, signal.SIGKILL)
                process.wait(timeout=30)
        for run_id in self.run_ids:
            keys = list(self.redis.scan_iter(match=f"{self.prefix(run_id)}*", count=1000))
            for start in range(0, len(keys), 1000):
                self.redis.delete(*keys[start:start + 1000])
            shutil.rmtree(self.cfg.paths.checkpoint_dir(run_id), ignore_errors=True)
            shutil.rmtree(self.cfg.paths.lake_dir(run_id), ignore_errors=True)
        if self.topics:
            delete_topics(self.cfg.kafka.bootstrap_servers, self.topics)


@pytest.fixture
def workspace(cfg, redis_client, job_module, tmp_path):
    ws = Workspace(cfg, redis_client, tmp_path)
    yield ws
    ws.cleanup()


def _simulate(ws: Workspace, label: str, *, seed: int, late_profile: str, horizon_s: float) -> Path:
    from ai_math_tutor.simulator import SimulationConfig, run_simulation

    sim_dir = ws.base / f"sim-{label}"
    run_simulation(SimulationConfig(learners=80, horizon_s=horizon_s, seed=seed, out_dir=sim_dir,
                                    target_rate=150.0, late_profile=late_profile, duplicate_rate=0.05))
    return sim_dir


def _simulate_and_replay(ws: Workspace, label: str, *, events_topic: str, watermark_s: int, seed: int,
                         late_profile: str, horizon_s: float, cutoff_s: float) -> tuple[Path, dict[str, Any]]:
    sim_dir = _simulate(ws, label, seed=seed, late_profile=late_profile, horizon_s=horizon_s)
    return sim_dir, _replay(ws, label, sim_dir, events_topic=events_topic, watermark_s=watermark_s, cutoff_s=cutoff_s)


def _replay(ws: Workspace, label: str, sim_dir: Path, *, events_topic: str, watermark_s: int,
            cutoff_s: float) -> dict[str, Any]:
    """Bộ phát lại thật (thời gian thực, heartbeat drain 5 s); trả nội dung replay-<run>.json."""
    from ai_math_tutor.ingestion.producer import ProducerSettings
    from ai_math_tutor.ingestion.replay import ReplaySettings, run_replay, write_results

    manifest = json.loads((sim_dir / "manifest.json").read_text(encoding="utf-8"))
    settings = ReplaySettings(sim_dir / "events.parquet", events_topic,
                              ProducerSettings(ws.cfg.kafka.bootstrap_servers), processes=1, duration_s=cutoff_s,
                              watermark_s=watermark_s, heartbeat_s=5)
    result = run_replay(settings)
    path = write_results(result, results_dir=ws.base / "results", run_id=f"replay-{label}-{ws.tag}",
                         manifest=manifest, rate_check=False, run_record=False)
    replay = json.loads(path.read_text(encoding="utf-8"))
    assert replay["status"] == "success", replay["status"]
    return replay


def _spawn(ws: Workspace, run_id: str, *, mode: str, model: str, topics: dict[str, str], watermark_s: int,
           progress: Path, log: Path, end_of_log_ms: int | None = None,
           end_of_log_file: Path | None = None) -> subprocess.Popen[bytes]:
    end = (["--end-of-log-ms", str(end_of_log_ms)] if end_of_log_ms is not None
           else ["--end-of-log-file", str(end_of_log_file)])
    command = [sys.executable, str(Path(__file__).resolve()), "child", "--run-id", run_id,
               "--prefix", ws.prefix(run_id), "--input-topic", topics["events"],
               "--dead-letter-topic", topics["dead"], "--alerts-topic", topics["alerts"], "--mode", mode,
               "--model", model, "--progress-out", str(progress), "--watermark-ms", str(watermark_s * 1000),
               "--max-offsets", str(MAX_OFFSETS_PER_TRIGGER), *end]
    handle = log.open("ab")
    process = subprocess.Popen(command, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    handle.close()
    ws.processes.append(process)
    return process


def _child_summary(process: subprocess.Popen[bytes], log: Path) -> dict[str, Any]:
    code = process.wait(timeout=CHILD_TIMEOUT_S + 120)
    lines = [line for line in log.read_text(encoding="utf-8", errors="replace").splitlines() if line.startswith("{")]
    summary = json.loads(lines[-1]) if lines else {}
    assert code == 0 and summary.get("drained"), f"tiến trình con thoát {code}; xem {log}"
    return summary


def _progress_batches(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        return []
    rows = []
    for line in path.read_text(encoding="utf-8").splitlines():
        if line.strip():
            rows.append(json.loads(line))
    return rows


def _checkpoint_log(cfg: Any, run_id: str) -> dict[str, int | None]:
    root = cfg.paths.checkpoint_dir(run_id)

    def last(sub: str) -> int | None:
        ids = [int(p.name) for p in (root / sub).glob("[0-9]*") if p.name.isdigit()]
        return max(ids) if ids else None

    return {"last_offsets_batch": last("offsets"), "last_commit_batch": last("commits")}


def _lake_files(cfg: Any, run_id: str) -> dict[str, Any]:
    import pyarrow.parquet as pq

    out: dict[str, Any] = {}
    for kind in ("applied", "latency", "alerts"):
        dirs = sorted((cfg.paths.lake_dir(run_id) / kind).glob("batch=*"))
        files_per_dir = {d.name: sorted(p.name for p in d.iterdir()) for d in dirs}
        out[kind] = {"batches": len(dirs),
                     "dirs_with_other_than_one_file": [k for k, v in files_per_dir.items() if v != ["part-00000.parquet"]]}
    latency_ids = []
    for d in sorted((cfg.paths.lake_dir(run_id) / "latency").glob("batch=*")):
        latency_ids += pq.read_table(d / "part-00000.parquet", columns=["event_id"]).column("event_id").to_pylist()
    out["latency_rows"] = len(latency_ids)
    out["latency_unique_event_ids"] = len(set(latency_ids))
    return out


def _reconcile(ws: Workspace, run_id: str, *, model: str, sim_dir: Path, replay: dict[str, Any],
               progress: Path) -> dict[str, Any]:
    from ai_math_tutor.streaming.reconcile import reconcile
    from ai_math_tutor.streaming.sinks import RedisKeys

    return reconcile(run_id=run_id, replay=replay, events_path=sim_dir / "events.parquet",
                     ground_truth_path=sim_dir / "ground_truth.jsonl", lake_root=ws.cfg.paths.lake_dir(run_id),
                     model=model, redis_client=ws.redis, keys=RedisKeys(ws.prefix(run_id)), progress_path=progress)


def _summary(report: dict[str, Any]) -> dict[str, Any]:
    dropped = report["dropped"]
    alerts = report["alerts"]
    return {
        "deliveries": dropped["deliveries"], "unique_events": dropped["unique_events"],
        "injected_duplicates": dropped["injected_duplicates"], "applied": dropped["applied_unique"],
        "dropped": dropped["total"], "dropped_by_connection": dropped["by_connection"],
        "late_by_connection": dropped["late_events_by_connection"],
        "spark_dropped_by_watermark_rows": dropped["spark_dropped_by_watermark_rows"],
        "spark_dropped_duplicate_rows": dropped["spark_dropped_duplicate_rows"],
        "state_vs_oracle": {k: report["state_vs_oracle"][k] for k in
                            ("pairs_expected", "matched", "accuracy", "mismatch_by_field", "missing_in_redis",
                             "extra_in_redis")},
        "state_vs_latent": report["state_vs_latent"],
        "alerts": {"stream_entries": alerts["stream_entries"], "unique_alert_ids": alerts["unique_alert_ids"],
                   "duplicate_stream_entries": alerts["duplicate_stream_entries"], "by_type": alerts["by_type"],
                   "vs_oracle": alerts["vs_oracle"], "vs_ground_truth": alerts["vs_ground_truth"]},
        "latency": report["latency"],
        "checks": report["checks"],
    }


def _write_evidence(cfg: Any, name: str, payload: dict[str, Any]) -> Path:
    out = cfg.paths.data_root / "it-evidence"
    out.mkdir(parents=True, exist_ok=True)
    path = out / f"{name}-{datetime.now(timezone.utc):%Y%m%dT%H%M%SZ}.json"
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
    print(f"\nminh chứng: {path}")
    return path


def _assert_correct_against_oracle(report: dict[str, Any], lake: dict[str, Any]) -> None:
    state = report["state_vs_oracle"]
    assert state["accuracy"] == 1.0 and state["missing_in_redis"] == 0 and state["extra_in_redis"] == 0, state
    assert all(report["checks"].values()), report["checks"]
    assert report["alerts"]["vs_oracle"]["missing"] == 0 and report["alerts"]["vs_oracle"]["extra"] == 0
    for kind in ("applied", "latency", "alerts"):
        assert lake[kind]["dirs_with_other_than_one_file"] == [], (kind, lake[kind])
    assert lake["latency_rows"] == lake["latency_unique_event_ids"] == report["dropped"]["applied_unique"]


def test_restart_from_checkpoint_matches_oracle(workspace):
    ws = workspace
    topics = {"events": ws.topic("restart-events", KAFKA_PARTITIONS), "dead": ws.topic("restart-dead", 1),
              "alerts": ws.topic("restart-alerts", 1)}
    sim_dir, replay = _simulate_and_replay(ws, "restart", events_topic=topics["events"],
                                           watermark_s=RESTART_WATERMARK_S, seed=31, late_profile="high",
                                           horizon_s=40, cutoff_s=45)
    end_of_log = int(replay["end_of_log_ms"])
    mode, model = "ordered", "bkt"

    # Lần 1: không gián đoạn.
    clean_id = ws.run_id("clean")
    clean_progress, clean_log = ws.base / "clean-progress.jsonl", ws.base / "clean.log"
    clean_child = _child_summary(_spawn(ws, clean_id, mode=mode, model=model, topics=topics,
                                        watermark_s=RESTART_WATERMARK_S, end_of_log_ms=end_of_log,
                                        progress=clean_progress, log=clean_log), clean_log)

    # Lần 2: SIGKILL giữa run, rồi chạy lại cùng checkpoint.
    crash_id = ws.run_id("crash")
    crash_progress, crash_log = ws.base / "crash-progress.jsonl", ws.base / "crash.log"
    process = _spawn(ws, crash_id, mode=mode, model=model, topics=topics, watermark_s=RESTART_WATERMARK_S,
                     end_of_log_ms=end_of_log, progress=crash_progress, log=crash_log)
    deadline = time.monotonic() + CHILD_TIMEOUT_S
    while not any(r.get("batch_id", -1) >= KILL_AFTER_BATCH for r in _progress_batches(crash_progress)):
        assert process.poll() is None, f"tiến trình con kết thúc trước khi bị giết; xem {crash_log}"
        assert time.monotonic() < deadline, "quá hạn chờ batch để giết"
        time.sleep(0.1)
    time.sleep(0.3)  # rơi vào giữa batch kế tiếp (trigger ASAP, còn dữ liệu)
    killed_at = datetime.now(timezone.utc).isoformat()
    os.killpg(process.pid, signal.SIGKILL)
    process.wait(timeout=60)
    after_kill = _checkpoint_log(ws.cfg, crash_id)
    batches_before_kill = [r["batch_id"] for r in _progress_batches(crash_progress) if r.get("event") == "progress"]
    restart_child = _child_summary(_spawn(ws, crash_id, mode=mode, model=model, topics=topics,
                                          watermark_s=RESTART_WATERMARK_S, end_of_log_ms=end_of_log,
                                          progress=crash_progress, log=crash_log), crash_log)
    crash_records = _progress_batches(crash_progress)
    sessions = [r for r in crash_records if r.get("event") == "started"]
    batches_after = [r["batch_id"] for r in crash_records if r.get("event") == "progress"][len(batches_before_kill):]

    reports = {}
    for run_id, progress in ((clean_id, clean_progress), (crash_id, crash_progress)):
        report = _reconcile(ws, run_id, model=model, sim_dir=sim_dir, replay=replay, progress=progress)
        lake = _lake_files(ws.cfg, run_id)
        reports[run_id] = {"reconcile": _summary(report), "lake": lake}
        _assert_correct_against_oracle(report, lake)

    evidence = {
        "mode": mode, "model": model, "watermark_s": RESTART_WATERMARK_S,
        "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER, "kafka_partitions": KAFKA_PARTITIONS,
        "shuffle_partitions": SHUFFLE_PARTITIONS, "replay": {k: replay[k] for k in
                                                             ("replay_start_ms", "scheduled", "sent", "end_of_log_ms",
                                                              "window", "heartbeat")},
        "clean": {"run_id": clean_id, "child": clean_child, **reports[clean_id]},
        "crash": {"run_id": crash_id, "killed_at": killed_at, "kill_after_batch": KILL_AFTER_BATCH,
                  "batches_before_kill": batches_before_kill, "checkpoint_after_kill": after_kill,
                  "batches_after_restart": batches_after, "sessions": len(sessions), "restart_child": restart_child,
                  **reports[crash_id]},
    }
    _write_evidence(ws.cfg, "checkpoint-restart", evidence)
    assert len(sessions) == 2
    # Batch đầu sau restart là batch chưa commit (chạy lại) hoặc batch kế tiếp batch đã commit.
    assert batches_after and batches_after[0] == (after_kill["last_commit_batch"] or 0) + 1

    # Chạy tiếp cùng checkpoint với chế độ khác bị từ chối trước khi query khởi động; checkpoint giữ nguyên.
    settings_file = ws.cfg.paths.checkpoint_dir(crash_id) / "job-settings.json"
    recorded = json.loads(settings_file.read_text(encoding="utf-8"))
    assert recorded["settings"]["mode"] == mode and recorded["settings"]["learner_model"] == model
    commits_before = _checkpoint_log(ws.cfg, crash_id)
    mismatch_log = ws.base / "mismatch.log"
    mismatch = _spawn(ws, crash_id, mode="eager", model=model, topics=topics, watermark_s=RESTART_WATERMARK_S,
                      end_of_log_ms=end_of_log, progress=ws.base / "mismatch-progress.jsonl", log=mismatch_log)
    assert mismatch.wait(timeout=300) != 0
    assert "không chạy tiếp checkpoint" in mismatch_log.read_text(encoding="utf-8", errors="replace")
    assert _checkpoint_log(ws.cfg, crash_id) == commits_before
    assert json.loads(settings_file.read_text(encoding="utf-8")) == recorded


def _wait_started(paths: list[Path], processes: list[subprocess.Popen[bytes]], timeout_s: float = 300) -> None:
    """Chờ mọi job đã khởi động query trạng thái (dòng `started` trong progress.jsonl)."""
    deadline = time.monotonic() + timeout_s
    while not all(any(r.get("event") == "started" for r in _progress_batches(p)) for p in paths):
        assert all(p.poll() is None for p in processes), "một job thoát trước khi khởi động xong"
        assert time.monotonic() < deadline, "quá hạn chờ job khởi động"
        time.sleep(0.5)


def test_late_events_eager_vs_ordered(workspace):
    """Job chạy **đồng thời** với bộ phát lại thời gian thực (độ trễ đo được là độ trễ thật, không phải
    tồn đọng của topic nạp sẵn); eager và ordered đọc cùng topic nên thấy cùng thứ tự đến."""
    ws = workspace
    evidence: dict[str, Any] = {"watermark_s": LATE_WATERMARK_S, "max_offsets_per_trigger": MAX_OFFSETS_PER_TRIGGER,
                                "concurrent_jobs": 2, "runs": {}}
    for profile, seed in (("low", 41), ("high", 41)):
        topics = {"events": ws.topic(f"late-{profile}-events", KAFKA_PARTITIONS),
                  "dead": ws.topic(f"late-{profile}-dead", 1), "alerts": ws.topic(f"late-{profile}-alerts", 1)}
        sim_dir = _simulate(ws, f"late-{profile}", seed=seed, late_profile=profile, horizon_s=60)
        end_file = ws.base / f"end-of-log-{profile}.txt"
        runs = {}
        for mode in ("eager", "ordered"):
            run_id = ws.run_id(f"late-{profile}-{mode}")
            progress, log = ws.base / f"{run_id}-progress.jsonl", ws.base / f"{run_id}.log"
            process = _spawn(ws, run_id, mode=mode, model="rule", topics=topics, watermark_s=LATE_WATERMARK_S,
                             end_of_log_file=end_file, progress=progress, log=log)
            runs[mode] = (run_id, progress, log, process)
        _wait_started([r[1] for r in runs.values()], [r[3] for r in runs.values()])
        replay = _replay(ws, f"late-{profile}", sim_dir, events_topic=topics["events"], watermark_s=LATE_WATERMARK_S,
                         cutoff_s=75)
        end_file.write_text(str(int(replay["end_of_log_ms"])), encoding="utf-8")
        for mode, (run_id, progress, log, process) in runs.items():
            child = _child_summary(process, log)
            report = _reconcile(ws, run_id, model="rule", sim_dir=sim_dir, replay=replay, progress=progress)
            evidence["runs"][f"{profile}/{mode}"] = {"run_id": run_id, "child": child, "reconcile": _summary(report),
                                                     "lake": _lake_files(ws.cfg, run_id)}
            assert all(report["checks"].values()), report["checks"]
            if mode == "ordered":
                assert report["state_vs_oracle"]["accuracy"] == 1.0
    _write_evidence(ws.cfg, "late-events", evidence)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "child":
        sys.exit(_child(sys.argv[2:]))
    sys.exit("dùng: python tests/integration/test_restart.py child ...")
