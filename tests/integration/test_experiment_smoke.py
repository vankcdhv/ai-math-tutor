"""Runner thí nghiệm chạy trọn một run streaming nhỏ (configs/experiments/smoke.yaml) trên stack thật.

Kiểm: một run record `success` đủ trường Phụ lục B (+ runtime_s, spark_conf, replay_start, môi trường),
reconcile.json cạnh run record, mọi kiểm tra đối soát đạt, drain xong; sau run không còn tiến trình
con (theo nhóm tiến trình ghi trong pidfile), topic `exp-<run_id>*`, khóa Redis trong DB thí nghiệm,
checkpoint/lake/thư mục làm việc của run. Kết quả ghi vào thư mục tạm (không chạm results/raw).
Run lại lần hai với cùng thư mục kết quả phải bỏ qua spec đã `success` (resume).
"""

from __future__ import annotations

import json
import os
import subprocess
import sys
import time
from dataclasses import replace
from pathlib import Path

import pytest

from ai_math_tutor.config import REPO_ROOT, load_config
from ai_math_tutor.experiments import reaper as R
from ai_math_tutor.experiments.runner import DEFAULT_REDIS_DB

pytestmark = pytest.mark.integration

SMOKE = REPO_ROOT / "configs" / "experiments" / "smoke.yaml"
RUN_TIMEOUT_S = 900
APPENDIX_B = ("run_id", "experiment", "timestamp", "git", "data_version", "machine", "software", "parameters",
              "seed", "timeout_s", "metrics", "status", "log_path", "runtime_s", "spark_conf", "java", "docker_images")


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def redis_db(cfg):
    import redis

    try:
        client = redis.Redis.from_url(replace(cfg, redis=replace(cfg.redis, db=DEFAULT_REDIS_DB)).redis.url,
                                      socket_timeout=10)
        client.ping()
    except Exception as exc:
        pytest.skip(f"Redis không sẵn sàng: {exc}")
    yield client
    client.close()


def _topics(cfg) -> list[str]:
    from ai_math_tutor.ingestion.topics import TopicError, admin_client, describe_topics

    try:
        return sorted(describe_topics(admin_client(cfg.kafka.bootstrap_servers)))
    except TopicError as exc:
        pytest.skip(f"Kafka không sẵn sàng: {exc}")


def _run_runner(results: Path, pid_dir: Path, seen: dict[int, str]) -> int | None:
    """Chạy runner (tiến trình con), ghi lại pgid của mọi tiến trình con nó khai báo trong pidfile."""
    log = results / "runner.log"
    with log.open("ab") as handle:
        proc = subprocess.Popen([sys.executable, "-m", "ai_math_tutor.experiments.runner", "--plan", str(SMOKE),
                                 "--exp", "all", "--results-dir", str(results), "--max-attempts", "1"],
                                cwd=REPO_ROOT, stdout=handle, stderr=subprocess.STDOUT, start_new_session=True)
    deadline = time.monotonic() + RUN_TIMEOUT_S
    try:
        while proc.poll() is None:
            for path in pid_dir.glob("*.jsonl"):
                for entry in R.read_entries(path):
                    if entry.role != "runner":
                        seen[entry.pgid] = entry.role
            assert time.monotonic() < deadline, f"runner quá hạn; xem {log}"
            time.sleep(0.5)
    finally:
        if proc.poll() is None:
            R.kill_group(proc.pid, grace_s=30)
    return proc.returncode


def test_smoke_run_end_to_end_then_resume(cfg, redis_db, tmp_path_factory):
    if redis_db.dbsize():
        pytest.skip(f"DB {DEFAULT_REDIS_DB} đang có dữ liệu (một runner khác đang chạy?)")
    results = Path(cfg.paths.data_root) / "it-experiment" / tmp_path_factory.mktemp("smoke").name
    results.mkdir(parents=True, exist_ok=True)
    pid_dir = Path(cfg.paths.data_root) / "exp-runner" / "pids"
    seen: dict[int, str] = {}
    try:
        code = _run_runner(results, pid_dir, seen)
        log = (results / "runner.log").read_text(encoding="utf-8", errors="replace")
        if "runner khác đang chạy" in log:
            pytest.skip("runner khác đang giữ khóa")
        assert code == 0, log[-4000:]
        records = [json.loads(line) for line in (results / "runs.jsonl").read_text(encoding="utf-8").splitlines()]
        assert len(records) == 1
        record = records[0]
        assert record["status"] == "success", record["metrics"].get("invalid_reasons")
        assert all(key in record for key in APPENDIX_B) and record["environment"]["cpu_model"]
        assert record["parameters"]["replay_start_ms"] == record["metrics"]["replay_start_ms"]
        assert record["spark_conf"]["spark.sql.shuffle.partitions"] == "3"
        assert record["spark_conf"]["resolved_jars"]
        metrics = record["metrics"]
        assert metrics["drained"] is True and metrics["rate"]["ok"] is True
        assert metrics["stack_at_start"]["healthy"] is True
        assert metrics["latency"]["system_ms"]["n"] > 0 and metrics["latency"]["events_excluded_warmup"] > 0
        assert all(metrics["reconcile"]["checks"].values()), metrics["reconcile"]["checks"]
        assert metrics["reconcile"]["dropped"]["applied_unique"] == metrics["reconcile"]["dropped"]["unique_events"]
        assert {"spark_job", "replayer"} <= set(metrics["resources"]["processes"])
        run_dir = results / record["run_id"]
        reconcile = json.loads((run_dir / "reconcile.json").read_text(encoding="utf-8"))
        assert reconcile["run_id"] == record["run_id"] and reconcile["status"] == "ok"
        assert {"job.json", "progress.jsonl.gz", "monitor.jsonl.gz"} <= {p.name for p in run_dir.iterdir()}

        # Không còn gì của run.
        assert set(seen.values()) == {"spark_job", "replayer"}
        assert all(R.group_members(pgid) == [] for pgid in seen), seen
        assert not [t for t in _topics(cfg) if record["run_id"] in t]
        assert redis_db.dbsize() == 0
        for path in (cfg.paths.checkpoint_dir(record["run_id"]), cfg.paths.lake_dir(record["run_id"]),
                     Path(cfg.paths.data_root) / "exp-work" / record["run_id"], pid_dir / f"{record['run_id']}.jsonl"):
            assert not path.exists(), path

        # Resume: spec đã success ⇒ không chạy lại, không thêm bản ghi.
        assert _run_runner(results, pid_dir, {}) == 0
        assert len((results / "runs.jsonl").read_text(encoding="utf-8").splitlines()) == 1
    finally:
        if os.environ.get("NHOM8_KEEP_IT_RESULTS") != "1":
            import shutil

            shutil.rmtree(results, ignore_errors=True)
