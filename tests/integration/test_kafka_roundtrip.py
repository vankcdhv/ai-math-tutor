"""Kafka thật (make up): quản lý topic, phát lại → đọc lại → đối soát, và dừng broker 30 s giữa lúc phát lại.

Mỗi test dùng topic riêng (tên ngẫu nhiên) và xóa khi xong. Test dừng broker gọi
`docker compose stop/start kafka` trong thư mục repo — không chạy song song với run khác dùng Kafka.
"""

from __future__ import annotations

import json
import subprocess
import threading
import time
import uuid
from pathlib import Path

import pytest

from ai_math_tutor.config import REPO_ROOT, load_config
from ai_math_tutor.ingestion.producer import ProducerSettings
from ai_math_tutor.ingestion.replay import ReplaySettings, run_replay, write_results
from ai_math_tutor.ingestion.topics import (
    TopicError,
    TopicSpec,
    admin_client,
    delete_topics,
    describe_topics,
    ensure_topics,
)
from ai_math_tutor.ingestion.verify_topic import verify_topic
from ai_math_tutor.simulator import SimulationConfig, run_simulation

pytestmark = pytest.mark.integration

OUTAGE_AFTER_S = 20.0
OUTAGE_S = 30.0


@pytest.fixture(scope="module")
def bootstrap() -> str:
    servers = load_config().kafka.bootstrap_servers
    try:
        describe_topics(admin_client(servers), timeout_s=5)
    except TopicError as exc:
        pytest.skip(f"Kafka không chạy ở {servers} (make up): {exc}")
    return servers


@pytest.fixture
def topic_name(bootstrap):
    name = f"it-{uuid.uuid4().hex[:10]}"
    yield name
    delete_topics(bootstrap, [name])


def _simulate(out: Path, **params) -> dict:
    return run_simulation(SimulationConfig(out_dir=out, **params))


def test_topics_are_idempotent_and_recreate_changes_partitions(bootstrap, topic_name):
    assert ensure_topics(bootstrap, [TopicSpec(topic_name, 1)]) == {topic_name: "created"}
    assert ensure_topics(bootstrap, [TopicSpec(topic_name, 1)]) == {topic_name: "kept"}
    with pytest.raises(TopicError, match="tạo lại"):
        ensure_topics(bootstrap, [TopicSpec(topic_name, 6)])
    assert ensure_topics(bootstrap, [TopicSpec(topic_name, 6)], recreate=[topic_name]) == {topic_name: "recreated"}
    assert describe_topics(admin_client(bootstrap))[topic_name] == 6
    assert delete_topics(bootstrap, [topic_name]) == [topic_name]
    assert topic_name not in describe_topics(admin_client(bootstrap))


def test_replay_roundtrip_reconciles_bytes_order_and_heartbeats(bootstrap, topic_name, tmp_path):
    manifest = _simulate(tmp_path / "sim", learners=120, horizon_s=30, seed=21, target_rate=200.0,
                         duplicate_rate=0.1, late_profile="high")
    events = tmp_path / "sim" / "events.parquet"
    ensure_topics(bootstrap, [TopicSpec(topic_name, 3)])
    settings = ReplaySettings(events, topic_name, ProducerSettings(bootstrap), processes=2, duration_s=35,
                              watermark_s=30, heartbeat_s=2)
    started: list[int] = []
    result = run_replay(settings, on_started=started.append)
    path = write_results(result, results_dir=tmp_path / "results", run_id="it-roundtrip", manifest=manifest,
                         rate_check=True)
    assert result["status"] == "success", result
    assert started == [result["replay_start_ms"]]
    assert result["sent"] == result["delivered"] == result["window"]["rows"]
    assert result["duplicates_from_cache"] == result["window"]["duplicate_deliveries"] > 0
    assert result["stamped"] == result["window"]["unique_events"]
    assert result["slip_ms"]["p95_ms"] <= 50
    assert result["heartbeat"]["rounds"] == 2 and result["heartbeat"]["sent"] == 6
    record = json.loads((tmp_path / "results" / "runs.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert record["experiment"] == "ingestion" and record["metrics"]["replay_start_ms"] == result["replay_start_ms"]
    assert json.loads(path.read_text(encoding="utf-8"))["run_id"] == "it-roundtrip"

    check = verify_topic(bootstrap_servers=bootstrap, topic=topic_name, events_path=events,
                         replay_start_ms=result["replay_start_ms"], cutoff_ms=result["cutoff_ms"],
                         watermark_s=30, manifest=manifest)
    assert check["ok"], check
    assert check["topic_unique_events"] == check["expected"]["unique_events"]
    assert check["topic_duplicate_deliveries"] == check["expected"]["duplicate_deliveries"] > 0
    assert check["drain"]["heartbeat_partitions"] == [0, 1, 2]
    assert check["drain"]["watermark_past_end_of_log"]
    assert check["heartbeats"] == 6
    if check["expected"]["window_complete"]:
        assert check["manifest_match"] is True


def _compose(*args: str) -> None:
    subprocess.run(["docker", "compose", *args], cwd=REPO_ROOT, check=True, capture_output=True, timeout=180)


def test_broker_outage_30s_loses_nothing_and_adds_no_duplicates(bootstrap, topic_name, tmp_path):
    """Dừng Kafka 30 s giữa lúc phát 1.000 ev/s: hàng đợi nhỏ ⇒ BufferError ⇒ backpressure; sau đó đối soát."""
    manifest = _simulate(tmp_path / "sim", learners=500, horizon_s=90, seed=33, target_rate=1000.0,
                         duplicate_rate=0.02, late_profile="low")
    events = tmp_path / "sim" / "events.parquet"
    ensure_topics(bootstrap, [TopicSpec(topic_name, 3)])
    timeline: dict[str, float] = {}
    errors: list[BaseException] = []

    def outage(replay_start_ms: int) -> None:
        def run() -> None:
            try:
                time.sleep(max(0.0, replay_start_ms / 1000 + OUTAGE_AFTER_S - time.time()))
                timeline["stop_called"] = time.time()
                _compose("stop", "kafka")
                timeline["stopped"] = time.time()
                time.sleep(max(0.0, timeline["stop_called"] + OUTAGE_S - time.time()))
                timeline["start_called"] = time.time()
                _compose("up", "-d", "--wait", "kafka")
                timeline["healthy"] = time.time()
            except BaseException as exc:  # noqa: BLE001 — báo lỗi về luồng test
                errors.append(exc)

        thread = threading.Thread(target=run, name="kafka-outage", daemon=True)
        threads.append(thread)
        thread.start()

    threads: list[threading.Thread] = []
    settings = ReplaySettings(
        events, topic_name,
        ProducerSettings(bootstrap, queue_max_messages=5_000),  # đầy sau ~5 s mất broker ở 1K ev/s
        processes=2, duration_s=80, watermark_s=30, heartbeat_s=2,
    )
    try:
        result = run_replay(settings, on_started=outage)
    finally:
        for thread in threads:
            thread.join(timeout=240)
        _compose("up", "-d", "--wait", "kafka")  # luôn trả broker về trạng thái chạy
    assert not errors, errors
    write_results(result, results_dir=tmp_path / "results", run_id="it-outage", manifest=manifest, rate_check=False)
    outage_s = timeline["healthy"] - timeline["stop_called"]
    print(json.dumps({"timeline_rel_s": {k: round(v - result["replay_start_ms"] / 1000, 3)
                                         for k, v in timeline.items()},
                      "outage_s": round(outage_s, 1),
                      "replay": {k: result[k] for k in ("scheduled", "sent", "delivered", "delivery_errors",
                                                        "buffer_full_events", "backpressure_s", "slip_ms",
                                                        "client_errors", "achieved_rate_eps", "target_rate_eps")}},
                     ensure_ascii=False))
    assert outage_s >= OUTAGE_S
    assert result["delivered_all"] and result["delivery_errors"] == 0
    assert result["buffer_full_events"] > 0 and result["backpressure_s"] > 5
    assert result["slip_ms"]["max_ms"] > 10_000  # độ trượt lịch do broker dừng được ghi lại

    check = verify_topic(bootstrap_servers=bootstrap, topic=topic_name, events_path=events,
                         replay_start_ms=result["replay_start_ms"], cutoff_ms=result["cutoff_ms"],
                         watermark_s=30, manifest=manifest)
    print(json.dumps({k: check[k] for k in ("ok", "topic_records", "topic_unique_events",
                                            "topic_duplicate_deliveries", "expected", "failures", "drain")},
                     ensure_ascii=False))
    assert check["failures"]["missing_events"] == 0
    assert check["failures"]["extra_duplicates"] == 0
    assert check["topic_duplicate_deliveries"] == check["expected"]["duplicate_deliveries"]
    assert check["ok"], check
