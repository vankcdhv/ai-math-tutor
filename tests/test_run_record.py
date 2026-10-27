import json
from pathlib import Path

import pytest

from ai_math_tutor import run_record
from ai_math_tutor.run_record import TRACKED_PACKAGES, append_run_record, build_run_record

# Các trường bắt buộc theo Phụ lục B của hướng dẫn bài tập lớn.
APPENDIX_B_FIELDS = {
    "run_id", "timestamp", "git", "data_version", "machine", "software",
    "parameters", "seed", "timeout_s", "metrics", "status", "log_path",
}
# Trường thêm để tái lập kết quả streaming.
REPRODUCIBILITY_FIELDS = {"runtime_s", "java", "docker_images", "spark_conf"}
FAKE_IMAGES = {"kafka": {"ref": "apache/kafka:4.3.2@sha256:abc", "image_id": "sha256:def"}}


def _record(**overrides):
    args = dict(
        experiment="expA-throughput",
        data_version="events-week1-sample@0.1",
        parameters={"rate_eps": 1000, "partitions": 3},
        seed=42,
        timeout_s=600,
        metrics={"p95_latency_ms": None},
        status="success",
        log_path="results/raw/logs/expA.log",
        repo_dir=Path(__file__).resolve().parents[1],
    )
    args.update(overrides)
    return build_run_record(**args)


def test_record_has_all_appendix_b_fields():
    record = _record()
    assert APPENDIX_B_FIELDS <= record.keys()
    assert {"os", "cpu", "cpu_count", "ram_bytes", "gpu"} <= record["machine"].keys()
    assert record["software"]["python"]
    assert record["run_id"].startswith("expA-throughput-")


def test_invalid_status_rejected():
    with pytest.raises(ValueError):
        _record(status="ok")


def test_append_only(tmp_path):
    path = tmp_path / "raw" / "runs.jsonl"
    append_run_record(_record(run_id="r1"), path)
    append_run_record(_record(run_id="r2"), path)
    ids = [json.loads(line)["run_id"] for line in path.read_text().splitlines()]
    assert ids == ["r1", "r2"]


def test_record_has_reproducibility_fields():
    conf = {"spark.sql.shuffle.partitions": "3", "trigger": "asap"}
    record = _record(runtime_s=12.5, spark_conf=conf, images=FAKE_IMAGES)
    assert REPRODUCIBILITY_FIELDS <= record.keys()
    assert record["runtime_s"] == 12.5
    assert record["spark_conf"] == conf and record["spark_conf"] is not conf
    assert record["docker_images"] == FAKE_IMAGES
    json.dumps(record)  # phải ghi được thành một dòng JSON


def test_reproducibility_fields_default_when_omitted():
    record = _record(images={})
    assert record["runtime_s"] is None
    assert record["spark_conf"] == {}
    assert record["docker_images"] == {}


@pytest.mark.parametrize("value", [-1, float("nan"), "12", True])
def test_invalid_runtime_rejected(value):
    with pytest.raises(ValueError):
        _record(runtime_s=value, images={})


def test_tracked_packages_include_streaming_and_service_stack():
    assert {"pandas", "pyarrow", "numpy", "locust", "streamlit", "uvicorn"} <= set(TRACKED_PACKAGES)
    software = _record(images={})["software"]
    assert set(TRACKED_PACKAGES) <= software.keys()
    assert software["pandas"]  # cài trong môi trường dự án


def test_java_version_reads_stderr(monkeypatch):
    calls = []

    def fake_run(command, *, stderr=False, timeout=5):
        calls.append((command, stderr))
        return 'openjdk version "17.0.20.1" 2026-08-18\nOpenJDK Runtime Environment'

    monkeypatch.setenv("JAVA_HOME", "/opt/jdk17")
    monkeypatch.setattr(run_record, "_run", fake_run)
    assert run_record.java_version() == 'openjdk version "17.0.20.1" 2026-08-18'
    assert calls == [([str(Path("/opt/jdk17") / "bin" / "java"), "-version"], True)]


def test_java_version_real_jdk():
    # JDK 17+ là yêu cầu môi trường (docs/setup.md); Spark không chạy được nếu thiếu.
    version = run_record.java_version()
    assert version is not None and "version" in version


@pytest.mark.integration
def test_docker_images_real_stack_matches_compose_pins():
    compose = (Path(__file__).resolve().parents[1] / "docker-compose.yml").read_text(encoding="utf-8")
    images = run_record.docker_images()
    assert images is not None and {"kafka", "redis"} <= images.keys(), "cần `make up` trước"
    for service in ("kafka", "redis"):
        ref = images[service]["ref"]
        assert "@sha256:" in ref and f"image: {ref}" in compose
        assert images[service]["image_id"].startswith("sha256:")


def test_java_version_none_when_missing(monkeypatch):
    monkeypatch.setattr(run_record, "_run", lambda *a, **k: None)
    assert run_record.java_version() is None


def test_docker_images_parses_compose_containers(monkeypatch):
    inspect = [
        {"Name": "/nhom8-kafka-1", "Image": "sha256:k",
         "Config": {"Image": "apache/kafka:4.3.2@sha256:aa", "Labels": {"com.docker.compose.service": "kafka"}}},
        {"Name": "/nhom8-redis-1", "Image": "sha256:r",
         "Config": {"Image": "redis:8.10.2-alpine@sha256:bb", "Labels": {"com.docker.compose.service": "redis"}}},
    ]

    def fake_run(command, *, stderr=False, timeout=5):
        if command[:2] == ["docker", "ps"]:
            assert "label=com.docker.compose.project=nhom8" in command
            return "id1\nid2"
        if command[:2] == ["docker", "inspect"]:
            assert command[2:] == ["id1", "id2"]
            return json.dumps(inspect)
        raise AssertionError(command)

    monkeypatch.setattr(run_record, "_run", fake_run)
    assert run_record.docker_images() == {
        "kafka": {"ref": "apache/kafka:4.3.2@sha256:aa", "image_id": "sha256:k"},
        "redis": {"ref": "redis:8.10.2-alpine@sha256:bb", "image_id": "sha256:r"},
    }


def test_docker_images_empty_when_stack_down_and_none_without_docker(monkeypatch):
    monkeypatch.setattr(run_record, "_run", lambda command, **k: "29.9.2" if command[1] == "info" else None)
    assert run_record.docker_images() == {}
    monkeypatch.setattr(run_record, "_run", lambda *a, **k: None)
    assert run_record.docker_images() is None


def test_dirty_flag_ignores_results_and_docs_but_not_code(tmp_path):
    import shutil
    import subprocess

    if shutil.which("git") is None:
        pytest.skip("không có git")

    def git(*args):
        subprocess.run(["git", "-C", str(tmp_path), *args], check=True, capture_output=True)

    git("init", "-q")
    (tmp_path / "src").mkdir()
    (tmp_path / "src" / "mod.py").write_text("x = 1\n", encoding="utf-8")
    (tmp_path / "results").mkdir()
    (tmp_path / "results" / "runs.jsonl").write_text("", encoding="utf-8")
    git("add", ".")
    git("-c", "user.name=t", "-c", "user.email=t@example.com", "commit", "-q", "-m", "init")
    assert run_record.git_commit(tmp_path)["dirty"] is False

    (tmp_path / "results" / "runs.jsonl").write_text("{}\n", encoding="utf-8")
    (tmp_path / "notes.md").write_text("ghi chú\n", encoding="utf-8")
    assert run_record.git_commit(tmp_path)["dirty"] is False  # chỉ kết quả/tài liệu đổi

    (tmp_path / "src" / "mod.py").write_text("x = 2\n", encoding="utf-8")
    assert run_record.git_commit(tmp_path)["dirty"] is True