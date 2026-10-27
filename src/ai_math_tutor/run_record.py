"""Bản ghi một lần chạy theo Phụ lục B của hướng dẫn bài tập lớn INT4418.

Mỗi lần chạy thí nghiệm ghi đúng một dòng JSON vào results/raw/runs.jsonl với
các trường: run_id, timestamp, git_commit, data_version, machine, software,
parameters, seed/timeout, metrics, status/log. Biểu đồ chỉ được sinh từ tệp này.
Thêm runtime_s, java, docker_images (image Kafka/Redis đang chạy) và spark_conf để kết
quả streaming tái lập được: song song hóa trạng thái do shuffle partitions và state store
quyết định, không chỉ do phiên bản gói.
"""

from __future__ import annotations

import json
import os
import platform
import shutil
import subprocess
import sys
import uuid
from datetime import datetime, timezone
from importlib import metadata
from pathlib import Path
from typing import Any

# Gói có ảnh hưởng tới kết quả; gói chưa cài được ghi là null thay vì bỏ qua.
TRACKED_PACKAGES = (
    "pyspark", "confluent-kafka", "sympy", "jsonschema", "redis", "fastapi",
    "pandas", "pyarrow", "numpy", "locust", "streamlit", "uvicorn",
)
VALID_STATUSES = ("success", "failed", "timeout", "aborted")
# Khớp khóa `name:` trong docker-compose.yml.
COMPOSE_PROJECT = "nhom8"


def _run(command: list[str], *, stderr: bool = False, timeout: float = 5) -> str | None:
    """Đầu ra của lệnh, hoặc None khi lệnh không có/lỗi: thiếu thông tin môi trường không làm hỏng run."""
    try:
        out = subprocess.run(command, capture_output=True, text=True, timeout=timeout, check=True)
    except (OSError, subprocess.SubprocessError):
        return None
    return (out.stderr if stderr else out.stdout).strip() or None


# Đường dẫn quyết định hành vi của một run. Kết quả (results/), tài liệu và phiếu rà soát không tính:
# chính runner ghi results/raw/runs.jsonl, nên xét cả repo thì mọi run trong một ma trận đều "dirty".
CODE_PATHS = ("src", "scripts", "configs", "contracts", "pyproject.toml", "uv.lock", "docker-compose.yml",
              "Makefile", ".env.example")


def git_commit(repo_dir: Path) -> dict[str, Any]:
    """Commit hiện tại và cờ dirty; kết quả từ mã chưa commit phải bị đánh dấu."""
    commit = _run(["git", "-C", str(repo_dir), "rev-parse", "HEAD"])
    status = _run(["git", "-C", str(repo_dir), "status", "--porcelain", "--", *CODE_PATHS])
    return {"commit": commit, "dirty": bool(status) if commit else None}


def _total_ram_bytes() -> int | None:
    if sys.platform == "darwin":
        value = _run(["sysctl", "-n", "hw.memsize"])
        return int(value) if value and value.isdigit() else None
    meminfo = Path("/proc/meminfo")
    if meminfo.exists():
        for line in meminfo.read_text().splitlines():
            if line.startswith("MemTotal:"):
                return int(line.split()[1]) * 1024
    return None


def _cpu_model() -> str:
    if sys.platform == "darwin":
        return _run(["sysctl", "-n", "machdep.cpu.brand_string"]) or platform.processor()
    return platform.processor() or platform.machine()


def machine_info() -> dict[str, Any]:
    gpu = _run(["nvidia-smi", "--query-gpu=name,memory.total", "--format=csv,noheader"]) if shutil.which("nvidia-smi") else None
    return {
        "hostname": platform.node(),
        "os": f"{platform.system()} {platform.release()}",
        "cpu": _cpu_model(),
        "cpu_count": os.cpu_count(),
        "ram_bytes": _total_ram_bytes(),
        "gpu": gpu,
    }


def software_versions() -> dict[str, str | None]:
    versions: dict[str, str | None] = {"python": platform.python_version()}
    for package in TRACKED_PACKAGES:
        try:
            versions[package] = metadata.version(package)
        except metadata.PackageNotFoundError:
            versions[package] = None
    return versions


def java_version() -> str | None:
    """Dòng đầu của `java -version` (in ra stderr); ưu tiên $JAVA_HOME giống Spark."""
    java_home = os.environ.get("JAVA_HOME")
    java = str(Path(java_home) / "bin" / "java") if java_home else "java"
    output = _run([java, "-version"], stderr=True)
    return output.splitlines()[0] if output else None


def docker_images(project: str = COMPOSE_PROJECT) -> dict[str, dict[str, str]] | None:
    """Image của các container Compose đang chạy, theo service: tham chiếu (ghim digest) và image ID.

    Lọc theo nhãn project nên không cần đọc docker-compose.yml/.env. None khi không gọi được
    Docker; {} khi Docker chạy nhưng stack chưa up.
    """
    ids = _run(["docker", "ps", "-q", "--filter", f"label=com.docker.compose.project={project}"], timeout=15)
    if ids is None:
        return {} if _run(["docker", "info", "--format", "{{.ServerVersion}}"], timeout=15) else None
    inspected = _run(["docker", "inspect", *ids.split()], timeout=15)
    if inspected is None:
        return None
    try:
        containers = json.loads(inspected)
    except json.JSONDecodeError:
        return None
    images: dict[str, dict[str, str]] = {}
    for container in containers:
        config = container.get("Config") or {}
        labels = config.get("Labels") or {}
        service = labels.get("com.docker.compose.service") or container.get("Name", "").lstrip("/")
        images[service] = {"ref": config.get("Image", ""), "image_id": container.get("Image", "")}
    return images


def build_run_record(
    *,
    experiment: str,
    data_version: str,
    parameters: dict[str, Any],
    seed: int,
    timeout_s: float,
    metrics: dict[str, Any],
    status: str,
    log_path: str,
    started_at: datetime | None = None,
    repo_dir: Path | None = None,
    run_id: str | None = None,
    runtime_s: float | None = None,
    spark_conf: dict[str, Any] | None = None,
    images: dict[str, dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Một bản ghi run đủ Phụ lục B.

    runtime_s: thời gian chạy thật của run (giây, đồng hồ tường); None nếu run không tới được đo.
    spark_conf: conf đã dùng (shuffle partitions, state store, trigger, jar đã resolve...).
    images: image Docker đã dùng; None → tự đọc từ các container Compose đang chạy.
    """
    if status not in VALID_STATUSES:
        raise ValueError(f"status phải thuộc {VALID_STATUSES}, nhận {status!r}")
    if runtime_s is not None and (
        isinstance(runtime_s, bool) or not isinstance(runtime_s, (int, float)) or not runtime_s >= 0
    ):
        raise ValueError(f"runtime_s phải là số giây ≥ 0, nhận {runtime_s!r}")
    started = started_at or datetime.now(timezone.utc)
    repo = repo_dir or Path.cwd()
    return {
        "run_id": run_id or f"{experiment}-{started:%Y%m%dT%H%M%SZ}-{uuid.uuid4().hex[:6]}",
        "experiment": experiment,
        "timestamp": started.astimezone(timezone.utc).isoformat(),
        "git": git_commit(repo),
        "data_version": data_version,
        "machine": machine_info(),
        "software": software_versions(),
        "java": java_version(),
        "docker_images": docker_images() if images is None else images,
        "spark_conf": dict(spark_conf) if spark_conf else {},
        "parameters": parameters,
        "seed": seed,
        "timeout_s": timeout_s,
        "runtime_s": runtime_s,
        "metrics": metrics,
        "status": status,
        "log_path": log_path,
    }


def append_run_record(record: dict[str, Any], path: Path) -> None:
    """Chỉ ghi nối; kết quả thô không bao giờ bị sửa tay hay ghi đè."""
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(record, ensure_ascii=False, sort_keys=True) + "\n")
