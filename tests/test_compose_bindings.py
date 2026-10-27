"""Mọi cổng publish của docker-compose.yml phải bind 127.0.0.1 và image phải ghim digest.

Đọc cấu hình đã nội suy bằng `docker compose config` (cần Docker CLI), nên bắt được cả
cổng khai báo dạng ngắn "9092:9092" (bind 0.0.0.0) lẫn dạng dài thiếu host_ip.
"""

import json
import os
import shutil
import subprocess
from pathlib import Path

import pytest

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[1]
# Giá trị giả chỉ để nội suy ${REDIS_PASSWORD:?} khi máy chưa có .env; không chạy container.
PLACEHOLDER_ENV = {"REDIS_PASSWORD": "compose-config-check"}


def compose_config(project_dir: Path) -> dict:
    if shutil.which("docker") is None:
        pytest.fail("Không tìm thấy Docker CLI; kiểm cổng compose cần Docker (docs/setup.md)")
    env = dict(os.environ)
    if "REDIS_PASSWORD" not in env and not (project_dir / ".env").is_file():
        env.update(PLACEHOLDER_ENV)
    result = subprocess.run(
        ["docker", "compose", "--project-directory", str(project_dir), "config", "--format", "json"],
        capture_output=True, text=True, timeout=60, env=env, cwd=project_dir,
    )
    assert result.returncode == 0, f"docker compose config lỗi: {result.stderr.strip()}"
    return json.loads(result.stdout)


def non_loopback_ports(config: dict) -> tuple[int, list[str]]:
    """(số cổng publish, danh sách cổng không bind 127.0.0.1)."""
    published, offenders = 0, []
    for name, service in config["services"].items():
        for port in service.get("ports") or []:
            published += 1
            if port.get("host_ip") != "127.0.0.1":
                offenders.append(f"{name}: {port}")
    return published, offenders


@pytest.fixture(scope="module")
def compose() -> dict:
    return compose_config(REPO_ROOT)


def test_every_published_port_binds_loopback(compose):
    published, offenders = non_loopback_ports(compose)
    assert published > 0, "compose không publish cổng nào — kiểm tra nhầm tệp?"
    assert not offenders, f"Cổng bind ngoài 127.0.0.1: {offenders}"


def test_checker_flags_ports_published_on_all_interfaces(tmp_path):
    # Dạng ngắn không có IP: Docker bind mọi giao diện — đúng lỗi mà kiểm tra phải bắt.
    (tmp_path / "docker-compose.yml").write_text(
        "services:\n"
        "  open:\n    image: redis\n    ports: [\"6379:6379\"]\n"
        "  wildcard:\n    image: redis\n    ports: [\"0.0.0.0:6380:6379\"]\n"
        "  safe:\n    image: redis\n    ports: [\"127.0.0.1:6381:6379\"]\n",
        encoding="utf-8",
    )
    published, offenders = non_loopback_ports(compose_config(tmp_path))
    assert published == 3
    assert sorted(o.split(":")[0] for o in offenders) == ["open", "wildcard"]


def test_images_pinned_by_digest(compose):
    unpinned = [f"{n}: {s.get('image')}" for n, s in compose["services"].items()
                if "image" in s and "@sha256:" not in s["image"]]
    assert not unpinned, f"Image chưa ghim digest: {unpinned}"


def test_redis_requires_password(compose):
    redis = compose["services"]["redis"]
    assert "--requirepass" in " ".join(redis.get("command") or [])
    assert redis["environment"]["REDIS_PASSWORD"], "REDIS_PASSWORD phải đến từ .env"
