"""Kiểm tĩnh docker-compose.yml (kể cả profile `app`) và docker/app.Dockerfile.

- Mọi cổng publish bind 127.0.0.1; image ghim digest hoặc build từ docker/app.Dockerfile của repo, và
  mọi image nền của Dockerfile cũng ghim digest.
- Không có bí mật trong compose hay Dockerfile: biến kiểu mật khẩu/khóa chỉ lấy từ .env (bắt buộc hoặc
  mặc định rỗng); .dockerignore loại .env khỏi build context.

Đọc cấu hình bằng `docker compose config` (cần Docker CLI; chỉ phân tích tệp, không khởi động
container), nên bắt được cả cổng khai báo dạng ngắn "9092:9092" (bind 0.0.0.0) lẫn dạng dài thiếu host_ip.
"""

import json
import os
import re
import shutil
import subprocess
from pathlib import Path

import pytest

from ai_math_tutor.config import read_dotenv

pytestmark = pytest.mark.integration

REPO_ROOT = Path(__file__).resolve().parents[1]
APP_DOCKERFILE = "docker/app.Dockerfile"
APP_PROFILE = "app"
APP_SERVICES = {"demo-setup", "streaming", "demo-replay", "hint-api", "dashboard"}
INFRA_SERVICES = {"kafka", "redis"}
# Giá trị giả chỉ để nội suy ${VAR:?} khi máy chưa đặt biến; không chạy container.
PLACEHOLDER_ENV = {"REDIS_PASSWORD": "compose-config-check", "NHOM8_HINT_API_KEY": "compose-config-check-key"}
SECRET_NAME = re.compile(r"(PASSWORD|SECRET|TOKEN|AUTH|_KEY)$")
# Chỉ chấp nhận tham chiếu biến: bắt buộc (${VAR:?...}, ${VAR?...}) hoặc mặc định rỗng (${VAR:-}, ${VAR}).
SECRET_REFERENCE = re.compile(r"^\$\{[A-Za-z_][A-Za-z0-9_]*(:?\?[^}]*|:?-)?\}$")
DIGEST = re.compile(r"@sha256:[0-9a-f]{64}$")


def compose_config(project_dir: Path, *, profiles: tuple[str, ...] = (), interpolate: bool = True) -> dict:
    if shutil.which("docker") is None:
        pytest.fail("Không tìm thấy Docker CLI; kiểm cổng compose cần Docker (docs/setup.md)")
    env = dict(os.environ)
    dotenv = read_dotenv(project_dir / ".env")
    for name, value in PLACEHOLDER_ENV.items():
        if name not in env and name not in dotenv:
            env[name] = value
    command = ["docker", "compose", "--project-directory", str(project_dir)]
    for profile in profiles:
        command += ["--profile", profile]
    command += ["config", "--format", "json"] + ([] if interpolate else ["--no-interpolate"])
    result = subprocess.run(command, capture_output=True, text=True, timeout=60, env=env, cwd=project_dir)
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


def secret_literals(config: dict) -> list[str]:
    """Biến môi trường kiểu bí mật có giá trị viết thẳng (cấu hình chưa nội suy)."""
    offenders = []
    for name, service in config["services"].items():
        sources = [service.get("environment") or {}, (service.get("build") or {}).get("args") or {}]
        for mapping in sources:
            for key, value in mapping.items():
                if SECRET_NAME.search(key) and value is not None and not SECRET_REFERENCE.match(str(value)):
                    offenders.append(f"{name}: {key}")
    return offenders


def dockerfile_base_images(text: str) -> list[str]:
    """Image ngoài của Dockerfile: mọi FROM và COPY --from (bỏ tên stage nội bộ)."""
    stages = set(re.findall(r"^\s*FROM\s+\S+\s+AS\s+(\S+)", text, re.MULTILINE | re.IGNORECASE))
    refs = re.findall(r"^\s*FROM\s+(?:--platform=\S+\s+)?(\S+)", text, re.MULTILINE | re.IGNORECASE)
    refs += re.findall(r"--from=(\S+)", text)
    return [ref for ref in refs if ref not in stages]


@pytest.fixture(scope="module")
def compose() -> dict:
    return compose_config(REPO_ROOT, profiles=(APP_PROFILE,))


@pytest.fixture(scope="module")
def compose_raw() -> dict:
    return compose_config(REPO_ROOT, profiles=(APP_PROFILE,), interpolate=False)


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


def test_app_profile_services_and_ports(compose):
    services = compose["services"]
    assert APP_SERVICES <= set(services), f"thiếu service của profile app: {APP_SERVICES - set(services)}"
    for name in APP_SERVICES:
        assert services[name].get("profiles") == [APP_PROFILE], f"{name} phải chỉ thuộc profile app"
        assert services[name]["environment"].get("NHOM8_ENV") == "container", f"{name} thiếu NHOM8_ENV=container"
    for name in INFRA_SERVICES:
        assert not services[name].get("profiles"), f"{name} phải chạy cả khi không bật profile (make up)"
    published = {name: {(p.get("host_ip"), str(p.get("published")), p.get("target"))
                        for p in services[name].get("ports") or []} for name in APP_SERVICES}
    assert published["hint-api"] == {("127.0.0.1", "8000", 8000)}
    assert published["dashboard"] == {("127.0.0.1", "8501", 8501)}
    assert not any(published[name] for name in APP_SERVICES - {"hint-api", "dashboard"})
    # Trong container phải nghe mọi giao diện thì Docker mới chuyển được cổng; phía host vẫn chỉ 127.0.0.1.
    hint_command = services["hint-api"]["command"]
    assert hint_command[hint_command.index("--host") + 1] == "0.0.0.0"
    dashboard_command = services["dashboard"]["command"]
    assert dashboard_command[dashboard_command.index("--server.address") + 1] == "0.0.0.0"


def test_images_pinned_by_digest_or_built_locally(compose):
    offenders = []
    for name, service in compose["services"].items():
        build = service.get("build")
        if build:
            if Path(build["context"]).resolve() != REPO_ROOT or build.get("dockerfile") != APP_DOCKERFILE:
                offenders.append(f"{name}: build {build}")
        elif not DIGEST.search(service.get("image", "")):
            offenders.append(f"{name}: {service.get('image')}")
    assert not offenders, f"Image chưa ghim digest và không build từ {APP_DOCKERFILE}: {offenders}"


def test_dockerfile_base_images_pinned_by_digest():
    refs = dockerfile_base_images((REPO_ROOT / APP_DOCKERFILE).read_text(encoding="utf-8"))
    assert refs, f"{APP_DOCKERFILE} không có FROM nào?"
    unpinned = [ref for ref in refs if not DIGEST.search(ref)]
    assert not unpinned, f"Image nền chưa ghim digest: {unpinned}"


def test_no_secrets_in_compose_or_image(compose_raw):
    offenders = secret_literals(compose_raw)
    assert not offenders, f"Biến bí mật phải lấy từ .env (${{VAR:?...}} hoặc ${{VAR:-}}), không viết thẳng: {offenders}"
    dockerfile = (REPO_ROOT / APP_DOCKERFILE).read_text(encoding="utf-8")
    baked = [m for m in re.findall(r"^\s*(?:ENV|ARG)\s+(.+)$", dockerfile, re.MULTILINE)
             if re.search(r"(PASSWORD|SECRET|TOKEN|API_KEY)", m)]
    assert not baked, f"Dockerfile đặt biến bí mật: {baked}"
    assert not re.search(r"^\s*(COPY|ADD)\s+.*\.env\b", dockerfile, re.MULTILINE), "Dockerfile chép .env vào image"
    ignored = {line.strip() for line in (REPO_ROOT / ".dockerignore").read_text(encoding="utf-8").splitlines()}
    assert {".env", "**/.env"} <= ignored, ".dockerignore phải loại .env khỏi build context"


def test_secret_checker_flags_literal_values():
    config = {"services": {
        "ok": {"environment": {"REDIS_PASSWORD": "${REDIS_PASSWORD:?đặt trong .env}", "TEACHER_PASSWORD": "${TEACHER_PASSWORD:-}",
                               "DEMO_KEY_PREFIX": "demo:"}},
        "literal": {"environment": {"NHOM8_HINT_API_KEY": "abc123abc123abc123"}},
        "default": {"environment": {"REDIS_PASSWORD": "${REDIS_PASSWORD:-hunter2}"}},
        "build_arg": {"build": {"args": {"GITHUB_TOKEN": "ghp_x"}}},
    }}
    assert sorted(o.split(":")[0] for o in secret_literals(config)) == ["build_arg", "default", "literal"]


def test_redis_requires_password(compose):
    redis = compose["services"]["redis"]
    assert "--requirepass" in " ".join(redis.get("command") or [])
    assert redis["environment"]["REDIS_PASSWORD"], "REDIS_PASSWORD phải đến từ .env"
