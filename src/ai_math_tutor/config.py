"""Cấu hình chạy: đọc configs/runtime.yaml rồi ghi đè bằng biến môi trường.

Một nguồn duy nhất cho bootstrap Kafka, tên topic, Redis, thư mục dữ liệu nóng và
Spark conf (gồm spark.sql.shuffle.partitions và state store RocksDB). Mật khẩu Redis
chỉ đến từ biến môi trường hoặc tệp .env (đã gitignore), không bao giờ từ YAML.
"""

from __future__ import annotations

import os
import re
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import quote

import yaml

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONFIG_PATH = REPO_ROOT / "configs" / "runtime.yaml"
DEFAULT_DOTENV_PATH = REPO_ROOT / ".env"
ENVIRONMENTS = ("host", "container")
TOPIC_KEYS = ("learner_events", "alerts", "dead_letter", "hint_requests")
# run_id thành tên thư mục: chỉ ký tự an toàn, không "..", không dấu phân cách đường dẫn.
_RUN_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$")


class ConfigError(ValueError):
    """Cấu hình thiếu hoặc sai kiểu; báo sớm thay vì để Spark/Kafka lỗi khó hiểu."""


@dataclass(frozen=True)
class KafkaConfig:
    bootstrap_servers: str
    partitions: int
    topics: dict[str, str]


@dataclass(frozen=True)
class RedisConfig:
    host: str
    port: int
    db: int
    password: str | None = field(default=None, repr=False)

    @property
    def url(self) -> str:
        """URL redis://:<mật khẩu>@host:port/db; thiếu mật khẩu là lỗi vì Redis luôn bật requirepass."""
        if not self.password:
            raise ConfigError("Thiếu REDIS_PASSWORD (đặt trong .env hoặc biến môi trường)")
        return f"redis://:{quote(self.password, safe='')}@{self.host}:{self.port}/{self.db}"

    @property
    def redacted_url(self) -> str:
        """URL an toàn để ghi log/run record."""
        return f"redis://:***@{self.host}:{self.port}/{self.db}"


@dataclass(frozen=True)
class PathsConfig:
    data_root: Path
    checkpoints: Path
    lake: Path
    results: Path

    def checkpoint_dir(self, run_id: str) -> Path:
        return self.checkpoints / validate_run_id(run_id)

    def lake_dir(self, run_id: str) -> Path:
        return self.lake / validate_run_id(run_id)


@dataclass(frozen=True)
class SparkConfig:
    master: str
    packages: tuple[str, ...]
    state_store_provider: str
    shuffle_partitions: int
    driver_memory: str
    extra_conf: dict[str, str]

    def conf(self) -> dict[str, str]:
        """Spark conf đầy đủ để đặt vào SparkSession.builder và ghi vào run record."""
        conf = dict(self.extra_conf)
        conf.update({
            "spark.master": self.master,
            "spark.jars.packages": ",".join(self.packages),
            "spark.sql.streaming.stateStore.providerClass": self.state_store_provider,
            "spark.sql.shuffle.partitions": str(self.shuffle_partitions),
            "spark.driver.memory": self.driver_memory,
        })
        return conf


@dataclass(frozen=True)
class RuntimeConfig:
    environment: str
    kafka: KafkaConfig
    redis: RedisConfig
    paths: PathsConfig
    spark: SparkConfig


def validate_run_id(run_id: str) -> str:
    if not isinstance(run_id, str) or not _RUN_ID_RE.match(run_id) or ".." in run_id:
        raise ConfigError(f"run_id không hợp lệ cho tên thư mục: {run_id!r}")
    return run_id


def read_dotenv(path: Path) -> dict[str, str]:
    """Đọc tệp KEY=VALUE tối giản (bỏ dòng trống, comment, dấu nháy bao ngoài)."""
    if not path.is_file():
        return {}
    values: dict[str, str] = {}
    for number, raw in enumerate(path.read_text(encoding="utf-8").splitlines(), start=1):
        line = raw.strip()
        if not line or line.startswith("#"):
            continue
        if line.startswith("export "):
            line = line[len("export "):].lstrip()
        key, sep, value = line.partition("=")
        key = key.strip()
        if not sep or not re.fullmatch(r"[A-Za-z_][A-Za-z0-9_]*", key):
            raise ConfigError(f"{path}:{number}: dòng không đúng dạng KEY=VALUE")
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "\"'":
            value = value[1:-1]
        values[key] = value
    return values


def _section(raw: Mapping[str, Any], name: str) -> Mapping[str, Any]:
    value = raw.get(name)
    if not isinstance(value, Mapping):
        raise ConfigError(f"runtime.yaml thiếu mục '{name}'")
    return value


def _int_at_least(value: Any, name: str, minimum: int) -> int:
    """Số nguyên từ YAML (int) hoặc biến môi trường (chuỗi chữ số); từ chối bool, số thực, rác."""
    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise ConfigError(f"{name} phải là số nguyên, nhận {value!r}")
    text = str(value).strip()
    if not re.fullmatch(r"-?[0-9]+", text):
        raise ConfigError(f"{name} phải là số nguyên, nhận {value!r}")
    number = int(text)
    if number < minimum:
        raise ConfigError(f"{name} phải ≥ {minimum}, nhận {value!r}")
    return number


def _positive_int(value: Any, name: str) -> int:
    return _int_at_least(value, name, minimum=1)


def _per_environment(value: Any, environment: str, name: str) -> str:
    if isinstance(value, Mapping):
        if environment not in value:
            raise ConfigError(f"{name} thiếu giá trị cho môi trường '{environment}'")
        value = value[environment]
    if not isinstance(value, str) or not value:
        raise ConfigError(f"{name} phải là chuỗi không rỗng")
    return value


def load_config(
    path: Path | None = None,
    env: Mapping[str, str] | None = None,
    dotenv_path: Path | None = DEFAULT_DOTENV_PATH,
) -> RuntimeConfig:
    """Đọc YAML, rồi áp ghi đè: biến môi trường > .env > runtime.yaml."""
    config_path = path or DEFAULT_CONFIG_PATH
    try:
        raw = yaml.safe_load(config_path.read_text(encoding="utf-8"))
    except OSError as exc:
        raise ConfigError(f"Không đọc được {config_path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise ConfigError(f"{config_path} không phải YAML hợp lệ: {exc}") from exc
    if not isinstance(raw, Mapping):
        raise ConfigError(f"{config_path} phải là một mapping YAML")

    merged: dict[str, str] = read_dotenv(dotenv_path) if dotenv_path else {}
    merged.update(os.environ if env is None else env)

    environment = merged.get("NHOM8_ENV", "host")
    if environment not in ENVIRONMENTS:
        raise ConfigError(f"NHOM8_ENV phải thuộc {ENVIRONMENTS}, nhận {environment!r}")

    kafka_raw = _section(raw, "kafka")
    topics_raw = kafka_raw.get("topics") or {}
    missing = [key for key in TOPIC_KEYS if not topics_raw.get(key)]
    if missing:
        raise ConfigError(f"kafka.topics thiếu {missing}")
    partitions = _positive_int(merged.get("NHOM8_KAFKA_PARTITIONS", kafka_raw.get("partitions")), "kafka.partitions")
    kafka = KafkaConfig(
        bootstrap_servers=merged.get("NHOM8_KAFKA_BOOTSTRAP")
        or _per_environment(kafka_raw.get("bootstrap_servers"), environment, "kafka.bootstrap_servers"),
        partitions=partitions,
        topics={key: str(topics_raw[key]) for key in TOPIC_KEYS},
    )

    redis_raw = _section(raw, "redis")
    redis = RedisConfig(
        host=merged.get("NHOM8_REDIS_HOST") or _per_environment(redis_raw.get("host"), environment, "redis.host"),
        port=_positive_int(merged.get("NHOM8_REDIS_PORT", redis_raw.get("port", 6379)), "redis.port"),
        db=_int_at_least(merged.get("NHOM8_REDIS_DB", redis_raw.get("db", 0)), "redis.db", minimum=0),
        password=merged.get("REDIS_PASSWORD") or None,
    )

    paths_raw = _section(raw, "paths")
    data_root = Path(merged.get("NHOM8_DATA_ROOT") or str(paths_raw.get("data_root", "~/nhom8-data"))).expanduser()
    results = Path(merged.get("NHOM8_RESULTS_DIR") or str(paths_raw.get("results", "results/raw"))).expanduser()
    paths = PathsConfig(
        data_root=data_root,
        checkpoints=data_root / str(paths_raw.get("checkpoints", "checkpoints")),
        lake=data_root / str(paths_raw.get("lake", "lake")),
        results=results if results.is_absolute() else REPO_ROOT / results,
    )

    spark_raw = _section(raw, "spark")
    shuffle_source = merged.get("NHOM8_SHUFFLE_PARTITIONS", spark_raw.get("shuffle_partitions"))
    packages = spark_raw.get("packages") or []
    if not isinstance(packages, list) or not all(isinstance(p, str) for p in packages):
        raise ConfigError("spark.packages phải là danh sách tọa độ Maven")
    extra = spark_raw.get("conf") or {}
    if not isinstance(extra, Mapping):
        raise ConfigError("spark.conf phải là mapping")
    spark = SparkConfig(
        master=str(spark_raw.get("master", "local[*]")),
        packages=tuple(packages),
        state_store_provider=str(spark_raw.get("state_store_provider", "")),
        shuffle_partitions=partitions if shuffle_source in (None, "") else _positive_int(shuffle_source, "spark.shuffle_partitions"),
        driver_memory=str(spark_raw.get("driver_memory", "4g")),
        extra_conf={str(k): str(v) for k, v in extra.items()},
    )
    if not spark.state_store_provider:
        raise ConfigError("spark.state_store_provider không được rỗng")

    return RuntimeConfig(environment=environment, kafka=kafka, redis=redis, paths=paths, spark=spark)
