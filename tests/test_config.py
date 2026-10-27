from pathlib import Path

import pytest

from ai_math_tutor.config import (
    DEFAULT_CONFIG_PATH,
    REPO_ROOT,
    ConfigError,
    load_config,
    read_dotenv,
    validate_run_id,
)


def _load(env=None, **kwargs):
    # dotenv_path=None: test không phụ thuộc .env cục bộ của máy chạy.
    kwargs.setdefault("dotenv_path", None)
    return load_config(env=env or {}, **kwargs)


def test_defaults_from_runtime_yaml():
    cfg = _load({"NHOM8_DATA_ROOT": "/data/nhom8"})
    assert cfg.environment == "host"
    assert cfg.kafka.bootstrap_servers == "127.0.0.1:9092"
    assert cfg.kafka.partitions == 3
    assert cfg.kafka.topics == {
        "learner_events": "learner-events",
        "alerts": "alerts",
        "dead_letter": "dead-letter",
        "hint_requests": "hint-requests",
    }
    assert cfg.redis.host == "127.0.0.1" and cfg.redis.port == 6379
    assert cfg.paths.checkpoint_dir("r1") == Path("/data/nhom8/checkpoints/r1")
    assert cfg.paths.lake_dir("r1") == Path("/data/nhom8/lake/r1")
    assert cfg.paths.results == REPO_ROOT / "results" / "raw"


def test_spark_conf_ties_shuffle_partitions_to_kafka_partitions():
    conf = _load({"NHOM8_KAFKA_PARTITIONS": "6"}).spark.conf()
    assert conf["spark.sql.shuffle.partitions"] == "6"
    assert conf["spark.sql.streaming.stateStore.providerClass"].endswith("RocksDBStateStoreProvider")
    assert conf["spark.jars.packages"] == "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.3"
    assert conf["spark.sql.session.timeZone"] == "UTC"


def test_explicit_shuffle_partitions_override():
    cfg = _load({"NHOM8_KAFKA_PARTITIONS": "3", "NHOM8_SHUFFLE_PARTITIONS": "200"})
    assert cfg.spark.conf()["spark.sql.shuffle.partitions"] == "200"
    assert cfg.kafka.partitions == 3


def test_container_environment_uses_internal_listener():
    cfg = _load({"NHOM8_ENV": "container"})
    assert cfg.kafka.bootstrap_servers == "kafka:29092"
    assert cfg.redis.host == "redis"


def test_env_overrides_bootstrap_and_redis():
    cfg = _load({"NHOM8_KAFKA_BOOTSTRAP": "10.0.0.5:9092", "NHOM8_REDIS_HOST": "cache", "NHOM8_REDIS_PORT": "6380"})
    assert cfg.kafka.bootstrap_servers == "10.0.0.5:9092"
    assert (cfg.redis.host, cfg.redis.port) == ("cache", 6380)


def test_env_overrides_redis_db_and_results_dir(tmp_path):
    cfg = _load({"NHOM8_REDIS_DB": "9", "NHOM8_RESULTS_DIR": str(tmp_path)})
    assert cfg.redis.db == 9
    assert cfg.paths.results == tmp_path
    with pytest.raises(ConfigError):
        _load({"NHOM8_REDIS_DB": "-1"})


def test_redis_url_requires_password_and_hides_it_in_repr():
    with pytest.raises(ConfigError):
        _ = _load().redis.url
    cfg = _load({"REDIS_PASSWORD": "p@ss/word"})
    assert cfg.redis.url == "redis://:p%40ss%2Fword@127.0.0.1:6379/0"
    assert "p@ss" not in repr(cfg)
    assert "***" in cfg.redis.redacted_url


def test_dotenv_is_read_and_environment_wins(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("# comment\nREDIS_PASSWORD='from-file'\nexport NHOM8_KAFKA_PARTITIONS=1\n", encoding="utf-8")
    assert read_dotenv(dotenv) == {"REDIS_PASSWORD": "from-file", "NHOM8_KAFKA_PARTITIONS": "1"}
    cfg = load_config(env={"NHOM8_KAFKA_PARTITIONS": "6"}, dotenv_path=dotenv)
    assert cfg.redis.password == "from-file"
    assert cfg.kafka.partitions == 6


def test_malformed_dotenv_line_rejected(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("NOT A PAIR\n", encoding="utf-8")
    with pytest.raises(ConfigError):
        read_dotenv(dotenv)


@pytest.mark.parametrize("value", ["0", "-1", "three", "1.5", ""])
def test_invalid_partitions_rejected(value):
    with pytest.raises(ConfigError):
        _load({"NHOM8_KAFKA_PARTITIONS": value})


def test_invalid_environment_rejected():
    with pytest.raises(ConfigError):
        _load({"NHOM8_ENV": "prod"})


@pytest.mark.parametrize("run_id", ["../etc", "a/b", "", ".hidden", "a..b", "x" * 200])
def test_unsafe_run_id_rejected(run_id):
    with pytest.raises(ConfigError):
        validate_run_id(run_id)


def test_missing_topic_rejected(tmp_path):
    text = DEFAULT_CONFIG_PATH.read_text(encoding="utf-8").replace("hint_requests: hint-requests", "")
    path = tmp_path / "runtime.yaml"
    path.write_text(text, encoding="utf-8")
    with pytest.raises(ConfigError):
        _load(path=path)


def test_missing_config_file_rejected(tmp_path):
    with pytest.raises(ConfigError):
        _load(path=tmp_path / "missing.yaml")
