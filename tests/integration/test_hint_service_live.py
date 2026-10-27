"""Dịch vụ gợi ý trên Redis và Kafka thật (make up): trạng thái pipeline, hintctx/lasthint có TTL,
nhật ký `hint-requests`, Redis không tới được → fail-closed.

Redis dùng chung: mọi khóa dưới tiền tố riêng `it-hint-<uuid>:` và chỉ xóa khóa của mình (không FLUSHDB).
Topic `hint-requests` dùng chung: chỉ đọc từ offset cuối lúc bắt đầu và lọc theo `event_id` của test.
"""

from __future__ import annotations

import json
import time
import uuid

import pytest
from fastapi.testclient import TestClient

from ai_math_tutor.config import load_config
from ai_math_tutor.hints.app import HintService, create_app
from ai_math_tutor.hints.audit_log import KafkaAuditSink
from ai_math_tutor.hints.auth import API_KEY_HEADER, ApiKeyAuth, RateLimiter
from ai_math_tutor.hints.generator import HintEngine
from ai_math_tutor.hints.state_client import HINTCTX_TTL_S, HintKeys, HintStateStore, redis_client
from ai_math_tutor.safe_math import SafeMath
from ai_math_tutor.streaming.sinks import RedisKeys, progress_pairs, state_value
from ai_math_tutor.problem_bank import ProblemBank, parse_problem

pytestmark = pytest.mark.integration
API_KEY = "it-key-0123456789abcdef"
CLASS = "C-10A1-2026"
SKILL = "linear_equation.isolate_variable"
_COMMON = {"source": "self", "kind": "equation", "skill_ids": [SKILL], "difficulty": 1, "solution_source": "authored"}
BANK_ROWS = [
    {**_COMMON, "problem_id": "P-TLIN-001", "statement": "Giải 2x + 3 = 11.", "equation": "2*x + 3 = 11",
     "answer": "4", "steps": ["2*x = 8", "x = 4"], "split": "test",
     "misconceptions": [{"type": "sign_error", "step_index": 1, "expression": "2*x = 14"}]},
    {**_COMMON, "problem_id": "P-DLIN-001", "statement": "Giải 3x + 5 = 20.", "equation": "3*x + 5 = 20",
     "answer": "5", "steps": ["3*x = 15", "x = 5"], "split": "dev",
     "misconceptions": [{"type": "sign_error", "step_index": 1, "expression": "3*x = 25"}]},
]


def envelope(**override) -> dict:
    body = {"event_id": str(uuid.uuid4()), "learner_id": "L-" + uuid.uuid4().hex[:16], "class_id": CLASS,
            "session_id": "S-1234abcd", "session_seq": 1, "problem_id": "P-TLIN-001", "skill_ids": [SKILL],
            "step_index": 1, "event_time": "2026-10-27T08:00:00.000Z", "sent_time": "2026-10-27T08:00:00.200Z",
            "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"}}
    body.update(override)
    return body


@pytest.fixture(scope="module")
def cfg():
    return load_config()


@pytest.fixture(scope="module")
def redis(cfg):
    try:
        client = redis_client(cfg.redis.url, socket_timeout_s=5.0)
        client.ping()
    except Exception as exc:  # stack chưa up hoặc thiếu REDIS_PASSWORD
        pytest.skip(f"Redis không sẵn sàng ({cfg.redis.redacted_url}): {exc}")
    yield client
    client.close()


@pytest.fixture
def prefix(redis):
    value = f"it-hint-{uuid.uuid4().hex[:10]}:"
    yield value
    keys = list(redis.scan_iter(match=f"{value}*", count=1000))
    if keys:
        redis.delete(*keys)


@pytest.fixture(scope="module")
def engine():
    return HintEngine.from_bank(ProblemBank(parse_problem(row) for row in BANK_ROWS))


@pytest.fixture(scope="module")
def checker():
    with SafeMath(timeout_s=2.0, workers=1) as instance:
        yield instance


def _service(engine, checker, store, audit) -> HintService:
    return HintService(engine, checker, store, ApiKeyAuth(API_KEY), RateLimiter(rate_per_min=600, burst=50), audit)


class _NoAudit:
    def send(self, record) -> None:
        return None

    def close(self) -> None:
        return None

    def stats(self) -> dict[str, int]:
        return {}


def test_state_from_pipeline_format_drives_levels_and_keys_expire(engine, checker, redis, prefix):
    keys = RedisKeys(prefix)
    learner = "L-" + uuid.uuid4().hex[:16]
    row = {"class_id": CLASS, "consec_wrong": 0, "state_version": 3, "last_event_time_ms": 1}
    redis.hset(keys.state(learner), SKILL, state_value(row, 7))
    pairs = progress_pairs({"class_id": CLASS, "wrong_answers": 2, "state_version": 3}, 7)
    redis.hset(keys.progress(learner, "P-TLIN-001"), mapping=dict(zip(pairs[::2], pairs[1::2])))
    store = HintStateStore(redis, HintKeys(keys))
    with TestClient(create_app(_service(engine, checker, store, _NoAudit()))) as client:
        bodies = [client.post("/hints", json=envelope(learner_id=learner, step_index=2),
                              headers={API_KEY_HEADER: API_KEY}).json() for _ in range(3)]
    assert [b["hint_level"] for b in bodies] == [1, 2, 3]
    assert {b["state_source"] for b in bodies} == {"redis"}
    assert bodies[-1]["math"] == ["x = 4"]  # wrong_answers = 2 ⇒ được đưa bước lộ đáp án
    hk = HintKeys(keys)
    assert 0 < redis.ttl(hk.hintctx(learner, "P-TLIN-001", 2)) <= HINTCTX_TTL_S
    assert json.loads(redis.get(hk.lasthint(learner)))["hint_level"] == 3
    assert redis.ttl(hk.lasthint(learner)) > 0


def test_unreachable_redis_is_fail_closed(engine, checker):
    dead = redis_client("redis://:unused@127.0.0.1:1/0", socket_timeout_s=0.2)
    store = HintStateStore(dead, HintKeys(RedisKeys("it-dead:")))
    service = _service(engine, checker, store, _NoAudit())
    started = time.perf_counter()
    outcome = service.handle(envelope(learner_id="L-" + uuid.uuid4().hex[:16], step_index=2))
    assert outcome.status == 200 and time.perf_counter() - started < 2.0
    assert (outcome.body["hint_level"], outcome.body["state_source"]) == (1, "default")


def test_audit_record_reaches_hint_requests_topic(cfg, engine, checker, redis, prefix):
    from confluent_kafka import Consumer, TopicPartition

    topic = cfg.kafka.topics["hint_requests"]
    consumer = Consumer({"bootstrap.servers": cfg.kafka.bootstrap_servers, "group.id": f"it-hint-{uuid.uuid4().hex}",
                         "enable.auto.commit": False})
    try:
        metadata = consumer.list_topics(topic, timeout=10)
        if topic not in metadata.topics or metadata.topics[topic].error is not None:
            pytest.skip(f"topic {topic} chưa có (make topics)")
        parts = []
        for partition in metadata.topics[topic].partitions:
            _, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            parts.append(TopicPartition(topic, partition, high))
        consumer.assign(parts)
        audit = KafkaAuditSink(cfg.kafka.bootstrap_servers, topic)
        service = _service(engine, checker, HintStateStore(redis, HintKeys(RedisKeys(prefix))), audit)
        body = envelope(learner_id="L-" + uuid.uuid4().hex[:16], expression="2*x = 14")
        outcome = service.handle(body)
        service.after_response(outcome.after)
        audit.close()
        found = None
        deadline = time.monotonic() + 20
        while found is None and time.monotonic() < deadline:
            message = consumer.poll(0.5)
            if message is None or message.error():
                continue
            record = json.loads(message.value())
            if record.get("event_id") == body["event_id"]:
                found = record
        assert found is not None, "không thấy nhật ký gợi ý trên topic"
        assert found["misconception"] == "sign_error" and "expression" not in found
        assert audit.stats()["delivered"] == 1
    finally:
        consumer.close()
