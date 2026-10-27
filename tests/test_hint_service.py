"""Dịch vụ gợi ý qua HTTP (TestClient + fakeredis): xác thực, phong bì, giới hạn tốc độ, fail-closed.

SymPy chạy thật trong worker `safe_math`. Các test quá hạn thay `diagnose` bằng hàm quay vòng CPU
(qua `extra_functions`, đúng cơ chế mà SafeMath dành cho kiểm thử) để chứng minh worker bị giết và
yêu cầu vẫn được trả lời đúng hạn.
"""

from __future__ import annotations

import json
import threading
import time
import uuid

import fakeredis
import numpy as np
import pytest
from fastapi.testclient import TestClient

from ai_math_tutor.hints import phrases
from ai_math_tutor.hints.app import HintService, create_app
from ai_math_tutor.hints.auth import (
    API_KEY_ENV,
    API_KEY_HEADER,
    ApiKeyAuth,
    AuthConfigError,
    RateLimiter,
    api_key_from_env,
)
from ai_math_tutor.hints.envelope import MAX_BODY_BYTES, REQUIRED_FIELDS
from ai_math_tutor.hints.generator import HintEngine
from ai_math_tutor.hints.state_client import HINTCTX_TTL_S, LASTHINT_TTL_S, HintKeys, HintStateStore
from ai_math_tutor.safe_math import SafeMath
from ai_math_tutor.streaming.sinks import RedisKeys, progress_pairs, state_value
from test_hint_slow_math import ATTACK, spinning_diagnose
from test_hint_verify import synthetic_bank, synthetic_rows

API_KEY = "test-key-0123456789abcdef"
CLASS = "C-10A1-2026"
SKILL = "linear_equation.isolate_variable"
RESPONSE_KEYS = {"hint_level", "text_vi", "math", "verified", "misconception", "source", "state_source"}

class ListAuditSink:
    def __init__(self) -> None:
        self.records: list[dict] = []

    def send(self, record) -> None:
        self.records.append(dict(record))

    def close(self) -> None:
        return None

    def stats(self) -> dict[str, int]:
        return {"queued": len(self.records)}


def learner_id() -> str:
    return "L-" + uuid.uuid4().hex[:16]


def envelope(**override) -> dict:
    body = {
        "event_id": str(uuid.uuid4()),
        "learner_id": "L-00000000000000a1",
        "class_id": CLASS,
        "session_id": "S-1234abcd",
        "session_seq": 1,
        "problem_id": "P-TLIN-001",
        "skill_ids": [SKILL],
        "step_index": 1,
        "event_time": "2026-10-27T08:00:00.000Z",
        "sent_time": "2026-10-27T08:00:00.200Z",
        "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"},
    }
    body.update(override)
    return body


def put_state(client, learner: str, *, skill: str = SKILL, consec_wrong: int = 0, stuck: bool = False,
              problem: str = "P-TLIN-001", wrong_answers: int = 0, prefix: str = "") -> None:
    """Ghi trạng thái đúng định dạng pipeline streaming (`sinks.state_value`, `sinks.progress_pairs`)."""
    keys = RedisKeys(prefix)
    row = {"class_id": CLASS, "consec_wrong": consec_wrong, "stuck": stuck, "state_version": 1,
           "opportunities": 4, "correct": 1, "streak": 0, "mastered": False, "last_event_time_ms": 1}
    client.hset(keys.state(learner), skill, state_value(row, 1))
    pairs = progress_pairs({"class_id": CLASS, "wrong_answers": wrong_answers, "last_step": 1,
                            "completed": False, "state_version": 1, "last_event_time_ms": 1}, 1)
    client.hset(keys.progress(learner, problem), mapping=dict(zip(pairs[::2], pairs[1::2])))


@pytest.fixture(scope="module")
def engine():
    return HintEngine.from_bank(synthetic_bank())


@pytest.fixture(scope="module")
def checker():
    with SafeMath(timeout_s=2.0, max_pending=16, workers=2) as instance:
        yield instance


def make_service(engine, checker, redis=None, *, generator="ladder", burst=50, audit=None) -> HintService:
    store = HintStateStore(redis, HintKeys(RedisKeys("t:"))) if redis is not None else None
    return HintService(engine, checker, store, ApiKeyAuth(API_KEY), RateLimiter(rate_per_min=600, burst=burst),
                       audit or ListAuditSink(), generator=generator)


@pytest.fixture
def redis():
    return fakeredis.FakeRedis(server=fakeredis.FakeServer())


@pytest.fixture
def audit():
    return ListAuditSink()


@pytest.fixture
def client(engine, checker, redis, audit):
    with TestClient(create_app(make_service(engine, checker, redis, audit=audit))) as test_client:
        yield test_client


def service_counters(client) -> dict[str, int]:
    return client.get("/healthz").json()["counters"]


def post(client, body, key: str | None = API_KEY):
    headers = {API_KEY_HEADER: key} if key is not None else {}
    return client.post("/hints", content=json.dumps(body), headers=headers)


# --- xác thực, phong bì, giới hạn ------------------------------------------------------------


@pytest.mark.parametrize("key", [None, "", "wrong-key-0123456789"])
def test_missing_or_wrong_api_key_is_401(client, key):
    assert post(client, envelope(), key=key).status_code == 401


@pytest.mark.parametrize("placeholder", ["change-me-generate-a-random-value", "change-me-0123456789abcdef",
                                         "  CHANGE-ME-generate-a-random-value"])
def test_env_example_placeholder_key_refuses_to_start(placeholder):
    with pytest.raises(AuthConfigError, match="giá trị mẫu"):
        api_key_from_env({API_KEY_ENV: placeholder})
    with pytest.raises(AuthConfigError, match="sinh một khóa ngẫu nhiên"):
        ApiKeyAuth(placeholder)
    assert api_key_from_env({API_KEY_ENV: API_KEY}) == API_KEY
    with pytest.raises(AuthConfigError):
        api_key_from_env({API_KEY_ENV: "short"})


def test_oversized_body_is_413(client):
    body = envelope(expression="1" * 100)
    raw = json.dumps(body) + " " * MAX_BODY_BYTES
    response = client.post("/hints", content=raw, headers={API_KEY_HEADER: API_KEY})
    assert response.status_code == 413


@pytest.mark.parametrize("field", REQUIRED_FIELDS)
def test_missing_contract_field_is_422(client, field):
    body = envelope()
    del body[field]
    response = post(client, body)
    assert response.status_code == 422 and response.json()["error"] == "invalid_envelope"


@pytest.mark.parametrize("override", [
    {"extra": 1},
    {"expression": "__import__('os')"},
    {"expression": "x" * 201},
    {"step_index": 0},
    {"learner_id": "L-0000000000000000"},  # learner dành riêng cho heartbeat
    {"event_id": "not-a-uuid"},
    {"event_time": "2026-10-27 08:00:00"},
])
def test_schema_violations_are_422(client, override):
    assert post(client, envelope(**override)).status_code == 422


def test_invalid_json_is_422(client):
    response = client.post("/hints", content="{not json", headers={API_KEY_HEADER: API_KEY})
    assert response.status_code == 422 and response.json()["error"] == "invalid_json"


@pytest.mark.parametrize(("override", "code"), [
    ({"problem_id": "P-NONE-999"}, "unknown_problem"),
    ({"skill_ids": ["arithmetic.percent"]}, "skill_mismatch"),
    ({"step_index": 4}, "step_out_of_range"),
])
def test_requests_inconsistent_with_the_bank_are_422(client, override, code):
    response = post(client, envelope(**override))
    assert response.status_code == 422 and response.json()["error"] == code


def test_rate_limit_per_learner_is_429(engine, checker, redis):
    with TestClient(create_app(make_service(engine, checker, redis, burst=3))) as limited:
        learner = learner_id()
        codes = [post(limited, envelope(learner_id=learner)).status_code for _ in range(4)]
        assert codes == [200, 200, 200, 429]
        blocked = post(limited, envelope(learner_id=learner))
        assert int(blocked.headers["Retry-After"]) >= 1
        assert post(limited, envelope(learner_id=learner_id())).status_code == 200  # người học khác không bị chặn


def test_healthz(client):
    body = client.get("/healthz").json()
    assert body["status"] == "ok" and body["redis"] == "ok" and body["problems"] == len(synthetic_rows())


# --- trạng thái, thang bậc, fail-closed ------------------------------------------------------


def test_response_matches_contract_and_escalates_with_state(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:")
    levels = []
    for _ in range(3):
        response = post(client, envelope(learner_id=learner))
        assert response.status_code == 200
        body = response.json()
        assert set(body) == RESPONSE_KEYS and body["state_source"] == "redis" and body["verified"] is True
        levels.append(body["hint_level"])
    assert levels == [1, 2, 3]
    ctx_key = HintKeys(RedisKeys("t:")).hintctx(learner, "P-TLIN-001", 1)
    assert 0 < redis.ttl(ctx_key) <= HINTCTX_TTL_S


def test_client_retry_with_same_event_id_does_not_escalate(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:")
    body = envelope(learner_id=learner)
    assert [post(client, body).json()["hint_level"] for _ in range(3)] == [1, 1, 1]


def test_struggling_learner_starts_higher(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:", consec_wrong=3)
    assert post(client, envelope(learner_id=learner)).json()["hint_level"] == 2


def test_missing_state_is_fail_closed_to_level_one(client):
    learner = learner_id()  # Redis sống nhưng không có state:{learner}
    bodies = [post(client, envelope(learner_id=learner, step_index=2)).json() for _ in range(5)]
    assert {b["hint_level"] for b in bodies} == {1} and {b["state_source"] for b in bodies} == {"default"}
    assert all("x = 4" not in b["math"] and "x = 4" not in b["text_vi"] for b in bodies)


def test_redis_unavailable_is_fail_closed_to_level_one(engine, checker):
    server = fakeredis.FakeServer()
    down = fakeredis.FakeRedis(server=server)
    learner = learner_id()
    put_state(down, learner, prefix="t:", wrong_answers=5, consec_wrong=5)
    server.connected = False
    service = make_service(engine, checker, down)
    with TestClient(create_app(service)) as test_client:
        for _ in range(4):
            response = post(test_client, envelope(learner_id=learner, step_index=2))
            assert response.status_code == 200
            body = response.json()
            assert (body["hint_level"], body["state_source"], body["verified"]) == (1, "default", True)
            assert "x = 4" not in body["text_vi"]
        assert test_client.get("/healthz").json()["redis"] == "unavailable"
    assert service.counters["lasthint_errors"] == 4


def test_answer_is_withheld_until_two_wrong_final_answers(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:", wrong_answers=1)
    bodies = [post(client, envelope(learner_id=learner, step_index=2)).json() for _ in range(3)]
    assert (bodies[-1]["hint_level"], bodies[-1]["source"], bodies[-1]["math"]) == (3, "ladder", [])
    assert "x = □" in bodies[-1]["text_vi"]
    assert all("x = 4" not in b["math"] and "x = 4" not in b["text_vi"] for b in bodies)
    put_state(redis, learner, prefix="t:", wrong_answers=2)
    unlocked = post(client, envelope(learner_id=learner, step_index=2)).json()
    assert (unlocked["hint_level"], unlocked["source"], unlocked["math"]) == (3, "ladder", ["x = 4"])


def test_misconception_is_diagnosed_in_safe_math(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:")
    body = post(client, envelope(learner_id=learner, expression="2*x = 14")).json()
    assert body["misconception"] == "sign_error" and phrases.phrase("equation", 1, "sign_error") in body["text_vi"]
    invalid = post(client, envelope(learner_id=learner_id(), expression=ATTACK)).json()
    assert invalid["misconception"] is None and "chưa đọc được" in invalid["text_vi"]


def test_expression_in_the_problems_own_variable_is_diagnosed(client, redis):
    learner = learner_id()
    put_state(redis, learner, prefix="t:", problem="P-TLIN-003")
    body = envelope(learner_id=learner, problem_id="P-TLIN-003", expression="3*y = 5")
    hint = post(client, body).json()
    assert hint["misconception"] == "sign_error" and "y" in hint["text_vi"]
    correct = post(client, envelope(learner_id=learner_id(), problem_id="P-TLIN-003", expression="3y = 9")).json()
    assert "Bước em vừa viết là đúng" in correct["text_vi"]


def test_lasthint_and_audit_record_are_written_after_response(client, redis, audit):
    learner = learner_id()
    put_state(redis, learner, prefix="t:")
    body = envelope(learner_id=learner, expression="2*x = 14")
    hint = post(client, body).json()
    key = HintKeys(RedisKeys("t:")).lasthint(learner)
    stored = json.loads(redis.get(key))
    assert stored["event_id"] == body["event_id"] and stored["text_vi"] == hint["text_vi"]
    assert 0 < redis.ttl(key) <= LASTHINT_TTL_S
    record = next(r for r in audit.records if r["event_id"] == body["event_id"])
    assert record["hint_level"] == hint["hint_level"] and record["state_source"] == "redis"
    assert "expression" not in record and "text_vi" not in record and record["latency_ms"] >= 0


def test_template_generator_can_be_selected(engine, checker, redis):
    with TestClient(create_app(make_service(engine, checker, redis, generator="template"))) as test_client:
        learner = learner_id()
        put_state(redis, learner, prefix="t:")
        sources = {post(test_client, envelope(learner_id=learner)).json()["source"] for _ in range(3)}
        assert sources == {"template"}


# --- SymPy quá hạn, quá tải, biểu thức tấn công ------------------------------------------------


@pytest.fixture(scope="module")
def spinning_checker():
    with SafeMath(timeout_s=0.3, max_pending=16, workers=2, extra_functions={"diagnose": spinning_diagnose}) as inst:
        inst.call("diagnose", synthetic_bank().problems[0].to_dict(), "x = 4", None)  # khởi động sẵn
        yield inst


def test_sympy_timeout_returns_safe_hint_on_time(engine, spinning_checker, redis):
    service = make_service(engine, spinning_checker, redis)
    learner = learner_id()
    put_state(redis, learner, prefix="t:")
    started = time.perf_counter()
    outcome = service.handle(envelope(learner_id=learner, expression=ATTACK))
    elapsed = time.perf_counter() - started
    assert outcome.status == 200 and outcome.body["verified"] is True and outcome.body["misconception"] is None
    assert "chưa kiểm được" in outcome.body["text_vi"]
    # Không đo sát đồng hồ (máy dùng chung, CPU bận): chỉ cần trả gợi ý an toàn thay vì treo vô hạn.
    # SLO 300 ms được đo riêng bằng tải thật (scripts/hint-load-test.py).
    assert elapsed < 10.0
    assert service.counters["diagnose_timeouts"] == 1


def test_full_math_queue_is_429(engine):
    with SafeMath(timeout_s=1.0, max_pending=1, workers=1, extra_functions={"diagnose": spinning_diagnose}) as one:
        service = make_service(engine, one, fakeredis.FakeRedis(server=fakeredis.FakeServer()))
        attacker = threading.Thread(target=service.handle, args=(envelope(learner_id=learner_id(), expression=ATTACK),))
        attacker.start()
        time.sleep(0.2)
        outcome = service.handle(envelope(learner_id=learner_id(), expression="2*x = 8"))
        attacker.join()
        assert outcome.status == 429 and outcome.body["error"] == "math_overloaded" and outcome.headers["Retry-After"]


def test_real_nested_power_is_rejected_cheaply(client):
    started = time.perf_counter()
    response = post(client, envelope(learner_id=learner_id(), expression=ATTACK))
    assert response.status_code == 200 and time.perf_counter() - started < 10.0
    assert service_counters(client)["diagnose_timeouts"] == 0  # bị từ chối ở bộ lọc, không phải quá hạn


def _latencies(service: HintService, n: int, offset: int) -> np.ndarray:
    values = []
    for i in range(n):
        body = envelope(learner_id=learner_id(), expression=f"2*x = {offset + i}")  # biểu thức mới: không trúng cache
        started = time.perf_counter()
        assert service.handle(body).status == 200
        values.append(time.perf_counter() - started)
    return np.array(values)


def test_attack_expressions_do_not_delay_other_requests_beyond_the_timeout(engine, spinning_checker, redis):
    service = make_service(engine, spinning_checker, redis, burst=1000)
    baseline_p95 = float(np.percentile(_latencies(service, 30, 100), 95))
    stop = threading.Event()

    def attack() -> None:
        while not stop.is_set():
            service.handle(envelope(learner_id=learner_id(), expression=ATTACK))

    attacker = threading.Thread(target=attack)
    attacker.start()
    try:
        time.sleep(0.05)
        under_attack = _latencies(service, 30, 200)
    finally:
        stop.set()
        attacker.join()
    assert service.counters["diagnose_timeouts"] >= 1
    assert float(np.percentile(under_attack, 95)) <= baseline_p95 + spinning_checker.timeout_s
