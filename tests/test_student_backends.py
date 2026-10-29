"""Trang Học sinh, phần I/O không cần mạng: cấu hình, client gợi ý (qua dịch vụ gợi ý thật bằng TestClient và
qua lỗi truyền tải), producer sự kiện (producer giả của librdkafka), roster trên fakeredis, giao diện AppTest.
"""

from __future__ import annotations

import json
from pathlib import Path

import fakeredis
import httpx
import pytest
import redis
import requests
import streamlit as st
from fastapi.testclient import TestClient
from streamlit import config as st_config
from streamlit.testing.v1 import AppTest

from ai_math_tutor.config import ConfigError
from ai_math_tutor.hints.app import create_app
from ai_math_tutor.hints.generator import HintEngine
from ai_math_tutor.safe_math import SafeMath
from ai_math_tutor.streaming.sinks import RedisKeys
from ai_math_tutor.student import app, backends, practice
from test_hint_service import API_KEY, make_service
from test_hint_verify import synthetic_bank
from test_replay_schedule import FakeKafkaProducer

T0 = 1_793_000_000_000
APP_PATH = Path(app.__file__)
PREFIX = "st-test:"


# ---------------------------------------------------------------- cấu hình


def test_settings_defaults_overrides_and_validation(tmp_path):
    defaults = backends.load_settings("learner-events", env={}, dotenv_path=None)
    assert (defaults.hint_url, defaults.events_topic, defaults.key_prefix, defaults.api_key) == (
        "http://127.0.0.1:8000", "learner-events", "", None)
    dotenv = tmp_path / ".env"
    dotenv.write_text("NHOM8_HINT_API_KEY=from-file-0123456789\n", encoding="utf-8")
    custom = backends.load_settings("learner-events", env={
        "NHOM8_HINT_URL": "http://hint-api:8000/", "NHOM8_STUDENT_EVENTS_TOPIC": "demo-learner-events",
        "NHOM8_STUDENT_KEY_PREFIX": "demo:"}, dotenv_path=dotenv)
    assert (custom.hint_url, custom.events_topic, custom.key_prefix) == ("http://hint-api:8000", "demo-learner-events",
                                                                         "demo:")
    assert custom.api_key == "from-file-0123456789" and "from-file" not in repr(custom)
    for env in ({"NHOM8_HINT_URL": "ftp://x"}, {"NHOM8_HINT_URL": "http://"}, {"NHOM8_HINT_URL": "http://h/?a=1"},
                {"NHOM8_STUDENT_EVENTS_TOPIC": "bad topic"}, {"NHOM8_STUDENT_KEY_PREFIX": "a b"}):
        with pytest.raises(ConfigError):
            backends.load_settings("learner-events", env=env, dotenv_path=None)


# ---------------------------------------------------------------- client gợi ý


class ForwardToApp(requests.adapters.BaseAdapter):
    """Chuyển yêu cầu của `requests` tới app ASGI thật (TestClient), không mở socket."""

    def __init__(self, test_client: TestClient) -> None:
        super().__init__()
        self.test_client = test_client

    def send(self, request, **kwargs):
        reply = self.test_client.request(request.method, httpx.URL(request.url).path, content=request.body,
                                         headers=dict(request.headers))
        response = requests.Response()
        response.status_code, response._content, response.url = reply.status_code, reply.content, request.url
        response.headers.update(reply.headers)
        response.request = request
        return response

    def close(self) -> None:
        return None


class Broken(requests.adapters.BaseAdapter):
    def __init__(self, exc: Exception | None = None, status: int = 200, content: bytes = b"") -> None:
        super().__init__()
        self.exc, self.status, self.content = exc, status, content

    def send(self, request, **kwargs):
        if self.exc is not None:
            raise self.exc
        response = requests.Response()
        response.status_code, response._content, response.request = self.status, self.content, request
        return response

    def close(self) -> None:
        return None


def hint_client(adapter: requests.adapters.BaseAdapter, api_key: str | None = API_KEY) -> backends.HintClient:
    session = requests.Session()
    session.mount("http://", adapter)
    return backends.HintClient("http://hint.test:8000", api_key, session=session)


@pytest.fixture(scope="module")
def engine():
    return HintEngine.from_bank(synthetic_bank())


@pytest.fixture(scope="module")
def checker():
    with SafeMath(timeout_s=5.0, max_pending=8, workers=1) as instance:
        yield instance


def envelope_for(problem_id: str = "P-TLIN-001") -> dict:
    learner = practice.LearnerSession.start("C-SIM-0001", "L-00000000000000c3")
    attempt, _ = practice.open_problem(learner, synthetic_bank().get(problem_id), T0)
    return practice.hint_envelope(learner, attempt, T0 + 1)


def test_hint_client_gets_a_verified_hint_and_maps_auth_and_rate_limit(engine, checker):
    with TestClient(create_app(make_service(engine, checker, burst=1))) as test_client:
        adapter = ForwardToApp(test_client)
        served = hint_client(adapter).request(envelope_for())
        assert served.status == 200 and served.hint is not None and served.hint.verified, served.message
        limited = hint_client(adapter).request(envelope_for())  # burst = 1 cho cùng learner_id
        assert limited.status == 429 and limited.hint is None and "giây" in limited.message
        denied = hint_client(adapter, api_key="wrong-key-0123456789").request(envelope_for())
        assert denied.status == 401 and "HTTP 401" in denied.message and "wrong-key" not in denied.message
        unknown = envelope_for()
        unknown["problem_id"] = "P-NONE-001"
        rejected = hint_client(adapter).request(unknown)
        assert rejected.status == 422 and "unknown_problem" in rejected.message


@pytest.mark.parametrize("adapter, expected", [
    (Broken(requests.ConnectionError("refused")), "Không kết nối được"),
    (Broken(requests.ReadTimeout("slow")), "không trả lời"),
    (Broken(status=503, content=b'{"error": "x"}'), "HTTP 503"),
    (Broken(status=200, content=b"not json"), "sai định dạng"),
    (Broken(status=418), "HTTP 418"),
])
def test_hint_client_reports_transport_and_server_failures(adapter, expected):
    outcome = hint_client(adapter).request(envelope_for())
    assert outcome.hint is None and expected in outcome.message and API_KEY not in outcome.message


def test_hint_client_refuses_without_key_or_with_a_broken_envelope():
    called = Broken(AssertionError("không được gửi"))
    assert "NHOM8_HINT_API_KEY" in hint_client(called, api_key=None).request(envelope_for()).message
    bad = envelope_for()
    bad["step_index"] = 0
    assert "sai hợp đồng" in hint_client(called).request(bad).message


# ---------------------------------------------------------------- Kafka


def encoded_events(count: int) -> list[tuple[bytes, bytes]]:
    learner = practice.LearnerSession.start("C-SIM-0001", "L-00000000000000c3")
    problem = synthetic_bank().get("P-TLIN-001")
    return [practice.encode_event(practice.open_problem(learner, problem, T0 + i)[1], T0 + i) for i in range(count)]


def test_publisher_sends_keyed_events_and_waits_for_acks():
    fakes: list[FakeKafkaProducer] = []
    publisher = backends.EventPublisher("127.0.0.1:9092", "demo-learner-events",
                                        producer_factory=lambda c: fakes.append(FakeKafkaProducer(c, capacity=10)) or fakes[-1])
    messages = encoded_events(3)
    outcome = publisher.publish(messages)
    assert outcome.ok and outcome.delivered == 3
    produced = fakes[0].produced
    assert [(t, k, v) for t, k, v, _ in produced] == [("demo-learner-events", k, v) for k, v in messages]
    config = fakes[0].config
    assert config["enable.idempotence"] is True and config["delivery.timeout.ms"] == backends.DELIVERY_TIMEOUT_MS
    assert publisher.publish([]).ok


def test_publisher_reports_undelivered_events():
    down = backends.EventPublisher("127.0.0.1:9092", "t", producer_factory=lambda c: FakeKafkaProducer(
        c, capacity=10, broker_up_after_polls=10**9))
    outcome = down.publish(encoded_events(2), flush_timeout_s=0)
    assert not outcome.ok and outcome.delivered == 0 and "2/2" in outcome.message


# ---------------------------------------------------------------- roster


def test_register_learner_adds_to_an_existing_roster_only():
    client = fakeredis.FakeRedis(server=fakeredis.FakeServer())
    keys = RedisKeys(PREFIX)
    client.sadd(keys.roster("C-SIM-0001"), "L-1111111111111111")
    backends.register_learner(client, keys, "C-SIM-0001", "L-00000000000000c3")
    assert client.smembers(keys.roster("C-SIM-0001")) == {b"L-1111111111111111", b"L-00000000000000c3"}
    assert backends.list_classes(client, keys) == ["C-SIM-0001"]
    with pytest.raises(ValueError):
        backends.register_learner(client, keys, "C-SIM-0099", "L-00000000000000c3")
    assert not client.exists(keys.roster("C-SIM-0099"))


# ---------------------------------------------------------------- giao diện


SECRET = "page-secret-key-0123456789"


@pytest.fixture
def page(monkeypatch):
    fake_server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=fake_server)
    monkeypatch.setattr(redis.Redis, "from_url", classmethod(lambda cls, url, **kw: fakeredis.FakeRedis(server=fake_server)))
    for name in ("NHOM8_ENV", "NHOM8_HINT_URL", "NHOM8_STUDENT_EVENTS_TOPIC"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("REDIS_PASSWORD", "unit-test")
    monkeypatch.setenv("NHOM8_STUDENT_KEY_PREFIX", PREFIX)
    monkeypatch.setenv("NHOM8_HINT_API_KEY", SECRET)
    st.cache_resource.clear()
    st_config.set_option("server.address", "127.0.0.1")
    yield client
    st.cache_resource.clear()
    st_config.set_option("server.address", None)


def _texts(at: AppTest) -> str:
    parts: list[str] = []

    def walk(node) -> None:
        proto = getattr(node, "proto", None)
        if proto is not None:
            parts.append(str(proto))
        for child in getattr(node, "children", {}).values():
            walk(child)

    walk(at._tree)
    return "\n".join(parts)


def _run(at: AppTest | None = None) -> AppTest:
    at = at or AppTest.from_file(str(APP_PATH), default_timeout=90)
    at.run()
    assert not at.exception, at.exception
    return at


def test_page_without_roster_asks_to_run_the_demo(page):
    rendered = _texts(_run())
    assert "Chưa có lớp nào trong roster" in rendered and SECRET not in rendered


def test_page_registers_the_pseudonym_in_the_chosen_class_roster(page):
    keys = RedisKeys(PREFIX)
    page.sadd(keys.roster("C-SIM-0002"), "L-1111111111111111")
    at = _run()
    assert at.selectbox[0].options == ["C-SIM-0002"]
    start = next(b for b in at.button if b.label == "Bắt đầu")
    start.click()
    at = _run(at)
    members = {m.decode() for m in page.smembers(keys.roster("C-SIM-0002"))}
    learner = at.session_state["learner"]
    assert learner.learner_id in members and len(members) == 2 and learner.class_id == "C-SIM-0002"
    rendered = _texts(at)
    assert "Chọn bài" in rendered and learner.learner_id in rendered and SECRET not in rendered


def test_bind_guard_blocks_non_loopback_on_host(page):
    st_config.set_option("server.address", "0.0.0.0")
    assert "chỉ được nghe trên 127.0.0.1" in _texts(_run())


def test_plain_escapes_markdown_and_latex():
    assert app.plain("2*x*y = $5") == r"2\*x\*y = \$5"
    assert json.dumps(app.plain("a_b")) == json.dumps(r"a\_b")


def test_inline_math_dollars_are_dropped_but_currency_kept():
    assert app.strip_inline_math("Solve for $x$: $6x - 4x = 380 - 10(x + 2)$.") == \
        "Solve for x: 6x - 4x = 380 - 10(x + 2)."
    assert app.strip_inline_math("A pen costs $5 and a book costs $10.") == "A pen costs $5 and a book costs $10."
    assert app.strip_inline_math("Price is $3.50, tax $0.20 total") == "Price is $3.50, tax $0.20 total"
