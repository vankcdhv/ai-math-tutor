"""I/O của trang Học sinh: cấu hình, gọi dịch vụ gợi ý, gửi sự kiện vào Kafka, ghi bút danh vào roster Redis.

Biến môi trường (hoặc `.env`):
    NHOM8_HINT_URL              gốc URL dịch vụ gợi ý (mặc định http://127.0.0.1:8000)
    NHOM8_HINT_API_KEY          khóa API của dịch vụ gợi ý; chỉ đi trong header `X-API-Key`, không hiện, không ghi log
    NHOM8_STUDENT_EVENTS_TOPIC  topic nhận sự kiện học tập (mặc định topic `learner_events` của runtime.yaml)
    NHOM8_STUDENT_KEY_PREFIX    tiền tố khóa Redis của run (rỗng khi chạy thật; demo dùng `demo:`)
Kafka và Redis (bootstrap, host, REDIS_PASSWORD) đọc qua `ai_math_tutor.config.load_config`.
"""

from __future__ import annotations

import json
import os
import re
import threading
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

import requests
from confluent_kafka import KafkaException

from ai_math_tutor.config import DEFAULT_DOTENV_PATH, ConfigError, read_dotenv
from ai_math_tutor.dashboard.queries import discover_classes
from ai_math_tutor.hints.auth import API_KEY_ENV, API_KEY_HEADER
from ai_math_tutor.hints.envelope import envelope_errors
from ai_math_tutor.ingestion.producer import IdempotentProducer, ProducerFatalError, ProducerSettings
from ai_math_tutor.ingestion.topics import TopicSpec
from ai_math_tutor.streaming.sinks import RedisKeys
from ai_math_tutor.student.practice import Hint

HINT_URL_ENV = "NHOM8_HINT_URL"
EVENTS_TOPIC_ENV = "NHOM8_STUDENT_EVENTS_TOPIC"
KEY_PREFIX_ENV = "NHOM8_STUDENT_KEY_PREFIX"
DEFAULT_HINT_URL = "http://127.0.0.1:8000"
HINT_TIMEOUT_S = 5.0
# Producer tương tác: mỗi tin có kết quả giao (thành công hoặc lỗi) trước khi flush hết hạn, nên số đếm
# của một lượt gửi là chính xác và tin không nằm chờ trong hàng đợi qua lượt sau.
DELIVERY_TIMEOUT_MS = 8_000
FLUSH_TIMEOUT_S = 12.0
_PREFIX_RE = re.compile(r"^[A-Za-z0-9._:-]{0,64}$")


@dataclass(frozen=True)
class StudentSettings:
    hint_url: str
    events_topic: str
    key_prefix: str = ""
    api_key: str | None = field(default=None, repr=False)


def load_settings(default_topic: str, env: Mapping[str, str] | None = None,
                  dotenv_path: Path | None = DEFAULT_DOTENV_PATH) -> StudentSettings:
    """Biến môi trường > .env, giống `load_config`. Kiểm URL, tên topic và tiền tố khóa ở đây."""
    merged: dict[str, str] = read_dotenv(dotenv_path) if dotenv_path else {}
    merged.update(os.environ if env is None else env)
    url = (merged.get(HINT_URL_ENV) or DEFAULT_HINT_URL).strip().rstrip("/")
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https") or not parts.hostname or parts.query or parts.fragment:
        raise ConfigError(f"{HINT_URL_ENV} phải là URL http(s)://host[:cổng], nhận {url!r}")
    topic = (merged.get(EVENTS_TOPIC_ENV) or default_topic).strip()
    try:
        TopicSpec(topic, 1)
    except ValueError as exc:
        raise ConfigError(f"{EVENTS_TOPIC_ENV}: {exc}") from exc
    prefix = merged.get(KEY_PREFIX_ENV, "")
    if not _PREFIX_RE.match(prefix):
        raise ConfigError(f"{KEY_PREFIX_ENV} chỉ gồm [A-Za-z0-9._:-], tối đa 64 ký tự: {prefix!r}")
    return StudentSettings(hint_url=url, events_topic=topic, key_prefix=prefix,
                           api_key=merged.get(API_KEY_ENV) or None)


# --- dịch vụ gợi ý ------------------------------------------------------------------------------


@dataclass(frozen=True)
class HintOutcome:
    hint: Hint | None = None
    message: str = ""  # thông báo tiếng Việt khi không có gợi ý
    status: int | None = None


def _error_code(response: requests.Response) -> str:
    try:
        body = response.json()
    except ValueError:
        return "?"
    code = body.get("error") if isinstance(body, Mapping) else None
    return code if isinstance(code, str) and re.fullmatch(r"[a-z_]{1,40}", code) else "?"


def _retry_after(response: requests.Response) -> int:
    value = response.headers.get("Retry-After", "")
    return int(value) if value.isdigit() and 0 < int(value) <= 3600 else 1


class HintClient:
    """Gửi phong bì tới `POST /hints` và đổi mọi kết quả (kể cả mất kết nối) thành `HintOutcome`."""

    def __init__(self, base_url: str, api_key: str | None, *, session: requests.Session | None = None,
                 timeout_s: float = HINT_TIMEOUT_S) -> None:
        self._url = base_url.rstrip("/") + "/hints"
        self._api_key = api_key
        self._session = session or requests.Session()
        self._timeout_s = timeout_s

    def request(self, envelope: Mapping[str, Any]) -> HintOutcome:
        errors = envelope_errors(envelope)
        if errors:
            return HintOutcome(message=f"Yêu cầu gợi ý sai hợp đồng (lỗi của trang): {errors[0]}")
        if not self._api_key:
            return HintOutcome(message=f"Trang chưa có khóa API của dịch vụ gợi ý ({API_KEY_ENV} trong .env).")
        try:
            response = self._session.post(self._url, data=json.dumps(envelope).encode("utf-8"), timeout=self._timeout_s,
                                          headers={API_KEY_HEADER: self._api_key, "Content-Type": "application/json"})
        except requests.Timeout:
            return HintOutcome(message=f"Dịch vụ gợi ý không trả lời trong {self._timeout_s:g} s. Thử lại sau.")
        except requests.RequestException:
            return HintOutcome(message="Không kết nối được dịch vụ gợi ý. Kiểm tra dịch vụ hint-api đang chạy.")
        status = response.status_code
        if status == 200:
            try:
                return HintOutcome(hint=Hint.from_body(response.json()), status=status)
            except ValueError:
                return HintOutcome(message="Dịch vụ gợi ý trả phản hồi sai định dạng.", status=status)
        if status == 401:
            message = f"Dịch vụ gợi ý từ chối khóa API (HTTP 401). Kiểm tra {API_KEY_ENV} của trang Học sinh."
        elif status in (413, 422):
            message = f"Dịch vụ gợi ý không nhận yêu cầu này (HTTP {status}, {_error_code(response)})."
        elif status == 429:
            message = f"Bạn xin gợi ý dồn dập quá. Chờ {_retry_after(response)} giây rồi thử lại."
        elif status >= 500:
            message = f"Dịch vụ gợi ý đang gặp lỗi (HTTP {status}). Thử lại sau."
        else:
            message = f"Dịch vụ gợi ý trả mã HTTP {status} không mong đợi."
        return HintOutcome(message=message, status=status)


# --- Kafka --------------------------------------------------------------------------------------


@dataclass(frozen=True)
class PublishOutcome:
    delivered: int
    total: int
    message: str = ""

    @property
    def ok(self) -> bool:
        return self.delivered == self.total and not self.message


class EventPublisher:
    """Gửi sự kiện của trang bằng producer idempotent của dự án (key = learner_id) và chờ broker xác nhận.

    Một lượt gửi giữ khóa nên số tin đã giao/lỗi đếm được chính xác dù nhiều phiên trình duyệt dùng chung.
    Lượt chưa giao hết thì bên gọi giữ nguyên các byte và gửi lại sau; tin đã tới broker mà bị gửi lại là bản
    trùng hợp lệ (cùng event_id, cùng nội dung), pipeline khử trùng theo `event_id`.
    """

    def __init__(self, bootstrap_servers: str, topic: str, *,
                 producer_factory: Callable[[dict[str, Any]], Any] | None = None) -> None:
        settings = ProducerSettings(bootstrap_servers=bootstrap_servers, linger_ms=0,
                                    delivery_timeout_ms=DELIVERY_TIMEOUT_MS, client_id="nhom8-student")
        self.topic = topic
        self._producer = IdempotentProducer(settings, producer_factory=producer_factory)
        self._lock = threading.Lock()

    def publish(self, messages: Sequence[tuple[bytes, bytes]], flush_timeout_s: float = FLUSH_TIMEOUT_S) -> PublishOutcome:
        total = len(messages)
        if not total:
            return PublishOutcome(0, 0)
        with self._lock:
            stats = self._producer.stats
            delivered_before, errors_before = stats.delivered, stats.delivery_errors
            try:
                for key, value in messages:
                    self._producer.send(self.topic, value, key)
                remaining = self._producer.flush(flush_timeout_s)
            except (KafkaException, ProducerFatalError) as exc:
                return PublishOutcome(0, total, f"Kafka từ chối sự kiện ({type(exc).__name__}).")
            delivered = stats.delivered - delivered_before
            failed = stats.delivery_errors - errors_before
            reasons = ", ".join(sorted(stats.error_counts)) or "chưa có xác nhận"
        if delivered >= total and not failed and not remaining:
            return PublishOutcome(total, total)
        return PublishOutcome(min(delivered, total), total,
                              f"Kafka chưa nhận {total - min(delivered, total)}/{total} sự kiện "
                              f"(topic {self.topic}; {reasons}).")


# --- Redis --------------------------------------------------------------------------------------


def list_classes(client: Any, keys: RedisKeys) -> list[str]:
    """Các lớp có `roster:{class}` dưới tiền tố của run (cùng cách dashboard tìm lớp)."""
    return discover_classes(client, keys)


def register_learner(client: Any, keys: RedisKeys, class_id: str, learner_id: str) -> None:
    """Thêm bút danh vào `roster:{class}` của run, như demo-setup nạp roster.json, nhưng giữ nguyên các thành viên cũ.

    Tổng hợp lớp và dashboard chỉ đếm thành viên roster, nên người học của trang phải có mặt ở đây trước
    sự kiện đầu tiên. Chỉ nhận lớp đã có roster: không tạo lớp mới dưới tiền tố của run.
    """
    roster = keys.roster(class_id)
    if not client.exists(roster):
        raise ValueError(f"lớp {class_id} không có roster dưới tiền tố {keys.prefix!r}")
    client.sadd(roster, learner_id)
