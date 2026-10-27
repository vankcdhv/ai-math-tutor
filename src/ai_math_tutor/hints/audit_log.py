"""Nhật ký kiểm toán của dịch vụ gợi ý trên topic Kafka `hint-requests` — bất đồng bộ, không chặn.

Dịch vụ **không** ghi vào `learner-events` (luồng sự kiện học tập là việc của client) và nhật ký này
không vào đối soát. Mỗi bản ghi: `event_id, learner_id, class_id, problem_id, step_index, hint_level,
source, verified, misconception, state_source, latency_ms, served_ms, generator` — không có body gốc
hay `expression` của người học, không có `text_vi`.

`produce()` của librdkafka chỉ đưa tin vào hàng đợi cục bộ; một luồng nền gọi `poll()` để phục vụ
callback. Hàng đợi đầy (broker chết lâu) → bỏ tin và đếm `dropped` thay vì chặn yêu cầu gợi ý.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Mapping
from typing import Any, Protocol

from ai_math_tutor.ingestion.producer import ProducerSettings

LOGGER = logging.getLogger(__name__)

AUDIT_QUEUE_MAX_MESSAGES = 10_000
AUDIT_DELIVERY_TIMEOUT_MS = 30_000
_POLL_INTERVAL_S = 0.1


class AuditSink(Protocol):
    def send(self, record: Mapping[str, Any]) -> None: ...
    def close(self) -> None: ...
    def stats(self) -> dict[str, int]: ...


class NullAuditSink:
    """Tắt nhật ký (`--no-audit-log`): chỉ đếm."""

    def __init__(self) -> None:
        self._sent = 0

    def send(self, record: Mapping[str, Any]) -> None:
        self._sent += 1

    def close(self) -> None:
        return None

    def stats(self) -> dict[str, int]:
        return {"queued": 0, "skipped": self._sent, "delivered": 0, "errors": 0, "dropped": 0}


class KafkaAuditSink:
    def __init__(self, bootstrap_servers: str, topic: str, *,
                 producer_factory: Callable[[dict[str, Any]], Any] | None = None) -> None:
        settings = ProducerSettings(bootstrap_servers=bootstrap_servers, linger_ms=5.0, batch_size=65_536,
                                    queue_max_messages=AUDIT_QUEUE_MAX_MESSAGES,
                                    delivery_timeout_ms=AUDIT_DELIVERY_TIMEOUT_MS, client_id="nhom8-hint-api")
        config = settings.to_config()
        config["error_cb"] = self._on_error
        if producer_factory is None:
            from confluent_kafka import Producer

            producer_factory = Producer
        self._producer = producer_factory(config)
        self._topic = topic
        self._lock = threading.Lock()
        self._counts = {"queued": 0, "delivered": 0, "errors": 0, "dropped": 0}
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._poll_loop, name="hint-audit-poll", daemon=True)
        self._thread.start()

    def _bump(self, name: str) -> None:
        with self._lock:
            self._counts[name] += 1

    def _on_error(self, error: Any) -> None:
        LOGGER.warning("Kafka (hint-requests): %s", error)

    def _on_delivery(self, error: Any, _message: Any) -> None:
        self._bump("errors" if error is not None else "delivered")

    def _poll_loop(self) -> None:
        while not self._stop.is_set():
            try:
                self._producer.poll(_POLL_INTERVAL_S)
            except Exception as exc:  # noqa: BLE001 — luồng nền không được chết vì một callback lỗi
                LOGGER.warning("poll nhật ký gợi ý lỗi: %s", type(exc).__name__)

    def send(self, record: Mapping[str, Any]) -> None:
        value = json.dumps(dict(record), ensure_ascii=False, separators=(",", ":")).encode("utf-8")
        key = str(record.get("learner_id", "")).encode("utf-8") or None
        try:
            self._producer.produce(self._topic, value=value, key=key, on_delivery=self._on_delivery)
        except BufferError:
            self._bump("dropped")
            return
        except Exception as exc:  # noqa: BLE001 — nhật ký kiểm toán không được làm hỏng yêu cầu gợi ý
            LOGGER.warning("Không gửi được nhật ký gợi ý: %s", type(exc).__name__)
            self._bump("dropped")
            return
        self._bump("queued")

    def close(self, timeout_s: float = 2.0) -> None:
        self._stop.set()
        self._thread.join(timeout=1.0)
        try:
            remaining = self._producer.flush(timeout_s)
        except Exception as exc:  # noqa: BLE001
            LOGGER.warning("flush nhật ký gợi ý lỗi: %s", type(exc).__name__)
            return
        if remaining:
            LOGGER.warning("%d bản ghi nhật ký gợi ý chưa giao khi dừng", remaining)

    def stats(self) -> dict[str, int]:
        with self._lock:
            return dict(self._counts)
