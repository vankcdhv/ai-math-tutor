"""Producer Kafka idempotent cho bộ phát lại: cấu hình, đếm lỗi giao, backpressure khi hàng đợi đầy.

Khái niệm chính (confluent-kafka 2.16 / librdkafka):
- `enable.idempotence=true`: broker gán producer id + số thứ tự cho từng batch ⇒ gửi lại do lỗi mạng
  không sinh bản ghi trùng và thứ tự trong partition được giữ (librdkafka tự đặt
  `max.in.flight=5`, `retries=INT32_MAX`). Mặc định của thư viện là tắt nên phải bật tường minh.
- `acks=all`: chỉ coi là đã giao khi broker ghi xong (bắt buộc khi bật idempotence).
- `linger.ms`, `batch.size`, `compression.type`: gom tin thành batch — đánh đổi độ trễ lấy thông lượng.
- `produce()` chỉ đưa tin vào hàng đợi cục bộ (mặc định 100.000 tin). Khi broker dừng, hàng đợi đầy
  và `produce()` ném `BufferError`; ở đây gọi `poll()` + chờ lùi dần (backpressure) rồi thử lại —
  không bao giờ bỏ tin. Thời gian bị chặn được cộng dồn để bộ phát lại ghi độ trượt lịch.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import asdict, dataclass, field
from typing import Any, Protocol

COMPRESSION_TYPES = ("none", "gzip", "snappy", "lz4", "zstd")
PARTITIONERS = ("murmur2_random", "murmur2", "consistent_random", "consistent", "fnv1a_random", "random")
DEFAULT_LINGER_MS = 5.0
DEFAULT_BATCH_SIZE = 1_000_000
DEFAULT_COMPRESSION = "lz4"
DEFAULT_QUEUE_MAX_MESSAGES = 100_000
DEFAULT_DELIVERY_TIMEOUT_MS = 300_000
# Giữ tối đa bấy nhiêu thông báo lỗi mẫu để báo cáo không phình theo số lỗi.
MAX_ERROR_SAMPLES = 20


class ProducerFatalError(RuntimeError):
    """librdkafka báo lỗi fatal (producer idempotent không thể tiếp tục an toàn)."""


class ProducerStopped(RuntimeError):
    """Có yêu cầu dừng trong lúc đang chờ hàng đợi có chỗ (backpressure)."""


@dataclass(frozen=True)
class ProducerSettings:
    bootstrap_servers: str
    linger_ms: float = DEFAULT_LINGER_MS
    batch_size: int = DEFAULT_BATCH_SIZE
    compression: str = DEFAULT_COMPRESSION
    queue_max_messages: int = DEFAULT_QUEUE_MAX_MESSAGES
    delivery_timeout_ms: int = DEFAULT_DELIVERY_TIMEOUT_MS
    # murmur2 giống partitioner mặc định của client Java ⇒ cùng key → cùng partition với mọi client.
    partitioner: str = "murmur2_random"
    client_id: str = "nhom8-replay"

    def __post_init__(self) -> None:
        if not isinstance(self.bootstrap_servers, str) or not self.bootstrap_servers.strip():
            raise ValueError("bootstrap_servers không được rỗng")
        if isinstance(self.linger_ms, bool) or not isinstance(self.linger_ms, (int, float)) \
                or not 0 <= self.linger_ms <= 900_000:
            raise ValueError(f"linger_ms phải trong [0, 900000], nhận {self.linger_ms!r}")
        for name, minimum in (("batch_size", 1), ("queue_max_messages", 1), ("delivery_timeout_ms", 1)):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < minimum:
                raise ValueError(f"{name} phải là số nguyên ≥ {minimum}, nhận {value!r}")
        if self.compression not in COMPRESSION_TYPES:
            raise ValueError(f"compression phải thuộc {COMPRESSION_TYPES}, nhận {self.compression!r}")
        if self.partitioner not in PARTITIONERS:
            raise ValueError(f"partitioner phải thuộc {PARTITIONERS}, nhận {self.partitioner!r}")

    def to_config(self) -> dict[str, Any]:
        """Cấu hình librdkafka đầy đủ (cũng ghi vào kết quả replay để tái lập)."""
        return {
            "bootstrap.servers": self.bootstrap_servers,
            "client.id": self.client_id,
            "enable.idempotence": True,
            "acks": "all",
            "linger.ms": self.linger_ms,
            "batch.size": self.batch_size,
            "compression.type": self.compression,
            "queue.buffering.max.messages": self.queue_max_messages,
            "delivery.timeout.ms": self.delivery_timeout_ms,
            "partitioner": self.partitioner,
        }


@dataclass
class ProducerStats:
    produced: int = 0
    delivered: int = 0
    delivery_errors: int = 0
    buffer_full_events: int = 0
    backpressure_s: float = 0.0
    last_ack_epoch_ms: int | None = None
    error_counts: dict[str, int] = field(default_factory=dict)
    error_samples: list[str] = field(default_factory=list)
    # Lỗi cấp client (mất kết nối broker...) — librdkafka tự thử lại, chỉ ghi nhận.
    client_errors: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return asdict(self)


class _KafkaProducer(Protocol):
    def produce(self, topic: str, value: bytes | None = ..., key: bytes | None = ..., **kwargs: Any) -> None: ...
    def poll(self, timeout: float = ...) -> int: ...
    def flush(self, timeout: float = ...) -> int: ...
    def list_topics(self, topic: str | None = ..., timeout: float = ...) -> Any: ...


def _default_factory(config: dict[str, Any]) -> _KafkaProducer:
    from confluent_kafka import Producer

    return Producer(config)


class IdempotentProducer:
    """Bọc `confluent_kafka.Producer`: gửi có backpressure, đếm lỗi giao qua callback, flush khi dừng.

    `producer_factory` nhận dict cấu hình (đã gồm `error_cb`) và trả đối tượng có API của
    `confluent_kafka.Producer`; mặc định tạo producer thật.
    `should_stop`: được hỏi trong lúc chờ backpressure; True ⇒ ném `ProducerStopped`.
    """

    def __init__(
        self,
        settings: ProducerSettings,
        *,
        producer_factory: Callable[[dict[str, Any]], _KafkaProducer] | None = None,
        should_stop: Callable[[], bool] | None = None,
        backoff_initial_s: float = 0.005,
        backoff_max_s: float = 0.25,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        if not 0 < backoff_initial_s <= backoff_max_s:
            raise ValueError("cần 0 < backoff_initial_s ≤ backoff_max_s")
        self.settings = settings
        self.stats = ProducerStats()
        self._fatal: Any = None
        self._should_stop = should_stop
        self._backoff_initial_s = backoff_initial_s
        self._backoff_max_s = backoff_max_s
        self._clock = clock
        config = settings.to_config()
        config["error_cb"] = self._on_error
        self._producer = (producer_factory or _default_factory)(config)

    def _on_error(self, error: Any) -> None:
        name = error.name() if hasattr(error, "name") else str(error)
        self.stats.client_errors[name] = self.stats.client_errors.get(name, 0) + 1
        if hasattr(error, "fatal") and error.fatal():
            self._fatal = error

    def _on_delivery(self, error: Any, message: Any) -> None:
        if error is None:
            self.stats.delivered += 1
            self.stats.last_ack_epoch_ms = time.time_ns() // 1_000_000
            return
        self.stats.delivery_errors += 1
        name = error.name() if hasattr(error, "name") else str(error)
        self.stats.error_counts[name] = self.stats.error_counts.get(name, 0) + 1
        if len(self.stats.error_samples) < MAX_ERROR_SAMPLES:
            self.stats.error_samples.append(str(error))

    def _check_fatal(self) -> None:
        if self._fatal is not None:
            raise ProducerFatalError(f"lỗi fatal từ librdkafka: {self._fatal}")

    def send(self, topic: str, value: bytes, key: bytes | None = None, partition: int | None = None) -> float:
        """Đưa một tin vào hàng đợi; trả số giây bị chặn vì hàng đợi đầy (0 khi không chặn)."""
        self._check_fatal()
        blocked = 0.0
        backoff = self._backoff_initial_s
        while True:
            try:
                if partition is None:
                    self._producer.produce(topic, value=value, key=key, on_delivery=self._on_delivery)
                else:
                    self._producer.produce(topic, value=value, key=key, partition=partition,
                                           on_delivery=self._on_delivery)
                break
            except BufferError:
                # Hàng đợi cục bộ đầy (broker chậm/dừng): phục vụ callback rồi thử lại — không bỏ tin.
                self.stats.buffer_full_events += 1
                started = self._clock()
                self._producer.poll(backoff)
                waited = self._clock() - started
                blocked += waited
                self.stats.backpressure_s += waited
                backoff = min(backoff * 2, self._backoff_max_s)
                self._check_fatal()
                if self._should_stop is not None and self._should_stop():
                    raise ProducerStopped("dừng trong lúc chờ hàng đợi producer có chỗ") from None
        self.stats.produced += 1
        self._producer.poll(0)
        return blocked

    def poll(self, timeout_s: float = 0.0) -> int:
        served = self._producer.poll(timeout_s)
        self._check_fatal()
        return served

    def flush(self, timeout_s: float) -> int:
        """Chờ mọi tin được giao hoặc hết hạn; trả số tin còn trong hàng đợi (chưa có kết quả giao)."""
        remaining = self._producer.flush(timeout_s)
        return int(remaining)

    def partition_count(self, topic: str, timeout_s: float = 10.0) -> int | None:
        """Số partition theo metadata (đồng thời mở sẵn kết nối tới broker); None nếu topic không có."""
        metadata = self._producer.list_topics(topic=topic, timeout=timeout_s)
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            return None
        return len(info.partitions)
