"""Xác thực nguyên mẫu và giới hạn tốc độ của dịch vụ gợi ý.

- Khóa API demo: header `X-API-Key`, so bằng `hmac.compare_digest` (thời gian không phụ thuộc nội
  dung). Khóa đến từ biến môi trường `NHOM8_HINT_API_KEY` (hoặc `.env`, đã gitignore), không bao giờ
  từ YAML hay mã nguồn. Khóa còn là giá trị mẫu `change-me-…` của `.env.example` bị từ chối khi khởi
  động (giá trị đó công khai trong repo). Đây là xác thực **mức nguyên mẫu**: một khóa chung cho mọi client, nên ai có
  khóa đều gửi được yêu cầu nhân danh bất kỳ `learner_id` nào (giới hạn đã biết "giả mạo yêu cầu gợi ý").
- Giới hạn tốc độ theo `learner_id`: token bucket trong bộ nhớ của tiến trình (dịch vụ chạy một
  tiến trình uvicorn), `burst` yêu cầu tức thời rồi `rate_per_min` yêu cầu mỗi phút. Hết token → 429
  kèm số giây nên chờ. Bộ nhớ bị chặn: quá `max_learners` thì bỏ các bucket lâu không dùng nhất.
"""

from __future__ import annotations

import hmac
import math
import threading
import time
from collections import OrderedDict
from collections.abc import Callable, Mapping

API_KEY_ENV = "NHOM8_HINT_API_KEY"
API_KEY_HEADER = "X-API-Key"
MIN_API_KEY_LENGTH = 16
PLACEHOLDER_PREFIX = "change-me"  # tiền tố giá trị mẫu trong .env.example
DEFAULT_RATE_PER_MIN = 60.0
DEFAULT_BURST = 10
DEFAULT_MAX_LEARNERS = 100_000


class AuthConfigError(ValueError):
    """Thiếu hoặc yếu khóa API: dịch vụ từ chối khởi động thay vì chạy không xác thực."""


def _reject_placeholder(key: str, name: str) -> None:
    """Giá trị mẫu của `.env.example` (`change-me-…`) là công khai trong repo: không bao giờ dùng làm khóa."""
    if key.strip().lower().startswith(PLACEHOLDER_PREFIX):
        raise AuthConfigError(
            f"{name} vẫn là giá trị mẫu của .env.example ({key.strip()[:len(PLACEHOLDER_PREFIX)]}…); hãy sinh một "
            "khóa ngẫu nhiên, ví dụ `python -c \"import secrets; print(secrets.token_urlsafe(32))\"`, rồi đặt vào .env")


def api_key_from_env(env: Mapping[str, str]) -> str:
    key = env.get(API_KEY_ENV, "")
    if len(key) < MIN_API_KEY_LENGTH:
        raise AuthConfigError(f"{API_KEY_ENV} phải có ít nhất {MIN_API_KEY_LENGTH} ký tự (đặt trong .env)")
    _reject_placeholder(key, API_KEY_ENV)
    return key


class ApiKeyAuth:
    def __init__(self, api_key: str) -> None:
        if not isinstance(api_key, str) or len(api_key) < MIN_API_KEY_LENGTH:
            raise AuthConfigError(f"khóa API phải có ít nhất {MIN_API_KEY_LENGTH} ký tự")
        _reject_placeholder(api_key, "khóa API")
        self._key = api_key.encode("utf-8")

    def check(self, presented: str | None) -> bool:
        if not presented:
            return False
        return hmac.compare_digest(presented.encode("utf-8"), self._key)


class RateLimiter:
    """Token bucket theo khóa (learner_id). An toàn đa luồng."""

    def __init__(self, rate_per_min: float = DEFAULT_RATE_PER_MIN, burst: int = DEFAULT_BURST,
                 max_learners: int = DEFAULT_MAX_LEARNERS, clock: Callable[[], float] = time.monotonic) -> None:
        if not rate_per_min > 0 or burst < 1 or max_learners < 1:
            raise ValueError("cần rate_per_min > 0, burst ≥ 1, max_learners ≥ 1")
        self._rate_per_s = rate_per_min / 60.0
        self._burst = float(burst)
        self._max = max_learners
        self._clock = clock
        self._buckets: OrderedDict[str, tuple[float, float]] = OrderedDict()  # khóa → (token, lúc cập nhật)
        self._lock = threading.Lock()

    def acquire(self, key: str) -> tuple[bool, int]:
        """(được phép, số giây nên chờ nếu bị từ chối — làm tròn lên, ≥ 1)."""
        now = self._clock()
        with self._lock:
            tokens, updated = self._buckets.pop(key, (self._burst, now))
            tokens = min(self._burst, tokens + (now - updated) * self._rate_per_s)
            allowed = tokens >= 1.0
            if allowed:
                tokens -= 1.0
            self._buckets[key] = (tokens, now)
            while len(self._buckets) > self._max:
                self._buckets.popitem(last=False)
        if allowed:
            return True, 0
        return False, max(1, math.ceil((1.0 - tokens) / self._rate_per_s))
