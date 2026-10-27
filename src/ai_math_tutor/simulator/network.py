"""Mô hình mạng theo `client.connection`: độ trễ gửi, sự kiện đến muộn và gửi trùng.

Với mỗi sự kiện (một `event_id`):
- **Độ trễ gửi** `sent_time − event_time`: log-normal nhỏ (wifi) hoặc đuôi dài hơn (cellular),
  luôn < ngưỡng muộn.
- **Muộn** với xác suất `p_late` của nhóm: client mất mạng, đệm rồi gửi lại thành công sau
  ngưỡng muộn + log-normal (đuôi dài). Theo hợp đồng, `sent_time` là lần gửi đầu được
  broker nhận, nên độ trễ này nằm trong `sent_time − event_time` (giống mẫu tuần 1).
- **Trùng** với xác suất `duplicate_rate` (mọi nhóm): client không nhận ack nên gửi lại
  **nguyên byte** (cùng `event_id`, cùng `sent_time` lần đầu); bản trùng tới broker sau một
  khoảng retry (wifi ngắn, cellular log-normal dài — retry muộn).

Ngưỡng muộn dùng chung với validator (`event_validation.DEFAULT_LATE_THRESHOLD_S`) để tỉ lệ
muộn cấu hình, ground truth và báo cáo validator cùng một định nghĩa.
"""

from __future__ import annotations

import math
import random
from collections.abc import Mapping
from dataclasses import asdict, dataclass

from ai_math_tutor.event_validation import DEFAULT_LATE_THRESHOLD_S
from ai_math_tutor.simulator.population import SimulationConfigError

LATE_THRESHOLD_MS = int(DEFAULT_LATE_THRESHOLD_S * 1000)
# Độ trễ thường bị chặn dưới ngưỡng muộn để "muộn" chỉ đến từ cơ chế mất mạng có tham số.
_MAX_BASE_DELAY_MS = LATE_THRESHOLD_MS - 1000
# Chặn đuôi muộn 1 giờ để một mẫu cực trị không kéo dài run vô hạn.
_MAX_LATE_EXTRA_MS = 3_600_000
MAX_DUPLICATE_RATE = 0.5


@dataclass(frozen=True, slots=True)
class ConnectionProfile:
    base_median_s: float
    base_sigma: float
    p_late: float
    late_median_s: float
    late_sigma: float
    retry_median_s: float
    retry_sigma: float


# Hồ sơ do nhóm tự đặt (giả định, có phân tích độ nhạy low/high), không đo từ mạng thật.
LATE_PROFILES: dict[str, dict[str, ConnectionProfile]] = {
    "low": {
        "wifi": ConnectionProfile(0.04, 0.5, 0.002, 20.0, 0.8, 0.5, 0.6),
        "cellular": ConnectionProfile(0.25, 1.0, 0.03, 40.0, 1.0, 2.0, 1.0),
    },
    "high": {
        "wifi": ConnectionProfile(0.06, 0.6, 0.01, 40.0, 1.0, 1.0, 0.8),
        "cellular": ConnectionProfile(0.5, 1.2, 0.12, 90.0, 1.2, 5.0, 1.2),
    },
}


def _lognormal_ms(rng: random.Random, median_s: float, sigma: float) -> int:
    return max(1, round(rng.lognormvariate(math.log(median_s * 1000.0), sigma)))


@dataclass(frozen=True, slots=True)
class Delivery:
    sent_ms: int
    duplicate_replay_ms: int | None
    late: bool


class NetworkModel:
    def __init__(self, late_profile: str, duplicate_rate: float):
        if late_profile not in LATE_PROFILES:
            raise SimulationConfigError(f"late_profile phải thuộc {sorted(LATE_PROFILES)}, nhận {late_profile!r}")
        if not 0.0 <= duplicate_rate <= MAX_DUPLICATE_RATE:
            raise SimulationConfigError(f"duplicate_rate phải trong [0, {MAX_DUPLICATE_RATE}]")
        self.late_profile = late_profile
        self.duplicate_rate = duplicate_rate
        self.profiles = LATE_PROFILES[late_profile]

    def deliver(self, rng: random.Random, connection: str, event_ms: int) -> Delivery:
        """Thời điểm gửi lần đầu và (nếu có) thời điểm bản trùng tới broker, tính bằng ms từ t0."""
        profile = self.profiles[connection]
        late = rng.random() < profile.p_late
        if late:
            extra = min(_lognormal_ms(rng, profile.late_median_s, profile.late_sigma), _MAX_LATE_EXTRA_MS)
            delay = LATE_THRESHOLD_MS + extra
        else:
            delay = min(_lognormal_ms(rng, profile.base_median_s, profile.base_sigma), _MAX_BASE_DELAY_MS)
        sent_ms = event_ms + delay
        duplicate_ms = None
        if rng.random() < self.duplicate_rate:
            duplicate_ms = sent_ms + _lognormal_ms(rng, profile.retry_median_s, profile.retry_sigma)
        return Delivery(sent_ms, duplicate_ms, late)

    def expected_late_rate(self, connection_shares: Mapping[str, float]) -> float:
        """Tỉ lệ sự kiện muộn kỳ vọng theo cơ cấu kết nối (trọng số theo số sự kiện)."""
        return sum(share * self.profiles[conn].p_late for conn, share in connection_shares.items())

    def describe(self) -> dict:
        return {
            "late_profile": self.late_profile,
            "duplicate_rate": self.duplicate_rate,
            "late_threshold_s": DEFAULT_LATE_THRESHOLD_S,
            "profiles": {conn: asdict(p) for conn, p in self.profiles.items()},
        }
