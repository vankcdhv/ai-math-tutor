"""Lịch chạy batch mô phỏng: các mốc cutoff theo ngày hoặc theo giờ, với pha lấy từ seed.

Mốc cutoff thứ k: `anchor + phase + k·period`, với `anchor` là mốc thời gian của log (t0 của mô
phỏng, hoặc `replay_start` khi chạy trên log phát lại) — nên phase 0 của lịch ngày là đúng 00:00
mô phỏng. Pha (giờ chạy batch trong ngày, hoặc phút chạy trong giờ) **không chọn tay**: lấy từ
SHA-256 của (lịch, seed), tính theo giây nguyên, và được ghi vào run record. Có thể ghi đè pha
(ví dụ 0 để chạy đúng nửa đêm), khi đó nguồn pha ghi là "override".

Một job tại cutoff c thấy mọi sự kiện có `sent_time ≤ c`. Danh sách cutoff của một run phủ từ
cutoff đầu tiên ≥ sự kiện đến sớm nhất tới cutoff đầu tiên ≥ sự kiện đến muộn nhất, nên mọi
sự kiện đều được ít nhất một job xử lý.
"""

from __future__ import annotations

import hashlib
from dataclasses import asdict, dataclass
from typing import Any

HOUR_MS = 3_600_000
DAY_MS = 24 * HOUR_MS
PERIODS_MS = {"daily": DAY_MS, "hourly": HOUR_MS}
SCHEDULES = tuple(PERIODS_MS)


class ScheduleError(ValueError):
    """Tham số lịch không hợp lệ."""


def seeded_phase_ms(schedule: str, seed: int) -> int:
    """Pha tất định trong [0, period), làm tròn giây, từ SHA-256 của (lịch, seed)."""
    if schedule not in PERIODS_MS:
        raise ScheduleError(f"lịch phải thuộc {SCHEDULES}, nhận {schedule!r}")
    if isinstance(seed, bool) or not isinstance(seed, int) or seed < 0:
        raise ScheduleError(f"seed phải là số nguyên ≥ 0, nhận {seed!r}")
    digest = hashlib.sha256(f"nhom8/batch-cutoff-phase/{schedule}/{seed}".encode()).digest()
    period_s = PERIODS_MS[schedule] // 1000
    return (int.from_bytes(digest[:8], "big") % period_s) * 1000


@dataclass(frozen=True)
class Schedule:
    name: str
    period_ms: int
    phase_ms: int
    anchor_ms: int
    phase_source: str  # "seed" | "override"

    def cutoff_at_or_after(self, t_ms: int) -> int:
        """Cutoff nhỏ nhất ≥ t_ms."""
        base = self.anchor_ms + self.phase_ms
        k = -((base - t_ms) // self.period_ms)  # ceil((t − base) / period) với số nguyên
        return base + k * self.period_ms

    def cutoffs_covering(self, first_ms: int, last_ms: int) -> list[int]:
        """Mọi cutoff từ cutoff đầu tiên ≥ first_ms tới cutoff đầu tiên ≥ last_ms (gồm cả hai)."""
        if first_ms > last_ms:
            raise ScheduleError(f"first_ms ({first_ms}) > last_ms ({last_ms})")
        start = self.cutoff_at_or_after(first_ms)
        stop = self.cutoff_at_or_after(last_ms)
        return list(range(start, stop + 1, self.period_ms))

    def describe(self) -> dict[str, Any]:
        data = asdict(self)
        data["phase_s"] = self.phase_ms / 1000
        data["period_s"] = self.period_ms / 1000
        return data


def make_schedule(name: str, anchor_ms: int, seed: int, phase_s: float | None = None) -> Schedule:
    """Lịch `daily`/`hourly`; pha từ seed trừ khi `phase_s` được cho (giây, trong [0, period))."""
    if name not in PERIODS_MS:
        raise ScheduleError(f"lịch phải thuộc {SCHEDULES}, nhận {name!r}")
    period_ms = PERIODS_MS[name]
    if phase_s is None:
        return Schedule(name, period_ms, seeded_phase_ms(name, seed), anchor_ms, "seed")
    phase_ms = round(phase_s * 1000)
    if not 0 <= phase_ms < period_ms:
        raise ScheduleError(f"phase_s phải trong [0, {period_ms // 1000}), nhận {phase_s!r}")
    return Schedule(name, period_ms, phase_ms, anchor_ms, "override")
