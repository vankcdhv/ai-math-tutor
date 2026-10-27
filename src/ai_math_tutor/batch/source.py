"""Nguồn event log cho batch: Parquet của bộ mô phỏng (tùy chọn đóng dấu lại như bộ phát lại) hoặc JSONL hợp đồng.

- Parquet (`events.parquet`, xem `simulator.output`): mỗi dòng là một lần giao, cột `value` là JSON
  sự kiện. Với `replay_start_ms`, `value` được đóng dấu lại bằng đúng `simulator.output.restamp`
  (event_time/sent_time = replay_start + offset) — cùng byte mà bộ phát lại gửi lên Kafka.
- JSONL: mỗi dòng một sự kiện hợp đồng đã mang thời gian tuyệt đối (ví dụ log đã phát lại).
  `replay_start_ms` khi đó chỉ là mốc thời gian để dịch ground truth, không đóng dấu lại.

Thời gian luôn đọc từ chính JSON sự kiện (`event_time`, `sent_time`) như luồng streaming.
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pyarrow.parquet as pq

FORMATS = ("parquet", "jsonl")
_JSONL_SUFFIXES = (".jsonl", ".json", ".ndjson")
_RESTAMP_COLUMNS = ("value", "event_offset_ms", "sent_offset_ms")


class SourceError(ValueError):
    """Đường dẫn/định dạng event log không dùng được."""


@dataclass(frozen=True)
class EventSource:
    path: Path
    format: str
    replay_start_ms: int | None = None

    @property
    def restamps(self) -> bool:
        """Có đóng dấu lại `value` (chỉ với Parquet của bộ mô phỏng có offset)."""
        return self.format == "parquet" and self.replay_start_ms is not None

    @property
    def spark_path(self) -> str:
        return str(self.path)

    def t0_epoch_ms(self) -> int | None:
        """t0 của mô phỏng từ metadata Parquet (`nhom8.t0_epoch_ms`); None nếu không có."""
        if self.format != "parquet" or not self.path.is_file():
            return None
        metadata = pq.read_schema(str(self.path)).metadata or {}
        raw = metadata.get(b"nhom8.t0_epoch_ms")
        return int(raw.decode()) if raw else None


def open_source(path: Path | str, fmt: str = "auto", replay_start_ms: int | None = None) -> EventSource:
    """Kiểm đường dẫn, đoán định dạng, kiểm cột cần thiết; báo lỗi sớm thay vì để Spark lỗi khó hiểu."""
    source_path = Path(path).expanduser().resolve()
    if not source_path.exists():
        raise SourceError(f"không thấy event log: {source_path}")
    if fmt == "auto":
        fmt = "jsonl" if source_path.is_file() and source_path.suffix.lower() in _JSONL_SUFFIXES else "parquet"
    if fmt not in FORMATS:
        raise SourceError(f"định dạng phải thuộc {FORMATS}, nhận {fmt!r}")
    if replay_start_ms is not None and (isinstance(replay_start_ms, bool) or not isinstance(replay_start_ms, int) or replay_start_ms < 0):
        raise SourceError(f"replay_start_ms phải là số nguyên ≥ 0, nhận {replay_start_ms!r}")
    if fmt == "parquet" and source_path.is_file():
        columns = set(pq.read_schema(str(source_path)).names)
        needed = _RESTAMP_COLUMNS if replay_start_ms is not None else ("value",)
        missing = [name for name in needed if name not in columns]
        if missing:
            raise SourceError(f"{source_path} thiếu cột {missing}")
    return EventSource(path=source_path, format=fmt, replay_start_ms=replay_start_ms)
