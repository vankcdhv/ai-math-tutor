"""Định dạng đầu ra `events.parquet` và tiện ích đọc/đóng dấu lại cho bộ phát lại, batch, validator.

Mỗi dòng Parquet là **một lần giao** tới broker (bản gốc hoặc bản trùng), theo thứ tự truyền:
`replay_offset_ms` không giảm. Cột:

| Cột | Kiểu | Ý nghĩa |
|---|---|---|
| `replay_offset_ms` | int64 | Lúc bộ phát lại gửi lần giao này (ms từ t0). Bản gốc: = `sent_offset_ms`; bản trùng: muộn hơn một khoảng retry |
| `event_offset_ms` | int64 | `event_time` − t0 (ms) |
| `sent_offset_ms` | int64 | `sent_time` − t0 (ms) — lần gửi đầu, giống nhau giữa bản gốc và bản trùng |
| `event_id`, `learner_id`, `class_id`, `session_id`, `event_type`, `problem_id`, `connection` | string | Sao từ sự kiện để lọc/phân shard không cần parse JSON |
| `session_seq` | int32 | Sao từ sự kiện |
| `value` | string | JSON hợp đồng LearnerEvent v0.1 (dạng `json.dumps` mặc định), `event_time`/`sent_time` = t0 + offset |

Bản trùng có `value` giống hệt từng byte với bản gốc. Không có cột ground truth.
Metadata Parquet: `nhom8.t0` (ISO), `nhom8.t0_epoch_ms`, `nhom8.schema_version`.
"""

from __future__ import annotations

import json
from collections.abc import Iterator, Sequence
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

SCHEMA_VERSION = "0.1"
EVENT_COLUMNS = (
    "replay_offset_ms", "event_offset_ms", "sent_offset_ms", "event_id", "learner_id", "class_id",
    "session_id", "session_seq", "event_type", "problem_id", "connection", "value",
)
_ARROW_TYPES = {
    "replay_offset_ms": pa.int64(), "event_offset_ms": pa.int64(), "sent_offset_ms": pa.int64(),
    "session_seq": pa.int32(),
}
_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
DAY_MS = 86_400_000


def arrow_schema(t0_iso: str, t0_ms: int) -> pa.Schema:
    fields = [pa.field(name, _ARROW_TYPES.get(name, pa.string()), nullable=False) for name in EVENT_COLUMNS]
    metadata = {"nhom8.t0": t0_iso, "nhom8.t0_epoch_ms": str(t0_ms), "nhom8.schema_version": SCHEMA_VERSION}
    return pa.schema(fields, metadata=metadata)


def parse_utc(text: str) -> datetime:
    """Chuỗi ISO 8601 UTC kết thúc bằng 'Z' → datetime có múi giờ."""
    if not text.endswith("Z"):
        raise ValueError(f"thời điểm phải là UTC kết thúc bằng 'Z': {text!r}")
    return datetime.fromisoformat(text[:-1] + "+00:00")


def epoch_ms(moment: datetime) -> int:
    return (moment - _EPOCH) // timedelta(milliseconds=1)


class IsoClock:
    """Định dạng nhanh epoch ms → 'YYYY-MM-DDTHH:MM:SS.mmmZ' (như mẫu tuần 1)."""

    def __init__(self) -> None:
        self._prefixes: dict[int, str] = {}

    def iso(self, ms: int) -> str:
        day, rem = divmod(ms, DAY_MS)
        prefix = self._prefixes.get(day)
        if prefix is None:
            prefix = (_EPOCH + timedelta(days=day)).strftime("%Y-%m-%dT")
            self._prefixes[day] = prefix
        hours, rem = divmod(rem, 3_600_000)
        minutes, rem = divmod(rem, 60_000)
        seconds, millis = divmod(rem, 1000)
        return f"{prefix}{hours:02d}:{minutes:02d}:{seconds:02d}.{millis:03d}Z"


_CLOCK = IsoClock()


def iso_from_epoch_ms(ms: int) -> str:
    return _CLOCK.iso(ms)


def restamp(value: str, replay_start_ms: int, event_offset_ms: int, sent_offset_ms: int) -> str:
    """Đóng dấu lại một sự kiện cho lần phát lại: event_time/sent_time = replay_start + offset.

    Giữ nguyên khoảng sent − event của mô phỏng. Cùng đầu vào ⇒ cùng byte, nên bản trùng
    đóng dấu lại vẫn giống hệt bản gốc (bộ phát lại vẫn nên cache theo event_id).
    """
    event = json.loads(value)
    event["event_time"] = iso_from_epoch_ms(replay_start_ms + event_offset_ms)
    event["sent_time"] = iso_from_epoch_ms(replay_start_ms + sent_offset_ms)
    return json.dumps(event)


class EventParquetWriter:
    """Ghi các lần giao theo khối (row group) để bộ nhớ không phụ thuộc độ dài run."""

    def __init__(self, path: Path, schema: pa.Schema, chunk_rows: int = 200_000):
        self._schema = schema
        self._writer = pq.ParquetWriter(str(path), schema, compression="zstd")
        self._chunk_rows = chunk_rows
        self._columns: list[list[Any]] = [[] for _ in EVENT_COLUMNS]
        self.rows = 0

    def append(self, row: Sequence[Any]) -> None:
        for column, value in zip(self._columns, row):
            column.append(value)
        if len(self._columns[0]) >= self._chunk_rows:
            self._flush()

    def _flush(self) -> None:
        if not self._columns[0]:
            return
        arrays = [pa.array(values, type=field.type) for values, field in zip(self._columns, self._schema)]
        self._writer.write_table(pa.Table.from_arrays(arrays, schema=self._schema))
        self.rows += len(self._columns[0])
        self._columns = [[] for _ in EVENT_COLUMNS]

    def close(self) -> None:
        self._flush()
        self._writer.close()

    def abort(self) -> None:
        self._writer.close()


def parquet_metadata(path: Path) -> dict[str, str]:
    raw = pq.read_schema(str(path)).metadata or {}
    return {k.decode(): v.decode() for k, v in raw.items()}


def iter_rows(path: Path, columns: Sequence[str] | None = None, batch_size: int = 65_536) -> Iterator[dict[str, Any]]:
    """Đọc từng dòng (dict) theo thứ tự truyền, theo lô để không nạp cả tệp."""
    parquet = pq.ParquetFile(str(path))
    for batch in parquet.iter_batches(batch_size=batch_size, columns=list(columns) if columns else None):
        yield from batch.to_pylist()


def iter_events(path: Path) -> Iterator[dict[str, Any]]:
    """Các sự kiện hợp đồng (đã parse) theo thứ tự broker nhận, gồm cả bản trùng."""
    for row in iter_rows(path, columns=["value"]):
        yield json.loads(row["value"])


def write_events_jsonl(parquet_path: Path, jsonl_path: Path, limit: int | None = None) -> int:
    """Xuất cột `value` ra JSONL (thứ tự truyền) để chạy `event_validation.validate_events`."""
    count = 0
    with Path(jsonl_path).open("w", encoding="utf-8", newline="\n") as handle:
        for row in iter_rows(parquet_path, columns=["value"]):
            if limit is not None and count >= limit:
                break
            handle.write(row["value"] + "\n")
            count += 1
    return count
