"""Tổng hợp sự kiện theo (learner_id, problem_id) kiểu MapReduce trên Spark RDD, kèm bản tham chiếu thuần Python.

Đầu vào là tệp văn bản, mỗi dòng là một lần giao (chuỗi JSON sự kiện, như `value` trên Kafka). Spark đọc bằng
`textFile` (TextInputFormat của Hadoop: mỗi input split là một khoảng byte, mỗi bản ghi là một dòng).

    dòng ─map(classify_line)─▶ (INVALID, lý do, event_id, raw) | (HEARTBEAT,) | (VALID, event_id, lần giao)
      ├─ map(stat_key) → (khóa, 1) ─reduceByKey(cộng)─▶ số lần giao theo loại/lý do         # word count
      ├─ filter(INVALID) ─▶ invalid/part-*  (event_id, lý do, đoạn đầu bản ghi)
      └─ filter(VALID) → (event_id, lần giao) ─reduceByKey(merge_delivery)─▶ sự kiện duy nhất  # khử trùng
           ├─ fold(dedup_stat) ─▶ số sự kiện duy nhất, bản trùng, event_id xung đột, sự kiện muộn
           └─ map → ((learner, problem), bộ đếm) ─reduceByKey(add_counters, partition = learner_id)
                ├─ counters/part-*  (một dòng JSON mỗi khóa)
                └─ map → (problem, số sai) ─reduceByKey(cộng)─mapPartitions(local_top_k)─reduce(merge_top_k)

**Map (`classify_line`).** Parse JSON, kiểm theo hợp đồng LearnerEvent v0.2. Mọi mẫu regex, enum, tập trường và
luật payload đọc từ chính `contracts/learner_event_v0_2.schema.json` (`event_validation.load_schema`), không chép
tay. Kiểm theo thứ tự, lấy lý do đầu tiên, cùng tên lý do với dead-letter của streaming: `malformed_json`,
`unknown_field`, `missing_field`, `schema_version`, `event_type`, `event_id`, `learner_id`, `event_time`,
`sent_time`, `heartbeat_contract`, `learning_fields`, `payload`. Thêm `sent_before_event` (sent_time sớm hơn
event_time, validator ngoại tuyến cũng coi là lỗi). Streaming có `future_event_time` so với timestamp Kafka; batch
không có timestamp đó nên không có lý do này. Bản ghi hỏng không làm mất bản ghi khác: nó thành một bản ghi
INVALID, được ghi riêng.

**Chuẩn hóa event_type.** `event_type` phải khớp đúng enum của hợp đồng; bản ghi viết khác (hoa/thường, khoảng
trắng) là INVALID lý do `event_type`, giống dead-letter của streaming, để batch và streaming nhận cùng một tập sự
kiện. Sau khi kiểm, mỗi sự kiện được chuẩn hóa về một mã hành động (`KIND_*`): `submit_step` tách thành bước đúng
và bước sai theo `payload.is_correct`. Heartbeat (bản ghi kỹ thuật của v0.2) hợp lệ thì bị bỏ, chỉ được đếm.

**Đến muộn.** Một sự kiện là muộn khi `sent_time − event_time > ngưỡng` (mặc định 30 s, như
`event_validation.DEFAULT_LATE_THRESHOLD_S` và bộ mô phỏng). Batch đọc cả log nên sự kiện muộn **vẫn được cộng**;
chỉ được đếm (trên sự kiện duy nhất, như validator).

**Khử trùng (`merge_delivery`).** Khóa `event_id`. Giữ lần giao "đầu tiên" theo thứ tự toàn phần
(`sent_time`, băm 64 bit của dòng gốc, các trường còn lại) nên kết quả không phụ thuộc thứ tự hay cách chia
partition. Cộng số lần giao (trùng = số lần giao − 1) và đánh dấu xung đột khi cùng `event_id` mà nội dung khác
(lỗi hợp đồng, quy tắc 2 của `docs/event-schema.md`): sự kiện vẫn được tính một lần theo bản giữ lại và
`event_id` được đếm ở `conflicting_event_ids`. Phép gộp kết hợp và giao hoán nên `reduceByKey` gộp sẵn ở phía map
(combiner) mà kết quả không đổi; một task chạy lại (retry) chỉ tạo lại cùng các cặp, không cộng thêm.

**Bộ đếm theo (learner_id, problem_id)**, trên sự kiện duy nhất:

- `events`: mọi sự kiện học tập (cả `open_problem`);
- `attempts`: số lần nộp được kiểm chứng = `submit_step` + `wrong_answer` + `complete`. Mỗi lần là một lần
  người học gửi bước hoặc đáp án cho bộ kiểm (đúng hoặc sai); `open_problem` và `request_hint` không phải lần thử;
- `wrong`: `submit_step` có `is_correct = false` + `wrong_answer`;
- `hint_requests`: `request_hint`;
- `completed`: đã thấy `complete`;
- `first_event_ms`, `last_event_ms`: event_time nhỏ nhất/lớn nhất (epoch ms).

Mọi phép gộp (cộng, OR, min, max) kết hợp và giao hoán ⇒ combiner-safe. Partition của bước này chỉ theo
`learner_id` (crc32, tất định giữa các tiến trình) như khóa phân vùng ở mục 1 của thiết kế.

**Top-K bài có nhiều lượt sai nhất.** Tổng số sai theo `problem_id` được gom trước (reduceByKey) nên mỗi bài nằm
trọn ở một partition; mỗi partition chọn top-K cục bộ bằng heap (`heapq.nsmallest`), rồi `reduce` gộp các danh sách
ứng viên thành top-K toàn cục. Thứ tự: số sai giảm dần, đồng hạng thì `problem_id` tăng dần; K mặc định 10.

**Tham chiếu (`reference_counts`)** tính lại mọi thứ trên cùng các dòng bằng một vòng lặp tuần tự với dict, không
Spark; chỉ dùng chung bước phân loại một bản ghi (`classify_line`). `compare_counts` so hai kết quả, trả cờ khớp
từng phần và các sai khác.

Mô-đun import được mà không cần pyspark; chỉ `run_problem_job` nhận một SparkContext.
"""

from __future__ import annotations

import gzip
import hashlib
import heapq
import json
import re
import time
import zlib
from collections import Counter
from collections.abc import Callable, Iterable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from functools import partial
from pathlib import Path
from typing import TYPE_CHECKING, Any

from ai_math_tutor.event_validation import (
    DEFAULT_LATE_THRESHOLD_S,
    HEARTBEAT_EVENT_TYPE,
    HEARTBEAT_LEARNER_ID,
    load_schema,
)

if TYPE_CHECKING:
    from pyspark import SparkContext

DEFAULT_TOP_K = 10
RAW_SNIPPET_CHARS = 300

# ---------------------------------------------------------------- hợp đồng, đọc từ JSON Schema

_SCHEMA = load_schema()
_PROPS: dict[str, Any] = _SCHEMA["properties"]
FIELD_NAMES = frozenset(_PROPS)
REQUIRED_FIELDS: tuple[str, ...] = tuple(_SCHEMA["required"])
SCHEMA_VERSIONS = frozenset(_PROPS["schema_version"]["enum"])
EVENT_TYPES = frozenset(_PROPS["event_type"]["enum"])
_HEARTBEAT_RULE = _SCHEMA["allOf"][0]["then"]
_LEARNING_RULE = _SCHEMA["allOf"][0]["else"]
HEARTBEAT_FIELDS = frozenset(_HEARTBEAT_RULE["propertyNames"]["enum"])
HEARTBEAT_SCHEMA_VERSION: str = _HEARTBEAT_RULE["properties"]["schema_version"]["const"]
LEARNING_REQUIRED_FIELDS: tuple[str, ...] = tuple(_LEARNING_RULE["required"])
_CLIENT = _PROPS["client"]
CLIENT_FIELDS = frozenset(_CLIENT["properties"])
CLIENT_REQUIRED = frozenset(_CLIENT["required"])
PLATFORMS = frozenset(_CLIENT["properties"]["platform"]["enum"])
CONNECTIONS = frozenset(_CLIENT["properties"]["connection"]["enum"])


def _pattern(spec: Mapping[str, Any]) -> re.Pattern[str]:
    return re.compile(spec["pattern"])


_EVENT_ID_RE = _pattern(_PROPS["event_id"])
_LEARNER_ID_RE = _pattern(_PROPS["learner_id"])
_CLASS_ID_RE = _pattern(_PROPS["class_id"])
_SESSION_ID_RE = _pattern(_PROPS["session_id"])
_PROBLEM_ID_RE = _pattern(_PROPS["problem_id"])
_SKILL_ID_RE = _pattern(_PROPS["skill_ids"]["items"])
_DATETIME_RE = _pattern(_PROPS["event_time"])
_APP_VERSION_RE = _pattern(_CLIENT["properties"]["app_version"])
_SESSION_SEQ_MIN: int = _PROPS["session_seq"]["minimum"]


@dataclass(frozen=True)
class PayloadRule:
    """Luật payload của một event_type (từ khối `if/then` của schema)."""

    required: frozenset[str]
    fields: Mapping[str, Mapping[str, Any]]
    closed: bool  # additionalProperties: false
    max_properties: int | None


def _payload_rules() -> dict[str, PayloadRule]:
    rules: dict[str, PayloadRule] = {}
    for block in _SCHEMA["allOf"][1:]:
        event_type = block["if"]["properties"]["event_type"]["const"]
        payload = block["then"]["properties"]["payload"]
        rules[event_type] = PayloadRule(
            required=frozenset(payload.get("required", ())),
            fields=dict(payload.get("properties", {})),
            closed=payload.get("additionalProperties", True) is False,
            max_properties=payload.get("maxProperties"),
        )
    return rules


PAYLOAD_RULES = _payload_rules()

# Mã hành động sau chuẩn hóa: (attempts, wrong, hint_requests, completed) mà một sự kiện đóng góp.
KIND_OPEN, KIND_STEP_OK, KIND_STEP_WRONG, KIND_WRONG_ANSWER, KIND_HINT, KIND_COMPLETE = range(6)
_KIND_CONTRIBUTION: dict[int, tuple[int, int, int, int]] = {
    KIND_OPEN: (0, 0, 0, 0),
    KIND_STEP_OK: (1, 0, 0, 0),
    KIND_STEP_WRONG: (1, 1, 0, 0),
    KIND_WRONG_ANSWER: (1, 1, 0, 0),
    KIND_HINT: (0, 0, 1, 0),
    KIND_COMPLETE: (1, 0, 0, 1),
}
_KIND_BY_TYPE = {"open_problem": KIND_OPEN, "wrong_answer": KIND_WRONG_ANSWER, "request_hint": KIND_HINT,
                 "complete": KIND_COMPLETE}

INVALID, HEARTBEAT, VALID = 0, 1, 2
INVALID_REASONS = (
    "malformed_json", "unknown_field", "missing_field", "schema_version", "event_type", "event_id", "learner_id",
    "event_time", "sent_time", "heartbeat_contract", "learning_fields", "payload", "sent_before_event",
)
COUNTER_FIELDS = ("events", "attempts", "wrong", "hint_requests", "completed", "first_event_ms", "last_event_ms")
STAT_FIELDS = (
    "deliveries", "valid_deliveries", "heartbeats", "invalid", "invalid_with_event_id", "unique_events",
    "duplicate_deliveries", "conflicting_event_ids", "late_events", "output_keys",
)

_EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)
_US = timedelta(microseconds=1)
_EVENT_ID_IN_RAW = re.compile(r'"event_id"\s*:\s*"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})"')

# Lần giao hợp lệ: (sent_us, digest, learner_id, problem_id, kind, event_us, late, n_deliveries, conflict)
Delivery = tuple[int, int, str, str, int, int, int, int, int]
# Bộ đếm: (events, attempts, wrong, hint_requests, completed, first_event_us, last_event_us)
Counters = tuple[int, int, int, int, int, int, int]


# ---------------------------------------------------------------- kiểm một bản ghi

def _is_int(value: Any) -> bool:
    """Số nguyên theo JSON Schema (1.0 cũng là integer), không nhận bool."""
    if isinstance(value, bool):
        return False
    return isinstance(value, int) or (isinstance(value, float) and value.is_integer())


def _match(pattern: re.Pattern[str], value: Any) -> bool:
    # JSON Schema dùng tìm kiếm (search) như thư viện jsonschema; mẫu nào cũng có ^…$.
    return isinstance(value, str) and pattern.search(value) is not None


def _time_us(value: Any) -> int | None:
    """Chuỗi date-time đúng mẫu và là thời điểm có thật → epoch µs; ngược lại None."""
    if not _match(_DATETIME_RE, value):
        return None
    try:
        moment = datetime.fromisoformat(value[:-1] + "+00:00")
    except ValueError:
        return None
    return (moment - _EPOCH) // _US


def _value_ok(spec: Mapping[str, Any], value: Any) -> bool:
    kind = spec.get("type")
    if kind == "integer":
        if not _is_int(value):
            return False
    elif kind == "string":
        if not isinstance(value, str):
            return False
        if len(value) < spec.get("minLength", 0) or len(value) > spec.get("maxLength", len(value)):
            return False
        return True
    elif kind == "boolean":
        return isinstance(value, bool)
    if "minimum" in spec and value < spec["minimum"]:
        return False
    if "maximum" in spec and value > spec["maximum"]:
        return False
    return True


def _payload_ok(event_type: str, payload: Any) -> bool:
    if not isinstance(payload, dict):
        return False
    rule = PAYLOAD_RULES.get(event_type)
    if rule is None:  # heartbeat: đã kiểm ở heartbeat_contract
        return True
    if rule.max_properties is not None and len(payload) > rule.max_properties:
        return False
    if not rule.required.issubset(payload):
        return False
    for name, value in payload.items():
        spec = rule.fields.get(name)
        if spec is None:
            if rule.closed:
                return False
            continue
        if not _value_ok(spec, value):
            return False
    return True


def _learning_fields_ok(event: dict[str, Any]) -> bool:
    if event["learner_id"] == HEARTBEAT_LEARNER_ID:
        return False
    if any(name not in event for name in LEARNING_REQUIRED_FIELDS):
        return False
    if not (_match(_CLASS_ID_RE, event["class_id"]) and _match(_SESSION_ID_RE, event["session_id"])
            and _match(_PROBLEM_ID_RE, event["problem_id"])):
        return False
    seq = event["session_seq"]
    if not _is_int(seq) or seq < _SESSION_SEQ_MIN:
        return False
    skills = event["skill_ids"]
    if not isinstance(skills, list) or not skills or not all(_match(_SKILL_ID_RE, s) for s in skills):
        return False
    if len(set(skills)) != len(skills):
        return False
    client = event["client"]
    if not isinstance(client, dict) or not CLIENT_REQUIRED.issubset(client) or not CLIENT_FIELDS.issuperset(client):
        return False
    platform, connection = client["platform"], client["connection"]
    return (isinstance(platform, str) and platform in PLATFORMS and isinstance(connection, str)
            and connection in CONNECTIONS and _match(_APP_VERSION_RE, client["app_version"]))


def _heartbeat_ok(event: dict[str, Any]) -> bool:
    payload = event["payload"]
    return (event["schema_version"] == HEARTBEAT_SCHEMA_VERSION and event["learner_id"] == HEARTBEAT_LEARNER_ID
            and HEARTBEAT_FIELDS.issuperset(event) and isinstance(payload, dict) and not payload)


def check_event(event: Any) -> tuple[str | None, int | None, int | None]:
    """(lý do không hợp lệ hoặc None, event_time µs, sent_time µs) của một bản ghi đã parse JSON."""
    if not isinstance(event, dict):
        return "malformed_json", None, None
    if not FIELD_NAMES.issuperset(event):
        return "unknown_field", None, None
    if any(name not in event for name in REQUIRED_FIELDS):
        return "missing_field", None, None
    version, event_type = event["schema_version"], event["event_type"]
    if not (isinstance(version, str) and version in SCHEMA_VERSIONS):
        return "schema_version", None, None
    if not (isinstance(event_type, str) and event_type in EVENT_TYPES):
        return "event_type", None, None
    if not _match(_EVENT_ID_RE, event["event_id"]):
        return "event_id", None, None
    if not _match(_LEARNER_ID_RE, event["learner_id"]):
        return "learner_id", None, None
    event_us = _time_us(event["event_time"])
    if event_us is None:
        return "event_time", None, None
    sent_us = _time_us(event["sent_time"])
    if sent_us is None:
        return "sent_time", event_us, None
    if event_type == HEARTBEAT_EVENT_TYPE:
        if not _heartbeat_ok(event):
            return "heartbeat_contract", event_us, sent_us
    elif not _learning_fields_ok(event):
        return "learning_fields", event_us, sent_us
    if not _payload_ok(event_type, event["payload"]):
        return "payload", event_us, sent_us
    if sent_us < event_us:
        return "sent_before_event", event_us, sent_us
    return None, event_us, sent_us


def _digest(line: str) -> int:
    return int.from_bytes(hashlib.blake2b(line.encode("utf-8"), digest_size=8).digest(), "big")


def _kind(event: dict[str, Any]) -> int:
    if event["event_type"] == "submit_step":
        return KIND_STEP_OK if event["payload"]["is_correct"] else KIND_STEP_WRONG
    return _KIND_BY_TYPE[event["event_type"]]


def classify_line(line: str, late_threshold_us: int = int(DEFAULT_LATE_THRESHOLD_S * 1_000_000)) -> tuple[Any, ...]:
    """Map của job: một dòng → (INVALID, lý do, event_id|None, đoạn đầu) | (HEARTBEAT,) | (VALID, event_id, Delivery)."""
    try:
        event = json.loads(line)
    except ValueError:  # gồm JSONDecodeError và chuỗi không giải mã được
        found = _EVENT_ID_IN_RAW.search(line)
        return (INVALID, "malformed_json", found.group(1) if found else None, line[:RAW_SNIPPET_CHARS])
    reason, event_us, sent_us = check_event(event)
    if reason is not None:
        event_id = event.get("event_id") if isinstance(event, dict) else None
        return (INVALID, reason, event_id if isinstance(event_id, str) else None, line[:RAW_SNIPPET_CHARS])
    if event["event_type"] == HEARTBEAT_EVENT_TYPE:
        return (HEARTBEAT,)
    assert event_us is not None and sent_us is not None  # check_event trả hai thời điểm khi hợp lệ
    late = 1 if sent_us - event_us > late_threshold_us else 0
    delivery: Delivery = (sent_us, _digest(line), event["learner_id"], event["problem_id"], _kind(event),
                          event_us, late, 1, 0)
    return (VALID, event["event_id"], delivery)


def is_blank(line: str) -> bool:
    return not line.strip()


# ---------------------------------------------------------------- các hàm map/reduce (dùng trên executor)

def stat_key(record: tuple[Any, ...]) -> tuple[str, int]:
    """Cặp (khóa đếm, 1) của một lần giao (word count): `valid`, `heartbeat` hoặc `invalid:<lý do>[:id]`."""
    tag = record[0]
    if tag == VALID:
        return ("valid", 1)
    if tag == HEARTBEAT:
        return ("heartbeat", 1)
    return (f"invalid:{record[1]}" + (":id" if record[2] is not None else ""), 1)


def merge_delivery(a: Delivery, b: Delivery) -> Delivery:
    """Gộp hai nhóm lần giao cùng event_id: giữ bản nhỏ nhất theo thứ tự toàn phần, cộng số lần giao, OR xung đột."""
    first = a if a[:7] <= b[:7] else b
    conflict = 1 if (a[8] or b[8] or a[1] != b[1]) else 0
    return (*first[:7], a[7] + b[7], conflict)  # type: ignore[return-value]


def dedup_stat(item: tuple[str, Delivery]) -> tuple[int, int, int, int]:
    """(1 sự kiện duy nhất, số bản trùng, xung đột, muộn)."""
    delivery = item[1]
    return (1, delivery[7] - 1, delivery[8], delivery[6])


def add_tuples(a: tuple[int, ...], b: tuple[int, ...]) -> tuple[int, ...]:
    return tuple(x + y for x, y in zip(a, b, strict=True))


def to_counter_pair(item: tuple[str, Delivery]) -> tuple[tuple[str, str], Counters]:
    _, (_, _, learner_id, problem_id, kind, event_us, _, _, _) = item
    attempts, wrong, hints, completed = _KIND_CONTRIBUTION[kind]
    return (learner_id, problem_id), (1, attempts, wrong, hints, completed, event_us, event_us)


def add_counters(a: Counters, b: Counters) -> Counters:
    return (a[0] + b[0], a[1] + b[1], a[2] + b[2], a[3] + b[3], max(a[4], b[4]), min(a[5], b[5]), max(a[6], b[6]))


def string_partition(key: str) -> int:
    """Partitioner tất định (crc32) cho khóa chuỗi; Spark lấy modulo số partition."""
    return zlib.crc32(key.encode("utf-8"))


def learner_partition(key: tuple[str, str]) -> int:
    """Partitioner theo learner_id của khóa (learner_id, problem_id)."""
    return zlib.crc32(key[0].encode("utf-8"))


def _rank(item: tuple[str, int]) -> tuple[int, str]:
    return (-item[1], item[0])


def local_top_k(k: int, items: Iterable[tuple[str, int]]) -> Iterator[list[tuple[str, int]]]:
    """Top-K cục bộ của một partition bằng heap; trả đúng một danh sách (có thể rỗng)."""
    yield heapq.nsmallest(k, items, key=_rank)


def merge_top_k(k: int, a: list[tuple[str, int]], b: list[tuple[str, int]]) -> list[tuple[str, int]]:
    """Gộp hai danh sách ứng viên thành top-K; kết hợp và giao hoán vì thứ tự là toàn phần."""
    return heapq.nsmallest(k, [*a, *b], key=_rank)


def counter_line(item: tuple[tuple[str, str], Counters]) -> str:
    (learner_id, problem_id), c = item
    return json.dumps([learner_id, problem_id, c[0], c[1], c[2], c[3], bool(c[4]), c[5] // 1000, c[6] // 1000])


def invalid_line(record: tuple[Any, ...]) -> str:
    return json.dumps({"event_id": record[2], "reason": record[1], "raw": record[3]}, ensure_ascii=False)


def _is_invalid(record: tuple[Any, ...]) -> bool:
    return record[0] == INVALID


def _is_valid(record: tuple[Any, ...]) -> bool:
    return record[0] == VALID


def _keyed_delivery(record: tuple[Any, ...]) -> tuple[str, Delivery]:
    return (record[1], record[2])


def _problem_wrong(item: tuple[tuple[str, str], Counters]) -> tuple[str, int]:
    return (item[0][1], item[1][2])


def _add(a: int, b: int) -> int:
    return a + b


# ---------------------------------------------------------------- kết quả

@dataclass
class ProblemCounts:
    """Kết quả so sánh được của job hoặc của bản tham chiếu (bộ đếm theo ms, như tệp đầu ra)."""

    stats: dict[str, int]
    invalid_by_reason: dict[str, int]
    counters: dict[tuple[str, str], tuple[int, int, int, int, bool, int, int]]
    top_k: list[tuple[str, int]]
    k: int = DEFAULT_TOP_K
    invalid_samples: list[dict[str, Any]] = field(default_factory=list)

    def to_json(self) -> dict[str, Any]:
        return {
            "stats": self.stats, "invalid_by_reason": self.invalid_by_reason, "k": self.k,
            "top_k": [list(item) for item in self.top_k],
            "counter_fields": list(COUNTER_FIELDS),
            "counters": [[learner, problem, *values] for (learner, problem), values in sorted(self.counters.items())],
            "invalid_samples": self.invalid_samples,
        }

    @classmethod
    def from_json(cls, data: Mapping[str, Any]) -> ProblemCounts:
        counters = {(row[0], row[1]): (int(row[2]), int(row[3]), int(row[4]), int(row[5]), bool(row[6]), int(row[7]),
                                       int(row[8])) for row in data["counters"]}
        return cls(stats={k: int(v) for k, v in data["stats"].items()},
                   invalid_by_reason={k: int(v) for k, v in data["invalid_by_reason"].items()},
                   counters=counters, top_k=[(str(p), int(w)) for p, w in data["top_k"]], k=int(data["k"]),
                   invalid_samples=list(data.get("invalid_samples") or []))

    def save(self, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        tmp = path.with_name(path.name + ".tmp")
        with gzip.open(tmp, "wt", encoding="utf-8") as handle:
            json.dump(self.to_json(), handle, ensure_ascii=False)
        tmp.replace(path)

    @classmethod
    def load(cls, path: Path) -> ProblemCounts:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            return cls.from_json(json.load(handle))


def _sample_invalid(samples: list[dict[str, Any]], per_reason: Counter[str], record: tuple[Any, ...], limit: int) -> None:
    if per_reason[record[1]] < limit:
        per_reason[record[1]] += 1
        samples.append({"event_id": record[2], "reason": record[1], "raw": record[3]})


def reference_counts(
    lines: Iterable[str],
    k: int = DEFAULT_TOP_K,
    late_threshold_s: float = DEFAULT_LATE_THRESHOLD_S,
    sample_per_reason: int = 5,
) -> ProblemCounts:
    """Bản tham chiếu tuần tự (không Spark) trên cùng các dòng: cùng định nghĩa, cách tính độc lập với job."""
    threshold_us = int(late_threshold_s * 1_000_000)
    deliveries = valid = heartbeats = invalid = invalid_with_id = 0
    by_reason: Counter[str] = Counter()
    samples: list[dict[str, Any]] = []
    sampled: Counter[str] = Counter()
    kept: dict[str, Delivery] = {}  # event_id → lần giao giữ lại (nhỏ nhất theo thứ tự toàn phần)
    digests: dict[str, set[int]] = {}
    for raw in lines:
        line = raw.rstrip("\r\n")
        if is_blank(line):
            continue
        deliveries += 1
        record = classify_line(line, threshold_us)
        if record[0] == INVALID:
            invalid += 1
            by_reason[record[1]] += 1
            invalid_with_id += record[2] is not None
            _sample_invalid(samples, sampled, record, sample_per_reason)
            continue
        if record[0] == HEARTBEAT:
            heartbeats += 1
            continue
        valid += 1
        event_id, delivery = record[1], record[2]
        digests.setdefault(event_id, set()).add(delivery[1])
        current = kept.get(event_id)
        if current is None or delivery[:7] < current[:7]:
            kept[event_id] = delivery

    counters: dict[tuple[str, str], list[int]] = {}
    late = 0
    for delivery in kept.values():
        _, _, learner_id, problem_id, kind, event_us, is_late, _, _ = delivery
        late += is_late
        attempts, wrong, hints, completed = _KIND_CONTRIBUTION[kind]
        row = counters.get((learner_id, problem_id))
        if row is None:
            counters[(learner_id, problem_id)] = [1, attempts, wrong, hints, completed, event_us, event_us]
            continue
        row[0] += 1
        row[1] += attempts
        row[2] += wrong
        row[3] += hints
        row[4] = row[4] or completed
        row[5] = min(row[5], event_us)
        row[6] = max(row[6], event_us)

    wrong_by_problem: Counter[str] = Counter()
    for (_, problem_id), row in counters.items():
        wrong_by_problem[problem_id] += row[2]
    top = sorted(wrong_by_problem.items(), key=lambda item: (-item[1], item[0]))[:k]
    stats = {
        "deliveries": deliveries, "valid_deliveries": valid, "heartbeats": heartbeats, "invalid": invalid,
        "invalid_with_event_id": invalid_with_id, "unique_events": len(kept),
        "duplicate_deliveries": valid - len(kept),
        "conflicting_event_ids": sum(1 for values in digests.values() if len(values) > 1),
        "late_events": late, "output_keys": len(counters),
    }
    return ProblemCounts(
        stats=stats, invalid_by_reason=dict(sorted(by_reason.items())),
        counters={key: (r[0], r[1], r[2], r[3], bool(r[4]), r[5] // 1000, r[6] // 1000) for key, r in counters.items()},
        top_k=top, k=k, invalid_samples=samples,
    )


def reference_counts_from_file(path: Path, k: int = DEFAULT_TOP_K,
                               late_threshold_s: float = DEFAULT_LATE_THRESHOLD_S) -> ProblemCounts:
    with Path(path).open(encoding="utf-8", newline="") as handle:
        return reference_counts(handle, k, late_threshold_s)


def compare_counts(actual: ProblemCounts, expected: ProblemCounts, max_diffs: int = 20) -> dict[str, Any]:
    """So kết quả job với tham chiếu: cờ khớp chính xác từng phần, và tối đa `max_diffs` sai khác mỗi phần."""
    stat_diffs = {name: {"actual": actual.stats.get(name), "expected": expected.stats.get(name)}
                  for name in sorted(set(actual.stats) | set(expected.stats))
                  if actual.stats.get(name) != expected.stats.get(name)}
    reason_diffs = {reason: {"actual": actual.invalid_by_reason.get(reason, 0),
                             "expected": expected.invalid_by_reason.get(reason, 0)}
                    for reason in sorted(set(actual.invalid_by_reason) | set(expected.invalid_by_reason))
                    if actual.invalid_by_reason.get(reason, 0) != expected.invalid_by_reason.get(reason, 0)}
    missing = sorted(set(expected.counters) - set(actual.counters))
    extra = sorted(set(actual.counters) - set(expected.counters))
    different = sorted(key for key in set(actual.counters) & set(expected.counters)
                       if actual.counters[key] != expected.counters[key])

    def totals(counts: ProblemCounts) -> list[int]:
        sums = [0, 0, 0, 0, 0]
        for values in counts.counters.values():
            for index in range(5):
                sums[index] += int(values[index])
        return sums

    checks = {
        "stats": not stat_diffs,
        "invalid_by_reason": not reason_diffs,
        "counters": not (missing or extra or different),
        "counter_totals": totals(actual) == totals(expected),
        "top_k": actual.k == expected.k and list(actual.top_k) == list(expected.top_k),
    }
    return {
        "match": all(checks.values()),
        "checks": checks,
        "diffs": {
            "stats": stat_diffs,
            "invalid_by_reason": reason_diffs,
            "counters": {
                "missing": len(missing), "extra": len(extra), "different": len(different),
                "examples": [{"key": list(key), "actual": actual.counters.get(key), "expected": expected.counters.get(key)}
                             for key in (missing + extra + different)[:max_diffs]],
            },
            "counter_totals": {"fields": list(COUNTER_FIELDS[:5]), "actual": totals(actual), "expected": totals(expected)},
            "top_k": None if checks["top_k"] else {"actual": [list(i) for i in actual.top_k],
                                                   "expected": [list(i) for i in expected.top_k]},
        },
    }


# ---------------------------------------------------------------- job Spark

def _clean_output(path: Path) -> None:
    import shutil

    if path.exists():
        shutil.rmtree(path)


def run_problem_job(
    sc: SparkContext,
    input_path: Path | str,
    out_dir: Path,
    *,
    input_partitions: int,
    partitions: int,
    k: int = DEFAULT_TOP_K,
    late_threshold_s: float = DEFAULT_LATE_THRESHOLD_S,
    job_group: str | None = None,
    on_action: Callable[[str], None] | None = None,
) -> dict[str, Any]:
    """Chạy toàn bộ job trên `input_path` (tệp văn bản, một sự kiện mỗi dòng); ghi đầu ra vào `out_dir`.

    Trả {stats, invalid_by_reason, top_k, actions: {nhãn: giây}, wall_s}. Mỗi action đặt job group
    `job_group` với mô tả là nhãn của action, để REST API của Spark tách được các stage theo bước.
    """
    from pyspark import StorageLevel

    if input_partitions < 1 or partitions < 1 or k < 1:
        raise ValueError("input_partitions, partitions và k phải ≥ 1")
    out_dir = Path(out_dir)
    for name in ("invalid", "counters"):
        _clean_output(out_dir / name)
    out_dir.mkdir(parents=True, exist_ok=True)
    threshold_us = int(late_threshold_s * 1_000_000)
    group = job_group or f"mapreduce-problems-{int(time.time() * 1000)}"
    actions: dict[str, float] = {}

    def action(label: str) -> float:
        sc.setJobGroup(group, label)
        if on_action is not None:
            on_action(label)
        return time.perf_counter()

    started = time.perf_counter()
    parsed = (sc.textFile(str(input_path), minPartitions=input_partitions)
              .filter(lambda line: not is_blank(line))
              .map(lambda line: classify_line(line, threshold_us))
              .persist(StorageLevel.MEMORY_AND_DISK))
    try:
        mark = action("stats")
        counts = dict(parsed.map(stat_key).reduceByKey(_add, partitions, partitionFunc=string_partition).collect())
        actions["stats"] = time.perf_counter() - mark

        mark = action("invalid_output")
        parsed.filter(_is_invalid).map(invalid_line).saveAsTextFile(str(out_dir / "invalid"))
        actions["invalid_output"] = time.perf_counter() - mark

        unique = parsed.filter(_is_valid).map(_keyed_delivery).reduceByKey(
            merge_delivery, partitions, partitionFunc=string_partition)
        counters = unique.map(to_counter_pair).reduceByKey(add_counters, partitions, partitionFunc=learner_partition)
        mark = action("counters_output")
        counters.map(counter_line).saveAsTextFile(str(out_dir / "counters"))
        actions["counters_output"] = time.perf_counter() - mark

        mark = action("dedup_stats")
        unique_events, duplicates, conflicts, late = unique.map(dedup_stat).fold((0, 0, 0, 0), add_tuples)
        actions["dedup_stats"] = time.perf_counter() - mark

        mark = action("top_k")
        top = (counters.map(_problem_wrong).reduceByKey(_add, partitions, partitionFunc=string_partition)
               .mapPartitions(partial(local_top_k, k)).reduce(partial(merge_top_k, k)))
        actions["top_k"] = time.perf_counter() - mark
    finally:
        parsed.unpersist()
    wall_s = time.perf_counter() - started

    by_reason: Counter[str] = Counter()
    invalid_with_id = 0
    for key, value in counts.items():
        if key.startswith("invalid:"):
            parts = key.split(":")
            by_reason[parts[1]] += value
            if len(parts) == 3:
                invalid_with_id += value
    valid = counts.get("valid", 0)
    stats = {
        "deliveries": sum(counts.values()), "valid_deliveries": valid, "heartbeats": counts.get("heartbeat", 0),
        "invalid": sum(by_reason.values()), "invalid_with_event_id": invalid_with_id, "unique_events": unique_events,
        "duplicate_deliveries": duplicates, "conflicting_event_ids": conflicts, "late_events": late,
    }
    summary = {"stats": stats, "invalid_by_reason": dict(sorted(by_reason.items())), "k": k,
               "top_k": [list(item) for item in top], "actions": {n: round(s, 3) for n, s in actions.items()},
               "wall_s": round(wall_s, 3)}
    (out_dir / "job.json").write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return summary


def _part_lines(directory: Path) -> Iterator[str]:
    for part in sorted(Path(directory).glob("part-*")):
        with part.open(encoding="utf-8") as handle:
            for line in handle:
                if line.strip():
                    yield line


def read_job_outputs(out_dir: Path, sample_per_reason: int = 5) -> ProblemCounts:
    """Đọc lại đầu ra của `run_problem_job` (job.json + part-*) thành ProblemCounts để so với tham chiếu."""
    out_dir = Path(out_dir)
    summary = json.loads((out_dir / "job.json").read_text(encoding="utf-8"))
    counters: dict[tuple[str, str], tuple[int, int, int, int, bool, int, int]] = {}
    for line in _part_lines(out_dir / "counters"):
        row = json.loads(line)
        key = (row[0], row[1])
        if key in counters:
            raise ValueError(f"khóa {key} xuất hiện hai lần trong đầu ra counters")
        counters[key] = (int(row[2]), int(row[3]), int(row[4]), int(row[5]), bool(row[6]), int(row[7]), int(row[8]))
    samples: list[dict[str, Any]] = []
    sampled: Counter[str] = Counter()
    written = 0
    for line in _part_lines(out_dir / "invalid"):
        written += 1
        row = json.loads(line)
        _sample_invalid(samples, sampled, (INVALID, row["reason"], row["event_id"], row["raw"]), sample_per_reason)
    stats = {**summary["stats"], "output_keys": len(counters)}
    if written != stats["invalid"]:
        raise ValueError(f"đầu ra invalid có {written} dòng nhưng job đếm {stats['invalid']}")
    return ProblemCounts(stats=stats, invalid_by_reason=summary["invalid_by_reason"], counters=counters,
                         top_k=[(str(p), int(w)) for p, w in summary["top_k"]], k=int(summary["k"]),
                         invalid_samples=samples)
