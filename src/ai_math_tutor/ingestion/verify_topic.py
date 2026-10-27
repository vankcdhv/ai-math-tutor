"""Đọc lại topic sau phát lại và đối soát với lịch đã phát (cửa sổ của `events.parquet`) và manifest.

Kiểm tra:
- `event_validation.validate_iter` trên mọi bản ghi theo thứ tự đọc (schema v0.2, trùng cùng nội dung,
  thống kê muộn/đảo thứ tự); heartbeat chỉ kiểm schema và bị loại khỏi thống kê người học.
- Không mất: tập `event_id` duy nhất trên topic = tập của cửa sổ đã phát (= manifest khi phát cả log).
- Không trùng ngoài ý muốn: số lần giao trùng trên topic = số bản trùng có chủ đích ⇒ producer
  idempotent không sinh thêm bản sao khi retry.
- Đúng byte: mỗi bản ghi giống từng byte với bản đóng dấu kỳ vọng (`restamp` với `replay_start`), nên
  bản trùng giống byte bản gốc.
- Thứ tự theo người học: chuỗi `event_id` của từng người học trên topic (theo offset) đúng thứ tự tệp,
  key = `learner_id`, mỗi người học nằm trong một partition.
- Drain: mọi partition có heartbeat với `event_time` > cuối log + W ⇒ watermark (max event_time − W)
  vượt cuối log.

CLI: python -m ai_math_tutor.ingestion.verify_topic --replay results/raw/replay-<run>.json [--out PATH]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import sys
import time
import uuid
from collections import defaultdict
from collections.abc import Iterator, Mapping
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

from ai_math_tutor.config import ConfigError, load_config
from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, is_heartbeat, load_problems, validate_iter
from ai_math_tutor.ingestion.replay import Stamper, iter_window_rows, summarize_window
from ai_math_tutor.simulator.output import epoch_ms, iso_from_epoch_ms, parse_utc

DEFAULT_TIMEOUT_S = 600.0
MAX_ERRORS_REPORTED = 50


class VerifyError(RuntimeError):
    """Không đọc được topic hoặc đầu vào đối soát."""


def _digest(value: bytes) -> bytes:
    return hashlib.blake2b(value, digest_size=16).digest()


@dataclass
class Expected:
    """Cái mà topic phải chứa sau khi phát cửa sổ: byte (digest) theo event_id, thứ tự theo người học."""

    digests: dict[str, bytes]
    order: dict[str, list[str]]
    rows: int
    duplicates: int
    end_of_log_ms: int
    complete: bool


def build_expected(events_path: Path, replay_start_ms: int, cutoff_ms: int | None) -> Expected:
    """Tính lại bản đóng dấu kỳ vọng — cùng `Stamper` (một lần mỗi event_id) như bộ phát lại."""
    window = summarize_window(events_path, cutoff_ms, processes=1)
    if window.rows == 0:
        raise VerifyError("cửa sổ phát lại rỗng")
    stamper = Stamper(replay_start_ms, window.shard_duplicates[0])
    digests: dict[str, bytes] = {}
    order: dict[str, list[str]] = defaultdict(list)
    for row in iter_window_rows(events_path, cutoff_ms=cutoff_ms):
        value = stamper.value_for(row)
        digests.setdefault(row.event_id, _digest(value))
        order[row.learner_id].append(row.event_id)
    return Expected(digests, dict(order), window.rows, window.duplicate_deliveries,
                    replay_start_ms + window.max_event_offset_ms, window.complete)


def consume_topic(bootstrap_servers: str, topic: str, timeout_s: float = DEFAULT_TIMEOUT_S) -> Iterator[tuple[int, int, bytes | None, bytes]]:
    """(partition, offset, key, value) từ đầu tới high watermark chụp lúc bắt đầu, theo thứ tự trong partition."""
    from confluent_kafka import Consumer, KafkaException, TopicPartition

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers,
        "group.id": f"nhom8-verify-{uuid.uuid4().hex[:8]}",
        "client.id": "nhom8-verify",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    try:
        metadata = consumer.list_topics(topic=topic, timeout=10)
        info = metadata.topics.get(topic)
        if info is None or info.error is not None:
            raise VerifyError(f"topic {topic} không tồn tại")
        bounds: dict[int, tuple[int, int]] = {}
        for partition in sorted(info.partitions):
            low, high = consumer.get_watermark_offsets(TopicPartition(topic, partition), timeout=10)
            if high > low:
                bounds[partition] = (low, high)
        if not bounds:
            return
        consumer.assign([TopicPartition(topic, p, low) for p, (low, _) in bounds.items()])
        remaining = set(bounds)
        deadline = time.monotonic() + timeout_s
        while remaining:
            if time.monotonic() > deadline:
                raise VerifyError(f"quá hạn đọc topic; còn partition {sorted(remaining)}")
            for message in consumer.consume(num_messages=10_000, timeout=1.0):
                if message.error() is not None:
                    raise VerifyError(f"lỗi đọc topic: {message.error()}")
                partition, offset = message.partition(), message.offset()
                high = bounds[partition][1]
                if offset >= high:
                    remaining.discard(partition)
                    continue
                yield partition, offset, message.key(), message.value()
                if offset >= high - 1:
                    remaining.discard(partition)
    except KafkaException as exc:
        raise VerifyError(f"lỗi Kafka khi đọc topic: {exc}") from exc
    finally:
        consumer.close()


@dataclass
class TopicCheck:
    records: int = 0
    heartbeats: int = 0
    undecodable: int = 0
    key_mismatch: int = 0
    content_mismatch: int = 0
    duplicate_bytes_mismatch: int = 0
    learners_in_many_partitions: int = 0
    max_event_ms: int | None = None
    max_learning_event_ms: int | None = None
    heartbeat_max_event_ms: dict[int, int] = field(default_factory=dict)


def _event_ms(event: Mapping[str, Any]) -> int | None:
    try:
        return epoch_ms(parse_utc(str(event["event_time"])))
    except (KeyError, ValueError):
        return None


def verify_topic(
    *,
    bootstrap_servers: str,
    topic: str,
    events_path: Path,
    replay_start_ms: int,
    cutoff_ms: int | None,
    watermark_s: float,
    manifest: Mapping[str, Any] | None = None,
    problems_path: Path | None = None,
    require_heartbeat: bool = True,
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, Any]:
    started = time.monotonic()
    expected = build_expected(events_path, replay_start_ms, cutoff_ms)
    check = TopicCheck()
    first_digest: dict[str, bytes] = {}
    got_order: dict[str, list[str]] = defaultdict(list)
    learner_partition: dict[str, int] = {}
    split_learners: set[str] = set()
    partitions_seen: set[int] = set()

    def records() -> Iterator[Any]:
        for partition, _offset, key, value in consume_topic(bootstrap_servers, topic, timeout_s):
            check.records += 1
            partitions_seen.add(partition)
            try:
                event = json.loads(value)
            except (TypeError, ValueError):
                check.undecodable += 1
                yield None  # validator ghi lỗi schema cho bản ghi này
                continue
            if not isinstance(event, dict):
                yield event
                continue
            moment = _event_ms(event)
            if moment is not None and (check.max_event_ms is None or moment > check.max_event_ms):
                check.max_event_ms = moment
            if is_heartbeat(event):
                check.heartbeats += 1
                if key != HEARTBEAT_LEARNER_ID.encode():
                    check.key_mismatch += 1
                if moment is not None:
                    check.heartbeat_max_event_ms[partition] = max(moment, check.heartbeat_max_event_ms.get(partition, moment))
                yield event
                continue
            event_id, learner = str(event.get("event_id")), str(event.get("learner_id"))
            if key is None or key != learner.encode():
                check.key_mismatch += 1
            digest = _digest(value)
            previous = first_digest.get(event_id)
            if previous is None:
                first_digest[event_id] = digest
            elif previous != digest:
                check.duplicate_bytes_mismatch += 1
            wanted = expected.digests.get(event_id)
            if wanted is not None and wanted != digest:
                check.content_mismatch += 1
            got_order[learner].append(event_id)
            if learner_partition.setdefault(learner, partition) != partition:
                split_learners.add(learner)
            if moment is not None and (check.max_learning_event_ms is None or moment > check.max_learning_event_ms):
                check.max_learning_event_ms = moment
            yield event

    problems = load_problems(problems_path) if problems_path else None
    report = validate_iter(records(), problems)
    check.learners_in_many_partitions = len(split_learners)

    topic_ids, expected_ids = first_digest.keys(), expected.digests.keys()
    missing = len(expected_ids - topic_ids)
    unexpected = len(topic_ids - expected_ids)
    extra_duplicates = report.duplicate_deliveries - expected.duplicates
    order_mismatch = sum(1 for learner in set(expected.order) | set(got_order)
                         if got_order.get(learner, []) != expected.order.get(learner, []))
    watermark_ms = None if check.max_event_ms is None else check.max_event_ms - round(watermark_s * 1000)
    drain_threshold = expected.end_of_log_ms + round(watermark_s * 1000)
    heartbeat_partitions = sorted(p for p, ms in check.heartbeat_max_event_ms.items() if ms > drain_threshold)
    all_partitions = _partition_total(bootstrap_servers, topic)
    drained = watermark_ms is not None and watermark_ms > expected.end_of_log_ms \
        and len(heartbeat_partitions) == all_partitions

    manifest_counts = (manifest or {}).get("counts") or {}
    manifest_match = None
    if expected.complete and manifest_counts:
        manifest_match = (len(expected.digests) == manifest_counts.get("unique_events")
                          and expected.duplicates == manifest_counts.get("duplicate_deliveries"))

    failures = {
        "validator_errors": len(report.errors),
        "missing_events": missing,
        "unexpected_events": unexpected,
        "extra_duplicates": extra_duplicates,
        "content_mismatch": check.content_mismatch,
        "duplicate_bytes_mismatch": check.duplicate_bytes_mismatch,
        "key_mismatch": check.key_mismatch,
        "order_mismatch_learners": order_mismatch,
        "learners_in_many_partitions": check.learners_in_many_partitions,
        "undecodable": check.undecodable,
    }
    ok = all(value == 0 for value in failures.values()) and (drained or not require_heartbeat) \
        and manifest_match is not False
    validation = asdict(report)
    validation["errors"] = report.errors[:MAX_ERRORS_REPORTED]
    validation["error_count"] = len(report.errors)
    validation["seq_gap_sessions"] = len(report.seq_gaps)
    validation.pop("seq_gaps")
    return {
        "ok": ok,
        "topic": topic,
        "partitions": all_partitions,
        "partitions_with_records": sorted(partitions_seen),
        "events_path": str(events_path),
        "replay_start_ms": replay_start_ms,
        "cutoff_ms": cutoff_ms,
        "expected": {
            "deliveries": expected.rows,
            "unique_events": len(expected.digests),
            "duplicate_deliveries": expected.duplicates,
            "window_complete": expected.complete,
            "end_of_log": iso_from_epoch_ms(expected.end_of_log_ms),
        },
        "manifest_counts": {k: manifest_counts.get(k) for k in ("unique_events", "duplicate_deliveries", "deliveries")}
        if manifest_counts else None,
        "manifest_match": manifest_match,
        "topic_records": check.records,
        "topic_unique_events": len(first_digest),
        "topic_duplicate_deliveries": report.duplicate_deliveries,
        "heartbeats": check.heartbeats,
        "failures": failures,
        "drain": {
            "required": require_heartbeat,
            "watermark_s": watermark_s,
            "max_event_time": iso_from_epoch_ms(check.max_event_ms) if check.max_event_ms is not None else None,
            "watermark": iso_from_epoch_ms(watermark_ms) if watermark_ms is not None else None,
            "watermark_past_end_of_log": watermark_ms is not None and watermark_ms > expected.end_of_log_ms,
            "heartbeat_partitions": heartbeat_partitions,
            "all_partitions_heartbeat": len(heartbeat_partitions) == all_partitions,
        },
        "validation": validation,
        "wall_s": round(time.monotonic() - started, 3),
    }


def _partition_total(bootstrap_servers: str, topic: str) -> int:
    from ai_math_tutor.ingestion.topics import TopicError, partition_count

    try:
        return partition_count(bootstrap_servers, topic)
    except TopicError as exc:
        raise VerifyError(str(exc)) from exc


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--replay", type=Path, required=True, help="replay-<run>.json do bộ phát lại ghi")
    parser.add_argument("--events", type=Path, default=None, help="ghi đè events.parquet")
    parser.add_argument("--manifest", type=Path, default=None, help="mặc định manifest.json cạnh events.parquet")
    parser.add_argument("--topic", default=None)
    parser.add_argument("--bootstrap", default=None)
    parser.add_argument("--problems", type=Path, default=None, help="kho bài để kiểm thêm nhãn toán (chậm)")
    parser.add_argument("--no-heartbeat-check", action="store_true")
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    parser.add_argument("--out", type=Path, default=None, help="mặc định verify-<run>.json cạnh --replay")
    args = parser.parse_args(argv)
    try:
        replay = json.loads(args.replay.read_text(encoding="utf-8"))
        cfg = load_config()
        events_path = args.events or Path(replay["events_path"])
        manifest_path = args.manifest or events_path.with_name("manifest.json")
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else None
        result = verify_topic(
            bootstrap_servers=args.bootstrap or cfg.kafka.bootstrap_servers,
            topic=args.topic or replay["topic"],
            events_path=events_path,
            replay_start_ms=int(replay["replay_start_ms"]),
            cutoff_ms=replay.get("cutoff_ms"),
            watermark_s=float(replay["watermark_s"]),
            manifest=manifest,
            problems_path=args.problems,
            require_heartbeat=not args.no_heartbeat_check and replay.get("heartbeat") is not None,
            timeout_s=args.timeout_s,
        )
    except (OSError, KeyError, ValueError, VerifyError, ConfigError) as exc:
        print(f"lỗi: {exc}", file=sys.stderr)
        return 1
    result["run_id"] = replay.get("run_id")
    out = args.out or args.replay.with_name(f"verify-{replay.get('run_id') or 'run'}.json")
    out.write_text(json.dumps(result, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    brief = {k: result[k] for k in ("ok", "topic_records", "topic_unique_events", "topic_duplicate_deliveries",
                                    "heartbeats", "expected", "manifest_match", "failures")}
    brief["drain"] = result["drain"]
    brief["out"] = str(out)
    print(json.dumps(brief, ensure_ascii=False, indent=2))
    return 0 if result["ok"] else 1


if __name__ == "__main__":
    sys.exit(main())
