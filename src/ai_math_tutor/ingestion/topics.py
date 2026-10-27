"""Tạo, xóa, tạo lại topic Kafka của dự án qua AdminClient — idempotent.

Broker tắt tự tạo topic (`KAFKA_AUTO_CREATE_TOPICS_ENABLE=false`), nên mọi topic phải tạo
tường minh trước khi gửi/đọc và số partition không bao giờ lấy mặc định ngầm của broker.

- `learner-events`: số partition là tham số thí nghiệm (1/3/6). Kafka chỉ cho tăng, không cho
  giảm partition, và tăng thì đổi ánh xạ key → partition giữa run ⇒ "đổi số partition" ở đây
  luôn là xóa rồi tạo lại (`recreate`), topic rỗng.
- `alerts`, `dead-letter`, `hint-requests`: số partition cố định (mặc định 3).

Gọi lại với cùng cấu hình không làm gì (giữ topic và dữ liệu). Topic đã có nhưng khác số
partition mà không yêu cầu tạo lại → `TopicError`, không âm thầm chạy với cấu hình sai.

CLI: python -m ai_math_tutor.ingestion.topics create [--partitions N] [--recreate | --recreate-all] [--topic TÊN]
     python -m ai_math_tutor.ingestion.topics delete [--all | TÊN ...]
     python -m ai_math_tutor.ingestion.topics describe
"""

from __future__ import annotations

import argparse
import json
import re
import sys
import time
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from ai_math_tutor.config import TOPIC_KEYS, ConfigError, RuntimeConfig, load_config

if TYPE_CHECKING:  # chỉ để gợi ý kiểu; confluent_kafka được import lười khi cần broker
    from confluent_kafka.admin import AdminClient

LEARNER_EVENTS_KEY = "learner_events"
AUX_TOPIC_KEYS = tuple(key for key in TOPIC_KEYS if key != LEARNER_EVENTS_KEY)
EXPERIMENT_PARTITIONS = (1, 3, 6)
DEFAULT_AUX_PARTITIONS = 3
# Một broker ⇒ hệ số nhân bản 1 (giới hạn của nguyên mẫu một máy).
REPLICATION_FACTOR = 1
DEFAULT_TIMEOUT_S = 60.0
_POLL_INTERVAL_S = 0.25
# Tên topic Kafka hợp lệ: ký tự [a-zA-Z0-9._-], dài tối đa 249, không phải "." hay "..".
_TOPIC_NAME_RE = re.compile(r"^[A-Za-z0-9._-]{1,249}$")


class TopicError(RuntimeError):
    """Topic không ở trạng thái mong muốn (sai số partition, tạo/xóa thất bại, quá hạn)."""


@dataclass(frozen=True)
class TopicSpec:
    name: str
    partitions: int
    replication_factor: int = REPLICATION_FACTOR

    def __post_init__(self) -> None:
        if not isinstance(self.name, str) or not _TOPIC_NAME_RE.match(self.name) or self.name in (".", ".."):
            raise ValueError(f"tên topic không hợp lệ: {self.name!r}")
        if isinstance(self.partitions, bool) or not isinstance(self.partitions, int) or self.partitions < 1:
            raise ValueError(f"số partition phải là số nguyên ≥ 1, nhận {self.partitions!r}")
        if isinstance(self.replication_factor, bool) or not isinstance(self.replication_factor, int) \
                or self.replication_factor < 1:
            raise ValueError(f"replication_factor phải ≥ 1, nhận {self.replication_factor!r}")


@dataclass(frozen=True)
class TopicAction:
    """Một bước trong kế hoạch: `create`, `delete` (trước khi tạo lại) hoặc `keep`."""

    kind: str
    spec: TopicSpec


def topic_specs(
    cfg: RuntimeConfig,
    learner_partitions: int | None = None,
    aux_partitions: int = DEFAULT_AUX_PARTITIONS,
) -> list[TopicSpec]:
    """Bốn topic của dự án theo tên trong `configs/runtime.yaml`; learner-events lấy `cfg.kafka.partitions` nếu bỏ trống."""
    partitions = cfg.kafka.partitions if learner_partitions is None else learner_partitions
    specs = [TopicSpec(cfg.kafka.topics[LEARNER_EVENTS_KEY], partitions)]
    specs.extend(TopicSpec(cfg.kafka.topics[key], aux_partitions) for key in AUX_TOPIC_KEYS)
    return specs


def plan_actions(
    existing: Mapping[str, int],
    specs: Sequence[TopicSpec],
    recreate: Iterable[str] = (),
) -> list[TopicAction]:
    """Kế hoạch thuần (không gọi broker): so topic đang có (tên → số partition) với cấu hình mong muốn."""
    names = [spec.name for spec in specs]
    if len(set(names)) != len(names):
        raise ValueError(f"tên topic bị lặp trong cấu hình: {names}")
    recreate_set = set(recreate)
    unknown = recreate_set - set(names)
    if unknown:
        raise ValueError(f"yêu cầu tạo lại topic không có trong cấu hình: {sorted(unknown)}")
    actions: list[TopicAction] = []
    for spec in specs:
        current = existing.get(spec.name)
        if current is None:
            actions.append(TopicAction("create", spec))
        elif spec.name in recreate_set:
            actions.extend([TopicAction("delete", spec), TopicAction("create", spec)])
        elif current == spec.partitions:
            actions.append(TopicAction("keep", spec))
        else:
            raise TopicError(
                f"topic {spec.name} đang có {current} partition, cấu hình cần {spec.partitions}; "
                "tạo lại (xóa dữ liệu) bằng recreate"
            )
    return actions


def admin_client(bootstrap_servers: str) -> AdminClient:
    from confluent_kafka.admin import AdminClient

    return AdminClient({"bootstrap.servers": bootstrap_servers, "client.id": "nhom8-topics"})


def describe_topics(admin: AdminClient, timeout_s: float = 10.0) -> dict[str, int]:
    """Topic người dùng đang có (bỏ topic nội bộ `__*`) → số partition."""
    from confluent_kafka import KafkaException

    try:
        metadata = admin.list_topics(timeout=timeout_s)
    except KafkaException as exc:
        raise TopicError(f"không đọc được metadata từ broker: {exc}") from exc
    return {
        name: len(topic.partitions)
        for name, topic in metadata.topics.items()
        if not name.startswith("__") and topic.error is None
    }


def _topic_ready(admin: AdminClient, spec: TopicSpec, timeout_s: float) -> bool:
    """Topic có đủ partition và mọi partition đã có leader (gửi/đọc được ngay)."""
    from confluent_kafka import KafkaException

    try:
        topic = admin.list_topics(topic=spec.name, timeout=timeout_s).topics.get(spec.name)
    except KafkaException:
        return False
    if topic is None or topic.error is not None or len(topic.partitions) != spec.partitions:
        return False
    return all(p.leader >= 0 and p.error is None for p in topic.partitions.values())


def _wait(predicate: Any, deadline: float, what: str) -> None:
    while not predicate():
        if time.monotonic() >= deadline:
            raise TopicError(f"quá hạn khi chờ {what}")
        time.sleep(_POLL_INTERVAL_S)


def _delete(admin: AdminClient, names: Sequence[str], deadline: float, missing_ok: bool) -> list[str]:
    from confluent_kafka import KafkaError, KafkaException

    if not names:
        return []
    remaining = max(1.0, deadline - time.monotonic())
    futures = admin.delete_topics(list(names), operation_timeout=remaining, request_timeout=remaining)
    deleted: list[str] = []
    for name, future in futures.items():
        try:
            future.result()
            deleted.append(name)
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            if missing_ok and isinstance(error, KafkaError) and error.code() == KafkaError.UNKNOWN_TOPIC_OR_PART:
                continue
            raise TopicError(f"xóa topic {name} thất bại: {exc}") from exc
    # Xóa là bất đồng bộ: chờ metadata không còn topic rồi mới cho tạo lại cùng tên.
    _wait(lambda: not set(deleted) & set(describe_topics(admin)), deadline, f"xóa xong {deleted}")
    return deleted


def _create(admin: AdminClient, spec: TopicSpec, deadline: float) -> None:
    from confluent_kafka import KafkaError, KafkaException
    from confluent_kafka.admin import NewTopic

    while True:
        remaining = max(1.0, deadline - time.monotonic())
        new_topic = NewTopic(spec.name, num_partitions=spec.partitions, replication_factor=spec.replication_factor)
        future = admin.create_topics([new_topic], operation_timeout=remaining, request_timeout=remaining)[spec.name]
        try:
            future.result()
            break
        except KafkaException as exc:
            error = exc.args[0] if exc.args else None
            code = error.code() if isinstance(error, KafkaError) else None
            if code == KafkaError.TOPIC_ALREADY_EXISTS:
                current = describe_topics(admin).get(spec.name)
                if current == spec.partitions:
                    break  # tiến trình khác vừa tạo cùng cấu hình
                if current is not None:
                    raise TopicError(f"topic {spec.name} đã có với {current} partition") from exc
                # Topic cũ đang được xóa nốt: thử lại tới hạn.
            elif not (isinstance(error, KafkaError) and error.retriable()):
                raise TopicError(f"tạo topic {spec.name} thất bại: {exc}") from exc
            if time.monotonic() >= deadline:
                raise TopicError(f"quá hạn khi tạo topic {spec.name}: {exc}") from exc
            time.sleep(_POLL_INTERVAL_S)
    _wait(lambda: _topic_ready(admin, spec, timeout_s=5.0), deadline, f"topic {spec.name} sẵn sàng")


def ensure_topics(
    bootstrap_servers: str,
    specs: Sequence[TopicSpec],
    *,
    recreate: Iterable[str] = (),
    timeout_s: float = DEFAULT_TIMEOUT_S,
) -> dict[str, str]:
    """Đưa broker về đúng `specs`; trả tên topic → `created` | `recreated` | `kept`.

    `recreate`: tên topic phải xóa và tạo lại rỗng (đổi số partition, làm sạch giữa các run).
    """
    deadline = time.monotonic() + timeout_s
    admin = admin_client(bootstrap_servers)
    actions = plan_actions(describe_topics(admin), specs, recreate)
    _delete(admin, [a.spec.name for a in actions if a.kind == "delete"], deadline, missing_ok=True)
    deleted = {a.spec.name for a in actions if a.kind == "delete"}
    summary: dict[str, str] = {}
    for action in actions:
        if action.kind == "create":
            _create(admin, action.spec, deadline)
            summary[action.spec.name] = "recreated" if action.spec.name in deleted else "created"
        elif action.kind == "keep":
            summary[action.spec.name] = "kept"
    return summary


def delete_topics(
    bootstrap_servers: str,
    names: Sequence[str],
    *,
    timeout_s: float = DEFAULT_TIMEOUT_S,
    missing_ok: bool = True,
) -> list[str]:
    """Xóa các topic (đợi đến khi metadata không còn); trả danh sách thực sự đã xóa."""
    for name in names:
        TopicSpec(name, 1)  # kiểm tên
    admin = admin_client(bootstrap_servers)
    existing = describe_topics(admin)
    targets = [name for name in names if name in existing]
    missing = [name for name in names if name not in existing]
    if missing and not missing_ok:
        raise TopicError(f"topic không tồn tại: {missing}")
    return _delete(admin, targets, time.monotonic() + timeout_s, missing_ok=missing_ok)


def partition_count(bootstrap_servers: str, topic: str, timeout_s: float = 10.0) -> int:
    """Số partition của một topic; `TopicError` nếu topic chưa được tạo."""
    count = describe_topics(admin_client(bootstrap_servers), timeout_s).get(topic)
    if count is None:
        raise TopicError(f"topic {topic} chưa được tạo (broker tắt tự tạo topic) — chạy: make topics")
    return count


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--bootstrap", help="ghi đè bootstrap servers (mặc định theo configs/runtime.yaml)")
    parser.add_argument("--timeout-s", type=float, default=DEFAULT_TIMEOUT_S)
    sub = parser.add_subparsers(dest="command", required=True)
    create = sub.add_parser("create", help="tạo các topic còn thiếu (idempotent)")
    create.add_argument("--partitions", type=int, default=None,
                        help=f"số partition learner-events (thí nghiệm: {EXPERIMENT_PARTITIONS}); mặc định theo cấu hình")
    create.add_argument("--aux-partitions", type=int, default=DEFAULT_AUX_PARTITIONS)
    create.add_argument("--topic", default=None,
                        help="chỉ tạo một topic sự kiện tên này (vd. learner-events-<run_id>) thay cho bốn topic dự án")
    group = create.add_mutually_exclusive_group()
    group.add_argument("--recreate", action="store_true", help="xóa và tạo lại learner-events (mất dữ liệu)")
    group.add_argument("--recreate-all", action="store_true", help="xóa và tạo lại cả bốn topic")
    delete = sub.add_parser("delete", help="xóa topic")
    delete.add_argument("names", nargs="*")
    delete.add_argument("--all", action="store_true", help="xóa cả bốn topic của dự án")
    sub.add_parser("describe", help="liệt kê topic và số partition")
    args = parser.parse_args(argv)

    try:
        cfg = load_config()
        bootstrap = args.bootstrap or cfg.kafka.bootstrap_servers
        if args.command == "create":
            specs = topic_specs(cfg, args.partitions, args.aux_partitions)
            if args.topic:
                specs = [TopicSpec(args.topic, specs[0].partitions)]
            recreate = [s.name for s in specs] if args.recreate_all else [specs[0].name] if args.recreate else []
            result: Any = ensure_topics(bootstrap, specs, recreate=recreate, timeout_s=args.timeout_s)
        elif args.command == "delete":
            names = list(cfg.kafka.topics.values()) if args.all else args.names
            if not names:
                parser.error("cần TÊN topic hoặc --all")
            result = {"deleted": delete_topics(bootstrap, names, timeout_s=args.timeout_s)}
        else:
            result = describe_topics(admin_client(bootstrap))
    except (TopicError, ConfigError, ValueError) as exc:
        print(f"lỗi: {exc}", file=sys.stderr)
        return 1
    print(json.dumps(result, ensure_ascii=False, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
