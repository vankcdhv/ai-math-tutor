"""Đọc trạng thái người học và ghi ngữ cảnh gợi ý trên Redis cho dịch vụ gợi ý.

Tên khóa: `state:` và `progress:` lấy từ `streaming.sinks.RedisKeys` (nguồn tên khóa duy nhất của
pipeline, cùng tiền tố). Dịch vụ gợi ý sở hữu thêm hai khóa, dựng cùng tiền tố đó:

| Khóa | Kiểu | Nội dung | TTL |
|---|---|---|---|
| `hintctx:{learner}:{problem}:{step}` | set | `h:<event_id>` mỗi yêu cầu gợi ý, `w:<event_id>` khi yêu cầu kèm biểu thức sai | 1 h, gia hạn mỗi yêu cầu |
| `lasthint:{learner}` | string JSON | gợi ý gần nhất: `event_id, problem_id, step_index, hint_level, source, verified, misconception, state_source, text_vi, served_ms` | 1 h |

`hint_count` = số phần tử `h:`, `step_wrong_count` = số phần tử `w:`. Đếm bằng tập `event_id` thay vì
INCR thuần để client gửi lại cùng yêu cầu (retry, cùng `event_id`) không bị đếm hai lần — nếu không,
một lần retry có thể đẩy người học lên bậc cao hơn.

Một yêu cầu = **một** transaction MULTI/EXEC (một vòng mạng, nguyên tử): HGET trạng thái kỹ năng,
HGETALL tiến độ, SADD + EXPIRE + SMEMBERS ngữ cảnh. Mọi lỗi Redis (mất kết nối, quá hạn, JSON hỏng)
cho `state_source = "default"` — tầng chọn bậc khi đó fail-closed về bậc 1.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any

from ai_math_tutor.streaming.sinks import RedisKeys

LOGGER = logging.getLogger(__name__)

HINTCTX_TTL_S = 3600
LASTHINT_TTL_S = 3600
HINT_MEMBER = "h:"
WRONG_MEMBER = "w:"
DEFAULT_SOCKET_TIMEOUT_S = 0.25


@dataclass(frozen=True)
class HintKeys:
    """Khóa Redis mà dịch vụ gợi ý đọc/ghi; mọi tên khóa ủy quyền cho `RedisKeys` của pipeline."""

    base: RedisKeys = RedisKeys()

    @property
    def prefix(self) -> str:
        return self.base.prefix

    def state(self, learner_id: str) -> str:
        return self.base.state(learner_id)

    def progress(self, learner_id: str, problem_id: str) -> str:
        return self.base.progress(learner_id, problem_id)

    def hintctx(self, learner_id: str, problem_id: str, step_index: int) -> str:
        return self.base.hintctx(learner_id, problem_id, step_index)

    def lasthint(self, learner_id: str) -> str:
        return self.base.lasthint(learner_id)


@dataclass(frozen=True, slots=True)
class LearnerSnapshot:
    state_source: str  # "redis" | "default"
    hint_count: int = 1
    step_wrong_count: int = 0
    consec_wrong: int = 0
    stuck: bool = False
    wrong_answers: int = 0
    error: str | None = None

    @classmethod
    def default(cls, error: str | None = None, wrong: bool = False) -> LearnerSnapshot:
        return cls(state_source="default", step_wrong_count=1 if wrong else 0, error=error)


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _non_negative_int(value: Any) -> int:
    number = int(_text(value))
    if number < 0:
        raise ValueError(f"giá trị âm: {number}")
    return number


def parse_skill_state(raw: Any) -> dict[str, Any] | None:
    """JSON trạng thái một kỹ năng (`sinks.state_value`); None nếu thiếu hoặc hỏng."""
    if raw is None:
        return None
    try:
        value = json.loads(_text(raw))
    except (ValueError, UnicodeDecodeError):
        return None
    return value if isinstance(value, dict) else None


def _members(raw: Any) -> list[str]:
    return [_text(member) for member in (raw or ())]


class HintStateStore:
    """Bao một client redis-py (đồng bộ) cho đường nhanh của dịch vụ gợi ý. An toàn đa luồng như redis-py."""

    def __init__(self, client: Any, keys: HintKeys | None = None) -> None:
        self._client = client
        self.keys = keys or HintKeys()

    def record_and_read(self, *, learner_id: str, problem_id: str, step_index: int, skill_id: str,
                        event_id: str, wrong: bool) -> LearnerSnapshot:
        """Ghi yêu cầu vào `hintctx:` và đọc trạng thái trong một transaction; lỗi → snapshot mặc định."""
        ctx_key = self.keys.hintctx(learner_id, problem_id, step_index)
        members = [HINT_MEMBER + event_id] + ([WRONG_MEMBER + event_id] if wrong else [])
        try:
            pipe = self._client.pipeline(transaction=True)
            pipe.hget(self.keys.state(learner_id), skill_id)
            pipe.hgetall(self.keys.progress(learner_id, problem_id))
            pipe.sadd(ctx_key, *members)
            pipe.expire(ctx_key, HINTCTX_TTL_S)
            pipe.smembers(ctx_key)
            raw_state, raw_progress, _, _, raw_members = pipe.execute()
        except Exception as exc:  # noqa: BLE001 — mọi lỗi Redis/mạng đều fail-closed, không làm hỏng yêu cầu
            LOGGER.warning("Redis không đọc được trạng thái: %s", type(exc).__name__)
            return LearnerSnapshot.default(error=type(exc).__name__, wrong=wrong)
        ctx = _members(raw_members)
        hint_count = max(1, sum(m.startswith(HINT_MEMBER) for m in ctx))
        step_wrong = sum(m.startswith(WRONG_MEMBER) for m in ctx)
        state = parse_skill_state(raw_state)
        progress = {_text(k): v for k, v in (raw_progress or {}).items()}
        try:
            wrong_answers = _non_negative_int(progress.get("wrong_answers", 0))
            consec_wrong = _non_negative_int(state.get("consec_wrong", 0)) if state else 0
        except (TypeError, ValueError):
            state, wrong_answers, consec_wrong = None, 0, 0
        if state is None:
            # Redis sống nhưng chưa có trạng thái kỹ năng (hoặc hỏng): coi như không có trạng thái.
            return LearnerSnapshot(state_source="default", hint_count=hint_count, step_wrong_count=step_wrong,
                                   error="state_missing")
        return LearnerSnapshot(
            state_source="redis",
            hint_count=hint_count,
            step_wrong_count=step_wrong,
            consec_wrong=consec_wrong,
            stuck=bool(state.get("stuck")),
            wrong_answers=wrong_answers,
        )

    def write_lasthint(self, learner_id: str, record: Mapping[str, Any]) -> bool:
        """Ghi gợi ý gần nhất (TTL 1 h) cho dashboard; trả False khi Redis lỗi (không ném)."""
        try:
            self._client.set(self.keys.lasthint(learner_id), json.dumps(dict(record), ensure_ascii=False,
                                                                           separators=(",", ":")),
                             ex=LASTHINT_TTL_S)
        except Exception as exc:  # noqa: BLE001 — ghi phụ cho dashboard, không được làm hỏng yêu cầu
            LOGGER.warning("Không ghi được lasthint: %s", type(exc).__name__)
            return False
        return True

    def ping(self) -> bool:
        try:
            return bool(self._client.ping())
        except Exception:  # noqa: BLE001 — healthz chỉ báo trạng thái
            return False


def redis_client(url: str, socket_timeout_s: float = DEFAULT_SOCKET_TIMEOUT_S) -> Any:
    """Client redis-py cho đường nhanh: hạn ngắn, **không** retry/backoff (redis-py mặc định có retry,
    khiến yêu cầu treo hàng giây khi Redis chết thay vì fail-closed ngay)."""
    import redis
    from redis.backoff import NoBackoff
    from redis.retry import Retry

    return redis.Redis.from_url(url, socket_timeout=socket_timeout_s, socket_connect_timeout=socket_timeout_s,
                                retry=Retry(NoBackoff(), 0), retry_on_timeout=False, health_check_interval=0)
