"""In thân yêu cầu `POST /hints` cho demo: người học vừa có cảnh báo bế tắc, xin gợi ý nhiều lần ở cùng bước.

Chọn cảnh báo `stuck` mới nhất trong `alerts:stream` (dưới tiền tố khóa của demo) mà người học đã có trạng
thái kỹ năng trong Redis (để dịch vụ đọc được trạng thái, `state_source = redis`). Chọn một bài trong kho có
kỹ năng chính là kỹ năng đó, người học chưa làm (không có `progress:`, tức chưa được phép đưa đáp án), và
bước đầu tiên ra đáp án của bài. Hỏi N lần ở bước đó với N `event_id` khác nhau thì bậc gợi ý tăng dần;
tới bậc 3, gợi ý chỉ đưa dạng của bước với số đã che bằng □ (đáp án bị khóa).

Mỗi dòng stdout là một thân JSON; người học, bài, bước được chọn in ra stderr để đối chiếu trên dashboard.

Chạy trong container hint-api (đã có mã, kho bài, mật khẩu Redis):
    docker compose exec -T hint-api python docker/demo-hint.py --count 3 > /tmp/demo-hints.jsonl
"""

from __future__ import annotations

import argparse
import json
import os
import secrets
import sys
import uuid
from collections.abc import Iterator
from datetime import datetime, timezone
from typing import Any

import redis

from ai_math_tutor.config import ConfigError, load_config
from ai_math_tutor.hints.generator import default_problem_bank_path
from ai_math_tutor.hints.verify import build_facts
from ai_math_tutor.problem_bank import Problem, ProblemBank, ProblemBankError
from ai_math_tutor.streaming.sinks import RedisKeys

# Số mục mới nhất của alerts:stream được xét; đủ để gặp một cảnh báo stuck khi demo đang chạy.
RECENT_ALERTS = 500
CLIENT = {"platform": "web", "app_version": "0.1.0", "connection": "wifi"}


def _text(value: Any) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def recent_stuck_alerts(client: Any, keys: RedisKeys) -> Iterator[dict[str, str]]:
    """Cảnh báo `stuck` từ mới tới cũ (mỗi người học một lần)."""
    seen: set[str] = set()
    for _entry_id, fields in client.xrevrange(keys.alerts_stream, count=RECENT_ALERTS):
        row = {_text(k): _text(v) for k, v in fields.items()}
        if row.get("alert_type") != "stuck" or row.get("learner_id") in seen:
            continue
        seen.add(row.get("learner_id", ""))
        yield row


def answer_step(problem: Problem) -> int | None:
    """Bước đầu tiên ra đáp án của bài đã qua kiểm SymPy; None nếu bài không đạt."""
    facts = build_facts(problem)
    if not facts.verified:
        return None
    return next((step.index for step in facts.steps if step.reveals_answer), None)


def pick(client: Any, keys: RedisKeys, bank: ProblemBank) -> tuple[dict[str, str], Problem, int] | None:
    for alert in recent_stuck_alerts(client, keys):
        learner, skill = alert.get("learner_id", ""), alert.get("skill_id", "")
        if not learner or not skill or not client.hexists(keys.state(learner), skill):
            continue
        for problem in bank.by_primary_skill(skill):
            if client.exists(keys.progress(learner, problem.problem_id)):
                continue
            step = answer_step(problem)
            if step is not None:
                return alert, problem, step
    return None


def request_bodies(alert: dict[str, str], problem: Problem, step: int, count: int) -> list[dict[str, Any]]:
    session = f"S-{secrets.token_hex(4)}"
    now = datetime.now(timezone.utc)
    stamp = now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"
    return [{
        "event_id": str(uuid.uuid4()),
        "learner_id": alert["learner_id"],
        "class_id": alert["class_id"],
        "session_id": session,
        "session_seq": seq,
        "problem_id": problem.problem_id,
        "skill_ids": list(problem.skill_ids),
        "step_index": step,
        "event_time": stamp,
        "sent_time": stamp,
        "client": dict(CLIENT),
    } for seq in range(1, count + 1)]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--count", type=int, default=3, help="số yêu cầu (mỗi yêu cầu một event_id)")
    parser.add_argument("--key-prefix", default=os.environ.get("DEMO_KEY_PREFIX", ""),
                        help="tiền tố khóa Redis của run demo (mặc định biến DEMO_KEY_PREFIX)")
    args = parser.parse_args(argv)
    if not 1 <= args.count <= 10:
        parser.error("--count phải trong khoảng 1–10 (giới hạn tức thời của dịch vụ là 10 yêu cầu/người học)")
    try:
        client = redis.Redis.from_url(load_config().redis.url)
        bank = ProblemBank.from_jsonl(default_problem_bank_path())
        chosen = pick(client, RedisKeys(args.key_prefix), bank)
    except (ConfigError, ProblemBankError, FileNotFoundError, redis.RedisError) as exc:
        print(f"lỗi: {exc}", file=sys.stderr)
        return 1
    if chosen is None:
        print("chưa có cảnh báo stuck nào dùng được dưới tiền tố "
              f"{args.key_prefix!r}; đợi job xử lý thêm vài batch rồi chạy lại", file=sys.stderr)
        return 1
    alert, problem, step = chosen
    print(f"người học {alert['learner_id']} · lớp {alert['class_id']} · kỹ năng {alert['skill_id']} · "
          f"bài {problem.problem_id} · bước {step}/{len(problem.steps)}", file=sys.stderr)
    for body in request_bodies(alert, problem, step, args.count):
        print(json.dumps(body, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    sys.exit(main())
