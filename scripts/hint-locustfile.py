"""Locust (headless) cho dịch vụ gợi ý: mỗi user là một người học trong fixture, gửi phong bì hợp lệ.

Do `scripts/hint-load-test.py` gọi; đọc cấu hình từ biến môi trường:
  HINT_FIXTURE         fixture JSON (người học, bài, bước, biểu thức) — đã nạp trạng thái vào Redis
  HINT_API_KEY         khóa API của lần chạy (sinh ngẫu nhiên mỗi run, không ghi ra đĩa)
  HINT_LOG             tệp JSONL ghi từng yêu cầu: t_ms, latency_ms, status, state_source, level, source
  HINT_ATTACK_FRACTION tỉ lệ yêu cầu mang biểu thức tấn công `9^9^9 = x` (mặc định 0)
  HINT_SEED            hạt giống chọn bước/biểu thức (tất định theo user)
  HINT_PACING_S        mỗi user gửi đều 1 yêu cầu / HINT_PACING_S giây (mặc định 1)
  HINT_START_JITTER    1 (mặc định): mỗi user chờ ngẫu nhiên U(0, nhịp) trước yêu cầu đầu để các user
                       lệch pha nhau; 0: mọi user bắn cùng lúc mỗi nhịp (kịch bản dồn cục xấu nhất —
                       Locust 2.47 sinh cả lô user gần như cùng một thời điểm)

Lịch gửi: yêu cầu thứ k của user có pha φ được hẹn lúc t0 + φ + k·nhịp (không dùng `constant_pacing`
của Locust: đo được là nó kéo các user đã lệch pha về lại cùng một thời điểm).
Vẫn là closed-loop: user chờ phản hồi rồi mới gửi tiếp; khi dịch vụ chậm hơn nhịp thì yêu cầu kế
tiếp gửi ngay và tải tự giảm (coordinated omission) — số yêu cầu vượt nhịp được đếm trong log.
"""

from __future__ import annotations

import itertools
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

import gevent
from locust import FastHttpUser, events, task

ATTACK = "9^9^9 = x"
_FIXTURE = json.loads(open(os.environ["HINT_FIXTURE"], encoding="utf-8").read())
_API_KEY = os.environ["HINT_API_KEY"]
_ATTACK_FRACTION = float(os.environ.get("HINT_ATTACK_FRACTION", "0"))
_SEED = int(os.environ.get("HINT_SEED", "7"))
_PACING_S = float(os.environ.get("HINT_PACING_S", "1"))
_START_JITTER = os.environ.get("HINT_START_JITTER", "1") == "1"
_USER_IDS = itertools.count()
_RECORDS: list[dict] = []


def _now_iso() -> str:
    now = datetime.now(timezone.utc)
    return now.strftime("%Y-%m-%dT%H:%M:%S.") + f"{now.microsecond // 1000:03d}Z"


class HintLearner(FastHttpUser):
    def wait_time(self) -> float:
        self.next_at += _PACING_S
        return max(0.0, self.next_at - time.monotonic())

    def on_start(self) -> None:
        index = next(_USER_IDS)
        self.learner = _FIXTURE["learners"][index % len(_FIXTURE["learners"])]
        self.rng = random.Random(_SEED * 1_000_003 + index)
        self.seq = 0
        phase = self.rng.uniform(0, _PACING_S) if _START_JITTER else 0.0
        self.next_at = time.monotonic() + phase
        gevent.sleep(phase)

    def _envelope(self) -> tuple[dict, str]:
        learner = self.learner
        step = self.rng.randint(1, learner["n_steps"] + 1)
        body = {
            "event_id": str(uuid.uuid4()),
            "learner_id": learner["learner_id"],
            "class_id": learner["class_id"],
            "session_id": learner["session_id"],
            "session_seq": self.seq,
            "problem_id": learner["problem_id"],
            "skill_ids": learner["skill_ids"],
            "step_index": step,
            "event_time": _now_iso(),
            "sent_time": _now_iso(),
            "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"},
        }
        if self.rng.random() < _ATTACK_FRACTION:
            body["expression"] = ATTACK
            return body, "attack"
        wrong = [m for m in learner["misconceptions"] if m["step_index"] == step]
        roll = self.rng.random()
        if wrong and roll < 0.5:
            body["expression"] = wrong[0]["expression"]
            return body, "wrong"
        if step <= learner["n_steps"] and roll < 0.75:
            body["expression"] = learner["steps"][step - 1]
            return body, "correct"
        return body, "none"

    @task
    def request_hint(self) -> None:
        self.seq += 1
        body, kind = self._envelope()
        started = time.perf_counter()
        with self.client.post("/hints", json=body, headers={"X-API-Key": _API_KEY}, name="/hints",
                              catch_response=True) as response:
            latency_ms = (time.perf_counter() - started) * 1000.0
            record = {"t_ms": time.time_ns() // 1_000_000, "latency_ms": round(latency_ms, 3),
                      "status": response.status_code, "kind": kind}
            if response.status_code == 200:
                payload = response.json()
                record.update(state_source=payload.get("state_source"), level=payload.get("hint_level"),
                              source=payload.get("source"), verified=payload.get("verified"))
                response.success()
            else:
                response.failure(f"HTTP {response.status_code}")
            _RECORDS.append(record)


@events.quitting.add_listener
def _write_log(environment, **_kwargs) -> None:
    path = os.environ.get("HINT_LOG")
    if not path:
        return
    with open(path, "w", encoding="utf-8") as handle:
        for record in _RECORDS:
            handle.write(json.dumps(record) + "\n")
