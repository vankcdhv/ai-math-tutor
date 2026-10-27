"""Dịch vụ gợi ý HTTP (FastAPI): `POST /hints` trả gợi ý 3 bậc đã kiểm chứng, `GET /healthz`.

Một yêu cầu đi qua (dừng ở bước đầu tiên thất bại):

1. khóa API (`X-API-Key`) → 401; thân > 4 KB → 413; JSON hỏng / sai phong bì hợp đồng → 422;
2. bài không có trong kho, `skill_ids` khác kho, `step_index` vượt số bước + 1 → 422;
3. giới hạn tốc độ theo `learner_id` → 429 (`Retry-After`);
4. `expression` (nếu có) được `diagnose` trong `safe_math` (worker giết được, có hạn, cache theo
   (bài, biểu thức, bước)); hàng đợi worker đầy → 429; quá hạn/lỗi worker → bỏ chẩn đoán, đi tiếp;
5. một transaction Redis: ghi `hintctx:`, đọc `state:`/`progress:` (lỗi → `state_source=default`);
6. `ladder.select_level` (fail-closed) → `HintGenerator` (template | ladder; bản nháp không qua
   kiểm → mẫu đã kiểm) → phản hồi `{hint_level, text_vi, math, verified, misconception, source,
   state_source}`;
7. sau khi trả lời (background): `SET lasthint:{learner}` và gửi nhật ký vào topic `hint-requests`.

Không ghi body hay biểu thức của người học vào log. Mặc định bind 127.0.0.1.

Chạy: python -m ai_math_tutor.hints.app [--port 8000] [--hint-generator ladder|template] [--problems PATH]
(cần `NHOM8_HINT_API_KEY` ≥ 16 ký tự và `REDIS_PASSWORD` trong môi trường hoặc `.env`).
"""

from __future__ import annotations

import argparse
import contextlib
import functools
import json
import logging
import os
import sys
import threading
import time
from collections.abc import AsyncIterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from starlette.background import BackgroundTask
from starlette.concurrency import run_in_threadpool

from ai_math_tutor import math_check
from ai_math_tutor.config import DEFAULT_DOTENV_PATH, load_config, read_dotenv
from ai_math_tutor.hints.audit_log import AuditSink, KafkaAuditSink, NullAuditSink
from ai_math_tutor.hints.auth import API_KEY_HEADER, ApiKeyAuth, RateLimiter, api_key_from_env
from ai_math_tutor.hints.envelope import MAX_BODY_BYTES, envelope_errors
from ai_math_tutor.hints.generator import GENERATORS, HintEngine, default_problem_bank_path, to_machine_expression
from ai_math_tutor.hints.ladder import LadderInput, select_level
from ai_math_tutor.hints.state_client import HintKeys, HintStateStore, LearnerSnapshot, redis_client
from ai_math_tutor.hints.templates import MISCONCEPTIONS
from ai_math_tutor.problem_bank import Problem, ProblemBank, ProblemBankError
from ai_math_tutor.safe_math import SafeMath, SafeMathError, SafeMathOverloaded, SafeMathTimeout
from ai_math_tutor.streaming.sinks import RedisKeys

LOGGER = logging.getLogger(__name__)

DEFAULT_HOST = "127.0.0.1"
DEFAULT_PORT = 8000
DEFAULT_MATH_TIMEOUT_S = 0.2
DEFAULT_MATH_WORKERS = 4
DEFAULT_MATH_MAX_PENDING = 32
DIAGNOSE_CACHE_SIZE = 8192
MAX_ERROR_DETAILS = 20


@dataclass(frozen=True)
class Outcome:
    """Kết quả xử lý một yêu cầu: mã HTTP, thân phản hồi, header, việc ghi sau khi trả lời."""

    status: int
    body: dict[str, Any]
    headers: dict[str, str] | None = None
    after: dict[str, Any] | None = None


def _error(status: int, code: str, detail: Any = None, headers: dict[str, str] | None = None) -> Outcome:
    body: dict[str, Any] = {"error": code}
    if detail is not None:
        body["detail"] = detail
    return Outcome(status=status, body=body, headers=headers)


class HintService:
    """Lõi của dịch vụ, không phụ thuộc HTTP: nhận phong bì đã giải mã, trả `Outcome`."""

    def __init__(self, engine: HintEngine, checker: SafeMath, store: HintStateStore | None, auth: ApiKeyAuth,
                 limiter: RateLimiter, audit: AuditSink, generator: str = "ladder",
                 diagnose_cache_size: int = DIAGNOSE_CACHE_SIZE) -> None:
        self.engine = engine
        self.generator = engine.generator(generator)
        self.checker = checker
        self.store = store
        self.auth = auth
        self.limiter = limiter
        self.audit = audit
        self._rows = {p.problem_id: p.to_dict() for p in engine.bank.problems}
        self._diagnose = functools.lru_cache(maxsize=diagnose_cache_size)(self._diagnose_uncached)
        self.counters = {"diagnose_timeouts": 0, "diagnose_errors": 0, "lasthint_errors": 0}
        self._counter_lock = threading.Lock()

    def _count(self, name: str) -> None:
        with self._counter_lock:
            self.counters[name] += 1

    # --- chẩn đoán biểu thức (SymPy chỉ chạy trong safe_math) ---------------------------------

    def _diagnose_uncached(self, problem_id: str, expression: str, step_arg: int | None) -> tuple[str, str | None]:
        """("correct" | "wrong" | "invalid", loại lỗi). Quá hạn/quá tải ném lỗi (không được cache)."""
        try:
            result = self.checker.call("diagnose", self._rows[problem_id], expression, step_arg)
        except math_check.MathInputError:
            return "invalid", None
        if result.get("equivalent"):
            return "correct", None
        misconception = result.get("misconception")
        return "wrong", misconception if misconception in MISCONCEPTIONS else None

    def diagnose(self, problem: Problem, expression: str | None, step_index: int) -> tuple[str, str | None]:
        """Chẩn đoán biểu thức người học (viết theo ẩn gốc của đề, được đổi về ẩn x trước khi kiểm)."""
        if expression is None:
            return "none", None
        step_arg = step_index if step_index <= len(problem.steps) else None
        machine = to_machine_expression(expression, problem.variable)
        try:
            return self._diagnose(problem.problem_id, machine, step_arg)
        except SafeMathOverloaded:
            raise
        except SafeMathTimeout:
            self._count("diagnose_timeouts")
            return "unchecked", None
        except SafeMathError:
            self._count("diagnose_errors")
            return "unchecked", None

    def warm_up(self, workers: int) -> None:
        """Khởi động sẵn các worker SymPy (gọi tuần tự `workers` lần, mỗi lần một worker mới)."""
        if not self.engine.bank.problems:
            return
        row = self.engine.bank.problems[0].to_dict()
        for _ in range(workers):
            try:
                self.checker.call("diagnose", row, row.get("answer", "0"), None)
            except (SafeMathError, math_check.MathInputError) as exc:
                LOGGER.warning("warm-up safe_math: %s", type(exc).__name__)

    # --- xử lý yêu cầu ------------------------------------------------------------------------

    def handle(self, body: Any, started: float | None = None) -> Outcome:
        started = time.perf_counter() if started is None else started
        errors = envelope_errors(body)
        if errors:
            return _error(422, "invalid_envelope", errors[:MAX_ERROR_DETAILS])
        try:
            problem = self.engine.bank.get(body["problem_id"])
        except ProblemBankError:
            return _error(422, "unknown_problem", body["problem_id"])
        if set(body["skill_ids"]) != set(problem.skill_ids):
            return _error(422, "skill_mismatch", list(problem.skill_ids))
        n_steps = len(problem.steps)
        step_index = body["step_index"]
        if step_index > n_steps + 1:
            return _error(422, "step_out_of_range", {"max_step_index": n_steps + 1})
        learner = body["learner_id"]
        allowed, retry_after = self.limiter.acquire(learner)
        if not allowed:
            return _error(429, "rate_limited", headers={"Retry-After": str(retry_after)})
        try:
            status, misconception = self.diagnose(problem, body.get("expression"), step_index)
        except SafeMathOverloaded:
            return _error(429, "math_overloaded", headers={"Retry-After": "1"})
        wrong = status == "wrong"
        if self.store is None:
            snapshot = LearnerSnapshot.default(error="redis_disabled", wrong=wrong)
        else:
            snapshot = self.store.record_and_read(learner_id=learner, problem_id=problem.problem_id,
                                                  step_index=step_index, skill_id=problem.primary_skill,
                                                  event_id=body["event_id"], wrong=wrong)
        decision = select_level(LadderInput(
            state_source=snapshot.state_source, hint_count=snapshot.hint_count,
            step_wrong_count=snapshot.step_wrong_count, consec_wrong=snapshot.consec_wrong,
            stuck=snapshot.stuck, wrong_answers=snapshot.wrong_answers,
        ))
        ctx = self.engine.context(problem.problem_id, step_index, status, misconception)
        hint = self.generator.generate(ctx, decision)
        response = hint.response(snapshot.state_source)
        served_ms = time.time_ns() // 1_000_000
        after = {
            "learner_id": learner,
            "lasthint": {"event_id": body["event_id"], "problem_id": problem.problem_id, "step_index": step_index,
                         "hint_level": hint.hint_level, "source": hint.source, "verified": hint.verified,
                         "misconception": hint.misconception, "state_source": snapshot.state_source,
                         "text_vi": hint.text_vi, "served_ms": served_ms},
            "audit": {"event_id": body["event_id"], "learner_id": learner, "class_id": body["class_id"],
                      "problem_id": problem.problem_id, "step_index": step_index, "hint_level": hint.hint_level,
                      "source": hint.source, "verified": hint.verified, "misconception": hint.misconception,
                      "state_source": snapshot.state_source, "expression_status": status,
                      "generator": self.generator.name, "level_reason": decision.reason,
                      "latency_ms": round((time.perf_counter() - started) * 1000, 3), "served_ms": served_ms},
        }
        return Outcome(status=200, body=response, after=after)

    def after_response(self, after: Mapping[str, Any]) -> None:
        """Ghi phụ sau khi đã trả lời: lasthint (dashboard) và nhật ký kiểm toán. Không bao giờ ném."""
        if self.store is not None and not self.store.write_lasthint(after["learner_id"], after["lasthint"]):
            self._count("lasthint_errors")
        self.audit.send(after["audit"])

    def health(self) -> dict[str, Any]:
        return {
            "status": "ok",
            "generator": self.generator.name,
            "problems": len(self.engine.bank),
            "unverified_problems": len(self.engine.unverified_problems),
            "redis": "disabled" if self.store is None else ("ok" if self.store.ping() else "unavailable"),
            "math_timeout_s": self.checker.timeout_s,
            "counters": self._counters_snapshot(),
            "audit": self.audit.stats(),
        }

    def _counters_snapshot(self) -> dict[str, int]:
        with self._counter_lock:
            return dict(self.counters)

    def close(self) -> None:
        self.checker.close()
        self.audit.close()


async def _read_limited(request: Request, limit: int) -> bytes | None:
    """Thân yêu cầu, hoặc None nếu vượt `limit` byte (đọc dừng ngay khi vượt)."""
    declared = request.headers.get("content-length")
    if declared is not None:
        try:
            if int(declared) > limit:
                return None
        except ValueError:
            return None
    chunks: list[bytes] = []
    size = 0
    async for chunk in request.stream():
        size += len(chunk)
        if size > limit:
            return None
        chunks.append(chunk)
    return b"".join(chunks)


def create_app(service: HintService, *, close_on_shutdown: bool = False) -> FastAPI:
    """Ứng dụng FastAPI quanh `service`. `close_on_shutdown`: đóng worker SymPy và flush nhật ký Kafka
    khi uvicorn tắt êm (uvicorn ném lại SIGTERM sau khi tắt nên `finally` ở `main` không đủ)."""

    @contextlib.asynccontextmanager
    async def lifespan(_app: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            if close_on_shutdown:
                service.close()

    app = FastAPI(title="nhom8 hint service", docs_url=None, redoc_url=None, openapi_url=None, lifespan=lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, Any]:
        return service.health()

    @app.post("/hints")
    async def hints(request: Request) -> JSONResponse:
        started = time.perf_counter()
        if not service.auth.check(request.headers.get(API_KEY_HEADER)):
            return JSONResponse({"error": "unauthorized"}, status_code=401)
        raw = await _read_limited(request, MAX_BODY_BYTES)
        if raw is None:
            return JSONResponse({"error": "body_too_large", "detail": {"max_bytes": MAX_BODY_BYTES}}, status_code=413)
        try:
            body = json.loads(raw)
        except (ValueError, UnicodeDecodeError):
            return JSONResponse({"error": "invalid_json"}, status_code=422)
        outcome = await run_in_threadpool(service.handle, body, started)
        task = BackgroundTask(service.after_response, outcome.after) if outcome.after else None
        return JSONResponse(outcome.body, status_code=outcome.status, headers=outcome.headers, background=task)

    return app


def build_service(args: argparse.Namespace, env: Mapping[str, str]) -> HintService:
    api_key = api_key_from_env(env)
    cfg = load_config(env=env, dotenv_path=None)
    bank = ProblemBank.from_jsonl(args.problems)
    started = time.perf_counter()
    engine = HintEngine.from_bank(bank)
    LOGGER.warning("Đã kiểm %d bài trong %.1f s (%d bài chưa qua kiểm, chỉ dùng mẫu)", len(bank),
                   time.perf_counter() - started, len(engine.unverified_problems))
    if engine.library.failed:
        LOGGER.warning("Mẫu gợi ý không qua kiểm (thay bằng gợi ý chung): %s", engine.library.failed)
    store = None
    if not args.no_redis:
        store = HintStateStore(redis_client(cfg.redis.url), HintKeys(RedisKeys(prefix=args.key_prefix)))
    audit: AuditSink = NullAuditSink() if args.no_audit_log else KafkaAuditSink(
        cfg.kafka.bootstrap_servers, cfg.kafka.topics["hint_requests"])
    checker = SafeMath(timeout_s=args.math_timeout_s, max_pending=args.math_max_pending, workers=args.math_workers)
    service = HintService(engine, checker, store, ApiKeyAuth(api_key),
                          RateLimiter(rate_per_min=args.rate_per_min, burst=args.burst), audit,
                          generator=args.hint_generator)
    service.warm_up(args.math_workers)
    return service


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--host", default=DEFAULT_HOST, help="mặc định 127.0.0.1; chỉ đổi khi chạy trong container")
    parser.add_argument("--port", type=int, default=DEFAULT_PORT)
    parser.add_argument("--problems", type=Path, default=None, help="kho bài JSONL (mặc định v2 nếu có, nếu không v1)")
    parser.add_argument("--hint-generator", choices=GENERATORS, default="ladder")
    parser.add_argument("--key-prefix", default="", help="tiền tố khóa Redis (test/thí nghiệm dùng chung Redis)")
    parser.add_argument("--rate-per-min", type=float, default=60.0)
    parser.add_argument("--burst", type=int, default=10)
    parser.add_argument("--math-timeout-s", type=float, default=DEFAULT_MATH_TIMEOUT_S)
    parser.add_argument("--math-workers", type=int, default=DEFAULT_MATH_WORKERS)
    parser.add_argument("--math-max-pending", type=int, default=DEFAULT_MATH_MAX_PENDING)
    parser.add_argument("--no-redis", action="store_true", help="chạy không Redis (mọi gợi ý fail-closed bậc 1)")
    parser.add_argument("--no-audit-log", action="store_true", help="không gửi nhật ký vào hint-requests")
    args = parser.parse_args(argv)
    if args.problems is None:
        args.problems = default_problem_bank_path()
    return args


def main(argv: list[str] | None = None) -> int:
    import uvicorn

    logging.basicConfig(level=logging.WARNING, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
    args = parse_args(argv)
    env = {**read_dotenv(DEFAULT_DOTENV_PATH), **os.environ}
    service = build_service(args, env)
    try:
        uvicorn.run(create_app(service, close_on_shutdown=True), host=args.host, port=args.port,
                    log_level="warning", access_log=False)
    finally:
        service.close()  # đóng hai lần là vô hại; phủ trường hợp uvicorn không khởi động được
    return 0


if __name__ == "__main__":
    sys.exit(main())
