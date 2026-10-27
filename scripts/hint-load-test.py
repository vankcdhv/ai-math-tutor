"""Đo độ trễ thật của dịch vụ gợi ý bằng Locust headless ở nhiều mức người dùng đồng thời.

Quy trình mỗi lần chạy (mọi tiến trình do script tự mở và tự dừng):
1. dựng fixture trạng thái **xác định** (seed) từ kho bài: mỗi người học một bài, `state:` của kỹ năng
   chính và `progress:` theo đúng định dạng pipeline (`sinks.state_value`, `sinks.progress_pairs`),
   nạp vào Redis dùng chung dưới tiền tố riêng `lt-<tag>:` (không FLUSHDB);
2. mở dịch vụ gợi ý (uvicorn, 127.0.0.1, cổng cố định) với khóa API ngẫu nhiên của lần chạy và cùng
   tiền tố khóa, chờ `/healthz`;
3. với mỗi mức user: xóa `hintctx:`/`lasthint:` của tiền tố (mọi mức bắt đầu như nhau), chạy Locust
   headless, lấy mẫu CPU của Locust (client) và dịch vụ (server) mỗi giây, đọc log từng yêu cầu và
   tính P50/P95/P99 (nearest-rank), tỉ lệ `state_source=default`, mã lỗi;
4. ghi tóm tắt nhỏ vào `results/raw/hint-load-<tag>.json` và một run record mỗi mức vào
   `results/raw/runs.jsonl`; log thô và CSV của Locust nằm dưới `$NHOM8_DATA_ROOT/hint-load/<tag>/`;
5. dừng dịch vụ, xóa mọi khóa của tiền tố.

Trạng thái run record: `aborted` nếu có yêu cầu `state_source=default` (quy tắc thí nghiệm E), `failed`
nếu có phản hồi khác 200, còn lại `success`.

Chạy (trong WSL): uv run python scripts/hint-load-test.py [--users 1,10,50,100] [--duration-s 60]
                  [--attack-fraction 0.1] [--hint-generator ladder|template] [--port 8765]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import secrets
import socket
import subprocess
import sys
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import numpy as np
import psutil
import requests

from ai_math_tutor.config import REPO_ROOT, load_config
from ai_math_tutor.hints.generator import GENERATORS, default_problem_bank_path
from ai_math_tutor.hints.state_client import redis_client
from ai_math_tutor.problem_bank import ProblemBank, file_checksum
from ai_math_tutor.run_record import append_run_record, build_run_record
from ai_math_tutor.streaming.sinks import RedisKeys, progress_pairs, state_value

LOCUSTFILE = REPO_ROOT / "scripts" / "hint-locustfile.py"
SLO_P95_MS = 300.0
HEALTH_TIMEOUT_S = 120.0


def build_fixture(bank: ProblemBank, n_learners: int, seed: int) -> dict[str, Any]:
    """Người học tổng hợp, mỗi người một bài; trạng thái kỹ năng/tiến độ chọn theo seed."""
    rng = random.Random(seed)
    problems = sorted(bank.problems, key=lambda p: p.problem_id)
    chosen = rng.sample(problems, min(n_learners, len(problems)))
    learners = []
    for i in range(n_learners):
        problem = chosen[i % len(chosen)]
        digest = hashlib.sha256(f"hint-load:{seed}:{i}".encode()).hexdigest()
        learners.append({
            "learner_id": f"L-{digest[:16]}",
            "class_id": f"C-LOAD-{i % 4}",
            "session_id": f"S-{digest[16:24]}",
            "problem_id": problem.problem_id,
            "skill_ids": list(problem.skill_ids),
            "n_steps": len(problem.steps),
            # Người học gõ theo ẩn gốc của đề (vd. y); dịch vụ đổi về x trước khi kiểm.
            "steps": list(problem.display_steps),
            "misconceptions": [{"step_index": m.step_index, "expression": problem.display(m.expression)}
                               for m in problem.misconceptions],
            "state": {"consec_wrong": rng.choice([0, 0, 1, 2, 3]), "stuck": rng.random() < 0.1,
                      "wrong_answers": rng.choice([0, 0, 1, 2])},
        })
    return {"seed": seed, "learners": learners}


def load_fixture(client: Any, keys: RedisKeys, fixture: dict[str, Any]) -> int:
    pipe = client.pipeline(transaction=False)
    for learner in fixture["learners"]:
        state = learner["state"]
        row = {"class_id": learner["class_id"], "consec_wrong": state["consec_wrong"], "stuck": state["stuck"],
               "state_version": 1, "opportunities": 5, "correct": 2, "streak": 0, "mastered": False,
               "last_event_time_ms": 1}
        pipe.hset(keys.state(learner["learner_id"]), learner["skill_ids"][0], state_value(row, 0))
        pairs = progress_pairs({"class_id": learner["class_id"], "wrong_answers": state["wrong_answers"],
                                "last_step": 0, "completed": False, "state_version": 1,
                                "last_event_time_ms": 1}, 0)
        pipe.hset(keys.progress(learner["learner_id"], learner["problem_id"]), mapping=dict(zip(pairs[::2], pairs[1::2])))
    pipe.execute()
    return len(fixture["learners"])


def delete_matching(client: Any, pattern: str) -> int:
    keys = list(client.scan_iter(match=pattern, count=1000))
    for start in range(0, len(keys), 500):
        client.delete(*keys[start:start + 500])
    return len(keys)


def port_in_use(port: int) -> bool:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        return sock.connect_ex(("127.0.0.1", port)) == 0


def wait_health(url: str, proc: subprocess.Popen, timeout_s: float) -> dict[str, Any]:
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            raise RuntimeError(f"dịch vụ gợi ý thoát sớm với mã {proc.returncode}")
        try:
            response = requests.get(f"{url}/healthz", timeout=1)
            if response.status_code == 200:
                return response.json()
        except requests.RequestException:
            pass
        time.sleep(0.5)
    raise RuntimeError(f"/healthz không sẵn sàng sau {timeout_s} s")


def stop_process(proc: subprocess.Popen, grace_s: float = 15.0) -> int | None:
    if proc.poll() is None:
        proc.terminate()
        try:
            proc.wait(grace_s)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait(5)
    return proc.returncode


def nearest_rank(values: np.ndarray, q: float) -> float | None:
    return float(np.percentile(values, q * 100.0, method="inverted_cdf")) if values.size else None


def latency_stats(values: np.ndarray) -> dict[str, Any]:
    return {"count": int(values.size), "p50_ms": nearest_rank(values, 0.50), "p95_ms": nearest_rank(values, 0.95),
            "p99_ms": nearest_rank(values, 0.99), "max_ms": float(values.max()) if values.size else None,
            "mean_ms": float(values.mean()) if values.size else None}


def _cpu_samples(proc: psutil.Process) -> float:
    try:
        return proc.cpu_percent(None) + sum(child.cpu_percent(None) for child in proc.children(recursive=True))
    except psutil.Error:
        return 0.0


def _locust_aggregate(csv_prefix: Path) -> dict[str, Any] | None:
    """Dòng Aggregated của `<prefix>_stats.csv` (phân vị Locust làm tròn — chỉ để đối chiếu)."""
    path = Path(f"{csv_prefix}_stats.csv")
    if not path.is_file():
        return None
    import csv

    with path.open(encoding="utf-8") as handle:
        for row in csv.DictReader(handle):
            if row.get("Name") == "Aggregated":
                return {"requests": int(row["Request Count"]), "failures": int(row["Failure Count"]),
                        "p50_ms": float(row["50%"]), "p95_ms": float(row["95%"]), "p99_ms": float(row["99%"]),
                        "rps": float(row["Requests/s"])}
    return None


def run_level(users: int, args: argparse.Namespace, url: str, server: psutil.Process, workdir: Path,
              env: dict[str, str]) -> dict[str, Any]:
    log_path = workdir / f"requests-u{users}.jsonl"
    csv_prefix = workdir / f"locust-u{users}"
    cmd = [sys.executable, "-m", "locust", "-f", str(LOCUSTFILE), "--headless", "-u", str(users),
           "-r", str(users), "-t", f"{args.duration_s}s", "--host", url, "--csv", str(csv_prefix),
           "--only-summary", "--loglevel", "WARNING", "--stop-timeout", "5"]
    level_env = {**env, "HINT_LOG": str(log_path)}
    started = time.monotonic()
    with (workdir / f"locust-u{users}.log").open("w", encoding="utf-8") as out:
        proc = subprocess.Popen(cmd, env=level_env, stdout=out, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
        print(f"  locust u={users} pid={proc.pid}", flush=True)
        client = psutil.Process(proc.pid)
        _cpu_samples(client), _cpu_samples(server)
        client_cpu, server_cpu = [], []
        try:
            while proc.poll() is None:
                time.sleep(1.0)
                client_cpu.append(_cpu_samples(client))
                server_cpu.append(_cpu_samples(server))
        finally:
            code = stop_process(proc, grace_s=args.duration_s + 60)
    runtime_s = time.monotonic() - started
    records = [json.loads(line) for line in log_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    ok = [r for r in records if r["status"] == 200]
    all_lat = np.array([r["latency_ms"] for r in records], dtype=float)
    normal = np.array([r["latency_ms"] for r in ok if r["kind"] != "attack"], dtype=float)
    attack = np.array([r["latency_ms"] for r in ok if r["kind"] == "attack"], dtype=float)
    default = sum(r.get("state_source") == "default" for r in ok)
    t_ms = [r["t_ms"] for r in records]
    span_s = (max(t_ms) - min(t_ms)) / 1000.0 if len(t_ms) > 1 else float(args.duration_s)
    return {
        "users": users,
        "locust_exit_code": code,
        "runtime_s": round(runtime_s, 3),
        "requests": len(records),
        "throughput_rps": round(len(records) / span_s, 3) if span_s > 0 else None,
        "status_counts": dict(Counter(str(r["status"]) for r in records)),
        "latency_all": latency_stats(all_lat),
        "latency_ok_non_attack": latency_stats(normal),
        "latency_attack": latency_stats(attack),
        "slo_p95_ms": SLO_P95_MS,
        "slo_met": bool(normal.size) and nearest_rank(normal, 0.95) <= SLO_P95_MS,
        "state_source_default": default,
        "state_source_default_ratio": (default / len(ok)) if ok else None,
        "hint_level_counts": dict(Counter(str(r.get("level")) for r in ok)),
        "source_counts": dict(Counter(str(r.get("source")) for r in ok)),
        "verified_false": sum(r.get("verified") is False for r in ok),
        "request_kind_counts": dict(Counter(r["kind"] for r in records)),
        "pacing_overruns": int((all_lat > args.pacing_s * 1000.0).sum()),
        "client_cpu_percent": {"mean": float(np.mean(client_cpu)) if client_cpu else None,
                               "max": float(np.max(client_cpu)) if client_cpu else None},
        "server_cpu_percent": {"mean": float(np.mean(server_cpu)) if server_cpu else None,
                               "max": float(np.max(server_cpu)) if server_cpu else None},
        "locust_aggregate": _locust_aggregate(csv_prefix),
        "request_log": str(log_path),
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--users", default="1,10,50,100")
    parser.add_argument("--duration-s", type=int, default=60)
    parser.add_argument("--pacing-s", type=float, default=1.0)
    parser.add_argument("--attack-fraction", type=float, default=0.0)
    parser.add_argument("--no-start-jitter", action="store_true",
                        help="mọi user bắn cùng lúc mỗi nhịp (kịch bản dồn cục); mặc định lệch pha ngẫu nhiên")
    parser.add_argument("--hint-generator", choices=GENERATORS, default="ladder")
    parser.add_argument("--problems", type=Path, default=None)
    parser.add_argument("--port", type=int, default=8765)
    parser.add_argument("--seed", type=int, default=7)
    parser.add_argument("--math-workers", type=int, default=4)
    parser.add_argument("--math-timeout-s", type=float, default=0.2)
    parser.add_argument("--no-audit-log", action="store_true")
    parser.add_argument("--no-run-record", action="store_true", help="chạy thử: không ghi runs.jsonl")
    parser.add_argument("--tag", default=None, help="nhãn lần chạy (mặc định theo thời gian UTC)")
    args = parser.parse_args(argv)
    user_levels = [int(x) for x in args.users.split(",") if x.strip()]
    if not user_levels or min(user_levels) < 1 or not 0 <= args.attack_fraction <= 1:
        parser.error("--users phải là danh sách số nguyên ≥ 1; --attack-fraction trong [0, 1]")

    started_at = datetime.now(timezone.utc)
    tag = args.tag or started_at.strftime("%Y%m%dT%H%M%SZ")
    cfg = load_config()
    workdir = cfg.paths.data_root / "hint-load" / tag
    workdir.mkdir(parents=True, exist_ok=True)
    bank_path = args.problems or default_problem_bank_path()
    bank = ProblemBank.from_jsonl(bank_path)
    fixture = build_fixture(bank, max(user_levels), args.seed)
    fixture_path = workdir / "fixture.json"
    fixture_path.write_text(json.dumps(fixture, ensure_ascii=False), encoding="utf-8")
    prefix = f"lt-{tag}:"
    keys = RedisKeys(prefix)
    redis = redis_client(cfg.redis.url, socket_timeout_s=5.0)
    if port_in_use(args.port):
        print(f"Cổng {args.port} đang bận: dừng tiến trình cũ của chính mình trước (không đổi cổng).", file=sys.stderr)
        return 2
    api_key = secrets.token_urlsafe(24)
    env = {**os.environ, "NHOM8_HINT_API_KEY": api_key, "HINT_API_KEY": api_key, "HINT_FIXTURE": str(fixture_path),
           "HINT_ATTACK_FRACTION": str(args.attack_fraction), "HINT_SEED": str(args.seed),
           "HINT_PACING_S": str(args.pacing_s), "HINT_START_JITTER": "0" if args.no_start_jitter else "1",
           "PYTHONUNBUFFERED": "1"}
    url = f"http://127.0.0.1:{args.port}"
    server_cmd = [sys.executable, "-m", "ai_math_tutor.hints.app", "--port", str(args.port), "--key-prefix", prefix,
                  "--hint-generator", args.hint_generator, "--problems", str(bank_path),
                  "--math-workers", str(args.math_workers), "--math-timeout-s", str(args.math_timeout_s)]
    if args.no_audit_log:
        server_cmd.append("--no-audit-log")
    summaries: list[dict[str, Any]] = []
    server_log = (workdir / "server.log").open("w", encoding="utf-8")
    server = subprocess.Popen(server_cmd, env=env, stdout=server_log, stderr=subprocess.STDOUT, cwd=REPO_ROOT)
    print(f"dịch vụ gợi ý pid={server.pid} cổng={args.port} tiền tố={prefix}", flush=True)
    health: dict[str, Any] = {}
    try:
        load_fixture(redis, keys, fixture)
        health = wait_health(url, server, HEALTH_TIMEOUT_S)
        server_ps = psutil.Process(server.pid)
        for users in user_levels:
            delete_matching(redis, f"{prefix}hintctx:*")
            delete_matching(redis, f"{prefix}lasthint:*")
            summary = run_level(users, args, url, server_ps, workdir, env)
            summaries.append(summary)
            print(json.dumps({k: summary[k] for k in ("users", "requests", "throughput_rps", "status_counts",
                                                      "state_source_default", "slo_met")}, ensure_ascii=False)
                  + f" p95_ms={summary['latency_ok_non_attack']['p95_ms']}", flush=True)
        health = requests.get(f"{url}/healthz", timeout=5).json()
    finally:
        server_code = stop_process(server)
        server_log.close()
        removed = delete_matching(redis, f"{prefix}*")
        print(f"đã dừng dịch vụ (mã {server_code}), xóa {removed} khóa {prefix}*", flush=True)

    data_version = f"{bank_path.name}@{file_checksum(bank_path)}"
    results_dir = cfg.paths.results
    common = {"duration_s": args.duration_s, "pacing_s": args.pacing_s, "attack_fraction": args.attack_fraction,
              "hint_generator": args.hint_generator, "math_workers": args.math_workers,
              "math_timeout_s": args.math_timeout_s, "port": args.port, "key_prefix": prefix,
              "fixture_learners": len(fixture["learners"]), "audit_log": not args.no_audit_log,
              "loop": "closed (Locust, fixed per-user schedule)", "uvicorn_workers": 1,
              "start_jitter": not args.no_start_jitter, "locust_user": "FastHttpUser"}
    for summary in ([] if args.no_run_record else summaries):
        if summary["state_source_default"]:
            status = "aborted"
        elif set(summary["status_counts"]) != {"200"}:
            status = "failed"
        else:
            status = "success"
        record = build_run_record(
            experiment="hint-api", data_version=data_version,
            parameters={**common, "users": summary["users"]}, seed=args.seed, timeout_s=float(args.duration_s),
            metrics={k: v for k, v in summary.items() if k != "request_log"}, status=status,
            log_path=summary["request_log"], started_at=started_at, repo_dir=REPO_ROOT,
            run_id=f"hint-api-u{summary['users']}-{tag}", runtime_s=summary["runtime_s"],
        )
        append_run_record(record, results_dir / "runs.jsonl")
    # Chạy thử (--no-run-record) không ghi vào results/raw (chỉ ghi nối kết quả thật).
    out = (workdir if args.no_run_record else results_dir) / f"hint-load-{tag}.json"
    out.write_text(json.dumps({"tag": tag, "parameters": common, "data_version": data_version,
                               "server_health_after": health, "levels": summaries}, ensure_ascii=False, indent=2)
                   + "\n", encoding="utf-8")
    print(f"tóm tắt: {out}", flush=True)
    return 0 if summaries and all(s["state_source_default"] == 0 for s in summaries) else 1


if __name__ == "__main__":
    sys.exit(main())
