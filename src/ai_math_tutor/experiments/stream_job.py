"""Tiến trình con: job streaming thật (`ai_math_tutor.streaming.job.main`) trên Redis DB và thư mục làm việc của thí nghiệm.

Runner cần job ghi vào **DB riêng cho thí nghiệm** (FLUSHDB đầu run chỉ an toàn ở DB đó) và ghi tiến
độ từng micro-batch lên ext4 (`<data_root>/exp-work/`, nén vào results/raw khi run xong). Hai mục này
đi qua biến môi trường mà `load_config()` đọc (`NHOM8_REDIS_DB`, `NHOM8_RESULTS_DIR`), nên job chạy
đúng CLI của nó — mọi tham số, xử lý tín hiệu (SIGTERM ⇒ dừng ở ranh giới batch), sinks và listener
giữ nguyên.

  python -m ai_math_tutor.experiments.stream_job --redis-db 9 --work-dir DIR -- <tham số của streaming.job>
"""

from __future__ import annotations

import argparse
import os
import sys
from collections.abc import MutableMapping
from pathlib import Path


def experiment_env(env: MutableMapping[str, str], *, redis_db: int, work_dir: Path) -> None:
    """Đặt DB Redis riêng và thư mục kết quả của job (job.json, progress.jsonl) dưới work_dir."""
    if redis_db < 0:
        raise ValueError("redis_db phải ≥ 0")
    env["NHOM8_REDIS_DB"] = str(redis_db)
    env["NHOM8_RESULTS_DIR"] = str(Path(work_dir))


def split_argv(argv: list[str]) -> tuple[list[str], list[str]]:
    if "--" not in argv:
        raise SystemExit("cú pháp: stream_job --redis-db N --work-dir DIR -- <tham số streaming.job>")
    index = argv.index("--")
    return argv[:index], argv[index + 1:]


def main(argv: list[str] | None = None) -> int:
    own, job_argv = split_argv(list(sys.argv[1:] if argv is None else argv))
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--redis-db", type=int, required=True)
    parser.add_argument("--work-dir", type=Path, required=True)
    args = parser.parse_args(own)
    if args.redis_db < 0:
        parser.error("--redis-db phải ≥ 0")

    experiment_env(os.environ, redis_db=args.redis_db, work_dir=args.work_dir)
    from ai_math_tutor.streaming import job

    return job.main(job_argv)


if __name__ == "__main__":
    sys.exit(main())
