"""CLI bộ mô phỏng người học.

Ví dụ (đầu ra lớn đặt dưới $NHOM8_DATA_ROOT, không ghi vào /mnt/d):
  uv run python -m ai_math_tutor.simulator.cli --learners 1000 --days 1 --seed 11 \\
      --out "$NHOM8_DATA_ROOT/sim/day-1k-s11"
  uv run python -m ai_math_tutor.simulator.cli --learners 10000 --minutes 30 --target-rate 1000 \\
      --seed 11 --late-profile high --duplicate-rate 0.02 --out "$NHOM8_DATA_ROOT/sim/b-10k-s11"

`--target-rate` bỏ trống ⇒ think-time tự nhiên và phiên theo khung giờ học (dùng cho batch 1 ngày);
có giá trị ⇒ think-time trung bình = learners / rate giây, phiên nối tiếp liên tục.
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from ai_math_tutor.problem_bank import ProblemBankError
from ai_math_tutor.simulator.engine import (
    DEFAULT_PROBLEMS,
    DEFAULT_SKILLS,
    DEFAULT_START,
    SimulationConfig,
    run_simulation,
)
from ai_math_tutor.simulator.network import LATE_PROFILES
from ai_math_tutor.simulator.population import SimulationConfigError


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="ai_math_tutor.simulator.cli", description=__doc__.splitlines()[0])
    parser.add_argument("--learners", type=int, required=True, help="số người học (khóa trạng thái)")
    span = parser.add_mutually_exclusive_group(required=True)
    span.add_argument("--days", type=float, help="thời lượng mô phỏng (ngày)")
    span.add_argument("--minutes", type=float, help="thời lượng mô phỏng (phút)")
    parser.add_argument("--target-rate", type=float, default=None, help="sự kiện/giây mục tiêu (bỏ trống = tự nhiên)")
    parser.add_argument("--seed", type=int, required=True)
    parser.add_argument("--late-profile", choices=sorted(LATE_PROFILES), default="low")
    parser.add_argument("--duplicate-rate", type=float, default=0.02, help="tỉ lệ sự kiện bị gửi trùng")
    parser.add_argument("--out", type=Path, required=True, help="thư mục đầu ra")
    parser.add_argument("--problems", type=Path, default=DEFAULT_PROBLEMS, help="kho bài JSONL")
    parser.add_argument("--split", choices=["dev", "test", "all"], default="dev", help="split kho bài dùng để sinh")
    parser.add_argument("--skills", type=Path, default=DEFAULT_SKILLS, help="cấu hình kỹ năng + khoảng tham số BKT")
    parser.add_argument("--class-size", type=int, default=35)
    parser.add_argument("--cellular-share", type=float, default=0.4, help="tỉ lệ người học dùng mạng di động")
    parser.add_argument("--start", default=DEFAULT_START, help="t0 danh nghĩa (UTC ISO, kết thúc bằng Z)")
    parser.add_argument("--overwrite", action="store_true", help="ghi đè đầu ra cũ trong --out")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    horizon_s = args.days * 86_400 if args.days is not None else args.minutes * 60
    try:
        config = SimulationConfig(
            learners=args.learners,
            horizon_s=horizon_s,
            seed=args.seed,
            out_dir=args.out,
            target_rate=args.target_rate,
            late_profile=args.late_profile,
            duplicate_rate=args.duplicate_rate,
            problems_path=args.problems,
            split=None if args.split == "all" else args.split,
            skills_path=args.skills,
            class_size=args.class_size,
            cellular_share=args.cellular_share,
            start=args.start,
        )
        manifest = run_simulation(config, overwrite=args.overwrite)
    except (SimulationConfigError, ProblemBankError, ValueError, OSError) as exc:
        print(f"lỗi: {exc}", file=sys.stderr)
        return 2
    summary = {
        "out": str(args.out),
        "counts": {k: manifest["counts"][k] for k in ("learners", "sessions", "unique_events", "deliveries",
                                                      "duplicate_deliveries", "late_events")},
        "rates": manifest["rates"],
        "stuck_episodes": manifest["ground_truth"]["stuck_episodes"],
        "generation_seconds": manifest["generation_seconds"],
    }
    print(json.dumps(summary, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
