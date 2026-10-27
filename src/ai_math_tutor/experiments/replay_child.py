"""Tiến trình con: phát lại cửa sổ log của một run (`ingestion.replay.run_replay`) rồi heartbeat tới khi job drain.

Chạy riêng một session để runner dừng được cả nhóm (gồm các shard spawn) và reaper dọn được khi
runner bị giết. Hook `heartbeat_until` đọc dòng cuối `progress.jsonl` của job: dừng heartbeat khi
watermark của job ≥ cuối log (= `replay_start` + offset event lớn nhất của cửa sổ). Không có
`--progress` (đo trần ingestion) thì không gửi heartbeat. Kết quả: `<out-dir>/replay-<run_id>.json`
(không ghi run record — runner ghi một bản ghi cho cả run); `--start-file` nhận `replay_start_ms`
ngay khi lịch bắt đầu. Mã thoát 0 khi bộ phát lại `success`, 1 khi không, 2 khi lỗi tham số/đầu vào.
"""

from __future__ import annotations

import argparse
import json
import signal
import sys
import threading
from pathlib import Path
from typing import Any

from ai_math_tutor.config import load_config, validate_run_id
from ai_math_tutor.experiments.monitor import tail_record


def watermark_passed(progress_path: Path, end_of_log_ms: int | None) -> bool:
    record = tail_record(progress_path)
    if record is None or end_of_log_ms is None:
        return False
    watermark = record.get("watermark_ms")
    return watermark is not None and int(watermark) >= end_of_log_ms


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--events", type=Path, required=True)
    parser.add_argument("--topic", required=True)
    parser.add_argument("--processes", type=int, default=1)
    parser.add_argument("--duration-s", type=float, required=True, help="cửa sổ: replay_offset < S giây")
    parser.add_argument("--watermark-s", type=float, default=120.0)
    parser.add_argument("--heartbeat-s", type=float, default=0.0, help="giới hạn trên của heartbeat (0 = không gửi)")
    parser.add_argument("--progress", type=Path, default=None, help="progress.jsonl của job (hook dừng heartbeat)")
    parser.add_argument("--out-dir", type=Path, required=True)
    parser.add_argument("--start-file", type=Path, default=None)
    parser.add_argument("--rate-check", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    from ai_math_tutor.ingestion.producer import ProducerSettings
    from ai_math_tutor.ingestion.replay import ReplayError, ReplaySettings, run_replay, summarize_window, write_results

    try:
        run_id = validate_run_id(args.run_id)
        cfg = load_config()
        heartbeat_s = args.heartbeat_s if args.heartbeat_s > 0 and args.progress is not None else None
        settings = ReplaySettings(args.events, args.topic, ProducerSettings(cfg.kafka.bootstrap_servers),
                                  processes=args.processes, duration_s=args.duration_s,
                                  watermark_s=args.watermark_s, heartbeat_s=heartbeat_s)
        max_event_offset_ms = summarize_window(args.events, settings.cutoff_ms, args.processes).max_event_offset_ms
        manifest_path = args.events.parent / "manifest.json"
        manifest = json.loads(manifest_path.read_text(encoding="utf-8")) if manifest_path.is_file() else None
    except (ValueError, OSError, json.JSONDecodeError, ReplayError) as exc:
        print(f"replay_child: {exc}", file=sys.stderr)
        return 2

    stop = threading.Event()

    def on_signal(signum: int, _frame: Any) -> None:
        print(f"replay_child: nhận tín hiệu {signum}, dừng phát lại", file=sys.stderr, flush=True)
        stop.set()

    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, on_signal)

    state: dict[str, int | None] = {"end_of_log_ms": None}

    def on_started(replay_start_ms: int) -> None:
        state["end_of_log_ms"] = replay_start_ms + max_event_offset_ms
        if args.start_file is not None:
            args.start_file.write_text(str(replay_start_ms), encoding="utf-8")

    until = None
    if args.progress is not None:
        progress = args.progress

        def until() -> bool:  # type: ignore[misc]
            return watermark_passed(progress, state["end_of_log_ms"])

    try:
        result = run_replay(settings, heartbeat_until=until, on_started=on_started, stop_requested=stop.is_set)
    except ReplayError as exc:
        print(f"replay_child: {exc}", file=sys.stderr)
        return 2
    path = write_results(result, results_dir=args.out_dir, run_id=run_id, manifest=manifest,
                         rate_check=args.rate_check, run_record=False)
    print(json.dumps({"path": str(path), "status": result["status"], "rate_ratio": result["rate_ratio"],
                      "end_of_log_ms": result["end_of_log_ms"], "heartbeat": result["heartbeat"]}, ensure_ascii=False),
          flush=True)
    return 0 if result["status"] == "success" else 1


if __name__ == "__main__":
    sys.exit(main())
