"""Dữ liệu của run thí nghiệm: cache đầu ra bộ mô phỏng và cửa sổ phát lại cho batch baseline.

**Cache mô phỏng.** Mỗi (learners, rate, late_profile, duplicate_rate, thời lượng, kho bài + split, seed) sinh một lần
vào `<data_root>/exp-sim/<khóa>/` (sinh vào thư mục tạm rồi đổi tên ⇒ không bao giờ có cache dở).
Dùng lại khi checksum `events.parquet`, `ground_truth.jsonl`, `roster.json` khớp `manifest.json`;
lệch thì sinh lại. Mọi cấu hình cùng khóa (vd. A với P = 1/3/6) đọc đúng cùng một log.

**Cửa sổ phát lại.** Bộ phát lại chỉ gửi các dòng có `replay_offset_ms < cutoff`. Batch baseline của
Exp C phải chạy trên **chính** log đó, nên `materialize_window` ghi: `events.parquet` = các dòng của
cửa sổ (giữ schema + metadata t0), `ground_truth.jsonl` = mọi dòng trừ `stuck_episode` có sự kiện
onset ngoài cửa sổ — cùng tập episode mà reconcile của run streaming tính recall/time-to-detect.
"""

from __future__ import annotations

import json
import os
import shutil
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pyarrow.compute as pc
import pyarrow.parquet as pq

from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.experiments.matrix import DATASET_MARGIN_S, PROBLEM_DEFAULTS, dataset_key
from ai_math_tutor.problem_bank import file_checksum

CHECKED_OUTPUTS = ("events.parquet", "ground_truth.jsonl", "roster.json")


class DatasetError(RuntimeError):
    """Không sinh/đọc được dữ liệu của run."""


def read_manifest(sim_dir: Path) -> dict[str, Any]:
    try:
        return json.loads((Path(sim_dir) / "manifest.json").read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise DatasetError(f"không đọc được manifest trong {sim_dir}: {exc}") from exc


def cache_valid(sim_dir: Path) -> bool:
    """Mọi tệp đầu ra có mặt và checksum khớp manifest."""
    try:
        manifest = read_manifest(sim_dir)
    except DatasetError:
        return False
    outputs = manifest.get("outputs") or {}
    for name in CHECKED_OUTPUTS:
        path = Path(sim_dir) / name
        expected = (outputs.get(name) or {}).get("checksum")
        if not path.is_file() or not expected or file_checksum(path) != expected:
            return False
    return True


def ensure_dataset(root: Path, params: Mapping[str, Any], seed: int) -> tuple[Path, bool]:
    """Thư mục mô phỏng của (tham số, seed); trả (đường dẫn, đã dùng cache?)."""
    from ai_math_tutor.simulator import SimulationConfig, run_simulation

    target = Path(root) / dataset_key(params, seed)
    if cache_valid(target):
        return target, True
    tmp = target.with_name(f".{target.name}.tmp-{os.getpid()}")
    shutil.rmtree(tmp, ignore_errors=True)
    try:
        run_simulation(SimulationConfig(
            learners=int(params["learners"]),
            horizon_s=float(params["duration_s"] + DATASET_MARGIN_S),
            seed=int(seed),
            out_dir=tmp,
            target_rate=float(params["rate_events_per_s"]),
            late_profile=str(params["late_profile"]),
            duplicate_rate=float(params["duplicate_rate"]),
            problems_path=REPO_ROOT / str(params.get("problem_bank", PROBLEM_DEFAULTS["problem_bank"])),
            split=str(params.get("problem_split", PROBLEM_DEFAULTS["problem_split"])),
        ))
        shutil.rmtree(target, ignore_errors=True)
        tmp.rename(target)
    except Exception as exc:
        shutil.rmtree(tmp, ignore_errors=True)
        raise DatasetError(f"sinh dữ liệu {target.name} thất bại: {exc}") from exc
    return target, False


def materialize_window(sim_dir: Path, out_dir: Path, cutoff_ms: int | None) -> dict[str, Any]:
    """Ghi log + ground truth của cửa sổ phát lại vào `out_dir`; trả số đếm (dòng, episode giữ/bỏ)."""
    sim_dir, out_dir = Path(sim_dir), Path(out_dir)
    table = pq.read_table(sim_dir / "events.parquet")
    if cutoff_ms is not None:
        table = table.filter(pc.less(table.column("replay_offset_ms"), cutoff_ms))
    if table.num_rows == 0:
        raise DatasetError("cửa sổ phát lại không có dòng nào")
    window_ids = set(table.column("event_id").to_pylist())
    out_dir.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out_dir / "events.parquet")  # schema của bảng giữ metadata nhom8.t0
    kept = dropped = 0
    with (sim_dir / "ground_truth.jsonl").open(encoding="utf-8") as src, \
            (out_dir / "ground_truth.jsonl").open("w", encoding="utf-8", newline="\n") as dst:
        for line in src:
            if not line.strip():
                continue
            record = json.loads(line)
            if record.get("record") == "stuck_episode":
                if record.get("onset_event_id") not in window_ids:
                    dropped += 1
                    continue
                kept += 1
            dst.write(line if line.endswith("\n") else line + "\n")
    return {"rows": table.num_rows, "unique_events": len(window_ids), "episodes_kept": kept,
            "episodes_outside_window": dropped, "cutoff_ms": cutoff_ms}
