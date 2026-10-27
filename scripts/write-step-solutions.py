"""Viết lời giải từng bước (đã kiểm) + misconception cho mọi ứng viên NuminaMath.

Đọc `$NHOM8_DATA_ROOT/numina/candidates.parquet` (do extract-numina-subset.py tạo), gọi
`ai_math_tutor.solution_writer` cho từng dòng và ghi:
- `$NHOM8_DATA_ROOT/numina/solutions.jsonl`: mỗi ứng viên một dòng, `status` = ok | dropped
  (kèm `reason` thuộc solution_writer.DROP_REASONS), sắp theo numina_row.
- `$NHOM8_DATA_ROOT/numina/solutions-summary.json`: số đạt/loại theo (nguồn, loại, lý do),
  sha256 đầu vào/đầu ra, thời gian chạy.

Bài số học chỉ nhận từ nguồn bài lời văn (gsm8k, orca_math); bài số học của math/synthetic_math
bị loại `arithmetic_source_excluded`. Bài phương trình mang theo `variable` (chữ cái ẩn gốc của đề)
để hiển thị; bước luôn viết theo ẩn x.

Bài số học mà cách tách từ CoT thất bại (trừ lý do `rounding_dependent` và
`arithmetic_source_excluded`) và có mục trong `data/problems/numina-authored.yaml` dùng bước viết
tay đó (`solution_source = authored`, ghi kèm `cot_drop_reason`). Mục viết trực tiếp cho bài đã tách
được từ CoT, hoặc cho bài bị loại vì nguồn/làm tròn, là lỗi (không ghi đè).

Tất định: cùng đầu vào cho cùng byte đầu ra (không có dấu thời gian trong solutions.jsonl).
Chạy (trong WSL): bash scripts/in-wsl.sh uv run python scripts/write-step-solutions.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
from collections import Counter
from pathlib import Path

import pyarrow.parquet as pq
import yaml

from ai_math_tutor import numina, solution_writer

REPO_ROOT = Path(__file__).resolve().parents[1]
REQUIRED_COLUMNS = ("numina_row", "source", "kind", "problem", "solution", "answer", "equation", "variable",
                    "sample_round")
# Lý do loại mà lời giải viết trực tiếp không được thay thế.
NOT_OVERRIDABLE = frozenset({"rounding_dependent", "arithmetic_source_excluded"})


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1 << 20), b""):
            digest.update(block)
    return "sha256:" + digest.hexdigest()


def _data_root() -> Path:
    root = os.environ.get("NHOM8_DATA_ROOT")
    if not root:
        raise SystemExit("Thiếu NHOM8_DATA_ROOT — chạy qua scripts/in-wsl.sh")
    return Path(root)


def load_overrides(path: Path) -> dict[int, list[str]]:
    """Bước viết trực tiếp theo numina_row từ `numina-authored.yaml` (khóa `numina_solutions`)."""
    if not path.is_file():
        return {}
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    items = data.get("numina_solutions") if isinstance(data, dict) else None
    if not isinstance(items, list):
        raise SystemExit(f"{path}: cần khóa 'numina_solutions' là danh sách")
    overrides: dict[int, list[str]] = {}
    for item in items:
        if not isinstance(item, dict) or set(item) != {"numina_row", "steps"}:
            raise SystemExit(f"{path}: mỗi mục cần đúng {{numina_row, steps}}: {item!r}")
        row, steps = item["numina_row"], item["steps"]
        if not isinstance(row, int) or row in overrides:
            raise SystemExit(f"{path}: numina_row không hợp lệ hoặc trùng: {row!r}")
        if not isinstance(steps, list) or not steps or not all(isinstance(s, str) for s in steps):
            raise SystemExit(f"{path}: steps của dòng {row} phải là danh sách chuỗi")
        overrides[row] = steps
    return overrides


def _row_result(row: dict, override: list[str] | None) -> dict:
    if row["kind"] == "equation" and not (isinstance(row["variable"], str) and len(row["variable"]) == 1):
        raise SystemExit(f"dòng {row['numina_row']}: bài phương trình thiếu chữ cái ẩn gốc (cột variable)")
    base = {
        "numina_row": int(row["numina_row"]),
        "source": row["source"],
        "sample_round": row["sample_round"],
        "kind": row["kind"],
        "problem": row["problem"],
        "answer": row["answer"],
        "equation": row["equation"],
        "variable": row["variable"],
    }
    try:
        written = solution_writer.write_solution(row["kind"], row["problem"], row["solution"], row["answer"],
                                                 row["equation"], source=row["source"])
    except solution_writer.SolutionRejected as rejected:
        if override is not None and rejected.reason in NOT_OVERRIDABLE:
            raise SystemExit(f"dòng {row['numina_row']} bị loại {rejected.reason}: không dùng lời giải viết trực tiếp")
        if override is None or row["kind"] != "arithmetic":
            return {**base, "status": "dropped", "reason": rejected.reason, "detail": rejected.detail[:300]}
        try:
            written = solution_writer.write_ai_arithmetic(row["problem"], row["answer"], override)
        except solution_writer.SolutionRejected as failed:
            raise SystemExit(f"lời giải viết trực tiếp cho dòng {row['numina_row']} không qua kiểm: {failed}") from failed
        base["cot_drop_reason"] = rejected.reason
    else:
        if override is not None:
            raise SystemExit(f"dòng {row['numina_row']} đã có lời giải {written.solution_source}; bỏ mục viết trực tiếp thừa")
    return {
        **base,
        "status": "ok",
        "solution_source": written.solution_source,
        "steps": list(written.steps),
        "misconceptions": [dict(item) for item in written.misconceptions],
        "skill_ids": list(written.skill_ids),
        "difficulty": written.difficulty,
    }


def main(argv: list[str] | None = None) -> int:
    root = _data_root()
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--candidates", type=Path, default=root / "numina" / "candidates.parquet")
    parser.add_argument("--out", type=Path, default=root / "numina" / "solutions.jsonl")
    parser.add_argument("--summary", type=Path, default=root / "numina" / "solutions-summary.json")
    parser.add_argument("--overrides", type=Path, default=REPO_ROOT / "data" / "problems" / "numina-authored.yaml")
    args = parser.parse_args(argv)
    overrides = load_overrides(args.overrides)

    if not args.candidates.is_file():
        print(f"Không thấy {args.candidates} — chạy scripts/extract-numina-subset.py trước", file=sys.stderr)
        return 2
    started = time.perf_counter()
    table = pq.read_table(args.candidates)
    missing = [column for column in REQUIRED_COLUMNS if column not in table.column_names]
    if missing:
        print(f"{args.candidates} thiếu cột {missing}", file=sys.stderr)
        return 2
    rows = sorted(table.select(list(REQUIRED_COLUMNS)).to_pylist(), key=lambda r: int(r["numina_row"]))
    if len({r["numina_row"] for r in rows}) != len(rows):
        print("numina_row trùng trong ứng viên", file=sys.stderr)
        return 2

    unknown = sorted(set(overrides) - {int(r["numina_row"]) for r in rows})
    if unknown:
        print(f"lời giải viết trực tiếp cho dòng không có trong ứng viên: {unknown}", file=sys.stderr)
        return 2
    results = [_row_result(row, overrides.get(int(row["numina_row"]))) for row in rows]
    elapsed = time.perf_counter() - started

    args.out.parent.mkdir(parents=True, exist_ok=True)
    tmp = args.out.with_suffix(".jsonl.tmp")
    with tmp.open("w", encoding="utf-8", newline="\n") as handle:
        for result in results:
            handle.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")
    tmp.replace(args.out)

    outcome = Counter((r["source"], r["kind"], r["status"] if r["status"] == "ok" else r["reason"]) for r in results)
    by_strategy = Counter(r["solution_source"] for r in results if r["status"] == "ok")
    by_round = Counter((r["sample_round"], f"{r['source']}/{r['kind']}", r["status"]) for r in results)
    strategy_yield = Counter((r["source"], r["kind"], r.get("solution_source", "dropped")) for r in results)
    misconception_types = Counter(m["type"] for r in results if r["status"] == "ok" for m in r["misconceptions"])
    summary = {
        "candidates": {"path": str(args.candidates), "sha256": _sha256(args.candidates), "rows": len(rows)},
        "solutions": {"path": str(args.out), "sha256": _sha256(args.out)},
        "ok": sum(r["status"] == "ok" for r in results),
        "dropped": sum(r["status"] != "ok" for r in results),
        "drop_reasons": dict(sorted(Counter(r["reason"] for r in results if r["status"] != "ok").items())),
        "by_source_kind_outcome": [
            {"source": s, "kind": k, "outcome": o, "count": c} for (s, k, o), c in sorted(outcome.items())
        ],
        "ok_by_solution_source": dict(sorted(by_strategy.items())),
        "by_source_kind_solution_source": [
            {"source": s, "kind": k, "solution_source": o, "count": c} for (s, k, o), c in sorted(strategy_yield.items())
        ],
        "by_sample_round": [
            {"sample_round": r, "stratum": s, "status": o, "count": c} for (r, s, o), c in sorted(by_round.items())
        ],
        "min_steps": solution_writer.MIN_STEPS,
        "arithmetic_sources": sorted(numina.ARITHMETIC_SOURCES),
        "misconception_types": dict(sorted(misconception_types.items())),
        "authored_overrides": {
            "path": str(args.overrides),
            "count": len(overrides),
            "replaced_cot_drop_reasons": dict(sorted(
                Counter(r["cot_drop_reason"] for r in results if "cot_drop_reason" in r).items())),
        },
        "elapsed_s": round(elapsed, 2),
    }
    args.summary.write_text(json.dumps(summary, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(f"{summary['ok']} đạt / {summary['dropped']} loại trên {len(rows)} ứng viên trong {elapsed:.1f} s")
    print(f"Lý do loại: {summary['drop_reasons']}")
    print(f"Ghi {args.out} ({summary['solutions']['sha256'][:19]}…) và {args.summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
