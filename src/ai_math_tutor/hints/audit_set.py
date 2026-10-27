"""Sinh offline bộ gợi ý để audit chất lượng: bài (mặc định tập **test**) × tình huống × bậc × nguồn.

Không lấy từ log dịch vụ: log không có `text_vi` và chỉ phủ những gì tải thử gửi tới. Ở đây mỗi bài
có các tình huống: chưa gửi biểu thức (bước 1) và mỗi lỗi đã khai trong kho (`misconceptions`:
biểu thức sai tại `step_index`, loại lỗi lấy từ `diagnose`). Với mỗi tình huống, sinh đủ bậc 1–3
bằng cả hai bộ sinh (`template`, `ladder`) dưới điều kiện chặt nhất: **chưa** được đưa đáp án
(`answer_allowed=False`, tức `progress.wrong_answers < 2`).

Mỗi dòng JSONL có đủ ngữ cảnh cho người chấm (đề, đáp án, đường giải, biểu thức của người học — viết
theo ẩn gốc của đề như người học thấy) và gợi ý (`text_vi`, `math`, `verified`, nguồn thực tế, ghi chú kiểm
toán `fallback_note`: lý do rơi về mẫu, hoặc `answer_locked` khi bậc 3 ladder chỉ đưa dạng bước). `audit_id` tất định.
SymPy chạy trong tiến trình vì dữ liệu là kho bài của dự án (không phải đầu vào người dùng).

`AUDIT_VERSION` tăng mỗi khi cách sinh gợi ý đổi (bản 2: bậc 3 ladder bị khóa đáp án đưa dạng của chính bước
thay vì rơi về mẫu). Tên tệp mặc định mang số bản, nên bộ của bản trước không bị ghi đè.

Chạy: python -m ai_math_tutor.hints.audit_set [--problems PATH] [--split test|dev|all] [--out PATH]
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
from collections import Counter
from collections.abc import Iterator
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from ai_math_tutor import math_check
from ai_math_tutor.hints.generator import GENERATORS, HintEngine, default_problem_bank_path
from ai_math_tutor.hints.ladder import LevelDecision
from ai_math_tutor.hints.templates import LEVELS
from ai_math_tutor.problem_bank import Problem, ProblemBank, file_checksum

AUDIT_VERSION = 2


def audit_id(problem_id: str, step_index: int, expression: str | None, level: int, generator: str) -> str:
    text = "|".join([problem_id, str(step_index), expression or "", str(level), generator])
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def scenarios(problem: Problem) -> list[tuple[int, str | None, str, str | None]]:
    """(step_index, biểu thức, trạng thái, loại lỗi): chưa gửi gì + mỗi lỗi đã khai trong kho."""
    items: list[tuple[int, str | None, str, str | None]] = [(1, None, "none", None)]
    row = problem.to_dict()
    for item in problem.misconceptions:
        result = math_check.diagnose(row, item.expression, item.step_index)
        status = "correct" if result["equivalent"] else "wrong"
        items.append((item.step_index, item.expression, status, result["misconception"]))
    return items


def iter_audit_rows(engine: HintEngine, split: str | None = "test") -> Iterator[dict[str, Any]]:
    for problem in engine.bank.select(split).problems:
        for step_index, expression, status, misconception in scenarios(problem):
            ctx = engine.context(problem.problem_id, step_index, status, misconception)
            for level in LEVELS:
                decision = LevelDecision(level=level, answer_allowed=False, reason="audit")
                for name in GENERATORS:
                    hint = engine.generator(name).generate(ctx, decision)
                    yield {
                        "audit_id": audit_id(problem.problem_id, step_index, expression, level, name),
                        "problem_id": problem.problem_id,
                        "split": problem.split,
                        "kind": problem.kind,
                        "skill_id": problem.primary_skill,
                        "statement": problem.statement_vi or problem.statement,
                        "variable": problem.display_variable,
                        "equation": problem.display(problem.equation) if problem.equation else None,
                        "answer": problem.answer,
                        "steps": list(problem.display_steps),
                        "step_index": step_index,
                        "learner_expression": problem.display(expression) if expression else None,
                        "expression_status": status,
                        "misconception": misconception,
                        "hint_level": hint.hint_level,
                        "answer_allowed": decision.answer_allowed,
                        "generator": name,
                        "source": hint.source,
                        "verified": hint.verified,
                        "text_vi": hint.text_vi,
                        "math": list(hint.math),
                        "fallback_note": hint.note,
                    }


def write_audit_set(engine: HintEngine, out: Path, split: str | None, bank_path: Path) -> dict[str, Any]:
    """Ghi JSONL (tệp tạm rồi đổi tên) và trả manifest (đếm theo bậc × bộ sinh × nguồn × ghi chú × loại lỗi).

    Ghi chú trong manifest chỉ giữ loại (phần trước dấu ':'), không giữ chi tiết lỗi kiểm chứng.
    """
    out.parent.mkdir(parents=True, exist_ok=True)
    tmp = out.with_suffix(out.suffix + ".tmp")
    counts: Counter[str] = Counter()
    by_cell: Counter[tuple[int, str, str, str, str]] = Counter()
    problems: set[str] = set()
    with tmp.open("w", encoding="utf-8") as handle:
        for row in iter_audit_rows(engine, split):
            handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")
            counts["rows"] += 1
            counts["verified"] += bool(row["verified"])
            problems.add(row["problem_id"])
            note = row["fallback_note"].split(":", 1)[0]
            by_cell[(row["hint_level"], row["generator"], row["source"], note, str(row["misconception"]))] += 1
    os.replace(tmp, out)
    return {
        "audit_version": AUDIT_VERSION,
        "created": datetime.now(timezone.utc).isoformat(),
        "problem_bank": str(bank_path),
        "problem_bank_sha256": file_checksum(bank_path),
        "split": split or "all",
        "problems": len(problems),
        "rows": counts["rows"],
        "verified_rows": counts["verified"],
        "answer_allowed": False,
        "cells": [{"hint_level": lv, "generator": g, "source": s, "fallback_note": f, "misconception": m, "rows": n}
                  for (lv, g, s, f, m), n in sorted(by_cell.items())],
        "output": str(out),
        "output_sha256": file_checksum(out),
    }


def default_output(data_root: Path, bank_path: Path, split: str) -> Path:
    """`<data_root>/hints/audit-set-v<bản>-<kho bài>-<split>.jsonl`."""
    return data_root / "hints" / f"audit-set-v{AUDIT_VERSION}-{bank_path.stem}-{split}.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--problems", type=Path, default=None)
    parser.add_argument("--split", choices=("test", "dev", "all"), default="test")
    parser.add_argument("--out", type=Path, default=None,
                        help="mặc định $NHOM8_DATA_ROOT/hints/audit-set-v<bản>-<kho bài>-<split>.jsonl")
    args = parser.parse_args(argv)
    bank_path = args.problems or default_problem_bank_path()
    split = None if args.split == "all" else args.split
    data_root = Path(os.environ.get("NHOM8_DATA_ROOT", "~/nhom8-data")).expanduser()
    out = args.out or default_output(data_root, bank_path, args.split)
    engine = HintEngine.from_bank(ProblemBank.from_jsonl(bank_path))
    manifest = write_audit_set(engine, out, split, bank_path)
    manifest_path = out.with_name(out.stem + "-manifest.json")
    manifest_path.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    print(json.dumps({k: manifest[k] for k in ("problems", "rows", "verified_rows", "output")}, ensure_ascii=False))
    return 0 if manifest["rows"] == manifest["verified_rows"] else 1


if __name__ == "__main__":
    sys.exit(main())
