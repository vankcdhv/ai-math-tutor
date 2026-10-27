"""Dựng kho bài v2: hợp nhất lời giải NuminaMath đã kiểm + bài tự soạn, chia split, kiểm lại, ghi chỉ mục.

Đầu vào:
- `$NHOM8_DATA_ROOT/numina/solutions.jsonl` + `solutions-summary.json` (scripts/write-step-solutions.py)
- `data/problems/self-*.yaml` (bài tự soạn, dev, lời giải viết trực tiếp — `authored`)
- `data_sample/numina-candidates-manifest.json` (revision, license, ghi công NuminaMath)

Đầu ra (commit được):
- `data_sample/problem-bank-v2.jsonl`  — sắp theo problem_id
- `data_sample/problem-bank-v2-manifest.json` — seed, số đếm, lý do loại, sha256 từng split
- `data_sample/problem-index-v2.json` — chỉ mục đảo (related_index)

v2 khác v1 (sau khi kiểm một mẫu kho v1): bài số học chỉ từ gsm8k/orca_math;
mỗi bài NuminaMath có ≥ `solution_writer.MIN_STEPS` bước không tầm thường; bài phương trình có
bước trung gian và trường `variable` (ẩn gốc của đề, chỉ để hiển thị).

Chia split tất định (seed cố định): test đúng `--test-size` bài NuminaMath, tỉ trọng phương
trình `--equation-share`; dev `--dev-size` bài NuminaMath + toàn bộ bài tự soạn. Trong mỗi loại,
chọn chung test+dev phân tầng theo nguồn (chia đều có tràn, numina.allocate_equal) rồi tách dev
theo tỉ lệ nguồn để dev và test cùng phân bố nguồn. Mọi bài được kiểm lại
bằng problem_bank.verify_problem và solution_writer.verify_record; một bài lỗi là dừng.

Chạy (trong WSL): bash scripts/in-wsl.sh uv run python scripts/build-problem-bank.py
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import sys
import time
from collections import Counter, defaultdict
from collections.abc import Iterable, Sequence
from pathlib import Path
from typing import Any

import yaml

from ai_math_tutor import numina, problem_bank, solution_writer
from ai_math_tutor.related_index import RelatedIndex

REPO_ROOT = Path(__file__).resolve().parents[1]
BANK_VERSION = "v2"
BANK_FILE = f"problem-bank-{BANK_VERSION}.jsonl"
MANIFEST_FILE = f"problem-bank-{BANK_VERSION}-manifest.json"
INDEX_FILE = f"problem-index-{BANK_VERSION}.json"
SEED = 20261027
TEST_SIZE = 1000
DEV_SIZE = 200
EQUATION_SHARE = 0.28
KINDS = ("arithmetic", "equation")
ID_PREFIX = {
    ("test", "arithmetic"): "NTA",
    ("test", "equation"): "NTE",
    ("dev", "arithmetic"): "NDA",
    ("dev", "equation"): "NDE",
}
MAX_PER_PREFIX = 999
AUTHORED_FIELDS = {"problem_id", "statement", "statement_vi", "kind", "equation", "answer", "skill_ids",
                   "steps", "misconceptions", "difficulty"}
AUTHORED_REQUIRED = AUTHORED_FIELDS - {"equation", "misconceptions"}


class BuildError(RuntimeError):
    """Đầu vào thiếu/không nhất quán hoặc có bài không qua kiểm."""


def sha256_bytes(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


def sha256_file(path: Path) -> str:
    return sha256_bytes(path.read_bytes())


def dump_line(record: dict[str, Any]) -> str:
    return json.dumps(record, ensure_ascii=False)


# ---------------------------------------------------------------------------
# Nạp đầu vào
# ---------------------------------------------------------------------------


def load_solutions(path: Path) -> list[dict[str, Any]]:
    if not path.is_file():
        raise BuildError(f"không thấy {path} — chạy scripts/write-step-solutions.py trước")
    rows = [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines() if line.strip()]
    if len({r["numina_row"] for r in rows}) != len(rows):
        raise BuildError("numina_row trùng trong solutions.jsonl")
    return rows


def load_authored(directory: Path) -> list[dict[str, Any]]:
    """Bài tự soạn từ mọi `self-*.yaml` (khóa `problems:`), kiểm khóa rồi kiểm toán học."""
    records: list[dict[str, Any]] = []
    files = sorted(directory.glob("self-*.yaml"))
    if not files:
        raise BuildError(f"không có bài tự soạn trong {directory}")
    for path in files:
        data = yaml.safe_load(path.read_text(encoding="utf-8"))
        if not isinstance(data, dict) or not isinstance(data.get("problems"), list):
            raise BuildError(f"{path}: cần khóa 'problems' là danh sách")
        for item in data["problems"]:
            if not isinstance(item, dict):
                raise BuildError(f"{path}: mỗi bài phải là mapping")
            pid = item.get("problem_id", "?")
            unknown = set(item) - AUTHORED_FIELDS
            missing = AUTHORED_REQUIRED - set(item)
            if unknown or missing:
                raise BuildError(f"{path}:{pid}: trường lạ {sorted(unknown)} / thiếu {sorted(missing)}")
            try:
                written = solution_writer.complete_authored(item)
            except (solution_writer.SolutionRejected, KeyError, ValueError) as exc:
                raise BuildError(f"{path}:{pid}: {exc}") from exc
            record = {
                "problem_id": pid,
                "source": "self",
                "statement": item["statement"],
                "kind": item["kind"],
            }
            if item["kind"] == "equation":
                record["equation"] = item["equation"]
            record.update({
                "answer": str(item["answer"]),
                "skill_ids": list(written.skill_ids),
                "steps": list(written.steps),
                "misconceptions": [dict(m) for m in written.misconceptions],
                "difficulty": written.difficulty,
                "split": "dev",
                "solution_source": written.solution_source,
                "statement_vi": item["statement_vi"],
                "verified_by": ["sympy"],
            })
            records.append(record)
    return records


# ---------------------------------------------------------------------------
# Chia split
# ---------------------------------------------------------------------------


def plan_quotas(available: dict[str, int], test_size: int, dev_size: int, equation_share: float) -> dict[str, dict[str, int]]:
    """Số bài mỗi (split, loại). Phương trình: test lấy round(share·test) nếu đủ, phần còn lại sang dev."""
    n_equation = available.get("equation", 0)
    n_arithmetic = available.get("arithmetic", 0)
    test_equation = min(round(equation_share * test_size), n_equation)
    dev_equation = min(n_equation - test_equation, dev_size)
    test_arithmetic = test_size - test_equation
    dev_arithmetic = dev_size - dev_equation
    if test_arithmetic + dev_arithmetic > n_arithmetic:
        raise BuildError(f"không đủ bài số học: cần {test_arithmetic + dev_arithmetic}, có {n_arithmetic}")
    return {
        "test": {"arithmetic": test_arithmetic, "equation": test_equation},
        "dev": {"arithmetic": dev_arithmetic, "equation": dev_equation},
    }


def stratified_pick(rows: Sequence[dict[str, Any]], quota: int, rng: random.Random
                    ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Chọn `quota` dòng, chia đều theo nguồn (tràn sang nguồn còn bài); trả (được chọn, còn lại)."""
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(rows, key=lambda r: r["numina_row"]):
        by_source[row["source"]].append(row)
    allocation = numina.allocate_equal({(source, ""): len(items) for source, items in by_source.items()}, quota)
    picked: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for source in sorted(by_source):
        items = list(by_source[source])
        rng.shuffle(items)
        take = allocation[(source, "")]
        picked.extend(items[:take])
        rest.extend(items[take:])
    if len(picked) != quota:
        raise BuildError(f"chỉ chọn được {len(picked)}/{quota}")
    return picked, rest


def proportional_pick(rows: Sequence[dict[str, Any]], quota: int, rng: random.Random
                      ) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Chọn `quota` dòng theo tỉ lệ số dòng của từng nguồn (phần dư lớn nhất, hòa thì theo tên nguồn)."""
    by_source: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in sorted(rows, key=lambda r: r["numina_row"]):
        by_source[row["source"]].append(row)
    total = len(rows)
    if quota > total:
        raise BuildError(f"cần {quota} dòng nhưng chỉ có {total}")
    exact = {source: quota * len(items) / total if total else 0.0 for source, items in by_source.items()}
    allocation = {source: int(value) for source, value in exact.items()}
    leftover = quota - sum(allocation.values())
    for source in sorted(exact, key=lambda s: (-(exact[s] - allocation[s]), s))[:leftover]:
        allocation[source] += 1
    picked: list[dict[str, Any]] = []
    rest: list[dict[str, Any]] = []
    for source in sorted(by_source):
        items = list(by_source[source])
        rng.shuffle(items)
        picked.extend(items[: allocation[source]])
        rest.extend(items[allocation[source]:])
    return picked, rest


def numina_record(row: dict[str, Any], problem_id: str, split: str) -> dict[str, Any]:
    record: dict[str, Any] = {
        "problem_id": problem_id,
        "source": f"numina:{row['numina_row']}",
        "statement": row["problem"],
        "kind": row["kind"],
    }
    if row["kind"] == "equation":
        record["equation"] = row["equation"]
        record["variable"] = row["variable"]
    record.update({
        "answer": row["answer"],
        "skill_ids": row["skill_ids"],
        "steps": row["steps"],
        "misconceptions": [
            {"type": m["type"], "step_index": m["step_index"], "expression": m["expression"]}
            for m in row["misconceptions"]
        ],
        "difficulty": row["difficulty"],
        "split": split,
        "solution_source": row["solution_source"],
        "verified_by": ["sympy"],
    })
    return record


def assign_split(solutions: Sequence[dict[str, Any]], seed: int, test_size: int, dev_size: int,
                 equation_share: float) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    ok = [row for row in solutions if row["status"] == "ok"]
    stray = [row["numina_row"] for row in ok
             if row["kind"] == "arithmetic" and row["source"] not in numina.ARITHMETIC_SOURCES]
    if stray:
        raise BuildError(f"bài số học ngoài nguồn bài lời văn {sorted(numina.ARITHMETIC_SOURCES)}: {stray[:5]}")
    pools = {kind: [row for row in ok if row["kind"] == kind] for kind in KINDS}
    available = {kind: len(rows) for kind, rows in pools.items()}
    quotas = plan_quotas(available, test_size, dev_size, equation_share)
    rng = random.Random(seed)
    records: list[dict[str, Any]] = []
    unused: dict[str, int] = {}
    for kind in KINDS:
        # Chọn chung test + dev trước (chia đều theo nguồn), rồi tách dev theo tỉ lệ nguồn để dev
        # có cùng phân bố nguồn với test (không để dev chỉ còn nguồn lớn nhất).
        chosen, rest = stratified_pick(pools[kind], quotas["test"][kind] + quotas["dev"][kind], rng)
        dev_rows, test_rows = proportional_pick(chosen, quotas["dev"][kind], rng)
        unused[kind] = len(rest)
        for split, chosen in (("test", test_rows), ("dev", dev_rows)):
            if len(chosen) > MAX_PER_PREFIX:
                raise BuildError(f"{split}/{kind}: {len(chosen)} bài vượt {MAX_PER_PREFIX} mã của một tiền tố")
            prefix = ID_PREFIX[(split, kind)]
            for number, row in enumerate(sorted(chosen, key=lambda r: r["numina_row"]), start=1):
                records.append(numina_record(row, f"P-{prefix}-{number:03d}", split))
    selection = {
        "available_ok": {kind: dict(sorted(Counter(r["source"] for r in pools[kind]).items())) for kind in KINDS},
        "quotas": quotas,
        "unused_surplus": unused,
    }
    return records, selection


# ---------------------------------------------------------------------------
# Kiểm và thống kê
# ---------------------------------------------------------------------------


def verify_all(records: Iterable[dict[str, Any]]) -> dict[str, int]:
    """Kiểm lược đồ + toán học mọi bài bằng cả problem_bank lẫn solution_writer; lỗi là dừng.

    Bài NuminaMath còn phải có ≥ MIN_STEPS bước (bài tự soạn của dev được phép có bài một bước).
    """
    errors: list[str] = []
    problems = steps = misconceptions = 0
    for record in records:
        try:
            problem = problem_bank.parse_problem(record)
        except problem_bank.ProblemBankError as exc:
            errors.append(str(exc))
            continue
        errors.extend(problem_bank.verify_problem(problem))
        errors.extend(solution_writer.verify_record(record))
        if record["source"] != "self" and len(record["steps"]) < solution_writer.MIN_STEPS:
            errors.append(f"{record['problem_id']}: {len(record['steps'])} bước < {solution_writer.MIN_STEPS}")
        problems += 1
        steps += len(record["steps"])
        misconceptions += len(record["misconceptions"])
    if errors:
        raise BuildError(f"{len(errors)} lỗi kiểm, ví dụ: {errors[:5]}")
    return {"problems": problems, "steps": steps, "misconceptions_round_trip": misconceptions, "failures": 0}


def _nested(counter: Counter) -> dict[str, dict[str, int]]:
    out: dict[str, dict[str, int]] = defaultdict(dict)
    for (outer, inner), count in sorted(counter.items()):
        out[outer][inner] = count
    return dict(out)


def counts(records: Sequence[dict[str, Any]]) -> dict[str, Any]:
    def source_family(record: dict[str, Any]) -> str:
        return "self" if record["source"] == "self" else "numina"

    return {
        "total": len(records),
        "by_split": dict(sorted(Counter(r["split"] for r in records).items())),
        "by_split_kind": _nested(Counter((r["split"], r["kind"]) for r in records)),
        "by_split_source": _nested(Counter((r["split"], source_family(r)) for r in records)),
        "by_split_solution_source": _nested(Counter((r["split"], r["solution_source"]) for r in records)),
        "by_split_primary_skill": _nested(Counter((r["split"], r["skill_ids"][0]) for r in records)),
        "misconception_types": dict(sorted(Counter(m["type"] for r in records for m in r["misconceptions"]).items())),
        "misconceptions_per_problem": dict(sorted(Counter(str(len(r["misconceptions"])) for r in records).items())),
        "steps_per_problem": dict(sorted(Counter(str(len(r["steps"])) for r in records).items(), key=lambda kv: int(kv[0]))),
    }


def outcome_table(rows: Iterable[dict[str, Any]]) -> dict[str, dict[str, int]]:
    """[{source, kind, outcome, count}] → {"source/kind": {outcome: count}} (gọn cho manifest)."""
    table: dict[str, dict[str, int]] = defaultdict(dict)
    for row in rows:
        table[f"{row['source']}/{row['kind']}"][row["outcome"]] = row["count"]
    return {key: dict(sorted(value.items())) for key, value in sorted(table.items())}


def numina_source_counts(records: Sequence[dict[str, Any]], solutions: Sequence[dict[str, Any]]) -> dict[str, Any]:
    by_row = {row["numina_row"]: row["source"] for row in solutions}
    counter: Counter = Counter()
    for record in records:
        if record["source"] != "self":
            row = int(record["source"].split(":", 1)[1])
            counter[(record["split"], f"{by_row[row]}/{record['kind']}")] += 1
    return _nested(counter)


# ---------------------------------------------------------------------------
# Điểm vào
# ---------------------------------------------------------------------------


def build(solutions_path: Path, summary_path: Path, authored_dir: Path, candidates_manifest: Path,
          out_bank: Path, out_manifest: Path, out_index: Path, *, seed: int = SEED, test_size: int = TEST_SIZE,
          dev_size: int = DEV_SIZE, equation_share: float = EQUATION_SHARE) -> dict[str, Any]:
    solutions = load_solutions(solutions_path)
    summary = json.loads(summary_path.read_text(encoding="utf-8"))
    if summary["solutions"]["sha256"] != sha256_file(solutions_path):
        raise BuildError("solutions-summary.json không khớp solutions.jsonl — chạy lại write-step-solutions.py")
    numina_meta = json.loads(candidates_manifest.read_text(encoding="utf-8"))

    numina_records, selection = assign_split(solutions, seed, test_size, dev_size, equation_share)
    authored = load_authored(authored_dir)
    records = sorted(numina_records + authored, key=lambda r: r["problem_id"])
    if len({r["problem_id"] for r in records}) != len(records):
        raise BuildError("problem_id trùng giữa bài NuminaMath và bài tự soạn")
    verification = verify_all(records)

    lines = [dump_line(record) for record in records]
    out_bank.write_text("\n".join(lines) + "\n", encoding="utf-8", newline="\n")
    split_sha = {
        split: sha256_bytes("".join(line + "\n" for line, r in zip(lines, records) if r["split"] == split).encode("utf-8"))
        for split in problem_bank.SPLITS
    }
    index = RelatedIndex.build(records)
    index.save(out_index)

    manifest = {
        "bank_version": BANK_VERSION,
        "seed": seed,
        "targets": {"test_size": test_size, "dev_size_numina": dev_size, "equation_share_test": equation_share},
        "id_scheme": {
            "P-NTA-###": "NuminaMath, test, arithmetic", "P-NTE-###": "NuminaMath, test, equation",
            "P-NDA-###": "NuminaMath, dev, arithmetic", "P-NDE-###": "NuminaMath, dev, equation",
            "P-SLIN-### / P-SQUAD-### / P-SARI-###": "tự soạn (dev): bậc nhất / bậc hai / số học",
            "order": "trong mỗi tiền tố NuminaMath, số thứ tự tăng theo numina_row",
        },
        "numina": {
            "dataset": numina_meta["dataset"],
            "url": numina_meta["url"],
            "revision": numina_meta["revision"],
            "split": numina_meta["split"],
            "license": numina_meta["license"]["spdx"],
            "attribution": numina_meta["attribution"],
            "candidates_sha256": summary["candidates"]["sha256"],
            "candidates_rows": summary["candidates"]["rows"],
            "candidates_by_round": numina_meta.get("candidate_counts_by_round", {}),
            "arithmetic_sources": sorted(numina.ARITHMETIC_SOURCES),
        },
        "solution_writing": {
            "solutions_sha256": summary["solutions"]["sha256"],
            "ok": summary["ok"],
            "dropped": summary["dropped"],
            "drop_reasons": summary["drop_reasons"],
            "by_source_kind_outcome": outcome_table(summary["by_source_kind_outcome"]),
            "ok_by_solution_source": summary["ok_by_solution_source"],
            "by_source_kind_solution_source": {
                f"{row['source']}/{row['kind']}/{row['solution_source']}": row["count"]
                for row in summary["by_source_kind_solution_source"]
            },
            "by_sample_round": {
                f"{row['sample_round']}/{row['stratum']}/{row['status']}": row["count"] for row in summary["by_sample_round"]
            },
            "authored_overrides": {k: v for k, v in summary["authored_overrides"].items() if k != "path"},
            "min_steps": solution_writer.MIN_STEPS,
            "strategies": {
                "derived_from_cot": ("bước a op b = c tách từ lời giải CoT của NuminaMath (chỉ gsm8k, orca_math), chuẩn hóa, "
                                     "mỗi bước qua arith_step; bỏ bước đồng nhất; chuỗi ≥ 2 bước, mọi số có nguồn gốc, "
                                     "mọi số liệu của đề và mọi kết quả trung gian được dùng (ghép cặp), giá trị cuối "
                                     "của CoT = đáp án \\boxed"),
                "generated_sympy": ("bước phương trình sinh bằng chương trình: vế chỉ có số thì tính dần (quy đồng, nhân "
                                    "nghịch đảo, (a ± b)², đặt thừa số chung); bậc nhất: phá ngoặc/quy đồng → chuyển vế → "
                                    "cô lập x; bậc hai: dạng chuẩn → tách hạng tử giữa → nhóm → nhân tử; ≥ 2 bước; bước "
                                    "viết theo ẩn x, ẩn gốc ở trường variable"),
                "authored": ("bước viết trực tiếp: 60 bài tự soạn và một số bài số học gsm8k mà cách tách "
                               "từ CoT thất bại (data/problems/numina-authored.yaml); vẫn qua cùng bộ kiểm và bộ lọc chuỗi"),
            },
        },
        "display": ("steps/equation/misconceptions viết theo ẩn x (math_check chỉ kiểm x); hiển thị thay x bằng "
                    "variable qua problem_bank.display_expression"),
        "selection": selection,
        "counts": counts(records),
        "numina_by_split_source_kind": numina_source_counts(records, solutions),
        "verification": {
            **verification,
            "checks": [
                "problem_bank.parse_problem (lược đồ, mẫu problem_id/skill_id của hợp đồng sự kiện)",
                "problem_bank.verify_problem (bước, đáp án, diagnose khứ hồi)",
                "solution_writer.verify_record (bước cuối = đáp án, không bước đồng nhất, mọi misconception được "
                "diagnose nhận đúng loại)",
                f"bài NuminaMath có ≥ {solution_writer.MIN_STEPS} bước",
            ],
        },
        "split_sha256": split_sha,
        "files": {
            "bank": {"path": f"data_sample/{BANK_FILE}", "sha256": sha256_file(out_bank),
                     "bytes": out_bank.stat().st_size},
            "index": {"path": f"data_sample/{INDEX_FILE}", "sha256": sha256_file(out_index),
                      "bytes": out_index.stat().st_size, "n_docs": index.n_docs, "terms": len(index.vocabulary)},
        },
    }
    out_manifest.write_text(json.dumps(manifest, ensure_ascii=False, indent=2) + "\n", encoding="utf-8", newline="\n")
    return manifest


def main(argv: list[str] | None = None) -> int:
    root = os.environ.get("NHOM8_DATA_ROOT")
    if not root:
        print("Thiếu NHOM8_DATA_ROOT — chạy qua scripts/in-wsl.sh", file=sys.stderr)
        return 2
    numina_dir = Path(root) / "numina"
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--solutions", type=Path, default=numina_dir / "solutions.jsonl")
    parser.add_argument("--summary", type=Path, default=numina_dir / "solutions-summary.json")
    parser.add_argument("--authored", type=Path, default=REPO_ROOT / "data" / "problems")
    parser.add_argument("--candidates-manifest", type=Path, default=REPO_ROOT / "data_sample" / "numina-candidates-manifest.json")
    parser.add_argument("--out-dir", type=Path, default=REPO_ROOT / "data_sample")
    parser.add_argument("--seed", type=int, default=SEED)
    parser.add_argument("--test-size", type=int, default=TEST_SIZE)
    parser.add_argument("--dev-size", type=int, default=DEV_SIZE)
    parser.add_argument("--equation-share", type=float, default=EQUATION_SHARE)
    args = parser.parse_args(argv)
    if not 0 <= args.equation_share <= 1 or args.test_size < 1 or args.dev_size < 0:
        print("tham số split không hợp lệ", file=sys.stderr)
        return 2

    started = time.perf_counter()
    try:
        manifest = build(
            args.solutions, args.summary, args.authored, args.candidates_manifest,
            args.out_dir / BANK_FILE, args.out_dir / MANIFEST_FILE, args.out_dir / INDEX_FILE,
            seed=args.seed, test_size=args.test_size, dev_size=args.dev_size, equation_share=args.equation_share,
        )
    except BuildError as exc:
        print(f"Lỗi build: {exc}", file=sys.stderr)
        return 1
    elapsed = time.perf_counter() - started
    c = manifest["counts"]
    print(f"Kho {manifest['bank_version']}: {c['total']} bài {c['by_split_kind']} trong {elapsed:.1f} s")
    print(f"split_sha256: {manifest['split_sha256']}")
    print(f"bank {manifest['files']['bank']['bytes'] / 1e6:.2f} MB, index {manifest['files']['index']['bytes'] / 1e6:.2f} MB")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
