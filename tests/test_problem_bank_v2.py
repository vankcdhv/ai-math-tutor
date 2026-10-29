"""Kho bài v2 đã commit: đúng 1.000 bài test, 100% bài qua kiểm tự động, ≥ 2 bước không tầm thường,
bài số học chỉ từ nguồn bài lời văn, bài phương trình hiển thị đúng ẩn gốc, split rời nhau, manifest khớp tệp."""

from __future__ import annotations

import hashlib
import importlib.util
import json
import os
import random
import re
from collections import Counter
from pathlib import Path

import pytest
import yaml

from ai_math_tutor import math_check, numina, problem_bank, solution_writer
from ai_math_tutor.problem_bank import load_problems

ROOT = Path(__file__).resolve().parents[1]
BANK = ROOT / "data_sample" / "problem-bank-v2.jsonl"
MANIFEST = ROOT / "data_sample" / "problem-bank-v2-manifest.json"
INDEX = ROOT / "data_sample" / "problem-index-v2.json"
OVERRIDES = ROOT / "data" / "problems" / "numina-authored.yaml"
PROBLEMS = load_problems(BANK)
NUMINA = [p for p in PROBLEMS if p.source != "self"]
ROWS = [json.loads(line) for line in BANK.read_text(encoding="utf-8").splitlines()]
MANIFEST_DATA = json.loads(MANIFEST.read_text(encoding="utf-8"))
ID_PREFIX = {("test", "arithmetic"): "NTA", ("test", "equation"): "NTE",
             ("dev", "arithmetic"): "NDA", ("dev", "equation"): "NDE"}


def _load_script(name: str):
    spec = importlib.util.spec_from_file_location(name.replace("-", "_"), ROOT / "scripts" / f"{name}.py")
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _sha256(data: bytes) -> str:
    return "sha256:" + hashlib.sha256(data).hexdigest()


# ---------------------------------------------------------------------------
# Kiểm tự động trên TOÀN BỘ kho (một test cho mỗi bài, không loại trừ bài nào)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("problem", PROBLEMS, ids=[p.problem_id for p in PROBLEMS])
def test_every_problem_passes_automated_verification(problem: problem_bank.Problem) -> None:
    assert problem_bank.verify_problem(problem) == []
    assert solution_writer.verify_record(problem.to_dict()) == []
    assert 1 <= len(problem.misconceptions) <= solution_writer.MAX_MISCONCEPTIONS


@pytest.mark.parametrize("problem", NUMINA, ids=[p.problem_id for p in NUMINA])
def test_every_numina_problem_has_at_least_two_non_trivial_steps(problem: problem_bank.Problem) -> None:
    assert len(problem.steps) >= solution_writer.MIN_STEPS
    if problem.kind == "arithmetic":
        assert not any(solution_writer.is_identity_step(step) for step in problem.steps)
    else:
        keys = [solution_writer._same_form_key(text) for text in (problem.equation or "", *problem.steps)]
        assert all(first != second for first, second in zip(keys, keys[1:])), "bước trùng hình thức bước trước/đề"


# ---------------------------------------------------------------------------
# Kích thước, split, nguồn gốc
# ---------------------------------------------------------------------------


def test_test_split_has_exactly_1000_numina_problems() -> None:
    test = [p for p in PROBLEMS if p.split == "test"]
    assert len(test) == 1000
    assert all(p.source.startswith("numina:") for p in test)
    share = sum(p.kind == "equation" for p in test) / len(test)
    assert 0.25 <= share <= 0.30


def test_dev_split_has_numina_and_self_authored_problems() -> None:
    dev = [p for p in PROBLEMS if p.split == "dev"]
    assert sum(p.source.startswith("numina:") for p in dev) == 200
    authored = [p for p in dev if p.source == "self"]
    assert len(authored) == 60
    assert all(p.statement_vi and p.solution_source == "authored" for p in authored)


def test_arithmetic_problems_come_only_from_word_problem_sources() -> None:
    by_split = MANIFEST_DATA["numina_by_split_source_kind"]
    for split in ("dev", "test"):
        arithmetic_sources = {key.split("/")[0] for key in by_split[split] if key.endswith("/arithmetic")}
        assert arithmetic_sources == set(numina.ARITHMETIC_SOURCES), split
    assert set(MANIFEST_DATA["selection"]["available_ok"]["arithmetic"]) <= set(numina.ARITHMETIC_SOURCES)
    assert MANIFEST_DATA["numina"]["arithmetic_sources"] == sorted(numina.ARITHMETIC_SOURCES)
    outcomes = MANIFEST_DATA["solution_writing"]["by_source_kind_outcome"]
    for source in ("math", "synthetic_math"):
        assert set(outcomes[f"{source}/arithmetic"]) == {"arithmetic_source_excluded"}


def test_splits_are_disjoint_by_id_row_and_statement() -> None:
    ids = [p.problem_id for p in PROBLEMS]
    assert len(set(ids)) == len(ids)
    rows = [p.source for p in PROBLEMS if p.source != "self"]
    assert len(set(rows)) == len(rows)
    keys = {split: {numina.problem_key(p.statement) for p in PROBLEMS if p.split == split} for split in ("dev", "test")}
    assert not keys["dev"] & keys["test"]


def test_problem_ids_encode_split_and_kind() -> None:
    for problem in PROBLEMS:
        prefix = problem.problem_id.split("-")[1]
        if problem.source == "self":
            assert prefix in {"SLIN", "SQUAD", "SARI"} and problem.split == "dev"
        else:
            assert prefix == ID_PREFIX[(problem.split, problem.kind)]


def test_solution_source_is_honest_per_strategy() -> None:
    overrides = yaml.safe_load(OVERRIDES.read_text(encoding="utf-8"))["numina_solutions"]
    hand_written = {item["numina_row"]: item["steps"] for item in overrides}
    for problem in PROBLEMS:
        if problem.source == "self":
            assert problem.solution_source == "authored"
        elif problem.kind == "equation":
            assert problem.solution_source == "generated_sympy"
        else:
            row = int(problem.source.split(":", 1)[1])
            if row in hand_written:
                assert problem.solution_source == "authored"
                assert list(problem.steps) == hand_written[row]
            else:
                assert problem.solution_source == "derived_from_cot"
        assert problem.verified_by == ("sympy",)


def test_hand_written_numina_solutions_are_a_minority_fallback() -> None:
    hand = [p for p in NUMINA if p.solution_source == "authored"]
    assert len(hand) <= 0.05 * len(NUMINA)
    assert MANIFEST_DATA["solution_writing"]["authored_overrides"]["count"] >= len(hand)


def test_skill_ids_exist_in_skills_config() -> None:
    config = yaml.safe_load((ROOT / "configs" / "skills.yaml").read_text(encoding="utf-8"))
    known = {skill["skill_id"]: skill["kind"] for skill in config["skills"]}
    for problem in PROBLEMS:
        for skill in problem.skill_ids:
            assert known[skill] == problem.kind, (problem.problem_id, skill)


def test_bank_file_is_compact_and_sorted() -> None:
    assert BANK.stat().st_size < 5_000_000
    assert [p.problem_id for p in PROBLEMS] == sorted(p.problem_id for p in PROBLEMS)


def test_previous_bank_version_is_not_in_the_working_tree() -> None:
    for name in ("problem-bank-v1.jsonl", "problem-bank-v1-manifest.json", "problem-index-v1.json"):
        assert not (ROOT / "data_sample" / name).exists()


# ---------------------------------------------------------------------------
# Ẩn gốc của bài phương trình: máy kiểm theo x, hiển thị theo ẩn của đề
# ---------------------------------------------------------------------------


def test_numina_equations_store_the_original_variable() -> None:
    equations = [p for p in NUMINA if p.kind == "equation"]
    assert equations and all(p.variable is not None for p in equations)
    assert any(p.variable != "x" for p in equations)
    for problem in equations:
        assert re.search(rf"(?<![A-Za-z\\]){problem.variable}(?![A-Za-z])", problem.statement), problem.problem_id


def test_machine_strings_use_x_and_display_uses_the_original_variable() -> None:
    renamed = [p for p in NUMINA if p.kind == "equation" and p.variable != "x"]
    assert renamed
    for problem in renamed:
        for step, shown in zip(problem.steps, problem.display_steps):
            assert not re.search(r"[a-wyz]", step)  # chuỗi máy chỉ có ẩn x
            assert "x" not in shown and problem.variable in shown
            assert math_check.step_is_equivalent(problem.equation, step)


# ---------------------------------------------------------------------------
# Manifest
# ---------------------------------------------------------------------------


def test_manifest_checksums_match_committed_files() -> None:
    files = MANIFEST_DATA["files"]
    assert files["bank"]["path"] == "data_sample/problem-bank-v2.jsonl"
    assert files["bank"]["sha256"] == _sha256(BANK.read_bytes())
    assert files["index"]["sha256"] == _sha256(INDEX.read_bytes())
    lines = BANK.read_text(encoding="utf-8").splitlines(keepends=True)
    for split, digest in MANIFEST_DATA["split_sha256"].items():
        selected = "".join(line for line, row in zip(lines, ROWS) if row["split"] == split)
        assert digest == _sha256(selected.encode("utf-8")), split


def test_manifest_counts_match_bank() -> None:
    counts = MANIFEST_DATA["counts"]
    assert MANIFEST_DATA["bank_version"] == "v2"
    assert counts["total"] == len(PROBLEMS)
    assert counts["by_split"] == dict(sorted(Counter(p.split for p in PROBLEMS).items()))
    for split in ("dev", "test"):
        kinds = Counter(p.kind for p in PROBLEMS if p.split == split)
        assert counts["by_split_kind"][split] == dict(sorted(kinds.items()))
    assert MANIFEST_DATA["verification"]["failures"] == 0
    assert MANIFEST_DATA["verification"]["problems"] == len(PROBLEMS)


def test_manifest_records_provenance_seed_and_drops() -> None:
    numina_meta = MANIFEST_DATA["numina"]
    assert numina_meta["revision"] == "9d8d210c9f6a36c8f3cd84045668c9b7800ef517"
    assert numina_meta["license"] == "apache-2.0"
    assert "Apache License 2.0" in numina_meta["attribution"]
    assert set(numina_meta["candidates_by_round"]) == {"base", "supplement"}
    assert sum(numina_meta["candidates_by_round"]["base"].values()) == 3000
    assert isinstance(MANIFEST_DATA["seed"], int)
    writing = MANIFEST_DATA["solution_writing"]
    assert set(writing["drop_reasons"]) <= set(solution_writer.DROP_REASONS)
    assert writing["ok"] + writing["dropped"] == numina_meta["candidates_rows"]
    assert sum(writing["drop_reasons"].values()) == writing["dropped"]
    assert writing["min_steps"] == solution_writer.MIN_STEPS


def test_candidates_manifest_keeps_the_base_sample_and_documents_the_supplement() -> None:
    candidates = json.loads((ROOT / "data_sample" / "numina-candidates-manifest.json").read_text(encoding="utf-8"))
    assert candidates["target"] == 3000
    assert set(candidates["supplement"]["sizes"]) == set(numina.ARITHMETIC_SOURCES)
    supplement = candidates["candidate_counts_by_round"]["supplement"]
    assert set(supplement) == {f"{source}/arithmetic" for source in numina.ARITHMETIC_SOURCES}
    assert candidates["candidate_total"] == 3000 + sum(supplement.values())


# ---------------------------------------------------------------------------
# Dựng lại tất định (cần dữ liệu cục bộ dưới $NHOM8_DATA_ROOT)
# ---------------------------------------------------------------------------


def _local_solutions() -> Path | None:
    root = os.environ.get("NHOM8_DATA_ROOT")
    if not root:
        return None
    path = Path(root) / "numina" / "solutions.jsonl"
    return path if path.is_file() and (path.parent / "solutions-summary.json").is_file() else None


def test_rebuild_is_deterministic(tmp_path: Path) -> None:
    solutions = _local_solutions()
    if solutions is None:
        pytest.skip("không có $NHOM8_DATA_ROOT/numina/solutions.jsonl (chạy scripts/write-step-solutions.py)")
    build = _load_script("build-problem-bank")
    manifest = build.build(
        solutions, solutions.parent / "solutions-summary.json", ROOT / "data" / "problems",
        ROOT / "data_sample" / "numina-candidates-manifest.json",
        tmp_path / "bank.jsonl", tmp_path / "manifest.json", tmp_path / "index.json",
    )
    if manifest["solution_writing"]["solutions_sha256"] != MANIFEST_DATA["solution_writing"]["solutions_sha256"]:
        pytest.skip("solutions.jsonl cục bộ khác bản đã dùng để dựng kho commit")
    assert manifest["split_sha256"] == MANIFEST_DATA["split_sha256"]
    assert (tmp_path / "bank.jsonl").read_bytes() == BANK.read_bytes()
    assert (tmp_path / "index.json").read_bytes() == INDEX.read_bytes()


def test_every_problem_has_a_vietnamese_statement_with_the_same_numbers() -> None:
    from ai_math_tutor.hints.verify import number_values

    assert all(p.statement_vi for p in PROBLEMS)
    for p in NUMINA:
        assert number_values(p.statement_vi) == number_values(p.statement), p.problem_id
        if p.kind == "equation":
            assert all(seg in p.statement_vi for seg in re.findall(r"\$[^$]+\$", p.statement)), p.problem_id


def test_translation_with_a_new_number_is_rejected() -> None:
    build = _load_script("build-problem-bank")
    row = next(dict(r) for r in ROWS if r["source"] != "self")
    bad = {row["problem_id"]: {"source": row["source"], "statement_vi": row["statement"] + " Đáp số là 7777."}}
    with pytest.raises(build.BuildError):
        build.attach_statements_vi([row], bad)


def test_quota_planning() -> None:
    build = _load_script("build-problem-bank")
    assert build.plan_quotas({"arithmetic": 900, "equation": 355}, 1000, 200, 0.28) == {
        "test": {"arithmetic": 720, "equation": 280}, "dev": {"arithmetic": 125, "equation": 75}}
    assert build.plan_quotas({"arithmetic": 1300, "equation": 100}, 1000, 200, 0.28)["test"]["equation"] == 100
    with pytest.raises(build.BuildError):
        build.plan_quotas({"arithmetic": 500, "equation": 355}, 1000, 200, 0.28)


def test_assign_split_rejects_arithmetic_from_competition_sources() -> None:
    build = _load_script("build-problem-bank")
    row = {"numina_row": 1, "source": "math", "kind": "arithmetic", "status": "ok"}
    with pytest.raises(build.BuildError, match="nguồn bài lời văn"):
        build.assign_split([row], 1, 1, 0, 0.0)


def test_proportional_pick_keeps_source_mix() -> None:
    build = _load_script("build-problem-bank")
    rows = [{"numina_row": i, "source": "a" if i < 60 else ("b" if i < 90 else "c")} for i in range(100)]
    picked, rest = build.proportional_pick(rows, 10, random.Random(1))
    assert Counter(r["source"] for r in picked) == {"a": 6, "b": 3, "c": 1}
    assert len(rest) == 90 and not {r["numina_row"] for r in picked} & {r["numina_row"] for r in rest}


def test_dev_numina_covers_every_source_kind_of_test() -> None:
    by_split = MANIFEST_DATA["numina_by_split_source_kind"]
    assert set(by_split["dev"]) == set(by_split["test"])
    assert sum(by_split["dev"].values()) == 200 and sum(by_split["test"].values()) == 1000
