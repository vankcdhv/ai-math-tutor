"""Bộ gợi ý offline cho audit: đủ tổ hợp tình huống × bậc × nguồn trên tập test, có text_vi, tất định."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from ai_math_tutor.hints.audit_set import AUDIT_VERSION, default_output, iter_audit_rows, scenarios, write_audit_set
from ai_math_tutor.hints.generator import GENERATORS, HintEngine
from ai_math_tutor.hints.templates import LEVELS
from test_hint_verify import synthetic_bank, synthetic_rows


@pytest.fixture(scope="module")
def engine():
    return HintEngine.from_bank(synthetic_bank())


def test_rows_cover_test_split_scenarios_levels_and_sources(engine):
    rows = list(iter_audit_rows(engine, "test"))
    test_problems = [p for p in engine.bank.problems if p.split == "test"]
    expected = sum(len(scenarios(p)) for p in test_problems) * len(LEVELS) * len(GENERATORS)
    assert len(rows) == expected
    assert {r["split"] for r in rows} == {"test"}
    assert {(r["hint_level"], r["generator"]) for r in rows} == {(lv, g) for lv in LEVELS for g in GENERATORS}
    assert all(r["verified"] and r["text_vi"] and r["answer_allowed"] is False for r in rows)
    assert len({r["audit_id"] for r in rows}) == len(rows)
    # Lỗi đã khai được chẩn đoán đúng loại; mỗi bộ sinh chỉ cho đúng nguồn của nó (ladder không rơi về mẫu).
    assert {r["misconception"] for r in rows} >= {"sign_error", "wrong_operation", "arithmetic_slip"}
    assert {r["source"] for r in rows if r["generator"] == "ladder"} == {"ladder"}
    assert {r["source"] for r in rows if r["generator"] == "template"} == {"template"}


def test_answer_locked_ladder_rows_carry_the_note_and_only_a_masked_form(engine):
    rows = list(iter_audit_rows(engine, "test"))
    locked = [r for r in rows if r["fallback_note"] == "answer_locked"]
    assert locked and all(r["generator"] == "ladder" and r["hint_level"] == 3 for r in locked)
    assert all(r["math"] == [] and "□" in r["text_vi"] for r in locked)
    assert {r["fallback_note"] for r in rows} == {"", "answer_locked"}


def test_audit_rows_never_contain_the_final_answer_step(engine):
    answers = {row["problem_id"]: row for row in synthetic_rows()}
    for row in iter_audit_rows(engine, "test"):
        final_step = answers[row["problem_id"]]["steps"][-1]
        if row["source"] == "ladder":
            assert final_step not in row["math"] and final_step not in row["text_vi"]


def test_write_is_deterministic_apart_from_timestamp(engine, tmp_path):
    bank_path = tmp_path / "bank.jsonl"
    bank_path.write_text("\n".join(json.dumps(r, ensure_ascii=False) for r in synthetic_rows()) + "\n", encoding="utf-8")
    first = write_audit_set(engine, tmp_path / "a.jsonl", "test", bank_path)
    second = write_audit_set(engine, tmp_path / "b.jsonl", "test", bank_path)
    assert (tmp_path / "a.jsonl").read_bytes() == (tmp_path / "b.jsonl").read_bytes()
    assert first["rows"] == first["verified_rows"] == second["rows"]
    assert first["output_sha256"] == second["output_sha256"]
    assert sum(cell["rows"] for cell in first["cells"]) == first["rows"]
    assert first["audit_version"] == AUDIT_VERSION == 2
    assert any(cell["fallback_note"] == "answer_locked" for cell in first["cells"])


def test_default_output_name_carries_the_audit_version(tmp_path):
    assert default_output(tmp_path, Path("data/problem-bank-v2.jsonl"), "test") == \
        tmp_path / "hints" / f"audit-set-v{AUDIT_VERSION}-problem-bank-v2-test.jsonl"
