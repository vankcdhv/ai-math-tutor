"""Dữ liệu mẫu tuần 1 phải đúng data contract, đúng toán và đúng kịch bản luồng."""

import json
import subprocess
import sys
from pathlib import Path

from ai_math_tutor import math_check
from ai_math_tutor.event_validation import validate_events

ROOT = Path(__file__).resolve().parents[1]
EVENTS = ROOT / "data_sample" / "events-week1-sample.jsonl"
PROBLEMS = ROOT / "data_sample" / "problems-week1-sample.jsonl"


def test_sample_is_reproducible_byte_for_byte(tmp_path):
    script = ROOT / "scripts" / "make-week1-sample.py"
    subprocess.run([sys.executable, str(script), "--out-dir", str(tmp_path)], check=True, capture_output=True)
    assert (tmp_path / EVENTS.name).read_bytes() == EVENTS.read_bytes()
    assert (tmp_path / PROBLEMS.name).read_bytes() == PROBLEMS.read_bytes()


def test_sample_matches_contract_math_and_stream_scenario():
    report = validate_events(EVENTS, PROBLEMS)
    assert report.errors == []
    assert report.lines == 20
    assert report.unique_events == 19
    assert report.duplicate_deliveries == 1
    assert report.late_events == 1
    assert report.out_of_order_events == 1
    assert report.sessions == 3
    assert report.seq_gaps == {}
    # 8 submit_step + 1 wrong_answer + 3 complete; open_problem/request_hint không mang nhãn toán.
    assert report.math_labels_checked == 12


def test_all_event_types_present():
    types = {json.loads(line)["event_type"] for line in EVENTS.read_text().splitlines()}
    assert types == {"open_problem", "submit_step", "wrong_answer", "request_hint", "complete"}


def test_problem_bank_answers_match_sympy():
    for line in PROBLEMS.read_text().splitlines():
        problem = json.loads(line)
        assert math_check.answer_matches(problem["equation"], problem["answer"]), problem["problem_id"]


def test_no_pii_or_ground_truth_fields():
    forbidden = {"name", "email", "phone", "source_id", "mastery", "true_skill", "is_stuck"}
    for line in EVENTS.read_text().splitlines():
        event = json.loads(line)
        assert forbidden.isdisjoint(event) and forbidden.isdisjoint(event["payload"])
        assert "hs-" not in line
