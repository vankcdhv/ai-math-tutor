"""Hợp đồng v0.2: tương thích ngược với v0.1, heartbeat đúng dạng, validator loại heartbeat khỏi thống kê."""

import copy
import json
from dataclasses import asdict
from pathlib import Path

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from ai_math_tutor.event_validation import (
    HEARTBEAT_LEARNER_ID,
    SCHEMA_RESOURCE,
    SCHEMA_RESOURCE_V0_1,
    is_heartbeat,
    load_schema,
    validate_events,
    validate_iter,
)
from ai_math_tutor.ingestion.replay import build_heartbeat

ROOT = Path(__file__).resolve().parents[1]
SAMPLE = ROOT / "data_sample" / "events-week1-sample.jsonl"
PROBLEMS = ROOT / "data_sample" / "problems-week1-sample.jsonl"
HEARTBEAT_ID = "0f8fad5b-d9cb-469f-a165-70867728950e"


def _sample() -> list[dict]:
    return [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines() if line.strip()]


def _errors(event) -> list[str]:
    validator = Draft202012Validator(load_schema(), format_checker=FormatChecker())
    return [error.message for error in validator.iter_errors(event)]


def _heartbeat(**changes) -> dict:
    beat = json.loads(build_heartbeat(1_800_000_000_000, event_id=HEARTBEAT_ID))
    beat.update(changes)
    return beat


def test_validator_uses_v0_2_and_v0_1_file_is_kept_unchanged():
    assert SCHEMA_RESOURCE == "learner_event_v0_2.schema.json"
    v1, v2 = load_schema(SCHEMA_RESOURCE_V0_1), load_schema()
    assert v1["properties"]["schema_version"] == {"const": "0.1"}
    assert "heartbeat" not in v1["properties"]["event_type"]["enum"]
    assert v2["properties"]["event_type"]["enum"] == v1["properties"]["event_type"]["enum"] + ["heartbeat"]
    assert set(v1["properties"]) == set(v2["properties"])  # không thêm/bớt trường nào
    Draft202012Validator.check_schema(v2)


def test_every_v0_1_sample_event_is_valid_v0_2_and_may_declare_0_2():
    for event in _sample():
        assert _errors(event) == []
        assert _errors(dict(event, schema_version="0.2")) == []
        assert _errors(dict(event, schema_version="0.3")) != []


def test_heartbeat_is_valid_and_recognised():
    beat = _heartbeat()
    assert _errors(beat) == [] and is_heartbeat(beat)
    assert not any(is_heartbeat(event) for event in _sample())
    assert not is_heartbeat(None) and not is_heartbeat(["heartbeat"])


@pytest.mark.parametrize("changes", [
    {"payload": {"x": 1}},                               # payload phải rỗng
    {"learner_id": "L-00000000000000a1"},                # chỉ learner dành riêng
    {"schema_version": "0.1"},                           # heartbeat chỉ có từ v0.2
    {"class_id": "C-SIM-0001"},                          # không mang trường học tập
    {"session_seq": 1},
    {"event_time": "2026-11-02 00:00:00"},               # sai định dạng thời gian
])
def test_malformed_heartbeats_are_rejected(changes):
    assert _errors(_heartbeat(**changes)) != []


@pytest.mark.parametrize("missing", ["event_id", "event_time", "sent_time", "payload", "learner_id"])
def test_heartbeat_missing_required_field_is_rejected(missing):
    beat = _heartbeat()
    del beat[missing]
    assert _errors(beat) != []


def test_learning_event_cannot_use_reserved_learner_or_drop_fields():
    event = _sample()[0]
    assert _errors(dict(event, learner_id=HEARTBEAT_LEARNER_ID)) != []
    for field in ("class_id", "session_id", "session_seq", "problem_id", "skill_ids", "client"):
        broken = copy.deepcopy(event)
        del broken[field]
        assert _errors(broken) != [], field


def test_validator_counts_heartbeats_separately_from_learner_statistics():
    sample = _sample()
    beats = [json.loads(build_heartbeat(1_800_000_000_000 + i * 1000)) for i in range(3)]
    mixed = sample[:10] + beats[:2] + sample[10:] + beats[2:]
    baseline, report = validate_iter(sample), validate_iter(mixed)
    assert report.errors == []
    assert report.heartbeats == 3 and baseline.heartbeats == 0
    assert report.lines == baseline.lines + 3
    without_counts = {k: v for k, v in asdict(report).items() if k not in ("lines", "heartbeats")}
    assert without_counts == {k: v for k, v in asdict(baseline).items() if k not in ("lines", "heartbeats")}
    assert report.unique_events == 19 and report.duplicate_deliveries == 1 and report.sessions == 3


def test_malformed_heartbeat_is_a_validator_error_not_a_statistic():
    report = validate_iter(_sample() + [_heartbeat(payload={"x": 1})])
    assert report.heartbeats == 0 and len(report.errors) >= 1 and report.unique_events == 19


def test_existing_jsonl_validation_behaviour_is_unchanged():
    report = validate_events(SAMPLE, PROBLEMS)
    assert report.errors == [] and report.heartbeats == 0
    assert (report.lines, report.unique_events, report.duplicate_deliveries, report.math_labels_checked) == (20, 19, 1, 12)
