"""Batch baseline: fold dùng learner_rules, khử trùng trước khi gọi luật, lọc sent_time ≤ cutoff, lịch, đánh giá.

Phần Spark chạy local[2] trên một log mô phỏng nhỏ (25 người học × 15 phút, trễ/trùng cao) và so với
bản tham chiếu viết thẳng bằng `learner_rules.apply_event`, không qua mã batch.
"""

from __future__ import annotations

import csv
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pytest

from ai_math_tutor import learner_rules
from ai_math_tutor.batch.evaluate import (
    GroundTruthError,
    JobResult,
    TrueEpisode,
    evaluate_episodes,
    first_detections,
    load_ground_truth,
    read_header,
    visibility_latency,
)
from ai_math_tutor.batch.fold import FoldInvariantError, fold_skill
from ai_math_tutor.batch.schedule import DAY_MS, HOUR_MS, ScheduleError, make_schedule, seeded_phase_ms
from ai_math_tutor.batch.source import SourceError, open_source
from ai_math_tutor.simulator import SimulationConfig, run_simulation
from ai_math_tutor.simulator.output import iter_rows, parquet_metadata

REPO = Path(__file__).resolve().parents[1]
C, W, H = learner_rules.OBSERVATION_CORRECT, learner_rules.OBSERVATION_WRONG, learner_rules.OBSERVATION_HINT


# ---------------------------------------------------------------- fold (thuần Python)

def _fold(observations):
    return fold_skill("L-1", "s", [(t, t + 5, f"e{t:03d}", o) for t, o in enumerate(observations)])


def test_fold_matches_learner_rules_and_records_onset_and_end():
    sequence = [W] * 9 + [H, W, C, C, H, C]
    result = _fold(sequence)
    profile = learner_rules.EMPTY_PROFILE
    for observation in sequence:
        profile = learner_rules.update(profile, observation)
    assert {k: result[k] for k in ("opportunities", "correct", "hints_used", "consec_wrong", "streak", "mastered")} == {
        k: v for k, v in profile.to_dict().items()
    }
    assert result["onset_event_id"] == "e010" and result["onset_event_time_ms"] == 10 and result["onset_sent_time_ms"] == 15
    assert result["end_event_id"] == "e014" and result["end_event_time_ms"] == 14
    assert result["stuck"] is False and result["mastered"] is True and result["last_event_time_ms"] == 14


def test_fold_reports_consec_wrong_alert_separately_from_stuck():
    alert_only = _fold([W, W, W])
    assert alert_only["consec_wrong_alert"] is True and alert_only["stuck"] is False and alert_only["onset_event_id"] is None
    stuck = _fold([C, W] * 5)
    assert stuck["stuck"] is True and stuck["consec_wrong_alert"] is False and stuck["end_event_id"] is None


def test_fold_rejects_empty_sequence():
    with pytest.raises(FoldInvariantError):
        fold_skill("L-1", "s", [])


# ---------------------------------------------------------------- lịch

def test_seeded_phase_is_deterministic_whole_seconds_and_seed_dependent():
    phases = {seeded_phase_ms("daily", seed) for seed in range(20)}
    assert seeded_phase_ms("daily", 11) == seeded_phase_ms("daily", 11)
    assert len(phases) > 15 and all(0 <= p < DAY_MS and p % 1000 == 0 for p in phases)
    assert all(0 <= seeded_phase_ms("hourly", s) < HOUR_MS for s in range(20))


def test_cutoffs_cover_first_and_last_arrival_inclusively():
    schedule = make_schedule("hourly", anchor_ms=1_000_000, seed=0, phase_s=600)
    first_cut = 1_000_000 + 600_000
    assert schedule.cutoff_at_or_after(first_cut) == first_cut
    assert schedule.cutoff_at_or_after(first_cut + 1) == first_cut + HOUR_MS
    assert schedule.cutoff_at_or_after(-2_500_000) == first_cut - HOUR_MS  # trước anchor vẫn đúng lưới
    cutoffs = schedule.cutoffs_covering(first_cut - 5, first_cut + 2 * HOUR_MS + 1)
    assert cutoffs == [first_cut + k * HOUR_MS for k in range(4)]
    assert schedule.describe()["phase_source"] == "override"
    assert make_schedule("daily", 0, seed=11).phase_source == "seed"


@pytest.mark.parametrize(("name", "phase_s"), [("weekly", None), ("daily", 86_400), ("hourly", -1)])
def test_invalid_schedules_are_rejected(name, phase_s):
    with pytest.raises(ScheduleError):
        make_schedule(name, 0, seed=1, phase_s=phase_s)


# ---------------------------------------------------------------- đánh giá

def _episode(learner, skill, onset, end=None, connection="wifi"):
    return TrueEpisode(learner, skill, connection, "low", f"on-{learner}", onset, end, False)


def _detection(learner, skill, sent):
    return {"learner_id": learner, "skill_id": skill, "onset_event_id": f"on-{learner}",
            "onset_event_time_ms": sent - 100, "onset_sent_time_ms": sent}


def test_time_to_detect_uses_first_job_and_its_runtime():
    schedule = make_schedule("hourly", anchor_ms=0, seed=0, phase_s=0)
    jobs = [
        JobResult(HOUR_MS, 2.5, (_detection("A", "s", 1_000),)),
        JobResult(2 * HOUR_MS, 3.0, (_detection("A", "s", 1_000), _detection("B", "s", HOUR_MS + 5))),
    ]
    found = first_detections(jobs)
    assert found[("A", "s")].visible_ms == HOUR_MS + 2_500
    truth = [_episode("A", "s", 900, end=HOUR_MS + 10_000), _episode("C", "s", 50, connection="cellular")]
    result = evaluate_episodes(truth, found, schedule)
    summary = result.summary
    assert summary["true_positives"] == 1 and summary["false_positives"] == 1 and summary["false_negatives"] == 1
    assert summary["precision"] == 0.5 and summary["recall"] == 0.5
    assert summary["time_to_detect_s"]["p50"] == pytest.approx((HOUR_MS + 2_500 - 900) / 1000)
    assert summary["detected_while_open"] == 1 and summary["trigger_rule_holds"] == 1 and summary["onset_event_match"] == 1
    assert summary["by_connection"]["cellular"]["recall"] == 0.0
    assert [fp["learner_id"] for fp in result.false_positives] == ["B"]
    with pytest.raises(ValueError):
        first_detections(list(reversed(jobs)))


def test_visibility_latency_uses_first_cutoff_at_or_after_sent_time():
    event = np.array([0, 50, 3_000, 9_000])
    sent = np.array([10, 1_000, 3_500, 9_500])
    stats = visibility_latency(event, sent, [1_000, 5_000], [0.5, 1.0])
    # visible: 1500, 1500, 6000, không có job ⇒ không tính
    assert stats["events"] == 4 and stats["not_covered"] == 1
    assert stats["mean_s"] == pytest.approx(((1_500 - 0) + (1_500 - 50) + (6_000 - 3_000)) / 3 / 1000)
    with pytest.raises(ValueError):
        visibility_latency(event, sent, [5_000, 1_000], [1.0, 1.0])


def test_ground_truth_is_shifted_by_the_time_base(tmp_path):
    path = tmp_path / "gt.jsonl"
    lines = [
        {"record": "run", "t0": "2026-11-02T00:00:00Z", "seed": 3, "time_unit": "seconds_from_t0"},
        {"record": "learner", "learner_id": "L"},
        {"record": "stuck_episode", "learner_id": "L", "skill_id": "s", "connection": "wifi", "ability_group": "low",
         "onset_event_id": "e", "onset_t_s": 1.5, "end_t_s": None, "latent_mastered_at_onset": True},
    ]
    path.write_text("".join(json.dumps(line) + "\n" for line in lines), encoding="utf-8")
    truth = load_ground_truth(path, base_ms=10_000)
    assert truth.episodes[0].onset_ms == 11_500 and truth.episodes[0].end_ms is None
    assert read_header(path)["seed"] == 3
    path.write_text(json.dumps({"record": "learner"}) + "\n", encoding="utf-8")
    with pytest.raises(GroundTruthError):
        load_ground_truth(path, base_ms=0)


def test_open_source_validates_path_format_and_restamp_columns(tmp_path, tiny_log):
    with pytest.raises(SourceError):
        open_source(tmp_path / "missing.parquet")
    jsonl = tmp_path / "events.jsonl"
    jsonl.write_text("{}\n", encoding="utf-8")
    assert open_source(jsonl).format == "jsonl"
    assert not open_source(jsonl, replay_start_ms=5).restamps
    assert open_source(tiny_log / "events.parquet", replay_start_ms=5).restamps
    with pytest.raises(SourceError):
        open_source(jsonl, fmt="csv")
    with pytest.raises(SourceError):
        open_source(jsonl, replay_start_ms=-1)


# ---------------------------------------------------------------- Spark trên log mô phỏng nhỏ

@pytest.fixture(scope="module")
def tiny_log(tmp_path_factory) -> Path:
    out = tmp_path_factory.mktemp("tiny-log")
    run_simulation(SimulationConfig(learners=25, horizon_s=900, seed=5, out_dir=out, target_rate=6.0,
                                    late_profile="high", duplicate_rate=0.05))
    return out


@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(master="local[2]", shuffle_partitions=2, data_root=tmp_path_factory.mktemp("spark"))
    with spark_session(conf) as session:
        yield session


def reference_profiles(log_dir: Path, cutoff_ms: int, dedup: bool = True) -> dict[tuple[str, str], learner_rules.SkillProfile]:
    """Tham chiếu độc lập: lọc sent ≤ cutoff, (tùy chọn) khử trùng, sắp, apply_event từng người học."""
    assert int(parquet_metadata(log_dir / "events.parquet")["nhom8.t0_epoch_ms"]) == T0_MS
    rows = list(iter_rows(log_dir / "events.parquet"))
    events, seen = [], set()
    for row in rows:
        event = json.loads(row["value"])
        sent = T0_MS + row["sent_offset_ms"]
        if sent > cutoff_ms or (dedup and event["event_id"] in seen):
            continue
        seen.add(event["event_id"])
        events.append((T0_MS + row["event_offset_ms"], event["session_seq"], event["event_id"], event))
    by_learner: dict[str, dict[str, learner_rules.SkillProfile]] = defaultdict(dict)
    for *_, event in sorted(events, key=lambda item: item[:3]):
        by_learner[event["learner_id"]] = learner_rules.apply_event(by_learner[event["learner_id"]], event)
    return {(learner, skill): p for learner, skills in by_learner.items() for skill, p in skills.items()}


T0_MS = 1_793_577_600_000  # 2026-11-02T00:00:00Z, t0 mặc định của bộ mô phỏng


def _batch_profiles(spark, source, cutoff_ms):
    from ai_math_tutor.batch.daily_profile import profiles_dataframe

    return {(r["learner_id"], r["skill_id"]): r.asDict() for r in profiles_dataframe(spark, source, cutoff_ms).collect()}


def _assert_same_rules(batch, reference):
    assert set(batch) == set(reference)
    for key, profile in reference.items():
        row = batch[key]
        assert {k: row[k] for k in profile.to_dict()} == profile.to_dict(), key
        assert row["stuck"] == learner_rules.is_wheel_spinning(profile)
        assert row["consec_wrong_alert"] == learner_rules.consec_wrong_alert(profile)


def test_dataframe_job_dedups_before_rules_and_filters_by_sent_time(spark, tiny_log):
    rows = list(iter_rows(tiny_log / "events.parquet", columns=["event_id", "sent_offset_ms"]))
    assert len(rows) > len({r["event_id"] for r in rows}), "log thử phải có bản trùng"
    source = open_source(tiny_log / "events.parquet")
    mid = T0_MS + sorted(r["sent_offset_ms"] for r in rows)[len(rows) // 2]
    end = T0_MS + max(r["sent_offset_ms"] for r in rows)
    for cutoff in (mid, end):
        _assert_same_rules(_batch_profiles(spark, source, cutoff), reference_profiles(tiny_log, cutoff))
    # Không khử trùng thì số đếm khác ⇒ test trên thật sự phân biệt được.
    assert reference_profiles(tiny_log, end, dedup=False) != reference_profiles(tiny_log, end)
    # Sự kiện có sent_time đúng bằng cutoff được tính, sau cutoff 1 ms thì không.
    first = T0_MS + min(r["sent_offset_ms"] for r in rows)
    assert _batch_profiles(spark, source, first - 1) == {}
    _assert_same_rules(_batch_profiles(spark, source, first), reference_profiles(tiny_log, first))


def test_final_cutoff_onsets_match_ground_truth_exactly(spark, tiny_log):
    rows = list(iter_rows(tiny_log / "events.parquet", columns=["sent_offset_ms"]))
    end = T0_MS + max(r["sent_offset_ms"] for r in rows)
    batch = _batch_profiles(spark, open_source(tiny_log / "events.parquet"), end)
    onsets = {k: r["onset_event_id"] for k, r in batch.items() if r["onset_event_id"]}
    truth = load_ground_truth(tiny_log / "ground_truth.jsonl", base_ms=T0_MS).episodes
    assert truth, "log thử phải có episode bế tắc"
    assert onsets == {e.key: e.onset_event_id for e in truth}
    assert {k for k, r in batch.items() if r["end_event_id"]} == {e.key for e in truth if e.end_ms is not None}


def test_restamped_replay_shifts_times_but_not_profiles(spark, tiny_log):
    rows = list(iter_rows(tiny_log / "events.parquet", columns=["sent_offset_ms"]))
    end_offset = max(r["sent_offset_ms"] for r in rows)
    replay_start = 1_900_000_000_123
    plain = _batch_profiles(spark, open_source(tiny_log / "events.parquet"), T0_MS + end_offset)
    replayed = _batch_profiles(spark, open_source(tiny_log / "events.parquet", replay_start_ms=replay_start),
                               replay_start + end_offset)
    shift = replay_start - T0_MS
    time_columns = ("last_event_time_ms", "onset_event_time_ms", "onset_sent_time_ms", "end_event_time_ms")
    assert set(plain) == set(replayed)
    for key, row in plain.items():
        moved = {c: (None if row[c] is None else row[c] + shift) for c in time_columns}
        assert {**row, **moved} == replayed[key]


def test_snapshot_adds_template_hints_only_for_stuck_profiles(spark):
    from ai_math_tutor.batch.fold import PROFILE_DDL
    from ai_math_tutor.batch.snapshot import finalize_snapshot, hint_table
    from ai_math_tutor.hints.templates import template_hint

    skill = "linear_equation.isolate_variable"
    base = ("L", skill, 10, 2, 0, 0, 0, False)
    rows = [
        ("L1", *base[1:], True, False, 5, "e", 1, 2, None, None),
        ("L2", *base[1:], True, True, 5, "e", 1, 2, None, None),
        ("L3", *base[1:], False, True, 5, None, None, None, None, None),
    ]
    snapshot = finalize_snapshot(spark.createDataFrame(rows, schema=PROFILE_DDL), 99, hint_table(spark))
    got = {r["learner_id"]: (r["hint_level"], r["hint_text_vi"], r["cutoff_ms"]) for r in snapshot.collect()}
    assert got["L1"] == (1, template_hint(skill, 1).text_vi, 99)
    assert got["L2"] == (2, template_hint(skill, 2).text_vi, 99)
    assert got["L3"] == (None, None, 99)


def test_cli_end_to_end_writes_snapshots_raw_results_and_run_record(tmp_path, tiny_log):
    results, snapshots = tmp_path / "results", tmp_path / "snapshots"
    env = {**os.environ, "NHOM8_DATA_ROOT": str(tmp_path / "data")}
    command = [
        sys.executable, "-m", "ai_math_tutor.batch.run_batch_baseline", "--schedule", "hourly",
        "--events", str(tiny_log / "events.parquet"), "--ground-truth", str(tiny_log / "ground_truth.jsonl"),
        "--results", str(results), "--out-root", str(snapshots), "--run-id", "batch-hourly-test",
        "--master", "local[2]", "--partitions", "2", "--parity-check",
    ]
    done = subprocess.run(command, cwd=REPO, env=env, capture_output=True, text=True, timeout=300)
    assert done.returncode == 0, done.stderr[-3000:]
    record = json.loads((results / "runs.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert record["experiment"] == "batch-hourly" and record["status"] == "success"
    assert record["run_id"] == "batch-hourly-test" and record["seed"] == 5
    metrics = record["metrics"]
    assert metrics["parity"]["identical"] is True
    assert metrics["episodes"]["recall"] == 1.0 and metrics["episodes"]["precision"] == 1.0
    assert metrics["episodes"]["trigger_rule_holds"] == metrics["episodes"]["true_positives"]
    assert 0 <= metrics["episodes"]["time_to_detect_s"]["max"] <= 3600 + metrics["job_runtime_s"]["max"] + 1
    assert metrics["rows_in"] > metrics["rows_unique"] > 0 and metrics["staleness"]["not_covered"] == 0
    assert record["parameters"]["schedule"]["phase_source"] == "seed"
    raw = results / "batch" / "batch-hourly-test"
    with (raw / "jobs.csv").open(encoding="utf-8") as handle:
        jobs = list(csv.DictReader(handle))
    assert len(jobs) == metrics["jobs"] >= 1
    assert all((snapshots / "batch-hourly-test" / f"cutoff_ms={j['cutoff_ms']}").is_dir() for j in jobs)
    assert (raw / "episodes.csv").is_file() and (raw / "summary.json").is_file()
    # Chạy lại với run_id đã có bị từ chối (kết quả thô không bị ghi đè).
    again = subprocess.run(command, cwd=REPO, env=env, capture_output=True, text=True, timeout=300)
    assert again.returncode != 0
