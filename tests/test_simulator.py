"""Bộ mô phỏng: tất định, hợp đồng + nhãn toán, tỉ lệ lỗi mạng, ground truth tái lập bằng learner_rules."""

import json
from collections import defaultdict
from pathlib import Path

import pytest
import yaml

from ai_math_tutor import learner_rules, math_check
from ai_math_tutor.event_validation import load_problems, validate_iter
from ai_math_tutor.problem_bank import load_problems as load_bank
from ai_math_tutor.simulator import SimulationConfig, run_simulation
from ai_math_tutor.simulator.behavior import PreparedProblem, wrong_answer_candidates
from ai_math_tutor.simulator.cli import main as cli_main
from ai_math_tutor.simulator.engine import DEFAULT_PROBLEMS, DEFAULT_SKILLS, SessionIds
from ai_math_tutor.simulator.output import (
    EVENT_COLUMNS,
    epoch_ms,
    iso_from_epoch_ms,
    iter_events,
    iter_rows,
    parquet_metadata,
    parse_utc,
    restamp,
)
from ai_math_tutor.simulator.population import SimulationConfigError, parse_skills_config


def simulate(out_dir: Path, **overrides):
    params = dict(learners=40, horizon_s=600, seed=7, out_dir=out_dir, target_rate=4.0)
    params.update(overrides)
    manifest = run_simulation(SimulationConfig(**params))
    return manifest, list(iter_rows(out_dir / "events.parquet"))


def ground_truth(out_dir: Path, record: str) -> list[dict]:
    with (out_dir / "ground_truth.jsonl").open(encoding="utf-8") as handle:
        return [r for r in map(json.loads, handle) if r["record"] == record]


@pytest.fixture(scope="module")
def natural_day(tmp_path_factory):
    """Một ngày tự nhiên, 300 người học — dùng chung cho các test sanity/ground truth."""
    out = tmp_path_factory.mktemp("day")
    manifest = run_simulation(SimulationConfig(learners=300, horizon_s=86_400, seed=11, out_dir=out))
    return out, manifest


def test_same_seed_gives_identical_outputs_and_other_seed_differs(tmp_path):
    first, _ = simulate(tmp_path / "a")
    again, _ = simulate(tmp_path / "b")
    other, _ = simulate(tmp_path / "c", seed=8)
    checksums = {name: info["checksum"] for name, info in first["outputs"].items()}
    assert checksums == {name: info["checksum"] for name, info in again["outputs"].items()}
    assert checksums["events.parquet"] != other["outputs"]["events.parquet"]["checksum"]
    assert checksums["ground_truth.jsonl"] != other["outputs"]["ground_truth.jsonl"]["checksum"]


def test_every_delivery_passes_contract_and_sympy_labels(tmp_path):
    manifest, _ = simulate(tmp_path, learners=60, horizon_s=900, target_rate=2.0, late_profile="high", duplicate_rate=0.05)
    counts = manifest["counts"]
    assert counts["unique_events"] >= 1000
    report = validate_iter(iter_events(tmp_path / "events.parquet"), load_problems(DEFAULT_PROBLEMS))
    assert report.errors == []
    assert report.lines == counts["deliveries"]
    assert report.unique_events == counts["unique_events"]
    assert report.duplicate_deliveries == counts["duplicate_deliveries"] > 0
    assert report.late_events == counts["late_events"] > 0
    assert report.seq_gaps == {}
    assert report.sessions == counts["sessions"]
    labelled = sum(v for k, v in counts["by_event_type"].items() if k in ("submit_step", "wrong_answer", "complete"))
    assert report.math_labels_checked == labelled >= 700


def test_duplicate_and_late_rates_within_ten_percent_of_configuration(tmp_path):
    manifest, rows = simulate(tmp_path, learners=200, horizon_s=200, target_rate=100.0,
                              late_profile="high", duplicate_rate=0.1)
    rates, counts = manifest["rates"], manifest["counts"]
    assert counts["unique_events"] >= 18_000
    assert rates["duplicate_rate_realized"] == pytest.approx(0.1, rel=0.10)
    assert rates["late_rate_realized"] == pytest.approx(rates["late_rate_expected"], rel=0.10)
    faults = ground_truth(tmp_path, "delivery")
    assert sum(f["kind"] == "duplicate" for f in faults) == counts["duplicate_deliveries"]
    assert sum(f["kind"] == "late" for f in faults) == counts["late_events"]
    # Mạng di động chịu muộn nhiều hơn wifi (hồ sơ high: 12% so với 1%).
    by_conn = counts["by_connection"]
    cell = by_conn["cellular"]["late_events"] / by_conn["cellular"]["unique_events"]
    wifi = by_conn["wifi"]["late_events"] / by_conn["wifi"]["unique_events"]
    assert cell > 5 * wifi


def test_parquet_layout_order_and_byte_identical_duplicates(tmp_path):
    manifest, rows = simulate(tmp_path, duplicate_rate=0.2)
    meta = parquet_metadata(tmp_path / "events.parquet")
    t0_ms = epoch_ms(parse_utc(meta["nhom8.t0"]))
    assert meta["nhom8.schema_version"] == "0.1" and int(meta["nhom8.t0_epoch_ms"]) == t0_ms
    assert list(rows[0]) == list(EVENT_COLUMNS)
    replay = [r["replay_offset_ms"] for r in rows]
    assert replay == sorted(replay)
    first_seen: dict[str, dict] = {}
    for row in rows:
        event = json.loads(row["value"])
        assert json.dumps(event) == row["value"]  # dạng json.dumps chuẩn ⇒ đóng dấu lại tất định
        assert event["event_time"] == iso_from_epoch_ms(t0_ms + row["event_offset_ms"])
        assert event["sent_time"] == iso_from_epoch_ms(t0_ms + row["sent_offset_ms"])
        assert row["sent_offset_ms"] > row["event_offset_ms"]
        for column in ("event_id", "learner_id", "class_id", "session_id", "session_seq", "event_type", "problem_id"):
            assert row[column] == event[column]
        assert row["connection"] == event["client"]["connection"]
        original = first_seen.get(row["event_id"])
        if original is None:
            assert row["replay_offset_ms"] == row["sent_offset_ms"]
            first_seen[row["event_id"]] = row
        else:
            assert row["value"] == original["value"]
            assert row["sent_offset_ms"] == original["sent_offset_ms"]
            assert row["replay_offset_ms"] > original["replay_offset_ms"]
    assert len(rows) - len(first_seen) == manifest["counts"]["duplicate_deliveries"] > 0


def test_ground_truth_onsets_match_shared_rule_on_true_sequence(natural_day):
    out, _ = natural_day
    unique = {}
    for event in iter_events(out / "events.parquet"):
        unique.setdefault(event["event_id"], event)
    by_learner = defaultdict(list)
    for event in unique.values():
        by_learner[event["learner_id"]].append(event)
    onsets, ends, onset_times = set(), set(), {}
    for events in by_learner.values():
        events.sort(key=lambda e: (e["event_time"], e["session_seq"]))
        profiles: dict[str, learner_rules.SkillProfile] = {}
        for event in events:
            updated = learner_rules.apply_event(profiles, event)
            for skill in event["skill_ids"]:
                was = learner_rules.is_wheel_spinning(profiles.get(skill, learner_rules.EMPTY_PROFILE))
                now = learner_rules.is_wheel_spinning(updated.get(skill, learner_rules.EMPTY_PROFILE))
                key = (event["learner_id"], skill, event["event_id"])
                if now and not was:
                    onsets.add(key)
                    onset_times[key] = event["event_time"]
                elif was and not now:
                    ends.add(key)
            profiles = updated
    episodes = ground_truth(out, "stuck_episode")
    assert episodes and {(e["learner_id"], e["skill_id"], e["onset_event_id"]) for e in episodes} == onsets
    assert {(e["learner_id"], e["skill_id"], e["end_event_id"]) for e in episodes if e["end_event_id"]} == ends
    t0_ms = epoch_ms(parse_utc(ground_truth(out, "run")[0]["t0"]))
    for e in episodes:
        expected = onset_times[(e["learner_id"], e["skill_id"], e["onset_event_id"])]
        assert iso_from_epoch_ms(t0_ms + round(e["onset_t_s"] * 1000)) == expected
        assert e["onset_opportunity"] == learner_rules.WHEEL_SPIN_OPPORTUNITIES


def test_sanity_low_ability_learners_get_stuck_more(natural_day):
    _, manifest = natural_day
    gt = manifest["ground_truth"]
    learners, stuck = gt["learners_by_group"], gt["stuck_learners_by_group"]
    assert stuck["low"] > 0
    assert stuck["low"] / learners["low"] > stuck["high"] / learners["high"]
    assert gt["mastery_transitions"] > 0


def test_mastery_records_follow_latent_start_state(natural_day):
    out, _ = natural_day
    learners = {r["learner_id"]: r for r in ground_truth(out, "learner")}
    seen = set()
    for record in ground_truth(out, "mastery"):
        key = (record["learner_id"], record["skill_id"])
        assert key not in seen  # không quên ⇒ mỗi kỹ năng thành thạo ẩn tối đa một lần
        seen.add(key)
        assert not learners[record["learner_id"]]["skills"][record["skill_id"]]["mastered_at_start"]


def test_nobody_wheel_spins_when_guess_is_high_and_slip_low(tmp_path):
    config = yaml.safe_load(DEFAULT_SKILLS.read_text(encoding="utf-8"))
    for skill in config["skills"]:
        skill["bkt"].update(p_init=[0.0, 0.05], p_learn=[0.0, 0.01], p_guess=[0.97, 0.98], p_slip=[0.005, 0.01])
    skills_path = tmp_path / "extreme.yaml"
    skills_path.write_text(yaml.safe_dump(config), encoding="utf-8")
    manifest, _ = simulate(tmp_path / "out", learners=100, horizon_s=1800, target_rate=20.0, skills_path=skills_path)
    assert manifest["counts"]["unique_events"] > 5000
    assert manifest["ground_truth"]["stuck_episodes"] == 0


def test_target_rate_sets_mean_think_time(tmp_path):
    manifest, rows = simulate(tmp_path, learners=200, horizon_s=240, target_rate=50.0, duplicate_rate=0.0)
    assert manifest["params"]["think_mean_s"] == pytest.approx(4.0)
    assert manifest["rates"]["realized_unique_events_per_s"] == pytest.approx(50.0, rel=0.10)
    assert max(r["event_offset_ms"] for r in rows) <= 240_000


def test_restamp_shifts_both_times_and_keeps_duplicates_identical(tmp_path):
    _, rows = simulate(tmp_path, duplicate_rate=0.3)
    replay_start = epoch_ms(parse_utc("2027-01-01T09:30:00Z"))
    stamped: dict[str, str] = {}
    for row in rows:
        value = restamp(row["value"], replay_start, row["event_offset_ms"], row["sent_offset_ms"])
        event = json.loads(value)
        assert event["event_time"] == iso_from_epoch_ms(replay_start + row["event_offset_ms"])
        assert event["sent_time"] == iso_from_epoch_ms(replay_start + row["sent_offset_ms"])
        assert {k: v for k, v in event.items() if not k.endswith("_time")} == {
            k: v for k, v in json.loads(row["value"]).items() if not k.endswith("_time")
        }
        assert stamped.setdefault(row["event_id"], value) == value


def test_roster_lists_every_learner_once(tmp_path):
    manifest, rows = simulate(tmp_path, learners=75, class_size=30)
    roster = json.loads((tmp_path / "roster.json").read_text(encoding="utf-8"))
    members = [learner for ids in roster.values() for learner in ids]
    assert len(members) == len(set(members)) == 75
    assert sorted(len(ids) for ids in roster.values()) == [15, 30, 30]
    assert {r["learner_id"] for r in rows} <= set(members)
    assert manifest["counts"]["classes"] == 3


@pytest.mark.parametrize("problem", load_bank(DEFAULT_PROBLEMS), ids=lambda p: p.problem_id)
def test_generated_wrong_answers_and_steps_are_really_wrong(problem):
    prepared = PreparedProblem.from_problem(problem)
    assert prepared.wrong_answers == wrong_answer_candidates(problem)
    for answer in prepared.wrong_answers:
        assert not math_check.answer_matches(problem.equation, answer)
    for options in prepared.wrong_steps:
        assert options and all(not math_check.step_is_equivalent(problem.equation, e) for e in options)


def test_session_ids_are_unique_for_all_learner_session_pairs():
    ids = SessionIds(seed=3, learners=1000)
    produced = {ids.get(learner, session) for learner in range(1000) for session in range(1, 40)}
    assert len(produced) == 1000 * 39


def test_invalid_configuration_rejected(tmp_path):
    with pytest.raises(SimulationConfigError):
        simulate(tmp_path / "a", late_profile="extreme")
    with pytest.raises(SimulationConfigError):
        simulate(tmp_path / "b", duplicate_rate=0.9)
    with pytest.raises(SimulationConfigError):
        simulate(tmp_path / "c", split="test")  # kho mini chỉ có dev
    with pytest.raises(SimulationConfigError):
        parse_skills_config({"version": 1, "ability_groups": {"all": {"share": 0.5, "quantile": [0, 1]}},
                             "skills": []})
    bad = yaml.safe_load(DEFAULT_SKILLS.read_text(encoding="utf-8"))
    bad["skills"][0]["bkt"]["p_guess"] = [0.5, 0.95]
    with pytest.raises(SimulationConfigError):
        parse_skills_config(bad)


def test_cli_writes_outputs_and_refuses_to_overwrite(tmp_path, capsys):
    out = tmp_path / "run"
    args = ["--learners", "20", "--minutes", "5", "--target-rate", "2", "--seed", "5", "--out", str(out)]
    assert cli_main(args) == 0
    summary = json.loads(capsys.readouterr().out)
    assert summary["counts"]["learners"] == 20
    assert {p.name for p in out.iterdir()} == {"events.parquet", "ground_truth.jsonl", "roster.json", "manifest.json"}
    manifest = json.loads((out / "manifest.json").read_text(encoding="utf-8"))
    assert manifest["seed"] == 5 and manifest["problem_bank"]["split"] == "dev"
    assert manifest["problem_bank"]["checksum"].startswith("sha256:")
    assert cli_main(args) == 2
    assert cli_main([*args, "--overwrite"]) == 0
    assert cli_main(["--learners", "20", "--minutes", "5", "--seed", "5", "--out", str(out),
                     "--overwrite", "--duplicate-rate", "0.9"]) == 2
