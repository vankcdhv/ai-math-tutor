"""Đối soát trên một ca nhỏ tự dựng (đáp án tính tay) và oracle so với processor thật.

Ca nhỏ (offset ms từ t0; replay_start = R):
- A (wifi): a1 open_problem, a2 sai, a3 đúng — đều được áp.
- B (cellular): b1 open_problem, b3 đúng được áp; b2 sai **đến muộn** và bị loại theo watermark,
  b2 có thêm một bản trùng có chủ đích ⇒ 7 lần giao, 6 sự kiện duy nhất, 1 bị loại.
- C (wifi): c1..c10 sai ⇒ bế tắc ở c10 (ground truth onset 9,0 s); cảnh báo hiện ở R + 10.500 ms.
"""

from __future__ import annotations

import json
import random

import fakeredis
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from ai_math_tutor.bkt import BktParams
from ai_math_tutor.streaming.oracle import OracleError, OracleEvent, compare_state, compute_oracle, load_window_events
from ai_math_tutor.streaming.processor import Event, MemoryLearnerStore, bkt_params_for
from ai_math_tutor.streaming.reconcile import (
    ReconcileError,
    drop_report,
    read_alert_stream,
    reconcile,
    write_report,
)
from ai_math_tutor.streaming.sinks import RedisKeys, write_batch_parquet

R = 1_800_000_000_000
SKILL = "linear_equation.isolate_variable"
LEARNERS = {"A": ("L-000000000000000a", "wifi"), "B": ("L-000000000000000b", "cellular"),
            "C": ("L-000000000000000c", "wifi")}
CLASS = "C-T-1"


def _value(event_id, learner, event_type, seq, correct=None):
    payload = {} if event_type == "open_problem" else {"step_index": 1, "expression": "x = 1", "is_correct": correct}
    return json.dumps({"event_id": event_id, "event_type": event_type, "learner_id": learner, "session_seq": seq,
                       "skill_ids": [SKILL], "payload": payload})


def _delivery(event_id, who, event_type, seq, event_ms, *, correct=None, sent_extra=40, replay_ms=None):
    learner, connection = LEARNERS[who]
    sent = event_ms + sent_extra
    return {"replay_offset_ms": sent if replay_ms is None else replay_ms, "event_offset_ms": event_ms,
            "sent_offset_ms": sent, "event_id": event_id, "learner_id": learner, "class_id": CLASS,
            "event_type": event_type, "connection": connection, "session_seq": seq,
            "value": _value(event_id, learner, event_type, seq, correct)}


def _deliveries():
    rows = [
        _delivery("a1", "A", "open_problem", 1, 0),
        _delivery("a2", "A", "submit_step", 2, 1000, correct=False),
        _delivery("a3", "A", "submit_step", 3, 2000, correct=True),
        _delivery("b1", "B", "open_problem", 1, 0),
        _delivery("b3", "B", "submit_step", 3, 2000, correct=True),
    ]
    rows += [_delivery(f"c{i}", "C", "submit_step", i, (i - 1) * 1000, correct=False) for i in range(1, 11)]
    late = _delivery("b2", "B", "submit_step", 2, 1000, correct=False, sent_extra=95_000)
    rows += [late, {**late, "replay_offset_ms": late["replay_offset_ms"] + 3000}]
    return sorted(rows, key=lambda r: r["replay_offset_ms"])


def _write_events(path, rows):
    schema = pa.schema([("replay_offset_ms", pa.int64()), ("event_offset_ms", pa.int64()),
                        ("sent_offset_ms", pa.int64()), ("event_id", pa.string()), ("learner_id", pa.string()),
                        ("class_id", pa.string()), ("event_type", pa.string()), ("connection", pa.string()),
                        ("session_seq", pa.int32()), ("value", pa.string())])
    pq.write_table(pa.Table.from_pylist(rows, schema=schema), path)


def _write_truth(path):
    a, b, c = (LEARNERS[k][0] for k in "ABC")
    records = [
        {"record": "run", "t0": "2026-11-02T00:00:00Z"},
        {"record": "learner", "learner_id": a, "class_id": CLASS, "connection": "wifi",
         "skills": {SKILL: {"mastered_at_start": False}}},
        {"record": "learner", "learner_id": b, "class_id": CLASS, "connection": "cellular",
         "skills": {SKILL: {"mastered_at_start": True}}},
        {"record": "learner", "learner_id": c, "class_id": CLASS, "connection": "wifi",
         "skills": {SKILL: {"mastered_at_start": False}}},
        {"record": "mastery", "learner_id": a, "skill_id": SKILL, "event_id": "a2", "t_s": 1.0, "opportunity": 1},
        {"record": "stuck_episode", "learner_id": c, "class_id": CLASS, "skill_id": SKILL, "connection": "wifi",
         "onset_event_id": "c10", "onset_t_s": 9.0, "onset_opportunity": 10, "end_event_id": None,
         "end_t_s": None, "end_opportunity": None},
        {"record": "delivery", "kind": "late", "event_id": "b2", "learner_id": b, "connection": "cellular",
         "event_t_s": 1.0, "sent_t_s": 96.0, "replay_t_s": 96.0},
        {"record": "delivery", "kind": "duplicate", "event_id": "b2", "learner_id": b, "connection": "cellular",
         "event_t_s": 1.0, "sent_t_s": 96.0, "replay_t_s": 99.0},
    ]
    path.write_text("\n".join(json.dumps(r) for r in records) + "\n", encoding="utf-8")


def _state(opportunities, correct, streak, consec_wrong, *, stuck=False, mastered=False):
    return json.dumps({"class_id": CLASS, "p_mastery": None, "opportunities": opportunities, "correct": correct,
                       "consec_wrong": consec_wrong, "streak": streak, "mastered": mastered, "stuck": stuck,
                       "state_version": opportunities + 1})


@pytest.fixture
def tiny(tmp_path):
    rows = _deliveries()
    _write_events(tmp_path / "events.parquet", rows)
    _write_truth(tmp_path / "ground_truth.jsonl")
    lake = tmp_path / "lake"
    applied = [r for r in rows if r["event_id"] != "b2"]
    write_batch_parquet(lake, "applied", 0, [
        {"event_id": r["event_id"], "learner_id": r["learner_id"], "class_id": CLASS, "skill_id": SKILL,
         "event_time_ms": R + r["event_offset_ms"], "sent_time_ms": R + r["sent_offset_ms"], "out_of_order": False,
         "state_version": 1, "batch_id": 0} for r in applied])
    write_batch_parquet(lake, "latency", 0, [
        {"event_id": r["event_id"], "learner_id": r["learner_id"], "class_id": CLASS,
         "event_time_ms": R + r["event_offset_ms"], "sent_time_ms": R + r["sent_offset_ms"],
         "visible_ms": R + 10_500, "system_latency_ms": 10_500 - r["sent_offset_ms"],
         "staleness_ms": 10_500 - r["event_offset_ms"], "out_of_order": False, "batch_id": 0} for r in applied])
    c = LEARNERS["C"][0]
    write_batch_parquet(lake, "alerts", 0, [{
        "alert_id": "alert-c", "alert_type": "stuck", "learner_id": c, "class_id": CLASS, "skill_id": SKILL,
        "onset_event_id": "c10", "alert_event_time_ms": R + 9000, "visible_ms": R + 10_500,
        "stream_id": f"{R + 10_500}-0", "batch_id": 0}])

    client = fakeredis.FakeRedis()
    keys = RedisKeys("t:")
    client.hset(keys.state(LEARNERS["A"][0]), SKILL, _state(2, 1, 1, 0))
    client.hset(keys.state(LEARNERS["B"][0]), SKILL, _state(2, 1, 1, 0))  # như thể b2 đã được áp: lệch oracle
    client.hset(keys.state(c), SKILL, _state(10, 0, 0, 10, stuck=True))
    client.xadd(keys.alerts_stream, {"alert_id": "alert-c", "alert_type": "stuck", "learner_id": c,
                                     "class_id": CLASS, "skill_id": SKILL, "onset_event_id": "c10",
                                     "alert_event_time_ms": str(R + 9000)}, id=f"{R + 10_500}-0")
    progress = tmp_path / "progress.jsonl"
    progress.write_text(json.dumps({"event": "progress", "watermark_ms": R + 200_000, "dropped_by_watermark": 2,
                                    "dropped_duplicates": 0}) + "\n", encoding="utf-8")
    replay = {"replay_start_ms": R, "cutoff_ms": None, "window": {"rows": len(rows)}, "stopped_early": False,
              "end_of_log_ms": R + 9000}
    return {"dir": tmp_path, "client": client, "keys": keys, "replay": replay, "progress": progress, "lake": lake}


def _run(tiny, **overrides):
    args = dict(run_id="tiny", replay=tiny["replay"], events_path=tiny["dir"] / "events.parquet",
                ground_truth_path=tiny["dir"] / "ground_truth.jsonl", lake_root=tiny["lake"], model="rule",
                redis_client=tiny["client"], keys=tiny["keys"], progress_path=tiny["progress"])
    args.update(overrides)
    return reconcile(**args)


def test_drops_are_unique_window_minus_applied_by_connection(tiny):
    report = _run(tiny)
    dropped = report["dropped"]
    assert (dropped["deliveries"], dropped["unique_events"], dropped["injected_duplicates"]) == (17, 16, 1)
    assert dropped["total"] == dropped["total_by_formula"] == 1
    assert dropped["by_connection"]["cellular"] == {"window": 3, "applied": 2, "dropped": 1, "drop_rate": 1 / 3}
    assert dropped["by_connection"]["wifi"]["dropped"] == 0
    assert dropped["by_event_type"]["open_problem"] == {"window": 2, "applied": 2, "dropped": 0, "drop_rate": 0.0}
    assert dropped["late_events_by_connection"] == {"cellular": {"window": 1, "applied": 0, "dropped": 1,
                                                                 "drop_rate": 1.0}}
    assert dropped["spark_dropped_by_watermark_rows"] == 2  # b2 và bản trùng của nó
    assert report["applied"]["event_ids_applied_more_than_once"] == 0
    assert report["checks"]["applied_subset_of_window"] and report["checks"]["watermark_past_end_of_log"]


def test_state_compared_with_oracle_over_non_dropped_events(tiny):
    report = _run(tiny)["state_vs_oracle"]
    assert report["pairs_expected"] == 3 and report["matched"] == 2 and report["accuracy"] == pytest.approx(2 / 3)
    assert report["mismatch_by_field"] == {"opportunities": 1}  # chỉ B lệch (b2 bị loại khỏi oracle)
    assert report["examples"][0]["learner_id"] == LEARNERS["B"][0]
    assert report["missing_in_redis"] == 0 and report["extra_in_redis"] == 0


def test_latent_mastery_and_alert_time_to_detect_shifted_by_replay_start(tiny):
    report = _run(tiny)
    latent = report["state_vs_latent"]["rule_mastered"]
    # A học ẩn ở 1 s, B thành thạo từ đầu, C chưa: hệ thống chưa gắn thành thạo cho ai ⇒ 2 fn, 1 tn.
    assert (latent["fn"], latent["tn"], latent["pairs"]) == (2, 1, 3)
    alerts = report["alerts"]
    assert alerts["unique_alert_ids"] == 1 and alerts["duplicate_stream_entries"] == 0
    assert alerts["lake_matches_stream"] is True
    assert alerts["vs_oracle"] == {"oracle_onsets": 1, "system_stuck_alerts": 1, "matched": 1, "missing": 0,
                                   "extra": 0}
    truth = alerts["vs_ground_truth"]
    assert truth["episodes_in_window"] == 1 and truth["detected_exact_onset"] == 1
    assert truth["alert_ids_per_episode"] == {"1": 1}
    # c10: event offset 9.000 ms, sent 9.040 ms; hiện ở R + 10.500 ms.
    assert truth["time_to_detect_system_ms"]["p50"] == 1460.0
    assert truth["time_to_detect_staleness_ms"]["p50"] == 1500.0
    assert report["latency"]["count"] == 15


def test_duplicate_stream_entry_and_report_file(tiny, tmp_path):
    keys, client = tiny["keys"], tiny["client"]
    client.xadd(keys.alerts_stream, {"alert_id": "alert-c", "alert_type": "stuck"}, id=f"{R + 20_000}-0")
    assert len(read_alert_stream(client, keys, count=1)) == 2  # đọc nhiều trang
    report = _run(tiny)
    assert report["alerts"]["duplicate_stream_entries"] == 1
    assert report["checks"]["no_duplicate_alert_in_stream"] is False
    path = write_report(report, tmp_path / "out" / "reconcile.json")
    assert json.loads(path.read_text(encoding="utf-8"))["run_id"] == "tiny"


def test_cutoff_restricts_window_and_bad_inputs_fail_loudly(tiny):
    window = load_window_events(tiny["dir"] / "events.parquet", cutoff_ms=1000)
    assert sorted(window) == ["a1", "b1", "c1"]
    with pytest.raises(ReconcileError, match="replay_start_ms"):
        _run(tiny, replay={**tiny["replay"], "replay_start_ms": None})
    with pytest.raises(ReconcileError, match="window.rows"):
        _run(tiny, replay={**tiny["replay"], "window": {"rows": 3}})
    with pytest.raises(ReconcileError, match="model"):
        _run(tiny, model="dkt")


def test_drop_report_counts_applied_outside_window():
    event = OracleEvent("x1", "L-1", "C-1", "submit_step", "wifi", 1, 0, 40, 40, (SKILL,), {"is_correct": True})
    report = drop_report({"x1": event}, ["x1", "ghost"], deliveries=1, late_event_ids=set())
    assert report["applied_not_in_window"] == 1 and report["total"] == 0


# ---------------------------------------------------------------- oracle


def test_oracle_matches_processor_in_ordered_mode_on_shuffled_arrivals(tmp_path):
    """Processor ordered (watermark qua hết) và oracle phải cho cùng trạng thái, cả rule và bkt."""
    rng = random.Random(7)
    skills = (SKILL, "quadratic_equation.factor")
    rows = []
    for i in range(1, 61):
        event_type = rng.choice(["submit_step", "submit_step", "wrong_answer", "complete", "request_hint",
                                 "open_problem"])
        correct = rng.random() < 0.4
        rows.append(_delivery(f"r{i:02d}", "A", event_type, i, i * 1000, correct=correct))
        body = json.loads(rows[-1]["value"])
        body["skill_ids"] = list(skills[: 1 + (i % 2)])
        body["payload"] = {"is_correct": correct} if event_type == "submit_step" else {}
        rows[-1]["value"] = json.dumps(body)
    _write_events(tmp_path / "events.parquet", rows)
    window = load_window_events(tmp_path / "events.parquet")
    arrivals = list(window.values())
    rng.shuffle(arrivals)
    events = [Event(e.event_id, e.event_type, R + e.event_offset_ms, R + e.sent_offset_ms, e.session_seq, CLASS,
                    "P-LIN-001", e.skill_ids, is_correct=e.payload.get("is_correct"),
                    step_index=1 if e.event_type == "submit_step" else None) for e in arrivals]
    for model in ("rule", "bkt"):
        store = MemoryLearnerStore(LEARNERS["A"][0])
        for start in range(0, len(events), 7):  # nhiều micro-batch, đến lộn xộn
            store.run(events[start:start + 7], "ordered", model, watermark_ms=None)
        store.run([], "ordered", model, watermark_ms=R + 10_000_000)
        oracle = compute_oracle(window.values(), model=model)
        assert set(oracle.states) == {(LEARNERS["A"][0], s) for s in store.skills}
        for (learner, skill), expected in oracle.states.items():
            actual = store.skills[skill]
            system = {"opportunities": actual.profile.opportunities, "correct": actual.profile.correct,
                      "consec_wrong": actual.profile.consec_wrong, "streak": actual.profile.streak,
                      "mastered": actual.profile.mastered, "stuck": actual.stuck, "p_mastery": actual.p_mastery}
            assert compare_state(expected, system) == [], (model, skill)


def test_oracle_bkt_uses_processor_params_and_requires_them():
    learner = LEARNERS["A"][0]
    events = [OracleEvent(f"e{i}", learner, CLASS, "submit_step", "wifi", i, i * 10, i * 10 + 1, i * 10 + 1,
                         (SKILL,), {"is_correct": i % 2 == 0}) for i in range(1, 5)]
    params = bkt_params_for(SKILL)
    expected = params.p_init
    for i in range(1, 5):
        expected = params.update(expected, i % 2 == 0)
    assert compute_oracle(events, model="bkt").states[(learner, SKILL)].p_mastery == pytest.approx(expected)
    custom = BktParams(0.5, 0.1, 0.2, 0.1)
    assert compute_oracle(events, model="bkt", params_for=lambda _s: custom).states[
        (learner, SKILL)].p_mastery != pytest.approx(expected)
    assert compute_oracle(events, model="rule").states[(learner, SKILL)].p_mastery is None
    with pytest.raises(ValueError):
        compute_oracle(events, model="dkt")


def test_window_rejects_same_event_id_with_different_bytes(tmp_path):
    row = _delivery("a1", "A", "open_problem", 1, 0)
    _write_events(tmp_path / "events.parquet", [row, {**row, "value": row["value"].replace("open", "opn")}])
    with pytest.raises(OracleError, match="nội dung khác"):
        load_window_events(tmp_path / "events.parquet")


def test_cli_reports_missing_inputs_with_exit_code_1(tmp_path, capsys):
    from ai_math_tutor.streaming.reconcile import main

    code = main(["--run-id", "tiny", "--sim-dir", str(tmp_path), "--model", "rule",
                 "--replay-json", str(tmp_path / "missing.json")])
    assert code == 1 and "reconcile:" in capsys.readouterr().err
    assert main(["--run-id", "../x", "--sim-dir", str(tmp_path), "--model", "rule"]) == 1
