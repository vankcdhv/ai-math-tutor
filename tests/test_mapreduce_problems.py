"""Job MapReduce theo (learner_id, problem_id): kiểm hợp đồng từng bản ghi, khử trùng, bộ đếm, top-K, đối chiếu.

Phần thuần Python chạy trong bộ test mặc định. Các test cần SparkSession mang marker `integration` và chỉ chạy khi
được chọn rõ bằng `-m integration` (bị bỏ qua khi chạy file này mà không có `-m`).
"""

from __future__ import annotations

import argparse
import copy
import itertools
import json
import random
import uuid
import zlib
from collections import defaultdict
from functools import reduce
from pathlib import Path
from typing import Any

import pytest
from jsonschema import Draft202012Validator, FormatChecker

from ai_math_tutor.batch import mapreduce_problems as mr
from ai_math_tutor.batch.mapreduce_run import (
    CONFIGS,
    data_checks,
    executor_deltas,
    mapreduce_spark_conf,
    summarize_rest,
    task_skew,
)
from ai_math_tutor.event_validation import load_schema
from ai_math_tutor.experiments import mapreduce_matrix as mm

REPO = Path(__file__).resolve().parents[1]
SAMPLE = REPO / "data_sample" / "events-week1-sample.jsonl"
VALIDATOR = Draft202012Validator(load_schema(), format_checker=FormatChecker())
_NAMESPACE = uuid.UUID("2f0a7d64-6f43-4a51-9d0c-0b6f1d4c8e11")


def _iso(seconds: float) -> str:
    whole = int(seconds)
    millis = round((seconds - whole) * 1000)
    minutes, sec = divmod(whole, 60)
    return f"2026-11-02T10:{minutes:02d}:{sec:02d}.{millis:03d}Z"


def make_event(event_type: str, learner: str, problem: str, t_s: float, *, delay_s: float = 0.2,
               payload: dict[str, Any] | None = None, seq: int = 1, name: str | None = None) -> dict[str, Any]:
    default_payload = {
        "open_problem": {},
        "submit_step": {"step_index": 1, "expression": "2*x = 4", "is_correct": True},
        "wrong_answer": {"answer": "3", "attempt": 1},
        "request_hint": {"hint_level": 1, "step_index": 1},
        "complete": {"answer": "2", "attempts": 1, "hints_used": 0},
    }[event_type]
    return {
        "schema_version": "0.1", "event_id": str(uuid.uuid5(_NAMESPACE, name or f"{learner}/{problem}/{t_s}")),
        "event_type": event_type, "learner_id": learner, "class_id": "C-10A1-2026", "session_id": "S-0000abcd",
        "session_seq": seq, "problem_id": problem, "skill_ids": ["linear_equation.distribute"],
        "event_time": _iso(t_s), "sent_time": _iso(t_s + delay_s),
        "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"},
        "payload": default_payload if payload is None else payload,
    }


L1, L2 = "L-00000000000000a1", "L-00000000000000b2"
P1, P2 = "P-LIN-001", "P-LIN-002"


def tiny_lines() -> list[str]:
    """Log nhỏ có trùng, muộn, heartbeat, bản ghi hỏng và dòng trống; bộ đếm kỳ vọng tính tay ở test bên dưới."""
    step_wrong = {"step_index": 1, "expression": "2*x = 5", "is_correct": False}
    events = [
        make_event("open_problem", L1, P1, 0, seq=1),
        make_event("submit_step", L1, P1, 1, seq=2),
        make_event("submit_step", L1, P1, 2, seq=3, payload=step_wrong),
        make_event("request_hint", L1, P1, 3, seq=4),
        make_event("wrong_answer", L1, P1, 4, seq=5),
        make_event("complete", L1, P1, 5, seq=6),
        make_event("open_problem", L1, P2, 10, seq=7, delay_s=45.0),  # đến muộn (> 30 s)
        make_event("submit_step", L1, P2, 11, seq=8, payload=step_wrong),
        make_event("wrong_answer", L2, P1, 20, seq=1),
        make_event("wrong_answer", L2, P1, 21, seq=2, payload={"answer": "4", "attempt": 2}),
    ]
    lines = [json.dumps(e) for e in events]
    lines.insert(8, lines[7])  # gửi trùng nguyên byte
    heartbeat = {"schema_version": "0.2", "event_id": "00000000-0000-4000-8000-000000000001", "event_type": "heartbeat",
                 "learner_id": "L-0000000000000000", "event_time": _iso(100), "sent_time": _iso(100), "payload": {}}
    bad_type = make_event("submit_step", L2, P2, 30, name="bad-type")
    bad_type["event_type"] = "Submit_Step"
    return [*lines, json.dumps(heartbeat), "{bad", json.dumps(bad_type), "   "]


def _us(seconds: float) -> int:
    return mr._time_us(_iso(seconds)) // 1000


# ---------------------------------------------------------------- bản tham chiếu trên log nhỏ

def test_reference_counts_follow_the_documented_definitions():
    result = mr.reference_counts(tiny_lines(), k=2)
    assert result.stats == {
        "deliveries": 14, "valid_deliveries": 11, "heartbeats": 1, "invalid": 2, "invalid_with_event_id": 1,
        "unique_events": 10, "duplicate_deliveries": 1, "conflicting_event_ids": 0, "late_events": 1,
        "output_keys": 3,
    }
    assert result.invalid_by_reason == {"event_type": 1, "malformed_json": 1}
    assert result.counters == {
        (L1, P1): (6, 4, 2, 1, True, _us(0), _us(5)),
        (L1, P2): (2, 1, 1, 0, False, _us(10), _us(11)),
        (L2, P1): (2, 2, 2, 0, False, _us(20), _us(21)),
    }
    assert result.top_k == [(P1, 4), (P2, 1)]


def test_week1_sample_matches_the_validator_report():
    result = mr.reference_counts(SAMPLE.read_text(encoding="utf-8").splitlines())
    stats = result.stats
    assert (stats["deliveries"], stats["unique_events"], stats["duplicate_deliveries"], stats["late_events"]) == (20, 19, 1, 1)
    assert stats["invalid"] == 0 and stats["heartbeats"] == 0


def test_same_event_id_with_different_content_counts_once_and_is_flagged():
    first = make_event("wrong_answer", L1, P1, 1, name="same")
    second = copy.deepcopy(first)
    second["payload"]["answer"] = "9"
    result = mr.reference_counts([json.dumps(first), json.dumps(second)])
    assert result.stats["unique_events"] == 1
    assert result.stats["duplicate_deliveries"] == 1
    assert result.stats["conflicting_event_ids"] == 1
    assert result.counters[(L1, P1)][:3] == (1, 1, 1)


# ---------------------------------------------------------------- kiểm hợp đồng: khớp JSON Schema

def _mutations() -> list[tuple[str, dict[str, Any]]]:
    base = make_event("submit_step", L1, P1, 1)
    heartbeat = {"schema_version": "0.2", "event_id": "00000000-0000-4000-8000-000000000002", "event_type": "heartbeat",
                 "learner_id": "L-0000000000000000", "event_time": _iso(5), "sent_time": _iso(5), "payload": {}}

    def edit(source: dict[str, Any], fn) -> dict[str, Any]:
        event = copy.deepcopy(source)
        fn(event)
        return event

    cases = [
        ("valid", base),
        ("valid heartbeat", heartbeat),
        ("schema 0.2 learning event", edit(base, lambda e: e.update(schema_version="0.2"))),
        ("schema 0.3", edit(base, lambda e: e.update(schema_version="0.3"))),
        ("float session_seq 2.0", edit(base, lambda e: e.update(session_seq=2.0))),
        ("bool session_seq", edit(base, lambda e: e.update(session_seq=True))),
        ("session_seq 0", edit(base, lambda e: e.update(session_seq=0))),
        ("extra payload field", edit(base, lambda e: e["payload"].update(note="x"))),
        ("is_correct not bool", edit(base, lambda e: e["payload"].update(is_correct=1))),
        ("expression empty", edit(base, lambda e: e["payload"].update(expression=""))),
        ("duplicate skills", edit(base, lambda e: e.update(skill_ids=["a_b.c_d", "a_b.c_d"]))),
        ("no skills", edit(base, lambda e: e.update(skill_ids=[]))),
        ("client extra", edit(base, lambda e: e["client"].update(os="x"))),
        ("client missing", edit(base, lambda e: e["client"].pop("connection"))),
        ("bad platform", edit(base, lambda e: e["client"].update(platform="desktop"))),
        ("bad month", edit(base, lambda e: e.update(event_time="2026-13-02T10:00:00.000Z"))),
        ("no fraction", edit(base, lambda e: e.update(event_time="2026-11-02T10:00:00Z"))),
        ("reserved learner", edit(base, lambda e: e.update(learner_id="L-0000000000000000"))),
        ("heartbeat with class", edit(heartbeat, lambda e: e.update(class_id="C-10A1-2026"))),
        ("heartbeat v0.1", edit(heartbeat, lambda e: e.update(schema_version="0.1"))),
        ("heartbeat payload", edit(heartbeat, lambda e: e.update(payload={"a": 1}))),
        ("hint level 4", edit(make_event("request_hint", L1, P1, 1), lambda e: e["payload"].update(hint_level=4))),
        ("open with payload", make_event("open_problem", L1, P1, 1, payload={"x": 1})),
        ("complete hints -1", edit(make_event("complete", L1, P1, 1), lambda e: e["payload"].update(hints_used=-1))),
        ("payload list", edit(base, lambda e: e.update(payload=[]))),
        ("event_id upper", edit(base, lambda e: e.update(event_id=e["event_id"].upper()))),
        ("missing payload", edit(base, lambda e: e.pop("payload"))),
        ("unknown top field", edit(base, lambda e: e.update(extra=1))),
        ("event_type list", edit(base, lambda e: e.update(event_type=["submit_step"]))),
    ]
    return cases


@pytest.mark.parametrize(("name", "event"), _mutations(), ids=[name for name, _ in _mutations()])
def test_record_check_agrees_with_the_json_schema(name, event):
    schema_valid = not list(VALIDATOR.iter_errors(event))
    reason, _, _ = mr.check_event(event)
    assert (reason is None) == schema_valid, (name, reason)


@pytest.mark.parametrize("kind", mm.INVALID_KINDS)
def test_injected_corruptions_get_their_intended_reason(kind):
    value = SAMPLE.read_text(encoding="utf-8").splitlines()[3]
    corrupted = mm.corrupt(value, kind)
    record = mr.classify_line(corrupted)
    assert record[0] == mr.INVALID and record[1] == kind
    assert record[2] == json.loads(value)["event_id"]  # event_id gốc vẫn tra được, kể cả khi JSON bị cắt
    if kind not in ("malformed_json", "sent_before_event"):
        assert list(VALIDATOR.iter_errors(json.loads(corrupted)))


def test_every_sample_line_is_valid_and_normalised():
    kinds = [mr.classify_line(line)[2][4] for line in SAMPLE.read_text(encoding="utf-8").splitlines()]
    assert set(kinds) <= {mr.KIND_OPEN, mr.KIND_STEP_OK, mr.KIND_STEP_WRONG, mr.KIND_WRONG_ANSWER, mr.KIND_HINT,
                          mr.KIND_COMPLETE}
    assert mr.KIND_STEP_WRONG in kinds and mr.KIND_COMPLETE in kinds


# ---------------------------------------------------------------- các phép reduce: combiner-safe

def test_merge_delivery_is_associative_and_commutative():
    a = (10, 5, L1, P1, mr.KIND_OPEN, 1, 0, 1, 0)
    b = (10, 7, L1, P1, mr.KIND_OPEN, 1, 0, 1, 0)  # cùng event_id, nội dung khác
    c = (12, 5, L1, P1, mr.KIND_OPEN, 1, 0, 1, 0)
    results = {reduce(mr.merge_delivery, order) for order in itertools.permutations([a, b, c])}
    results |= {mr.merge_delivery(x, mr.merge_delivery(y, z)) for x, y, z in itertools.permutations([a, b, c])}
    assert results == {(10, 5, L1, P1, mr.KIND_OPEN, 1, 0, 3, 1)}
    same = reduce(mr.merge_delivery, [a, a, a])
    assert same[7:] == (3, 0)


def test_add_counters_is_order_independent():
    parts = [(1, 1, 0, 0, 0, 30, 30), (1, 0, 0, 1, 0, 10, 10), (1, 1, 0, 0, 1, 20, 20)]
    results = {reduce(mr.add_counters, order) for order in itertools.permutations(parts)}
    assert results == {(3, 2, 0, 1, 1, 10, 30)}


def test_top_k_merge_breaks_ties_by_problem_id_and_matches_a_full_sort():
    rng = random.Random(7)
    totals = [(f"P-T-{i:03d}", rng.choice([0, 1, 2, 3, 5])) for i in range(60)]
    expected = sorted(totals, key=lambda kv: (-kv[1], kv[0]))[:10]
    for parts in (1, 3, 7):
        partitions = [totals[i::parts] for i in range(parts)]
        local = [next(mr.local_top_k(10, part)) for part in partitions]
        rng.shuffle(local)
        assert reduce(lambda x, y: mr.merge_top_k(10, x, y), local) == expected
    assert next(mr.local_top_k(2, [("P-B-001", 3), ("P-A-001", 3), ("P-C-001", 4)])) == [("P-C-001", 4), ("P-A-001", 3)]
    assert next(mr.local_top_k(3, [])) == []


def _partitioned_reduce(pairs, func, parts, partition_fn):
    """Mô phỏng reduceByKey: combiner trong từng partition đầu vào, chia theo partitioner, reduce phía nhận."""
    out = [dict() for _ in range(parts)]
    for chunk in (pairs[i::parts] for i in range(parts)):
        combined: dict[Any, Any] = {}
        for key, value in chunk:
            combined[key] = func(combined[key], value) if key in combined else value
        for key, value in combined.items():
            target = out[partition_fn(key) % parts]
            target[key] = func(target[key], value) if key in target else value
    return out


def simulate_job(lines: list[str], parts: int, k: int) -> mr.ProblemCounts:
    """Cùng chuỗi map/reduce của run_problem_job, không Spark: kiểm các hàm executor độc lập với số partition."""
    records = [mr.classify_line(line) for line in lines if not mr.is_blank(line)]
    stat_parts = _partitioned_reduce([mr.stat_key(r) for r in records], lambda a, b: a + b, parts, mr.string_partition)
    counts = {key: value for part in stat_parts for key, value in part.items()}
    unique_parts = _partitioned_reduce([mr._keyed_delivery(r) for r in records if mr._is_valid(r)], mr.merge_delivery,
                                       parts, mr.string_partition)
    unique = [item for part in unique_parts for item in part.items()]
    counter_parts = _partitioned_reduce([mr.to_counter_pair(item) for item in unique], mr.add_counters, parts,
                                        mr.learner_partition)
    counters = {key: value for part in counter_parts for key, value in part.items()}
    problem_parts = _partitioned_reduce([mr._problem_wrong(item) for item in counters.items()], lambda a, b: a + b,
                                        parts, mr.string_partition)
    top = reduce(lambda a, b: mr.merge_top_k(k, a, b), [next(mr.local_top_k(k, p.items())) for p in problem_parts])
    dedup = reduce(mr.add_tuples, map(mr.dedup_stat, unique), (0, 0, 0, 0))
    by_reason: dict[str, int] = defaultdict(int)
    with_id = 0
    for key, value in counts.items():
        if key.startswith("invalid:"):
            by_reason[key.split(":")[1]] += value
            with_id += value if key.endswith(":id") else 0
    stats = {"deliveries": sum(counts.values()), "valid_deliveries": counts.get("valid", 0),
             "heartbeats": counts.get("heartbeat", 0), "invalid": sum(by_reason.values()),
             "invalid_with_event_id": with_id, "unique_events": dedup[0], "duplicate_deliveries": dedup[1],
             "conflicting_event_ids": dedup[2], "late_events": dedup[3], "output_keys": len(counters)}
    rows = {key: tuple(json.loads(mr.counter_line((key, value)))[2:]) for key, value in counters.items()}
    return mr.ProblemCounts(stats=stats, invalid_by_reason=dict(by_reason), counters=rows, top_k=top, k=k)


@pytest.mark.parametrize("parts", [1, 3, 7])
def test_map_reduce_functions_match_the_reference_for_any_partitioning(parts):
    lines = tiny_lines() + SAMPLE.read_text(encoding="utf-8").splitlines()
    result = mr.compare_counts(simulate_job(lines, parts, k=3), mr.reference_counts(lines, k=3))
    assert result["match"], result["diffs"]


def test_partitioners_are_stable_across_processes():
    assert mr.string_partition("abc") == zlib.crc32(b"abc")
    assert mr.learner_partition((L1, P1)) == mr.learner_partition((L1, P2)) == zlib.crc32(L1.encode())


# ---------------------------------------------------------------- đối chiếu và đọc đầu ra

def test_compare_reports_each_kind_of_difference():
    expected = mr.reference_counts(tiny_lines(), k=2)
    assert mr.compare_counts(expected, expected)["match"]
    actual = mr.ProblemCounts.from_json(expected.to_json())
    actual.stats["late_events"] += 1
    actual.counters[(L2, P1)] = (2, 2, 1, 0, False, _us(20), _us(21))
    del actual.counters[(L1, P2)]
    actual.top_k = [(P2, 1), (P1, 4)]
    actual.invalid_by_reason["payload"] = 1
    result = mr.compare_counts(actual, expected)
    assert result["match"] is False
    assert result["checks"] == {"stats": False, "invalid_by_reason": False, "counters": False,
                                "counter_totals": False, "top_k": False}
    assert result["diffs"]["stats"] == {"late_events": {"actual": 2, "expected": 1}}
    assert result["diffs"]["counters"]["missing"] == 1 and result["diffs"]["counters"]["different"] == 1


def test_counts_round_trip_through_the_cache_file(tmp_path):
    counts = mr.reference_counts(tiny_lines(), k=2)
    counts.save(tmp_path / "ref.json.gz")
    loaded = mr.ProblemCounts.load(tmp_path / "ref.json.gz")
    assert mr.compare_counts(loaded, counts)["match"]
    assert loaded.invalid_samples == counts.invalid_samples


def test_read_job_outputs_parses_part_files(tmp_path):
    expected = mr.reference_counts(tiny_lines(), k=2)
    (tmp_path / "counters").mkdir()
    (tmp_path / "invalid").mkdir()
    items = sorted(expected.counters.items())
    rows = [json.dumps([key[0], key[1], *values]) for key, values in items]
    (tmp_path / "counters" / "part-00000").write_text("\n".join(rows[:2]) + "\n", encoding="utf-8")
    (tmp_path / "counters" / "part-00001").write_text(rows[2] + "\n", encoding="utf-8")
    lines = [line for line in tiny_lines() if not mr.is_blank(line)]  # job bỏ dòng trống trước map
    invalid = [r for r in map(mr.classify_line, lines) if r[0] == mr.INVALID]
    (tmp_path / "invalid" / "part-00000").write_text("".join(mr.invalid_line(r) + "\n" for r in invalid), encoding="utf-8")
    stats = {k: v for k, v in expected.stats.items() if k != "output_keys"}
    (tmp_path / "job.json").write_text(json.dumps({"stats": stats, "invalid_by_reason": expected.invalid_by_reason,
                                                   "k": 2, "top_k": expected.top_k}), encoding="utf-8")
    assert mr.compare_counts(mr.read_job_outputs(tmp_path), expected)["match"]


# ---------------------------------------------------------------- chuẩn bị dữ liệu và kế hoạch ma trận

def test_scenarios_follow_the_design_numbering():
    ids = [s.scenario_id for s in mm.scenarios([100, 1000, 10000], ["A", "B"])]
    assert ids == ["s1-d1-l100-A", "s2-d1-l100-B", "s3-d2-l1000-A", "s4-d2-l1000-B", "s5-d3-l10000-A",
                   "s6-d3-l10000-B"]
    assert [s.scenario_id for s in mm.scenarios([10000], ["B"])] == ["s6-d3-l10000-B"]


def test_write_deliveries_injects_known_invalid_records_and_heartbeats(tmp_path):
    lines = SAMPLE.read_text(encoding="utf-8").splitlines() * 30
    values = [(line, i * 1000) for i, line in enumerate(lines)]
    out = tmp_path / "deliveries.jsonl"
    meta = mm.write_deliveries(values, out, t0_ms=1_793_577_600_000, invalid_rate=0.2, heartbeats=3, seed=11)
    again = mm.write_deliveries(values, tmp_path / "again.jsonl", t0_ms=1_793_577_600_000, invalid_rate=0.2,
                                heartbeats=3, seed=11)
    assert out.read_bytes() == (tmp_path / "again.jsonl").read_bytes() and meta == again
    injected = meta["injected"]
    assert injected["invalid_total"] > 0 and meta["lines"] == len(lines) + injected["invalid_total"] + 3
    result = mr.reference_counts_from_file(out)
    assert result.stats["invalid"] == injected["invalid_total"]
    assert result.invalid_by_reason == {k: v for k, v in injected["invalid_by_reason"].items() if v}
    assert result.stats["heartbeats"] == 3
    assert result.stats["unique_events"] == 19  # bản sao của mẫu đều là bản trùng


def test_prepared_dataset_reference_agrees_with_the_simulator_manifest(tmp_path):
    """Mô phỏng nhỏ → deliveries.jsonl (chèn bản ghi hỏng, heartbeat) → tham chiếu; số đếm khớp manifest và cache."""
    kwargs = dict(duration_s=300, rate_per_learner=0.2, seed=5, invalid_rate=0.05, heartbeats=2, k=5,
                  late_threshold_s=30.0)
    dataset = mm.prepare_dataset(tmp_path, 40, **kwargs)
    reference = mr.ProblemCounts.load(Path(dataset["reference"]))
    checks = data_checks(reference.stats, dataset)
    assert checks["ok"], checks
    assert dataset["injected"]["invalid_total"] > 0 and reference.stats["late_events"] >= 0
    assert reference.stats["unique_events"] == dataset["manifest_counts"]["unique_events"]
    again = mm.prepare_dataset(tmp_path, 40, **kwargs)
    assert again["cache"] == {"simulation": True, "input": True, "reference": True}
    assert again["input_checksum"] == dataset["input_checksum"]


def _args(*argv: str) -> argparse.Namespace:
    return mm.build_parser().parse_args(list(argv))


def test_dry_run_plan_sizes_and_estimates(tmp_path):
    plan = mm.build_plan(_args("--dry-run"), tmp_path)
    assert [d["learners"] for d in plan["datasets"]] == [100, 1000, 10000]
    largest = plan["datasets"][-1]
    assert largest["params"]["rate_events_per_s"] == 1000.0
    assert largest["input_lines_source"] == "estimate"
    assert 1_000_000 < largest["input_lines"] < 1_500_000
    assert len(plan["scenarios"]) == 6 and all(len(s["runs"]) == 3 for s in plan["scenarios"])
    assert 0 < plan["estimate"]["total_minutes"] < plan["estimate"]["pessimistic_minutes"]
    single = mm.build_plan(_args("--scales", "10000", "--configs", "B", "--repeats", "1"), tmp_path)
    assert [s["scenario"] for s in single["scenarios"]] == ["s6-d3-l10000-B"]


def test_cli_rejects_unknown_scales_and_configs():
    with pytest.raises(SystemExit):
        _args("--scales", "500")
    with pytest.raises(SystemExit):
        _args("--configs", "C")


def test_spark_conf_for_both_modes(tmp_path):
    conf = mapreduce_spark_conf(CONFIGS["B"], "local-cluster", partitions=8, ui_port=4555, data_root=tmp_path)
    assert conf["spark.master"] == "local-cluster[2,2,4096]"
    assert conf["spark.executor.memory"] == "4096m" and conf["spark.executor.cores"] == "2"
    assert conf["spark.ui.enabled"] == "true" and conf["spark.ui.port"] == "4555"
    assert conf["spark.driver.bindAddress"] == "127.0.0.1"
    local = mapreduce_spark_conf(CONFIGS["A"], "local", partitions=8, ui_port=4556, data_root=tmp_path)
    assert local["spark.master"] == "local[2]" and local["spark.driver.memory"] == "4096m"
    assert "spark.executor.memory" not in local


# ---------------------------------------------------------------- số đo REST và tổng hợp

def _task(index: int, records: int, run_ms: int, executor: str = "0", attempt: int = 0, status: str = "SUCCESS"):
    return {"index": index, "attempt": attempt, "status": status, "executorId": executor,
            "taskMetrics": {"executorRunTime": run_ms, "inputMetrics": {"recordsRead": 0},
                            "shuffleReadMetrics": {"recordsRead": records}}}


def test_task_skew_reports_max_median_and_ratio():
    skew = task_skew([_task(0, 10, 100), _task(1, 30, 300, "1"), _task(2, 20, 200), _task(3, 0, 5, status="FAILED")])
    assert skew["tasks"] == 3
    assert skew["records"] == {"max": 30.0, "median": 20.0, "max_over_median": 1.5}
    assert skew["run_time_ms"]["max_over_median"] == 1.5
    assert skew["slowest_task"]["executor"] == "1" and skew["tasks_by_executor"] == {"0": 2, "1": 1}
    assert task_skew([])["records"]["max"] is None


def test_summarize_rest_labels_stages_by_action_and_counts_retries():
    raw = {
        "jobs": [{"jobId": 3, "description": "counters_output", "stageIds": [4, 5, 6], "status": "SUCCEEDED"},
                 {"jobId": 4, "description": "dedup_stats", "stageIds": [5, 7], "status": "SUCCEEDED"}],
        "stages": [
            {"stageId": 4, "attemptId": 0, "status": "COMPLETE", "numTasks": 2, "shuffleWriteBytes": 100,
             "submissionTime": "2026-10-28T10:00:00.000GMT", "completionTime": "2026-10-28T10:00:01.500GMT"},
            {"stageId": 5, "attemptId": 0, "status": "COMPLETE", "numTasks": 2, "shuffleReadRecords": 40,
             "shuffleReadBytes": 100, "shuffleWriteBytes": 50, "numFailedTasks": 1},
            {"stageId": 6, "attemptId": 0, "status": "COMPLETE", "numTasks": 2, "shuffleReadRecords": 30,
             "shuffleReadBytes": 50, "diskBytesSpilled": 7},
            {"stageId": 7, "attemptId": 0, "status": "SKIPPED"},
        ],
        "tasks": {"5.0": [_task(0, 20, 10), _task(1, 20, 12, attempt=1)], "6.0": [_task(0, 10, 10), _task(1, 20, 30)]},
    }
    summary = summarize_rest(raw)
    assert [s["label"] for s in summary["stages"]] == ["counters_output"] * 3
    assert summary["stages"][0]["duration_ms"] == 1500
    totals = summary["totals"]
    assert totals["shuffle_write_bytes"] == 150 and totals["shuffle_read_bytes"] == 150
    assert totals["failed_tasks"] == 1 and totals["task_attempts_retried"] == 1 and totals["disk_spilled_bytes"] == 7
    assert totals["stages_skipped"] == 1 and totals["jobs"] == 2
    assert summary["skew_by_learner_reduce"]["stage_id"] == 6
    assert summary["skew_by_learner_reduce"]["run_time_ms"]["max_over_median"] == 1.5


def test_executor_deltas_subtract_the_session_totals():
    before = [{"id": "0", "totalTasks": 10, "failedTasks": 0}]
    after = [{"id": "0", "totalTasks": 25, "failedTasks": 1}, {"id": "1", "totalTasks": 4}]
    deltas = executor_deltas(before, after)
    assert deltas["0"]["totalTasks"] == 15 and deltas["0"]["failedTasks"] == 1 and deltas["1"]["totalTasks"] == 4


def test_data_checks_compare_with_the_simulator_manifest():
    dataset = {"lines": 14, "manifest_counts": {"unique_events": 10, "duplicate_deliveries": 1, "late_events": 1},
               "injected": {"invalid_total": 2, "heartbeats": 1}}
    stats = mr.reference_counts(tiny_lines()).stats
    assert data_checks(stats, dataset)["ok"]
    assert not data_checks({**stats, "late_events": 0}, dataset)["ok"]


def _metrics(scenario: str, repeat: int, wall: float, status: str = "success") -> dict[str, Any]:
    return {"run_key": f"{scenario}-r{repeat}", "scenario": scenario, "config": scenario[-1],
            "scale_learners": int(scenario.split("-")[2][1:]), "repeat": repeat, "run_id": f"mr-{scenario}-r{repeat}",
            "status": status, "job_wall_s": wall, "throughput_deliveries_per_s": 1000 / wall,
            "correctness": {"match": True}, "counts": {"deliveries": 1000, "invalid": 1},
            "spark": {"totals": {"shuffle_write_bytes": 2_000_000}, "stages": []}}


def test_summary_rows_have_mean_median_min_max_per_scenario(tmp_path):
    runs = [mm.run_row(_metrics("s1-d1-l100-A", r, w)) for r, w in ((1, 2.0), (2, 4.0), (3, 9.0))]
    runs.append(mm.run_row(_metrics("s2-d1-l100-B", 1, 1.0, status="failed")))
    rows = mm.aggregate_rows(runs)
    by_type = {row["row_type"]: row for row in rows}
    assert set(by_type) == {"mean", "median", "min", "max"}  # kịch bản chỉ có run lỗi không được tổng hợp
    assert by_type["mean"]["job_wall_s"] == 5.0 and by_type["median"]["job_wall_s"] == 4.0
    assert by_type["min"]["job_wall_s"] == 2.0 and by_type["max"]["job_wall_s"] == 9.0
    assert by_type["mean"]["n_runs"] == 3


def test_summarize_writes_csv_figures_and_error_analysis(tmp_path):
    metrics = tmp_path / "metrics"
    metrics.mkdir()
    for scenario in ("s1-d1-l100-A", "s3-d2-l1000-A", "s2-d1-l100-B"):
        for repeat, wall in ((1, 2.0), (2, 3.0)):
            (metrics / f"{scenario}-r{repeat}.json").write_text(json.dumps(_metrics(scenario, repeat, wall)),
                                                                encoding="utf-8")
    out = mm.summarize(tmp_path)
    assert out["runs"] == 6
    assert {"completion-time.png", "throughput.png", "shuffle.png", "skew.png"} <= set(out["figures"])
    assert (tmp_path / "metrics" / "summary.csv").read_text(encoding="utf-8").count("\n") == 1 + 6 + 3 * 4
    assert (tmp_path / "error_analysis" / "data_quality.csv").is_file()
    assert json.loads((tmp_path / "error_analysis" / "overview.json").read_text(encoding="utf-8"))["success"] == 6


# ---------------------------------------------------------------- Spark (chỉ khi chọn -m integration)

@pytest.fixture(scope="module")
def spark(request, tmp_path_factory):
    if "integration" not in (request.config.getoption("-m") or ""):
        pytest.skip("test Spark: chạy bằng -m integration")
    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(master="local[2]", shuffle_partitions=3, data_root=tmp_path_factory.mktemp("spark"))
    with spark_session(conf) as session:
        yield session


@pytest.fixture()
def tiny_file(tmp_path):
    path = tmp_path / "deliveries.jsonl"
    lines = tiny_lines() + SAMPLE.read_text(encoding="utf-8").splitlines()
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


@pytest.mark.integration
@pytest.mark.parametrize("parts", [1, 3, 7])
def test_spark_job_matches_the_reference(spark, tiny_file, tmp_path, parts):
    summary = mr.run_problem_job(spark.sparkContext, tiny_file, tmp_path / f"out-{parts}", input_partitions=parts,
                                 partitions=parts, k=3)
    assert set(summary["actions"]) == {"stats", "invalid_output", "counters_output", "dedup_stats", "top_k"}
    result = mr.compare_counts(mr.read_job_outputs(tmp_path / f"out-{parts}"), mr.reference_counts_from_file(tiny_file, 3))
    assert result["match"], result["diffs"]


@pytest.mark.integration
def test_local_cluster_session_runs_measures_and_matches(request, tmp_path, tiny_file):
    """Một session local-cluster[1,2,1024] đầy đủ: warm-up + một run, metrics, REST, run record."""
    if "integration" not in (request.config.getoption("-m") or ""):
        pytest.skip("test Spark: chạy bằng -m integration")
    from pyspark import SparkContext

    from ai_math_tutor.batch import mapreduce_run

    # Session local[2] của các test phía trên còn sống (fixture theo module) thì getOrCreate trả lại nó thay vì
    # dựng local-cluster; test này phải đứng cuối file vì dừng session đó.
    if SparkContext._active_spark_context is not None:
        SparkContext._active_spark_context.stop()
    reference = tmp_path / "ref.json.gz"
    mr.reference_counts_from_file(tiny_file, 3).save(reference)
    dataset = {"learners": 3, "dataset_key": "tiny", "seed": 0, "params": {}, "input": str(tiny_file),
               "input_checksum": "sha256:test", "lines": None, "reference": str(reference), "manifest_counts": {},
               "injected": {}}
    results = tmp_path / "results"
    plan = {"config": "A", "resources": CONFIGS["A"].describe(), "spark_mode": "local-cluster", "driver_memory": "1g",
            "input_partitions": 2, "partitions": 3, "k": 3, "late_threshold_s": 30.0, "job_timeout_s": 300,
            "results_dir": str(results), "runs_jsonl": str(tmp_path / "runs.jsonl"), "data_root": str(tmp_path),
            "work_dir": str(tmp_path / "out"),
            "runs": [{"run_key": "warmup-A", "warmup": True, "scenario": "warmup-A", "repeat": 0, "dataset": dataset},
                     {"run_key": "s1-tiny-A-r1", "warmup": False, "scenario": "s1-tiny-A", "repeat": 1,
                      "dataset": dataset}]}
    original = mapreduce_run.CONFIGS["A"]
    mapreduce_run.CONFIGS["A"] = mapreduce_run.ResourceConfig("A", workers=1, cores=2, memory_mb=1024)
    try:
        assert mapreduce_run.run_session(plan) == 0
    finally:
        mapreduce_run.CONFIGS["A"] = original
    metrics = json.loads((results / "metrics" / "s1-tiny-A-r1.json").read_text(encoding="utf-8"))
    assert metrics["status"] == "success" and metrics["correctness"]["match"]
    assert metrics["spark"]["totals"]["shuffle_write_bytes"] > 0
    assert any(role.startswith("executor-") for role in metrics["resources"]["per_role"])
    record = json.loads((tmp_path / "runs.jsonl").read_text(encoding="utf-8").splitlines()[-1])
    assert record["experiment"] == "batch-mapreduce" and record["status"] == "success"
