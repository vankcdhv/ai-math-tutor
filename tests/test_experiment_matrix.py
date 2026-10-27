"""Ma trận thí nghiệm v1.0: số cấu hình/run mỗi thí nghiệm (gồm ablation, stress), cấu hình dùng chung,
tên run/topic hợp lệ, resume (done/pending/abandoned) và ước lượng giờ máy."""

from __future__ import annotations

from pathlib import Path

import pytest
import yaml

from ai_math_tutor.config import REPO_ROOT, validate_run_id
from ai_math_tutor.experiments import matrix as M

PLAN = REPO_ROOT / "configs" / "experiments" / "plan-v1.0.yaml"
SMOKE = REPO_ROOT / "configs" / "experiments" / "smoke.yaml"
STAMP = "20261027T153000Z"
DEFAULT_SLUG = "l1000-r1000-p3-rp2-eager-w120-low-dup2-rule-tasap-600s-pbv2test"


@pytest.fixture(scope="module")
def plan() -> M.Plan:
    return M.load_plan(PLAN)


def _configs(plan: M.Plan, label: str, kind: str | None = None) -> set[str]:
    return {s.slug for s in plan.label_specs(label) if kind is None or s.kind == kind}


def test_main_experiments_have_expected_configs_and_three_seeds(plan):
    assert len(_configs(plan, "A_throughput")) == 9          # 3 tốc độ × 3 partition
    assert len(_configs(plan, "B_cardinality")) == 3
    assert len(_configs(plan, "C_freshness_cost", "stream")) == 4
    assert len(_configs(plan, "D_late_duplicate")) == 14      # 2 mode × 3 W × 2 hồ sơ + none × 2 hồ sơ
    assert len(_configs(plan, "E_hint_service")) == 4
    for label in ("A_throughput", "B_cardinality", "C_freshness_cost", "D_late_duplicate", "E_hint_service"):
        for slug in _configs(plan, label):
            seeds = {s.seed for s in plan.label_specs(label) if s.slug == slug}
            assert seeds == {11, 22, 33}, (label, slug)


def test_d_axis_is_mode_watermark_late_profile_with_none_mode(plan):
    specs = [s for s in plan.label_specs("D_late_duplicate") if s.seed == 11]
    cells = {(s.params["mode"], s.params["watermark_s"], s.params["late_profile"]) for s in specs}
    expected = {(m, w, lp) for m in ("eager", "ordered") for w in (30, 120, 600) for lp in ("low", "high")}
    expected |= {("none", None, "low"), ("none", None, "high")}
    assert cells == expected


def test_ablations_change_one_factor_and_count_in_matrix(plan):
    for name, key, values in (("dedup", "dedup", {True, False}), ("late_handling", "mode", {"eager", "ordered"}),
                              ("learner_model", "learner_model", {"rule", "bkt"}),
                              ("hint_generator", "hint_generator", {"template", "ladder"})):
        specs = plan.label_specs(f"ablation:{name}")
        assert {s.params[key] for s in specs} == values
        assert len(specs) == 2 * 3                              # 2 biến thể × 3 seed, cùng seed
        assert {s.seed for s in specs} == {11, 22, 33}


def test_identical_configs_are_one_spec_shared_by_every_experiment(plan):
    default = [s for s in plan.specs if s.slug == DEFAULT_SLUG]
    assert len(default) == 3
    assert set(default[0].groups) == {"A_throughput", "C_freshness_cost", "D_late_duplicate", "B_cardinality",
                                      "ablation:dedup", "ablation:late_handling", "ablation:learner_model"}
    keys = [s.key for s in plan.specs]
    assert len(keys) == len(set(keys))
    assert len(plan.specs) == 115


def test_batch_baselines_follow_the_asap_stream_config_of_c(plan):
    batch = [s for s in plan.specs if s.kind == "batch"]
    assert {(s.params["schedule"], s.seed) for s in batch} == {(sch, seed) for sch in ("daily", "hourly")
                                                                for seed in (11, 22, 33)}
    assert {s.source_slug for s in batch} == {DEFAULT_SLUG}
    ordered = M.select_specs(plan, ["C_freshness_cost"])
    kinds = [s.kind for s in ordered]
    assert kinds.index("batch") > max(i for i, k in enumerate(kinds) if k == "stream")


def test_stress_items_are_in_the_plan(plan):
    soak = plan.label_specs("soak")
    assert len(soak) == 1 and soak[0].params["duration_s"] == 1800 and soak[0].params["rate_events_per_s"] == 1000
    ceiling = plan.label_specs("ingest_ceiling")
    assert {(s.params["rate_events_per_s"], s.params["replay_processes"]) for s in ceiling} == {
        (r, p) for r in (30000, 60000) for p in (1, 4, 8)}
    assert all(s.kind == "ingest" for s in ceiling)


def test_run_ids_and_topic_names_are_valid_for_every_spec(plan):
    for spec in plan.specs:
        run_id = spec.run_id(STAMP)
        assert validate_run_id(run_id) == run_id
        assert run_id.startswith(spec.run_prefix)
        for topic in (f"exp-{run_id}", f"exp-{run_id}-dl", f"exp-{run_id}-al"):
            assert len(topic) <= 249 and all(c.isalnum() or c in "._-" for c in topic)
    prefixes = [s.run_prefix for s in plan.specs]
    assert not any(a != b and b.startswith(a) for a in prefixes for b in prefixes)


def test_hint_tag_makes_the_load_script_produce_the_spec_run_id(plan):
    spec = next(s for s in plan.specs if s.kind == "hint")
    run_id = spec.run_id(STAMP)
    assert f"hint-api-u{spec.params['concurrent_users']}-{spec.hint_tag(run_id)}" == run_id


def test_interval_parsing_matches_the_streaming_job():
    job = pytest.importorskip("ai_math_tutor.streaming.job")
    for text in ("30 seconds", "2 minutes", "10 minutes", "1 second", "60 seconds", "30s", "2m", 45):
        assert M.parse_interval_s(text) * 1000 == job.parse_interval_ms(str(text))
    with pytest.raises(M.PlanError):
        M.parse_interval_s("soon")


def test_local_choice_lists_match_the_components():
    from ai_math_tutor.simulator.network import LATE_PROFILES

    assert set(M.LATE_PROFILES) == set(LATE_PROFILES)
    generator = pytest.importorskip("ai_math_tutor.hints.generator")
    assert set(M.HINT_GENERATORS) == set(generator.GENERATORS)


def _raw(**block) -> dict:
    base = yaml.safe_load(SMOKE.read_text(encoding="utf-8"))
    base["experiments"] = {"x": {"kind": "stream", **block}}
    base["order"] = ["x"]
    return base


def test_yaml_on_off_and_none_mode_normalize():
    plan = M.build_plan(_raw(vary={"dedup": ["on", "off"], "mode": ["none"]}), Path("t.yaml"))
    slugs = {s.slug for s in plan.specs}
    assert any("nodedup" in s for s in slugs)
    none = next(s for s in plan.specs if s.params["mode"] == "none")
    assert none.params["watermark_s"] is None and "-w" not in none.slug


@pytest.mark.parametrize("block, message", [
    ({"vary": {"partitions": [1]}}, "không thuộc loại"),
    ({"warmup_s": 60}, "warmup_s"),
    ({"repeats": 2}, "repeats"),
    ({"vary": {"mode": ["fast"]}}, "mode"),
    ({"bogus": 1}, "khóa không hiểu"),
])
def test_invalid_blocks_are_rejected(block, message):
    with pytest.raises(M.PlanError, match=message):
        M.build_plan(_raw(**block), Path("t.yaml"))


def test_repeats_override_applies_only_to_blocks_without_their_own(plan):
    two = M.load_plan(PLAN, repeats_override=2)
    assert {s.seed for s in two.label_specs("A_throughput")} == {11, 22}
    assert len(two.label_specs("soak")) == 1
    with pytest.raises(M.PlanError):
        M.load_plan(PLAN, repeats_override=4)


def test_label_resolution(plan):
    assert M.resolve_labels(plan, ["A"]) == ["A_throughput"]
    assert M.resolve_labels(plan, ["ablations"]) == [l for l in plan.labels if l.startswith("ablation:")]
    assert M.resolve_labels(plan, ["ceiling"]) == ["ingest_ceiling"]
    assert M.resolve_labels(plan, ["stress"]) == ["soak", "ingest_ceiling"]
    assert M.resolve_labels(plan, ["all"]) == plan.labels
    with pytest.raises(M.PlanError):
        M.resolve_labels(plan, ["Z"])
    only = M.select_specs(plan, ["A_throughput"], only="r10000")
    assert only and all("r10000" in s.slug for s in only)


def _record(run_id: str, status: str, **metrics) -> dict:
    return {"run_id": run_id, "status": status, "metrics": metrics}


def test_resume_states(plan):
    spec = plan.label_specs("B_cardinality")[0]
    stamp = iter(f"2026102{i}T000000Z" for i in range(9))
    assert M.spec_state(spec, [], 3).status == "pending"
    failed = [_record(spec.run_id(next(stamp)), "failed", error="boom"),
              _record(spec.run_id(next(stamp)), "timeout")]
    state = M.spec_state(spec, failed, 3)
    assert (state.status, state.attempts) == ("pending", 2)
    interrupted = _record(spec.run_id(next(stamp)), "aborted", interrupted=True)
    assert M.spec_state(spec, [*failed, interrupted], 3).attempts == 2          # runner bị dừng: không tính
    real_abort = _record(spec.run_id(next(stamp)), "aborted")                   # vd. state_source=default
    assert M.spec_state(spec, [*failed, real_abort], 3).status == "abandoned"
    done = M.spec_state(spec, [*failed, _record(spec.run_id(next(stamp)), "success")], 3)
    assert done.status == "done" and done.success_run_id
    other = next(s for s in plan.specs if s.key != spec.key)
    assert M.spec_state(other, [*failed], 3).attempts == 0


def test_estimate_covers_every_experiment_and_dataset(plan):
    estimate = M.estimate_plan(plan.specs)
    assert set(estimate["by_experiment"]) == {s.experiment for s in plan.specs}
    assert estimate["total_runs"] == 115
    assert estimate["datasets"] == 21
    # Mỗi run streaming tối thiểu bằng thời lượng đo: 600 s × 3 seed × 9 cấu hình A = 4,5 giờ.
    assert estimate["by_experiment"]["A_throughput"]["hours"] >= 4.5
    assert estimate["total_hours"] > sum(v["hours"] for v in estimate["by_experiment"].values())


def test_smoke_plan_is_one_tiny_stream_run():
    plan = M.load_plan(SMOKE)
    assert len(plan.specs) == 1
    spec = plan.specs[0]
    assert (spec.kind, spec.params["learners"], spec.params["duration_s"], spec.params["rate_events_per_s"]) == (
        "stream", 60, 60, 100)


def test_experiments_simulate_the_1000_problem_test_split_and_the_bank_is_part_of_the_identity():
    assert M.PROBLEM_DEFAULTS == {"problem_bank": "data_sample/problem-bank-v2.jsonl", "problem_split": "test"}
    plan = M.load_plan(REPO_ROOT / "configs" / "experiments" / "plan-v1.0.yaml")
    streams = [s for s in plan.specs if s.kind == "stream"]
    assert streams and all(s.params["problem_bank"] == "data_sample/problem-bank-v2.jsonl" for s in streams)
    assert all(s.params["problem_split"] == "test" for s in streams)
    assert all(s.slug.endswith("-pbv2test") for s in streams)

    base = {"learners": 10, "rate_events_per_s": 5, "late_profile": "low", "duplicate_rate": 0.0, "duration_s": 60}
    mini = {**base, "problem_bank": "data_sample/problem-bank-mini.jsonl", "problem_split": "dev"}
    assert M.problems_tag(base) == "pbv2test" and M.problems_tag(mini) == "pbminidev"
    assert M.dataset_key(base, 1) != M.dataset_key(mini, 1)  # kho khác ⇒ cache dữ liệu khác
    with pytest.raises(M.PlanError):
        M._problems({"problem_split": "train"})