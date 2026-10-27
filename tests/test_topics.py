"""Quản lý topic không cần broker: kế hoạch idempotent, tạo lại khi đổi partition, cấu hình theo runtime.yaml."""

import pytest

from ai_math_tutor.config import load_config
from ai_math_tutor.ingestion.topics import (
    AUX_TOPIC_KEYS,
    DEFAULT_AUX_PARTITIONS,
    EXPERIMENT_PARTITIONS,
    TopicAction,
    TopicError,
    TopicSpec,
    plan_actions,
    topic_specs,
)

SPECS = [TopicSpec("learner-events", 3), TopicSpec("alerts", 3)]


def _kinds(actions: list[TopicAction]) -> list[tuple[str, str, int]]:
    return [(a.kind, a.spec.name, a.spec.partitions) for a in actions]


def test_missing_topics_are_created_and_existing_matching_are_kept():
    assert _kinds(plan_actions({}, SPECS)) == [("create", "learner-events", 3), ("create", "alerts", 3)]
    assert _kinds(plan_actions({"learner-events": 3, "alerts": 3, "other": 9}, SPECS)) == [
        ("keep", "learner-events", 3), ("keep", "alerts", 3)]


def test_partition_mismatch_requires_explicit_recreate():
    existing = {"learner-events": 1, "alerts": 3}
    with pytest.raises(TopicError, match="learner-events đang có 1 partition"):
        plan_actions(existing, SPECS)
    assert _kinds(plan_actions(existing, SPECS, recreate=["learner-events"])) == [
        ("delete", "learner-events", 3), ("create", "learner-events", 3), ("keep", "alerts", 3)]


def test_recreate_also_empties_topic_with_same_partition_count():
    assert _kinds(plan_actions({"learner-events": 3}, SPECS, recreate=["learner-events", "alerts"])) == [
        ("delete", "learner-events", 3), ("create", "learner-events", 3), ("create", "alerts", 3)]


def test_plan_rejects_unknown_recreate_and_duplicate_names():
    with pytest.raises(ValueError, match="không có trong cấu hình"):
        plan_actions({}, SPECS, recreate=["nope"])
    with pytest.raises(ValueError, match="lặp"):
        plan_actions({}, [TopicSpec("a", 1), TopicSpec("a", 3)])


@pytest.mark.parametrize("name", ["", ".", "..", "has space", "x" * 250, "ký-tự"])
def test_topic_spec_rejects_illegal_names(name):
    with pytest.raises(ValueError):
        TopicSpec(name, 1)


@pytest.mark.parametrize("partitions", [0, -1, True, 1.5, "3"])
def test_topic_spec_rejects_bad_partition_counts(partitions):
    with pytest.raises(ValueError):
        TopicSpec("learner-events", partitions)


def test_topic_specs_follow_runtime_config():
    cfg = load_config(env={"NHOM8_KAFKA_PARTITIONS": "6"}, dotenv_path=None)
    specs = topic_specs(cfg)
    assert [s.name for s in specs] == ["learner-events", "alerts", "dead-letter", "hint-requests"]
    assert specs[0].partitions == 6
    assert all(s.partitions == DEFAULT_AUX_PARTITIONS and s.replication_factor == 1 for s in specs[1:])
    assert [s.partitions for s in topic_specs(cfg, learner_partitions=1, aux_partitions=2)] == [1, 2, 2, 2]
    assert AUX_TOPIC_KEYS == ("alerts", "dead_letter", "hint_requests")
    assert EXPERIMENT_PARTITIONS == (1, 3, 6)
