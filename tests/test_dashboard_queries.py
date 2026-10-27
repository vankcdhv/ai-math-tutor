"""Dashboard: ngưỡng k cho mọi số liệu nhóm, chống suy ngược bằng hiệu, khóa vắng, chỉ đọc, 10K người học.

Dữ liệu Redis được ghi bằng chính mã của pipeline (`sinks.state_value`, `load_roster`, tính lại tổng hợp
lớp của `ForeachBatchSink`, `listener.metrics_fields`) vào fakeredis với tiền tố riêng, rồi đọc lại bằng
`dashboard.queries` — không có số liệu dựng tay trong giao diện.
"""

from __future__ import annotations

import dataclasses
import json
import random
import sys
import time

import fakeredis
import pytest

from ai_math_tutor.config import load_config
from ai_math_tutor.dashboard import queries
from ai_math_tutor.dashboard.queries import (
    HIDDEN,
    K_MIN,
    PIPELINE_FIELDS,
    PipelineMetrics,
    protect_count,
    protect_rate,
    protect_total,
    round_to_k,
)
from ai_math_tutor.streaming.listener import metrics_fields, progress_record
from ai_math_tutor.streaming.processor import alert_id
from ai_math_tutor.streaming.sinks import (
    TREND_BUCKET_MS,
    BatchPlan,
    ForeachBatchSink,
    LatencyWindow,
    RedisKeys,
    alert_fields,
    load_roster,
    state_value,
)

T0 = 1_793_577_600_000  # 2026-11-02T00:00:00Z
PREFIX = "unit-dash:"
SKILLS = ("linear_equation.isolate_variable", "arithmetic.fractions")


# ---------------------------------------------------------------- dựng dữ liệu bằng mã pipeline


def _state_row(class_id: str, *, mastered: bool = False, stuck: bool = False, onset: str | None = None,
               last_ms: int = T0, opportunities: int = 6, consec_wrong: int = 0, version: int = 1) -> dict:
    return {
        "class_id": class_id, "p_mastery": 0.9 if mastered else 0.3, "opportunities": opportunities,
        "correct": opportunities // 2, "consec_wrong": consec_wrong, "streak": 0, "mastered": mastered,
        "stuck": stuck, "stuck_onset_event_id": onset if stuck else None, "state_version": version,
        "last_event_time_ms": last_ms,
    }


class Pipeline:
    """Ghi trạng thái người học như foreachBatch rồi tính lại tổng hợp lớp bằng ForeachBatchSink."""

    def __init__(self, client, tmp_path, prefix: str = PREFIX) -> None:
        self.client = client
        self.keys = RedisKeys(prefix)
        cfg = load_config(env={"REDIS_PASSWORD": "unit-test", "NHOM8_DATA_ROOT": str(tmp_path)}, dotenv_path=None)
        self.sink = ForeachBatchSink(cfg, "unit-dash", mode="eager", model="bkt", key_prefix=prefix,
                                     redis_client=client, window=LatencyWindow())
        self.batch = 0

    def roster(self, roster: dict[str, list[str]]) -> None:
        load_roster(self.client, roster, self.keys)

    def write(self, states: dict[str, dict[str, dict]], event_ms: int = T0) -> None:
        """states: learner → skill → _state_row(...)."""
        self.batch += 1
        pipe = self.client.pipeline(transaction=False)
        classes: dict[str, int] = {}
        for learner, skills in states.items():
            pipe.hset(self.keys.state(learner), mapping={s: state_value(row, self.batch) for s, row in skills.items()})
            for row in skills.values():
                classes[row["class_id"]] = event_ms
        pipe.execute()
        self.sink._refresh_classes(BatchPlan(touched_classes=classes), self.batch)


def _learners(class_id: str, n: int) -> list[str]:
    return [f"L-{class_id[-4:]}{i:012x}" for i in range(n)]


def _class_states(class_id: str, learners: list[str], *, mastered: int, stuck: int, skill: str = SKILLS[0],
                  last_ms: int = T0) -> dict[str, dict[str, dict]]:
    """Người đầu `mastered` thành thạo, `stuck` người cuối bế tắc ở `skill`; ai cũng có dữ liệu."""
    out = {}
    for i, learner in enumerate(learners):
        is_stuck = i >= len(learners) - stuck
        out[learner] = {skill: _state_row(class_id, mastered=i < mastered, stuck=is_stuck,
                                          onset=f"e-{learner}" if is_stuck else None, last_ms=last_ms)}
    return out


def _render(snapshot: queries.ClassSnapshot) -> dict:
    """Mọi thứ chế độ lớp hiển thị cho một snapshot (dùng để so hai lần làm mới)."""
    return {
        "heatmap": [(r["class_id"], r["skill_id"], r["label"]) for r in queries.heatmap_rows(snapshot)],
        "detail": {c: queries.class_detail_rows(snapshot, c) for c in snapshot.classes},
        "learners": {c: queries.class_learners_label(snapshot, c) for c in snapshot.classes},
        "school": queries.school_rows(snapshot),
        "school_learners": queries.school_learners_label(snapshot),
    }


def _read(pipeline: Pipeline) -> queries.ClassSnapshot:
    classes = queries.discover_classes(pipeline.client, pipeline.keys)
    return queries.read_class_snapshot(pipeline.client, pipeline.keys, classes, SKILLS)


def _labels(rendered: dict) -> list[str]:
    out: list[str] = []
    out += [label for *_, label in rendered["heatmap"]]
    for rows in rendered["detail"].values():
        out += [str(v) for row in rows for k, v in row.items() if k != "skill_id"]
    out += list(rendered["learners"].values())
    out += [str(v) for row in rendered["school"] for k, v in row.items() if k != "skill_id"]
    out.append(rendered["school_learners"])
    return out


# ---------------------------------------------------------------- quy tắc ngưỡng k (hàm thuần)


def test_counts_below_k_never_shown_and_others_rounded_to_multiples_of_k():
    for count in range(0, 400):
        shown = protect_count(count)
        if count < K_MIN:
            assert shown.value is None and shown.label == "<5" and shown.state == "below_k"
        else:
            assert shown.value is not None and shown.value >= K_MIN and shown.value % K_MIN == 0
            assert abs(shown.value - count) <= K_MIN // 2
            assert shown.label == f"≈{shown.value}"
    assert [protect_count(c).value for c in (5, 7, 8, 12, 13)] == [5, 5, 10, 10, 15]
    with pytest.raises(ValueError):
        protect_count(-1)
    with pytest.raises(ValueError):
        protect_count(3, k=1)


def test_one_learner_changing_moves_a_displayed_count_by_zero_or_at_least_k():
    for count in range(0, 400):
        before, after = protect_count(count), protect_count(count + 1)
        if before.value is not None and after.value is not None:
            assert after.value - before.value in (0, K_MIN)
        else:  # đổi qua ngưỡng: một bên là "<5", không có hiệu số nào được hiển thị
            assert before.value is None or after.value is None


def test_rate_needs_numerator_denominator_and_complement_at_least_k():
    assert protect_rate(4, 30).label == "<5 thành thạo"
    assert protect_rate(27, 30).label == "<5 chưa thành thạo"
    assert protect_rate(3, 4).label == "<5 có dữ liệu"
    assert protect_rate(0, 0).value is None
    shown = protect_rate(12, 30)
    assert shown.value == pytest.approx(10 / 30) and shown.label == "≈33%"
    for observed in range(K_MIN, 80):
        for mastered in range(observed):
            rate, nxt = protect_rate(mastered, observed), protect_rate(mastered + 1, observed)
            if rate.value is not None:
                assert rate.value == pytest.approx(round_to_k(mastered) / round_to_k(observed))
            if rate.value is not None and nxt.value is not None and round_to_k(mastered) == round_to_k(mastered + 1):
                assert rate.value == nxt.value  # tỉ lệ chỉ đổi khi một số đếm hiển thị đổi
    with pytest.raises(ValueError):
        protect_rate(5, 4)


def test_totals_are_thresholded_and_hidden_when_a_cell_of_the_line_is_below_k():
    assert protect_total([1, 2, 1]).label == "<5"  # tổng < k
    assert protect_total([6, 12, 30]).label == "≈50"  # mọi ô ≥ k: tổng làm tròn
    assert protect_total([3, 12, 30]).state == HIDDEN  # tổng − ô hiện = ô ẩn ⇒ không hiện tổng
    assert protect_total([4, 4]).state == HIDDEN
    assert protect_total([]).label == "<5"
    # Mọi giá trị 0..k−1 của ô bị ẩn cho cùng một tổng hiển thị: trừ không ra được ô đó.
    for others in ([12], [12, 30], [5, 5, 5], [0, 9]):
        assert len({protect_total([hidden, *others]).label for hidden in range(K_MIN)}) == 1


# ---------------------------------------------------------------- snapshot qua mã ghi thật của pipeline


@pytest.fixture
def pipeline(tmp_path):
    return Pipeline(fakeredis.FakeRedis(), tmp_path)


def test_class_views_apply_k_to_every_number_including_row_and_column_totals(pipeline):
    big, small = _learners("C-SIM-0001", 12), _learners("C-SIM-0002", 3)
    pipeline.roster({"C-SIM-0001": big, "C-SIM-0002": small})
    pipeline.write({**_class_states("C-SIM-0001", big, mastered=6, stuck=2),
                    **_class_states("C-SIM-0002", small, mastered=1, stuck=1)})
    snapshot = _read(pipeline)
    assert snapshot.classes == ("C-SIM-0001", "C-SIM-0002") and not snapshot.problems
    rendered = _render(snapshot)

    assert rendered["learners"] == {"C-SIM-0001": "≈10", "C-SIM-0002": "<5"}
    assert rendered["school_learners"] == queries.HIDDEN_LABEL  # một lớp < k ⇒ tổng sĩ số bị ẩn
    detail = {r["skill_id"]: r for r in rendered["detail"]["C-SIM-0001"]}[SKILLS[0]]
    assert (detail["observed"], detail["mastered"], detail["stuck"], detail["mastery_rate"]) == (
        "≈10", "≈5", "<5", "≈50%")
    small_detail = {r["skill_id"]: r for r in rendered["detail"]["C-SIM-0002"]}[SKILLS[0]]
    assert small_detail == {"skill_id": SKILLS[0], "observed": "<5", "mastered": "<5", "stuck": "<5",
                            "mastery_rate": "<5 có dữ liệu"}
    school = {r["skill_id"]: r for r in rendered["school"]}[SKILLS[0]]
    assert school["observed"] == school["mastered"] == school["mastery_rate"] == "ẩn"
    assert school["stuck"] == "<5"  # tổng bế tắc 2 + 1 < k
    # Kỹ năng không ai có dữ liệu (khóa agg vắng) = 0 ⇒ "<5", không phải một trạng thái riêng.
    untouched = {r["skill_id"]: r for r in rendered["school"]}[SKILLS[1]]
    assert untouched["observed"] == "<5"
    # Không có số đếm nào hiển thị dưới k ở bất kỳ đâu.
    for label in _labels(rendered):
        digits = "".join(ch for ch in label.split("%")[0] if ch.isdigit())
        if label.startswith("≈") and not label.endswith("%"):
            assert int(digits) >= K_MIN, label


def test_school_totals_shown_when_every_class_cell_reaches_k(pipeline):
    a, b = _learners("C-SIM-0001", 11), _learners("C-SIM-0002", 14)
    pipeline.roster({"C-SIM-0001": a, "C-SIM-0002": b})
    pipeline.write({**_class_states("C-SIM-0001", a, mastered=5, stuck=5),
                    **_class_states("C-SIM-0002", b, mastered=7, stuck=6)})
    rendered = _render(_read(pipeline))
    school = {r["skill_id"]: r for r in rendered["school"]}[SKILLS[0]]
    assert (school["observed"], school["mastered"], school["stuck"], school["mastery_rate"]) == (
        "≈25", "≈10", "≈10", "≈40%")
    assert rendered["school_learners"] == "≈25"


def test_learner_changing_inside_a_small_group_leaves_every_view_unchanged(pipeline):
    """Suy ngược bằng hiệu giữa hai lần làm mới: một người đổi trạng thái trong nhóm < k không đổi màn hình."""
    a, b = _learners("C-SIM-0001", 12), _learners("C-SIM-0002", 9)
    pipeline.roster({"C-SIM-0001": a, "C-SIM-0002": b})
    pipeline.write({**_class_states("C-SIM-0001", a, mastered=6, stuck=2),
                    **_class_states("C-SIM-0002", b, mastered=5, stuck=0)})
    before = _render(_read(pipeline))
    for stuck in (3, 4, 1, 0):  # bế tắc của lớp 1 đi qua mọi giá trị < k
        pipeline.write(_class_states("C-SIM-0001", a, mastered=6, stuck=stuck))
        assert _render(_read(pipeline)) == before


def test_learner_changing_a_count_at_or_above_k_changes_labels_by_multiples_of_k(pipeline):
    learners = _learners("C-SIM-0001", 30)
    pipeline.roster({"C-SIM-0001": learners})
    previous = None
    for mastered in range(5, 26):
        pipeline.write(_class_states("C-SIM-0001", learners, mastered=mastered, stuck=0))
        detail = {r["skill_id"]: r for r in queries.class_detail_rows(_read(pipeline), "C-SIM-0001")}[SKILLS[0]]
        value = int(detail["mastered"].lstrip("≈"))
        if previous is not None:
            assert value - previous in (0, K_MIN)
        previous = value


def test_trend_blocks_are_read_per_five_minute_bucket_and_thresholded(pipeline):
    learners = _learners("C-SIM-0001", 12)
    pipeline.roster({"C-SIM-0001": learners})
    pipeline.write(_class_states("C-SIM-0001", learners, mastered=2, stuck=1), event_ms=T0 + 1_000)
    later = T0 + 2 * TREND_BUCKET_MS + 5_000
    pipeline.write(_class_states("C-SIM-0001", learners, mastered=7, stuck=5, last_ms=later), event_ms=later)
    snapshot = _read(pipeline)
    assert snapshot.as_of_event_ms("C-SIM-0001") == later
    points, problems = queries.read_trend(pipeline.client, pipeline.keys, "C-SIM-0001",
                                          snapshot.as_of_event_ms("C-SIM-0001"), SKILLS)
    assert not problems
    rows = queries.trend_rows(points)
    assert [(r["bucket_ms"], r["mastery_label"], r["stuck"]) for r in rows] == [
        (T0, "<5 thành thạo", "<5"), (T0 + 2 * TREND_BUCKET_MS, "≈50%", "≈5")]
    assert rows[0]["mastery_rate"] is None
    assert len(queries.trend_bucket_range(later)) == queries.TREND_BUCKETS == 24


# ---------------------------------------------------------------- khóa vắng, giá trị hỏng, chỉ đọc


def test_absent_keys_render_as_empty_views_without_errors():
    client = fakeredis.FakeRedis()
    keys = RedisKeys("absent:")
    assert queries.discover_classes(client, keys) == []
    snapshot = queries.read_class_snapshot(client, keys, ["C-NONE"], SKILLS)
    assert snapshot.learners == {"C-NONE": 0} and snapshot.counts == {} and not snapshot.problems
    assert {r["label"] for r in queries.heatmap_rows(snapshot)} == {"<5 có dữ liệu"}
    assert snapshot.as_of_event_ms() is None and snapshot.batch_id() is None
    assert queries.read_trend(client, keys, "C-NONE", None, SKILLS) == ([], [])
    assert queries.read_trend(client, keys, "C-NONE", T0, SKILLS) == ([], [])
    assert queries.read_pipeline_metrics(client, keys) is None
    assert queries.read_stuck_learners(client, keys, "C-NONE", now_ms=T0) == ([], [])
    assert queries.read_recent_alerts(client, keys) == ([], [])


def test_malformed_values_are_reported_not_rendered():
    client = fakeredis.FakeRedis()
    keys = RedisKeys("bad:")
    client.sadd(keys.roster("C-1"), "L-a")
    client.hset(keys.agg("C-1", SKILLS[0]), mapping={"observed": "abc", "mastered": "1", "stuck": "0"})
    client.hset(keys.agg("C-1", SKILLS[1]), mapping={"observed": "3", "mastered": "9", "stuck": "0"})
    client.hset(keys.trend("C-1", T0), mapping={SKILLS[0]: "{not json", SKILLS[1]: json.dumps([1])})
    client.sadd(keys.stuck_set("C-1"), "no-separator")
    snapshot = queries.read_class_snapshot(client, keys, ["C-1"], SKILLS)
    assert snapshot.counts == {} and len(snapshot.problems) == 2
    points, problems = queries.read_trend(client, keys, "C-1", T0, SKILLS)
    assert points == [] and len(problems) == 2
    stuck, problems = queries.read_stuck_learners(client, keys, "C-1", now_ms=T0)
    assert stuck == [] and len(problems) == 1


def _dump(client) -> dict:
    out = {}
    for key in sorted(client.scan_iter("*")):
        kind = client.type(key).decode()
        value = {"hash": client.hgetall, "set": client.smembers, "string": client.get,
                 "stream": lambda k: client.xrange(k)}[kind](key)
        out[key] = (kind, value, client.ttl(key))
    return out


def test_queries_never_write_to_redis(pipeline):
    learners = _learners("C-SIM-0001", 8)
    pipeline.roster({"C-SIM-0001": learners})
    pipeline.write(_class_states("C-SIM-0001", learners, mastered=5, stuck=2))
    client, keys = pipeline.client, pipeline.keys
    client.hset(keys.metrics_pipeline, mapping=metrics_fields({"run_id": "r", "batch_id": 1}))
    client.set(queries.lasthint_key(keys, learners[-1]), json.dumps({"level": 2}))
    before = _dump(client)
    snapshot = queries.read_class_snapshot(client, keys, queries.discover_classes(client, keys), SKILLS)
    queries.read_trend(client, keys, "C-SIM-0001", snapshot.as_of_event_ms(), SKILLS)
    queries.read_pipeline_metrics(client, keys)
    queries.read_stuck_learners(client, keys, "C-SIM-0001", now_ms=T0)
    queries.read_recent_alerts(client, keys)
    assert _dump(client) == before


# ---------------------------------------------------------------- Pipeline: metrics:pipeline của listener


def test_pipeline_metrics_fields_match_listener_contract():
    assert tuple(f.name for f in dataclasses.fields(PipelineMetrics)) == PIPELINE_FIELDS


def test_agg_fields_read_by_dashboard_are_written_by_the_pipeline(pipeline):
    learners = _learners("C-SIM-0001", 5)
    pipeline.roster({"C-SIM-0001": learners})
    pipeline.write(_class_states("C-SIM-0001", learners, mastered=1, stuck=0))
    written = {k.decode() for k in pipeline.client.hkeys(pipeline.keys.agg("C-SIM-0001", SKILLS[0]))}
    assert set(queries.AGG_FIELDS) <= written


def test_pipeline_metrics_round_trip_from_listener_record():
    progress = {
        "id": "q", "runId": "r", "batchId": 41, "timestamp": "2026-11-02T00:00:05.000Z",
        "numInputRows": 3120, "inputRowsPerSecond": 1040.3466, "processedRowsPerSecond": 998.72,
        "batchDuration": 3124, "durationMs": {"addBatch": 2900, "triggerExecution": 3124},
        "eventTime": {"watermark": "2026-11-02T00:00:01.250Z"},
        "stateOperators": [{"operatorName": "transformWithStateInPandasExec", "numRowsTotal": 900,
                            "numRowsDroppedByWatermark": 2}],
        "sources": [{"endOffset": json.dumps({"learner-events": {"0": 100, "1": 120, "2": 90}})}],
    }
    record = progress_record("exp-a-1", progress, latest_offsets={"learner-events": {0: 130, 1: 120, 2: 95}},
                             batch={"foreach_ms": 812.5},
                             latency_1m={"count": 5000, "system_ms": {"p95": 412.0}, "staleness_ms": {"p95": 655.5}},
                             rss_bytes=3 * 2**30)
    client = fakeredis.FakeRedis()
    keys = RedisKeys("m:")
    client.hset(keys.metrics_pipeline, mapping=metrics_fields(record))
    metrics = queries.read_pipeline_metrics(client, keys)
    assert metrics == PipelineMetrics(
        run_id="exp-a-1", batch_id=41, updated_ms=1_793_577_605_000, input_rows_per_s=1040.3466,
        processed_rows_per_s=998.72, batch_duration_ms=3124, foreach_ms=812.5, watermark_ms=1_793_577_601_250,
        lag_total=35, lag_max_partition=30, p95_system_ms_1m=412.0, p95_staleness_ms_1m=655.5,
        latency_samples_1m=5000, dropped_by_watermark=2, rss_bytes=3 * 2**30)
    # Listener ghi "" cho số đo thiếu (chưa có watermark, chưa đo được lag) ⇒ None, không lỗi.
    client.hset(keys.metrics_pipeline, mapping=metrics_fields(progress_record("r", {"batchId": 0},
                                                                              latest_offsets=None)))
    empty = queries.read_pipeline_metrics(client, keys)
    assert empty is not None and empty.batch_id == 0 and empty.watermark_ms is None and empty.lag_total is None


# ---------------------------------------------------------------- chế độ giáo viên


def _publish_alert(client, keys, learner: str, skill: str, class_id: str, onset: str, event_ms: int) -> str:
    """Cùng hiệu ứng như redis_alert.lua: XADD alerts:stream rồi đánh dấu alert:{id}."""
    row = {"alert_id": alert_id(learner, skill, "stuck", onset), "alert_type": "stuck", "learner_id": learner,
           "class_id": class_id, "skill_id": skill, "stuck_onset_event_id": onset, "alert_event_time_ms": event_ms}
    fields = alert_fields(row, "unit-dash", 1)
    stream_id = client.xadd(keys.alerts_stream, fields).decode()
    client.set(keys.alert(fields["alert_id"]), f"produced:{stream_id}")
    return fields["alert_id"]


def _hint_record(level: int, text: str) -> dict:
    """Bản ghi lasthint đúng dạng dịch vụ gợi ý dựng sau mỗi lần trả lời."""
    return {"event_id": "e-hint-1", "problem_id": "P-LIN-001", "step_index": 1, "hint_level": level,
            "source": "template", "verified": True, "misconception": None, "state_source": "redis",
            "text_vi": text, "served_ms": T0}


def test_stuck_learners_with_onset_duration_and_last_hint(pipeline):
    learners = _learners("C-SIM-0001", 6)
    pipeline.roster({"C-SIM-0001": learners})
    pipeline.write(_class_states("C-SIM-0001", learners, mastered=0, stuck=3))
    client, keys = pipeline.client, pipeline.keys
    stuck_learners = learners[-3:]
    _publish_alert(client, keys, stuck_learners[0], SKILLS[0], "C-SIM-0001", f"e-{stuck_learners[0]}", T0 - 90_000)
    _publish_alert(client, keys, stuck_learners[1], SKILLS[0], "C-SIM-0001", f"e-{stuck_learners[1]}", T0 - 30_000)
    client.set(queries.lasthint_key(keys, stuck_learners[0]), json.dumps(_hint_record(2, "Thử chuyển 3 sang vế phải")))
    client.hset(queries.lasthint_key(keys, stuck_learners[1]), mapping={"hint_level": "1", "text_vi": "Xem lại dấu"})

    result, problems = queries.read_stuck_learners(client, keys, "C-SIM-0001", now_ms=T0)
    assert not problems
    assert [(s.learner_id, s.stuck_for_ms) for s in result] == [
        (stuck_learners[0], 90_000), (stuck_learners[1], 30_000), (stuck_learners[2], None)]
    first = result[0]
    assert first.skill_id == SKILLS[0] and first.onset_event_id == f"e-{stuck_learners[0]}"
    assert first.onset_event_ms == T0 - 90_000 and first.opportunities == 6
    assert first.last_hint == "hint_level=2; problem_id=P-LIN-001; step_index=1; verified=True; text_vi=Thử chuyển 3 sang vế phải"
    assert result[1].last_hint == "hint_level=1; text_vi=Xem lại dấu"
    assert result[2].last_hint is None and result[2].onset_event_ms is None  # chưa có cảnh báo/gợi ý


def test_recent_alerts_are_newest_first_unique_and_paged():
    client = fakeredis.FakeRedis()
    keys = RedisKeys("al:")
    ids = [_publish_alert(client, keys, f"L-{i:016x}", SKILLS[0], "C-1", f"e{i}", T0 + i) for i in range(30)]
    # Một mục lặp lại alert_id cũ (ví dụ ghi lại sau sự cố) không được hiện hai lần.
    client.xadd(keys.alerts_stream, {"alert_id": ids[29], "alert_type": "stuck", "learner_id": "L-x",
                                     "class_id": "C-1", "skill_id": SKILLS[0], "alert_event_time_ms": str(T0)})
    alerts, problems = queries.read_recent_alerts(client, keys, page=7)
    assert not problems
    assert len(alerts) == queries.RECENT_ALERTS
    assert [a.alert_id for a in alerts] == [ids[29], *reversed(ids[10:29])]
    assert alerts[0].learner_id == "L-x"  # mục mới nhất của alert_id đó được giữ
    assert alerts[1].alert_event_time_ms == T0 + 28 and alerts[1].visible_ms > 0


def test_last_hint_written_by_hint_service_is_read_when_that_service_is_installed():
    state_client = pytest.importorskip("ai_math_tutor.hints.state_client")
    client = fakeredis.FakeRedis()
    keys = RedisKeys("hint-contract:")
    learner = "L-00000000000000b1"
    assert queries.lasthint_key(keys, learner) == f"{keys.prefix}lasthint:{learner}"
    store = state_client.HintStateStore(client, state_client.HintKeys(keys))
    assert store.write_lasthint(learner, _hint_record(3, "Đặt x ra một vế"))
    problems: list[str] = []
    hints = queries.read_last_hints(client, keys, [learner], problems)
    assert not problems
    assert hints == {learner: "hint_level=3; problem_id=P-LIN-001; step_index=1; verified=True; text_vi=Đặt x ra một vế"}


def test_last_hint_key_does_not_need_the_hint_service_module(monkeypatch):
    monkeypatch.setitem(sys.modules, "ai_math_tutor.hints.state_client", None)  # import ⇒ ImportError
    assert queries.lasthint_key(RedisKeys("p:"), "L-1") == "p:lasthint:L-1"


def test_hint_summary_accepts_text_json_hash_and_truncates():
    assert queries.summarize_hint(None) is None
    assert queries.summarize_hint("Thử chia hai vế cho 2") == "Thử chia hai vế cho 2"
    assert queries.summarize_hint(json.dumps({"other": 1})) == '{"other": 1}'
    assert queries.summarize_hint({"hint_level": "3", "step": "2"}) == "hint_level=3"
    long = queries.summarize_hint("x" * 500)
    assert long is not None and len(long) == queries.HINT_SUMMARY_CHARS and long.endswith("…")


# ---------------------------------------------------------------- hiệu năng: 10K người học


def test_refresh_reads_only_aggregate_keys_for_10k_learners(tmp_path):
    client = fakeredis.FakeRedis()
    pipeline = Pipeline(client, tmp_path, prefix="perf:")
    skills = queries_skills()
    rng = random.Random(8)
    roster = {f"C-SIM-{c + 1:04d}": [f"L-{c * 35 + i:016x}" for i in range(35)] for c in range(286)}
    roster["C-SIM-0286"] = roster["C-SIM-0286"][:25]  # 10.000 người học như bộ mô phỏng (35 người/lớp)
    pipeline.roster(roster)
    states = {}
    for class_id, learners in roster.items():
        for learner in learners:
            states[learner] = {s: _state_row(class_id, mastered=rng.random() < 0.4, stuck=rng.random() < 0.05,
                                             onset="e", last_ms=T0) for s in skills}
    assert len(states) == 10_000
    pipeline.write(states)

    classes = queries.discover_classes(client, pipeline.keys)
    timings = []
    for _ in range(5):
        started = time.perf_counter()
        snapshot = queries.read_class_snapshot(client, pipeline.keys, classes, skills)
        queries.read_trend(client, pipeline.keys, classes[0], snapshot.as_of_event_ms(classes[0]), skills)
        timings.append((time.perf_counter() - started) * 1000.0)
        _render(snapshot)
    # Số lệnh theo lớp × kỹ năng, không theo người học (không đọc state:{learner}).
    assert snapshot.commands == len(classes) * (1 + len(skills)) == 286 * 13
    assert len(snapshot.counts) == 286 * len(skills) and not snapshot.problems
    print(f"\n10K learners, fakeredis: refresh read {sorted(timings)[len(timings) // 2]:.1f} ms (median of 5)")
    # fakeredis chạy trong Python nên chậm hơn Redis thật nhiều lần; giới hạn ở đây chỉ chặn hồi quy
    # O(số người học). Ngân sách 100 ms đo trên Redis thật trong tests/integration/test_dashboard_live.py.
    assert min(timings) < 2_000


def queries_skills() -> tuple[str, ...]:
    from ai_math_tutor.simulator.population import load_skills_config
    from ai_math_tutor.streaming.processor import SKILLS_CONFIG_PATH

    return load_skills_config(SKILLS_CONFIG_PATH).skill_ids


def test_snapshot_read_restores_gc_state(pipeline):
    import gc

    assert gc.isenabled()
    queries.read_class_snapshot(pipeline.client, pipeline.keys, ["C-NONE"], SKILLS)
    assert gc.isenabled()
    gc.disable()
    try:
        queries.read_class_snapshot(pipeline.client, pipeline.keys, ["C-NONE"], SKILLS)
        assert not gc.isenabled()
    finally:
        gc.enable()
