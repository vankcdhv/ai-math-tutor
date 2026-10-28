"""Schema Spark của streaming khớp hợp đồng JSON Schema v0.2; parse + kiểm hợp đồng phân loại đúng bản ghi lỗi."""

from __future__ import annotations

import copy
import datetime as dt
import json
import re
import uuid

import pytest
from pyspark.sql.types import ArrayType, BooleanType, LongType, StringType, StructType

from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, load_schema
from ai_math_tutor.streaming import job
from ai_math_tutor.streaming import schema as S

SAMPLE = REPO_ROOT / "data_sample" / "events-week1-sample.jsonl"
CONTRACT = load_schema()
JSON_TO_SPARK = {"string": StringType(), "integer": LongType(), "boolean": BooleanType()}
# Hợp đồng đầu ra processor ↔ sinks: một dòng phẳng mỗi bản ghi (`kind` = `update` | `alert` | `progress`),
# đúng các cột và đúng thứ tự dưới đây; đổi cột là đổi hợp đồng, phải sửa cả hai phía.
CONTRACT_OUTPUT_COLUMNS = (
    "kind", "learner_id", "class_id", "skill_id", "p_mastery", "opportunities", "correct", "consec_wrong", "streak",
    "mastered", "stuck", "stuck_onset_event_id", "state_version", "last_event_time_ms", "applied_event_ids",
    "applied_event_time_ms", "applied_sent_time_ms", "applied_out_of_order", "alert_type", "alert_id",
    "alert_event_time_ms", "problem_id", "wrong_answers", "last_step", "completed",
)


def _spark_type(prop: dict):
    if "enum" in prop or "const" in prop:
        return StringType()
    kind = prop["type"]
    if kind == "array":
        return ArrayType(_spark_type(prop["items"]))
    return JSON_TO_SPARK[kind]


def test_event_schema_fields_and_types_match_json_schema():
    props = CONTRACT["properties"]
    assert set(S.EVENT_FIELD_NAMES) == set(props)
    fields = {f.name: f.dataType for f in S.EVENT_SCHEMA.fields}
    for name, prop in props.items():
        if name in ("client", "payload"):
            assert isinstance(fields[name], StructType)
            continue
        assert fields[name] == _spark_type(prop), name
    client = CONTRACT["properties"]["client"]["properties"]
    assert {f.name: f.dataType for f in S.CLIENT_SCHEMA.fields} == {k: _spark_type(v) for k, v in client.items()}


def test_payload_struct_is_union_of_payload_branches():
    expected: dict = {}
    for branch in CONTRACT["allOf"][1:]:
        payload = branch["then"]["properties"]["payload"]
        for name, prop in payload.get("properties", {}).items():
            spark_type = _spark_type(prop)
            assert expected.setdefault(name, spark_type) == spark_type, name
    assert {f.name: f.dataType for f in S.PAYLOAD_SCHEMA.fields} == expected


def test_patterns_enums_and_required_fields_match_contract():
    props = CONTRACT["properties"]
    assert S.EVENT_ID_PATTERN == props["event_id"]["pattern"]
    assert S.LEARNER_ID_PATTERN == props["learner_id"]["pattern"]
    assert S.CLASS_ID_PATTERN == props["class_id"]["pattern"]
    assert S.SESSION_ID_PATTERN == props["session_id"]["pattern"]
    assert S.PROBLEM_ID_PATTERN == props["problem_id"]["pattern"]
    assert S.SKILL_ID_PATTERN == props["skill_ids"]["items"]["pattern"]
    assert S.DATETIME_PATTERN == props["event_time"]["pattern"] == props["sent_time"]["pattern"]
    assert S.APP_VERSION_PATTERN == props["client"]["properties"]["app_version"]["pattern"]
    assert tuple(props["schema_version"]["enum"]) == S.SCHEMA_VERSIONS
    assert tuple(props["event_type"]["enum"]) == S.EVENT_TYPES
    assert tuple(props["client"]["properties"]["platform"]["enum"]) == S.PLATFORMS
    assert tuple(props["client"]["properties"]["connection"]["enum"]) == S.CONNECTIONS
    assert tuple(CONTRACT["required"]) == S.REQUIRED_FIELDS
    heartbeat, learning = CONTRACT["allOf"][0]["then"], CONTRACT["allOf"][0]["else"]
    assert tuple(heartbeat["propertyNames"]["enum"]) == S.HEARTBEAT_FIELDS
    assert heartbeat["properties"]["learner_id"]["const"] == S.HEARTBEAT_LEARNER_ID == HEARTBEAT_LEARNER_ID
    assert tuple(learning["required"]) == S.LEARNING_REQUIRED_FIELDS


def test_output_schema_matches_interface_contract():
    assert S.OUTPUT_COLUMNS == CONTRACT_OUTPUT_COLUMNS
    types = {f.name: f.dataType for f in S.OUTPUT_SCHEMA.fields}
    assert types["applied_event_ids"] == ArrayType(StringType())
    assert types["applied_event_time_ms"] == types["applied_sent_time_ms"] == ArrayType(LongType())
    assert types["applied_out_of_order"] == ArrayType(BooleanType())
    assert types["state_version"] == types["last_event_time_ms"] == types["alert_event_time_ms"] == LongType()


# ---------------------------------------------------------------- parse + kiểm hợp đồng trên Spark cục bộ

@pytest.fixture(scope="module")
def spark(tmp_path_factory):
    from ai_math_tutor.batch.session import batch_spark_conf, spark_session

    conf = batch_spark_conf(master="local[2]", shuffle_partitions=2, data_root=tmp_path_factory.mktemp("spark"))
    with spark_session(conf) as session:
        yield session


def _sample() -> list[dict]:
    return [json.loads(line) for line in SAMPLE.read_text(encoding="utf-8").splitlines()]


def _heartbeat(**overrides) -> dict:
    event = {"schema_version": "0.2", "event_id": str(uuid.uuid4()), "event_type": "heartbeat",
             "learner_id": HEARTBEAT_LEARNER_ID, "event_time": "2026-10-27T08:05:00.000Z",
             "sent_time": "2026-10-27T08:05:00.000Z", "payload": {}}
    event.update(overrides)
    return event


def _with(event: dict, **changes) -> dict:
    out = copy.deepcopy(event)
    for key, value in changes.items():
        if value is _DROP:
            out.pop(key)
        else:
            out[key] = value
    return out


_DROP = object()


def _bad_cases() -> list[tuple[str, str]]:
    sample = _sample()
    open_problem, submit, wrong_answer, hint, complete = sample[0], sample[3], sample[11], sample[15], sample[5]
    cases: list[tuple[str, object]] = [
        ("malformed_json", "{not json"),
        ("malformed_json", "[1, 2]"),
        ("unknown_field", _with(open_problem, extra=1)),
        ("missing_field", _with(open_problem, sent_time=_DROP)),
        ("schema_version", _with(open_problem, schema_version="0.3")),
        ("event_type", _with(open_problem, event_type="logout")),
        ("event_id", _with(open_problem, event_id="not-a-uuid")),
        ("learner_id", _with(open_problem, learner_id="L-XYZ")),
        ("event_time", _with(open_problem, event_time="2026-10-27 08:00:00")),
        ("sent_time", _with(open_problem, sent_time="2026-13-45T08:00:00Z")),
        ("heartbeat_contract", _heartbeat(schema_version="0.1")),
        ("heartbeat_contract", _heartbeat(class_id="C-10A1-2026")),
        ("heartbeat_contract", _heartbeat(payload={"x": 1})),
        ("learning_fields", _with(open_problem, learner_id=HEARTBEAT_LEARNER_ID)),
        ("learning_fields", _with(open_problem, client=_DROP)),
        ("learning_fields", _with(open_problem, skill_ids=[])),
        ("learning_fields", _with(open_problem, skill_ids=["a.b", "a.b"])),
        ("learning_fields", _with(open_problem, session_seq=0)),
        ("learning_fields", _with(open_problem, problem_id="LIN-1")),
        ("payload", _with(submit, payload={"step_index": 1, "expression": "x = 1", "is_correct": "yes"})),
        ("payload", _with(submit, payload={"step_index": 1, "expression": "x = 1"})),
        ("payload", _with(wrong_answer, payload={"answer": "2", "attempt": 0})),
        ("payload", _with(hint, payload={"hint_level": 4, "step_index": 1})),
        ("payload", _with(complete, payload={"answer": "", "attempts": 1, "hints_used": 0})),
        ("payload", _with(open_problem, payload={"step_index": 1})),
        ("future_event_time", _with(open_problem, event_time="2099-01-01T00:00:00Z")),
    ]
    return [(reason, value if isinstance(value, str) else json.dumps(value)) for reason, value in cases]


# Timestamp Kafka mặc định của bản ghi test: sau mọi sự kiện mẫu (08:00–08:03:30) và heartbeat mẫu (08:05).
KAFKA_STAMP = dt.datetime(2026, 10, 27, 8, 10, tzinfo=dt.timezone.utc)


def _raw_frame(spark, values: list[str | None], stamps: list[dt.datetime] | None = None):
    stamps = stamps or [KAFKA_STAMP] * len(values)
    rows = [(None, None if v is None else v.encode("utf-8"), "learner-events", 0, i, stamp)
            for i, (v, stamp) in enumerate(zip(values, stamps, strict=True))]
    return spark.createDataFrame(
        rows, "key binary, value binary, topic string, partition int, offset long, timestamp timestamp")


def _iso(moment: dt.datetime) -> str:
    return moment.astimezone(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f")[:-3] + "Z"


def test_week1_sample_and_heartbeat_parse_as_valid(spark):
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    heartbeat = json.dumps(_heartbeat())
    rows = job.parse_events(_raw_frame(spark, [*lines, heartbeat])).collect()
    assert [r.invalid_reason for r in rows] == [None] * 21
    first = rows[0]
    assert first.event_time_ms == int(dt.datetime(2026, 10, 27, 8, tzinfo=dt.timezone.utc).timestamp() * 1000)
    assert first.sent_time_ms - first.event_time_ms == 200
    assert rows[3].payload.is_correct is True and rows[3].payload.step_index == 1
    assert rows[-1].event_type == "heartbeat" and rows[-1].class_id is None
    # Phần thập phân 1–6 chữ số và không có phần thập phân đều hợp lệ.
    variants = [_with(_sample()[0], event_time=t, sent_time=t)
                for t in ("2026-10-27T08:00:00Z", "2026-10-27T08:00:00.5Z", "2026-10-27T08:00:00.123456Z")]
    parsed = job.parse_events(_raw_frame(spark, [json.dumps(v) for v in variants])).collect()
    assert [r.invalid_reason for r in parsed] == [None] * 3
    assert parsed[1].event_time_ms - parsed[0].event_time_ms == 500


def test_contract_violations_get_first_failing_reason(spark):
    cases = _bad_cases()
    rows = job.parse_events(_raw_frame(spark, [v for _, v in cases] + [None])).collect()
    got = [r.invalid_reason for r in rows]
    assert got == [reason for reason, _ in cases] + ["malformed_json"]


def test_future_event_time_is_measured_against_the_kafka_timestamp_of_the_record(spark):
    """Sự kiện học tập: đi trước timestamp Kafka tối đa 120 s; heartbeat: tối đa W + phần dư (mặc định 30 + 60 s).
    Kết luận chỉ phụ thuộc bản ghi (không phụ thuộc giờ xử lý), nên query trạng thái và dead-letter
    phân loại giống nhau."""
    open_problem = _sample()[0]
    stamp = dt.datetime(2026, 10, 27, 9, tzinfo=dt.timezone.utc)
    second = dt.timedelta(seconds=1)
    milli = dt.timedelta(milliseconds=1)

    def learning(lead):
        return json.dumps(_with(open_problem, event_id=str(uuid.uuid4()), event_time=_iso(stamp + lead),
                                sent_time=_iso(stamp + lead)))

    def beat(lead):
        return json.dumps(_heartbeat(event_time=_iso(stamp + lead), sent_time=_iso(stamp + lead)))

    cases = [
        (learning(120 * second), None),
        (learning(120 * second + milli), "future_event_time"),
        (learning(-3 * 3600 * second), None),  # cũ hơn nhiều: không phải việc của kiểm tra này
        (beat(90 * second), None),  # W mặc định 30 s + phần dư 60 s
        (beat(90 * second + milli), "future_event_time"),
        (beat(31 * second), None),  # heartbeat của bộ phát lại: W + 1 s
    ]
    values = [v for v, _ in cases]
    rows = job.parse_events(_raw_frame(spark, values, [stamp] * len(values))).collect()
    assert [r.invalid_reason for r in rows] == [reason for _, reason in cases]
    # Ngưỡng theo tham số của run: W = 10 s, phần dư 0 ⇒ heartbeat đi trước 31 s bị loại.
    settings = job.JobSettings(run_id="r", watermark_ms=10_000, heartbeat_allowance_s=0, max_future_s=5)
    assert settings.heartbeat_max_future_ms == 10_000
    rows = job.parse_events(_raw_frame(spark, values, [stamp] * len(values)), settings.max_future_s,
                            settings.heartbeat_max_future_ms).collect()
    assert [r.invalid_reason for r in rows] == ["future_event_time", "future_event_time", None,
                                                 "future_event_time", "future_event_time", "future_event_time"]
    # Cùng một bản ghi, timestamp Kafka khác ⇒ kết luận theo timestamp của chính bản ghi.
    same = [learning(150 * second)] * 2
    rows = job.parse_events(_raw_frame(spark, same, [stamp, stamp + 60 * second])).collect()
    assert [r.invalid_reason for r in rows] == ["future_event_time", None]
    dead = job.dead_letter_records(job.parse_events(_raw_frame(spark, same, [stamp, stamp + 60 * second]))).collect()
    assert [json.loads(r.value)["reason"] for r in dead] == ["future_event_time"]


def test_dead_letter_records_keep_raw_value_and_reason(spark):
    values = [SAMPLE.read_text(encoding="utf-8").splitlines()[0], "{broken"]
    records = job.dead_letter_records(job.parse_events(_raw_frame(spark, values))).collect()
    assert len(records) == 1
    body = json.loads(records[0].value)
    assert body == {"reason": "malformed_json", "source_topic": "learner-events", "partition": 0, "offset": 1,
                    "kafka_timestamp_ms": body["kafka_timestamp_ms"], "raw": "{broken"}


def test_processor_input_drops_invalid_and_heartbeats_with_expected_columns(spark):
    lines = SAMPLE.read_text(encoding="utf-8").splitlines()
    values = [*lines, json.dumps(_heartbeat()), "{broken"]
    settings = job.JobSettings(run_id="schema-test", mode="none", dedup=False)  # batch: không có watermark
    frame = job.processor_input(job.parse_events(_raw_frame(spark, values)), settings)
    assert [(f.name, f.dataType) for f in frame.schema.fields] == list(S.PROCESSOR_INPUT_FIELDS)
    rows = frame.collect()
    assert len(rows) == 20 and {r.learner_id for r in rows} == {e["learner_id"] for e in _sample()}


@pytest.mark.parametrize("dedup", [True, False])
def test_heartbeat_filter_stays_above_watermark_in_streaming_plan(spark, dedup):
    """Heartbeat phải tới được EventTimeWatermark (để đẩy watermark) rồi mới bị lọc, kể cả khi tắt khử trùng."""
    import contextlib
    import io

    from pyspark.sql import functions as F

    rate = spark.readStream.format("rate").load()
    raw = rate.select(F.lit(None).cast("binary").alias("key"), F.lit(json.dumps(_heartbeat())).cast("binary").alias("value"),
                      F.lit("learner-events").alias("topic"), F.lit(0).alias("partition"),
                      F.col("value").alias("offset"), F.col("timestamp"))
    settings = job.JobSettings(run_id="plan-test", mode="eager", dedup=dedup)
    buf = io.StringIO()
    with contextlib.redirect_stdout(buf):
        job.processor_input(job.parse_events(raw), settings).explain("extended")
    optimized = buf.getvalue().split("== Optimized Logical Plan ==")[1].split("== Physical Plan ==")[0]
    lines = optimized.splitlines()
    watermark = next(i for i, line in enumerate(lines) if "EventTimeWatermark" in line)
    heartbeat_filter = next(i for i, line in enumerate(lines)
                            if re.search(rf"Filter .*\(event_type#\d+ = {S.HEARTBEAT_EVENT_TYPE}\)", line))
    assert heartbeat_filter < watermark, optimized


def test_job_settings_and_interval_parsing():
    assert job.parse_interval_ms("30") == job.parse_interval_ms("30s") == job.parse_interval_ms("30 seconds") == 30_000
    assert job.parse_interval_ms("2 minutes") == 120_000 and job.parse_interval_ms("0.5s") == 500
    assert job.spark_interval(30_000) == "30 seconds" and job.spark_interval(1_500) == "1500 milliseconds"
    for bad in ("", "abc", "0", "-5s", "3 hours"):
        with pytest.raises(job.JobError):
            job.parse_interval_ms(bad)
    args = job.build_parser().parse_args(["--run-id", "r1", "--mode", "ordered", "--watermark", "45s", "--dedup",
                                          "off", "--trigger", "2 seconds", "--learner-model", "bkt",
                                          "--max-offsets-per-trigger", "500", "--inactivity-s", "0"])
    settings = job.settings_from_args(args)
    assert (settings.mode, settings.watermark_ms, settings.dedup, settings.trigger_ms, settings.learner_model,
            settings.max_offsets_per_trigger, settings.inactivity_ms) == ("ordered", 45_000, False, 2_000, "bkt",
                                                                          500, None)
    assert job.settings_from_args(job.build_parser().parse_args(["--run-id", "r2"])).trigger_ms is None
    assert settings.max_future_s == 120 and settings.heartbeat_allowance_s == 60
    tuned = job.settings_from_args(job.build_parser().parse_args(["--run-id", "r3", "--max-future-s", "30",
                                                                  "--heartbeat-allowance-s", "3600"]))
    assert (tuned.max_future_s, tuned.heartbeat_max_future_ms) == (30, 30_000 + 3_600_000)
    for kwargs in ({"run_id": "../x"}, {"run_id": "r", "mode": "late"}, {"run_id": "r", "starting_offsets": "{bad"},
                   {"run_id": "r", "max_offsets_per_trigger": 0}, {"run_id": "r", "learner_model": "lstm"},
                   {"run_id": "r", "redis_key_prefix": "bad prefix*"}, {"run_id": "r", "max_future_s": 0},
                   {"run_id": "r", "heartbeat_allowance_s": -1}, {"run_id": "r", "heartbeat_allowance_s": 3601}):
        with pytest.raises(job.JobError):
            job.JobSettings(**kwargs)
    job.JobSettings(run_id="r", starting_offsets='{"learner-events": {"0": 5}}')


def test_job_config_overrides_topics_and_spark_conf(monkeypatch, tmp_path):
    from ai_math_tutor.config import load_config

    cfg = load_config(env={"NHOM8_DATA_ROOT": str(tmp_path), "NHOM8_KAFKA_PARTITIONS": "3"}, dotenv_path=None)
    settings = job.JobSettings(run_id="r1", input_topic="in-r1", dead_letter_topic="dl-r1", alerts_topic="al-r1",
                               shuffle_partitions=6)
    run_cfg = job.job_config(cfg, settings)
    assert run_cfg.kafka.topics == {**cfg.kafka.topics, "learner_events": "in-r1", "dead_letter": "dl-r1",
                                    "alerts": "al-r1"}
    conf = job.spark_conf(run_cfg, settings)
    assert conf["spark.sql.shuffle.partitions"] == "6"
    assert conf["spark.sql.streaming.stateStore.providerClass"].endswith("RocksDBStateStoreProvider")
    assert conf["spark.local.dir"].startswith(str(tmp_path))
    assert job.spark_conf(run_cfg, job.JobSettings(run_id="r1"))["spark.sql.shuffle.partitions"] == "3"


def test_checkpoint_settings_recorded_on_first_start_and_mismatch_refused(tmp_path):
    checkpoint = tmp_path / "checkpoints" / "r1"
    settings = job.JobSettings(run_id="r1", mode="eager", learner_model="rule", watermark_ms=30_000)
    first = job.ensure_checkpoint_settings(checkpoint, settings, 3)
    assert first["adopted_on_resume"] is False
    assert first["settings"] == {"mode": "eager", "learner_model": "rule", "dedup": True, "watermark_ms": 30_000,
                                 "inactivity_ms": settings.inactivity_ms, "state_ttl_ms": settings.state_ttl_ms,
                                 "shuffle_partitions": 3}
    stored = (checkpoint / job.CHECKPOINT_SETTINGS_FILE).read_text(encoding="utf-8")
    # Chạy lại cùng tham số (tham số không ngữ nghĩa có thể đổi): chấp nhận, tệp giữ nguyên.
    assert job.ensure_checkpoint_settings(checkpoint, job.JobSettings(run_id="r1", trigger_ms=2_000), 3) == first
    assert (checkpoint / job.CHECKPOINT_SETTINGS_FILE).read_text(encoding="utf-8") == stored
    for changed, partitions in ((job.JobSettings(run_id="r1", mode="ordered"), 3),
                                (job.JobSettings(run_id="r1", learner_model="bkt"), 3),
                                (job.JobSettings(run_id="r1", state_ttl_ms=3_600_000), 3),
                                (job.JobSettings(run_id="r1", watermark_ms=60_000), 3),
                                (job.JobSettings(run_id="r1"), 6)):
        with pytest.raises(job.JobError, match="không chạy tiếp checkpoint"):
            job.ensure_checkpoint_settings(checkpoint, changed, partitions)
    assert (checkpoint / job.CHECKPOINT_SETTINGS_FILE).read_text(encoding="utf-8") == stored

    legacy = tmp_path / "checkpoints" / "old"
    (legacy / "offsets").mkdir(parents=True)
    assert job.ensure_checkpoint_settings(legacy, job.JobSettings(run_id="old"), 3)["adopted_on_resume"] is True
    (legacy / job.CHECKPOINT_SETTINGS_FILE).write_text("{broken", encoding="utf-8")
    with pytest.raises(job.JobError, match="không đọc được"):
        job.ensure_checkpoint_settings(legacy, job.JobSettings(run_id="old"), 3)


def test_job_record_is_never_overwritten_on_restart(tmp_path):
    from dataclasses import replace

    from ai_math_tutor.config import load_config

    cfg = load_config(env={"NHOM8_DATA_ROOT": str(tmp_path)}, dotenv_path=None)
    cfg = replace(cfg, paths=replace(cfg.paths, results=tmp_path / "results"))
    settings = job.JobSettings(run_id="r1")
    conf = {"spark.sql.shuffle.partitions": "3"}
    topics = {cfg.kafka.topics["learner_events"]: 3}
    paths = [job.write_job_record(cfg, settings, conf, topics=topics, resumed=resumed)
             for resumed in (False, True, True)]
    assert [p.name for p in paths] == ["job.json", "job-1.json", "job-2.json"]
    records = [json.loads(p.read_text(encoding="utf-8")) for p in paths]
    assert [r["resumed_from_checkpoint"] for r in records] == [False, True, True]
