"""Schema Spark của pipeline streaming: đầu vào theo hợp đồng LearnerEvent v0.2 và đầu ra của processor.

- `EVENT_SCHEMA`: StructType cho `from_json` trên `value` của topic `learner-events`. Sinh tay
  từ `contracts/learner_event_v0_2.schema.json`; `tests/test_streaming_schema.py` đối chiếu từng
  trường (tên, kiểu) với JSON Schema nên hai bên không thể lệch nhau âm thầm.
  `payload` là hợp (union) các trường của mọi loại sự kiện — trường không thuộc loại đó là null.
- `PROCESSOR_INPUT_FIELDS`: các cột sau khi parse đưa vào `transformWithStateInPandas`.
- `OUTPUT_SCHEMA`: một schema chung cho mọi dòng processor phát ra, cột `kind` phân loại
  (`update` | `alert` | `progress`). Đây là hợp đồng với sinks (foreachBatch).

Quy ước của các dòng `alert`: `stuck_onset_event_id` chứa sự kiện kích hoạt cảnh báo — với
`stuck` là sự kiện làm `is_wheel_spinning` chuyển sang True, với `consec_wrong` là lần sai thứ 3
liên tiếp, với `inactivity` là sự kiện cuối trước khi người học ngừng hoạt động.
"""

from __future__ import annotations

from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

HEARTBEAT_EVENT_TYPE = "heartbeat"
HEARTBEAT_LEARNER_ID = "L-0000000000000000"
SCHEMA_VERSIONS = ("0.1", "0.2")
LEARNING_EVENT_TYPES = ("open_problem", "submit_step", "wrong_answer", "request_hint", "complete")
EVENT_TYPES = (*LEARNING_EVENT_TYPES, HEARTBEAT_EVENT_TYPE)
PLATFORMS = ("web", "android", "ios")
CONNECTIONS = ("wifi", "cellular", "unknown")

# Mẫu regex chép từ JSON Schema (test kiểm khớp từng ký tự).
EVENT_ID_PATTERN = r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$"
LEARNER_ID_PATTERN = r"^L-[0-9a-f]{16}$"
CLASS_ID_PATTERN = r"^C-[A-Z0-9-]{2,32}$"
SESSION_ID_PATTERN = r"^S-[0-9a-f]{8}$"
PROBLEM_ID_PATTERN = r"^P-[A-Z]+-[0-9]{3}$"
SKILL_ID_PATTERN = r"^[a-z_]+\.[a-z_]+$"
DATETIME_PATTERN = r"^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}(\.[0-9]{1,6})?Z$"
APP_VERSION_PATTERN = r"^[0-9]+\.[0-9]+\.[0-9]+$"
# Định dạng Spark cho event_time/sent_time: phần thập phân 1–6 chữ số là tùy chọn.
SPARK_DATETIME_FORMAT = "yyyy-MM-dd'T'HH:mm:ss[.SSSSSS]X"

CLIENT_SCHEMA = StructType([
    StructField("platform", StringType()),
    StructField("app_version", StringType()),
    StructField("connection", StringType()),
])

PAYLOAD_SCHEMA = StructType([
    StructField("step_index", LongType()),
    StructField("expression", StringType()),
    StructField("is_correct", BooleanType()),
    StructField("answer", StringType()),
    StructField("attempt", LongType()),
    StructField("hint_level", LongType()),
    StructField("attempts", LongType()),
    StructField("hints_used", LongType()),
])

EVENT_SCHEMA = StructType([
    StructField("schema_version", StringType()),
    StructField("event_id", StringType()),
    StructField("event_type", StringType()),
    StructField("learner_id", StringType()),
    StructField("class_id", StringType()),
    StructField("session_id", StringType()),
    StructField("session_seq", LongType()),
    StructField("problem_id", StringType()),
    StructField("skill_ids", ArrayType(StringType())),
    StructField("event_time", StringType()),
    StructField("sent_time", StringType()),
    StructField("client", CLIENT_SCHEMA),
    StructField("payload", PAYLOAD_SCHEMA),
])

EVENT_FIELD_NAMES: tuple[str, ...] = tuple(EVENT_SCHEMA.fieldNames())
REQUIRED_FIELDS = ("schema_version", "event_id", "event_type", "learner_id", "event_time", "sent_time", "payload")
LEARNING_REQUIRED_FIELDS = ("class_id", "session_id", "session_seq", "problem_id", "skill_ids", "client")
HEARTBEAT_FIELDS = REQUIRED_FIELDS

# Cột đưa vào processor (thứ tự cố định; processor đọc theo tên).
PROCESSOR_INPUT_FIELDS: tuple[tuple[str, DataType], ...] = (
    ("learner_id", StringType()),
    ("class_id", StringType()),
    ("event_id", StringType()),
    ("event_type", StringType()),
    ("event_time_ms", LongType()),
    ("sent_time_ms", LongType()),
    ("session_seq", LongType()),
    ("problem_id", StringType()),
    ("skill_ids", ArrayType(StringType())),
    ("is_correct", BooleanType()),
    ("step_index", LongType()),
)
PROCESSOR_INPUT_COLUMNS: tuple[str, ...] = tuple(name for name, _ in PROCESSOR_INPUT_FIELDS)

OUTPUT_KINDS = ("update", "alert", "progress")
ALERT_TYPES = ("stuck", "consec_wrong", "inactivity")

OUTPUT_SCHEMA = StructType([
    StructField("kind", StringType()),
    StructField("learner_id", StringType()),
    StructField("class_id", StringType()),
    StructField("skill_id", StringType()),
    StructField("p_mastery", DoubleType()),
    StructField("opportunities", IntegerType()),
    StructField("correct", IntegerType()),
    StructField("consec_wrong", IntegerType()),
    StructField("streak", IntegerType()),
    StructField("mastered", BooleanType()),
    StructField("stuck", BooleanType()),
    StructField("stuck_onset_event_id", StringType()),
    StructField("state_version", LongType()),
    StructField("last_event_time_ms", LongType()),
    StructField("applied_event_ids", ArrayType(StringType())),
    StructField("applied_event_time_ms", ArrayType(LongType())),
    StructField("applied_sent_time_ms", ArrayType(LongType())),
    StructField("applied_out_of_order", ArrayType(BooleanType())),
    StructField("alert_type", StringType()),
    StructField("alert_id", StringType()),
    StructField("alert_event_time_ms", LongType()),
    StructField("problem_id", StringType()),
    StructField("wrong_answers", IntegerType()),
    StructField("last_step", IntegerType()),
    StructField("completed", BooleanType()),
])
OUTPUT_COLUMNS: tuple[str, ...] = tuple(OUTPUT_SCHEMA.fieldNames())
