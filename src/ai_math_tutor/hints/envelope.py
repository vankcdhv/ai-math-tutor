"""Phong bì hợp đồng của `POST /hints`: kiểm theo JSON Schema dựng từ hợp đồng LearnerEvent v0.2.

Trường định danh/thời gian/client dùng **đúng** định nghĩa của `learner_event_v0_2.schema.json` (một
nguồn cho mẫu `L-`/`C-`/`S-`/`P-`, UUID, thời gian UTC `...Z`), cộng hai trường của yêu cầu gợi ý:
`step_index` (≥ 1) và `expression` (tùy chọn; chỉ chữ số, chữ cái thường, phép toán, ngoặc — whitelist trước cả `math_check`).
Không có trường thừa; thiếu trường bắt buộc → 422.
"""

from __future__ import annotations

import copy
from typing import Any

from jsonschema import Draft202012Validator, FormatChecker

from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, load_schema

MAX_BODY_BYTES = 4096
MAX_STEP_INDEX = 64
MAX_EXPRESSION_LENGTH = 200
# Ký tự của một bước (`... = ...`) hoặc đáp án (`4`, `{2, 3}`), ẩn là một chữ cái thường (ẩn gốc
# của đề, vd. `2*y = 8`; dịch vụ đổi về `x` trước khi kiểm). math_check còn kiểm chặt hơn sau đó.
EXPRESSION_PATTERN = r"^[0-9a-z+\-*/^().=,{}\s]+$"
_CONTRACT_FIELDS = ("event_id", "learner_id", "class_id", "session_id", "session_seq", "problem_id",
                    "skill_ids", "event_time", "sent_time", "client")
REQUIRED_FIELDS = (*_CONTRACT_FIELDS, "step_index")


def hint_request_schema() -> dict[str, Any]:
    contract = load_schema()["properties"]
    properties: dict[str, Any] = {name: copy.deepcopy(contract[name]) for name in _CONTRACT_FIELDS}
    properties["learner_id"] = {"allOf": [properties["learner_id"], {"not": {"const": HEARTBEAT_LEARNER_ID}}]}
    properties["step_index"] = {"type": "integer", "minimum": 1, "maximum": MAX_STEP_INDEX}
    properties["expression"] = {"type": "string", "minLength": 1, "maxLength": MAX_EXPRESSION_LENGTH,
                                "pattern": EXPRESSION_PATTERN}
    return {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "HintRequest (phong bì LearnerEvent v0.2)",
        "type": "object",
        "additionalProperties": False,
        "required": list(REQUIRED_FIELDS),
        "properties": properties,
    }


_VALIDATOR = Draft202012Validator(hint_request_schema(), format_checker=FormatChecker())


def envelope_errors(body: Any) -> list[str]:
    """Lỗi theo schema, dạng `đường/dẫn: thông báo` (rỗng = hợp lệ)."""
    errors = sorted(_VALIDATOR.iter_errors(body), key=lambda e: list(e.path))
    return [f"{'/'.join(map(str, e.path)) or '<gốc>'}: {e.message}" for e in errors]
