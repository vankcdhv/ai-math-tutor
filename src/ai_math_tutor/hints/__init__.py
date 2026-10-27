"""Gợi ý toán 3 bậc đã kiểm chứng: mẫu tiếng Việt, thang gợi ý cá nhân hóa, dịch vụ HTTP.

Mô-đun: `templates` (gợi ý mẫu không cá nhân hóa — baseline và đường dự phòng), `phrases` (bảng câu
mẫu theo loại bài × bậc × loại lỗi), `verify` (sự thật đã kiểm của từng bài + kiểm bản nháp gợi ý),
`ladder` (chọn bậc, fail-closed), `generator` (`HintGenerator`: template | ladder), `envelope`,
`auth`, `state_client`, `audit_log`, `app` (FastAPI), `audit_set` (bộ gợi ý offline cho audit).
Gói chỉ nạp phần mẫu; dịch vụ HTTP import riêng `ai_math_tutor.hints.app`.
"""

from ai_math_tutor.hints.templates import (
    LEVELS,
    MISCONCEPTIONS,
    SKILL_TEMPLATES,
    HintTemplate,
    all_templates,
    template_hint,
    verify_template,
)

__all__ = [
    "LEVELS",
    "MISCONCEPTIONS",
    "SKILL_TEMPLATES",
    "HintTemplate",
    "all_templates",
    "template_hint",
    "verify_template",
]
