"""Chọn bậc gợi ý (bảng quyết định, fail-closed).

Đầu vào lấy từ Redis: số lần xin gợi ý và số lần gửi biểu thức sai **ở cùng bước** (`hintctx:`,
do dịch vụ gợi ý tự đếm, ghi đồng bộ), `consec_wrong`/`stuck` của kỹ năng chính (`state:`, pipeline
streaming ghi) và số lần nộp đáp án cuối sai (`progress:`).

| Điều kiện | Bậc | Được đưa đáp án cuối |
|---|---|---|
| không có trạng thái (`state_source = default`: Redis lỗi, khóa trống, JSON hỏng) | 1 | không |
| lần xin thứ k ở bước này | min(k, 3) | — |
| đang chật vật: `consec_wrong ≥ 3`, hoặc `stuck`, hoặc ≥ 2 biểu thức sai ở bước này | +1 (tối đa 3) | — |
| `progress.wrong_answers ≥ 2` | — | có |

"Được đưa đáp án" chỉ mở khóa bước lộ đáp án ở bậc 3; mọi bậc khác không bao giờ có đáp án.
"""

from __future__ import annotations

from dataclasses import dataclass

from ai_math_tutor.learner_rules import CONSEC_WRONG_ALERT

STATE_SOURCES = ("redis", "default")
MAX_LEVEL = 3
MAX_LEVEL_WITHOUT_STATE = 1
ANSWER_AFTER_WRONG_ANSWERS = 2
STRUGGLE_STEP_WRONG = 2


@dataclass(frozen=True, slots=True)
class LadderInput:
    state_source: str
    hint_count: int = 1        # gồm cả yêu cầu hiện tại
    step_wrong_count: int = 0  # biểu thức sai đã gửi kèm yêu cầu gợi ý ở bước này, gồm yêu cầu hiện tại
    consec_wrong: int = 0
    stuck: bool = False
    wrong_answers: int = 0

    def __post_init__(self) -> None:
        if self.state_source not in STATE_SOURCES:
            raise ValueError(f"state_source phải thuộc {STATE_SOURCES}, nhận {self.state_source!r}")
        for name in ("hint_count", "step_wrong_count", "consec_wrong", "wrong_answers"):
            value = getattr(self, name)
            if isinstance(value, bool) or not isinstance(value, int) or value < 0:
                raise ValueError(f"{name} phải là số nguyên ≥ 0, nhận {value!r}")


@dataclass(frozen=True, slots=True)
class LevelDecision:
    level: int
    answer_allowed: bool
    reason: str


def select_level(inp: LadderInput) -> LevelDecision:
    """Bậc gợi ý theo bảng quyết định ở đầu mô-đun."""
    if inp.state_source != "redis":
        return LevelDecision(level=MAX_LEVEL_WITHOUT_STATE, answer_allowed=False, reason="no_state")
    level = min(max(inp.hint_count, 1), MAX_LEVEL)
    reason = f"hint_count={inp.hint_count}"
    struggling = (inp.consec_wrong >= CONSEC_WRONG_ALERT or inp.stuck
                  or inp.step_wrong_count >= STRUGGLE_STEP_WRONG)
    if struggling and level < MAX_LEVEL:
        level += 1
        reason += "+struggling"
    return LevelDecision(level=level, answer_allowed=inp.wrong_answers >= ANSWER_AFTER_WRONG_ANSWERS, reason=reason)
