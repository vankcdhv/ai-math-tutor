"""Hàm `diagnose` quay vòng CPU với biểu thức tấn công, dùng cho test quá hạn của dịch vụ gợi ý.

Đặt trong mô-đun nhẹ (chỉ import `math_check`, đã nạp sẵn trong forkserver của safe_math) để worker
thay thế unpickle được hàm này trong vài ms — nếu đặt trong mô-đun test nặng, chính việc import đã
vượt thời hạn và làm sai số đo.
"""

from __future__ import annotations

from ai_math_tutor import math_check

ATTACK = "9^9^9 = x"


def spinning_diagnose(problem, expression, step_index=None):
    if expression == ATTACK:
        while True:  # mô phỏng biểu thức lọt qua bộ lọc nhưng làm SymPy chạy mãi
            pass
    return math_check.diagnose(problem, expression, step_index)


def test_spinning_diagnose_delegates_for_normal_expressions():
    problem = {"kind": "equation", "equation": "2*x + 3 = 11", "answer": "4", "steps": ["2*x = 8"],
               "misconceptions": []}
    assert spinning_diagnose(problem, "2*x = 8", 1) == {"equivalent": True, "misconception": None}
