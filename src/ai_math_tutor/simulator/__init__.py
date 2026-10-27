"""Bộ mô phỏng người học: lịch sự kiện hợp lệ theo hợp đồng + ground truth tách riêng + chèn lỗi mạng.

Mô-đun: `population` (quần thể, tham số BKT ẩn), `behavior` (phiên học), `network`
(độ trễ/muộn/trùng theo kết nối), `ground_truth` (mastery ẩn, episode bế tắc, lỗi giao),
`output` (định dạng events.parquet, đóng dấu lại cho bộ phát lại), `engine` (điều phối), `cli`.
"""

from ai_math_tutor.simulator.engine import SimulationConfig, run_simulation

__all__ = ["SimulationConfig", "run_simulation"]
