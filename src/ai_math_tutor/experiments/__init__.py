"""Bộ chạy thí nghiệm: ma trận cấu hình → vòng đời từng run → run record (Phụ lục B).

Mô-đun: `matrix` (đọc YAML, mở rộng ma trận + ablation + stress, resume, ước lượng giờ máy),
`runner` (vòng đời run, CLI), `metrics` (chỉ số sau warm-up, lag bền vững, độ ổn định RSS),
`monitor` (CPU/RAM của tiến trình con và container), `reaper` (pidfile, dọn tiến trình mồ côi, khóa
runner), `datasets` (cache dữ liệu mô phỏng, cửa sổ phát lại cho batch), `stream_job` và
`replay_child` (hai tiến trình con do runner khởi động).
"""
