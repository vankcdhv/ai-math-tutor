"""Dashboard lớp và theo dõi pipeline (Streamlit, chỉ đọc Redis).

Mô-đun: `queries` (đọc khóa Redis của pipeline streaming, áp ngưỡng k cho mọi số liệu nhóm; hàm thuần,
test bằng fakeredis), `app` (giao diện 3 tab Lớp / Giáo viên / Pipeline, tự làm mới bằng `st.fragment`).
Chạy: `streamlit run src/ai_math_tutor/dashboard/app.py --server.address 127.0.0.1`.
"""
