# Threat / privacy note v0.2 (v0.1: minh chứng tuần 1; v0.2: T5 theo hạ tầng Compose)

Phạm vi: hệ thống nguyên mẫu chạy trên một máy (Docker Compose) cho môn INT4418, dữ liệu mặc định là **mô phỏng**. Ghi chú này xác định hệ thống lưu gì, bảo vệ gì, và rủi ro còn lại.

## 1. Kiểm kê dữ liệu

| Dữ liệu | Nơi lưu | Nhạy cảm | Thời hạn giữ |
|---|---|---|---|
| Sự kiện học tập (bút danh) | Kafka `learner-events`, lake Parquet | Trung bình: hành vi học, lỗi sai | Kafka 24 h; lake đến hết môn |
| Trạng thái người học (mastery, bế tắc) | Redis, snapshot Parquet | Trung bình: có thể bị dùng để dán nhãn học sinh | Ghi đè liên tục; snapshot đến hết môn |
| Ground truth bộ mô phỏng | `ground_truth.jsonl` trong thư mục `--out` của bộ mô phỏng, dưới `$NHOM8_DATA_ROOT` (tách khỏi luồng sự kiện) | Thấp (dữ liệu giả) | Theo lần chạy |
| Salt HMAC | Biến môi trường, không vào Git | **Cao**: lộ salt + danh sách mã gốc ⇒ nối lại danh tính | Đổi khi nghi lộ |
| Mã người học gốc | **Không lưu** trong hệ thống | — | — |

## 2. Mối đe dọa và biện pháp

| # | Mối đe dọa | Biện pháp trong thiết kế | Kiểm chứng |
|---|---|---|---|
| T1 | Nhận dạng lại người học từ `learner_id` | HMAC-SHA256 với salt bí mật; không lưu mã gốc; salt mẫu công khai chỉ dùng cho `data_sample/` | Test: không có `hs-`, tên, email trong mẫu |
| T2 | PII lọt qua trường tự do | Không có trường văn bản tự do; `expression`/`answer` chỉ chấp nhận ký tự toán học, ≤ 200 ký tự | `tests/test_math_check.py` |
| T3 | Chèn mã khi phân tích biểu thức (`sympy.parse_expr` dùng `eval`) | Danh sách ký tự cho phép + chặn `__` trước khi parse; ngoài ra chạy dịch vụ gợi ý không quyền, có timeout | Test chuỗi độc hại bị từ chối |
| T4 | Dashboard lộ cá nhân trong nhóm nhỏ | Mọi số cấp lớp/trường: < k = 5 hiện "<5"; ≥ k làm tròn tới bội số của 5 (hai lần làm mới chỉ chênh 0 hoặc ≥ 5); tỉ lệ cần tử, mẫu, phần bù ≥ 5; tổng hàng/cột chỉ hiện khi mọi ô ≥ 5 (chống suy ngược bằng hiệu); không hiện trung bình nhóm. learner_id và cảnh báo chỉ ở chế độ giáo viên (env + mật khẩu). Dashboard chỉ nghe 127.0.0.1 | `tests/test_dashboard_queries.py`, `tests/test_dashboard_app.py` |
| T5 | Giả mạo / bơm sự kiện, truy cập trái phép Kafka/Redis | Kafka (9092) và Redis (6379) chỉ publish trên 127.0.0.1. Redis bật `requirepass` từ `.env` (gitignored, mẫu `.env.example`). Image Kafka/Redis ghim theo digest. Spark driver bind 127.0.0.1, Spark UI tắt. Kafka không tự tạo topic. Giới hạn: Kafka không xác thực client; hệ thống thật cần xác thực client + chữ ký | `make check` chạy `tests/test_compose_bindings.py`, thất bại nếu có cổng bind giao diện khác 127.0.0.1, image không ghim digest, hoặc Redis thiếu `--requirepass` |
| T6 | Sự kiện trùng/muộn làm sai trạng thái → can thiệp sai | Khử trùng theo `event_id`, watermark, tính theo `event_time`; đo độ đúng với ground truth | Thí nghiệm D |
| T7 | Thiên lệch với người dùng mạng kém (nhiều sự kiện muộn bị loại) | Lát cắt công bằng theo `client.connection`; báo cáo chênh lệch độ đúng và thời gian phát hiện bế tắc | Thí nghiệm D |
| T8 | Gợi ý sai toán học | Mọi biểu thức trong gợi ý qua SymPy; trượt thì dùng mẫu đã kiểm | Hint validity (tuần 6–8) |
| T9 | Giả mạo yêu cầu gợi ý | Khóa API demo là một khóa chung (header `X-API-Key`, so thời gian hằng). Ai có khóa đều gửi được yêu cầu nhân danh bất kỳ `learner_id` nào, đẩy bậc gợi ý của người khác lên (qua `hintctx`) hoặc đọc gợi ý cho bài của họ. Giảm nhẹ: bind 127.0.0.1; giới hạn 60 yêu cầu/phút/learner (burst 10) → 429; gợi ý không chứa đáp án trước khi người học sai đáp án cuối 2 lần (lấy từ `progress:` do pipeline ghi, client không ghi được); retry cùng `event_id` không tăng bậc; nhật ký `hint-requests` để truy vết. Giới hạn của nguyên mẫu: chưa có xác thực theo người học (token ký theo phiên) | `tests/test_hint_service.py` (401/422/429) |

## 3. Dữ liệu thật

Mặc định không dùng. Nếu nhóm muốn dùng log thật (kể cả từ nơi làm việc của thành viên): cần phê duyệt bằng văn bản của đơn vị sở hữu, ẩn danh trước khi rời hệ thống nguồn, tối thiểu hóa trường, và chỉ dùng số liệu **tổng hợp** (phân bố tốc độ sự kiện, khoảng cách giữa các sự kiện) để hiệu chỉnh bộ mô phỏng. Không đưa bản ghi thật vào Git hoặc vào báo cáo.

## 4. Rủi ro còn lại (đưa vào phần limitation)

- Bút danh không phải ẩn danh: người có salt và danh sách mã gốc vẫn nối lại được.
- Nguyên mẫu không mã hóa đường truyền; Kafka không xác thực client, Redis chỉ có mật khẩu (không TLS). Biện pháp hiện tại dựa vào việc chỉ nghe trên 127.0.0.1.
- Đánh giá công bằng trên dữ liệu mô phỏng chỉ cho thấy cơ chế, không đo được thiên lệch ngoài thực tế.
- Làm tròn số trên dashboard chỉ cho khoảng, không phải bảo đảm tuyệt đối: một học sinh có thể đẩy số qua ngưỡng làm tròn đúng lúc em đó làm bài; chỉ nhiễu kiểu differential privacy mới chặn hẳn được.
