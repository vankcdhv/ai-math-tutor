# Kịch bản demo (5–7 phút)

Luồng trình diễn: bộ mô phỏng → Kafka → Spark Structured Streaming (trạng thái người học) → cảnh báo bế tắc → yêu cầu gợi ý → gợi ý đã kiểm chứng → dashboard. Toàn bộ chạy trong Docker Compose, profile `app`, bằng một lệnh `make demo`.

Các số đo trong báo cáo được đo trên host, không đo trong container. Demo chỉ để trình diễn chức năng.

## Cấu hình demo

Tham số nằm trong `docker-compose.yml`, khối `x-demo-env`:

| Tham số | Giá trị |
|---|---|
| Dữ liệu mô phỏng | 350 người học, lớp 35 người (10 lớp), 100 sự kiện/giây, 10 phút, seed 11 |
| Job streaming | run id `demo`, `--mode eager`, watermark W = 30 s, mô hình `rule` |
| Bộ phát lại | cùng W = 30 s, cùng run id `demo` |
| Topic | `demo-learner-events`, `demo-dead-letter`, `demo-alerts` (3 partition). Các topic của lần chạy trên host không bị đụng tới |
| Khóa Redis | tiền tố `demo:` |
| Cổng | dashboard `127.0.0.1:8501`, dịch vụ gợi ý `127.0.0.1:8000` (chỉ loopback) |

Thứ tự khởi động:

1. `demo-setup` (one-shot) tạo topic, sinh dữ liệu và nạp roster.
2. `streaming` chạy job Spark. Service này healthy khi query trạng thái đã khởi động.
3. `demo-replay` (one-shot) phát lại log trong 10 phút.
4. `hint-api` và `dashboard` chạy song song với các bước trên.

## Chuẩn bị (trước buổi bảo vệ, không tính giờ)

Mọi lệnh chạy trong shell Linux tại thư mục repo. Trên Windows, dùng shell WSL đã chuẩn bị như `docs/huong-dan-chay-code.md` mục 0.

1. Tạo `.env` từ `.env.example`. Đặt giá trị ngẫu nhiên cho `REDIS_PASSWORD` và `NHOM8_HINT_API_KEY` (≥ 16 ký tự). Để mở tab Giáo viên, bật thêm hai dòng:
   ```bash
   NHOM8_TEACHER_MODE=1
   TEACHER_PASSWORD=<giá trị ngẫu nhiên>
   ```
2. Build image trước, vì lần build đầu cần mạng (image nền, gói Debian, PyPI, connector Kafka của Spark):
   ```bash
   docker compose --profile app build
   ```
   Image đã có sẵn connector Spark, nên lúc demo không cần mạng.
3. Xóa trạng thái của lần chạy thử trước. Lệnh này chỉ xóa container ứng dụng và volume dữ liệu demo; Kafka, Redis và volume Kafka được giữ nguyên:
   ```bash
   make demo-clean
   ```
4. Diễn tập trọn kịch bản ít nhất một lần, rồi chạy lại `make demo-clean`.
5. Mở sẵn hai cửa sổ terminal và một trình duyệt:
   - **Cửa sổ 1:** để chạy `make demo`, xem log.
   - **Cửa sổ 2:** để gõ lệnh. Chạy trước:
     ```bash
     export COMPOSE_PROFILES=app
     KEY="$(grep '^NHOM8_HINT_API_KEY=' .env | cut -d= -f2-)"
     ```
   - **Trình duyệt:** mở sẵn tab `http://127.0.0.1:8501`.

## Khởi động (ngay trước phần demo)

Cửa sổ 1:

```bash
make demo
```

Chờ tới khi log của `streaming` có dòng `JOB ... run demo: mode=eager watermark=30000 ms ...` và `demo-replay` có dòng `DEMO ... phát lại ...`.

Bộ phát lại chạy 10 phút, nên cả kịch bản diễn ra khi dữ liệu còn đang chảy. Đừng khởi động quá sớm: bước 7 cần bộ phát lại còn đang chạy.

## Kịch bản

| Bước | Thời gian | Nội dung |
|---|---|---|
| 1 | 0:00–0:45 | Stack đang chạy |
| 2 | 0:45–1:30 | Dữ liệu đang được phát lại |
| 3 | 1:30–2:30 | Tab Lớp: số liệu cấp lớp với ngưỡng k = 5 |
| 4 | 2:30–3:15 | Cảnh báo bế tắc, chế độ giáo viên |
| 5 | 3:15–4:30 | Yêu cầu gợi ý: bậc tăng dần, đáp án bị khóa |
| 6 | 4:30–5:15 | Tab Pipeline: lag và P95 |
| 7 | 5:15–6:30 | Tắt hẳn job rồi chạy lại: không có cảnh báo trùng |
| — | 6:30–7:00 | Tổng kết |

### Bước 1. Stack đang chạy (0:00–0:45)

Cửa sổ 2:

```bash
docker compose ps -a
```

Chỉ vào màn hình:

- `kafka`, `redis`, `streaming`, `hint-api`, `dashboard` có trạng thái `healthy`.
- `demo-setup` đã `Exited (0)`; `demo-replay` đang chạy.
- Cột PORTS chỉ có địa chỉ `127.0.0.1`: `8000` (gợi ý), `8501` (dashboard), cùng `9092` (Kafka) và `6379` (Redis). Không cổng nào mở ra mạng ngoài.
- Ba thành phần ứng dụng (job Spark, dịch vụ gợi ý, dashboard) dùng chung một image. Chúng nối Kafka qua listener nội bộ `kafka:29092`.

### Bước 2. Dữ liệu đang được phát lại (0:45–1:30)

```bash
docker compose logs --no-log-prefix demo-setup | tail -n 25
docker compose logs --tail 3 demo-replay streaming
```

Chỉ vào màn hình:

- JSON tóm tắt của bộ mô phỏng: `counts.learners` = 350, số phiên, số sự kiện trùng có chủ đích, số sự kiện muộn.
- Dòng `DEMO roster: 350 người học, 10 lớp`.
- Dòng phát lại `W = 30 s, run demo`. Job cũng chạy với `watermark=30000 ms` và cùng run id. Hai bên phải cùng W, nếu không heartbeat cuối log bị loại và watermark không vượt qua cuối log.

### Bước 3. Tab Lớp: số liệu cấp lớp với ngưỡng k = 5 (1:30–2:30)

Trình duyệt: `http://127.0.0.1:8501`, tab **Lớp**. Trang tự làm mới mỗi 2 s.

Chỉ vào màn hình:

- Dòng chú thích đầu trang ghi tiền tố khóa `demo:` và Redis đã che mật khẩu.
- Bảng "Tỉ lệ thành thạo theo lớp × kỹ năng" đầy dần theo thời gian.
- Ngưỡng k = 5:
  - Số < 5 hiện "<5".
  - Số ≥ 5 được làm tròn tới bội số của 5.
  - Tổng hàng/cột có ô "<5" thì hiện "ẩn", để không suy ngược được số của một người từ hiệu hai tổng.
- Chọn một lớp để xem chi tiết và xu hướng theo khối 5 phút.
- Tab này không có `learner_id` nào.

### Bước 4. Cảnh báo bế tắc, chế độ giáo viên (2:30–3:15)

Tab **Giáo viên** → nhập `TEACHER_PASSWORD`.

Chỉ vào màn hình:

- Bảng "Đang bế tắc — lớp …" liệt kê người học bế tắc, kèm kỹ năng, thời điểm bắt đầu, số lần sai liên tiếp.
- Bảng "20 cảnh báo mới nhất" có cảnh báo mới xuất hiện trong lúc trình bày.
- `learner_id` là bút danh mô phỏng (`L-…`). Danh tính từng người chỉ hiện ở chế độ giáo viên, và chế độ này phải bật bằng cấu hình và mật khẩu.

### Bước 5. Yêu cầu gợi ý: bậc tăng dần, đáp án bị khóa (3:15–4:30)

Cửa sổ 2. Bước này tạo ba yêu cầu cho cùng một bước của cùng một bài. Người học được chọn là người vừa có cảnh báo bế tắc. Bài được chọn có cùng kỹ năng, và người học chưa làm bài này. Bước được chọn là bước ra đáp án.

```bash
docker compose exec -T hint-api python docker/demo-hint.py --count 3 > /tmp/demo-hints.jsonl
while read -r body; do
  curl -s -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
    --data "$body" http://127.0.0.1:8000/hints | python3 -m json.tool --no-ensure-ascii --indent 2
done < /tmp/demo-hints.jsonl
```

Chỉ vào màn hình:

- Dòng đầu (stderr) in người học, lớp, kỹ năng, bài và bước đã chọn. Đây là người học vừa thấy ở tab Giáo viên.
- Mỗi phản hồi có:
  - `"state_source": "redis"`: dịch vụ đọc trạng thái mà job streaming vừa ghi.
  - `"verified": true` và `"source": "ladder"`.
- `hint_level` tăng theo số lần xin ở cùng bước, tối đa 3. Người học đang bế tắc thì thang gợi ý cộng thêm một bậc, nên lần đầu có thể đã là bậc 2.
- Ở bậc 3, `text_vi` chỉ đưa **dạng** của bước tiếp theo, với các số thay bằng □. Bước này ra đáp án, và người học chưa nộp sai đáp án cuối hai lần, nên đáp án bị khóa.
- Mọi biểu thức trong gợi ý là bước giải đã được SymPy kiểm. Nếu kiểm chứng lỗi hoặc không đọc được Redis, dịch vụ trả gợi ý an toàn bậc 1.

Quay lại tab Giáo viên, chọn lớp của người học đó. Cột "gợi ý gần nhất" hiện gợi ý vừa trả.

### Bước 6. Tab Pipeline: lag và P95 (4:30–5:15)

Trình duyệt: `http://127.0.0.1:8501/?tab=Pipeline`.

Chỉ vào màn hình:

- `input rows/s` và `processed rows/s` xấp xỉ tốc độ phát lại.
- `processing lag (tổng offset)` nhỏ và không tăng dần.
- `P95 độ trễ hệ thống (1 phút)`: từ lúc client gửi tới lúc trạng thái hiện trong Redis.
- `P95 độ cũ trạng thái (1 phút)`.
- `watermark` đi theo event time.
- Số liệu lấy từ `StreamingQueryListener` của chính query đang chạy, không phải số tính tay.

### Bước 7. Tắt hẳn job rồi chạy lại: không có cảnh báo trùng (5:15–6:30)

Cửa sổ 2. Bộ phát lại vẫn đang gửi dữ liệu trong suốt bước này.

```bash
docker compose exec redis sh -c 'redis-cli XLEN demo:alerts:stream'
docker compose kill streaming                  # SIGKILL: job không được dừng êm
docker compose ps -a streaming
```

Tab Pipeline: "tuổi số liệu" tăng dần. Sau 30 s, trang cảnh báo số liệu cũ (job có thể đã dừng). Trong lúc đó Kafka vẫn nhận sự kiện.

```bash
docker compose start streaming
docker compose logs --tail 5 streaming
```

Nếu Docker đã tự khởi động lại container theo restart policy, `start` không làm gì thêm; log vẫn cho thấy lần chạy tiếp.

Chỉ vào màn hình:

- Log job có `resumed=True`: job chạy tiếp từ checkpoint trong volume `/data/checkpoints/demo`.
- Tab Pipeline: `batch_id` tiếp nối số cũ và lag giảm dần về mức trước.

Kiểm cảnh báo trùng. Lệnh dưới in hai số: số mục trong `alerts:stream` và số `alert_id` khác nhau. Hai số bằng nhau nghĩa là không cảnh báo nào được ghi hai lần, kể cả các batch chạy lại sau khi job bị tắt.

```bash
docker compose exec redis sh -c 'echo "mục trong stream: $(redis-cli XLEN demo:alerts:stream)"; echo "alert_id khác nhau: $(redis-cli --raw XRANGE demo:alerts:stream - + | awk "prev == \"alert_id\" {print} {prev = \$0}" | sort -u | wc -l)"'
```

Cơ chế cần nêu:

- `alert_id` là hàm tất định của (người học, kỹ năng, loại, sự kiện khởi phát).
- Script Redis chỉ `XADD` khi `alert:{id}` chưa có.
- Batch chạy lại sau checkpoint giữ nguyên `batch_id`, nên không ghi đè trạng thái mới hơn.

### Tổng kết (6:30–7:00)

Nhắc lại luồng: sự kiện → Kafka → trạng thái theo người học (khử trùng theo `event_id`, watermark) → cảnh báo idempotent → gợi ý có kiểm chứng, đọc trạng thái mới nhất → dashboard tôn trọng ngưỡng k.

## Sau demo (khi có câu hỏi)

Khi `demo-replay` đã `Exited (0)` (bộ phát lại chạy hết 10 phút), đối soát toàn run:

```bash
docker compose exec streaming python -m ai_math_tutor.streaming.reconcile \
  --run-id demo --sim-dir /data/demo/sim --model rule --key-prefix demo:
```

`checks` phải toàn `true`, gồm `no_event_applied_twice`, `no_duplicate_alert_in_stream`, `lake_alerts_match_stream` và `watermark_past_end_of_log`.

Dừng ứng dụng, giữ Kafka/Redis và mọi volume:

```bash
make demo-down
```

Ctrl-C ở cửa sổ 1 dừng **mọi** container của project, kể cả Kafka và Redis. Volume vẫn còn; chạy `make up` để bật lại hạ tầng.

## Phương án dự phòng

1. **Một service lỗi giữa chừng.** Xem log rồi chạy lại riêng service đó. Dữ liệu và checkpoint nằm trong volume nên không mất.
   ```bash
   docker compose logs --tail 50 <service>
   docker compose up -d <service>
   ```
   Nếu `demo-replay` báo lần phát lại trước bị ngắt giữa chừng, chạy `make demo-clean` rồi `make demo`. Phát lại lần hai vào cùng topic sẽ áp một sự kiện hai lần, nên script từ chối.
2. **Docker không build hoặc không chạy được.** Chạy cùng luồng trên host ở chế độ phát lại, theo `docs/huong-dan-chay-code.md`:
   - mục 6: job, phát lại, nạp roster, với run `demo1` và tiền tố `demo1:`;
   - mục 8: `make hint-api HINT_ARGS="--key-prefix demo1:"`;
   - mục 9: `NHOM8_DASHBOARD_KEY_PREFIX=demo1: make dashboard`.

   Thân yêu cầu gợi ý cho bước 5 lấy bằng:
   ```bash
   uv run python docker/demo-hint.py --key-prefix demo1: --count 3
   ```
   Cách này cần JDK 17 và `uv sync` trên máy trình diễn, nên phải kiểm trước buổi bảo vệ.
3. **Cả hai cách đều không chạy.** Mở video demo đã quay sẵn theo đúng kịch bản này, cùng ảnh chụp ba tab dashboard, rồi trình bày các bước theo video.
