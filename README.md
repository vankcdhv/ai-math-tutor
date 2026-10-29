# Nhóm 8 — AI Math Tutor (INT4418 Dữ liệu lớn, PTIT)

Xử lý luồng sự kiện học tập, cập nhật trạng thái nhiều người học gần thời gian thực và đưa ra gợi ý toán có kiểm chứng, đồng thời bảo vệ quyền riêng tư. Đề bài: `HuongDan_Bai_Tap_Lon_BigData_INT4418.pdf`, Nhóm 8 (tr. 24–26).

**Trạng thái:** đã có bộ mô phỏng, Kafka ingestion, pipeline trạng thái Spark Structured Streaming, batch baseline (DataFrame + MapReduce), dịch vụ gợi ý có kiểm chứng, dashboard lớp và runner thí nghiệm; ma trận thí nghiệm và đánh giá gợi ý đang chạy. Cách chạy: [`docs/huong-dan-chay-code.md`](docs/huong-dan-chay-code.md).

## Kiến trúc

```
Bộ mô phỏng (ground truth riêng) ─▶ Kafka learner-events (key = learner_id)
                                        │
                                        ▼
                  Spark Structured Streaming: khử trùng event_id, watermark,
                  trạng thái theo người học (BKT, bế tắc bằng timer)
                     ├─▶ Redis: trạng thái mới nhất ─▶ Dashboard lớp
                     ├─▶ topic alerts (người học bế tắc)
                     └─▶ Lake Parquet theo run (applied, latency, alerts) ─▶ đối soát, phân tích
Cùng event log mô phỏng ─▶ batch baseline daily/hourly (DataFrame + MapReduce/RDD)
Client ─HTTP─▶ Dịch vụ gợi ý (FastAPI): đọc Redis ▶ SymPy kiểm chứng ▶ gợi ý 3 bậc
```

Gợi ý là yêu cầu đồng bộ nên **không** đi qua micro-batch; luồng Spark chỉ cập nhật trạng thái và cảnh báo.

## Cài đặt

Yêu cầu: Python ≥ 3.10, [uv](https://docs.astral.sh/uv/), JDK 17, Docker + Compose, `make`. Hướng dẫn đầy đủ cho macOS và máy thí nghiệm Windows + WSL2 (kể cả bẫy quoting PowerShell): [`docs/setup.md`](docs/setup.md).

```bash
uv sync                          # cài phụ thuộc theo uv.lock (PySpark 4.1.3, confluent-kafka, ...)
cp .env.example .env             # đặt REDIS_PASSWORD ngẫu nhiên (tệp đã gitignore)
make up                          # Kafka KRaft 127.0.0.1:9092 + Redis 127.0.0.1:6379
make check                       # JDK ≥ 17, unit test, validator dữ liệu mẫu, kiểm cổng compose
```

Máy Windows: chạy mọi lệnh trong WSL qua `wsl -d Ubuntu-24.04 -- bash /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor/scripts/in-wsl.sh make check`.

## Dữ liệu

- `data_sample/events-week1-sample.jsonl` — 20 dòng sự kiện mô phỏng (có 1 trùng, 1 muộn); `problems-week1-sample.jsonl` — 3 bài.
- `data_sample/problem-bank-v2.jsonl` — kho 1.260 bài (test 1.000 bài NuminaMath, dev 260), lời giải từng bước đã kiểm máy; mẫu 100 bài test đang được hai thành viên nhóm rà soát độc lập (`docs/data-card.md` mục 4). Chỉ mục bài liên quan `problem-index-v2.json` (chỉ trả bài dev). Bước phương trình viết theo ẩn `x`; ẩn gốc ở trường `variable`, hiển thị qua `Problem.display_steps`. Dựng lại: `make numina-candidates problem-bank`.
- Không có dữ liệu cá nhân; bút danh HMAC với salt mẫu công khai. Xem `docs/data-card.md`, `docs/privacy-threat-note.md`.
- Dữ liệu đầy đủ và checkpoint không đưa vào Git (`.gitignore`).

## Lệnh chạy nhanh

```bash
make up | down | ps                                              # hạ tầng Kafka + Redis
make check                                                       # kiểm tra nhanh toàn repo
make spike                                                       # spike Spark TWS + Kafka; kết quả: $NHOM8_DATA_ROOT/spike/
make clean-state [RUN_ID=...]                                    # xóa checkpoint/lake trên SSD
bash scripts/disk-check.sh 10                                    # dừng nếu SSD trống < 10 GB
uv run pytest -q -m integration                                  # test cần Docker (sau make up)
uv run python scripts/make-week1-sample.py                       # tạo lại dữ liệu mẫu (tất định)
uv run python -m ai_math_tutor.event_validation \
  data_sample/events-week1-sample.jsonl --problems data_sample/problems-week1-sample.jsonl

# Kafka ingestion (broker tắt tự tạo topic ⇒ tạo tường minh trước khi phát lại)
make topics PARTITIONS=3
make replay EVENTS="$NHOM8_DATA_ROOT/sim-ingest/r1000/events.parquet" PROCESSES=2 DURATION=60 REPLAY_ARGS=--rate-check
make verify-topic REPLAY_JSON=results/raw/replay-<run_id>.json

# Streaming (Ctrl-C dừng ở ranh giới micro-batch; chạy lại cùng RUN_ID = resume từ checkpoint).
# Bộ phát lại phải dùng cùng W và cùng run id với job, nếu không heartbeat bị loại (future_event_time)
# và watermark không qua cuối log. Trình tự đầy đủ: docs/huong-dan-chay-code.md mục 6.
make stream RUN_ID=r1 STREAM_ARGS="--mode eager --watermark 30s --learner-model bkt"
make replay EVENTS="$NHOM8_DATA_ROOT/sim/<tên>/events.parquet" REPLAY_ARGS="--run-id r1 --watermark-s 30"   # shell khác
make reconcile RUN_ID=r1 SIM_DIR="$NHOM8_DATA_ROOT/sim/<tên>" MODEL=bkt   # → results/raw/r1/reconcile.json
uv run pytest -q -s -m integration tests/integration/test_restart.py      # restart + sự kiện muộn (~5 phút)

# Batch baseline (cùng event log với streaming): snapshot dưới $NHOM8_DATA_ROOT/batch/<run_id>/,
# kết quả thô results/raw/batch/<run_id>/ (jobs.csv, episodes.csv, false_positives.csv, summary.json), run record runs.jsonl
make baseline                                                    # daily + hourly trên log 1K × 1 ngày
uv run python -m ai_math_tutor.batch.run_batch_baseline --schedule daily \
  --events "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/events.parquet" \
  --ground-truth "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/ground_truth.jsonl" --parity-check
# --schedule hourly | --impl rdd (MapReduce) | --replay-start-ms <epoch ms> (đóng dấu lại như bộ phát lại, dịch ground truth)
# --phase-s 0 (chạy đúng 00:00; mặc định pha lấy từ seed) | --partitions N | --timeout-s 600
make mapreduce-matrix MAPREDUCE_ARGS=--dry-run   # MapReduce (learner, problem): 3 quy mô × Config A/B × 3 lần → results/mapreduce/ (docs/experiment-protocol.md §5)
```

### Dịch vụ gợi ý

```bash
make hint-api HINT_ARGS="--hint-generator ladder"   # 127.0.0.1:8000, POST /hints (header X-API-Key), GET /healthz
make hint-load LOAD_ARGS="--users 1,10,50,100 --duration-s 60"   # Locust headless, ghi results/raw/runs.jsonl
make hint-audit-set   # bộ gợi ý offline của tập test (đầu vào đánh giá gợi ý) dưới $NHOM8_DATA_ROOT/hints/
```

Không dùng LLM lúc chạy: gợi ý 3 bậc lấy từ bước giải đã kiểm SymPy, kiểm lại trước khi trả; lỗi kiểm chứng, quá hạn hoặc Redis hỏng ⇒ gợi ý an toàn bậc 1, không lộ đáp án.

### Dashboard lớp

`make dashboard` rồi mở http://127.0.0.1:8501 (tab Lớp / Giáo viên / Pipeline; `?tab=Pipeline` mở thẳng một tab). Chỉ đọc Redis, tự làm mới mỗi 2 s. Mọi số cấp lớp/trường qua ngưỡng k = 5: < 5 hiện "<5", còn lại làm tròn tới bội số của 5; tổng có ô "<5" hiện "ẩn". Chế độ giáo viên cần `NHOM8_TEACHER_MODE=1` và `TEACHER_PASSWORD`. Xem một run thử: `NHOM8_DASHBOARD_KEY_PREFIX=<prefix của job> make dashboard`.

### Trang Học sinh

`make student` rồi mở http://127.0.0.1:8502: đóng vai một người học (bút danh `L-…` ngẫu nhiên, chọn lớp có trong roster), chọn bài của tập test, nhập từng bước và được kiểm bằng cùng bộ kiểm SymPy của dịch vụ gợi ý; bước sai không lộ bước đúng; nút "Xin gợi ý" gọi `POST /hints`. Mỗi thao tác là một sự kiện LearnerEvent gửi vào Kafka, nên job streaming, dịch vụ gợi ý (`state_source: redis`) và dashboard đều thấy người học này. Nối với demo Docker: `NHOM8_STUDENT_EVENTS_TOPIC=demo-learner-events NHOM8_STUDENT_KEY_PREFIX=demo: make student` (hoặc dùng service `student` của `make demo`).

## Demo bằng Docker

Profile `app` của `docker-compose.yml` chạy job Spark, dịch vụ gợi ý, dashboard và trang Học sinh trong container. Các thành phần dùng chung image `docker/app.Dockerfile` (JDK 17 + PySpark), nối Kafka qua listener nội bộ `kafka:29092`. Một bộ dữ liệu mô phỏng nhỏ (350 người học, 10 lớp, 100 sự kiện/giây, 10 phút) được phát lại với cùng W và cùng run id với job.

```bash
cp .env.example .env     # đặt REDIS_PASSWORD và NHOM8_HINT_API_KEY ngẫu nhiên; tùy chọn NHOM8_TEACHER_MODE=1 + TEACHER_PASSWORD
make demo                # docker compose --profile app up --build
# dashboard: http://127.0.0.1:8501   học sinh: http://127.0.0.1:8502   dịch vụ gợi ý: http://127.0.0.1:8000 (chỉ 127.0.0.1)
make demo-down           # dừng các container ứng dụng; Kafka, Redis và volume giữ nguyên
make demo-clean          # như demo-down, rồi xóa volume dữ liệu demo để lần sau chạy mới
```

Lần build đầu cần mạng (image nền, gói Debian, PyPI, connector Kafka của Spark). Sau đó demo chạy được khi không có mạng. Kịch bản trình diễn từng bước: [`docs/demo-script.md`](docs/demo-script.md). Các số đo thí nghiệm vẫn được đo trên host, không đo trong container.

## Tái tạo kết quả chính

Mỗi lần chạy thí nghiệm ghi một dòng vào `results/raw/runs.jsonl` qua `ai_math_tutor.run_record` (đủ trường Phụ lục B); mọi số liệu và hình trong `results/figures/` chỉ lấy từ tệp đó. Ma trận cấu hình: `configs/experiments/plan-v1.0.yaml` (v0.1 giữ làm lịch sử); quy ước thống kê: `docs/experiment-protocol.md`.

```bash
make run-exp EXP=all EXP_ARGS=--dry-run              # 115 run, trạng thái resume, ước lượng ~20,6 giờ máy
make run-exp EXP=A                                    # Exp A × 3 seed; mỗi run một dòng results/raw/runs.jsonl
make run-exp PLAN=configs/experiments/smoke.yaml      # kiểm runner (~1,5 phút)
make reproduce-small   # make up + một run streaming nhỏ từ đầu (cấu hình smoke), kết quả trong thư mục mới, in tóm tắt
```

Quy trình đánh giá chất lượng (audit gợi ý, rà soát kho bài) và báo cáo nộp riêng, không nằm trong repo này.

## Cấu trúc (theo mục 4 của hướng dẫn)

| Thư mục/tệp | Nội dung |
|---|---|
| `src/ai_math_tutor/` | `contracts/` (JSON Schema), `event_validation.py`, `math_check.py`, `safe_math.py`, `run_record.py`, `simulator/` (bộ mô phỏng), `ingestion/` (Kafka), `streaming/` (pipeline trạng thái), `batch/` (batch baseline), `hints/` (dịch vụ gợi ý), `dashboard/` (dashboard lớp), `student/` (trang Học sinh), `experiments/` (runner thí nghiệm) |
| `tests/` | Kiểm thử hợp đồng, toán học, ghi run |
| `configs/experiments/` | Ma trận thí nghiệm |
| `data_sample/` | Dữ liệu mẫu hợp lệ |
| `scripts/` | Tạo dữ liệu, dựng kho bài, đo tải gợi ý, chạy thí nghiệm |
| `results/raw/`, `results/figures/` | Kết quả thô (chỉ ghi nối) và hình sinh bằng script |
| `docs/` | Event schema, privacy note, data card, giao thức thí nghiệm, tài liệu tham khảo đã kiểm, cài đặt (`setup.md`), hướng dẫn chạy, báo cáo kỹ thuật restart và sự kiện muộn (`reports/`) |
