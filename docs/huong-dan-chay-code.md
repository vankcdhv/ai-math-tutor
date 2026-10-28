# Hướng dẫn chạy code

Mọi lệnh chạy trong shell Linux tại thư mục repo. Trên máy thí nghiệm Windows, shell đó là WSL2 Ubuntu 24.04; trên Linux thuần, chạy thẳng. Cài đặt chi tiết và các bẫy môi trường: `docs/setup.md`. Danh sách target: `make help`.

## 0. Cách gọi lệnh

| Môi trường | Cách gọi |
|---|---|
| Windows, lệnh đơn giản (không có `$`, dấu nháy, dấu ngoặc) | PowerShell: `wsl -d Ubuntu-24.04 -- bash /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor/scripts/in-wsl.sh make check` |
| Windows, lệnh có `$`, nháy, ngoặc (đa số lệnh dưới đây) | Mở shell WSL bằng `wsl -d Ubuntu-24.04`, chạy khối "chuẩn bị shell" bên dưới, rồi gõ lệnh trong shell đó |
| Linux thuần | `cd <repo>`. uv dùng `.venv` trong repo; `NHOM8_DATA_ROOT` mặc định là `~/nhom8-data` (`Makefile`, `config.py`) |

Chuẩn bị shell WSL (đặt đúng những gì `scripts/in-wsl.sh` đặt):

```bash
export UV_PROJECT_ENVIRONMENT="$HOME/.venvs/nhom8" NHOM8_DATA_ROOT="$HOME/nhom8-data"
export PATH="$HOME/.local/bin:$PATH"
mkdir -p "$NHOM8_DATA_ROOT"
cd /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor
```

- venv và dữ liệu nóng (checkpoint, RocksDB, lake, log mô phỏng) phải nằm trên ext4 của WSL, không trên `/mnt/d` (I/O qua 9P chậm).
- `"$NHOM8_DATA_ROOT"` trong các lệnh dưới đây do shell mở rộng **trước** khi gọi `make`, nên biến phải được export ở shell.
- Giữ cửa sổ WSL này mở suốt phiên làm việc. Khi không còn tiến trình nào, WSL tắt distro, kéo Kafka/Redis dừng theo (`docs/setup.md`, mục "WSL tự tắt distro khi rảnh").

## 1. Cài đặt (một lần)

Cần: JDK 17, uv, Docker Engine + Compose, `make`; trên Windows thêm `.wslconfig` giới hạn RAM/CPU cho WSL. Các bước cài: `docs/setup.md` mục 1.

```bash
uv sync          # cài phụ thuộc theo uv.lock (PySpark 4.1.3, confluent-kafka, FastAPI, Streamlit, ...)
java -version    # openjdk version "17..."
```

Lần đầu tạo SparkSession, connector Kafka (~63 MB) được tải từ Maven Central vào `~/.ivy2.5.2`, nên cần mạng một lần.

## 2. Tệp `.env`

```bash
cp .env.example .env
python3 -c "import secrets; print(secrets.token_urlsafe(24))"   # chạy hai lần, lấy hai giá trị
```

Mở `.env`, thay hai dòng sau bằng hai giá trị vừa sinh:

| Biến | Dùng cho | Bắt buộc |
|---|---|---|
| `REDIS_PASSWORD` | Redis `requirepass`; mọi thành phần Python | Có (`make up` dừng nếu thiếu) |
| `NHOM8_HINT_API_KEY` | header `X-API-Key` của dịch vụ gợi ý, ≥ 16 ký tự | Có cho `make hint-api`. Giá trị mẫu trong `.env.example` đủ dài để chạy, nên phải tự thay |
| `NHOM8_TEACHER_MODE=1` + `TEACHER_PASSWORD` | chế độ giáo viên của dashboard | Không (mặc định tắt) |
| `NHOM8_DASHBOARD_KEY_PREFIX` | xem một run có tiền tố khóa Redis riêng | Không |

`.env` đã nằm trong `.gitignore`. Thứ tự ưu tiên cấu hình: biến môi trường > `.env` > `configs/runtime.yaml`.

## 3. Hạ tầng và kiểm tra nhanh

```bash
make up        # Kafka KRaft 127.0.0.1:9092 + Redis 127.0.0.1:6379, chờ healthcheck
make check     # JDK ≥ 17, pytest -m "not integration", validator dữ liệu mẫu, kiểm cổng compose
make ps        # trạng thái hai service kafka, redis của project Compose nhom8
make topics    # tạo learner-events (3 partition theo runtime.yaml), alerts, dead-letter, hint-requests
```

Kết quả mong đợi:

- `make up`: hai container ở trạng thái healthy.
- `make check`: dòng `java OK: openjdk version "17...`; pytest kết thúc không có `failed`; validator in JSON có `"ok": true`; `tests/test_compose_bindings.py` passed.
- `make topics`: JSON liệt kê từng topic được tạo hoặc giữ nguyên. Broker tắt tự tạo topic, nên topic phải tạo trước mọi lần phát lại.

Kiểm riêng dữ liệu mẫu tuần 1 (20 dòng, 19 sự kiện duy nhất, 1 trùng, 1 muộn):

```bash
uv run python -m ai_math_tutor.event_validation data_sample/events-week1-sample.jsonl \
  --problems data_sample/problems-week1-sample.jsonl
```

Spike Spark TWS + Kafka (vài phút, kết quả dưới `$NHOM8_DATA_ROOT/spike/`): `make spike`.

## 4. Sinh dữ liệu mô phỏng

```bash
# 1K người học × 1 ngày, nhịp tự nhiên: đầu vào mặc định của make baseline
uv run python -m ai_math_tutor.simulator.cli --learners 1000 --days 1 --seed 11 \
  --out "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11"

# log ngắn tốc độ cố định cho phát lại + streaming: 350 người học × 3 phút ở 100 ev/s
uv run python -m ai_math_tutor.simulator.cli --learners 350 --minutes 3 --target-rate 100 --seed 11 \
  --late-profile low --out "$NHOM8_DATA_ROOT/sim/demo-350-r100"
```

- Lệnh in JSON tóm tắt: `out`, `counts` (`learners`, `sessions`, `unique_events`, `deliveries`, `duplicate_deliveries`, `late_events`), `rates`, `stuck_episodes`, `generation_seconds`. Run 1K × 1 ngày seed 11 cho 73.212 sự kiện duy nhất, 74.680 lần giao, sinh trong khoảng 2 s.
- Thư mục `--out` có `events.parquet`, `ground_truth.jsonl`, `roster.json`, `manifest.json`. Cùng seed cho cùng checksum. Thư mục đã có dữ liệu thì cần `--overwrite`.
- Tham số khác: `--late-profile low|high`, `--duplicate-rate` (mặc định 0,02), `--cellular-share` (0,4), `--class-size` (35), `--problems`, `--split dev|test|all`.

## 5. Kafka: phát lại và đối soát topic

```bash
make topics PARTITIONS=3 TOPICS_ARGS=--recreate     # learner-events rỗng (bắt buộc khi đổi số partition)
make replay EVENTS="$NHOM8_DATA_ROOT/sim/demo-350-r100/events.parquet" PROCESSES=2 DURATION=60 \
  REPLAY_ARGS="--rate-check --run-id ingest-demo-1 --no-run-record"
make verify-topic REPLAY_JSON=results/raw/replay-ingest-demo-1.json
```

- `make replay` in JSON có `"status": "success"`, `rate_ratio` ≈ 1,0, `delivery_errors: 0` và `result_path`. `--rate-check` đánh dấu run hỏng nếu tốc độ gửi thực < 95% mục tiêu.
- `make verify-topic` in JSON có `"ok": true` và ghi `verify-<run>.json` cạnh tệp replay: không mất, không trùng thêm, đúng byte, đúng thứ tự từng người học, heartbeat vượt cuối log + W ở mọi partition.
- `--no-run-record` giữ `results/raw/runs.jsonl` chỉ cho các lần đo chính thức. Bỏ cờ này khi đo thật.
- Topic phải rỗng trước mỗi lần phát lại; nếu không, `verify-topic` báo bản ghi ngoài dự kiến.

## 6. Streaming: job, phát lại, đối soát

Cần hai shell WSL đã chuẩn bị như mục 0. Ví dụ dùng run `demo1` và tiền tố khóa Redis `demo1:`, nên không đụng khóa của run khác trên cùng Redis.

**Shell 1: chuẩn bị và chạy job.**

```bash
make topics PARTITIONS=3 TOPICS_ARGS=--recreate
make stream RUN_ID=demo1 STREAM_ARGS="--mode eager --watermark 30s --learner-model bkt --redis-key-prefix demo1:"
```

- Job ghi `results/raw/demo1/job.json` và `progress.jsonl` (một dòng `started`, rồi một dòng mỗi micro-batch); checkpoint ở `$NHOM8_DATA_ROOT/checkpoints/demo1`, lake ở `$NHOM8_DATA_ROOT/lake/demo1`.
- Tham số: `--mode eager|ordered|none`, `--watermark` (mặc định 30 s), `--dedup on|off`, `--trigger asap|"N seconds"`, `--learner-model rule|bkt` (mặc định `rule`), `--run-seconds`, `--max-offsets-per-trigger`.
- Chạy lại cùng `RUN_ID` là chạy tiếp từ checkpoint. Đổi `mode`, `learner_model`, `dedup`, `watermark` hay số shuffle partition trên checkpoint cũ thì job dừng ngay với lỗi "không chạy tiếp checkpoint …": dùng `RUN_ID` mới hoặc `make clean-state RUN_ID=demo1`.

**Shell 2: phát lại khi job đã có dòng `started`.**

```bash
grep -m1 '"started"' results/raw/demo1/progress.jsonl   # in ra một dòng khi query trạng thái đã chạy
make replay EVENTS="$NHOM8_DATA_ROOT/sim/demo-350-r100/events.parquet" PROCESSES=2 \
  REPLAY_ARGS="--run-id demo1 --watermark-s 30 --no-run-record"
```

- `--run-id demo1` để `reconcile` tự tìm `results/raw/replay-demo1.json`.
- `--watermark-s` của bộ phát lại **phải bằng** `--watermark` của job. Mặc định hai bên khác nhau (bộ phát lại 120 s, job 30 s). Khi đó heartbeat có `event_time` đi trước timestamp Kafka của nó gần 121 s, vượt mức job cho phép với heartbeat (W + 60 s = 90 s), nên bị đưa sang `dead-letter` với lý do `future_event_time` và watermark không vượt cuối log (`job.parse_events`, `docs/event-schema.md` mục Dead-letter).

**Dừng job và đối soát.** Khi bộ phát lại in kết quả xong và `progress.jsonl` có thêm vài batch, nhấn Ctrl-C ở shell 1 (job chờ hết micro-batch đang chạy rồi mới dừng). Sau đó:

```bash
make reconcile RUN_ID=demo1 SIM_DIR="$NHOM8_DATA_ROOT/sim/demo-350-r100" MODEL=bkt \
  RECONCILE_ARGS="--key-prefix demo1:"
```

- In JSON `{path, status, dropped, state_accuracy, checks}` và ghi `results/raw/demo1/reconcile.json`.
- `status` là `ok` khi đã phát hết cửa sổ. Mọi mục trong `checks` phải là `true`: `no_event_applied_twice`, `no_duplicate_alert_in_stream`, `lake_alerts_match_stream`, `watermark_past_end_of_log`, `applied_subset_of_window`.
- `state_accuracy` = 1,0 ở chế độ `ordered`. Ở `eager` có thể < 1,0, vì sự kiện đến muộn được áp ngay, sai thứ tự (`docs/reports/late-events.md`).
- `MODEL` phải khớp `--learner-model` của job.

**Tab Lớp của dashboard cần roster.** Tổng hợp lớp (`agg:*`, `stuck:set:*`) chỉ được tính cho lớp có `roster:{class}`. Runner thí nghiệm tự nạp roster; khi chạy tay, nạp bằng `streaming.sinks.load_roster` **trước** khi bắt đầu phát lại:

```bash
uv run python - "$NHOM8_DATA_ROOT/sim/demo-350-r100/roster.json" demo1: <<'PY'
import json, sys
import redis
from ai_math_tutor.config import load_config
from ai_math_tutor.streaming.sinks import RedisKeys, load_roster
client = redis.Redis.from_url(load_config().redis.url)
roster = json.loads(open(sys.argv[1], encoding="utf-8").read())
print(load_roster(client, roster, RedisKeys(sys.argv[2])), "người học đã nạp")
PY
```

Restart và sự kiện muộn có test riêng (khoảng 5 phút, cần `make up`): `uv run pytest -q -s -m integration tests/integration/test_restart.py`. Kết quả tham chiếu: `docs/reports/checkpoint-restart.md`, `docs/reports/late-events.md`.

## 7. Batch baseline (DataFrame và MapReduce)

```bash
make baseline     # daily + hourly trên $NHOM8_DATA_ROOT/sim-bench/day-1k-s11, có --parity-check
make baseline BATCH_DATA="$NHOM8_DATA_ROOT/sim/demo-350-r100"   # thư mục khác có events.parquet + ground_truth.jsonl

uv run python -m ai_math_tutor.batch.run_batch_baseline --schedule hourly --impl rdd \
  --events "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/events.parquet" \
  --ground-truth "$NHOM8_DATA_ROOT/sim-bench/day-1k-s11/ground_truth.jsonl"
```

- Mỗi lần chạy in một dòng JSON `{run_id, status, raw_dir, metrics}`, ghi `results/raw/batch/<run_id>/` (`jobs.csv`, `episodes.csv`, `false_positives.csv`, `summary.json`), snapshot Parquet dưới `$NHOM8_DATA_ROOT/batch/<run_id>/`, và nối một run record vào `results/raw/runs.jsonl`.
- `--parity-check` so bảng hồ sơ của DataFrame và RDD ở cutoff cuối; trên log 1K × 1 ngày hai đường cho cùng 5.882 dòng.
- Tùy chọn: `--impl dataframe|rdd`, `--phase-s 0` (chạy đúng 00:00; mặc định pha lấy từ seed), `--replay-start-ms <epoch ms>` (đóng dấu lại như bộ phát lại, dịch ground truth), `--partitions N`, `--timeout-s`.
- Thời gian tham chiếu: run chậm nhất (hourly, DataFrame, 18 job) mất 105 s đầu–cuối.

## 8. Dịch vụ gợi ý

```bash
make topics                 # cần topic hint-requests cho nhật ký (hoặc HINT_ARGS=--no-audit-log)
make hint-api               # 127.0.0.1:8000; lúc khởi động kiểm toàn bộ kho bài v2 bằng SymPy (vài giây)
```

Từ shell WSL khác:

```bash
curl -s http://127.0.0.1:8000/healthz
```

Mong đợi: `"status": "ok"`, `"generator": "ladder"`, `"problems": 1260`, `"unverified_problems": 0`, `"redis": "ok"`.

Gửi một yêu cầu (thân JSON viết vào tệp để tránh lỗi quoting):

```bash
KEY="$(grep '^NHOM8_HINT_API_KEY=' .env | cut -d= -f2-)"
cat > /tmp/hint-request.json <<'JSON'
{"event_id": "8f0c2a4e-1b7d-4c3a-9e2f-5a6b7c8d9e01", "learner_id": "L-00000000000000a1",
 "class_id": "C-10A1-2026", "session_id": "S-1234abcd", "session_seq": 1,
 "problem_id": "P-NDA-001", "skill_ids": ["arithmetic.multi_step"], "step_index": 1,
 "expression": "1085 + 4 = 1089",
 "event_time": "2026-10-27T08:00:00.000Z", "sent_time": "2026-10-27T08:00:00.200Z",
 "client": {"platform": "web", "app_version": "0.1.0", "connection": "wifi"}}
JSON
curl -s -H "X-API-Key: $KEY" -H "Content-Type: application/json" \
  --data @/tmp/hint-request.json http://127.0.0.1:8000/hints
```

- Phản hồi 200 có `hint_level`, `text_vi`, `math`, `verified`, `misconception`, `source`, `state_source`.
- Learner này chưa có trạng thái trong Redis, nên dịch vụ fail-closed: `"hint_level": 1`, `"state_source": "default"`, `"verified": true`. Biểu thức `1085 + 4 = 1089` là lỗi đã khai ở bước 1 của bài `P-NDA-001` (kiểu `wrong_operation`).
- Khi dịch vụ chạy với tiền tố khóa của một run streaming (`HINT_ARGS="--key-prefix demo1:"`) và dùng `learner_id`, `problem_id`, `skill_ids` có trong run đó, `state_source` là `redis`.
- Mã lỗi: 401 sai/thiếu khóa; 413 thân > 4 KB; 422 sai phong bì, bài không có trong kho hoặc `skill_ids` khác kho; 429 vượt 10 yêu cầu tức thời / 60 yêu cầu mỗi phút cho một `learner_id`, hoặc hàng đợi SymPy đầy.

Đo tải (tự mở dịch vụ riêng ở cổng 8765 với fixture trạng thái, chạy Locust headless, rồi dọn):

```bash
make hint-load LOAD_ARGS="--users 1,10,50,100 --duration-s 60"            # ghi runs.jsonl + results/raw/hint-load-<tag>.json
make hint-load LOAD_ARGS="--users 10 --duration-s 20 --no-run-record"     # chạy thử, không ghi runs.jsonl
```

Tùy chọn khác: `--attack-fraction 0.1` (trộn biểu thức tấn công), `--no-start-jitter` (dồn cục), `--hint-generator template|ladder`.

## 9. Dashboard lớp

```bash
make dashboard                                         # http://127.0.0.1:8501
NHOM8_DASHBOARD_KEY_PREFIX=demo1: make dashboard        # xem run có tiền tố demo1:
```

- Ba tab Lớp / Giáo viên / Pipeline; `http://127.0.0.1:8501/?tab=Pipeline` mở thẳng tab Pipeline. Mỗi tab tự làm mới 2 s, chỉ đọc Redis.
- Trình duyệt trên Windows mở được địa chỉ này khi dashboard chạy trong WSL. Máy khác trong mạng thì không, vì cổng chỉ nghe `127.0.0.1`; app từ chối chạy nếu `server.address` không phải loopback.
- Số cấp lớp qua ngưỡng k = 5: < 5 hiện "<5", còn lại làm tròn tới bội số của 5, tổng có ô "<5" hiện "ẩn".
- Tab Giáo viên cần `NHOM8_TEACHER_MODE=1` và `TEACHER_PASSWORD` trong `.env`.

## 10. Thí nghiệm

Runner chạy mỗi run theo cùng vòng đời (dataset, reset, khởi động job, phát lại, drain, dừng job, đối soát, metrics) và ghi đúng một dòng vào `results/raw/runs.jsonl`. Một máy chỉ chạy được một runner (khóa `flock`). Khi ma trận đang chạy, không chạy job, phát lại hay `make down`/`make clean-state` (không có `RUN_ID`) trên cùng máy: chúng tranh CPU với phép đo hoặc xóa dữ liệu của run đang chạy (`docs/experiment-protocol.md`, mục "Môi trường đo").

```bash
make run-exp EXP=all EXP_ARGS=--dry-run          # kế hoạch, trạng thái resume, ước lượng giờ máy; không chạy gì
make run-exp PLAN=configs/experiments/smoke.yaml # một run streaming nhỏ để kiểm runner (~1,5 phút)
make run-exp EXP=A                               # Exp A × 3 seed
make run-exp EXP=D EXP_ARGS="--only ordered"     # chỉ cấu hình có slug chứa "ordered"
make reproduce-small                             # make up + một run smoke từ đầu, kết quả dưới $NHOM8_DATA_ROOT/reproduce-small/<thời điểm>/
```

- `make reproduce-small` dùng runner với `configs/experiments/smoke.yaml` (đổi bằng `REPRO_PLAN=...`), ghi vào một thư mục mới nên không bị bỏ qua vì run cũ đã `success` và không chạm `results/raw/`. Cuối lệnh in tóm tắt: `status`, SLO, thông lượng, `checks` của đối soát, số cảnh báo; thoát 1 nếu run không `success`. Lệnh dùng chung khóa runner, nên không chạy được khi ma trận đang chạy.

- `--dry-run` in bảng (thí nghiệm, loại, seed, trạng thái, số lần thử, phút ước lượng, slug cấu hình) rồi một JSON có `estimate`. Với `plan-v1.0.yaml` chưa chạy run nào: 115 run, ước lượng khoảng 20,6 giờ máy.
- `EXP` nhận `A`, `B`, `C`, `D`, `E`, `ablations`, `stress`, `soak`, `ceiling`, `all`, hoặc nhiều giá trị cách nhau bằng dấu phẩy. Run đã `success` được bỏ qua; chạy lại cùng lệnh là chạy tiếp.
- `EXP_ARGS` khác: `--archive-dir /mnt/<hdd>/int4418-archive` (chuyển lake/log thô sang HDD thay vì xóa), `--keep-lake`, `--repeats N`, `--max-attempts`, `--redis-db` (mặc định 9, không được là 0), `--disk-threshold-gb` (mặc định 10).
- Chạy cả ma trận qua đêm, trong một cửa sổ WSL để mở suốt thời gian chạy:
  ```bash
  mkdir -p "$NHOM8_DATA_ROOT/exp-runner"
  nohup bash scripts/run-experiment.sh all --archive-dir /mnt/<hdd>/int4418-archive \
    > "$NHOM8_DATA_ROOT/exp-runner/matrix.log" 2>&1 &
  ```
- Kết quả mỗi run: một dòng trong `results/raw/runs.jsonl`; thư mục `results/raw/<run_id>/` có `reconcile.json`, `job.json`, `replay-<run_id>.json`, `progress.jsonl`, `monitor.jsonl` và log nén. Spec bị bỏ sau 3 lần thử được ghi vào `results/raw/experiment-abandoned.jsonl`.
- Runner bị dừng bằng Ctrl-C/SIGTERM ghi bản ghi `aborted` cho run dở và dọn tiến trình con. Bị SIGKILL thì lần khởi động sau tự dọn tiến trình mồ côi, topic, checkpoint, lake và Redis của run dở.

## 11. Bộ gợi ý offline của tập test

```bash
make hint-audit-set                    # 18.000 gợi ý offline của tập test → $NHOM8_DATA_ROOT/hints/
```

Bộ này là đầu vào cho đánh giá chất lượng gợi ý. Quy trình đánh giá (rubric, phiếu chấm, tổng hợp) và kết quả nộp riêng, không nằm trong repo này; định nghĩa cố định trước khi chấm ở `docs/experiment-protocol.md` mục 1.

## 12. Dựng lại kho bài

Kho bài đã có sẵn trong `data_sample/` (`problem-bank-v2.jsonl`, manifest, `problem-index-v2.json`). Chỉ dựng lại khi đổi cách sinh lời giải.

```bash
make numina-candidates   # tải NuminaMath-CoT ở revision ghim (cần mạng), kiểm sha256 → $NHOM8_DATA_ROOT/numina/
make problem-bank        # lời giải từng bước → kho v2 + manifest + chỉ mục
```

- Dựng lại từ cùng đầu vào cho cùng byte. Sau `make problem-bank`, `git status` không được thấy thay đổi ở `data_sample/problem-bank-v2*`; nếu có thay đổi, checksum tập test trong manifest không còn khớp mẫu đã rà soát.
- Thời gian tham chiếu: tách ứng viên 60,5 s (gồm tải 39,7 s), viết lời giải 21,1 s, dựng kho 2,9 s.

## 13. Kiểm thử

```bash
make check                                                     # unit test + validator + kiểm cổng compose
uv run pytest -q -m integration                                # test cần Kafka/Redis thật (sau make up)
uv run pytest -q -s -m integration tests/integration/test_restart.py   # restart + sự kiện muộn (~5 phút)
```

Test tích hợp dùng topic và tiền tố khóa Redis riêng, chỉ xóa khóa của mình.

## 14. Dọn dẹp

```bash
# Ctrl-C ở shell của make stream / make hint-api / make dashboard để dừng từng tiến trình
make clean-state RUN_ID=demo1                       # xóa checkpoint + lake của một run
uv run python -m ai_math_tutor.ingestion.topics delete --all     # xóa bốn topic của dự án
docker compose exec redis sh -c 'redis-cli --scan --pattern "demo1:*" | xargs -r redis-cli del'   # khóa của run demo1
make down                                           # dừng stack, giữ volume Kafka
docker compose down -v                              # dừng và xóa hẳn volume Kafka
bash scripts/disk-check.sh 10                       # thoát 1 nếu ổ chứa NHOM8_DATA_ROOT trống < 10 GB
```

- `make clean-state` không có `RUN_ID` xóa toàn bộ `checkpoints/`, `lake/`, `spike/` dưới `$NHOM8_DATA_ROOT`.
- `topics delete --all` chỉ xóa `learner-events`, `alerts`, `dead-letter`, `hint-requests`. Topic `exp-<run_id>*` của runner do runner tự xóa.
- Kiểm tiến trình còn sót: `ss -ltnp | grep -E ':(8000|8501|8765)\b'` và `pgrep -af "ai_math_tutor|streamlit"`. Chỉ dừng tiến trình do chính mình mở.

## 15. Lỗi thường gặp

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| Lệnh gõ từ PowerShell chạy khác lệnh đã gõ (thư mục lạ, biến rỗng) | PowerShell và `wsl` xử lý lại `$`, nháy, ngoặc. Chỉ gọi lệnh đơn giản qua `in-wsl.sh`; lệnh phức tạp gõ trong shell WSL hoặc viết vào tệp `.sh` (xuống dòng LF). Chạy xong kiểm `git status` |
| Kafka/Redis dừng giữa chừng; `Connection to node -1 ... could not be established` ngay sau khi mở WSL | WSL tắt distro khi rảnh, dockerd dừng theo, rồi tự khởi động lại ở lần gọi sau. Giữ một cửa sổ WSL mở; chạy `make up` (idempotent, chờ healthy) trước mỗi phiên |
| `Thiếu .env — chạy: cp .env.example .env ...` | Chưa tạo `.env` (mục 2) |
| `Thiếu REDIS_PASSWORD (đặt trong .env hoặc biến môi trường)` | `.env` thiếu dòng `REDIS_PASSWORD`, hoặc lệnh chạy ngoài thư mục repo nên không thấy `.env` |
| `NHOM8_HINT_API_KEY phải có ít nhất 16 ký tự (đặt trong .env)` | Dịch vụ gợi ý từ chối khởi động khi khóa thiếu hoặc ngắn |
| `curl` trả `{"error": "unauthorized"}` | Sai hoặc thiếu header `X-API-Key` |
| `Bind for 127.0.0.1:9092 failed: port is already allocated` | Container/tiến trình cũ còn giữ cổng: `docker ps --filter publish=9092`, `ss -ltnp 'sport = :9092'`. Dừng tiến trình cũ của chính mình; không đổi cổng |
| Cổng 8765 bận khi chạy `make hint-load` | Runner (Exp E) cũng dùng 8765. Không đo tải gợi ý khi ma trận đang chạy |
| Đường dẫn dạng `/sim/...` hoặc `/events.parquet` không tồn tại | `NHOM8_DATA_ROOT` chưa export trong shell (mục 0) |
| uv tạo `.venv` trong `/mnt/d/...`, cài rất chậm | `UV_PROJECT_ENVIRONMENT` chưa đặt (mục 0) |
| `Failed to find data source: kafka` | Máy chưa từng tải được connector từ Maven Central, hoặc SparkSession không tạo qua `ai_math_tutor.config` |
| `UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES` | TWS chạy trên HDFS state store; phải là RocksDB (`configs/runtime.yaml`) |
| Job dừng ngay với "không chạy tiếp checkpoint …" | Tham số khác checkpoint cũ cùng `RUN_ID`. Dùng `RUN_ID` mới hoặc `make clean-state RUN_ID=...` |
| `make topics` báo lệch số partition | Topic đã có với số partition khác: `TOPICS_ARGS=--recreate` (xóa dữ liệu của topic) |
| `reconcile` có `watermark_past_end_of_log: false`; topic `dead-letter` có heartbeat với lý do `future_event_time` | `--watermark-s` của bộ phát lại khác `--watermark` của job (mục 6) |
| `verify-topic` báo bản ghi ngoài dự kiến | Topic còn dữ liệu của lần phát trước: tạo lại bằng `TOPICS_ARGS=--recreate` |
| Tab Lớp trống dù job đang chạy | Chưa nạp `roster:{class}` (mục 6), hoặc `NHOM8_DASHBOARD_KEY_PREFIX` khác `--redis-key-prefix` của job |
| Gợi ý luôn `state_source: default` | Không có `state:`/`progress:` cho learner đó dưới tiền tố đang dùng (`--key-prefix` của `make hint-api` phải bằng tiền tố của job), hoặc Redis không nối được. Đây là hành vi fail-closed, không phải lỗi dịch vụ |
| Trình duyệt Windows không mở được `127.0.0.1:8501` | Dashboard chưa chạy hoặc đã thoát; kiểm `ss -ltnp | grep 8501` trong WSL |
| `disk-check.sh` thoát 1, Spark chậm | SSD gần đầy hoặc dữ liệu nằm trên `/mnt/*`: kiểm `NHOM8_DATA_ROOT`, chạy `make clean-state` |

## 16. Demo bằng Docker

Profile `app` của `docker-compose.yml` chạy job Spark, dịch vụ gợi ý và dashboard trong container. Ba thành phần dùng chung image `docker/app.Dockerfile` (Python 3.12, JDK 17, PySpark theo `uv.lock`). Trong container, `NHOM8_ENV=container` làm Kafka đi qua listener nội bộ `kafka:29092` và Redis đi qua tên service `redis`. Phía host chỉ publish `127.0.0.1:8000` (dịch vụ gợi ý) và `127.0.0.1:8501` (dashboard).

```bash
make demo           # docker compose --profile app up --build (log ở cửa sổ này)
make demo-down      # dừng + xóa container ứng dụng; Kafka, Redis, volume giữ nguyên
make demo-clean     # như demo-down, rồi xóa volume nhom8_demo-data (không bao giờ xóa kafka-data)
```

`.env` cần `REDIS_PASSWORD` và `NHOM8_HINT_API_KEY`. Thiếu một trong hai thì Compose dừng với thông báo. Tab Giáo viên cần thêm `NHOM8_TEACHER_MODE=1` và `TEACHER_PASSWORD`.

Thứ tự khởi động:

| Service | Việc làm |
|---|---|
| `demo-setup` (one-shot) | Tạo topic `demo-learner-events`, `demo-dead-letter`, `demo-alerts` (và `hint-requests` nếu chưa có); sinh 350 người học × 10 lớp × 100 sự kiện/giây × 10 phút; nạp roster dưới tiền tố `demo:` |
| `streaming` | Job run `demo`, W = 30 s, mô hình `rule`; checkpoint, lake và kết quả trong volume `/data` |
| `demo-replay` (one-shot) | Chạy khi `streaming` healthy; phát lại với cùng W và run id |
| `hint-api` | Đọc khóa `demo:` |
| `dashboard` | Đọc khóa `demo:` |

Tham số nằm trong khối `x-demo-env` của `docker-compose.yml`.

- Lần đầu chạy (chưa có checkpoint của run `demo`), các topic của demo được tạo lại rỗng và khóa `demo:*` trong Redis bị xóa. Khởi động lại container thì giữ nguyên mọi thứ, job chạy tiếp từ checkpoint.
- Một run chỉ được phát lại một lần. Nếu lần phát lại bị ngắt giữa chừng, `demo-replay` từ chối phát lại; chạy `make demo-clean` rồi `make demo`.
- Lần build đầu cần mạng (image nền, gói Debian, PyPI, connector Kafka của Spark, được tải sẵn vào image). Sau đó demo chạy được khi không có mạng.
- Các số đo trong báo cáo vẫn đo trên host theo các mục trên, không đo trong container.
- Ctrl-C ở cửa sổ `make demo` dừng mọi container của project, kể cả Kafka/Redis. Muốn giữ hạ tầng thì dùng `make demo-down`.

Kịch bản trình diễn 5–7 phút: `docs/demo-script.md`.
