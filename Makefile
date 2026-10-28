# Lệnh chung của dự án. Chạy trong shell Linux/macOS — trên máy Windows chạy bên trong
# WSL Ubuntu: wsl -d Ubuntu-24.04 -- bash scripts/in-wsl.sh make check (xem docs/setup.md).
SHELL := bash
.SHELLFLAGS := -euo pipefail -c
.DEFAULT_GOAL := help

# Dữ liệu nóng (checkpoint, lake, spike) nằm trên ext4/SSD, không trong repo.
export NHOM8_DATA_ROOT ?= $(HOME)/nhom8-data
UV ?= uv
COMPOSE ?= docker compose
JAVA_MIN_MAJOR := 17
SAMPLE_EVENTS := data_sample/events-week1-sample.jsonl
SAMPLE_PROBLEMS := data_sample/problems-week1-sample.jsonl
# Tham số spike: make spike SPIKE_ARGS="--rates 100,1000 --shuffle-partitions 3,200"
SPIKE_ARGS ?=
# clean-state xóa một run (RUN_ID=...) hoặc toàn bộ checkpoint/lake khi bỏ trống.
RUN_ID ?=

.PHONY: help up down ps check check-java check-compose spike clean-state

# `help` và `problem-bank` là luật hai dấu hai chấm: tệp nạp tùy chọn ở cuối Makefile có thể nối thêm bước.
help::
	@echo "make up | down | ps | check | spike [SPIKE_ARGS=...] | clean-state [RUN_ID=...]"
	@echo "make topics [PARTITIONS=1|3|6] [TOPICS_ARGS=--recreate] | replay EVENTS=... [PROCESSES=P] [DURATION=S] | verify-topic REPLAY_JSON=..."
	@echo "make numina-candidates | problem-bank"
	@echo "make stream RUN_ID=... [STREAM_ARGS=...] | reconcile RUN_ID=... SIM_DIR=... [MODEL=rule|bkt]"
	@echo "make hint-api [HINT_ARGS=...] | hint-load [LOAD_ARGS=...] | hint-audit-set"
	@echo "make dashboard [DASHBOARD_PORT=8501]"
	@echo "make run-exp EXP=A|B|C|D|E|ablations|stress|soak|ceiling|all [PLAN=...] [EXP_ARGS=--dry-run]"
	@echo "make baseline [BATCH_DATA=<thư mục events.parquet + ground_truth.jsonl>]"
	@echo "make mapreduce-matrix [MAPREDUCE_ARGS=--dry-run | \"--scales 10000 --configs B\" | --summarize-only]"
	@echo "make reproduce-small [REPRO_PLAN=...] | demo | demo-down | demo-clean"

.env:
	@echo "Thiếu .env — chạy: cp .env.example .env rồi đặt REDIS_PASSWORD ngẫu nhiên (docs/setup.md)" >&2
	@exit 1

up: .env
	$(COMPOSE) up -d --wait

down:
	$(COMPOSE) down

ps:
	$(COMPOSE) ps

check-java:
	@line="$$(java -version 2>&1 | head -n 1)" || { echo "Không tìm thấy java (cần JDK $(JAVA_MIN_MAJOR)+)" >&2; exit 1; }; \
	major="$$(sed -E 's/.*version "([0-9]+).*/\1/' <<<"$$line")"; \
	if ! [[ "$$major" =~ ^[0-9]+$$ ]] || (( major < $(JAVA_MIN_MAJOR) )); then \
	  echo "Cần JDK $(JAVA_MIN_MAJOR)+, đang có: $$line" >&2; exit 1; fi; \
	echo "java OK: $$line"

# Kiểm tĩnh docker-compose.yml: mọi cổng bind 127.0.0.1, image ghim digest, Redis có mật khẩu.
check-compose:
	$(UV) run pytest -q -m integration tests/test_compose_bindings.py

check: check-java
	$(UV) run pytest -q -m "not integration"
	$(UV) run python -m ai_math_tutor.event_validation $(SAMPLE_EVENTS) --problems $(SAMPLE_PROBLEMS)
	$(MAKE) --no-print-directory check-compose

spike:
	$(UV) run python scripts/spike-tws.py $(SPIKE_ARGS)

clean-state:
	@[[ -n "$(NHOM8_DATA_ROOT)" && "$(NHOM8_DATA_ROOT)" != "/" ]] || { echo "NHOM8_DATA_ROOT không hợp lệ" >&2; exit 1; }
	@if [[ -n "$(RUN_ID)" ]]; then \
	  [[ "$(RUN_ID)" =~ ^[A-Za-z0-9][A-Za-z0-9._-]*$$ && "$(RUN_ID)" != *..* ]] || { echo "RUN_ID không hợp lệ" >&2; exit 1; }; \
	  rm -rf -- "$(NHOM8_DATA_ROOT)/checkpoints/$(RUN_ID)" "$(NHOM8_DATA_ROOT)/lake/$(RUN_ID)"; \
	  echo "Đã xóa checkpoint/lake của run $(RUN_ID)"; \
	else \
	  rm -rf -- "$(NHOM8_DATA_ROOT)/checkpoints" "$(NHOM8_DATA_ROOT)/lake" "$(NHOM8_DATA_ROOT)/spike"; \
	  echo "Đã xóa checkpoints/, lake/, spike/ dưới $(NHOM8_DATA_ROOT)"; \
	fi

# --- Kafka ingestion (broker tắt tự tạo topic ⇒ chạy `make topics` trước replay) ---
# Số partition learner-events; bỏ trống = theo configs/runtime.yaml / NHOM8_KAFKA_PARTITIONS.
PARTITIONS ?=
# Ví dụ: TOPICS_ARGS=--recreate (xóa + tạo lại learner-events rỗng, bắt buộc khi đổi số partition).
TOPICS_ARGS ?=
# events.parquet của bộ mô phỏng (dưới $(NHOM8_DATA_ROOT)); PROCESSES = số tiến trình gửi.
EVENTS ?=
PROCESSES ?= 1
# Chỉ phát các dòng có replay_offset < DURATION giây; bỏ trống = cả log.
DURATION ?=
# Ví dụ: REPLAY_ARGS="--rate-check --watermark-s 120 --heartbeat-s 10"
REPLAY_ARGS ?=
REPLAY_JSON ?=
VERIFY_ARGS ?=

.PHONY: topics replay verify-topic

topics:
	$(UV) run python -m ai_math_tutor.ingestion.topics create $(if $(PARTITIONS),--partitions $(PARTITIONS)) $(TOPICS_ARGS)

replay:
	@[[ -n "$(EVENTS)" ]] || { echo "Cần EVENTS=<đường dẫn events.parquet>" >&2; exit 1; }
	$(UV) run python -m ai_math_tutor.ingestion.replay --events "$(EVENTS)" --processes $(PROCESSES) $(if $(DURATION),--duration $(DURATION)) $(REPLAY_ARGS)

verify-topic:
	@[[ -n "$(REPLAY_JSON)" ]] || { echo "Cần REPLAY_JSON=results/raw/replay-<run>.json" >&2; exit 1; }
	$(UV) run python -m ai_math_tutor.ingestion.verify_topic --replay "$(REPLAY_JSON)" $(VERIFY_ARGS)

# --- Kho bài: ứng viên NuminaMath (dưới $(NHOM8_DATA_ROOT)/numina) → lời giải từng bước → kho v2 + chỉ mục ---
.PHONY: numina-candidates problem-bank

numina-candidates:
	$(UV) run python scripts/extract-numina-subset.py

problem-bank::
	$(UV) run python scripts/write-step-solutions.py
	$(UV) run python scripts/build-problem-bank.py

# --- Streaming: make stream RUN_ID=r1 [STREAM_ARGS="--mode ordered --watermark 2m --learner-model bkt"] ---
STREAM_ARGS ?=
# Đối soát một run: make reconcile RUN_ID=… SIM_DIR=… MODEL=rule|bkt [RECONCILE_ARGS="--replay-json …"]
SIM_DIR ?=
MODEL ?= bkt
RECONCILE_ARGS ?=

.PHONY: stream reconcile

stream:
	@[[ -n "$(RUN_ID)" ]] || { echo "Cần RUN_ID=..." >&2; exit 1; }
	$(UV) run python -m ai_math_tutor.streaming.job --run-id $(RUN_ID) $(STREAM_ARGS)

reconcile:
	@[[ -n "$(RUN_ID)" && -n "$(SIM_DIR)" ]] || { echo "Cần RUN_ID=... SIM_DIR=..." >&2; exit 1; }
	$(UV) run python -m ai_math_tutor.streaming.reconcile --run-id $(RUN_ID) --sim-dir "$(SIM_DIR)" --model $(MODEL) $(RECONCILE_ARGS)

# --- Dịch vụ gợi ý (cần NHOM8_HINT_API_KEY ≥ 16 ký tự trong .env) ---
HINT_ARGS ?=
LOAD_ARGS ?=

.PHONY: hint-api hint-load hint-audit-set

hint-api:
	$(UV) run python -m ai_math_tutor.hints.app $(HINT_ARGS)

hint-load:
	$(UV) run python scripts/hint-load-test.py $(LOAD_ARGS)

hint-audit-set:
	$(UV) run python -m ai_math_tutor.hints.audit_set

# --- Dashboard lớp (chỉ đọc Redis; chỉ nghe 127.0.0.1) ---
DASHBOARD_PORT ?= 8501

.PHONY: dashboard

dashboard: .env
	$(UV) run streamlit run src/ai_math_tutor/dashboard/app.py --server.address 127.0.0.1 \
	  --server.port $(DASHBOARD_PORT) --server.headless true --browser.gatherUsageStats false

# --- Thí nghiệm (resume được; run success được bỏ qua) ---
# make run-exp EXP=A|B|C|D|E|ablations|stress|soak|ceiling|all [PLAN=…] [EXP_ARGS="--dry-run" | "--archive-dir /mnt/e/int4418-archive"]
PLAN ?= configs/experiments/plan-v1.0.yaml
EXP ?= all
EXP_ARGS ?=

.PHONY: run-exp

run-exp:
	$(UV) run python -m ai_math_tutor.experiments.runner --plan $(PLAN) --exp $(EXP) $(EXP_ARGS)

# --- Batch baseline (cùng event log với streaming) ---
BATCH_DATA ?= $(NHOM8_DATA_ROOT)/sim-bench/day-1k-s11

.PHONY: baseline

# batch daily + hourly trên log mô phỏng 1K × 1 ngày; run record vào results/raw/runs.jsonl
baseline:
	for s in daily hourly; do \
	  $(UV) run python -m ai_math_tutor.batch.run_batch_baseline --schedule $$s \
	    --events $(BATCH_DATA)/events.parquet --ground-truth $(BATCH_DATA)/ground_truth.jsonl --parity-check; \
	done

# --- MapReduce theo (learner, problem): 3 quy mô × Config A/B × 3 lần (resume; run success được bỏ qua) ---
# Minh chứng dưới results/mapreduce/, run record vào results/raw/runs.jsonl. Ví dụ: MAPREDUCE_ARGS=--dry-run
MAPREDUCE_ARGS ?=

.PHONY: mapreduce-matrix

mapreduce-matrix:
	$(UV) run python -m ai_math_tutor.experiments.mapreduce_matrix $(MAPREDUCE_ARGS)

# --- Tái lập nhỏ trên host: một run streaming đầy đủ qua runner thí nghiệm ---
# Vòng đời của runner: sinh dữ liệu mô phỏng → topic riêng của run → job (run id riêng) → phát lại cùng W
# → drain → đối soát → run record. Kết quả ghi vào thư mục mới dưới REPRO_ROOT (không chạm results/raw);
# không chạy được khi ma trận thí nghiệm đang chạy (khóa runner dùng chung).
REPRO_PLAN ?= configs/experiments/smoke.yaml
REPRO_ROOT ?= $(NHOM8_DATA_ROOT)/reproduce-small

# Tóm tắt run record cuối của runs.jsonl; thoát 1 nếu run không success.
define REPRO_SUMMARY_PY
import json, sys
lines = [line for line in open(sys.argv[1], encoding="utf-8") if line.strip()]
if not lines:
    sys.exit("runs.jsonl rỗng: runner không ghi run record nào")
record = json.loads(lines[-1])
metrics = record.get("metrics") or {}
reconcile = metrics.get("reconcile") or {}
alerts = reconcile.get("alerts") or {}
print(json.dumps({
    "run_id": record.get("run_id"), "status": record.get("status"), "runtime_s": record.get("runtime_s"),
    "slo": metrics.get("slo"), "throughput": metrics.get("throughput"),
    "reconcile_status": reconcile.get("status"), "checks": reconcile.get("checks"),
    "alerts": {key: alerts.get(key) for key in ("unique_alert_ids", "duplicate_stream_entries", "recall",
                                                "time_to_detect_system_ms")},
}, ensure_ascii=False, indent=2))
sys.exit(0 if record.get("status") == "success" else 1)
endef
export REPRO_SUMMARY_PY

.PHONY: reproduce-small

reproduce-small: up
	@out="$(REPRO_ROOT)/$$(date -u +%Y%m%dT%H%M%SZ)"; mkdir -p "$$out"; \
	echo "reproduce-small: kết quả ở $$out"; \
	rc=0; $(UV) run python -m ai_math_tutor.experiments.runner --plan $(REPRO_PLAN) --results-dir "$$out" || rc=$$?; \
	if [[ -s "$$out/runs.jsonl" ]]; then $(UV) run python -c "$$REPRO_SUMMARY_PY" "$$out/runs.jsonl" || rc=1; fi; \
	exit $$rc

# --- Demo bằng Docker: profile app (Spark job, dịch vụ gợi ý, dashboard trong container) ---
# Kịch bản trình diễn: docs/demo-script.md. Thí nghiệm vẫn đo trên host, không trong container.
APP_SERVICES := demo-setup streaming demo-replay hint-api dashboard
DEMO_VOLUME := nhom8_demo-data

.PHONY: demo demo-down demo-clean

demo: .env
	$(COMPOSE) --profile app up --build

# Dừng và xóa container của profile app; Kafka, Redis và mọi volume giữ nguyên.
demo-down:
	$(COMPOSE) --profile app rm --stop --force $(APP_SERVICES)

# Như demo-down, rồi xóa volume dữ liệu của demo (checkpoint, lake, mô phỏng) để lần sau chạy mới.
# Không bao giờ xóa volume kafka-data.
demo-clean: demo-down
	docker volume rm --force $(DEMO_VOLUME)

# Target cục bộ ngoài phạm vi pipeline (nếu có tệp).
-include report/tasks.mk
