#!/usr/bin/env bash
# Các bước của demo chạy trong container (profile `app` của docker-compose.yml):
#   setup   tạo topic, sinh dữ liệu mô phỏng, nạp roster vào Redis (one-shot, trước job)
#   stream  chạy job Spark Structured Streaming của run demo (service streaming)
#   replay  phát lại log mô phỏng vào Kafka với cùng W và run id của job (one-shot, sau khi job chạy)
#
# Tham số của demo đặt một chỗ trong docker-compose.yml (khối x-demo-env); script này bắt buộc có đủ.
# Lần đầu (chưa có checkpoint của run) là "chạy mới": topic của demo được tạo lại rỗng, khóa Redis
# dưới tiền tố của demo bị xóa. Khi checkpoint đã có (khởi động lại container), mọi thứ được giữ để
# job chạy tiếp từ checkpoint. Làm lại từ đầu: make demo-clean (xóa volume dữ liệu của demo).
set -euo pipefail

: "${DEMO_RUN_ID:?}" "${DEMO_KEY_PREFIX:?}" "${DEMO_WATERMARK_S:?}" "${DEMO_MODEL:?}"
: "${DEMO_LEARNERS:?}" "${DEMO_CLASS_SIZE:?}" "${DEMO_RATE:?}" "${DEMO_MINUTES:?}" "${DEMO_SEED:?}"
: "${DEMO_EVENTS_TOPIC:?}" "${DEMO_DEAD_LETTER_TOPIC:?}" "${DEMO_ALERTS_TOPIC:?}" "${DEMO_PARTITIONS:?}"
: "${NHOM8_DATA_ROOT:?}" "${NHOM8_RESULTS_DIR:?}"

APP_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DEMO_DIR="$NHOM8_DATA_ROOT/demo"
SIM_DIR="$DEMO_DIR/sim"
CHECKPOINT_DIR="$NHOM8_DATA_ROOT/checkpoints/$DEMO_RUN_ID"
REPLAY_JSON="$NHOM8_RESULTS_DIR/replay-$DEMO_RUN_ID.json"
REPLAY_STARTED="$DEMO_DIR/replay-$DEMO_RUN_ID.started"

log() { printf 'DEMO %s %s\n' "$(date -u +%H:%M:%S)" "$*" >&2; }
die() { log "lỗi: $*"; exit 1; }

# Tiền tố rỗng sẽ làm bước xóa khóa chạm vào mọi khóa của Redis: chỉ nhận dạng "<tên>:".
[[ "$DEMO_KEY_PREFIX" =~ ^[A-Za-z0-9._-]{1,63}:$ ]] || die "DEMO_KEY_PREFIX phải có dạng <tên>: (nhận '$DEMO_KEY_PREFIX')"

ensure_topics() {
  # hint-requests: nhật ký của dịch vụ gợi ý (topic dùng chung của dự án, chỉ ghi nối, không tạo lại).
  python -m ai_math_tutor.ingestion.topics create --topic hint-requests --partitions "$DEMO_PARTITIONS"
  # Topic riêng của demo, để không đụng learner-events/alerts/dead-letter của các lần chạy trên host.
  local topic
  for topic in "$DEMO_EVENTS_TOPIC" "$DEMO_DEAD_LETTER_TOPIC" "$DEMO_ALERTS_TOPIC"; do
    python -m ai_math_tutor.ingestion.topics create --topic "$topic" --partitions "$DEMO_PARTITIONS" "$@"
  done
}

reset_redis_prefix() {
  python - "$DEMO_KEY_PREFIX" <<'PY'
import sys

import redis

from ai_math_tutor.config import load_config
from ai_math_tutor.experiments.runner import delete_prefix

prefix = sys.argv[1]
client = redis.Redis.from_url(load_config().redis.url)
print(f"DEMO đã xóa {delete_prefix(client, prefix)} khóa {prefix}*", file=sys.stderr)
PY
}

load_roster() {
  python - "$SIM_DIR/roster.json" "$DEMO_KEY_PREFIX" <<'PY'
import json
import sys
from pathlib import Path

import redis

from ai_math_tutor.config import load_config
from ai_math_tutor.streaming.sinks import RedisKeys, load_roster

roster = json.loads(Path(sys.argv[1]).read_text(encoding="utf-8"))
client = redis.Redis.from_url(load_config().redis.url)
learners = load_roster(client, roster, RedisKeys(sys.argv[2]))
print(f"DEMO roster: {learners} người học, {len(roster)} lớp", file=sys.stderr)
PY
}

simulate() {
  # manifest.json được ghi sau cùng: thiếu nó nghĩa là lần sinh trước chưa xong ⇒ sinh lại (--overwrite).
  if [[ -f "$SIM_DIR/manifest.json" ]]; then
    log "dữ liệu mô phỏng đã có: $SIM_DIR"
    return
  fi
  log "sinh dữ liệu mô phỏng: $DEMO_LEARNERS người học, lớp $DEMO_CLASS_SIZE, $DEMO_RATE ev/s, $DEMO_MINUTES phút"
  python -m ai_math_tutor.simulator.cli --learners "$DEMO_LEARNERS" --class-size "$DEMO_CLASS_SIZE" \
    --minutes "$DEMO_MINUTES" --target-rate "$DEMO_RATE" --seed "$DEMO_SEED" --late-profile low \
    --problems "$APP_ROOT/data_sample/problem-bank-v2.jsonl" --split test --out "$SIM_DIR" --overwrite
}

setup() {
  mkdir -p "$DEMO_DIR" "$NHOM8_RESULTS_DIR"
  if [[ -d "$CHECKPOINT_DIR" ]]; then
    log "checkpoint $CHECKPOINT_DIR đã có: giữ topic và khóa Redis để job chạy tiếp"
    ensure_topics
  else
    log "chạy mới run $DEMO_RUN_ID: tạo lại topic của demo, xóa khóa Redis $DEMO_KEY_PREFIX*"
    ensure_topics --recreate
    reset_redis_prefix
    rm -f -- "$REPLAY_JSON" "$REPLAY_STARTED"
  fi
  simulate
  # Tổng hợp lớp chỉ tính cho lớp có roster:{class}, nên roster phải có trước khi phát lại.
  load_roster
}

stream() {
  exec python -m ai_math_tutor.streaming.job --run-id "$DEMO_RUN_ID" --mode eager \
    --watermark "$DEMO_WATERMARK_S seconds" --learner-model "$DEMO_MODEL" \
    --input-topic "$DEMO_EVENTS_TOPIC" --dead-letter-topic "$DEMO_DEAD_LETTER_TOPIC" \
    --alerts-topic "$DEMO_ALERTS_TOPIC" --shuffle-partitions "$DEMO_PARTITIONS" \
    --redis-key-prefix "$DEMO_KEY_PREFIX"
}

replay() {
  if [[ -f "$REPLAY_JSON" ]]; then
    log "run $DEMO_RUN_ID đã phát lại xong ($REPLAY_JSON); không phát lại lần hai"
    return
  fi
  # Phát lại lần hai vào cùng topic sẽ đưa cùng event_id vào lại sau khi trạng thái khử trùng đã hết
  # hạn ⇒ áp hai lần. Lần trước bị ngắt giữa chừng thì dừng và yêu cầu làm lại từ đầu.
  [[ ! -f "$REPLAY_STARTED" ]] || die "lần phát lại trước của run $DEMO_RUN_ID bị ngắt giữa chừng; chạy make demo-clean rồi make demo"
  [[ -f "$SIM_DIR/events.parquet" ]] || die "chưa có $SIM_DIR/events.parquet (service demo-setup chưa chạy xong?)"
  date -u +%Y-%m-%dT%H:%M:%SZ > "$REPLAY_STARTED"
  log "phát lại $SIM_DIR/events.parquet vào $DEMO_EVENTS_TOPIC (W = $DEMO_WATERMARK_S s, run $DEMO_RUN_ID)"
  python -m ai_math_tutor.ingestion.replay --events "$SIM_DIR/events.parquet" --topic "$DEMO_EVENTS_TOPIC" \
    --processes 1 --watermark-s "$DEMO_WATERMARK_S" --run-id "$DEMO_RUN_ID" --no-run-record
}

case "${1:-}" in
  setup) setup ;;
  stream) stream ;;
  replay) replay ;;
  *) die "cách dùng: $0 setup|stream|replay" ;;
esac
