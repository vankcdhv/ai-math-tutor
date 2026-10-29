#!/usr/bin/env bash
# Lệnh cho từng bước của kịch bản demo (docs/demo-script.md). Chạy trong WSL, tại thư mục repo:
#   bash scripts/demo-steps.sh <bước>
#
#   start      dựng lại demo từ đầu (make demo-clean + make demo chạy nền), chờ các service healthy
#   status     bước 1: container, cổng (chỉ 127.0.0.1)
#   data       bước 2: roster, dòng phát lại, W của job
#   hints      bước 5: người học vừa bế tắc xin gợi ý 3 lần ở cùng một bước
#   kill       bước 7a: SIGKILL job Spark (giữa micro-batch), đếm cảnh báo trước khi giết
#   resume     bước 7b: chạy lại job từ checkpoint, rồi so số mục cảnh báo với số alert_id khác nhau
#   stop       tắt các container ứng dụng (Kafka, Redis giữ nguyên)
#
# Khóa API và mật khẩu Redis chỉ đọc từ env của container, không in ra.
set -euo pipefail
cd "$(dirname "$0")/.."
export COMPOSE_PROFILES=app
LOG=/tmp/nhom8-demo-up.log

wait_healthy() {
  # Lần đầu sau khi đổi mã/kho bài phải build lại image: có thể mất vài phút.
  local deadline=$((SECONDS + 900))
  until [ "$(docker compose ps --format '{{.Service}} {{.Status}}' |
             grep -cE '^(streaming|hint-api|dashboard|student) .*\(healthy\)')" = 4 ]; do
    ((SECONDS < deadline)) || { echo "quá 15 phút chưa healthy — xem: tail -n 50 $LOG" >&2; exit 1; }
    sleep 5
  done
}

case "${1:-}" in
  start)
    make demo-clean >/dev/null
    nohup make demo >"$LOG" 2>&1 &
    echo "make demo chạy nền (log: $LOG), chờ service healthy…"
    wait_healthy
    docker compose ps --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'
    echo "Mở: http://127.0.0.1:8501 (dashboard) · http://127.0.0.1:8502 (Học sinh)"
    ;;
  status)
    docker compose ps -a --format 'table {{.Service}}\t{{.Status}}\t{{.Ports}}'
    ;;
  data)
    docker compose logs --no-log-prefix demo-setup | grep -E 'roster|learners' | tail -n 5 || true
    docker compose logs --tail 3 demo-replay streaming
    ;;
  hints)
    docker compose exec -T hint-api sh -c '
      python docker/demo-hint.py --count 3 > /tmp/demo-hints.jsonl
      while IFS= read -r body; do
        python - "$body" <<PY
import json, os, sys, urllib.request
req = urllib.request.Request("http://127.0.0.1:8000/hints", data=sys.argv[1].encode(), method="POST",
    headers={"Content-Type": "application/json", "X-API-Key": os.environ["NHOM8_HINT_API_KEY"]})
r = json.load(urllib.request.urlopen(req))
print("bậc", r["hint_level"], "| verified:", r["verified"], "| trạng thái từ:", r["state_source"])
print("  ", r["text_vi"])
if r["math"]:
    print("   math:", r["math"])
PY
      done < /tmp/demo-hints.jsonl'
    ;;
  kill)
    docker compose exec -T redis sh -c 'echo "mục cảnh báo trước khi giết: $(redis-cli XLEN demo:alerts:stream)"'
    docker compose kill streaming
    docker compose ps -a streaming --format 'table {{.Service}}\t{{.Status}}'
    echo "Tab Pipeline: 'tuổi số liệu' tăng dần; sau 30 s có cảnh báo số liệu cũ."
    ;;
  resume)
    docker compose start streaming
    sleep 20
    docker compose logs --tail 200 streaming | grep -E 'resumed=|run demo' | tail -n 2 || true
    docker compose exec -T redis sh -c '
      n=$(redis-cli XLEN demo:alerts:stream)
      u=$(redis-cli --raw XRANGE demo:alerts:stream - + \
          | awk "prev == \"alert_id\" {print} {prev = \$0}" | sort -u | wc -l)
      echo "mục trong alerts:stream: $n · alert_id khác nhau: $u"
      [ "$n" = "$u" ] && echo "→ không cảnh báo nào bị ghi hai lần" || echo "→ CÓ cảnh báo trùng"'
    ;;
  stop)
    make demo-down
    ;;
  *)
    sed -n '2,13p' "$0"; exit 2
    ;;
esac
