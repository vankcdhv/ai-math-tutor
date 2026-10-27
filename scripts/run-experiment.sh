#!/usr/bin/env bash
# Chạy ma trận thí nghiệm (resume được: run đã success được bỏ qua). Ví dụ trong WSL:
#   scripts/run-experiment.sh A                     # Exp A × 3 seed theo plan-v1.0
#   scripts/run-experiment.sh all --dry-run         # kế hoạch, trạng thái resume, ước lượng giờ máy
#   scripts/run-experiment.sh ceiling --archive-dir /mnt/e/int4418-archive
#   PLAN=configs/experiments/smoke.yaml scripts/run-experiment.sh all
# Tham số thứ nhất = --exp (A|B|C|D|E|ablations|stress|soak|ceiling|all, phân tách bằng dấu phẩy);
# phần còn lại chuyển nguyên cho ai_math_tutor.experiments.runner. Chạy dài: dùng nohup/tmux trong
# một phiên WSL còn sống (WSL tắt distro rảnh), log của từng run nằm trong results/raw/<run_id>/.
set -euo pipefail
cd "$(dirname "$0")/.."
if [[ $# -lt 1 ]]; then
  echo "dùng: scripts/run-experiment.sh <EXP> [tham số runner...]" >&2
  exit 2
fi
exp="$1"
shift
export NHOM8_DATA_ROOT="${NHOM8_DATA_ROOT:-$HOME/nhom8-data}"
exec "${UV:-uv}" run python -m ai_math_tutor.experiments.runner \
  --plan "${PLAN:-configs/experiments/plan-v1.0.yaml}" --exp "$exp" "$@"
