#!/usr/bin/env bash
# Chạy một lệnh trong WSL tại thư mục dự án, dùng venv Linux trên ext4 (không đặt venv trên /mnt/d).
# Gọi từ PowerShell (không có $ hay dấu ngoặc trong tham số):
#   wsl -d Ubuntu-24.04 -- bash /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor/scripts/in-wsl.sh uv run pytest -q
set -euo pipefail
cd "$(dirname "$0")/.."
export UV_PROJECT_ENVIRONMENT="${UV_PROJECT_ENVIRONMENT:-$HOME/.venvs/nhom8}"
export PATH="$HOME/.local/bin:$PATH"
# Dữ liệu nóng (Kafka log, checkpoint, lake) nằm trên ext4 của WSL (SSD), không trên ổ Windows.
export NHOM8_DATA_ROOT="${NHOM8_DATA_ROOT:-$HOME/nhom8-data}"
mkdir -p "$NHOM8_DATA_ROOT"
exec "$@"
