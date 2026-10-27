#!/usr/bin/env bash
# Báo dung lượng trống của filesystem chứa NHOM8_DATA_ROOT (Kafka log tách riêng trong
# data-root của Docker, thường cùng ổ ext4 của WSL). Dùng trước mỗi run thí nghiệm.
#   scripts/disk-check.sh [NGƯỠNG_GB]     (mặc định 10)
# Thoát 0 khi đủ chỗ, 1 khi trống < ngưỡng, 2 khi tham số sai hoặc không đọc được df.
set -euo pipefail

threshold_gb="${1:-10}"
if ! [[ "$threshold_gb" =~ ^[0-9]+([.][0-9]+)?$ ]]; then
  echo "disk-check: ngưỡng phải là số GB không âm, nhận '$threshold_gb'" >&2
  exit 2
fi

data_root="${NHOM8_DATA_ROOT:-$HOME/nhom8-data}"
# Thư mục có thể chưa tồn tại trên máy mới: đo ở thư mục cha gần nhất đang có.
probe="$data_root"
while [ ! -e "$probe" ]; do
  probe="$(dirname "$probe")"
done

if ! line="$(df -Pk "$probe" | awk 'NR==2 {print $1, $4, $6}')" || [ -z "$line" ]; then
  echo "disk-check: không đọc được df cho $probe" >&2
  exit 2
fi
read -r filesystem avail_kb mountpoint <<<"$line"

free_gb="$(awk -v kb="$avail_kb" 'BEGIN {printf "%.1f", kb / 1024 / 1024}')"
echo "NHOM8_DATA_ROOT=$data_root filesystem=$filesystem mount=$mountpoint free_gb=$free_gb threshold_gb=$threshold_gb"

if awk -v kb="$avail_kb" -v t="$threshold_gb" 'BEGIN {exit !(kb / 1024 / 1024 < t)}'; then
  echo "disk-check: KHÔNG ĐỦ CHỖ — trống ${free_gb} GB < ${threshold_gb} GB; dọn checkpoint/lake/topic của run cũ (make clean-state)" >&2
  exit 1
fi
