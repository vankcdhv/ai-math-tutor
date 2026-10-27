# Cài đặt môi trường

Hai môi trường dùng chung một repo và cùng lệnh `make`:

| | Máy phát triển (macOS của thành viên) | Máy thí nghiệm (Windows 11 + WSL2 Ubuntu 24.04) |
|---|---|---|
| Chạy ở đâu | Terminal macOS | **Mọi thứ bên trong WSL** (Python, Spark, Docker) |
| Repo | thư mục bất kỳ | `D:\develop\thacsi\bigdata\nhom8-ai-math-tutor` = `/mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor` |
| venv | `.venv` trong repo (mặc định của uv) | `~/.venvs/nhom8` trên ext4 (đặt bởi `scripts/in-wsl.sh`) |
| Dữ liệu nóng (`NHOM8_DATA_ROOT`) | `~/nhom8-data` | `~/nhom8-data` trên ext4 của WSL (SSD) |

Phiên bản đã kiểm: Python 3.12 (dự án cần ≥ 3.10), JDK 17, pyspark 4.1.3 + `spark-sql-kafka-0-10_2.13:4.1.3`, confluent-kafka 2.16.0, Docker Engine 29.9 + Compose v5.6, `apache/kafka:4.3.2` và `redis:8.10.2-alpine` (ghim digest trong `docker-compose.yml`).

## 1. Cài công cụ

### macOS
```bash
brew install --cask temurin@17          # JDK 17 (Apple Silicon: bản arm64)
brew install uv make
# Docker Desktop (hoặc Colima); cấp ≥ 4 GB RAM cho VM Docker
export JAVA_HOME="$(/usr/libexec/java_home -v 17)"   # thêm vào ~/.zshrc
```

### Windows + WSL2 (máy thí nghiệm)
1. Trong PowerShell: `wsl --install -d Ubuntu-24.04`.
2. `.wslconfig` (`C:\Users\<user>\.wslconfig`) đặt giới hạn tài nguyên WSL, ví dụ máy 32 GB: `memory` ≈ 24 GB, `processors` đủ lõi, `swap` nhỏ. Áp dụng bằng `wsl --shutdown`. Không để mặc định (WSL chỉ lấy 50% RAM).
3. Trong Ubuntu:
   ```bash
   sudo apt-get update && sudo apt-get install -y openjdk-17-jdk-headless make curl
   curl -LsSf https://astral.sh/uv/install.sh | sh          # uv vào ~/.local/bin
   # Docker Engine trong WSL (hoặc Docker Desktop với WSL backend):
   # https://docs.docker.com/engine/install/ubuntu/ ; sau đó: sudo usermod -aG docker $USER
   ```
4. Dữ liệu nóng (Kafka log trong volume Docker, checkpoint/RocksDB, lake) phải nằm trên ext4 của WSL đặt trên SSD. **Không** đặt trên `/mnt/c` hay `/mnt/d` (I/O qua 9P chậm hơn nhiều lần). Ổ HDD (`/mnt/<hdd>`) chỉ dùng lưu trữ sau run.

### Kiểm tra phiên bản
```bash
java -version            # openjdk version "17..." (Spark 4.1.3 cần 17 hoặc 21)
uv --version
docker version --format '{{.Server.Version}}' && docker compose version
```

## 2. Cài phụ thuộc và cấu hình

```bash
uv sync                               # cài theo uv.lock (gồm nhóm dev)
cp .env.example .env                  # rồi đặt REDIS_PASSWORD ngẫu nhiên:
python3 -c "import secrets; print(secrets.token_urlsafe(24))"
```

`.env` đã nằm trong `.gitignore`; Docker Compose và `ai_math_tutor.config` đều đọc tệp này. Cấu hình chung ở `configs/runtime.yaml`; biến môi trường ghi đè được liệt kê ở đầu tệp đó (`NHOM8_DATA_ROOT`, `NHOM8_KAFKA_PARTITIONS`, `NHOM8_SHUFFLE_PARTITIONS`, `NHOM8_ENV`...).

Lần đầu tạo SparkSession, `spark.jars.packages` tải connector Kafka từ Maven Central vào `~/.ivy2.5.2` (~63 MB, cần mạng một lần).

## 3. Chạy

```bash
make up        # Kafka (127.0.0.1:9092) + Redis (127.0.0.1:6379), chờ healthcheck
make check     # JDK ≥ 17, pytest -m "not integration", validator dữ liệu mẫu, kiểm cổng compose
make spike     # spike Spark TWS + Kafka (vài phút); kết quả trong $NHOM8_DATA_ROOT/spike/
make ps        # trạng thái container
make down      # dừng stack (giữ volume Kafka; xóa hẳn: docker compose down -v)
make clean-state [RUN_ID=...]          # xóa checkpoint/lake (một run hoặc tất cả)
bash scripts/disk-check.sh 10          # thoát 1 nếu ổ chứa NHOM8_DATA_ROOT trống < 10 GB
```

### Gọi từ PowerShell trên máy thí nghiệm

Luôn đi qua helper — nó `cd` vào repo, đặt `UV_PROJECT_ENVIRONMENT=~/.venvs/nhom8` và `NHOM8_DATA_ROOT=~/nhom8-data`:

```powershell
wsl -d Ubuntu-24.04 -- bash /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor/scripts/in-wsl.sh make up
wsl -d Ubuntu-24.04 -- bash /mnt/d/develop/thacsi/bigdata/nhom8-ai-math-tutor/scripts/in-wsl.sh make check
```

**Bẫy quoting PowerShell 5.1:** khi truyền tham số cho `wsl`, PowerShell nuốt hoặc đổi `$`, dấu nháy và dấu ngoặc (`$(...)`, `"$VAR"`, `{...}`), và `wsl` còn ghép lại tham số qua shell Linux một lần nữa — lệnh chạy khác lệnh bạn gõ (đã từng tạo nhầm thư mục rác). Quy tắc:
- Tham số đơn giản (không `$`, nháy, ngoặc): gọi thẳng qua `in-wsl.sh` như trên.
- Lệnh phức tạp: viết vào tệp `.sh` (xuống dòng LF) rồi chạy `wsl -d Ubuntu-24.04 -- bash /mnt/c/.../tep.sh`, hoặc mở shell WSL (`wsl -d Ubuntu-24.04`) rồi gõ lệnh trong đó.
- Chạy xong kiểm `git status` để chắc không có tệp/thư mục lạ.

### WSL tự tắt distro khi rảnh

Khi lệnh `wsl ...` kết thúc và không còn tiến trình nào trong distro, WSL tắt distro sau vài giây (journal ghi `The system will power off now!`): dockerd dừng, Kafka/Redis dừng theo, rồi tự chạy lại ở lần gọi `wsl` kế tiếp (`restart: unless-stopped`; volume Kafka giữ nguyên). Đã quan sát trên máy thí nghiệm ngày 27/10/2026. Hệ quả:
- Chạy spike/thí nghiệm trong **một** lệnh dài (`make up && make spike`, runner) thay vì rải qua nhiều lệnh `wsl` rời rạc; luôn `make up` trước (idempotent, chờ healthy).
- Khi làm việc tương tác, giữ một cửa sổ `wsl -d Ubuntu-24.04` mở để distro không bị tắt giữa chừng.
- Lệnh chạy ngay sau khi distro vừa khởi động có thể gặp Kafka chưa sẵn sàng (`Connection to node -1 ... could not be established`) — chờ `make up` báo healthy.

## 4. Sự cố thường gặp

| Triệu chứng | Nguyên nhân / cách xử lý |
|---|---|
| `make up`: `Bind for 127.0.0.1:9092 failed: port is already allocated` | Tiến trình/container cũ còn giữ cổng. Tìm chủ: `docker ps --filter publish=9092`, `ss -ltnp 'sport = :9092'` (macOS: `lsof -i :9092`). Dừng tiến trình cũ của chính mình (`make down`, `docker stop <id>`). **Không đổi cổng** — cổng cố định là hợp đồng giữa các thành phần. |
| `Thiếu .env` / `REDIS_PASSWORD` | `cp .env.example .env` và đặt mật khẩu ngẫu nhiên. |
| Spark: `Failed to find data source: kafka` | Thiếu `spark.jars.packages` (SparkSession phải tạo qua `ai_math_tutor.config`), hoặc máy chưa từng tải được Maven Central. |
| `UNSUPPORTED_FEATURE.STATE_STORE_MULTIPLE_COLUMN_FAMILIES` | `transformWithStateInPandas` chạy trên HDFS state store — phải dùng RocksDB (`spark.sql.streaming.stateStore.providerClass` trong `configs/runtime.yaml`). |
| `UnsupportedClassVersionError` hoặc lỗi JNI RocksDB | JDK sai phiên bản/kiến trúc: dùng JDK 17 (Apple Silicon: Temurin arm64), kiểm `JAVA_HOME`. |
| Spark chậm/ổ đầy trên WSL | Dữ liệu nằm trên `/mnt/*` hoặc SSD gần đầy: kiểm `NHOM8_DATA_ROOT`, chạy `scripts/disk-check.sh`, `make clean-state`. |
| Client trên host không nối được Kafka | Dùng `127.0.0.1:9092` (listener EXTERNAL). Container trong mạng Compose dùng `kafka:29092` (listener INTERNAL, `NHOM8_ENV=container`). |

## 5. Bảo mật cục bộ

Mọi cổng Compose chỉ publish trên `127.0.0.1` (`make check` thất bại nếu có cổng bind giao diện khác); Redis bật `requirepass` từ `.env`; image ghim theo digest; Spark driver bind `127.0.0.1` và tắt Spark UI. Không commit `.env`, khóa API hay salt thật.
