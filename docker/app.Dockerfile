# Image ứng dụng dùng chung cho profile `app` của docker-compose.yml: job Spark Structured Streaming,
# dịch vụ gợi ý (FastAPI), dashboard (Streamlit) và các bước dữ liệu của demo.
# Build: docker compose --profile app build   (hoặc make demo)
#
# Image nền ghim theo digest bản linux/amd64 (tag để đọc, digest để tái lập). Cập nhật: lấy digest mới
# của tag từ registry (docker buildx imagetools inspect <tag>) rồi thay cả tag lẫn digest bên dưới.
# Không có bí mật nào trong image: mật khẩu Redis, khóa API, mật khẩu giáo viên chỉ đến từ biến môi
# trường lúc chạy (docker-compose.yml đọc .env). .dockerignore loại .env khỏi build context.

# python:3.12-slim-bookworm = Python 3.12.16 trên Debian 12 (đẩy lên Docker Hub 2026-10-26).
# Debian 13 (nền của tag 3.12-slim) không còn gói openjdk-17, nên dùng biến thể bookworm.
FROM python:3.12-slim-bookworm@sha256:72c1d56207fb14ca388f9b3855ac0e55ab557703928180ec40aa9d4c4f5d5f80

# JDK 17 cho PySpark 4.1 (chỉ cần JRE headless); procps cho `ps` trong script khởi động của Spark.
# Thư mục man1 phải có sẵn thì bước update-alternatives của gói Java trên image slim mới chạy được.
RUN mkdir -p /usr/share/man/man1 \
    && apt-get update \
    && DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends openjdk-17-jre-headless procps \
    && rm -rf /var/lib/apt/lists/*

# uv 0.13.6 (cùng bản đã ghi uv.lock), bản linux/amd64 của image chính thức.
COPY --from=ghcr.io/astral-sh/uv:0.13.6@sha256:8b7774a14bb3abde914fda03efcb83f1f7e9163124e5c13e7fb4cc825293f5fa /uv /uvx /bin/

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home --home-dir /home/app --shell /usr/sbin/nologin app \
    && mkdir -p /data \
    && chown app:app /data

WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=never \
    UV_PROJECT_ENVIRONMENT=/app/.venv

# Phụ thuộc trước (lớp cache riêng), mã nguồn sau. --no-dev: không cài pytest/locust; nhóm `report`
# (python-docx, python-pptx) không thuộc nhóm mặc định nên cũng không được cài.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project

COPY src ./src
COPY configs ./configs
COPY data_sample ./data_sample
COPY docker/demo.sh docker/demo-hint.py ./docker/
RUN uv sync --frozen --no-dev

# Container luôn chạy với NHOM8_ENV=container: Kafka qua listener nội bộ kafka:29092, Redis qua tên
# service `redis` (configs/runtime.yaml). Dữ liệu nóng (checkpoint RocksDB, lake, mô phỏng, kết quả
# của demo) nằm trong volume gắn vào /data, không ghi vào /app.
ENV PATH=/app/.venv/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    NHOM8_ENV=container \
    NHOM8_DATA_ROOT=/data \
    NHOM8_RESULTS_DIR=/data/results

USER app

# Tải sẵn connector Kafka của Spark (tọa độ Maven trong configs/runtime.yaml) vào ~/.ivy2.5.2 của
# người dùng app, để container chạy được khi không có mạng (phòng bảo vệ). Cần mạng lúc build.
RUN python -c "from pyspark.sql import SparkSession; from ai_math_tutor.config import load_config; packages = ','.join(load_config().spark.packages); spark = SparkSession.builder.master('local[1]').config('spark.jars.packages', packages).config('spark.ui.enabled', 'false').config('spark.driver.host', '127.0.0.1').config('spark.driver.bindAddress', '127.0.0.1').getOrCreate(); spark.stop()"

# Không đặt lệnh mặc định riêng: mỗi service của profile app trong docker-compose.yml khai báo `command`.
