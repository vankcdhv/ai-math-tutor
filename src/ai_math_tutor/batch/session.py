"""SparkSession cho batch: local mode, conf chung từ `configs/runtime.yaml`, thư mục tạm dưới $NHOM8_DATA_ROOT.

Không nạp connector Kafka (batch chỉ đọc tệp) và không đặt state store streaming. Thư mục tạm
của Spark (shuffle spill, block) và warehouse nằm trên ext4/SSD của WSL thay vì /tmp hay /mnt/d.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING

from ai_math_tutor.config import load_config

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

# Khóa conf chỉ có nghĩa với streaming/Kafka.
_STREAMING_ONLY_KEYS = ("spark.jars.packages", "spark.sql.streaming.stateStore.providerClass")


def batch_spark_conf(
    master: str | None = None,
    shuffle_partitions: int | None = None,
    data_root: Path | None = None,
) -> dict[str, str]:
    """Conf đầy đủ (để đặt vào builder và ghi vào run record)."""
    cfg = load_config()
    conf = {k: v for k, v in cfg.spark.conf().items() if k not in _STREAMING_ONLY_KEYS}
    root = Path(data_root) if data_root else cfg.paths.data_root
    local_dir = root / "spark-tmp" / "batch"
    warehouse = root / "spark-warehouse"
    local_dir.mkdir(parents=True, exist_ok=True)
    warehouse.mkdir(parents=True, exist_ok=True)
    conf.update({
        "spark.app.name": "nhom8-batch-baseline",
        "spark.local.dir": str(local_dir),
        "spark.sql.warehouse.dir": str(warehouse),
        "spark.driver.extraJavaOptions": f"-Djava.io.tmpdir={local_dir}",
        "spark.sql.execution.arrow.pyspark.enabled": "true",
        "spark.sql.session.timeZone": "UTC",
    })
    if master:
        conf["spark.master"] = master
    if shuffle_partitions is not None:
        if shuffle_partitions < 1:
            raise ValueError(f"shuffle_partitions phải ≥ 1, nhận {shuffle_partitions}")
        conf["spark.sql.shuffle.partitions"] = str(shuffle_partitions)
    return conf


@contextmanager
def spark_session(conf: dict[str, str]) -> Iterator[SparkSession]:
    """Mở SparkSession với conf đã cho và luôn dừng nó khi ra khỏi khối (kể cả khi lỗi)."""
    from pyspark.sql import SparkSession

    # Worker Python dùng cùng interpreter/venv với driver.
    os.environ.setdefault("PYSPARK_PYTHON", sys.executable)
    os.environ.setdefault("PYSPARK_DRIVER_PYTHON", sys.executable)
    builder = SparkSession.builder
    for key, value in conf.items():
        builder = builder.config(key, value)
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    try:
        yield spark
    finally:
        spark.stop()
