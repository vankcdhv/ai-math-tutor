"""Batch baseline: hồ sơ người học theo lịch ngày/giờ trên cùng event log với streaming.

- `source`: nguồn log (Parquet mô phỏng, có thể đóng dấu lại như bộ phát lại; hoặc JSONL hợp đồng).
- `fold`: gấp chuỗi quan sát bằng `learner_rules` (luật duy nhất), dùng chung cho hai hiện thực.
- `daily_profile`: Spark DataFrame + applyInPandas (đường chính).
- `mapreduce_profile`: Spark RDD map → reduceByKey (đếm, combiner-safe) + secondary sort (chuỗi).
- `snapshot`: cột cutoff, gợi ý mẫu, ghi/đọc Parquet theo cutoff.
- `schedule`: các mốc cutoff với pha lấy từ seed.
- `evaluate`: time-to-detect so với ground truth, độ cũ trạng thái.
- `run_batch_baseline`: CLI chạy một lịch end-to-end và ghi run record.

Chỉ `fold`, `schedule`, `evaluate`, `source` không cần Spark; các mô-đun còn lại import pyspark.
"""
