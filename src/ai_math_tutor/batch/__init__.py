"""Batch baseline: hồ sơ người học theo lịch ngày/giờ trên cùng event log với streaming.

- `source`: nguồn log (Parquet mô phỏng, có thể đóng dấu lại như bộ phát lại; hoặc JSONL hợp đồng).
- `fold`: gấp chuỗi quan sát bằng `learner_rules` (luật duy nhất), dùng chung cho hai hiện thực.
- `daily_profile`: Spark DataFrame + applyInPandas (đường chính).
- `mapreduce_profile`: Spark RDD map → reduceByKey (đếm, combiner-safe) + secondary sort (chuỗi).
- `mapreduce_problems`: bộ đếm theo (learner, problem), khử trùng, bản ghi lỗi, top-K bài sai nhiều nhất trên RDD,
  kèm bản tham chiếu thuần Python; `mapreduce_run`: một cấu hình tài nguyên (session, số đo REST/CPU/RSS).
- `snapshot`: cột cutoff, gợi ý mẫu, ghi/đọc Parquet theo cutoff.
- `schedule`: các mốc cutoff với pha lấy từ seed.
- `evaluate`: time-to-detect so với ground truth, độ cũ trạng thái.
- `run_batch_baseline`: CLI chạy một lịch end-to-end và ghi run record.

`fold`, `schedule`, `evaluate`, `source`, `mapreduce_problems` và `mapreduce_run` import được không cần pyspark
(hai mô-đun sau chỉ cần Spark khi chạy job); các mô-đun còn lại import pyspark.
"""
