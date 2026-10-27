"""Pipeline streaming trạng thái người học (Spark Structured Streaming + transformWithStateInPandas).

Mô-đun: `schema` (schema đầu vào hợp đồng v0.2 và đầu ra processor), `processor` (logic thuần
`apply_events` + `LearnerStateProcessor`), `job` (CLI: Kafka → parse/dead-letter → watermark/khử
trùng → trạng thái → foreachBatch), `sinks` (Redis/alerts/lake idempotent), `listener` (tiến độ,
lag), `reconcile` (đối soát với ground truth). Import từng mô-đun con khi cần; gói không nạp Spark.
"""
