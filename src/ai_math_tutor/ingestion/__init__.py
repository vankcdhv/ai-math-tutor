"""Đưa lịch sự kiện mô phỏng vào Kafka: quản lý topic, producer idempotent, bộ phát lại open-loop, đối soát topic.

- `topics`: tạo/xóa/tạo lại topic (learner-events 1/3/6 partition, alerts, dead-letter, hint-requests).
- `producer`: producer confluent-kafka idempotent (`acks=all`), đếm lỗi giao, backpressure khi hàng đợi đầy.
- `replay`: phát lại `events.parquet` đúng lịch (open-loop), đóng dấu một lần mỗi `event_id`, heartbeat drain.
- `verify_topic`: đọc lại topic, chạy `event_validation.validate_iter`, so với lịch đã phát.
"""
