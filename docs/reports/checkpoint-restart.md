# Minh chứng tuần 4: restart từ checkpoint không mất, không trùng ghi

**Kết luận:** job bị SIGKILL giữa micro-batch 6 rồi chạy lại từ cùng checkpoint. Lần chạy lại cho đúng kết quả mà oracle đòi hỏi, giống hệt lần chạy không gián đoạn:
- 558/558 cặp (người học, kỹ năng) khớp oracle;
- không sự kiện nào được áp hai lần;
- 75/75 episode bế tắc của oracle có đúng một `alert_id` trong `alerts:stream`, 0 mục trùng;
- mỗi batch có đúng một tệp `latency`/`applied`/`alerts`, 5.578 dòng latency cho 5.578 sự kiện đã áp.

Ngày đo: 27/10/2026. Tệp số đo gốc: `$NHOM8_DATA_ROOT/it-evidence/checkpoint-restart-20261027T120929Z.json` (máy thí nghiệm).

## Cách làm (xác định)

Test: `tests/integration/test_restart.py::test_restart_from_checkpoint_matches_oracle`, chạy bằng `uv run pytest -q -s -m integration tests/integration/test_restart.py -k restart_from_checkpoint` sau `make up`.

1. **Dữ liệu:** bộ mô phỏng gồm 80 người học, 40 s, 150 ev/s, seed 31, hồ sơ trễ `high`, 5% gửi trùng. Bộ phát lại thật (`ingestion.replay`) cắt cửa sổ ở 45 s, W = 10 s, heartbeat 5 s vào cả 3 partition. Nạp **trước** vào topic riêng 3 partition. Kết quả: 5.838 lần giao, gồm 5.587 sự kiện duy nhất và 251 bản trùng có chủ đích, cộng 15 heartbeat.
2. **Job:** các hàm công khai của `ai_math_tutor.streaming.job` (`JobSettings`, `start_streams`, ...) chạy trong tiến trình con. Cấu hình: chế độ `ordered`, model `bkt`, W = 10 s, `maxOffsetsPerTrigger = 400`, `spark.sql.shuffle.partitions = 3`, RocksDB. Trigger ASAP, `startingOffsets = earliest`.
   - Topic đã nạp sẵn và giới hạn offset mỗi batch cố định, nên ranh giới batch chỉ phụ thuộc offset. Batch chạy lại đọc đúng dải offset ghi trong `offsets/<id>`.
   - Khóa Redis có tiền tố riêng của run; topic `alerts`/`dead-letter` cũng riêng.
3. **Lần 1 (không gián đoạn):** chạy tới khi drain xong, tức là có batch không dữ liệu với lag = 0 và watermark > cuối log.
4. **Lần 2 (sập):**
   - khi listener báo xong batch 5, chờ 0,3 s rồi `SIGKILL` cả nhóm tiến trình (driver Python + JVM), không có bước dọn dẹp nào;
   - sau đó chạy lại **cùng run_id, cùng checkpoint** tới khi drain.
5. **So sánh:** mỗi lần chạy được đối soát riêng với **oracle** bằng `streaming.reconcile`, không so hai lần với nhau.
   - Oracle tính trên đúng các sự kiện có trong `lake/<run>/applied`, đã khử trùng và sắp theo `(event_time, session_seq, event_id)`, bằng `learner_rules` + `bkt` với tham số `processor.bkt_params_for`.
   - Ngoài trạng thái Redis, test kiểm `alerts:stream` và thư mục lake.

## Kết quả

| Chỉ số | Không gián đoạn | Sập + chạy lại |
|---|---|---|
| Phiên Spark (dòng `started`) | 1 | 2 |
| Checkpoint ngay sau SIGKILL | — | `offsets/` tới batch 6, `commits/` tới batch 5 ⇒ batch 6 đang chạy dở |
| Batch đầu sau restart | — | 6 (chạy lại batch chưa commit), rồi 7 … 15 |
| Số batch có progress | 16 (0–15) | 6 (0–5) + 10 (6–15) |
| Lần giao / duy nhất / trùng có chủ đích | 5.838 / 5.587 / 251 | 5.838 / 5.587 / 251 |
| Đã áp / bị loại theo watermark | 5.578 / 9 | 5.578 / 9 |
| event_id áp hơn một lần | 0 | 0 |
| Trạng thái khớp oracle | 558/558 (100%) | 558/558 (100%) |
| Cảnh báo duy nhất (stuck + consec_wrong) | 367 (75 + 292) | 367 (75 + 292) |
| Mục trùng trong `alerts:stream` | 0 | 0 |
| Onset bế tắc: oracle / hệ thống / khớp | 75 / 75 / 75 | 75 / 75 / 75 |
| Thư mục batch có ≠ 1 tệp (applied, latency, alerts) | 0, 0, 0 | 0, 0, 0 |
| Dòng latency = event_id duy nhất = đã áp | 5.578 | 5.578 |
| Watermark cuối > cuối log | có | có |

Cùng seed, hai lần chạy test (12:04 và 12:09 UTC) cho cùng mọi con số đếm ở trên. Lần chạy đầu giết quá sớm: listener khi đó còn ghi cả query dead-letter, nên batch id đếm nhầm. Lần đó job bị giết ngay trong batch 0 của query trạng thái, nhưng sau restart vẫn khớp oracle 100%. Lỗi listener đã sửa (xem "Phát hiện").

Vì sao không mất, không trùng:
- **Trạng thái Spark:** checkpoint lưu offset đã lập kế hoạch cho batch 6 cùng phiên bản state store RocksDB của batch 5. Batch 6 chạy lại từ đúng trạng thái đó, với đúng dải offset đó.
- **Redis:** `redis_upsert.lua` chỉ ghi khi cặp (`batch_id`, `state_version`) mới lớn hơn bản đang lưu (so `batch_id` trước). Nếu batch 6 đã ghi được một phần trước khi chết, lần chạy lại cùng batch 6 không thay đổi gì thêm. (Lần đo trong báo cáo này chạy với bản cũ chỉ so `state_version`; bản sửa sau review giữ nguyên kết quả: `tests/integration/test_restart.py` chạy lại sau khi sửa vẫn khớp oracle 558/558.)
- **Cảnh báo:** `redis_alert.lua` kiểm khóa `alert:{id}`, rồi `XADD`, rồi `SET` trong cùng một script, nên mỗi id được XADD đúng một lần và XADD lỗi không để lại khóa dở.
- **Parquet:** batch 6 được ghi đè vào cùng tệp `batch=6/part-00000.parquet`.

Độ trễ trong test này **không** phải số đo SLO: topic được nạp sẵn nên "visible − sent" gồm cả thời gian sự kiện nằm chờ trong topic (P50 49 s / 88 s). Số đo độ trễ thật ở `docs/reports/late-events.md`.

## Phát hiện trong lúc đo

- Listener của Spark là toàn cục cho mọi query của session, mà job có hai query (trạng thái và dead-letter). Bản đầu của `ProgressListener` ghi cả hai query vào `progress.jsonl` và `metrics:pipeline`. Bản hiện tại chỉ nhận query `learner_state_<run_id>` (khớp `JobSettings.query_name`), và có test cho việc này.
- Redis không hoàn tác lệnh ghi khi script Lua trả lỗi giữa chừng. `redis_upsert.lua` vì vậy kiểm toàn bộ tham số trước rồi mới ghi; test truyền tham số sai và xác nhận không khóa nào bị tạo.

## Giới hạn

- Một máy, một seed, một điểm giết (giữa batch 6). Chưa thử giết đúng lúc đang `commit` RocksDB hay đang ghi Parquet. Tệp tạm cùng `os.replace` bảo đảm không có tệp dở, và test đơn vị xóa tệp tạm sót.
- Kafka `alerts` có ngữ nghĩa at-least-once. Nếu job chết sau `XADD` nhưng trước khi Kafka nhận tin, batch chạy lại gửi lại cùng nội dung. Consumer khử trùng theo `alert_id`; `tests/test_redis_lua.py` kiểm đường này.
- Restart đo ở chế độ `ordered` (phải khớp oracle 100%). Chế độ `eager` cũng tất định theo offset, nhưng lệch oracle do áp sai thứ tự (xem báo cáo sự kiện muộn), nên không dùng làm tiêu chí restart.
