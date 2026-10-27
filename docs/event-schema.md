# Event schema và data contract — LearnerEvent v0.2

Nguồn chuẩn (máy đọc): `src/ai_math_tutor/contracts/learner_event_v0_2.schema.json` (JSON Schema 2020-12; bản v0.1 giữ nguyên ở `learner_event_v0_1.schema.json` để tra lịch sử). Tài liệu này giải thích ý nghĩa và các quy tắc mà schema không tự diễn đạt được. Kiểm tra bằng: `uv run python -m ai_math_tutor.event_validation <tệp.jsonl> --problems <kho-bài.jsonl>` (validator dùng v0.2, nhận cả sự kiện v0.1).

v0.2 là bản phụ tương thích ngược: chỉ thêm loại `heartbeat` (mục [Hợp đồng v0.2](#hợp-đồng-v02--heartbeat)). Mọi bảng và quy tắc dưới đây về **sự kiện học tập** giữ nguyên như v0.1.

## Trường

| Trường | Kiểu | Ý nghĩa / quy tắc |
|---|---|---|
| `schema_version` | `"0.1"` \| `"0.2"` | Đổi khi hợp đồng đổi; consumer từ chối phiên bản lạ. Sự kiện học tập dùng `"0.1"` hoặc `"0.2"` (cùng cấu trúc); heartbeat chỉ `"0.2"` |
| `event_id` | UUID | Client sinh **một lần**; retry gửi lại **nguyên byte** (kể cả `sent_time`). Khóa khử trùng |
| `event_type` | enum | `open_problem`, `submit_step`, `wrong_answer`, `request_hint`, `complete` |
| `learner_id` | `L-` + 16 hex | Bút danh HMAC-SHA256(salt bí mật, mã gốc); không đảo ngược được nếu không có salt |
| `class_id` | `C-...` | Lớp; dùng cho dashboard (áp ngưỡng k) |
| `session_id`, `session_seq` | `S-` + 8 hex, số nguyên ≥ 1 | Thứ tự tăng dần trong phiên ⇒ phát hiện mất (khoảng trống), trùng, đảo thứ tự |
| `problem_id`, `skill_ids` | chuỗi | Ánh xạ bài → kỹ năng (knowledge component) |
| `event_time` | UTC ISO 8601 `...Z` | Lúc người học thao tác. **Mọi trạng thái tính theo trường này** (event time) |
| `sent_time` | UTC ISO 8601 `...Z` | Lúc client gửi lần đầu. `sent_time − event_time` lớn ⇒ sự kiện đến muộn |
| `client` | object | `platform`, `app_version`, `connection` (`wifi`/`cellular`/`unknown`) — dùng cho lát cắt công bằng |
| `payload` | object | Phụ thuộc `event_type` (bảng dưới); không có trường thừa |

| `event_type` | `payload` bắt buộc |
|---|---|
| `open_problem` | `{}` |
| `submit_step` | `step_index ≥ 1`, `expression` (phương trình theo cú pháp SymPy), `is_correct` (kết quả kiểm chứng tương đương) |
| `wrong_answer` | `answer` (`"4"` hoặc `"{2, 3}"`), `attempt ≥ 1` |
| `request_hint` | `hint_level` 1–3, `step_index ≥ 1` |
| `complete` | `answer`, `attempts ≥ 1`, `hints_used ≥ 0` |

## Quy tắc hợp đồng (ngoài schema)

1. **Kafka:** topic `learner-events`, key = `learner_id` ⇒ mọi sự kiện của một người học vào cùng partition, giữ thứ tự gửi trong partition. Thứ tự gửi ≠ thứ tự `event_time` khi có retry.
2. **Trùng:** cùng `event_id` + cùng nội dung = gửi lại hợp lệ → bỏ. Cùng `event_id` khác nội dung = lỗi hợp đồng → dead-letter.
3. **Không có ground truth:** mức thành thạo thật, cờ "bế tắc thật" của bộ mô phỏng ghi ở log riêng, không bao giờ vào payload.
4. **Không PII:** không tên, email, số điện thoại, văn bản tự do. `expression`/`answer` bị giới hạn ký tự toán học (xem `math_check.py`).
5. **Nhãn toán học:** bài `equation`: `is_correct` = tập nghiệm thực của bước bằng tập nghiệm của đề (SymPy `solveset`); `complete` chỉ khi đáp án khớp **toàn bộ** tập nghiệm. Bài `arithmetic`: `is_correct` = bước `a = b` tính đúng chính xác (số hữu tỉ) **và** kết quả nằm trên đường giải mẫu (`steps`/`answer`); `complete` khi đáp án bằng `answer`. Bước phải có `=`, đáp án không có `=`. Kiểm chứng chạy trong `safe_math` (worker giết được, thời hạn 5 s/nhãn khi validate). `expression`/`answer` bị giới hạn: ≤ 200 ký tự mỗi vế, ngoặc ≤ 20 tầng, số ≤ 15 chữ số, số mũ nguyên trong [-10, 10], không lũy thừa lồng nhau, bậc theo x ≤ 2. `step_index` đếm từ 1.
6. **Tiến hóa:** thêm trường tùy chọn → tăng bản phụ (0.2); đổi nghĩa/xóa trường → bản chính mới + giai đoạn chạy song song.

## Hợp đồng v0.2 — heartbeat

**Vấn đề:** pipeline streaming tính trạng thái theo `event_time` với watermark = max `event_time` đã thấy − W. Khi bộ phát lại hết log, không còn sự kiện mới nên watermark đứng yên, timer event-time và cửa sổ cuối không bao giờ đóng. Heartbeat là bản ghi kỹ thuật để đẩy watermark qua cuối log (drain).

| Trường | Giá trị |
|---|---|
| `schema_version` | `"0.2"` (bắt buộc) |
| `event_id` | UUID mới cho mỗi heartbeat |
| `event_type` | `"heartbeat"` |
| `learner_id` | `L-0000000000000000` — **dành riêng**; sự kiện học tập dùng mã này là lỗi hợp đồng |
| `event_time` | Thời điểm watermark cần đạt tới (tổng hợp, không phải thao tác người học) |
| `sent_time` | Bằng `event_time` |
| `payload` | `{}` |

Heartbeat **không** có `class_id`, `session_id`, `session_seq`, `problem_id`, `skill_ids`, `client` (schema từ chối nếu có). Spark `from_json` theo schema v0.2 sẽ cho các cột này `null` ở dòng heartbeat.

Quy tắc:

1. **Gửi:** bộ phát lại (`ai_math_tutor.ingestion.replay`) gửi heartbeat sau khi hết log, vào **từng partition** của `learner-events` (chỉ định partition tường minh, key = `L-0000000000000000`). Vòng thứ k (k = 0, 1, …, mỗi giây một vòng) có `event_time = cuối log + W + 1 s + k s`, với cuối log = max `event_time` của các sự kiện đã phát. Lặp tới khi runner xác nhận watermark đã qua cuối log (hàm `heartbeat_until`) hoặc hết `--heartbeat-s`.
2. **Lọc:** pipeline nhận heartbeat (để watermark tiến) nhưng lọc nó **trước** khử trùng/trạng thái người học; heartbeat không bao giờ thành người học, không vào thống kê, Redis hay lake. `event_validation.is_heartbeat(event)` là vị từ dùng chung; validator đếm riêng ở `ValidationReport.heartbeats`.
3. **Thời điểm áp dụng:** watermark mới chỉ có hiệu lực ở micro-batch **kế tiếp** (thường là batch không dữ liệu) sau batch chứa heartbeat (đã quan sát trong spike Spark TWS + Kafka, `make spike`) — runner chờ watermark ≥ cuối log rồi mới đối soát.
4. **Sau drain:** sự kiện học tập có `event_time` cũ hơn watermark sẽ bị loại theo watermark (đúng ngữ nghĩa); drain chỉ làm sau khi phát hết log.

## Phát lại vào Kafka (đóng dấu thời gian)

- Bộ phát lại đặt `event_time`/`sent_time` = `replay_start + offset` (`simulator.output.restamp`), giữ nguyên khoảng `sent_time − event_time` của mô phỏng. `replay_start` (epoch ms) ghi trong `results/raw/replay-<run>.json` và run record; mọi phép nối với ground truth dịch bằng giá trị này: `abs_ms = replay_start_ms + t_s·1000`.
- Đóng dấu **một lần cho mỗi `event_id`**: bản trùng có chủ đích gửi lại đúng các byte của bản gốc (quy tắc 2 ở trên).
- Producer idempotent (`enable.idempotence=true`, `acks=all`): retry do lỗi mạng/broker không sinh thêm bản sao — trên topic, số lần giao trùng = số bản trùng có chủ đích (`ai_math_tutor.ingestion.verify_topic` kiểm).

## Dữ liệu mẫu tuần 1 (20 dòng)

`data_sample/events-week1-sample.jsonl`, tạo lại bằng `uv run python scripts/make-week1-sample.py` (tất định, test kiểm byte-đối-byte).

| Người học | Kết nối | Bài | Diễn biến |
|---|---|---|---|
| A | wifi/web | P-LIN-001, P-QUAD-001 | Giải đúng bài 1; bài 2 làm đúng bước 1 rồi xin gợi ý bậc 1 (đang dở) |
| B | cellular/android | P-LIN-002 | Lỗi phân phối `3x − 1` → xin gợi ý (**đến muộn 95 s, sai thứ tự**) → sửa đúng → hoàn thành; bước 2 bị **gửi trùng** |
| C | wifi/ios | P-QUAD-001 | Sai dấu khi phân tích nhân tử → sửa → **thiếu nghiệm** (`wrong_answer` "2") → gợi ý bậc 2 → hoàn thành `{2, 3}` |

Kết quả kiểm tra: 20 dòng, 19 sự kiện duy nhất, 1 gửi trùng, 1 đến muộn (> 30 s), 1 đảo thứ tự, 3 phiên, 0 khoảng trống `session_seq`, 12 nhãn toán học khớp SymPy, 0 lỗi.

## Khóa Redis

Nguồn tên khóa duy nhất trong code: `ai_math_tutor.streaming.sinks.RedisKeys` (dịch vụ gợi ý, dashboard, runner import lớp này, không tự ghép chuỗi). Tên trường của `metrics:pipeline`: `ai_math_tutor.streaming.listener.metrics_fields`. `RedisKeys(prefix)` thêm tiền tố vào **mọi** khóa; khi chạy thật, tiền tố rỗng. Test dùng chung Redis đặt tiền tố riêng (`it-<uuid>:`) và chỉ xóa khóa của mình.

Phạm vi "run": runner thí nghiệm chạy `FLUSHDB` ở **đầu** mỗi run trên DB riêng của run, rồi nạp `roster:{class}`. Runner dùng DB riêng cho thí nghiệm (mặc định DB 9, `--redis-db`; job con nhận qua `NHOM8_REDIS_DB`), từ chối FLUSHDB trên DB 0 hoặc trên DB có khóa không thuộc bảng dưới; DB được FLUSHDB ở đầu và cuối mỗi run. Không có TTL. Mọi giá trị là chuỗi (số ghi thập phân, boolean trong hash phẳng ghi `0`/`1`).

| Khóa | Kiểu | Ghi | Đọc | TTL / phạm vi xóa | Nội dung |
|---|---|---|---|---|---|
| `state:{learner}` | hash | P6 foreachBatch, `redis_upsert.lua` (chỉ ghi khi (`batch_id`, `state_version`) mới > cũ theo thứ tự từ điển) | P7, P8 | run | Mỗi trường là một `skill_id`; giá trị JSON `{class_id, p_mastery (null khi model=rule), opportunities, correct, consec_wrong, streak, mastered, stuck, stuck_onset_event_id, state_version, last_event_time_ms, batch_id}` |
| `progress:{learner}:{problem}` | hash | P6, `redis_upsert.lua` (so cặp trường `batch_id`, `state_version`) | P7 | run | `class_id, wrong_answers, last_step, completed (0/1), state_version, last_event_time_ms, batch_id` |
| `roster:{class}` | set | runner P9 từ `roster.json` (`sinks.load_roster`, DEL + SADD) | P6, P8 | run | `learner_id` của lớp |
| `agg:class:{class}:skill:{skill}` | hash | P6 (tính lại toàn bộ cho lớp bị chạm trong batch, từ `state:*` của roster — qua bộ đệm trên driver, cùng kết quả) | P8 | run | `class_id, skill_id, learners (cỡ roster), observed, mastered, stuck, mean_p_mastery ("" khi rule), mean_opportunities, accuracy, as_of_event_ms, batch_id`. Ngưỡng k-ẩn danh áp ở P8 |
| `stuck:set:{class}` | set | P6 (DEL + SADD cùng lúc với agg) | P8 (chế độ giáo viên) | run | `"{learner}\|{skill}"` đang bế tắc |
| `alert:{alert_id}` | string | P6 `redis_alert.lua` (chỉ khi chưa có: `XADD` rồi `SET`) | P6 | run | `pending:<stream id>` sau XADD; `produced:<stream id>` khi topic `alerts` đã nhận |
| `alerts:stream` | stream | P6 `redis_alert.lua` (`XADD MAXLEN ~ 100000`, chỉ khi `alert:{id}` chưa có giá trị hợp lệ) | P8 | run; giới hạn MAXLEN | Mục: xem "Schema cảnh báo" dưới. Stream id `<ms>-<seq>` = lúc cảnh báo hiện lần đầu |
| `trend:{class}:{bucket5m}` | hash | P6 (ghi đè snapshot lớp) | P8 | 2 h (`EXPIRE 7200`) | `bucket5m` = epoch ms của ô 5 phút chứa event time mà snapshot lớp phản ánh (max `as_of_event_ms` trên mọi kỹ năng của lớp), nên không bao giờ ghi đè một ô cũ hơn bằng dữ liệu mới hơn. Mỗi trường là `skill_id`, giá trị JSON `{observed, mastered, stuck, mean_p_mastery, accuracy, as_of_event_ms}` |
| `metrics:pipeline` | hash | P6 listener (mỗi micro-batch) | P8, P9 | run | `run_id, batch_id, updated_ms, input_rows_per_s, processed_rows_per_s, batch_duration_ms, foreach_ms, watermark_ms, lag_total, lag_max_partition, p95_system_ms_1m, p95_staleness_ms_1m, latency_samples_1m, dropped_by_watermark, rss_bytes` ("" khi chưa có) |
| `hintctx:{learner}:{problem}:{step}` | set | dịch vụ gợi ý (MULTI: SADD + EXPIRE, mỗi yêu cầu) | dịch vụ gợi ý | 1 h, gia hạn mỗi yêu cầu | `h:<event_id>` mỗi yêu cầu gợi ý, `w:<event_id>` khi yêu cầu kèm biểu thức sai (đếm theo tập `event_id` ⇒ retry cùng `event_id` không đếm lại) |
| `lasthint:{learner}` | string JSON | dịch vụ gợi ý (sau khi trả lời) | dashboard | 1 h | `event_id, problem_id, step_index, hint_level, source, verified, misconception, state_source, text_vi, served_ms` |

Tên hai khóa gợi ý cũng sinh từ `RedisKeys` (`hintctx(learner, problem, step)`, `lasthint(learner)`).

### Nhật ký `hint-requests`

Topic Kafka `hint-requests`, key = `learner_id`, value JSON `{event_id, learner_id, class_id, problem_id, step_index, hint_level, source, verified, misconception, state_source, expression_status, generator, level_reason, latency_ms, served_ms}`: một tin cho mỗi yêu cầu gợi ý. Không chứa `expression` hay `text_vi`. Đối soát (`reconcile`) không dùng topic này.

Dashboard chỉ đọc, không ghi khóa nào: `roster:*` (SCAN + SCARD), `agg:*` (HMGET `observed, mastered, stuck, as_of_event_ms, batch_id`), `trend:*`, `stuck:set:*`, `state:*` (chỉ ở chế độ giáo viên), `alert:*`/`alerts:stream`, `lasthint:*`, `metrics:pipeline`.

Bất biến idempotent (foreachBatch chỉ at-least-once): chạy lại một batch không đổi bất kỳ khóa nào do pipeline ghi ở bảng trên. `state:`/`progress:` bị chặn bởi cặp (`batch_id`, `state_version`): batch chạy lại giữ `batch_id` nên không ghi gì, còn version quay về 1 sau khi state hết hạn (TTL theo event time) vẫn được ghi vì thuộc batch sau (`batch_id` tăng đơn điệu qua restart, đọc từ checkpoint). `alert:`/`alerts:stream` bởi kiểm khóa → `XADD` → `SET` trong một script (khóa chỉ được ghi sau khi `XADD` thành công; giá trị không đúng dạng `<pending|produced>:<id>` coi như chưa có); `agg:`/`stuck:set:`/`trend:` được tính lại từ trạng thái hiện có nên cho cùng kết quả. Một run mới (checkpoint mới, `batch_id` lại từ 0) cần Redis sạch — runner `FLUSHDB` DB của run trước khi chạy. Không bao giờ ghi ground truth vào Redis.

### Schema cảnh báo

Một cảnh báo xuất hiện ở ba nơi với cùng `alert_id`: mục `alerts:stream`, tin Kafka topic `alerts` (key = `learner_id`, value JSON) và `lake/<run_id>/alerts/batch=<id>/`.

| Trường | Ý nghĩa |
|---|---|
| `alert_id` | sha256 của chuỗi `learner`, `skill`, `loại`, `sự kiện kích hoạt` nối bằng dấu gạch đứng, lấy 24 ký tự hex đầu (`processor.alert_id`) — tất định, consumer khử trùng theo trường này |
| `alert_type` | `stuck` (bế tắc: `is_wheel_spinning` chuyển sang True, một lần mỗi episode), `consec_wrong` (lần sai thứ 3 liên tiếp, cảnh báo phụ), `inactivity` (timer event-time: không hoạt động khi bài cuối chưa xong) |
| `learner_id`, `class_id`, `skill_id` | |
| `onset_event_id` | sự kiện kích hoạt |
| `alert_event_time_ms` | event time của sự kiện kích hoạt (với `inactivity`: hạn của timer) |
| `run_id`, `batch_id` | run và micro-batch phát cảnh báo |
| `visible_ms`, `stream_id` | chỉ trong tin Kafka và lake: lúc XADD lần đầu (ms của stream id) |

Kafka at-least-once: nếu job chết sau XADD nhưng trước khi topic nhận tin, batch chạy lại thấy `pending:` và gửi lại cùng nội dung; stream không bao giờ có mục trùng.

### Lake theo run (Parquet, dưới `$NHOM8_DATA_ROOT/lake/<run_id>/`)

Mỗi batch ghi đè đúng một tệp `<kind>/batch=<batch_id>/part-00000.parquet` (ghi tệp tạm rồi `os.replace`), batch chạy lại thay đúng tệp cũ.

| kind | Một dòng cho | Cột |
|---|---|---|
| `applied` | mỗi `event_id` đã áp (mọi loại trừ heartbeat) | `event_id, learner_id, class_id, skill_id (skill_ids[0]), event_time_ms, sent_time_ms, out_of_order, state_version, batch_id` |
| `latency` | mỗi `event_id` đã áp | `event_id, learner_id, class_id, event_time_ms, sent_time_ms, visible_ms, system_latency_ms (= visible − sent_time), staleness_ms (= visible − event_time), out_of_order, batch_id` |
| `alerts` | mỗi `alert_id` trong batch | `alert_id, alert_type, learner_id, class_id, skill_id, onset_event_id, alert_event_time_ms, visible_ms, stream_id, batch_id` |

`visible_ms` lấy **sau khi** mọi lệnh Redis của batch xong (trạng thái, cảnh báo, tổng hợp lớp). Batch chạy lại sau restart ghi `visible_ms` của lần chạy hoàn tất, nên độ trễ của nó gồm cả thời gian job chết.

## Dead-letter (pipeline streaming)

Bản ghi trên `learner-events` vi phạm hợp đồng được job streaming chuyển sang topic `dead-letter` (query thứ hai trên cùng topic, checkpoint `checkpoints/<run>/_dead_letter`). Key giữ nguyên; value là JSON `{"reason", "source_topic", "partition", "offset", "kafka_timestamp_ms", "raw"}`. `reason` là lý do đầu tiên trong: `malformed_json`, `unknown_field`, `missing_field`, `schema_version`, `event_type`, `event_id`, `learner_id`, `event_time`, `sent_time`, `heartbeat_contract`, `learning_fields`, `payload`, `future_event_time`. Bản ghi lỗi bị tách trước watermark nên không làm watermark nhảy.

`future_event_time` so `event_time` với **timestamp Kafka của chính bản ghi** (không phải giờ xử lý): cả query trạng thái lẫn query dead-letter dùng cùng biểu thức, nên mỗi bản ghi vào đúng một trong hai nơi.

| Loại bản ghi | Được đi trước timestamp Kafka tối đa | Tham số |
|---|---|---|
| Sự kiện học tập | 120 s | `--max-future-s` (mặc định 120) |
| Heartbeat | W + 60 s (W = `--watermark`) | `--heartbeat-allowance-s` (mặc định 60, trong [0, 3600] ⇒ tối đa W + 1 h) |

Heartbeat của bộ phát lại đi trước đúng W + 1 s (event_time = cuối log + W + 1 s + k, gửi lúc ≈ cuối log + k), nên phần dư 60 s chỉ để bù trễ hàng đợi producer. Một máy lệch giờ dưới ngưỡng vẫn đẩy watermark tối đa chừng đó; đặt `--max-future-s` ≤ W nếu cần chặn chặt hơn.

Giới hạn của kiểm tra trực tuyến (các trường hợp sau chỉ `event_validation` bắt được, ngoại tuyến):

- trường thừa **trong `payload`** và `uniqueItems` theo nghĩa đầy đủ;
- **cùng `event_id` nhưng khác nội dung** (quy tắc 2): `dropDuplicatesWithinWatermark` giữ bản đầu và bỏ bản sau mà không so nội dung. Muốn bắt trực tuyến phải giữ băm nội dung theo `event_id` trong state. Bộ phát lại chỉ gửi lại đúng byte của bản gốc (mục "Phát lại vào Kafka"), nên trường hợp này không xuất hiện trong thí nghiệm.