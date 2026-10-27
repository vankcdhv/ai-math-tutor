# Minh chứng tuần 5: sự kiện muộn — hồ sơ trễ low/high × chế độ eager/ordered

**Kết luận:** với W = 30 s, gần như mọi sự kiện "muộn" của bộ mô phỏng (gửi sau sự kiện > 30 s) bị loại theo watermark, ở cả hai chế độ. Tỉ lệ bị loại cao hơn hẳn ở người học dùng `cellular`. Trên phần sự kiện không bị loại:
- `ordered` khớp oracle 100%, đổi lại độ trễ hệ thống P95 ≈ 44,5 s;
- `eager` có độ trễ P95 2,0–2,2 s nhưng chỉ khớp oracle 89,5% (trễ high) và 96,9% (trễ low), vì áp sai thứ tự.

Đây là đánh đổi của RQ3: đúng thứ tự thì tốn khoảng W (+ thời gian batch) độ trễ.

Ngày đo: 27/10/2026. Tệp số đo gốc: `$NHOM8_DATA_ROOT/it-evidence/late-events-20261027T121232Z.json` (máy thí nghiệm).

## Cách làm

Test: `tests/integration/test_restart.py::test_late_events_eager_vs_ordered`, chạy bằng `uv run pytest -q -s -m integration tests/integration/test_restart.py -k late_events` sau `make up`.

- **Dữ liệu:** mỗi hồ sơ trễ gồm 80 người học, 60 s, 150 ev/s, seed 41, 5% gửi trùng, hồ sơ `low` hoặc `high` (`simulator.network.LATE_PROFILES`).
- **Phát lại:** bộ phát lại thật, thời gian thực, cửa sổ 75 s, heartbeat 5 s vào cả 3 partition.
- **Job:** hai job (`eager` và `ordered`) khởi động **trước** rồi đọc cùng topic, chạy **đồng thời** với bộ phát lại. Độ trễ đo được vì vậy là độ trễ thật, không phải tồn đọng.
  - Cấu hình chung: W = 30 s, model `rule`, 3 Kafka partition = 3 shuffle partition, trigger ASAP.
  - Hai Spark cùng chạy trên một máy (i9-14900K, WSL2), nên số đo độ trễ có phần tranh chấp CPU giữa hai job.
- **Đối soát:** `streaming.reconcile` tính các đại lượng sau:
  - bị loại = sự kiện duy nhất của cửa sổ − đã áp (theo `client.connection`);
  - trạng thái so oracle (chỉ sự kiện không bị loại, đúng thứ tự);
  - thành thạo theo luật so thành thạo ẩn;
  - time-to-detect bế tắc so ground truth, dịch bằng `replay_start_ms`.
- **Hai định nghĩa thời gian**, đo tại foreachBatch sau khi Redis ghi xong:
  - độ trễ hệ thống = visible − `sent_time`;
  - độ cũ trạng thái = visible − `event_time`.

## Kết quả

### Bị loại theo watermark (giống nhau ở hai chế độ: loại xảy ra trước processor)

| Hồ sơ | Lần giao | Duy nhất | Trùng có chủ đích | Đã áp | Bị loại | cellular: loại / cửa sổ | wifi: loại / cửa sổ | Sự kiện muộn (ground truth) bị loại |
|---|---|---|---|---|---|---|---|---|
| low | 9.361 | 8.887 | 474 | 8.863 | 24 | 21 / 3.139 (0,67%) | 3 / 5.748 (0,05%) | 24 / 24 (cellular 21, wifi 3) |
| high | 9.009 | 8.565 | 444 | 8.515 | 50 | 37 / 2.856 (1,30%) | 13 / 5.709 (0,23%) | 50 / 51 (1 sự kiện muộn cellular vẫn kịp) |

Đối chiếu với Spark: tổng `numRowsDroppedByWatermark` của `dedupeWithinWatermark` là 27 (low) và 58 (high). Số này lớn hơn số sự kiện bị loại (24, 50) đúng bằng số bản trùng của chính các sự kiện đó (3, 8). Tổng `numDroppedDuplicateRows` là 471 = 474 − 3 và 436 = 444 − 8. Hai cách đếm khớp nhau: mọi lần giao thừa đều bị bỏ đúng một lần, hoặc vì trùng, hoặc vì muộn.

### Độ đúng trạng thái và độ trễ

| Hồ sơ / chế độ | Khớp oracle | Trường lệch (số cặp) | Áp sai thứ tự | Hệ thống P50 / P95 / P99 (ms) | Độ cũ P50 / P95 / P99 (ms) |
|---|---|---|---|---|---|
| low / eager | 599/618 (96,9%) | streak 19, consec_wrong 1, mastered 1 | 204 | 1.295 / 2.247 / 3.449 | 1.431 / 2.816 / 3.745 |
| low / ordered | 618/618 (100%) | — | 0 | 32.463 / 44.383 / 46.859 | 32.595 / 44.549 / 47.004 |
| high / eager | 553/618 (89,5%) | streak 61, consec_wrong 9, mastered 6, stuck 3 | 552 | 1.362 / 2.011 / 3.093 | 1.558 / 3.321 / 6.033 |
| high / ordered | 618/618 (100%) | — | 0 | 32.491 / 44.598 / 47.168 | 32.693 / 44.906 / 47.361 |

- Eager: độ cũ lớn hơn độ trễ hệ thống đúng bằng độ trễ mạng mô phỏng (sent − event). Đuôi P99 của độ cũ (6,0 s ở hồ sơ high) là sự kiện cellular đến chậm nhưng vẫn trong W.
- Ordered: sự kiện chỉ được áp khi watermark (= max event_time − 30 s) vượt `event_time` của nó. Độ trễ P50 vì vậy ≈ W + 2,5 s (thời gian batch và nhịp timer). P95 44 s cao hơn W khoảng 14 s, vì watermark chỉ tiến khi có batch có dữ liệu mới, và timer bắn ở batch sau đó.
- P95 độ trễ hệ thống của eager (2,0–2,2 s ở 150 ev/s, hai job chạy cùng lúc) nằm dưới SLO 5 s. SLO chính thức đo ở 1K ev/s (Exp A/B của ma trận thí nghiệm).

### Cảnh báo bế tắc so oracle và ground truth

| Hồ sơ / chế độ | Onset oracle / hệ thống / khớp | Episode ground truth trong cửa sổ | Phát hiện (recall) | alert_id / episode (0 · 1 · ≥2) | Time-to-detect hệ thống P50 / P95 (ms) | Time-to-detect độ cũ P50 / P95 (ms) |
|---|---|---|---|---|---|---|
| low / eager | 117 / 117 / 115 | 120 | 115 (95,8%) | 5 · 115 · 0 | 1.299 / 1.966 | 1.404 / 2.236 |
| low / ordered | 117 / 117 / 117 | 120 | 117 (97,5%) | 3 · 117 · 0 | 32.767 / 45.461 | 32.873 / 45.495 |
| high / eager | 111 / 115 / 102 | 118 | 104 (88,1%) | 14 · 104 · 0 | 1.409 / 3.128 | 1.551 / 3.822 |
| high / ordered | 111 / 111 / 111 | 118 | 107 (90,7%) | 11 · 107 · 0 | 32.709 / 44.149 | 32.867 / 44.363 |

- Không episode nào có từ 2 `alert_id` trở lên. `alerts:stream` không có mục trùng ở cả bốn run.
- Ordered khớp mọi onset của oracle. Eager vừa sinh onset sai (13 thừa ở hồ sơ high) vừa bỏ sót (9), do áp sai thứ tự làm đổi chuỗi đúng/sai.
- Ground truth tính trên chuỗi **đầy đủ**, nên episode nào có sự kiện bị loại đều có thể không được phát hiện. Đây là lý do chính làm recall của ordered < 100% (3 và 11 episode). Thêm vào đó, ở chế độ ordered, 8 (low) và 22 (high) episode được phát hiện với onset khác ground truth, vì chuỗi hệ thống thiếu sự kiện bị loại.

### Thành thạo theo luật so thành thạo ẩn (cuối cửa sổ)

| Hồ sơ / chế độ | Độ đúng | tp / fp / fn / tn |
|---|---|---|
| low / eager | 97,9% | 588 / 1 / 12 / 17 |
| low / ordered | 98,1% | 589 / 1 / 11 / 17 |
| high / eager | 95,1% | 571 / 1 / 29 / 17 |
| high / ordered | 95,8% | 575 / 1 / 25 / 17 |

## Giới hạn

- Mỗi ô chỉ một lần chạy 60 s ở 150 ev/s, một seed. Ma trận thí nghiệm lặp 3 lần ở tải thí nghiệm.
- Hai job chạy đồng thời trên một máy, nên độ trễ tuyệt đối bị tranh chấp CPU. So sánh eager và ordered vẫn công bằng vì hai job thấy cùng thứ tự đến và cùng tải.
- "Muộn" là theo watermark **toàn cục** (max event_time mọi partition − W). Ngưỡng muộn của bộ mô phỏng (30 s) trùng với W, nên tỉ lệ bị loại gần bằng tỉ lệ muộn. Với W khác, tỉ lệ sẽ khác (trục của Exp D).
- Chế độ `none` (không watermark, khử trùng trong state) không có trong báo cáo này.
