# Giao thức thí nghiệm và báo cáo thống kê v0.1 (đăng ký trước khi có dữ liệu)

Viết trước khi chạy để không chọn metric/cách tổng hợp có lợi sau khi thấy kết quả (yêu cầu chung số 4 của đề). Mỗi hình dự kiến có một "figure contract" (mục 4). Ma trận cấu hình máy đọc: `configs/experiments/plan-v1.0.yaml` (v0.1 giữ làm lịch sử).

> Cập nhật 27/10/2026, v1.0 (trước khi chạy ma trận): thêm trục `mode` của Exp D, ablation có cấu trúc, mục stress (soak, trần ingestion) và quy tắc resume; lý do: runner thí nghiệm cần ma trận đầy đủ và quy tắc dừng rõ ràng. Các quy ước mới nằm cuối mục 2.

## 1. Đơn vị độc lập và số lần lặp

| Phép đo | Đơn vị độc lập (`n`) | Đo trong đơn vị | Tổng hợp báo cáo |
|---|---|---|---|
| Độ trễ đầu-cuối, lag, throughput | **Một lần chạy** (run) với seed riêng | Hàng chục nghìn sự kiện/run — **không** phải n | Mỗi run → P50, P95, P99 trên sự kiện; qua ≥ 3 run → **median và min–max** của từng chỉ số |
| Độ đúng trạng thái vs ground truth | Một run | Tỉ lệ (người học, kỹ năng) có trạng thái đúng ở cuối run | Median và min–max qua run |
| Thời gian phát hiện bế tắc | Một run | Mỗi lần bế tắc thật trong bộ mô phỏng là một quan sát | P50/P95 trong run → median qua run |
| Hint validity | Một gợi ý (mẫu phân tầng theo bậc và loại lỗi) | — | Tỉ lệ + khoảng tin cậy 95% Wilson |
| Rubric sư phạm | Một gợi ý, 2 rater độc lập (hai thành viên nhóm), chấm mù | — | Điểm trung vị theo tiêu chí; Cohen's κ (khoảng tin cậy bootstrap) |
| Chênh lệch công bằng | Một run | Chênh lệch giữa nhóm `cellular` và `wifi` | Median và min–max qua run |

Số mẫu hint cho audit: 120 (phân tầng theo bậc × loại lỗi × nguồn).

### Audit gợi ý: định nghĩa cố định trước khi chấm (27/10/2026)

Phần này chốt ngày 27/10/2026, trước khi có bất kỳ đánh giá nào. Không sửa sau khi đã xem kết quả. Hướng dẫn
chấm chi tiết, ví dụ và thiết kế mẫu đi kèm bộ phiếu chấm, nộp riêng.

- **Năm nhãn nhị phân** (`yes`/`no`) theo Scarlatos et al. [14]:
  - `correct`: không có phát biểu sai, và liên quan tới bài và biểu thức của người học;
  - `not_revealing`: không trực tiếp lộ đáp án cuối của bài;
  - `suggestion`: có đề xuất mà làm theo sẽ tiến tới đáp án đúng;
  - `diagnostic`: chỉ đúng lỗi hoặc quan niệm sai của người học;
  - `positive`: tích cực, giọng khích lệ.
- **Hint validity** của một gợi ý theo một rater = `verified` (SymPy, từ khóa giải mù) **∧** `correct = yes`
  **∧** `not_revealing = yes`. Báo tỉ lệ theo từng rater và theo cả hai rater cùng coi là hợp lệ, kèm Wilson
  95% [16].
- **Đồng thuận:** với mỗi nhãn và với validity, báo Cohen's κ (`sklearn.metrics.cohen_kappa_score`, không trọng
  số) và tỉ lệ đồng thuận thô.
  - Khoảng tin cậy 95% của κ tính bằng bootstrap phân vị: 2.000 lần lấy lại gợi ý có hoàn lại, seed 20261027.
    Lần lấy lại nào cho κ không xác định (cả hai rater chỉ dùng một nhãn) thì bị bỏ.
  - Diễn giải theo Landis & Koch [15], chỉ như quy ước.
- **Điểm rubric:** s = y_correct · (tổng 5 nhãn) / 5, theo [14]. Báo trung bình và trung vị theo rater.
- **Hai vòng:**
  - Vòng hiệu chỉnh có 20 gợi ý, không trùng bài với vòng chính và không tính vào kết quả. Kết quả vòng này
    tổng hợp ở chế độ mù, không mở khóa.
  - Vòng chính có 120 gợi ý. Mỗi rater chấm độc lập, mù nguồn sinh.
  - Rater 1 và rater 2 là hai thành viên của nhóm.
- Tỉ lệ được tính trên mẫu phân tầng, không có trọng số theo tổng thể. So sánh giữa các tầng bằng bảng phân
  nhóm.

> Cập nhật 27/10/2026, sau khi thử bộ sinh trên phiếu hiệu chỉnh và trước khi rút phiếu vòng chính (commit
> `884280f`): khi bước tiếp theo lộ đáp án mà chưa được phép đưa đáp án, gợi ý bậc 3 của bộ sinh `ladder` không
> còn rơi về câu mẫu chung của kỹ năng (một ví dụ giải trên bài khác) mà đưa **dạng** của chính bước đó, số thay
> bằng ô □. Lý do: một gợi ý chỉ được khẳng định điều gì về bước tiếp theo của bài khi điều đó suy ra từ sự thật
> đã kiểm của chính bài. Rubric 5 nhãn, định nghĩa hint validity, cỡ mẫu 120 và thiết kế phân bổ không đổi; tầng
> nguồn của ô ladder bậc 3 tách theo nguồn thực tế đã sinh (`ladder` và `ladder+locked`), tỉ lệ với tổng thể như
> trước.
> Vòng chính được rút lại từ bộ audit bản 2, sau khi bỏ 20 bài của vòng hiệu chỉnh khỏi tổng thể. Cùng lúc, gợi ý
> bị khóa không có loại lỗi bỏ câu "làm theo đúng bước dưới đây" (sai khi bước đang bị che). Các run thí nghiệm có
> gọi dịch vụ gợi ý (Exp E, ablation `hint_generator`) chạy sau commit này.

> Cập nhật 27/10/2026, về người chấm: hai thành viên nhóm chấm cả hai vòng từ tệp trống, theo đúng phiếu,
> rubric, định nghĩa validity và thiết kế mẫu ở trên (không đổi). Kết quả audit chính thức: đang chấm.

## 2. Quy ước cố định

- **Warm-up:** loại 60 s đầu mỗi run khỏi mọi chỉ số độ trễ/throughput; ghi rõ trong chú thích hình.
- **Thời lượng:** 10 phút/run (5 phút với thí nghiệm E). Run bị timeout/lỗi vẫn ghi vào `results/raw/runs.jsonl` với `status` tương ứng, **không xóa**.
- **Độ trễ — hai định nghĩa:** cả hai đo tại foreachBatch, sau khi mọi lệnh Redis của batch xong (`visible_ms`), cho từng sự kiện đã áp (`lake/<run>/latency/`):
  - **độ trễ hệ thống** = visible − `sent_time` (SLO P95 ≤ 5 s ở 1K ev/s; Exp A/B/E);
  - **độ cũ trạng thái** = visible − `event_time` (gồm độ trễ mạng mô phỏng; RQ1/RQ3).

  P95 trượt 1 phút của cả hai có trong `progress.jsonl` (`latency_1m`) và `metrics:pipeline`. Time-to-detect bế tắc = lúc cảnh báo hiện lần đầu (stream id của `alerts:stream`) trừ `sent_time`/`event_time` của sự kiện khởi phát; ground truth dịch bằng `replay_start_ms`. Một máy ⇒ chung đồng hồ; nếu nhiều máy, ghi độ lệch NTP.
- **Chế độ `ordered`** đệm sự kiện tới khi watermark vượt qua, nên độ trễ hệ thống ≥ W theo thiết kế; SLO 5 s áp cho chế độ `eager`. So sánh hai chế độ là một phần của RQ3 (đánh đổi độ đúng – độ trễ).
- **Exp D (chế độ `none`):** không có watermark và timer, nên không có cảnh báo `inactivity` và trạng thái kỹ năng/bài không hết hạn (chỉ tỉa `seen_event_ids` theo event time); đủ cho run ≤ 30 phút.
- **Throughput bền vững:** tốc độ gửi cao nhất mà consumer lag không tăng trong 5 phút cuối run (theo định nghĩa sustainable throughput của Karimov et al. [8]).
- **Tốc độ gửi thực tế** của bộ sinh tải luôn được ghi; nếu thấp hơn mục tiêu > 5% thì run bị đánh dấu không hợp lệ (đo nhầm bộ sinh tải).
- **Không dùng p-value** với n = 3 run. So sánh bằng tỉ số median (vd. P95 streaming / P95 batch) kèm min–max; không dùng chữ "có ý nghĩa thống kê".
- **Đa so sánh:** các cấu hình được so mô tả, không kiểm định; không chọn cấu hình "tốt nhất" mà không nêu đánh đổi.
- Mỗi run ghi đủ trường Phụ lục B bằng `ai_math_tutor.run_record`.
- **Chế độ theo thí nghiệm:** A, B, C, E và ablation chạy `eager`, W = 2 phút. Exp D (v1.0) = `mode ∈ {eager, ordered}` × `watermark ∈ {30 s, 2 phút, 10 phút}` × `late_profile ∈ {low, high}` + `mode = none` × `late_profile` = 14 cấu hình.
- **Cấu hình dùng chung:** cấu hình xuất hiện ở nhiều thí nghiệm là một cấu hình, chạy một lần mỗi seed; run record ghi `parameters.groups`. Ablation vì vậy dùng đúng dữ liệu và seed của run gốc.
- **Warm-up** tính theo `sent_time`: sự kiện gửi trong 60 s đầu kể từ `replay_start` bị loại. Cửa sổ đo = [`replay_start` + 60 s, cuối phần phát lại].
- **Throughput bền vững** (vận hành hóa): độ dốc hồi quy tuyến tính của processing lag (end offset mới nhất − offset batch đã đọc) trong 5 phút cuối phần phát lại ≤ 1% tốc độ mục tiêu.
- **Drain/timeout:** run xong khi có batch đo sau khi phát lại xong với lag = 0 và watermark ≥ cuối log (chế độ none: chỉ lag = 0), hạn 2·W + 2 chu kỳ trigger + 60 s; giới hạn cứng cho phần phát lại = thời lượng + 2·W + 5 phút; quá hạn ⇒ `timeout`.
- **Resume:** run `success` được bỏ qua; `failed`/`timeout` (và `aborted` không do runner bị dừng, vd. `state_source=default` ở Exp E) là một lần thử; tối đa 3 lần thử rồi `abandoned` (ghi lý do trong `results/raw/experiment-abandoned.jsonl`); `aborted` do runner bị dừng không tính.
- **Stress:** soak 30 phút ở 1K ev/s (1 run; RSS 5 phút đầu/cuối, tỉ lệ cửa sổ P95 1 phút vượt SLO) và trần ingestion 30K/60K ev/s × P = 1/4/8 (1 run mỗi ô; tốc độ thực < mục tiêu là kết quả đo `sustained=false`, không làm run hỏng).
- **Kho bài của bộ mô phỏng:** mọi run streaming/ingestion mô phỏng trên **tập test 1.000 bài** của kho v2 (`problem_bank`, `problem_split` trong `plan-v1.0.yaml`; thẻ `pbv2test` nằm trong run_id và khóa dữ liệu). Cập nhật 27/10/2026: hai run Exp A đầu tiên (`st-l1000-r100-p1-…-600s-s11/-s22-20261027T14…`, không có thẻ `pb…`) chạy nhầm kho mini 13 bài của bộ mô phỏng; ma trận được dừng, sửa và chạy lại từ đầu. Hai bản ghi đó giữ trong `runs.jsonl` (chỉ ghi nối) nhưng không dùng cho kết quả.
- **Môi trường đo:** ma trận chạy khi máy không có tác vụ nặng khác; distro WSL được giữ chạy suốt đợt đo để Kafka/Redis không bị khởi động lại giữa chừng.

## 3. Bản nháp đoạn "Thiết lập thí nghiệm / Statistical analysis" (dán vào báo cáo, sửa số khi có)

> Mỗi cấu hình được chạy 3 lần độc lập với seed khác nhau; một lần chạy là đơn vị phân tích. Trong mỗi lần chạy, độ trễ đầu-cuối của từng sự kiện được tóm tắt bằng phân vị 50, 95 và 99 sau khi loại 60 giây khởi động; giữa các lần chạy, nhóm báo cáo trung vị và khoảng min–max của các phân vị này. Vì chỉ có 3 lần chạy mỗi cấu hình, nhóm không thực hiện kiểm định giả thuyết và chỉ so sánh mô tả. Độ đúng trạng thái được tính so với ground truth của bộ mô phỏng. Gợi ý được đánh giá trên mẫu phân tầng, do hai rater độc lập chấm mù theo rubric; mức đồng thuận được báo bằng Cohen's κ. Phần mềm: Python `AUTHOR_INPUT_NEEDED`, PySpark `AUTHOR_INPUT_NEEDED`, Kafka `AUTHOR_INPUT_NEEDED` (lấy tự động từ bản ghi run).

## 4. Figure contracts (hình dự kiến trong báo cáo)

Kết luận cốt lõi để trống cho đến khi có dữ liệu; dưới đây là **câu hỏi** mà mỗi hình phải trả lời, không phải kết luận.

| Hình | Câu hỏi cấp Results | Panel | Bằng chứng chính / phụ | Thống kê | Rủi ro bị phản biện |
|---|---|---|---|---|---|
| H1 | Hệ thống gồm những đường xử lý nào? | Sơ đồ kiến trúc vector (vẽ bằng code) | — | — | Sơ đồ khác code thật → vẽ lại sau tuần 6 |
| H2 (RQ1, hình chính) | Streaming rút ngắn thời gian phát hiện bế tắc bao nhiêu và tốn thêm bao nhiêu tài nguyên? | a: thời gian phát hiện (log) theo cấu hình batch ngày → ASAP; b: CPU/RAM cùng cấu hình | a chính; b đánh đổi | Điểm = median qua 3 run, thanh = min–max | Batch ngày tính bằng lịch chạy + runtime đo được — ghi rõ cách tính |
| H3 | Hệ thống mở rộng theo tải và số partition thế nào? | a: P95 theo ev/s × partitions; b: consumer lag; c: tốc độ gửi thực tế | a chính; c kiểm soát | như H2 | Một máy ⇒ đo song song hóa, không đo phân tán |
| H4 (RQ3) | Watermark đánh đổi độ đúng và độ trễ ra sao, ai chịu thiệt? | a: độ đúng trạng thái theo watermark; b: tách `wifi`/`cellular` | b là bằng chứng công bằng | như H2 | Hồ sơ trễ do nhóm tự đặt → phân tích độ nhạy 2 hồ sơ |
| H5 (RQ2) | Gợi ý đúng, có ích và đủ nhanh không? | a: validity theo bậc; b: điểm rubric; c: P95 API theo số yêu cầu đồng thời | a, b chính | Wilson CI; κ | Chấm mù nguồn; nêu rõ quy trình chấm |

Hình sinh bằng script từ `results/raw/` vào `results/figures/` (Python, xuất PDF + PNG), không chỉnh tay.
