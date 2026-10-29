# Data card v0.4

Thẻ dữ liệu của dự án gồm bốn nguồn, mỗi nguồn một mục. Lịch sử phiên bản:

| Phiên bản | Nội dung thêm |
|---|---|
| v0.1 | Dữ liệu mẫu tuần 1 (mục 1) |
| v0.2 | Ứng viên bài toán từ NuminaMath-CoT (mục 2) |
| v0.3 | Dữ liệu mô phỏng người học có ground truth (mục 3) |
| v0.4 | Kho bài v2 có lời giải từng bước và phiếu rà soát (mục 4); mục 2 cập nhật lượt lấy thêm ứng viên |

Không nguồn nào chứa dữ liệu người thật.

## 1. Dữ liệu mẫu tuần 1

| Mục | Nội dung |
|---|---|
| Tên | `events-week1-sample.jsonl`, `problems-week1-sample.jsonl` |
| Phiên bản | `events-week1-sample@0.1` (schema LearnerEvent 0.1) |
| Cách tạo | Viết tay theo kịch bản, sinh tất định bằng `scripts/make-week1-sample.py`; không có dữ liệu người thật |
| Kích thước | 20 dòng sự kiện (19 duy nhất), 3 người học, 3 bài toán |
| Mục đích | Minh họa data contract; ca kiểm thử cho trùng, muộn, đảo thứ tự, nhãn toán học |
| Không dùng cho | Đo hiệu năng, huấn luyện hoặc kết luận về hành vi học |
| Kho bài | 3 bài nhóm tự biên soạn (phương trình bậc nhất, bậc hai), đáp án kiểm bằng SymPy |
| Quyền riêng tư | Bút danh HMAC với salt mẫu công khai (cố ý, chỉ cho mẫu); không PII |
| Giấy phép | Nhóm tự tạo, dùng cho môn học |
| Hạn chế | Quá nhỏ để thống kê; hành vi do người viết kịch bản quyết định |
| Kế hoạch | Tuần 2: bộ mô phỏng có ground truth (BKT) + chèn trùng/muộn có tham số. Đã thực hiện, xem mục 3 |

## 2. NuminaMath-CoT (nguồn ứng viên cho tập test/dev)

| Mục | Nội dung |
|---|---|
| Tên | `AI-MO/NuminaMath-CoT`, tách thành `candidates.parquet` (ngoài repo, dưới `$NHOM8_DATA_ROOT/numina/`); mẫu trong repo: `data_sample/numina-candidates-sample.jsonl` (30 dòng) |
| Phiên bản | Revision ghim `9d8d210c9f6a36c8f3cd84045668c9b7800ef517` (2024-11-25). Chạy lại cho ra cùng byte |
| Giấy phép | Apache License 2.0, kiểm ở 3 nơi: HF API `cardData`, tag, và YAML của README tại đúng revision |
| Ghi công | Jia LI, Edward Beeching, Lewis Tunstall và cộng sự (AI-MO, 2024) |
| Phạm vi dùng | Chỉ split train (859.494 dòng) và 4 nguồn gsm8k, orca_math, synthetic_math, math (336.007 dòng) |
| Cách tạo | `scripts/extract-numina-subset.py`. Đáp án là `\boxed{}` cuối cùng (đếm ngoặc cân bằng); chỉ giữ số hữu tỉ hoặc tập nghiệm ≤ 2 số. Loại thêm đề có hình `[asy]`, đề dùng ký hiệu chữ nhưng không phải phương trình một ẩn bậc ≤ 2, đề trùng lặp, đề lẫn lời thoại của mô hình sinh. Còn 192.802 dòng đạt lọc |
| Lấy mẫu | Phân tầng theo (nguồn × loại), seed 20261027, được 3.000 ứng viên (2.645 bài số học, 355 phương trình). Lượt lấy thêm cho kho v2: +1.000 gsm8k và +2.500 orca_math số học, chỉ từ các dòng chưa chọn, seed riêng từng tầng; 3.000 dòng ban đầu giữ nguyên (tổng 6.500) |
| Trường | Đề và lời giải giữ nguyên văn; `kind`, `answer`, `equation` là trường do nhóm suy ra |
| Minh bạch | Số dòng bị loại theo từng lý do, sha256 từng file và quy tắc chuẩn hóa nằm ở `data_sample/numina-candidates-manifest.json` |
| Hạn chế | synthetic_math và orca_math là dữ liệu do mô hình sinh, nên đáp án có thể sai hoặc chỉ là số làm tròn. Lời giải từng bước viết sau phải qua kiểm SymPy; bài không đạt bị loại |

## 3. Dữ liệu mô phỏng người học

| Mục | Nội dung |
|---|---|
| Tên | Lịch sự kiện mô phỏng `events.parquet` + `ground_truth.jsonl` + `roster.json` + `manifest.json` |
| Phiên bản | simulator 1.0.0, schema LearnerEvent 0.1, ground truth v1. Khi phát lại vào Kafka, bộ phát lại thêm heartbeat (LearnerEvent 0.2) sau cuối log: bản ghi kỹ thuật đẩy watermark, không phải dữ liệu học tập, bị lọc trước mọi thống kê |
| Cách tạo | `ai_math_tutor.simulator.cli`, tất định theo seed (checksum trong manifest). BKT ẩn theo (người học, kỹ năng), tham số lấy trong `configs/skills.yaml` theo nhóm năng lực low/medium/high (25/50/25%); bước sai lấy từ misconception đã kiểm SymPy |
| Lỗi mạng chèn | Theo `client.connection` (wifi 60% / cellular 40%): độ trễ log-normal; muộn (> 30 s) low: 0,2% / 3%, high: 1% / 12%; trùng nguyên byte theo `--duplicate-rate` |
| Ground truth | Mastery ẩn theo thời gian, episode bế tắc (luật 10 cơ hội / 3 đúng liên tiếp áp trên chuỗi thật), sự kiện bị chèn trùng/muộn — tệp riêng, không vào luồng sự kiện |
| Quyền riêng tư | Không người thật; bút danh HMAC với salt mô phỏng công khai, `source_id` giả không ra file |
| Không dùng cho | Kết luận sư phạm; hiệu ứng gợi ý lên học (+0.05) là giả định mô hình |
| Hạn chế | Hành vi do tham số nhóm tự đặt ("mô phỏng quá đẹp"); muộn độc lập từng sự kiện (không mô phỏng mất mạng theo đợt); một người học một kết nối cố định; mô hình kết hợp cho bài nhiều kỹ năng; luật bế tắc gắn cờ cả người vừa thành thạo ẩn (47% episode ở mẫu 1K × 1 ngày) — báo song song mastery ẩn |

## 4. Kho bài v2 (lời giải từng bước)

| Mục | Nội dung |
|---|---|
| Tên | `data_sample/problem-bank-v2.jsonl` (1.260 bài), manifest `problem-bank-v2-manifest.json` (checksum từng split), chỉ mục bài liên quan `problem-index-v2.json` |
| Tập test | 1.000 bài NuminaMath-CoT (revision ghim ở mục 2, Apache-2.0): 720 bài số học lời văn chỉ từ gsm8k và orca_math (360 + 360), 280 phương trình bậc ≤ 2 (synthetic_math 241, orca_math 23, math 16) |
| Tập dev | 200 bài NuminaMath cùng phân bố + 60 bài nhóm tự soạn. Gợi ý "bài liên quan" chỉ trả bài dev |
| Cách tạo | `scripts/write-step-solutions.py` → `build-problem-bank.py` (`make problem-bank`), tất định: dựng lại cho cùng byte. `solution_source` của test: tách phép tính từ CoT 706, SymPy sinh bước phương trình 280, viết tay 14 |
| Kiểm máy | Mọi bước: phép tính đúng chính xác (số hữu tỉ), phương trình giữ nguyên tập nghiệm, bước cuối bằng đáp án `\boxed{}`; mỗi bài NuminaMath ≥ 2 bước không tầm thường. Bài số học còn phải dùng mọi số liệu của đề và mọi kết quả trung gian (mỗi lần một vai), không có bước đồng nhất, kết quả âm, hay CoT tự viết đẳng thức sai |
| Hiệu suất lọc | gsm8k 1.156/1.662, orca_math 442/3.161, phương trình 353/355; số học `math`/`synthetic_math` bị loại theo nguồn (1.322). Lý do loại khác: quá ít bước 817, thiếu mắt xích 768, không tách được bước 555, không ra đáp án 474, phụ thuộc làm tròn 393, số liệu không dùng 137, bước rẽ nhánh 73, đại lượng âm 9, CoT tính sai 1 |
| Ẩn | Chuỗi máy (`equation`, `steps`, misconception) luôn theo ẩn `x` vì bộ kiểm chỉ nhận `x`; ẩn gốc của đề ở trường `variable`, chỉ dùng để hiển thị |
| Đề tiếng Việt | Mọi bài có `statement_vi`. Bài tự soạn: đề viết bằng tiếng Việt. 1.200 bài NuminaMath: bản dịch (thêm ngày 28/10/2026, `data/problems/numina-statements-vi.jsonl`), đề gốc tiếng Anh giữ ở `statement`. `build-problem-bank.py` từ chối bản dịch có tập số khác đề gốc (số của đề không bị coi là lộ đáp án, nên số mới có thể làm lọt đáp án) hoặc làm mất đoạn `$…$` của bài phương trình; tiền viết "N đô la". Bước giải, đáp án và chỉ mục bài liên quan không đổi (bản dịch không vào chỉ mục), nên các số đo thí nghiệm và mẫu audit trước ngày này vẫn áp dụng; checksum split đổi vì thêm trường |
| Kho v1 | Các nhóm lỗi tìm thấy khi kiểm một mẫu kho v1 dẫn tới việc sửa cách sinh lời giải ở v2. Kho v1 đã bị thay nên không có kết quả rà soát chính thức |
| Rà soát vòng 2 (kho v2) | Quy trình: mẫu tất định 100 bài test (seed 20261027; 69 số học, 31 phương trình); hai thành viên nhóm chấm độc lập từng bài `yes`/`no` theo tiêu chí lời giải hợp lý, đúng đề, mỗi `no` kèm ghi chú; tổng hợp tỉ lệ đạt từng người, đạt chung, bất đồng, Cohen's κ và Wilson 95%. Cách chấm thực tế (khác kế hoạch chấm từ phiếu trống): theo yêu cầu của nhóm, mỗi người nhận phiếu Excel đã điền sẵn nhãn nháp (hai bản nháp độc lập, người 1 nhận bản 1, người 2 nhận bản 2), đọc từng bài và sửa nhãn mình không đồng ý, làm độc lập; so với bản nháp, người 1 sửa 0 ô, người 2 sửa 2 ô. Kết quả (nhãn do người rà và sửa): người 1 đạt 98/100 = 98,0%, người 2 đạt 96/100 = 96,0%, đạt chung 96/100 = 96,0% (Wilson 95%: 90,2–98,4%), 2 bất đồng, Cohen's κ = 0,658; ngưỡng đạt chung 95% → **đạt**, dù cận dưới Wilson vẫn dưới 95%. κ có thể bị đẩy cao vì hai người cùng bắt đầu từ bản nháp |
| Lỗi đã biết | P-NTA-497: đề orca_math mua bán lại xe đặt sai (khoản mua lại bị tính như lãi). P-NTE-049: bước 1 bỏ qua khai triển bình phương và chia hai vế cho 3, chuỗi dừng ở dạng tích. Hai bài giữ nguyên trong test để tập bài và lời giải khớp phiếu rà soát |
| Hạn chế | Đề orca_math có thể tự mâu thuẫn mà bộ kiểm tự động không bắt được; một kết quả trung gian có thể tình cờ trùng một đại lượng chưa tính; ước lượng tỉ lệ lỗi dựa trên mẫu 100 bài |