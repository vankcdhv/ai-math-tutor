# Tài liệu tham khảo đã kiểm chứng (v0.1 — 27/10/2026)

Quy trình kiểm chứng: phân giải DOI qua Crossref API, so từng trường tác giả/tiêu đề/năm/tập/trang; bài không có DOI thì đối chiếu arXiv API hoặc đọc trực tiếp PDF gốc. **Không dùng số liệu từ báo cáo tổng hợp thứ cấp** khi chưa đối chiếu với nguồn gốc.

## Kết quả kiểm chứng: 16 mục

Mục [14]–[16] (audit gợi ý) được thêm ngày 27/10/2026 và kiểm theo cùng quy trình: Crossref API, arXiv API kèm
bản HTML của bài, và PubMed E-utilities cho số trang.

- ✅ Verified: 12
- ⚠️ Check suggested: 4
- ❌ Needs fix: 0
- ❓ Unverifiable: 0

### ⚠️ Cần lưu ý khi trích
| # | Vấn đề | Chi tiết | Cách trích của nhóm |
|---|---|---|---|
| [1] | Năm theo tập ≠ năm Crossref | Tập 4(4) thường được trích là 1994; Crossref ghi `issued` = 1995 | Ghi 1995 theo Crossref |
| [2] | Nơi công bố chưa kiểm qua API | arXiv xác nhận tác giả/tiêu đề/ngày 19/06/2015; arXiv không ghi nơi đăng | Trích bản arXiv |
| [3] | Thiếu số tập LNCS trong Crossref | Crossref có tiêu đề, tác giả, năm, trang 431–440; không trả số tập | Trích kèm DOI, không ghi số tập |
| [15] | Crossref thiếu trang cuối; chưa đọc bản gốc | Crossref ghi `page` = "159" (tháng 3/1977). PubMed (PMID 843571) ghi trang "159-74". Bản gốc trên JSTOR không truy cập được. Các dải diễn giải κ (0–0,20 slight, 0,21–0,40 fair, 0,41–0,60 moderate, 0,61–0,80 substantial, 0,81–1 almost perfect) lấy qua trang Wikipedia "Cohen's kappa" (nguồn thứ cấp) | Trích trang 159–174 theo PubMed. Dùng các dải chỉ như quy ước, ghi rõ là diễn giải tùy ý |

## Danh mục (dùng trong báo cáo, kiểu đánh số)

**Công trình liên quan trực tiếp (8 nguồn, đáp ứng yêu cầu 3–8 nguồn của Phụ lục C):**

1. A. T. Corbett, J. R. Anderson. Knowledge tracing: Modeling the acquisition of procedural knowledge. *User Modeling and User-Adapted Interaction*, 4(4):253–278, 1995. doi:10.1007/BF01099821 — ⚠️ năm
2. C. Piech, J. Spencer, J. Huang, S. Ganguli, M. Sahami, L. Guibas, J. Sohl-Dickstein. Deep Knowledge Tracing. arXiv:1506.05908, 2015. — ⚠️ nơi đăng
3. J. E. Beck, Y. Gong. Wheel-spinning: Students who fail to master a skill. *AIED 2013*, Lecture Notes in Computer Science, pp. 431–440, 2013. doi:10.1007/978-3-642-39112-5_44 — ⚠️ số tập
4. N. Matsuda, S. Chandrasekaran, J. Stamper. How quickly can wheel spinning be detected? *EDM 2016*. https://www.educationaldatamining.org/EDM2016/proceedings/paper_66.pdf — ✅ đọc từ PDF gốc
5. Z. A. Pardos, S. Bhandari. ChatGPT-generated help produces learning gains equivalent to human tutor-authored help on mathematics skills. *PLOS ONE*, 19(5):e0304013, 2024. doi:10.1371/journal.pone.0304013 — ✅
6. M. Armbrust, T. Das, J. Torres, B. Yavuz, S. Zhu, R. Xin, et al. Structured Streaming: A declarative API for real-time applications in Apache Spark. *SIGMOD 2018*, pp. 601–613. doi:10.1145/3183713.3190664 — ✅ (Crossref chỉ lưu phần tiêu đề chính "Structured Streaming")
7. T. Akidau, R. Bradshaw, C. Chambers, S. Chernyak, R. Fernández-Moctezuma, R. Lax, et al. The Dataflow Model: A practical approach to balancing correctness, latency, and cost in massive-scale, unbounded, out-of-order data processing. *PVLDB*, 8(12):1792–1803, 2015. doi:10.14778/2824032.2824076 — ✅
8. J. Karimov, T. Rabl, A. Katsifodimos, R. Samarev, H. Heiskanen, V. Markl. Benchmarking distributed stream data processing systems. *ICDE 2018*, pp. 1507–1518. doi:10.1109/ICDE.2018.00169 — ✅ (DOI viết hoa trả lỗi qua API; tìm theo tiêu đề trả đúng bản ghi)

**Công cụ và kỹ thuật:**

9. A. Meurer, C. P. Smith, M. Paprocki, O. Čertík, S. B. Kirpichev, M. Rocklin, et al. SymPy: symbolic computing in Python. *PeerJ Computer Science*, 3:e103, 2017. doi:10.7717/peerj-cs.103 — ✅
10. L. Sweeney. k-anonymity: A model for protecting privacy. *Int. J. Uncertainty, Fuzziness and Knowledge-Based Systems*, 10(5):557–570, 2002. doi:10.1142/S0218488502001648 — ✅
11. Apache Kafka documentation. https://kafka.apache.org/documentation/ (Phụ lục D [11]) — ✅ truy cập 27/10/2026
12. Apache Spark. Structured Streaming Programming Guide và trang "Components of a TransformWithState Query" (Spark 4.2.0). https://spark.apache.org/docs/latest/streaming/ — ✅ truy cập 27/10/2026
13. PySpark API reference: `DataFrame.dropDuplicatesWithinWatermark`. https://spark.apache.org/docs/latest/api/python/reference/pyspark.sql/api/pyspark.sql.DataFrame.dropDuplicatesWithinWatermark.html — ✅ truy cập 27/10/2026

**Đánh giá gợi ý (audit):**

14. A. Scarlatos, D. Smith, S. Woodhead, A. Lan. Improving the validity of automatically generated feedback via reinforcement learning. *Artificial Intelligence in Education (AIED 2024)*, Lecture Notes in Computer Science, pp. 280–294. Springer Nature Switzerland, 2024. doi:10.1007/978-3-031-64302-6_20 (arXiv:2403.01304) — ✅ (Crossref và arXiv khớp tác giả, tiêu đề, trang và DOI; Crossref không ghi số tập LNCS)
15. J. R. Landis, G. G. Koch. The measurement of observer agreement for categorical data. *Biometrics*, 33(1):159–174, 1977. doi:10.2307/2529310 — ⚠️ trang cuối, dải diễn giải
16. E. B. Wilson. Probable inference, the law of succession, and statistical inference. *Journal of the American Statistical Association*, 22(158):209–212, 1927. doi:10.1080/01621459.1927.10502953 — ✅

## Dữ kiện đã trích từ nguồn gốc (được phép dùng trong báo cáo)

| Dữ kiện | Nguồn |
|---|---|
| Wheel spinning do Beck & Gong (2013) định nghĩa: không thành thạo kỹ năng sau 10 cơ hội luyện tập; cách vận hành thường dùng: không đúng 3 lần liên tiếp trong 10 cơ hội đầu | [3]; cách vận hành được Ayyappan dẫn lại từ Zhang và cộng sự (2019) — chưa đọc bài Zhang gốc |
| Bộ phát hiện BKT + mạng nơ-ron: recall 0.79, precision 0.25 trên dữ liệu Geometry Cognitive Tutor, nhãn do người gán | [4], phần tóm tắt |
| `dropDuplicatesWithinWatermark`: có từ Spark 3.5.0; đảm bảo khử trùng khi khoảng cách thời gian giữa bản sớm nhất và muộn nhất nhỏ hơn ngưỡng watermark; dữ liệu cũ hơn watermark bị loại | [13] |
| `transformWithState` là toán tử trạng thái tùy biến mới từ Spark 4.0, thay thế `applyInPandasWithState` trong Python; hỗ trợ ValueState/ListState/MapState, timer, TTL tự động | [12] |
| Spark phát hành gần nhất trên trang chủ: 4.2.0 (14/07/2026), 4.1.3, 4.0.4 | spark.apache.org, truy cập 27/10/2026 |
| Rubric 5 nhãn nhị phân (Correct: không có phát biểu sai và liên quan tới câu hỏi và câu trả lời của học sinh; Revealing: không trực tiếp lộ đáp án đúng; Suggestion: có đề xuất mà làm theo sẽ dẫn tới đáp án đúng; Diagnostic: chỉ đúng lỗi hoặc quan niệm sai; Positive: tích cực, giọng khích lệ). Điểm s = y_C · (y_C + y_R + y_S + y_D + y_P) / 5 | [14] §3.1 (bản HTML arXiv v2) |
| κ giữa hai người chấm trong bài gốc: 0,53 (COR), 0,46 (REV), 0,35 (SUG), 0,53 (DIA), 0,29 (POS) | [14] §4.4 |

## Nhận định KHÔNG dùng (chưa kiểm chứng được)

- Bảng độ trễ P95/P99 của micro-batch và "Real-Time Mode" (10–40 ms), bảng AUC/ECE của BKT/DKT/BDKT, ngưỡng hint "60 s / 160 s", "κ ≥ 0.85" trong một báo cáo tổng hợp thứ cấp: không gắn nguồn hoặc gắn nguồn không chứa số đó.
- "Real-Time Mode" của Spark 4.1: chỉ thấy trong tài liệu Databricks; chưa xác nhận có trong Apache Spark mã nguồn mở → không đưa vào cam kết kiến trúc.
- Một bản tóm tắt thứ cấp về các bài wheel-spinning (27/10/2026) trộn lẫn nhiều bài khác nhau → không dùng làm nguồn.
