"""Bảng câu mẫu tiếng Việt cho thang gợi ý cá nhân hóa: (loại bài × bậc × loại lỗi) → một câu.

Soạn trên các bài **dev** của kho bài (đọc đường giải và lỗi đã khai), không chỉnh theo tập test.
Bất biến (test kiểm): không câu nào chứa chữ số hay dấu '=' — mọi con số và phương trình trong gợi ý
đến từ biểu thức đã kiểm chứng (`verify`), nên câu chữ không thể tự lộ đáp án.

- Bậc 1: lời nhắc khái niệm gắn với loại lỗi (đi kèm khái niệm của kỹ năng và một bài tương tự).
- Bậc 2: câu chẩn đoán loại lỗi (đi kèm **dạng** của bước tiếp, số được thay bằng ô trống).
- Bậc 3: lời dẫn ngắn trước bước tiếp theo cụ thể, đã kiểm; nếu bước đó lộ đáp án mà chưa được phép đưa
  thì lời dẫn nói rõ điều này và chỉ đưa **dạng** của bước (như bậc 2).
"""

from __future__ import annotations

from ai_math_tutor import math_check

KINDS = math_check.PROBLEM_KINDS
LEVELS = (1, 2, 3)
MISCONCEPTIONS: tuple[str | None, ...] = (None, *math_check.MISCONCEPTION_TYPES)
FORM_SLOT = "□"

_EQUATION: dict[int, dict[str | None, str]] = {
    1: {
        None: "Nhớ rằng mỗi bước biến đổi phải giữ nguyên tập nghiệm: làm cùng một phép toán ở cả hai vế.",
        "sign_error": "Khi chuyển một hạng tử từ vế này sang vế kia, dấu của nó phải đổi.",
        "distribution_error": "Khi phá ngoặc, số đứng trước ngoặc phải nhân với từng hạng tử bên trong, kể cả dấu của hạng tử.",
        "arithmetic_slip": "Hướng biến đổi thường không sai; hãy tính lại thật cẩn thận từng phép cộng, trừ, nhân, chia.",
        "missing_root": "Phương trình bậc hai có thể có hai nghiệm; không chia hai vế cho một biểu thức chứa x.",
        "wrong_factor": "Sau khi phân tích thành nhân tử, nhân ngược lại để kiểm tra có ra đúng biểu thức ban đầu không.",
        "wrong_operation": "Muốn đưa x về một mình thì dùng phép ngược của phép đang có: bỏ phép cộng bằng phép trừ, bỏ phép nhân bằng phép chia.",
    },
    2: {
        None: "Bước vừa rồi chưa giữ đúng tập nghiệm của đề; hãy so từng hạng tử với dòng trước đó.",
        "sign_error": "Bước vừa rồi có vẻ sai dấu: một hạng tử đã chuyển vế mà chưa đổi dấu, hoặc dấu trừ trước ngoặc chưa áp cho mọi hạng tử.",
        "distribution_error": "Bước vừa rồi có vẻ phá ngoặc chưa đúng: số đứng trước ngoặc chưa được nhân với mọi hạng tử trong ngoặc.",
        "arithmetic_slip": "Hướng biến đổi đúng nhưng có một phép tính bị nhầm; hãy tính lại các hệ số và số hạng tự do.",
        "missing_root": "Đáp án có vẻ còn thiếu nghiệm: hãy đưa về dạng tích bằng không rồi xét từng nhân tử.",
        "wrong_factor": "Phân tích nhân tử có vẻ chưa đúng: nhân các nhân tử lại phải ra đúng vế ban đầu.",
        "wrong_operation": "Có vẻ đã chọn sai phép toán: phép làm ở hai vế phải là phép ngược của phép đang gắn với x.",
    },
    3: {
        None: "Làm theo đúng bước dưới đây.",
        "sign_error": "Chú ý đổi dấu khi chuyển vế.",
        "distribution_error": "Chú ý nhân số trước ngoặc với mọi hạng tử trong ngoặc.",
        "arithmetic_slip": "Chú ý tính lại từng phép tính.",
        "missing_root": "Chú ý giữ đủ mọi nghiệm.",
        "wrong_factor": "Chú ý kiểm tra nhân tử bằng cách nhân ngược.",
        "wrong_operation": "Chú ý dùng phép ngược của phép đang gắn với x.",
    },
}

_ARITHMETIC: dict[int, dict[str | None, str]] = {
    1: {
        None: "Đọc kỹ đề, xác định mỗi phép tính cho ra đại lượng nào trước khi tính.",
        "sign_error": "Khi trừ, giữ đúng thứ tự: số bị trừ đứng trước, số trừ đứng sau.",
        "distribution_error": "Khi nhân một số với một tổng, nhân với từng số hạng của tổng.",
        "arithmetic_slip": "Tính lại cẩn thận từng phép tính; có thể ước lượng để kiểm tra kết quả có hợp lý không.",
        "missing_root": "Kiểm tra xem đã trả lời đủ mọi đại lượng mà đề hỏi chưa.",
        "wrong_factor": "Mỗi thừa số trong phép nhân phải là một đại lượng có trong đề hoặc vừa tính được.",
        "wrong_operation": "Xem đề yêu cầu gộp lại (cộng), bớt đi (trừ), gấp lên (nhân) hay chia đều (chia).",
    },
    2: {
        None: "Kết quả chưa khớp với đường giải của bài; hãy kiểm tra lại đại lượng vừa tính.",
        "sign_error": "Kết quả có vẻ bị ngược dấu: có thể đã trừ theo thứ tự ngược lại.",
        "distribution_error": "Có vẻ đã nhân chưa đủ: số nhân phải áp cho mọi phần của tổng.",
        "arithmetic_slip": "Cách làm đúng nhưng có một phép tính bị nhầm; hãy tính lại.",
        "missing_root": "Có vẻ còn thiếu một phần của câu trả lời; đọc lại câu hỏi cuối của đề.",
        "wrong_factor": "Có vẻ đã dùng sai một thừa số; đối chiếu lại với dữ kiện trong đề.",
        "wrong_operation": "Có vẻ đã chọn sai phép toán cho bước này; đọc lại đề xem cần cộng, trừ, nhân hay chia.",
    },
    3: {
        None: "Làm theo đúng phép tính dưới đây.",
        "sign_error": "Chú ý thứ tự của phép trừ.",
        "distribution_error": "Chú ý nhân với mọi phần của tổng.",
        "arithmetic_slip": "Chú ý tính lại từng phép tính.",
        "missing_root": "Chú ý trả lời đủ mọi đại lượng đề hỏi.",
        "wrong_factor": "Chú ý chọn đúng thừa số từ dữ kiện.",
        "wrong_operation": "Chú ý chọn đúng phép toán.",
    },
}

_TABLE: dict[str, dict[int, dict[str | None, str]]] = {"equation": _EQUATION, "arithmetic": _ARITHMETIC}

# Câu theo trạng thái biểu thức người học gửi kèm (sau khi `diagnose` chạy trong safe_math).
EXPRESSION_STATUSES = ("none", "correct", "wrong", "invalid", "unchecked")
_STATUS_VI: dict[str, str] = {
    "none": "",
    "wrong": "",
    "correct": "Bước em vừa viết là đúng; hãy làm tiếp bước sau.",
    "invalid": "Biểu thức em nhập chưa đọc được: chỉ dùng số, ẩn x, các phép cộng, trừ, nhân, chia, lũy thừa và ngoặc.",
    "unchecked": "Lúc này chưa kiểm được biểu thức em nhập, nên gợi ý dưới đây chỉ dựa trên đề bài.",
}

# Bậc 2 khi người học chưa gửi biểu thức sai nào (không có gì để chẩn đoán): câu định hướng trung tính.
UNDIAGNOSED_L2_VI = {
    "equation": "Hãy nhìn lại phương trình hiện có và nghĩ xem phép biến đổi nào giữ nguyên tập nghiệm.",
    "arithmetic": "Hãy xác định đại lượng cần tính tiếp theo và các dữ kiện của đề dùng để tính nó.",
}

# Khung câu cố định quanh biểu thức/dạng bước (cũng không có chữ số hay '=').
SIMILAR_EQUATION_VI = "Thử bài tương tự {problem_id}: giải {equation}."
SIMILAR_TEXT_VI = "Thử bài tương tự {problem_id}: {statement}"
SIMILAR_REFERENCE_VI = "Xem bài tương tự {problem_id} trong kho bài (cùng kỹ năng, đã có lời giải)."
FORM_LEAD_VI = {"equation": "Bước tiếp theo của bài này có dạng", "arithmetic": "Phép tính tiếp theo của bài này có dạng"}
FORM_TAIL_VI = f", mỗi ô {FORM_SLOT} là một số cần tìm."
STEP_LEAD_VI = {"equation": "Bước tiếp theo đúng của bài này là", "arithmetic": "Phép tính tiếp theo đúng của bài này là"}
STEP_TAIL_VI = ". Hãy tự kiểm tra lại rồi làm tiếp."
# Bậc 3 khi bước tiếp theo lộ đáp án mà chưa được phép đưa: chỉ đưa dạng của chính bước đó (số thay bằng □).
LOCKED_FORM_LEAD_VI = {
    "equation": "Bước tiếp theo là bước ra đáp án nên chưa đưa cụ thể; nó có dạng",
    "arithmetic": "Phép tính tiếp theo cho ra đáp án nên chưa đưa cụ thể; nó có dạng",
}


def phrase(kind: str, level: int, misconception: str | None) -> str:
    """Câu mẫu cho (loại bài, bậc, loại lỗi); tổ hợp lạ → ValueError."""
    try:
        return _TABLE[kind][level][misconception]
    except KeyError as exc:
        raise ValueError(f"không có câu mẫu cho {(kind, level, misconception)!r}") from exc


def status_phrase(status: str) -> str:
    if status not in _STATUS_VI:
        raise ValueError(f"trạng thái biểu thức lạ: {status!r}")
    return _STATUS_VI[status]


def all_phrases() -> list[str]:
    """Mọi câu cố định của bảng (để test bất biến không số, không '=')."""
    fixed = [*_STATUS_VI.values(), FORM_TAIL_VI, STEP_TAIL_VI, *FORM_LEAD_VI.values(), *STEP_LEAD_VI.values(),
             *LOCKED_FORM_LEAD_VI.values(), *UNDIAGNOSED_L2_VI.values()]
    table = [_TABLE[k][lv][m] for k in KINDS for lv in LEVELS for m in MISCONCEPTIONS]
    return table + [text for text in fixed if text]
