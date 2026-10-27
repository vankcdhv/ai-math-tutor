"""Gợi ý theo mẫu (template) tiếng Việt cho baseline và làm đường dự phòng của dịch vụ gợi ý.

Một gợi ý mẫu được xác định bởi (kỹ năng, bậc, loại lỗi) và **không cá nhân hóa**: không
nhắc tới bài, biểu thức hay đáp án của người học. Ba bậc:

- bậc 1: nhắc khái niệm của kỹ năng (kèm lời nhắc theo loại lỗi nếu biết) và một bài tương tự;
- bậc 2: chẩn đoán theo loại lỗi và mô tả **dạng** của bước tiếp theo;
- bậc 3: một bước giải mẫu cụ thể trên **bài ví dụ** (không phải bài của người học).

Mọi biểu thức toán xuất hiện trong `text_vi` đều nằm trong `math` và được kiểm bằng
`math_check` (`verify_template`): với kỹ năng phương trình, đề ví dụ phải phân tích được và
mỗi bước phải tương đương đề ví dụ (cùng tập nghiệm); với kỹ năng số học, mỗi bước `a = b`
phải tính đúng chính xác. Ngoài các biểu thức đó, câu chữ không chứa dấu `=`.
"""

from __future__ import annotations

from collections.abc import Iterable, Iterator
from dataclasses import dataclass
from typing import Any

from ai_math_tutor import math_check

LEVELS = (1, 2, 3)
SOURCE = "template"


@dataclass(frozen=True, slots=True)
class SkillTemplate:
    """Câu chữ và ví dụ đã kiểm cho một kỹ năng."""

    skill_id: str
    kind: str  # "equation" | "arithmetic"
    concept_vi: str
    next_step_vi: str
    similar_vi: str  # câu dẫn bài tương tự; chứa "{example}" với kỹ năng phương trình
    example: str | None  # đề ví dụ (phương trình); None với kỹ năng số học
    worked_steps: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class HintTemplate:
    """Một gợi ý mẫu đã ghép, sẵn để trả cho người học."""

    skill_id: str
    kind: str
    hint_level: int
    misconception: str | None
    text_vi: str
    math: tuple[str, ...]
    example: str | None

    def as_dict(self) -> dict[str, Any]:
        return {
            "skill_id": self.skill_id,
            "hint_level": self.hint_level,
            "misconception": self.misconception,
            "text_vi": self.text_vi,
            "math": list(self.math),
            "source": SOURCE,
        }


_SKILLS: tuple[SkillTemplate, ...] = (
    SkillTemplate(
        skill_id="arithmetic.multi_step",
        kind="arithmetic",
        concept_vi="Bài nhiều bước: tách đề thành từng phép tính nhỏ, ghi rõ mỗi phép tính cho ra đại lượng nào rồi mới làm phép tính sau.",
        next_step_vi="Bước tiếp theo nên là một phép tính cho đại lượng trung gian kế tiếp mà đề đã cho đủ dữ kiện.",
        similar_vi="Thử bài tương tự: một hộp có 12 cái bút, mua 3 hộp rồi tặng bạn 8 cái; còn lại bao nhiêu cái?",
        example=None,
        worked_steps=("3*12 = 36", "36 - 8 = 28"),
    ),
    SkillTemplate(
        skill_id="arithmetic.fractions",
        kind="arithmetic",
        concept_vi="Cộng, trừ phân số: quy đồng mẫu số trước, rồi cộng (trừ) các tử số và giữ nguyên mẫu chung.",
        next_step_vi="Bước tiếp theo nên là viết các phân số với cùng một mẫu số chung.",
        similar_vi="Thử bài tương tự: tính tổng của một phần hai và một phần ba.",
        example=None,
        worked_steps=("1/2 + 1/3 = 3/6 + 2/6", "3/6 + 2/6 = 5/6"),
    ),
    SkillTemplate(
        skill_id="arithmetic.percent",
        kind="arithmetic",
        concept_vi="Tìm p phần trăm của một số: nhân số đó với p rồi chia cho 100.",
        next_step_vi="Bước tiếp theo nên là xác định số gốc và tỉ lệ phần trăm, rồi viết phép nhân với tỉ lệ đó.",
        similar_vi="Thử bài tương tự: tìm 20 phần trăm của 150.",
        example=None,
        worked_steps=("150*20/100 = 30",),
    ),
    SkillTemplate(
        skill_id="arithmetic.rates",
        kind="arithmetic",
        concept_vi="Vận tốc là quãng đường chia cho thời gian (cùng đơn vị); quãng đường là vận tốc nhân thời gian.",
        next_step_vi="Bước tiếp theo nên là đổi các đại lượng về cùng đơn vị rồi chọn phép nhân hay phép chia cho đúng.",
        similar_vi="Thử bài tương tự: ô tô đi 120 km trong 2 giờ; vận tốc trung bình là bao nhiêu?",
        example=None,
        worked_steps=("120/2 = 60",),
    ),
    SkillTemplate(
        skill_id="linear_equation.isolate_variable",
        kind="equation",
        concept_vi="Cô lập ẩn: làm cùng một phép toán ở cả hai vế; chuyển số hạng tự do sang vế kia (đổi dấu) rồi chia hai vế cho hệ số của x.",
        next_step_vi="Bước tiếp theo nên còn một số hạng chứa x ở một vế và chỉ còn số ở vế kia.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="3x + 5 = 20",
        worked_steps=("3x = 15", "x = 5"),
    ),
    SkillTemplate(
        skill_id="linear_equation.distribute",
        kind="equation",
        concept_vi="Phá ngoặc: nhân số đứng trước ngoặc với từng hạng tử trong ngoặc, giữ đúng dấu, rồi thu gọn.",
        next_step_vi="Bước tiếp theo nên là phương trình không còn ngoặc, mỗi hạng tử trong ngoặc đã được nhân.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="2(x + 4) = 18",
        worked_steps=("2x + 8 = 18", "2x = 10"),
    ),
    SkillTemplate(
        skill_id="linear_equation.both_sides",
        kind="equation",
        concept_vi="Ẩn ở hai vế: gom mọi số hạng chứa x về một vế và các số về vế kia, nhớ đổi dấu khi chuyển vế.",
        next_step_vi="Bước tiếp theo nên chỉ còn số hạng chứa x ở một vế.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="5x - 2 = 3x + 6",
        worked_steps=("2x - 2 = 6", "2x = 8"),
    ),
    SkillTemplate(
        skill_id="linear_equation.fractions",
        kind="equation",
        concept_vi="Phương trình có mẫu số: nhân hai vế với mẫu chung để khử mẫu (nhân mọi hạng tử), hoặc chuyển vế trước rồi mới nhân.",
        next_step_vi="Bước tiếp theo nên là phương trình không còn mẫu số, hoặc phân thức chứa x đứng một mình ở một vế.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="x/3 + 2 = 5",
        worked_steps=("x/3 = 3", "x = 9"),
    ),
    SkillTemplate(
        skill_id="quadratic_equation.factorization",
        kind="equation",
        concept_vi="Đưa về dạng tích bằng 0: tìm hai số có tích bằng hệ số tự do và tổng bằng hệ số của x; tích bằng 0 khi một nhân tử bằng 0.",
        next_step_vi="Bước tiếp theo nên là vế trái viết thành tích của hai nhân tử bậc nhất, vế phải bằng 0.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="x^2 - 5x + 6 = 0",
        worked_steps=("(x - 2)(x - 3) = 0",),
    ),
    SkillTemplate(
        skill_id="quadratic_equation.square_root",
        kind="equation",
        concept_vi="Dạng x bình phương bằng k với k dương có hai nghiệm đối nhau: căn bậc hai của k và số đối của nó.",
        next_step_vi="Bước tiếp theo nên là chuyển hết về một vế rồi phân tích thành hiệu hai bình phương, để thấy đủ hai nghiệm.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="x^2 = 49",
        worked_steps=("x^2 - 49 = 0", "(x - 7)(x + 7) = 0"),
    ),
    SkillTemplate(
        skill_id="quadratic_equation.formula",
        kind="equation",
        concept_vi="Công thức nghiệm: tính biệt thức (b bình phương trừ 4ac) trước; hoặc thêm bớt để vế trái thành một bình phương đủ.",
        next_step_vi="Bước tiếp theo nên là một vế có dạng bình phương của (x cộng một số), vế kia là một số.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="x^2 + 4x - 5 = 0",
        worked_steps=("x^2 + 4x + 4 = 9", "(x + 2)^2 = 9"),
    ),
    SkillTemplate(
        skill_id="quadratic_equation.missing_root",
        kind="equation",
        concept_vi="Không chia hai vế cho biểu thức chứa x (có thể làm mất nghiệm bằng 0); chuyển hết về một vế rồi đặt nhân tử chung.",
        next_step_vi="Bước tiếp theo nên là một tích bằng 0, rồi xét từng nhân tử để lấy đủ mọi nghiệm.",
        similar_vi="Thử bài tương tự: giải {example}.",
        example="x^2 = 3x",
        worked_steps=("x^2 - 3x = 0", "x(x - 3) = 0"),
    ),
)

SKILL_TEMPLATES: dict[str, SkillTemplate] = {skill.skill_id: skill for skill in _SKILLS}

# Lời nhắc ngắn (bậc 1) và câu chẩn đoán (bậc 2) theo loại lỗi; None = chưa rõ loại lỗi.
_REMINDER_VI: dict[str | None, str] = {
    None: "",
    "sign_error": "Chú ý dấu khi chuyển một hạng tử sang vế bên kia.",
    "distribution_error": "Khi phá ngoặc, nhân với mọi hạng tử trong ngoặc.",
    "arithmetic_slip": "Tính lại cẩn thận từng phép tính.",
    "missing_root": "Kiểm tra xem đã tìm đủ mọi nghiệm chưa.",
    "wrong_factor": "Nhân ngược các nhân tử để kiểm tra.",
    "wrong_operation": "Xem lại phép toán mà đề bài yêu cầu.",
}
_DIAGNOSIS_VI: dict[str | None, str] = {
    None: "Hãy đọc lại đề và kiểm tra lại từng bước đã làm.",
    "sign_error": "Có vẻ bước vừa rồi bị sai dấu: khi chuyển một hạng tử sang vế kia, dấu của nó phải đổi.",
    "distribution_error": "Có vẻ bước phá ngoặc chưa đúng: số đứng trước ngoặc phải nhân với mọi hạng tử trong ngoặc.",
    "arithmetic_slip": "Hướng biến đổi đúng nhưng có một phép tính bị nhầm; hãy tính lại từng phép tính.",
    "missing_root": "Có vẻ còn thiếu nghiệm: phương trình bậc hai có thể có hai nghiệm, hãy xét mọi trường hợp.",
    "wrong_factor": "Có vẻ phân tích nhân tử chưa đúng: nhân ngược các nhân tử phải ra đúng biểu thức ban đầu.",
    "wrong_operation": "Có vẻ đã chọn sai phép toán: đọc lại đề xem cần cộng, trừ, nhân hay chia.",
}
MISCONCEPTIONS: tuple[str | None, ...] = (None, *math_check.MISCONCEPTION_TYPES)


def _validate(skill_id: str, level: int, misconception: str | None) -> SkillTemplate:
    skill = SKILL_TEMPLATES.get(skill_id)
    if skill is None:
        raise KeyError(f"không có mẫu gợi ý cho kỹ năng {skill_id!r}")
    if isinstance(level, bool) or level not in LEVELS:
        raise ValueError(f"bậc gợi ý phải thuộc {LEVELS}, nhận {level!r}")
    if misconception not in _DIAGNOSIS_VI:
        raise ValueError(f"loại lỗi không hỗ trợ: {misconception!r}")
    return skill


def _join(*parts: str) -> str:
    return " ".join(part for part in parts if part)


def template_hint(skill_id: str, level: int, misconception: str | None = None) -> HintTemplate:
    """Gợi ý mẫu cho (kỹ năng, bậc, loại lỗi). Kỹ năng lạ → KeyError; bậc/loại lỗi sai → ValueError."""
    skill = _validate(skill_id, level, misconception)
    if level == 1:
        similar = skill.similar_vi.format(example=skill.example) if skill.example else skill.similar_vi
        text = _join(skill.concept_vi, _REMINDER_VI[misconception], similar)
        math: tuple[str, ...] = (skill.example,) if skill.example else ()
    elif level == 2:
        text = _join(_DIAGNOSIS_VI[misconception], skill.next_step_vi)
        math = ()
    else:
        steps = ", rồi ".join(skill.worked_steps)
        if skill.example:
            text = f"Ví dụ với phương trình {skill.example}: bước đúng là {steps}. Làm tương tự với bài của em."
            math = (skill.example, *skill.worked_steps)
        else:
            text = f"Ví dụ: {skill.similar_vi.removeprefix('Thử bài tương tự: ')} Cách làm: {steps}. Làm tương tự với bài của em."
            math = skill.worked_steps
    return HintTemplate(
        skill_id=skill.skill_id,
        kind=skill.kind,
        hint_level=level,
        misconception=misconception,
        text_vi=text,
        math=math,
        example=skill.example,
    )


def reminder_vi(misconception: str | None) -> str:
    """Lời nhắc ngắn theo loại lỗi (rỗng khi chưa rõ loại lỗi); không chứa số hay dấu '='."""
    if misconception not in _REMINDER_VI:
        raise ValueError(f"loại lỗi không hỗ trợ: {misconception!r}")
    return _REMINDER_VI[misconception]


def diagnosis_vi(misconception: str | None) -> str:
    """Câu chẩn đoán theo loại lỗi; không chứa số hay dấu '='."""
    if misconception not in _DIAGNOSIS_VI:
        raise ValueError(f"loại lỗi không hỗ trợ: {misconception!r}")
    return _DIAGNOSIS_VI[misconception]


_GENERIC_VI = "Hãy đọc lại đề, xem lại từng bước đã làm rồi thử lại; có thể xin gợi ý tiếp nếu vẫn chưa ra."


def generic_hint(skill_id: str, kind: str, level: int, misconception: str | None = None) -> HintTemplate:
    """Gợi ý an toàn khi kỹ năng không có mẫu: chỉ lời khuyên chung, không có biểu thức toán nào."""
    if isinstance(level, bool) or level not in LEVELS:
        raise ValueError(f"bậc gợi ý phải thuộc {LEVELS}, nhận {level!r}")
    text = _join(diagnosis_vi(misconception) if misconception is not None else "", _GENERIC_VI)
    return HintTemplate(skill_id=skill_id, kind=kind, hint_level=level, misconception=misconception,
                        text_vi=text, math=(), example=None)


def template_for_skills(skill_ids: Iterable[str], kind: str, level: int,
                        misconception: str | None = None) -> HintTemplate:
    """Mẫu của kỹ năng đầu tiên (theo thứ tự `skill_ids`) có mẫu; không kỹ năng nào có → gợi ý chung."""
    ids = list(skill_ids)
    for skill_id in ids:
        if skill_id in SKILL_TEMPLATES:
            return template_hint(skill_id, level, misconception)
    return generic_hint(ids[0] if ids else "unknown.skill", kind, level, misconception)


def all_templates() -> Iterator[HintTemplate]:
    """Mọi tổ hợp kỹ năng × bậc × loại lỗi (gồm loại lỗi chưa rõ), theo thứ tự cố định."""
    for skill_id in SKILL_TEMPLATES:
        for level in LEVELS:
            for misconception in MISCONCEPTIONS:
                yield template_hint(skill_id, level, misconception)


def verify_template(template: HintTemplate) -> list[str]:
    """Danh sách lỗi kiểm chứng (rỗng = đạt). Dùng SymPy qua `math_check`; tốn vài ms mỗi mẫu khi cache lạnh."""
    errors: list[str] = []
    remaining = template.text_vi
    for expression in template.math:
        if expression not in remaining:
            errors.append(f"biểu thức {expression!r} không xuất hiện trong text_vi")
        remaining = remaining.replace(expression, "")
        try:
            if template.kind == "equation":
                if template.example is None:
                    errors.append("mẫu phương trình thiếu đề ví dụ")
                elif not math_check.step_is_equivalent(template.example, expression):
                    errors.append(f"{expression!r} không tương đương {template.example!r}")
            elif not math_check.arith_step(expression):
                errors.append(f"bước số học sai: {expression!r}")
        except math_check.MathInputError as exc:
            errors.append(f"biểu thức không hợp lệ {expression!r}: {exc}")
    if "=" in remaining:
        errors.append("text_vi chứa dấu '=' ngoài các biểu thức đã kiểm")
    return errors
