"""Chỉ mục đảo tìm bài liên quan trong kho bài (mô hình chỉ mục đảo của Chương 4).

- Token hóa: chữ thường, bỏ dấu câu, bỏ lệnh LaTeX (giữ `\\times`/`\\cdot` thành `*`,
  `\\div` thành `/`), giữ token toán: số (`12`, `2.5`), ẩn/chữ, toán tử `= + - * / ^ %`.
  Dấu gạch nối giữa hai chữ (`two-thirds`) là dấu nối từ, không phải phép trừ.
- Mỗi bài là một tài liệu: `statement` (+ `statement_vi` khi đó là đề gốc của bài tự soạn; đề dịch của bài
  NuminaMath không vào chỉ mục, để việc chọn bài tương tự không đổi khi thêm bản dịch). Posting của một
  term là danh sách `(problem_id, tf)` sắp theo problem_id; `df` = số tài liệu chứa
  term, `cf` = tổng số lần xuất hiện trong cả kho.
- Kỹ năng là danh sách posting riêng (`skill_id` → các problem_id đã sắp).
- Chỉ mục xây trên dev ∪ test (df/cf phản ánh cả kho) nhưng truy vấn CHỈ trả bài
  dev: bài ví dụ dùng làm gợi ý không bao giờ là bài test (tránh rò rỉ tập đánh giá).
"""

from __future__ import annotations

import json
import math
import re
from collections import Counter
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

INDEX_VERSION = 1
HINT_SPLIT = "dev"
SELF_SOURCE = "self"  # bài tự soạn: statement_vi là đề gốc, không phải bản dịch
MODES = ("and", "or")

_LATEX_TIMES = re.compile(r"\\(?:times|cdot)(?![A-Za-z])")
_LATEX_DIV = re.compile(r"\\div(?![A-Za-z])")
_LATEX_COMMAND = re.compile(r"\\[A-Za-z]+")
_THOUSANDS = re.compile(r"(?<=\d),(?=\d{3}(?!\d))")
_WORD_HYPHEN = re.compile(r"(?<=[^\W\d_])-(?=[^\W\d_])")
_TOKEN = re.compile(r"\d+(?:\.\d+)?|[^\W\d_]+|[=+\-*/^%]")


def tokenize(text: str) -> list[str]:
    """Văn bản đề → danh sách token (giữ thứ tự, có lặp — dùng để đếm tf)."""
    lowered = text.lower()
    lowered = _LATEX_TIMES.sub(" * ", lowered)
    lowered = _LATEX_DIV.sub(" / ", lowered)
    lowered = _LATEX_COMMAND.sub(" ", lowered)
    lowered = _THOUSANDS.sub("", lowered)
    lowered = _WORD_HYPHEN.sub(" ", lowered)
    return _TOKEN.findall(lowered)


@dataclass(frozen=True, slots=True)
class Posting:
    problem_id: str
    tf: int


def _field(problem: Any, name: str) -> Any:
    if isinstance(problem, Mapping):
        return problem.get(name)
    return getattr(problem, name, None)


def _intersect(lists: Sequence[Sequence[str]]) -> list[str]:
    """Giao các danh sách đã sắp bằng trộn hai con trỏ, danh sách ngắn nhất trước."""
    if not lists:
        return []
    ordered = sorted(lists, key=len)
    result = list(ordered[0])
    for other in ordered[1:]:
        merged: list[str] = []
        i = j = 0
        while i < len(result) and j < len(other):
            if result[i] == other[j]:
                merged.append(result[i])
                i += 1
                j += 1
            elif result[i] < other[j]:
                i += 1
            else:
                j += 1
        result = merged
        if not result:
            break
    return result


def _union(lists: Sequence[Sequence[str]]) -> list[str]:
    return sorted({item for items in lists for item in items})


class RelatedIndex:
    """Chỉ mục đảo bất biến; dựng bằng `build` hoặc nạp từ JSON."""

    def __init__(self, terms: Mapping[str, Sequence[tuple[str, int]]], skills: Mapping[str, Sequence[str]],
                 splits: Mapping[str, str]):
        self._splits = dict(splits)
        self._terms: dict[str, tuple[Posting, ...]] = {}
        for term, postings in terms.items():
            items = tuple(Posting(pid, int(tf)) for pid, tf in postings)
            ids = [p.problem_id for p in items]
            if ids != sorted(ids) or len(set(ids)) != len(ids) or any(p.tf < 1 for p in items):
                raise ValueError(f"posting của {term!r} phải sắp theo problem_id, không trùng, tf ≥ 1")
            if any(pid not in self._splits for pid in ids):
                raise ValueError(f"posting của {term!r} trỏ tới bài không có trong chỉ mục")
            self._terms[term] = items
        self._skills: dict[str, tuple[str, ...]] = {}
        for skill, ids in skills.items():
            if list(ids) != sorted(set(ids)) or any(pid not in self._splits for pid in ids):
                raise ValueError(f"danh sách kỹ năng {skill!r} phải sắp, không trùng, trỏ tới bài có thật")
            self._skills[skill] = tuple(ids)

    @classmethod
    def build(cls, problems: Iterable[Any]) -> RelatedIndex:
        """Dựng chỉ mục từ các bài (`Problem` hoặc dict theo lược đồ kho bài)."""
        counts: dict[str, Counter[str]] = {}
        skills: dict[str, set[str]] = {}
        splits: dict[str, str] = {}
        for problem in problems:
            pid = _field(problem, "problem_id")
            if not isinstance(pid, str) or pid in splits:
                raise ValueError(f"problem_id thiếu hoặc trùng: {pid!r}")
            splits[pid] = str(_field(problem, "split"))
            text = str(_field(problem, "statement") or "")
            statement_vi = _field(problem, "statement_vi")
            if statement_vi and _field(problem, "source") == SELF_SOURCE:
                text = f"{text}\n{statement_vi}"
            counts[pid] = Counter(tokenize(text))
            for skill in _field(problem, "skill_ids") or ():
                skills.setdefault(skill, set()).add(pid)
        terms: dict[str, list[tuple[str, int]]] = {}
        for pid in sorted(counts):
            for term, tf in counts[pid].items():
                terms.setdefault(term, []).append((pid, tf))
        return cls(terms, {skill: sorted(ids) for skill, ids in skills.items()}, splits)

    # --- thống kê -----------------------------------------------------------

    @property
    def n_docs(self) -> int:
        return len(self._splits)

    @property
    def vocabulary(self) -> tuple[str, ...]:
        return tuple(sorted(self._terms))

    def postings(self, term: str) -> tuple[Posting, ...]:
        return self._terms.get(term, ())

    def df(self, term: str) -> int:
        return len(self._terms.get(term, ()))

    def cf(self, term: str) -> int:
        return sum(p.tf for p in self._terms.get(term, ()))

    def idf(self, term: str) -> float:
        df = self.df(term)
        return math.log(self.n_docs / df) if df else 0.0

    def skill_postings(self, skill: str) -> tuple[str, ...]:
        return self._skills.get(skill, ())

    def split_of(self, problem_id: str) -> str:
        return self._splits[problem_id]

    # --- truy vấn -----------------------------------------------------------

    def search(self, tokens: Iterable[str] = (), skills: Iterable[str] = (), mode: str = "or",
               limit: int = 5, exclude: Iterable[str] = ()) -> list[tuple[str, float]]:
        """Bài DEV khớp truy vấn, sắp theo điểm tf·idf giảm dần rồi problem_id.

        `skills`: bộ lọc bắt buộc — bài phải có MỌI kỹ năng (giao các danh sách).
        `tokens`: token hóa lại rồi kết hợp theo `mode` — "and" (bài chứa mọi token)
        hoặc "or" (chứa ít nhất một token). Không có token thì chỉ lọc theo kỹ năng,
        điểm 0. Không có cả hai thì trả rỗng. Bài test không bao giờ được trả về.
        """
        if mode not in MODES:
            raise ValueError(f"mode phải thuộc {MODES}, nhận {mode!r}")
        if limit < 1:
            raise ValueError("limit phải ≥ 1")
        skill_list = sorted(set(skills))
        query = sorted({token for text in tokens for token in tokenize(text)})
        if not skill_list and not query:
            return []
        candidate_lists: list[list[str]] = []
        if skill_list:
            candidate_lists.append(_intersect([list(self.skill_postings(s)) for s in skill_list]))
        if query:
            term_lists = [[p.problem_id for p in self.postings(t)] for t in query]
            candidate_lists.append(_intersect(term_lists) if mode == "and" else _union(term_lists))
        candidates = _intersect(candidate_lists)
        excluded = set(exclude)
        scores: dict[str, float] = {pid: 0.0 for pid in candidates
                                    if self._splits[pid] == HINT_SPLIT and pid not in excluded}
        for term in query:
            weight = self.idf(term)
            for posting in self.postings(term):
                if posting.problem_id in scores:
                    scores[posting.problem_id] += posting.tf * weight
        ranked = sorted(scores.items(), key=lambda item: (-item[1], item[0]))
        return [(pid, round(score, 6)) for pid, score in ranked[:limit]]

    def related(self, problem: Any, limit: int = 3) -> list[str]:
        """Bài dev cùng kỹ năng chính, gần nhất theo từ ngữ đề (OR), không gồm chính bài đó."""
        skill_ids = _field(problem, "skill_ids") or ()
        if not skill_ids:
            return []
        text = str(_field(problem, "statement") or "")
        hits = self.search(tokens=[text], skills=[skill_ids[0]], mode="or", limit=limit,
                           exclude=[_field(problem, "problem_id")])
        if not hits:
            hits = self.search(skills=[skill_ids[0]], limit=limit, exclude=[_field(problem, "problem_id")])
        return [pid for pid, _ in hits]

    # --- tuần tự hóa --------------------------------------------------------

    def to_dict(self) -> dict[str, Any]:
        return {
            "version": INDEX_VERSION,
            "n_docs": self.n_docs,
            "hint_split": HINT_SPLIT,
            "splits": {pid: self._splits[pid] for pid in sorted(self._splits)},
            "skills": {skill: list(self._skills[skill]) for skill in sorted(self._skills)},
            "terms": {
                term: {"df": self.df(term), "cf": self.cf(term),
                       "postings": [[p.problem_id, p.tf] for p in self._terms[term]]}
                for term in sorted(self._terms)
            },
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> RelatedIndex:
        if data.get("version") != INDEX_VERSION:
            raise ValueError(f"phiên bản chỉ mục không hỗ trợ: {data.get('version')!r}")
        terms = data["terms"]
        index = cls({term: [tuple(p) for p in entry["postings"]] for term, entry in terms.items()},
                    data["skills"], data["splits"])
        for term, entry in terms.items():
            if entry["df"] != index.df(term) or entry["cf"] != index.cf(term):
                raise ValueError(f"df/cf của {term!r} không khớp posting")
        if data.get("n_docs") != index.n_docs:
            raise ValueError("n_docs không khớp")
        return index

    def save(self, path: Path) -> None:
        text = json.dumps(self.to_dict(), ensure_ascii=False, separators=(",", ":"), sort_keys=True)
        Path(path).write_text(text + "\n", encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> RelatedIndex:
        return cls.from_dict(json.loads(Path(path).read_text(encoding="utf-8")))
