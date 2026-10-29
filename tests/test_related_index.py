"""Chỉ mục đảo bài liên quan: token hóa, posting/df/cf tính tay, truy vấn AND/OR, chỉ trả bài dev."""

from __future__ import annotations

import json
import math
from pathlib import Path

import pytest

from ai_math_tutor.problem_bank import load_problems
from ai_math_tutor.related_index import Posting, RelatedIndex, tokenize

ROOT = Path(__file__).resolve().parents[1]
BANK = ROOT / "data_sample" / "problem-bank-v2.jsonl"
INDEX = ROOT / "data_sample" / "problem-index-v2.json"

ISOLATE = "linear_equation.isolate_variable"
DISTRIBUTE = "linear_equation.distribute"
PERCENT = "arithmetic.percent"

# Kho tí hon tính tay:
#   P-A-001 dev  "Solve 2x + 3 = 11."                    → solve 2 x + 3 = 11
#   P-A-002 test "Solve x + 3 = 5, then check x."        → solve x + 3 = 5 then check x
#   P-A-003 dev  "A shirt costs 200,000 VND; the discount is 20%."
#   P-A-004 dev  "Solve 2(x + 3) = 14." + vi "Giải phương trình 2(x + 3) = 14."
CORPUS = [
    {"problem_id": "P-A-001", "split": "dev", "skill_ids": [ISOLATE], "statement": "Solve 2x + 3 = 11."},
    {"problem_id": "P-A-002", "split": "test", "skill_ids": [ISOLATE], "statement": "Solve x + 3 = 5, then check x."},
    {"problem_id": "P-A-003", "split": "dev", "skill_ids": [PERCENT],
     "statement": "A shirt costs 200,000 VND; the discount is 20%."},
    {"problem_id": "P-A-004", "split": "dev", "skill_ids": [ISOLATE, DISTRIBUTE], "statement": "Solve 2(x + 3) = 14.",
     "statement_vi": "Giải phương trình 2(x + 3) = 14.", "source": "self"},
]


def test_translated_statements_of_numina_problems_are_not_indexed() -> None:
    translated = [dict(CORPUS[0], source="numina:1", statement_vi="Giải phương trình 2x + 3 = 11.")]
    assert RelatedIndex.build(translated).to_dict() == RelatedIndex.build([CORPUS[0]]).to_dict()


@pytest.fixture()
def index() -> RelatedIndex:
    return RelatedIndex.build(CORPUS)


@pytest.mark.parametrize(
    ("text", "tokens"),
    [
        ("Solve 2x + 3 = 11.", ["solve", "2", "x", "+", "3", "=", "11"]),
        (r"Two-thirds of 1,500 is $\frac{2}{3} \times 1500$.",
         ["two", "thirds", "of", "1500", "is", "2", "3", "*", "1500"]),
        ("x^2 - 5x = 0.5", ["x", "^", "2", "-", "5", "x", "=", "0.5"]),
        ("Giải phương trình (x + 1)/2 = 4!", ["giải", "phương", "trình", "x", "+", "1", "/", "2", "=", "4"]),
        ("A discount of 20%?", ["a", "discount", "of", "20", "%"]),
    ],
)
def test_tokenize(text: str, tokens: list[str]) -> None:
    assert tokenize(text) == tokens


def test_postings_df_cf_match_hand_computation(index: RelatedIndex) -> None:
    assert index.n_docs == 4
    assert index.postings("x") == (Posting("P-A-001", 1), Posting("P-A-002", 2), Posting("P-A-004", 2))
    assert (index.df("x"), index.cf("x")) == (3, 5)
    assert index.postings("3") == (Posting("P-A-001", 1), Posting("P-A-002", 1), Posting("P-A-004", 2))
    assert (index.df("3"), index.cf("3")) == (3, 4)
    assert (index.df("2"), index.cf("2")) == (2, 3)
    assert (index.df("solve"), index.cf("solve")) == (3, 3)
    assert index.postings("%") == (Posting("P-A-003", 1),)
    assert index.postings("200000") == (Posting("P-A-003", 1),)
    assert index.postings("giải") == (Posting("P-A-004", 1),)
    assert index.postings("missing") == () and index.df("missing") == 0 and index.cf("missing") == 0
    assert index.skill_postings(ISOLATE) == ("P-A-001", "P-A-002", "P-A-004")
    assert index.idf("x") == pytest.approx(math.log(4 / 3))


def test_every_posting_list_is_sorted_by_problem_id(index: RelatedIndex) -> None:
    for term in index.vocabulary:
        ids = [p.problem_id for p in index.postings(term)]
        assert ids == sorted(ids)


def test_and_query_intersects_and_ranks_by_tf_idf(index: RelatedIndex) -> None:
    idf = math.log(4 / 3)
    hits = index.search(tokens=["solve x"], mode="and")
    assert hits == [("P-A-004", round(3 * idf, 6)), ("P-A-001", round(2 * idf, 6))]


def test_or_query_unions(index: RelatedIndex) -> None:
    hits = index.search(tokens=["discount 14"], mode="or")
    assert hits == [("P-A-004", round(2 * math.log(4), 6)), ("P-A-003", round(math.log(4), 6))]
    assert index.search(tokens=["discount 14"], mode="and") == []


def test_skill_filter_is_conjunctive_and_returns_only_dev(index: RelatedIndex) -> None:
    assert index.search(skills=[ISOLATE]) == [("P-A-001", 0.0), ("P-A-004", 0.0)]
    assert index.search(skills=[ISOLATE, DISTRIBUTE]) == [("P-A-004", 0.0)]
    assert index.search(skills=[ISOLATE], tokens=["discount"]) == []


def test_test_items_are_never_returned(index: RelatedIndex) -> None:
    assert index.postings("check") == (Posting("P-A-002", 1),)
    assert index.search(tokens=["check"]) == []
    assert "P-A-002" not in index.related(CORPUS[1])
    assert index.related(CORPUS[1]) == ["P-A-004", "P-A-001"]


def test_related_excludes_the_problem_itself(index: RelatedIndex) -> None:
    assert index.related(CORPUS[0]) == ["P-A-004"]


def test_bad_query_arguments(index: RelatedIndex) -> None:
    assert index.search() == []
    with pytest.raises(ValueError):
        index.search(tokens=["x"], mode="xor")
    with pytest.raises(ValueError):
        index.search(tokens=["x"], limit=0)


def test_round_trip_and_tamper_detection(index: RelatedIndex, tmp_path: Path) -> None:
    path = tmp_path / "index.json"
    index.save(path)
    assert RelatedIndex.load(path).to_dict() == index.to_dict()
    data = json.loads(path.read_text(encoding="utf-8"))
    data["terms"]["x"]["df"] = 99
    with pytest.raises(ValueError):
        RelatedIndex.from_dict(data)
    with pytest.raises(ValueError):
        RelatedIndex({"x": [("P-A-002", 1), ("P-A-001", 1)]}, {}, {"P-A-001": "dev", "P-A-002": "dev"})
    with pytest.raises(ValueError):
        RelatedIndex.build([CORPUS[0], CORPUS[0]])


def test_committed_index_matches_committed_bank() -> None:
    problems = load_problems(BANK)
    committed = RelatedIndex.load(INDEX)
    assert committed.to_dict() == RelatedIndex.build(problems).to_dict()
    assert committed.n_docs == len(problems)


def test_hint_examples_for_every_test_problem_are_dev_with_shared_skill() -> None:
    problems = load_problems(BANK)
    by_id = {p.problem_id: p for p in problems}
    index = RelatedIndex.load(INDEX)
    with_examples = 0
    for problem in problems:
        if problem.split != "test":
            continue
        related = index.related(problem)
        with_examples += bool(related)
        for pid in related:
            assert by_id[pid].split == "dev"
            assert problem.primary_skill in by_id[pid].skill_ids
    assert with_examples == sum(p.split == "test" for p in problems)
