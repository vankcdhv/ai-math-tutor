"""Lượt lấy mẫu bổ sung của bộ tách NuminaMath: tất định, không đụng lượt base, chỉ cho bài lời văn."""

from __future__ import annotations

import importlib.util
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]


def _extractor():
    spec = importlib.util.spec_from_file_location("extract_numina_subset", ROOT / "scripts" / "extract-numina-subset.py")
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module  # dataclass cần module đã đăng ký
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


EXTRACT = _extractor()


def _eligible() -> list:
    rows = []
    for row in range(400):
        source = ("gsm8k", "orca_math", "math", "synthetic_math")[row % 4]
        kind = "equation" if source == "synthetic_math" and row % 8 == 3 else "arithmetic"
        rows.append(EXTRACT.Eligible(row, source, kind, "1", "1", "x = 1" if kind == "equation" else None,
                                     "x" if kind == "equation" else None))
    return rows


def test_base_sample_is_unchanged_and_supplement_only_adds_unused_word_problems() -> None:
    eligible = _eligible()
    base, _ = EXTRACT.stratified_sample(eligible, 120, 7)
    again, _ = EXTRACT.stratified_sample(eligible, 120, 7)
    assert base == again
    extra, report = EXTRACT.supplement_sample(eligible, base, {"gsm8k": 30, "orca_math": 500}, 7)
    taken = {item.numina_row for item in base}
    assert not taken & {item.numina_row for item in extra}
    assert {(item.source, item.kind) for item in extra} == {("gsm8k", "arithmetic"), ("orca_math", "arithmetic")}
    assert report["gsm8k/arithmetic"]["sampled"] == 30
    orca_left = report["orca_math/arithmetic"]["remaining_eligible"]
    assert report["orca_math/arithmetic"]["sampled"] == orca_left  # yêu cầu vượt số còn lại → lấy hết
    assert [item.numina_row for item in extra] == sorted(item.numina_row for item in extra)


def test_supplement_is_seed_stable_per_stratum() -> None:
    eligible = _eligible()
    base, _ = EXTRACT.stratified_sample(eligible, 120, 7)
    first, _ = EXTRACT.supplement_sample(eligible, base, {"gsm8k": 20, "orca_math": 10}, 7)
    second, _ = EXTRACT.supplement_sample(eligible, base, {"gsm8k": 20, "orca_math": 0}, 7)
    gsm = lambda items: [item.numina_row for item in items if item.source == "gsm8k"]  # noqa: E731
    assert gsm(first) == gsm(second)  # tầng gsm8k không phụ thuộc cỡ của tầng orca_math
    other_seed, _ = EXTRACT.supplement_sample(eligible, base, {"gsm8k": 20, "orca_math": 10}, 8)
    assert gsm(other_seed) != gsm(first)


@pytest.mark.parametrize("sizes", [{"math": 10}, {"gsm8k": -1}])
def test_supplement_rejects_competition_sources_and_negative_sizes(sizes) -> None:
    eligible = _eligible()
    with pytest.raises(ValueError):
        EXTRACT.supplement_sample(eligible, [], sizes, 7)
