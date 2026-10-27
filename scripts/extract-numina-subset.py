"""Tách tập ứng viên NuminaMath-CoT cho kho bài (bước trước khi viết lời giải từng bước).

Quy trình (tất định với cùng revision + seed):
1. Hỏi HF API theo commit SHA ghim cố định, kiểm license = apache-2.0 (API + README card).
2. Tải các file parquet của split train (có resume theo HTTP Range, kiểm kích thước và
   sha256 khớp LFS oid) vào $NHOM8_DATA_ROOT/numina/raw/<sha>/ — không bao giờ vào repo.
3. Lượt 1: đọc cột source/problem/solution, lọc từng dòng bằng ai_math_tutor.numina,
   đếm số dòng theo nguồn và số loại theo lý do, khử trùng đề.
4. Lấy mẫu phân tầng theo (source, kind), chia đều có tràn phần dư, seed cố định
   (lượt `base`, 3.000 dòng — giữ nguyên từ bản tách đầu tiên).
5. Lấy mẫu bổ sung (lượt `supplement`) cho hai tầng bài lời văn gsm8k/arithmetic và
   orca_math/arithmetic, từ các dòng đạt lọc CHƯA được chọn ở lượt base, mỗi tầng một
   random.Random(f"{seed}:supplement:{source}:{kind}") riêng. Lượt base không đổi một dòng
   nào nên tập ứng viên cũ là tập con của tập mới (thay đổi tương thích seed). Lý do: sau
   rà soát vòng 1, bài số học chỉ lấy từ hai nguồn này, nên cần thêm ứng viên để tập test
   vẫn đủ 1.000 bài.
6. Lượt 2: lấy lại văn bản của các dòng được chọn → candidates.parquet + manifest;
   chép manifest và 30 dòng mẫu vào data_sample/.
7. Xóa parquet thô (giữ dữ liệu NuminaMath cục bộ nhỏ) trừ khi có --keep-raw.

Chạy: uv run python scripts/extract-numina-subset.py [--keep-raw] [--download-only]
      [--supplement-gsm8k N] [--supplement-orca N]
Không dùng gói `datasets`; chỉ requests + pyarrow.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import platform
import random
import re
import shutil
import sys
import time
from collections import Counter, defaultdict
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import requests

from ai_math_tutor import numina

ROOT = Path(__file__).resolve().parents[1]
DATASET = "AI-MO/NuminaMath-CoT"
# Commit main của dataset tại thời điểm tách (lastModified 2024-11-25); đổi = đổi dữ liệu.
DEFAULT_REVISION = "9d8d210c9f6a36c8f3cd84045668c9b7800ef517"
EXPECTED_LICENSE = "apache-2.0"
DEFAULT_SEED = 20261027
DEFAULT_TARGET = 3000
# Số dòng lấy thêm cho mỗi tầng bài lời văn (đo trên lượt base: gsm8k đạt ~68%, orca_math ~15%
# lời giải kiểm được với bộ lọc chuỗi hiện tại; cần ~850 bài số học cho test + dev).
DEFAULT_SUPPLEMENT = {"gsm8k": 1000, "orca_math": 2500}
ROUND_BASE = "base"
ROUND_SUPPLEMENT = "supplement"
SAMPLE_ROWS = 30
COLUMNS = ("source", "problem", "solution")
API_REVISION = "https://huggingface.co/api/datasets/{dataset}/revision/{revision}"
API_TREE = "https://huggingface.co/api/datasets/{dataset}/tree/{revision}/data"
RESOLVE = "https://huggingface.co/datasets/{dataset}/resolve/{revision}/{path}"
USER_AGENT = "nhom8-ai-math-tutor/0.1 (INT4418 course project; requests)"
CHUNK = 1 << 20
MAX_ATTEMPTS = 6
TIMEOUT = (30, 120)
TRANSIENT_STATUS = frozenset({429, 500, 502, 503, 504})
ATTRIBUTION = (
    "NuminaMath-CoT (AI-MO, https://huggingface.co/datasets/AI-MO/NuminaMath-CoT), Apache License 2.0. "
    "Jia LI, Edward Beeching, Lewis Tunstall, Ben Lipkin, Roman Soletskyi, Shengyi Costa Huang, Kashif Rasul, "
    "Longhui Yu, Albert Jiang, Ziju Shen, Zihan Qin, Bin Dong, Li Zhou, Yann Fleureau, Guillaume Lample, "
    "Stanislas Polu (2024). The rows here are an unmodified subset (problem, solution); fields kind/answer "
    "are derived by this project."
)
CANDIDATE_SCHEMA = pa.schema(
    [
        ("numina_row", pa.int64()),
        ("source", pa.string()),
        ("kind", pa.string()),
        ("problem", pa.string()),
        ("solution", pa.string()),
        ("answer_raw", pa.string()),
        ("answer", pa.string()),
        ("equation", pa.string()),
        ("variable", pa.string()),
        ("sample_round", pa.string()),
    ]
)


def log(message: str) -> None:
    print(f"[{datetime.now().strftime('%H:%M:%S')}] {message}", file=sys.stderr, flush=True)


# ---------------------------------------------------------------------------
# HF metadata + tải file
# ---------------------------------------------------------------------------


def get_with_retry(session: requests.Session, url: str, **kwargs) -> requests.Response:
    last_error: Exception | None = None
    for attempt in range(1, MAX_ATTEMPTS + 1):
        try:
            response = session.get(url, timeout=TIMEOUT, **kwargs)
            if response.status_code in TRANSIENT_STATUS:
                raise requests.HTTPError(f"HTTP {response.status_code}", response=response)
            response.raise_for_status()
            return response
        except (requests.ConnectionError, requests.Timeout, requests.HTTPError) as error:
            status = getattr(error.response, "status_code", None) if isinstance(error, requests.HTTPError) else None
            if status is not None and status < 500 and status != 429:
                raise
            last_error = error
            wait = min(60, 2**attempt)
            log(f"lỗi tải {url} (lần {attempt}/{MAX_ATTEMPTS}): {error}; thử lại sau {wait}s")
            time.sleep(wait)
    raise RuntimeError(f"không tải được {url}: {last_error}")


@dataclass(frozen=True)
class RemoteFile:
    path: str
    size: int
    sha256: str


def fetch_metadata(session: requests.Session, revision: str) -> dict:
    """Thông tin dataset ở revision đã ghim; dừng nếu license không phải apache-2.0."""
    info = get_with_retry(session, API_REVISION.format(dataset=DATASET, revision=revision)).json()
    sha = info.get("sha")
    if not isinstance(sha, str) or not re.fullmatch(r"[0-9a-f]{40}", sha):
        raise RuntimeError(f"HF API không trả commit sha hợp lệ: {sha!r}")
    if re.fullmatch(r"[0-9a-f]{40}", revision) and sha != revision:
        raise RuntimeError(f"HF API trả sha {sha} khác revision ghim {revision}")
    card_license = (info.get("cardData") or {}).get("license")
    tag_licenses = sorted(tag.split(":", 1)[1] for tag in info.get("tags", []) if tag.startswith("license:"))
    if card_license != EXPECTED_LICENSE or tag_licenses != [EXPECTED_LICENSE]:
        raise RuntimeError(f"license không như kỳ vọng: cardData={card_license!r}, tags={tag_licenses!r}")
    readme = get_with_retry(session, RESOLVE.format(dataset=DATASET, revision=sha, path="README.md")).text
    front_matter = readme.split("---", 2)[1] if readme.startswith("---") else ""
    if not re.search(rf"^license:\s*{re.escape(EXPECTED_LICENSE)}\s*$", front_matter, re.MULTILINE):
        raise RuntimeError("README card không khai báo license: apache-2.0 trong YAML front matter")
    license_sentence = next(
        (line.strip() for line in readme.splitlines() if "Apache License" in line), None
    )
    tree = get_with_retry(session, API_TREE.format(dataset=DATASET, revision=sha)).json()
    files = []
    for entry in tree:
        path = entry.get("path", "")
        if entry.get("type") != "file" or not re.fullmatch(r"data/train-\d{5}-of-\d{5}\.parquet", path):
            continue
        lfs = entry.get("lfs") or {}
        if not re.fullmatch(r"[0-9a-f]{64}", str(lfs.get("oid", ""))) or int(entry.get("size", -1)) <= 0:
            raise RuntimeError(f"thiếu sha256/size LFS cho {path}")
        files.append(RemoteFile(path, int(entry["size"]), lfs["oid"]))
    if not files:
        raise RuntimeError("không thấy file data/train-*.parquet ở revision này")
    files.sort(key=lambda remote: remote.path)
    return {
        "sha": sha,
        "last_modified": info.get("lastModified"),
        "license_card": card_license,
        "license_tags": tag_licenses,
        "license_readme_front_matter": EXPECTED_LICENSE,
        "license_readme_sentence": license_sentence,
        "files": files,
    }


def sha256_of(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(CHUNK * 8), b""):
            digest.update(block)
    return digest.hexdigest()


def download_file(session: requests.Session, url: str, remote: RemoteFile, dest: Path) -> int:
    """Tải có resume; trả số byte thực sự truyền qua mạng. Kiểm size + sha256 trước khi đổi tên."""
    if dest.exists():
        if dest.stat().st_size == remote.size and sha256_of(dest) == remote.sha256:
            log(f"đã có {dest.name} (sha256 khớp), bỏ qua tải")
            return 0
        log(f"{dest.name} có sẵn nhưng không khớp, tải lại")
        dest.unlink()
    part = dest.with_name(dest.name + ".part")
    transferred = 0
    for attempt in range(1, MAX_ATTEMPTS + 1):
        offset = part.stat().st_size if part.exists() else 0
        if offset > remote.size:
            part.unlink()
            offset = 0
        if offset == remote.size:
            break
        headers = {"Range": f"bytes={offset}-"} if offset else {}
        try:
            with session.get(url, headers=headers, stream=True, timeout=TIMEOUT) as response:
                if response.status_code == 206:
                    mode = "ab"
                elif response.status_code == 200:
                    mode, offset = "wb", 0  # máy chủ bỏ qua Range → tải lại từ đầu
                elif response.status_code in TRANSIENT_STATUS:
                    raise requests.ConnectionError(f"HTTP {response.status_code} tạm thời")
                else:
                    raise RuntimeError(f"HTTP {response.status_code} khi tải {remote.path}")
                if offset:
                    log(f"tiếp tục {remote.path} từ {offset / 1e6:.1f} MB")
                next_report = offset + remote.size // 10
                written = offset
                with part.open(mode) as handle:
                    for block in response.iter_content(CHUNK):
                        handle.write(block)
                        transferred += len(block)
                        written += len(block)
                        if written >= next_report:
                            log(f"  {remote.path}: {written / remote.size:5.1%} ({written / 1e6:.0f} MB)")
                            next_report += remote.size // 10
        except (requests.ConnectionError, requests.Timeout, requests.exceptions.ChunkedEncodingError) as error:
            wait = min(60, 2**attempt)
            log(f"đứt kết nối {remote.path} (lần {attempt}/{MAX_ATTEMPTS}): {error}; resume sau {wait}s")
            time.sleep(wait)
            continue
        if part.stat().st_size >= remote.size:
            break
    else:
        raise RuntimeError(f"tải {remote.path} thất bại sau {MAX_ATTEMPTS} lần")
    size = part.stat().st_size
    digest = sha256_of(part)
    if size != remote.size or digest != remote.sha256:
        part.unlink()
        raise RuntimeError(f"{remote.path}: size/sha256 không khớp ({size} B, {digest}) — đã xóa, chạy lại")
    part.rename(dest)
    return transferred


# ---------------------------------------------------------------------------
# Lọc + lấy mẫu
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class Eligible:
    numina_row: int
    source: str
    kind: str
    answer_raw: str
    answer: str
    equation: str | None
    variable: str | None


def iter_rows(paths: list[Path], columns: tuple[str, ...]):
    """(numina_row, {cột: giá trị}) theo thứ tự file; numina_row = chỉ số dòng trong split train."""
    row = 0
    for path in paths:
        parquet = pq.ParquetFile(path)
        missing = set(columns) - set(parquet.schema_arrow.names)
        if missing:
            raise RuntimeError(f"{path.name} thiếu cột {sorted(missing)}")
        for batch in parquet.iter_batches(columns=list(columns), batch_size=4096):
            data = {name: batch.column(name).to_pylist() for name in columns}
            for offset in range(batch.num_rows):
                yield row + offset, {name: data[name][offset] for name in columns}
            row += batch.num_rows


def scan(paths: list[Path]) -> tuple[list[Eligible], dict]:
    source_counts: Counter[str] = Counter()
    drops: Counter[str] = Counter()
    drops_by_source: dict[str, Counter[str]] = defaultdict(Counter)
    seen: set[str] = set()
    eligible: list[Eligible] = []
    total = 0
    for numina_row, row in iter_rows(paths, COLUMNS):
        total += 1
        if total % 200_000 == 0:
            log(f"  đã quét {total:,} dòng, {len(eligible):,} đạt")
        source = row["source"] or ""
        source_counts[source] += 1
        try:
            candidate = numina.screen_row(source, row["problem"] or "", row["solution"] or "")
            key = numina.problem_key(row["problem"])
            if key in seen:
                raise numina.AnswerRejected("duplicate_problem")
            seen.add(key)
        except numina.AnswerRejected as rejected:
            drops[rejected.reason] += 1
            drops_by_source[source][rejected.reason] += 1
            continue
        eligible.append(
            Eligible(numina_row, source, candidate.kind, candidate.answer_raw, candidate.answer, candidate.equation,
                     candidate.variable)
        )
    stats = {
        "rows_scanned": total,
        "source_counts": dict(sorted(source_counts.items())),
        "drop_counts": dict(drops.most_common()),
        "drop_counts_by_source": {
            source: dict(counter.most_common()) for source, counter in sorted(drops_by_source.items())
        },
    }
    return eligible, stats


def stratified_sample(eligible: list[Eligible], target: int, seed: int) -> tuple[list[Eligible], dict]:
    strata: dict[tuple[str, str], list[Eligible]] = defaultdict(list)
    for item in eligible:
        strata[(item.source, item.kind)].append(item)
    sizes = {key: len(items) for key, items in strata.items()}
    allocation = numina.allocate_equal(sizes, target)
    chosen: list[Eligible] = []
    for key in sorted(strata):
        rng = random.Random(f"{seed}:{key[0]}:{key[1]}")
        chosen.extend(rng.sample(sorted(strata[key], key=lambda item: item.numina_row), allocation[key]))
    chosen.sort(key=lambda item: item.numina_row)
    report = {
        f"{source}/{kind}": {"eligible": sizes[(source, kind)], "sampled": allocation[(source, kind)]}
        for source, kind in sorted(sizes)
    }
    return chosen, report


def supplement_sample(eligible: list[Eligible], chosen: list[Eligible], sizes: dict[str, int],
                      seed: int) -> tuple[list[Eligible], dict]:
    """Thêm dòng cho tầng (source, arithmetic) của bài lời văn, chỉ từ dòng đạt lọc chưa được chọn.

    Mỗi tầng có bộ sinh ngẫu nhiên riêng theo seed, lấy mẫu trên danh sách sắp theo numina_row,
    nên kết quả tất định và không phụ thuộc thứ tự các tầng khác.
    """
    taken = {item.numina_row for item in chosen}
    extra: list[Eligible] = []
    report: dict[str, dict[str, int]] = {}
    for source in sorted(sizes):
        if source not in numina.ARITHMETIC_SOURCES:
            raise ValueError(f"chỉ bổ sung nguồn bài lời văn {sorted(numina.ARITHMETIC_SOURCES)}, nhận {source!r}")
        if sizes[source] < 0:
            raise ValueError(f"số dòng bổ sung của {source} phải ≥ 0")
        pool = sorted((item for item in eligible if item.source == source and item.kind == numina.KIND_ARITHMETIC
                       and item.numina_row not in taken), key=lambda item: item.numina_row)
        count = min(sizes[source], len(pool))
        rng = random.Random(f"{seed}:supplement:{source}:{numina.KIND_ARITHMETIC}")
        extra.extend(rng.sample(pool, count))
        report[f"{source}/{numina.KIND_ARITHMETIC}"] = {"remaining_eligible": len(pool), "requested": sizes[source],
                                                        "sampled": count}
    extra.sort(key=lambda item: item.numina_row)
    return extra, report


def fetch_texts(paths: list[Path], rows: set[int]) -> dict[int, tuple[str, str]]:
    texts: dict[int, tuple[str, str]] = {}
    for numina_row, row in iter_rows(paths, ("problem", "solution")):
        if numina_row in rows:
            texts[numina_row] = (row["problem"], row["solution"])
    if len(texts) != len(rows):
        raise RuntimeError(f"lượt 2 chỉ tìm thấy {len(texts)}/{len(rows)} dòng đã chọn")
    return texts


def build_records(chosen: list[Eligible], texts: dict[int, tuple[str, str]], rounds: dict[int, str]) -> list[dict]:
    return [
        {
            "numina_row": item.numina_row,
            "source": item.source,
            "kind": item.kind,
            "problem": texts[item.numina_row][0],
            "solution": texts[item.numina_row][1],
            "answer_raw": item.answer_raw,
            "answer": item.answer,
            "equation": item.equation,
            "variable": item.variable,
            "sample_round": rounds[item.numina_row],
        }
        for item in chosen
    ]


def pick_sample(records: list[dict], seed: int, size: int) -> list[dict]:
    """Mẫu nhỏ phủ mọi tầng (source, kind), tất định theo seed."""
    strata: dict[tuple[str, str], list[dict]] = defaultdict(list)
    for record in records:
        strata[(record["source"], record["kind"])].append(record)
    allocation = numina.allocate_equal({key: len(items) for key, items in strata.items()}, size)
    rng = random.Random(f"{seed}:sample")
    picked = [record for key in sorted(strata) for record in rng.sample(strata[key], allocation[key])]
    return sorted(picked, key=lambda record: record["numina_row"])


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def parse_args(argv: list[str] | None) -> argparse.Namespace:
    default_root = Path(os.environ.get("NHOM8_DATA_ROOT", Path.home() / "nhom8-data")) / "numina"
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--revision", default=DEFAULT_REVISION, help="commit SHA của dataset trên HF")
    parser.add_argument("--data-root", type=Path, default=default_root, help="thư mục dữ liệu (ngoài repo)")
    parser.add_argument("--target", type=int, default=DEFAULT_TARGET, help="số ứng viên lượt base")
    parser.add_argument("--supplement-gsm8k", type=int, default=DEFAULT_SUPPLEMENT["gsm8k"],
                        help="số dòng gsm8k/arithmetic lấy thêm (lượt supplement)")
    parser.add_argument("--supplement-orca", type=int, default=DEFAULT_SUPPLEMENT["orca_math"],
                        help="số dòng orca_math/arithmetic lấy thêm (lượt supplement)")
    parser.add_argument("--seed", type=int, default=DEFAULT_SEED)
    parser.add_argument("--keep-raw", action="store_true", help="giữ parquet thô sau khi tách")
    parser.add_argument("--download-only", action="store_true", help="chỉ tải + kiểm sha256")
    args = parser.parse_args(argv)
    if args.target <= 0:
        parser.error("--target phải > 0")
    if args.supplement_gsm8k < 0 or args.supplement_orca < 0:
        parser.error("--supplement-* phải ≥ 0")
    args.data_root = args.data_root.expanduser().resolve()
    if args.data_root == ROOT or ROOT in args.data_root.parents:
        parser.error(f"--data-root phải nằm ngoài repo ({ROOT})")
    return args


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    started = time.monotonic()
    session = requests.Session()
    session.headers["User-Agent"] = USER_AGENT

    meta = fetch_metadata(session, args.revision)
    sha = meta["sha"]
    log(f"{DATASET}@{sha} license={meta['license_card']} ({len(meta['files'])} file train)")
    raw_dir = args.data_root / "raw" / sha
    raw_dir.mkdir(parents=True, exist_ok=True)

    download_started = time.monotonic()
    transferred = 0
    paths: list[Path] = []
    for remote in meta["files"]:
        dest = raw_dir / Path(remote.path).name
        transferred += download_file(session, RESOLVE.format(dataset=DATASET, revision=sha, path=remote.path), remote, dest)
        paths.append(dest)
    download_seconds = time.monotonic() - download_started
    total_size = sum(remote.size for remote in meta["files"])
    log(f"tải xong: {transferred / 1e6:.1f} MB qua mạng trong {download_seconds:.1f}s (tổng {total_size / 1e6:.1f} MB)")
    if args.download_only:
        return 0

    scan_started = time.monotonic()
    eligible, stats = scan(paths)
    scan_seconds = time.monotonic() - scan_started
    log(f"quét {stats['rows_scanned']:,} dòng trong {scan_seconds:.1f}s; {len(eligible):,} dòng đạt lọc")

    base, strata_report = stratified_sample(eligible, args.target, args.seed)
    if len(base) < args.target:
        log(f"CẢNH BÁO: chỉ có {len(base)} ứng viên < target {args.target}")
    supplement_sizes = {"gsm8k": args.supplement_gsm8k, "orca_math": args.supplement_orca}
    extra, supplement_report = supplement_sample(eligible, base, supplement_sizes, args.seed)
    rounds = {item.numina_row: ROUND_BASE for item in base} | {item.numina_row: ROUND_SUPPLEMENT for item in extra}
    chosen = sorted(base + extra, key=lambda item: item.numina_row)
    log(f"lượt base {len(base)} dòng + lượt supplement {len(extra)} dòng = {len(chosen)} ứng viên")
    texts = fetch_texts(paths, {item.numina_row for item in chosen})
    records = build_records(chosen, texts, rounds)

    out_path = args.data_root / "candidates.parquet"
    table = pa.Table.from_pylist(records, schema=CANDIDATE_SCHEMA)
    pq.write_table(table, out_path.with_suffix(".parquet.tmp"), compression="zstd")
    out_path.with_suffix(".parquet.tmp").replace(out_path)

    sample = pick_sample(records, args.seed, SAMPLE_ROWS)
    sample_path = ROOT / "data_sample" / "numina-candidates-sample.jsonl"
    sample_path.write_text(
        "".join(json.dumps(record, ensure_ascii=False) + "\n" for record in sample), encoding="utf-8", newline="\n"
    )

    eligible_by = Counter((item.source, item.kind) for item in eligible)
    sampled_by = Counter((item.source, item.kind) for item in chosen)
    manifest = {
        "dataset": DATASET,
        "url": f"https://huggingface.co/datasets/{DATASET}",
        "revision": sha,
        "revision_last_modified": meta["last_modified"],
        "split": "train",
        "license": {
            "spdx": EXPECTED_LICENSE,
            "hf_card_data": meta["license_card"],
            "hf_tags": meta["license_tags"],
            "readme_front_matter": meta["license_readme_front_matter"],
            "readme_sentence": meta["license_readme_sentence"],
        },
        "attribution": ATTRIBUTION,
        "files": [{"path": remote.path, "size": remote.size, "sha256": remote.sha256} for remote in meta["files"]],
        "download": {
            "total_bytes": total_size,
            "transferred_bytes_this_run": transferred,
            "seconds": round(download_seconds, 1),
        },
        "seed": args.seed,
        "target": args.target,
        "supplement": {
            "sizes": supplement_sizes,
            "rule": "lượt supplement chỉ cho tầng (gsm8k|orca_math, arithmetic): lấy từ dòng đạt lọc chưa có ở "
                    "lượt base, random.Random(f'{seed}:supplement:{source}:{kind}').sample trên danh sách sắp theo "
                    "numina_row; lượt base giữ nguyên, nên ứng viên cũ là tập con của ứng viên mới",
            "reason": "bài số học vào kho chỉ lấy từ nguồn bài lời văn (numina.ARITHMETIC_SOURCES) sau rà soát vòng 1",
            "strata": supplement_report,
        },
        "allowed_sources": sorted(numina.ALLOWED_SOURCES),
        "arithmetic_sources": sorted(numina.ARITHMETIC_SOURCES),
        "numina_row": "chỉ số dòng 0-based trong split train, theo thứ tự file data/train-0000k-of-00005.parquet",
        **stats,
        "drop_reasons": numina.DROP_REASONS,
        "normalization_rules": list(numina.NORMALIZATION_RULES),
        "kind_heuristic": (
            "equation: đề có từ khóa giải/tìm ẩn và đúng một đoạn công thức chứa '=' với một ẩn chữ cái "
            "(math/synthetic_math: đoạn $...$, \\[...\\]; gsm8k/orca_math: chuỗi số/chữ cái đơn/toán tử liên tiếp, "
            "vì '$' ở đó là ký hiệu tiền), không phải phép gán sẵn 'x = số', là đa thức bậc 1–2 (SymPy) và đáp án "
            "\\boxed trùng TOÀN BỘ tập nghiệm thực (math_check.answer_matches); còn lại arithmetic, trừ đề có "
            "ẩn/ký hiệu chữ trong công thức (loại symbolic_problem)."
        ),
        "variable": "chữ cái ẩn gốc của đề (bài phương trình); cột equation luôn viết theo ẩn x vì math_check chỉ "
                    "kiểm ẩn x, ẩn gốc chỉ dùng để hiển thị",
        "sampling": "lượt base: phân tầng theo (source, kind), chia đều có tràn phần dư sang tầng còn chỗ; "
        "random.Random(f'{seed}:{source}:{kind}').sample trên danh sách sắp theo numina_row; sau đó lượt supplement "
        "(xem 'supplement')",
        "eligible_counts": {f"{s}/{k}": n for (s, k), n in sorted(eligible_by.items())},
        "eligible_total": len(eligible),
        "strata": strata_report,
        "candidate_counts": {f"{s}/{k}": n for (s, k), n in sorted(sampled_by.items())},
        "candidate_counts_by_round": {
            sample_round: {f"{s}/{k}": n for (s, k), n in sorted(
                Counter((item.source, item.kind) for item in chosen if rounds[item.numina_row] == sample_round).items())}
            for sample_round in (ROUND_BASE, ROUND_SUPPLEMENT)
        },
        "candidate_total": len(chosen),
        "outputs": {
            "candidates": {
                "path": "$NHOM8_DATA_ROOT/numina/candidates.parquet",
                "rows": table.num_rows,
                "columns": CANDIDATE_SCHEMA.names,
                "size": out_path.stat().st_size,
                "sha256": sha256_of(out_path),
            },
            "sample": {
                "path": "data_sample/numina-candidates-sample.jsonl",
                "rows": len(sample),
                "sha256": sha256_of(sample_path),
            },
        },
        "timing_seconds": {
            "download": round(download_seconds, 1),
            "scan": round(scan_seconds, 1),
            "total": round(time.monotonic() - started, 1),
        },
        "environment": {"python": platform.python_version(), "pyarrow": pa.__version__, "requests": requests.__version__},
        "generated_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
    }
    manifest_text = json.dumps(manifest, ensure_ascii=False, indent=2) + "\n"
    (args.data_root / "manifest.json").write_text(manifest_text, encoding="utf-8", newline="\n")
    (ROOT / "data_sample" / "numina-candidates-manifest.json").write_text(manifest_text, encoding="utf-8", newline="\n")
    log(f"ghi {out_path} ({table.num_rows} dòng, {out_path.stat().st_size / 1e6:.2f} MB) + manifest + mẫu {len(sample)} dòng")

    if not args.keep_raw:
        shutil.rmtree(raw_dir)
        log(f"đã xóa parquet thô {raw_dir} (dùng --keep-raw để giữ)")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
