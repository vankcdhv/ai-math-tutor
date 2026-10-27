"""Đánh giá batch so với ground truth: thời gian phát hiện bế tắc (time-to-detect) và độ cũ của trạng thái.

Quy ước thời gian (giao thức thí nghiệm): một kết quả của job tại cutoff c chỉ **hiển thị** khi
job chạy xong, tức `visible_time = c + runtime đo được của job đó`. Sự kiện có `sent_time ≤ c`
mới vào job; nên với sự kiện kích hoạt (onset) đến lúc s, thường `visible_time = cutoff đầu tiên
≥ s + runtime` — `trigger_rule_holds` đếm số episode thỏa đúng đẳng thức này.

- Episode thật lấy từ `ground_truth.jsonl` (offset giây từ t0), dịch sang ms tuyệt đối bằng cùng
  mốc thời gian với log (`t0` hoặc `replay_start`).
- Một (người học, kỹ năng) có tối đa một episode bế tắc (luật thành thạo dính), nên đối sánh theo
  khóa (learner_id, skill_id): batch phát hiện = job đầu tiên có onset cho khóa đó.
- time-to-detect = visible_time − onset thật (event_time của sự kiện onset), tính bằng giây.
- Độ cũ (staleness) của một sự kiện = visible_time của job đầu tiên chứa nó − event_time.
Phân vị dùng nội suy tuyến tính của numpy (`np.percentile` mặc định).
"""

from __future__ import annotations

import csv
import json
from collections import defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np

from ai_math_tutor.batch.schedule import Schedule

GROUND_TRUTH_TIME_UNIT = "seconds_from_t0"


class GroundTruthError(ValueError):
    """Tệp ground truth sai dạng."""


@dataclass(frozen=True)
class TrueEpisode:
    learner_id: str
    skill_id: str
    connection: str
    ability_group: str
    onset_event_id: str
    onset_ms: int
    end_ms: int | None
    latent_mastered_at_onset: bool

    @property
    def key(self) -> tuple[str, str]:
        return (self.learner_id, self.skill_id)


@dataclass(frozen=True)
class GroundTruth:
    header: dict[str, Any]
    episodes: tuple[TrueEpisode, ...]


def _abs_ms(base_ms: int, t_s: Any) -> int:
    return base_ms + round(float(t_s) * 1000)


def read_header(path: Path) -> dict[str, Any]:
    """Dòng `run` (dòng đầu) của ground truth: t0, seed, ngưỡng luật."""
    with Path(path).open(encoding="utf-8") as handle:
        first = handle.readline()
    try:
        header = json.loads(first)
    except json.JSONDecodeError as exc:
        raise GroundTruthError(f"{path}: dòng đầu không phải JSON: {exc}") from exc
    if not isinstance(header, dict) or header.get("record") != "run" or "t0" not in header:
        raise GroundTruthError(f"{path}: dòng đầu phải là record 'run' có t0")
    return header


def load_ground_truth(path: Path, base_ms: int) -> GroundTruth:
    """Đọc dòng `run` và các `stuck_episode`; thời gian dịch sang ms tuyệt đối theo `base_ms`."""
    header: dict[str, Any] | None = None
    episodes: list[TrueEpisode] = []
    seen: set[tuple[str, str]] = set()
    with Path(path).open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise GroundTruthError(f"{path}:{number}: JSON hỏng: {exc}") from exc
            kind = record.get("record")
            if header is None:
                if kind != "run":
                    raise GroundTruthError(f"{path}: dòng đầu phải là record 'run'")
                if record.get("time_unit") != GROUND_TRUTH_TIME_UNIT:
                    raise GroundTruthError(f"{path}: time_unit phải là {GROUND_TRUTH_TIME_UNIT!r}")
                header = record
                continue
            if kind != "stuck_episode":
                continue
            try:
                episode = TrueEpisode(
                    learner_id=str(record["learner_id"]),
                    skill_id=str(record["skill_id"]),
                    connection=str(record.get("connection", "unknown")),
                    ability_group=str(record.get("ability_group", "unknown")),
                    onset_event_id=str(record["onset_event_id"]),
                    onset_ms=_abs_ms(base_ms, record["onset_t_s"]),
                    end_ms=None if record.get("end_t_s") is None else _abs_ms(base_ms, record["end_t_s"]),
                    latent_mastered_at_onset=bool(record.get("latent_mastered_at_onset", False)),
                )
            except (KeyError, TypeError, ValueError) as exc:
                raise GroundTruthError(f"{path}:{number}: stuck_episode thiếu/sai trường: {exc}") from exc
            if episode.key in seen:
                raise GroundTruthError(f"{path}:{number}: hai episode cho cùng {episode.key}")
            seen.add(episode.key)
            episodes.append(episode)
    if header is None:
        raise GroundTruthError(f"{path}: tệp rỗng")
    return GroundTruth(header=header, episodes=tuple(episodes))


@dataclass(frozen=True)
class JobResult:
    """Một job tại một cutoff: runtime đo được và các onset mà snapshot của nó chứa."""

    cutoff_ms: int
    runtime_s: float
    detections: tuple[Mapping[str, Any], ...] = field(default_factory=tuple)

    @property
    def visible_ms(self) -> int:
        return self.cutoff_ms + round(self.runtime_s * 1000)


@dataclass(frozen=True)
class Detection:
    learner_id: str
    skill_id: str
    cutoff_ms: int
    visible_ms: int
    onset_event_id: str
    onset_event_time_ms: int
    onset_sent_time_ms: int


def first_detections(jobs: Iterable[JobResult]) -> dict[tuple[str, str], Detection]:
    """Phát hiện đầu tiên theo khóa (người học, kỹ năng); job phải theo thứ tự cutoff tăng."""
    found: dict[tuple[str, str], Detection] = {}
    previous = None
    for job in jobs:
        if previous is not None and job.cutoff_ms <= previous:
            raise ValueError("các job phải theo thứ tự cutoff tăng dần")
        previous = job.cutoff_ms
        for row in job.detections:
            key = (str(row["learner_id"]), str(row["skill_id"]))
            if key in found:
                continue
            found[key] = Detection(
                learner_id=key[0],
                skill_id=key[1],
                cutoff_ms=job.cutoff_ms,
                visible_ms=job.visible_ms,
                onset_event_id=str(row["onset_event_id"]),
                onset_event_time_ms=int(row["onset_event_time_ms"]),
                onset_sent_time_ms=int(row["onset_sent_time_ms"]),
            )
    return found


def summarize(values: Sequence[float]) -> dict[str, float | int | None]:
    """n, mean, P50, P95, max (None khi rỗng)."""
    if not values:
        return {"n": 0, "mean": None, "p50": None, "p95": None, "max": None}
    array = np.asarray(values, dtype=float)
    return {
        "n": int(array.size),
        "mean": float(array.mean()),
        "p50": float(np.percentile(array, 50)),
        "p95": float(np.percentile(array, 95)),
        "max": float(array.max()),
    }


def _ratio(numerator: int, denominator: int) -> float | None:
    return numerator / denominator if denominator else None


@dataclass
class EpisodeEvaluation:
    episodes: list[dict[str, Any]]
    false_positives: list[dict[str, Any]]
    summary: dict[str, Any]


def evaluate_episodes(
    truth: Sequence[TrueEpisode],
    detections: Mapping[tuple[str, str], Detection],
    schedule: Schedule,
) -> EpisodeEvaluation:
    """Precision/recall theo episode, time-to-detect, tỉ lệ phát hiện khi episode còn mở."""
    rows: list[dict[str, Any]] = []
    ttd: list[float] = []
    by_connection: dict[str, list[dict[str, Any]]] = defaultdict(list)
    open_hits = onset_match = trigger_holds = 0
    truth_keys = set()
    for episode in truth:
        truth_keys.add(episode.key)
        found = detections.get(episode.key)
        row: dict[str, Any] = {
            "learner_id": episode.learner_id,
            "skill_id": episode.skill_id,
            "connection": episode.connection,
            "ability_group": episode.ability_group,
            "latent_mastered_at_onset": episode.latent_mastered_at_onset,
            "true_onset_event_id": episode.onset_event_id,
            "true_onset_ms": episode.onset_ms,
            "true_end_ms": episode.end_ms,
            "detected": found is not None,
            "detect_cutoff_ms": None,
            "visible_ms": None,
            "time_to_detect_s": None,
            "detected_while_open": None,
            "onset_event_match": None,
            "trigger_rule_holds": None,
        }
        if found is not None:
            seconds = (found.visible_ms - episode.onset_ms) / 1000
            while_open = episode.end_ms is None or found.visible_ms < episode.end_ms
            same_onset = found.onset_event_id == episode.onset_event_id
            holds = found.cutoff_ms == schedule.cutoff_at_or_after(found.onset_sent_time_ms)
            row.update(
                detect_cutoff_ms=found.cutoff_ms,
                visible_ms=found.visible_ms,
                time_to_detect_s=seconds,
                detected_while_open=while_open,
                onset_event_match=same_onset,
                trigger_rule_holds=holds,
            )
            ttd.append(seconds)
            open_hits += while_open
            onset_match += same_onset
            trigger_holds += holds
        rows.append(row)
        by_connection[episode.connection].append(row)

    false_positives = [
        {
            "learner_id": d.learner_id,
            "skill_id": d.skill_id,
            "detect_cutoff_ms": d.cutoff_ms,
            "visible_ms": d.visible_ms,
            "onset_event_id": d.onset_event_id,
            "onset_event_time_ms": d.onset_event_time_ms,
        }
        for key, d in sorted(detections.items())
        if key not in truth_keys
    ]
    true_positives = len(ttd)
    summary = {
        "true_episodes": len(truth),
        "detected_episodes": len(detections),
        "true_positives": true_positives,
        "false_positives": len(false_positives),
        "false_negatives": len(truth) - true_positives,
        "precision": _ratio(true_positives, len(detections)),
        "recall": _ratio(true_positives, len(truth)),
        "time_to_detect_s": summarize(ttd),
        "detected_while_open": open_hits,
        "detected_while_open_share": _ratio(open_hits, true_positives),
        "onset_event_match": onset_match,
        "trigger_rule_holds": trigger_holds,
        "by_connection": {
            connection: {
                "true_episodes": len(items),
                "recall": _ratio(sum(r["detected"] for r in items), len(items)),
                "time_to_detect_s": summarize([r["time_to_detect_s"] for r in items if r["detected"]]),
            }
            for connection, items in sorted(by_connection.items())
        },
    }
    return EpisodeEvaluation(episodes=rows, false_positives=false_positives, summary=summary)


def visibility_latency(
    event_ms: np.ndarray,
    sent_ms: np.ndarray,
    cutoffs_ms: Sequence[int],
    runtimes_s: Sequence[float],
) -> dict[str, Any]:
    """Độ cũ theo sự kiện: visible_time của job đầu tiên có cutoff ≥ sent_time, trừ event_time (giây)."""
    if len(cutoffs_ms) != len(runtimes_s):
        raise ValueError("cutoffs_ms và runtimes_s phải cùng độ dài")
    events = np.asarray(event_ms, dtype=np.int64)
    sent = np.asarray(sent_ms, dtype=np.int64)
    if events.shape != sent.shape:
        raise ValueError("event_ms và sent_ms phải cùng kích thước")
    cut = np.asarray(cutoffs_ms, dtype=np.int64)
    if cut.size == 0:
        return {"events": int(events.size), "not_covered": int(events.size), **{f"{k}_s": v for k, v in summarize([]).items() if k != "n"}}
    if np.any(np.diff(cut) <= 0):
        raise ValueError("cutoffs_ms phải tăng ngặt")
    visible_at = cut + np.round(np.asarray(runtimes_s, dtype=float) * 1000).astype(np.int64)
    index = np.searchsorted(cut, sent, side="left")
    covered = index < cut.size
    latency_s = (visible_at[index[covered]] - events[covered]) / 1000.0
    stats = summarize(latency_s.tolist())
    return {
        "events": int(events.size),
        "not_covered": int((~covered).sum()),
        "mean_s": stats["mean"],
        "p50_s": stats["p50"],
        "p95_s": stats["p95"],
        "max_s": stats["max"],
    }


def write_csv(path: Path, rows: Iterable[Mapping[str, Any]], columns: Sequence[str]) -> int:
    """Ghi CSV (UTF-8, LF); None thành ô trống. Trả số dòng dữ liệu."""
    path.parent.mkdir(parents=True, exist_ok=True)
    count = 0
    with path.open("w", encoding="utf-8", newline="") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(columns), lineterminator="\n", extrasaction="ignore")
        writer.writeheader()
        for row in rows:
            writer.writerow({k: ("" if row.get(k) is None else row.get(k)) for k in columns})
            count += 1
    return count
