"""Đối soát một run streaming với cửa sổ phát lại, oracle và ground truth → `results/raw/<run_id>/reconcile.json`.

Đầu vào:
- kết quả phát lại `results/raw/replay-<run>.json` (`replay_start_ms`, `cutoff_ms`, `events_path`);
- bộ mô phỏng: `events.parquet` (cửa sổ = các dòng `replay_offset_ms < cutoff`), `ground_truth.jsonl`;
- lake của run: `applied/` (sự kiện đã áp), `alerts/`, `latency/`;
- Redis: `state:{learner}`, `alerts:stream`; `progress.jsonl` của listener (đối chiếu số bị loại).

Đầu ra (khóa chính):
- `dropped`: bị loại = sự kiện duy nhất của cửa sổ − đã áp (tương đương lần giao − đã áp − bản trùng
  có chủ đích), tách theo `client.connection`, theo loại sự kiện, riêng nhóm sự kiện muộn (ground truth);
- `state_vs_oracle`: trạng thái Redis so oracle (chỉ sự kiện không bị loại, đúng thứ tự);
- `state_vs_latent`: thành thạo theo hệ thống so thành thạo ẩn của bộ mô phỏng;
- `alerts`: số alert_id duy nhất, số mục trùng trong stream, so onset của oracle, số alert_id/episode,
  time-to-detect theo hai định nghĩa (hệ thống: visible − sent_time của sự kiện khởi phát; độ cũ:
  visible − event_time), ground truth dịch bằng `abs_ms = replay_start_ms + t_s·1000`;
- `latency`: phân bố hai loại độ trễ trên mọi sự kiện đã áp.

CLI: python -m ai_math_tutor.streaming.reconcile --run-id R --sim-dir DIR --model rule|bkt
     [--replay-json P] [--lake-dir D] [--key-prefix K] [--progress P] [--out P]
"""

from __future__ import annotations

import argparse
import json
import math
import sys
from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

from ai_math_tutor.bkt import DEFAULT_MASTERY_THRESHOLD
from ai_math_tutor.config import ConfigError, load_config, validate_run_id
from ai_math_tutor.streaming.oracle import (
    MODELS,
    OracleError,
    OracleEvent,
    OracleResult,
    ParamsFor,
    compare_state,
    compute_oracle,
    load_window_events,
)
from ai_math_tutor.streaming.sinks import LAKE_FILE, LAKE_SCHEMAS, RedisKeys, latency_summary, nearest_rank

RECONCILE_FILE = "reconcile.json"
MAX_EXAMPLES = 10
REDIS_CHUNK = 1_000


class ReconcileError(RuntimeError):
    """Thiếu/sai đầu vào đối soát."""


# ---------------------------------------------------------------------------------------------
# Đọc đầu vào.
# ---------------------------------------------------------------------------------------------


def read_lake(lake_root: Path, kind: str) -> pa.Table:
    """Gộp mọi `<kind>/batch=*/part-00000.parquet` (bảng rỗng đúng schema khi chưa có batch nào)."""
    schema = LAKE_SCHEMAS[kind]
    files = sorted((Path(lake_root) / kind).glob(f"batch=*/{LAKE_FILE}"))
    if not files:
        return schema.empty_table()
    return pa.concat_tables([pq.read_table(path, schema=schema) for path in files])


@dataclass
class GroundTruth:
    run: dict[str, Any] = field(default_factory=dict)
    learners: dict[str, dict[str, Any]] = field(default_factory=dict)
    mastery: dict[tuple[str, str], float] = field(default_factory=dict)
    episodes: list[dict[str, Any]] = field(default_factory=list)
    late_event_ids: set[str] = field(default_factory=set)


def read_ground_truth(path: Path) -> GroundTruth:
    truth = GroundTruth()
    with Path(path).open(encoding="utf-8") as handle:
        for number, line in enumerate(handle, start=1):
            if not line.strip():
                continue
            try:
                record = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ReconcileError(f"{path}:{number}: JSON hỏng: {exc}") from exc
            kind = record.get("record")
            if kind == "run":
                truth.run = record
            elif kind == "learner":
                truth.learners[record["learner_id"]] = record
            elif kind == "mastery":
                key = (record["learner_id"], record["skill_id"])
                truth.mastery[key] = min(truth.mastery.get(key, math.inf), float(record["t_s"]))
            elif kind == "stuck_episode":
                truth.episodes.append(record)
            elif kind == "delivery" and record.get("kind") == "late":
                truth.late_event_ids.add(record["event_id"])
    return truth


def read_progress(path: Path | None) -> list[dict[str, Any]]:
    if path is None or not Path(path).is_file():
        return []
    records = []
    with Path(path).open(encoding="utf-8") as handle:
        for line in handle:
            if line.strip():
                record = json.loads(line)
                if record.get("event") == "progress":
                    records.append(record)
    return records


def read_redis_states(client: Any, keys: RedisKeys, learners: Iterable[str]) -> dict[tuple[str, str], dict[str, Any]]:
    """`state:{learner}` → {(learner, skill): trạng thái JSON đã giải mã}."""
    ordered = sorted(set(learners))
    states: dict[tuple[str, str], dict[str, Any]] = {}
    for start in range(0, len(ordered), REDIS_CHUNK):
        part = ordered[start:start + REDIS_CHUNK]
        pipe = client.pipeline(transaction=False)
        for learner in part:
            pipe.hgetall(keys.state(learner))
        for learner, raw in zip(part, pipe.execute()):
            for skill, value in raw.items():
                skill_id = skill.decode() if isinstance(skill, bytes) else str(skill)
                text = value.decode() if isinstance(value, bytes) else str(value)
                states[(learner, skill_id)] = json.loads(text)
    return states


def read_alert_stream(client: Any, keys: RedisKeys, count: int = 10_000) -> list[dict[str, Any]]:
    """Mọi mục của `alerts:stream` theo thứ tự (XRANGE theo trang); `visible_ms` = ms của stream id."""
    entries: list[dict[str, Any]] = []
    start = "-"
    while True:
        page = client.xrange(keys.alerts_stream, min=start, max="+", count=count)
        if not page:
            break
        for stream_id, fields in page:
            sid = stream_id.decode() if isinstance(stream_id, bytes) else str(stream_id)
            entry = {(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
                     for k, v in fields.items()}
            entry["stream_id"] = sid
            entry["visible_ms"] = int(sid.split("-", 1)[0])
            entries.append(entry)
        last = entries[-1]["stream_id"]
        start = "(" + last
        if len(page) < count:
            break
    return entries


# ---------------------------------------------------------------------------------------------
# Các phép đối soát (hàm thuần).
# ---------------------------------------------------------------------------------------------


def _rate(part: int, whole: int) -> float | None:
    return None if whole == 0 else part / whole


def drop_report(
    window: Mapping[str, OracleEvent],
    applied_ids: Iterable[str],
    *,
    deliveries: int,
    late_event_ids: set[str],
    spark_progress: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Bị loại = duy nhất trong cửa sổ − đã áp; tách theo kết nối, loại sự kiện, sự kiện muộn."""
    applied = set(applied_ids)
    unique = len(window)
    dropped_ids = set(window) - applied
    by_connection: dict[str, Counter[str]] = defaultdict(Counter)
    by_type: dict[str, Counter[str]] = defaultdict(Counter)
    late: dict[str, Counter[str]] = defaultdict(Counter)
    for event_id, event in window.items():
        status = "dropped" if event_id in dropped_ids else "applied"
        by_connection[event.connection]["window"] += 1
        by_connection[event.connection][status] += 1
        by_type[event.event_type]["window"] += 1
        by_type[event.event_type][status] += 1
        if event_id in late_event_ids:
            late[event.connection]["window"] += 1
            late[event.connection][status] += 1

    def table(counts: Mapping[str, Counter[str]]) -> dict[str, dict[str, Any]]:
        return {name: {"window": c["window"], "applied": c["applied"], "dropped": c["dropped"],
                       "drop_rate": _rate(c["dropped"], c["window"])} for name, c in sorted(counts.items())}

    injected = deliveries - unique
    return {
        "deliveries": deliveries,
        "unique_events": unique,
        "injected_duplicates": injected,
        "applied_unique": len(applied & set(window)),
        "applied_not_in_window": len(applied - set(window)),
        "total": len(dropped_ids),
        "total_by_formula": deliveries - len(applied & set(window)) - injected,
        "rate": _rate(len(dropped_ids), unique),
        "by_connection": table(by_connection),
        "by_event_type": table(by_type),
        "late_events_by_connection": table(late),
        "spark_dropped_by_watermark_rows": sum(int(p.get("dropped_by_watermark") or 0) for p in spark_progress),
        "spark_dropped_duplicate_rows": sum(int(p.get("dropped_duplicates") or 0) for p in spark_progress),
    }


def applied_duplicates(applied: pa.Table) -> dict[str, Any]:
    """event_id xuất hiện ở hơn một batch = áp hai lần (restart nhân đôi)."""
    ids = applied.column("event_id").to_pylist()
    counts = Counter(ids)
    repeated = {k: v for k, v in counts.items() if v > 1}
    return {"rows": len(ids), "unique": len(counts), "event_ids_applied_more_than_once": len(repeated),
            "examples": sorted(repeated)[:MAX_EXAMPLES]}


def state_vs_oracle(oracle: OracleResult, system: Mapping[tuple[str, str], Mapping[str, Any]]) -> dict[str, Any]:
    expected_keys = set(oracle.states)
    system_keys = set(system)
    matched = 0
    field_mismatch: Counter[str] = Counter()
    examples: list[dict[str, Any]] = []
    for key in sorted(expected_keys & system_keys):
        diff = compare_state(oracle.states[key], system[key])
        if not diff:
            matched += 1
            continue
        field_mismatch.update(diff)
        if len(examples) < MAX_EXAMPLES:
            exp = oracle.states[key]
            examples.append({"learner_id": key[0], "skill_id": key[1], "fields": diff,
                             "expected": {**exp.profile.to_dict(), "stuck": exp.stuck, "p_mastery": exp.p_mastery},
                             "actual": {k: system[key].get(k) for k in (*exp.profile.to_dict(), "stuck", "p_mastery")
                                        if k in system[key]}})
    missing = sorted(expected_keys - system_keys)
    extra = sorted(system_keys - expected_keys)
    total = len(expected_keys)
    return {
        "model": oracle.model,
        "pairs_expected": total,
        "pairs_in_redis": len(system_keys),
        "matched": matched,
        "accuracy": _rate(matched, total),
        "mismatch_by_field": dict(sorted(field_mismatch.items())),
        "missing_in_redis": len(missing),
        "extra_in_redis": len(extra),
        "examples": examples,
        "missing_examples": [list(k) for k in missing[:MAX_EXAMPLES]],
    }


def latent_mastered(truth: GroundTruth, learner: str, skill: str, end_offset_s: float) -> bool | None:
    learner_record = truth.learners.get(learner)
    if learner_record is None:
        return None
    skill_record = (learner_record.get("skills") or {}).get(skill)
    if skill_record is None:
        return None
    if skill_record.get("mastered_at_start"):
        return True
    t_s = truth.mastery.get((learner, skill))
    return t_s is not None and t_s <= end_offset_s


def state_vs_latent(system: Mapping[tuple[str, str], Mapping[str, Any]], truth: GroundTruth, *,
                    end_offset_s: float, model: str,
                    threshold: float = DEFAULT_MASTERY_THRESHOLD) -> dict[str, Any]:
    """Thành thạo theo luật (và theo BKT nếu model=bkt) so thành thạo ẩn ở cuối cửa sổ."""
    def confusion() -> Counter[str]:
        return Counter(tp=0, fp=0, fn=0, tn=0)

    rule, bkt = confusion(), confusion()
    unknown = 0
    for (learner, skill), state in system.items():
        latent = latent_mastered(truth, learner, skill, end_offset_s)
        if latent is None:
            unknown += 1
            continue
        for counts, predicted in ((rule, bool(state.get("mastered"))),
                                  (bkt, None if state.get("p_mastery") is None
                                   else float(state["p_mastery"]) >= threshold)):
            if predicted is None:
                continue
            counts[("t" if predicted == latent else "f") + ("p" if predicted else "n")] += 1

    def summary(counts: Counter[str]) -> dict[str, Any]:
        n = sum(counts.values())
        return {**dict(counts), "pairs": n, "accuracy": _rate(counts["tp"] + counts["tn"], n)}

    report = {"end_offset_s": end_offset_s, "pairs_without_ground_truth": unknown, "rule_mastered": summary(rule)}
    if model == "bkt":
        report["bkt_threshold"] = threshold
        report["bkt_mastered"] = summary(bkt)
    return report


def _stats(values: Sequence[float]) -> dict[str, Any]:
    array = np.asarray(values, dtype=np.float64)
    return {"n": int(array.size), "p50": nearest_rank(array, 0.50), "p95": nearest_rank(array, 0.95),
            "max": float(array.max()) if array.size else None, "mean": float(array.mean()) if array.size else None}


def alert_report(
    stream: Sequence[Mapping[str, Any]],
    lake_alerts: pa.Table,
    *,
    oracle: OracleResult,
    truth: GroundTruth,
    window: Mapping[str, OracleEvent],
    replay_start_ms: int,
) -> dict[str, Any]:
    """Cảnh báo: trùng trong stream, so oracle, alert_id/episode, time-to-detect (hai định nghĩa)."""
    ids = [e.get("alert_id") for e in stream]
    unique_ids = set(ids)
    by_id: dict[str, Mapping[str, Any]] = {}
    for entry in stream:
        by_id.setdefault(str(entry.get("alert_id")), entry)
    lake_ids = set(lake_alerts.column("alert_id").to_pylist())
    stuck = [e for e in by_id.values() if e.get("alert_type") == "stuck"]

    oracle_onsets = {(o.learner_id, o.skill_id, o.onset_event_id) for o in oracle.stuck_onsets}
    system_onsets = {(e["learner_id"], e["skill_id"], e.get("onset_event_id") or "") for e in stuck}

    alerts_by_pair: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for entry in sorted(stuck, key=lambda e: int(e["visible_ms"])):
        alerts_by_pair[(entry["learner_id"], entry["skill_id"])].append(entry)

    per_episode: Counter[str] = Counter()
    ttd_system: list[float] = []
    ttd_staleness: list[float] = []
    exact = detected = in_window = 0
    for episode in truth.episodes:
        onset_id = episode["onset_event_id"]
        onset_event = window.get(onset_id)
        if onset_event is None:
            continue  # onset ngoài cửa sổ phát lại
        in_window += 1
        onset_abs = replay_start_ms + round(float(episode["onset_t_s"]) * 1000)
        end_abs = math.inf if episode.get("end_t_s") is None else replay_start_ms + round(float(episode["end_t_s"]) * 1000)
        candidates = alerts_by_pair.get((episode["learner_id"], episode["skill_id"]), [])
        in_interval = [a for a in candidates if a.get("onset_event_id") == onset_id
                       or (a.get("alert_event_time_ms") not in (None, "")
                           and onset_abs <= int(a["alert_event_time_ms"]) < end_abs)]
        distinct = len({a["alert_id"] for a in in_interval})
        per_episode["2+" if distinct >= 2 else str(distinct)] += 1
        exact_match = [a for a in in_interval if a.get("onset_event_id") == onset_id]
        chosen = exact_match[0] if exact_match else (in_interval[0] if in_interval else None)
        if chosen is None:
            continue
        detected += 1
        exact += 1 if exact_match else 0
        visible = int(chosen["visible_ms"])
        ttd_system.append(visible - (replay_start_ms + onset_event.sent_offset_ms))
        ttd_staleness.append(visible - onset_abs)

    return {
        "stream_entries": len(stream),
        "unique_alert_ids": len(unique_ids),
        "duplicate_stream_entries": len(stream) - len(unique_ids),
        "by_type": dict(sorted(Counter(str(e.get("alert_type")) for e in by_id.values()).items())),
        "lake_unique_alert_ids": len(lake_ids),
        "lake_matches_stream": lake_ids == unique_ids,
        "vs_oracle": {
            "oracle_onsets": len(oracle_onsets),
            "system_stuck_alerts": len(system_onsets),
            "matched": len(oracle_onsets & system_onsets),
            "missing": len(oracle_onsets - system_onsets),
            "extra": len(system_onsets - oracle_onsets),
        },
        "vs_ground_truth": {
            "episodes_in_window": in_window,
            "detected": detected,
            "detected_exact_onset": exact,
            "recall": _rate(detected, in_window),
            "alert_ids_per_episode": dict(sorted(per_episode.items())),
            "time_to_detect_system_ms": _stats(ttd_system),
            "time_to_detect_staleness_ms": _stats(ttd_staleness),
        },
    }


def latency_report(latency: pa.Table) -> dict[str, Any]:
    system = np.asarray(latency.column("system_latency_ms").to_pylist(), dtype=np.int64)
    staleness = np.asarray(latency.column("staleness_ms").to_pylist(), dtype=np.int64)
    out_of_order = sum(1 for v in latency.column("out_of_order").to_pylist() if v)
    return {**latency_summary(system, staleness), "out_of_order": out_of_order}


# ---------------------------------------------------------------------------------------------
# Ghép.
# ---------------------------------------------------------------------------------------------


def reconcile(
    *,
    run_id: str,
    replay: Mapping[str, Any],
    events_path: Path,
    ground_truth_path: Path,
    lake_root: Path,
    model: str,
    redis_client: Any,
    keys: RedisKeys = RedisKeys(),
    progress_path: Path | None = None,
    params_for: ParamsFor | None = None,
) -> dict[str, Any]:
    if model not in MODELS:
        raise ReconcileError(f"model phải thuộc {MODELS}, nhận {model!r}")
    replay_start_ms = replay.get("replay_start_ms")
    if not isinstance(replay_start_ms, int):
        raise ReconcileError("kết quả phát lại thiếu replay_start_ms")
    cutoff_ms = replay.get("cutoff_ms")
    window = load_window_events(events_path, cutoff_ms)
    if not window:
        raise ReconcileError("cửa sổ phát lại không có sự kiện")
    deliveries = int((replay.get("window") or {}).get("rows") or 0)
    if deliveries < len(window):
        raise ReconcileError(f"window.rows ({deliveries}) < số sự kiện duy nhất ({len(window)})")
    truth = read_ground_truth(ground_truth_path)
    applied = read_lake(lake_root, "applied")
    applied_ids = set(applied.column("event_id").to_pylist())
    progress = read_progress(progress_path)

    oracle = compute_oracle((window[e] for e in applied_ids if e in window), model=model, params_for=params_for)
    learners = {event.learner_id for event in window.values()}
    system = read_redis_states(redis_client, keys, learners)
    stream = read_alert_stream(redis_client, keys)
    end_offset_s = max(event.event_offset_ms for event in window.values()) / 1000.0

    report: dict[str, Any] = {
        "run_id": run_id,
        "model": model,
        "replay_start_ms": replay_start_ms,
        "cutoff_ms": cutoff_ms,
        "replay_stopped_early": bool(replay.get("stopped_early")),
        "inputs": {"events": str(events_path), "ground_truth": str(ground_truth_path), "lake": str(lake_root),
                   "progress": None if progress_path is None else str(progress_path), "key_prefix": keys.prefix},
        "progress_batches": len(progress),
        "final_watermark_ms": next((p["watermark_ms"] for p in reversed(progress) if p.get("watermark_ms")), None),
        "end_of_log_ms": replay.get("end_of_log_ms"),
        "dropped": drop_report(window, applied_ids, deliveries=deliveries, late_event_ids=truth.late_event_ids,
                               spark_progress=progress),
        "applied": applied_duplicates(applied),
        "state_vs_oracle": state_vs_oracle(oracle, system),
        "state_vs_latent": state_vs_latent(system, truth, end_offset_s=end_offset_s, model=model),
        "alerts": alert_report(stream, read_lake(lake_root, "alerts"), oracle=oracle, truth=truth, window=window,
                               replay_start_ms=replay_start_ms),
        "latency": latency_report(read_lake(lake_root, "latency")),
    }
    watermark = report["final_watermark_ms"]
    end_of_log = report["end_of_log_ms"]
    report["checks"] = {
        "no_event_applied_twice": report["applied"]["event_ids_applied_more_than_once"] == 0,
        "no_duplicate_alert_in_stream": report["alerts"]["duplicate_stream_entries"] == 0,
        "lake_alerts_match_stream": report["alerts"]["lake_matches_stream"],
        "watermark_past_end_of_log": None if watermark is None or end_of_log is None else watermark >= end_of_log,
        "applied_subset_of_window": report["dropped"]["applied_not_in_window"] == 0,
    }
    report["status"] = "incomplete" if report["replay_stopped_early"] else "ok"
    return report


def write_report(report: Mapping[str, Any], out_path: Path) -> Path:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return out_path


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--sim-dir", type=Path, required=True, help="thư mục bộ mô phỏng (events.parquet, ground_truth.jsonl)")
    parser.add_argument("--model", choices=MODELS, required=True)
    parser.add_argument("--replay-json", type=Path, default=None, help="mặc định results/raw/replay-<run_id>.json")
    parser.add_argument("--lake-dir", type=Path, default=None, help="mặc định $NHOM8_DATA_ROOT/lake/<run_id>")
    parser.add_argument("--progress", type=Path, default=None, help="mặc định results/raw/<run_id>/progress.jsonl")
    parser.add_argument("--key-prefix", default="", help="tiền tố khóa Redis (chỉ khi run dùng tiền tố)")
    parser.add_argument("--out", type=Path, default=None, help="mặc định results/raw/<run_id>/reconcile.json")
    args = parser.parse_args(argv)
    try:
        run_id = validate_run_id(args.run_id)
        cfg = load_config()
        replay_path = args.replay_json or cfg.paths.results / f"replay-{run_id}.json"
        replay = json.loads(replay_path.read_text(encoding="utf-8"))
        import redis

        client = redis.Redis.from_url(cfg.redis.url, socket_timeout=30)
        report = reconcile(
            run_id=run_id,
            replay=replay,
            events_path=args.sim_dir / "events.parquet",
            ground_truth_path=args.sim_dir / "ground_truth.jsonl",
            lake_root=args.lake_dir or cfg.paths.lake_dir(run_id),
            model=args.model,
            redis_client=client,
            keys=RedisKeys(args.key_prefix),
            progress_path=args.progress or cfg.paths.results / run_id / "progress.jsonl",
        )
        path = write_report(report, args.out or cfg.paths.results / run_id / RECONCILE_FILE)
    except (ConfigError, ReconcileError, OracleError, OSError, json.JSONDecodeError) as exc:
        print(f"reconcile: {exc}", file=sys.stderr)
        return 1
    summary = {"path": str(path), "status": report["status"], "dropped": report["dropped"]["total"],
               "state_accuracy": report["state_vs_oracle"]["accuracy"], "checks": report["checks"]}
    print(json.dumps(summary, ensure_ascii=False))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
