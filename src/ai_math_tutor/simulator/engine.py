"""Điều phối một lần sinh: quần thể → hành vi → mạng → events.parquet + ground_truth.jsonl + roster + manifest.

Sinh theo **thời gian sự kiện toàn cục** (heap theo sự kiện kế tiếp của từng người học), rồi
đẩy mỗi lần giao vào heap thứ hai theo `replay_offset_ms`. Vì mọi sự kiện sinh sau có
event_time ≥ thời điểm hiện tại và độ trễ gửi ≥ 0, mọi lần giao có replay ≤ thời điểm hiện
tại đều xuất được ngay ⇒ tệp ra đúng thứ tự truyền mà bộ nhớ chỉ giữ các lần giao "đang bay".
"""

from __future__ import annotations

import heapq
import json
import os
import platform
import random
import sys
import time
import uuid
from collections import Counter
from collections.abc import Iterator
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any

import pyarrow

from ai_math_tutor import learner_rules
from ai_math_tutor.problem_bank import ProblemBank, file_checksum
from ai_math_tutor.simulator.behavior import BehaviorSettings, LearnerSim, PreparedProblem, SimEvent
from ai_math_tutor.simulator.ground_truth import GroundTruthWriter
from ai_math_tutor.simulator.network import NetworkModel
from ai_math_tutor.simulator.output import (
    SCHEMA_VERSION,
    EventParquetWriter,
    IsoClock,
    arrow_schema,
    epoch_ms,
    parse_utc,
)
from ai_math_tutor.simulator.population import (
    LearnerProfile,
    SimulationConfigError,
    build_population,
    derive_seed,
    load_skills_config,
)

SIMULATOR_VERSION = "1.0.0"
EVENT_NAMESPACE = uuid.UUID("0b7e2f4a-58c1-4d3e-9a1f-6c2d8e4b5a70")
OUTPUT_FILES = ("events.parquet", "ground_truth.jsonl", "roster.json", "manifest.json")
REPO_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_PROBLEMS = REPO_ROOT / "data_sample" / "problem-bank-mini.jsonl"
DEFAULT_SKILLS = REPO_ROOT / "configs" / "skills.yaml"
DEFAULT_START = "2026-11-02T00:00:00Z"
_SESSION_MULTIPLIER = 0x9E3779B1  # lẻ ⇒ nhân modulo 2^32 là song ánh ⇒ session_id không trùng


@dataclass(frozen=True)
class SimulationConfig:
    learners: int
    horizon_s: float
    seed: int
    out_dir: Path
    target_rate: float | None = None
    late_profile: str = "low"
    duplicate_rate: float = 0.02
    problems_path: Path = DEFAULT_PROBLEMS
    split: str | None = "dev"
    skills_path: Path = DEFAULT_SKILLS
    class_size: int = 35
    cellular_share: float = 0.4
    start: str = DEFAULT_START
    behavior: BehaviorSettings = field(default_factory=BehaviorSettings)

    def __post_init__(self) -> None:
        if self.horizon_s <= 0:
            raise SimulationConfigError("thời lượng mô phỏng phải > 0")
        if self.target_rate is not None and self.target_rate <= 0:
            raise SimulationConfigError("target_rate phải > 0")
        if not 0 <= self.seed < 2**63:
            raise SimulationConfigError("seed phải là số nguyên không âm < 2^63")

    @property
    def think_mean_s(self) -> float:
        """Think-time trung bình: learners / rate ở chế độ tốc độ, ngược lại think-time tự nhiên."""
        if self.target_rate is None:
            return self.behavior.think_mean_s
        return self.learners / self.target_rate

    def describe(self) -> dict[str, Any]:
        data = asdict(self)
        data["out_dir"] = str(self.out_dir)
        data["problems_path"] = str(self.problems_path)
        data["skills_path"] = str(self.skills_path)
        data["mode"] = "rate" if self.target_rate is not None else "natural"
        data["think_mean_s"] = self.think_mean_s
        return data


class SessionIds:
    """session_id 'S-' + 8 hex duy nhất trong run: song ánh trên 32 bit của (người học, số phiên)."""

    def __init__(self, seed: int, learners: int):
        self._learner_bits = max(1, (learners - 1).bit_length())
        self._session_bits = 32 - self._learner_bits
        self._offset = derive_seed(seed, "session") & 0xFFFFFFFF

    def get(self, learner_index: int, session_no: int) -> str:
        if session_no >= 1 << self._session_bits:
            raise SimulationConfigError("quá nhiều phiên cho một người học trong không gian session_id 32 bit")
        key = (learner_index << self._session_bits) | session_no
        return f"S-{(key * _SESSION_MULTIPLIER + self._offset) & 0xFFFFFFFF:08x}"


def _prepare_curriculum(bank: ProblemBank, skill_order: tuple[str, ...]) -> dict[str, tuple[PreparedProblem, ...]]:
    """Kỹ năng chính có bài, theo thứ tự chương trình của cấu hình kỹ năng."""
    primary = set(bank.primary_skills())
    return {
        skill: tuple(PreparedProblem.from_problem(p) for p in bank.by_primary_skill(skill))
        for skill in skill_order if skill in primary
    }


def _event_json(
    event_id: str, event_type: str, learner_part: str, session_id: str, seq: int,
    problem_part: str, event_time: str, sent_time: str, client_part: str, payload: dict,
) -> str:
    # Ghép mảnh đã json.dumps sẵn; kết quả bằng đúng json.dumps(dict) theo thứ tự trường hợp đồng.
    return (
        f'{{"schema_version": "{SCHEMA_VERSION}", "event_id": "{event_id}", "event_type": "{event_type}", '
        f'{learner_part}, "session_id": "{session_id}", "session_seq": {seq}, {problem_part}, '
        f'"event_time": "{event_time}", "sent_time": "{sent_time}", {client_part}, "payload": {json.dumps(payload)}}}'
    )


def _clean_outputs(out_dir: Path) -> None:
    for name in OUTPUT_FILES:
        (out_dir / name).unlink(missing_ok=True)


def run_simulation(config: SimulationConfig, overwrite: bool = False) -> dict[str, Any]:
    """Sinh đầy đủ đầu ra vào `config.out_dir`; trả manifest (đã ghi ra manifest.json)."""
    started = time.perf_counter()
    out_dir = Path(config.out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)
    existing = [name for name in OUTPUT_FILES if (out_dir / name).exists()]
    if existing and not overwrite:
        raise SimulationConfigError(f"{out_dir} đã có {existing}; dùng --overwrite hoặc thư mục khác")
    _clean_outputs(out_dir)

    full_bank = ProblemBank.from_jsonl(config.problems_path)
    bank = full_bank.select(config.split)
    if not len(bank):
        raise SimulationConfigError(f"kho bài không có bài nào thuộc split {config.split!r}")
    skills_cfg = load_skills_config(config.skills_path)
    bank_skills = {s for p in bank.problems for s in p.skill_ids}
    unknown = sorted(bank_skills - set(skills_cfg.skill_ids))
    if unknown:
        raise SimulationConfigError(f"kỹ năng trong kho bài chưa có tham số trong {config.skills_path}: {unknown}")
    skill_order = tuple(s for s in skills_cfg.skill_ids if s in bank_skills)
    curriculum = _prepare_curriculum(bank, skill_order)
    population = build_population(config.learners, config.seed, skills_cfg, skill_order, config.class_size, config.cellular_share)
    network = NetworkModel(config.late_profile, config.duplicate_rate)

    t0 = parse_utc(config.start)
    t0_ms = epoch_ms(t0)
    horizon_ms = round(config.horizon_s * 1000)
    continuous = config.target_rate is not None
    sims = [
        LearnerSim(p, curriculum, config.behavior, random.Random(derive_seed(config.seed, "behavior", p.index)),
                   horizon_ms, config.think_mean_s, continuous)
        for p in population
    ]
    net_rngs = [random.Random(derive_seed(config.seed, "network", p.index)) for p in population]
    sessions = SessionIds(config.seed, config.learners)
    clock = IsoClock()

    learner_parts = [f'"learner_id": "{p.learner_id}", "class_id": "{p.class_id}"' for p in population]
    client_parts = [
        '"client": ' + json.dumps({"platform": p.platform, "app_version": p.app_version, "connection": p.connection})
        for p in population
    ]
    problem_parts = {
        pp.problem.problem_id: f'"problem_id": "{pp.problem.problem_id}", "skill_ids": {json.dumps(list(pp.problem.skill_ids))}'
        for problems in curriculum.values() for pp in problems
    }

    events_path, gt_path = out_dir / "events.parquet", out_dir / "ground_truth.jsonl"
    writer = EventParquetWriter(events_path, arrow_schema(config.start, t0_ms))
    gt = GroundTruthWriter(gt_path, {
        "t0": config.start,
        "horizon_s": config.horizon_s,
        "seed": config.seed,
        "time_unit": "seconds_from_t0",
        "wheel_spin_opportunities": learner_rules.WHEEL_SPIN_OPPORTUNITIES,
        "mastery_streak": learner_rules.MASTERY_STREAK,
        "late_threshold_s": network.describe()["late_threshold_s"],
    })
    counts: Counter[str] = Counter()
    by_type: Counter[str] = Counter()
    by_connection: dict[str, Counter[str]] = {c: Counter() for c in ("wifi", "cellular")}
    session_keys: set[tuple[int, int]] = set()
    last_event_ms = last_replay_ms = 0

    try:
        gt.learners(population)
        streams: list[Iterator[SimEvent]] = [sim.events() for sim in sims]
        schedule: list[tuple[int, int, SimEvent]] = []
        for index, stream in enumerate(streams):
            first = next(stream, None)
            if first is not None:
                schedule.append((first[0], index, first))
        heapq.heapify(schedule)
        pending: list[tuple[int, int, tuple]] = []
        order = 0

        while schedule:
            t_ms, index, sim_event = schedule[0]

            # Mọi lần giao có replay ≤ t_ms không còn bị sự kiện nào sinh sau vượt lên trước.
            while pending and pending[0][0] <= t_ms:
                row = heapq.heappop(pending)[2]
                writer.append(row)
                last_replay_ms = row[0]

            _, session_no, seq, event_type, problem, payload, learned = sim_event
            learner: LearnerProfile = population[index]
            session_id = sessions.get(index, session_no)
            session_keys.add((index, session_no))
            event_id = str(uuid.uuid5(EVENT_NAMESPACE, f"{session_id}/{seq}"))
            delivery = network.deliver(net_rngs[index], learner.connection, t_ms)
            value = _event_json(
                event_id, event_type, learner_parts[index], session_id, seq, problem_parts[problem.problem_id],
                clock.iso(t0_ms + t_ms), clock.iso(t0_ms + delivery.sent_ms), client_parts[index], payload,
            )
            row = (delivery.sent_ms, t_ms, delivery.sent_ms, event_id, learner.learner_id, learner.class_id,
                   session_id, seq, event_type, problem.problem_id, learner.connection, value)
            order += 1
            heapq.heappush(pending, (delivery.sent_ms, order, row))
            conn_counts = by_connection[learner.connection]
            counts["unique_events"] += 1
            conn_counts["unique_events"] += 1
            by_type[event_type] += 1
            last_event_ms = t_ms
            if delivery.late:
                counts["late_events"] += 1
                conn_counts["late_events"] += 1
                gt.delivery(learner, event_id, "late", t_ms, delivery.sent_ms, delivery.sent_ms)
            if delivery.duplicate_replay_ms is not None:
                order += 1
                heapq.heappush(pending, (delivery.duplicate_replay_ms, order, (delivery.duplicate_replay_ms, *row[1:])))
                counts["duplicate_deliveries"] += 1
                conn_counts["duplicate_deliveries"] += 1
                gt.delivery(learner, event_id, "duplicate", t_ms, delivery.sent_ms, delivery.duplicate_replay_ms)

            gt.observe(learner, sims[index].latent, learned, event_id, t_ms, event_type, payload, problem.skill_ids)
            for skill in learned:
                gt.mastery(learner, skill, event_id, t_ms)

            # Chỉ tiến người học này sau khi đã ghi xong sự kiện hiện tại (trạng thái ẩn khớp sự kiện).
            nxt = next(streams[index], None)
            if nxt is None:
                heapq.heappop(schedule)
            else:
                heapq.heapreplace(schedule, (nxt[0], index, nxt))

        while pending:
            row = heapq.heappop(pending)[2]
            writer.append(row)
            last_replay_ms = row[0]
        writer.close()
        gt_stats = gt.close()
    except BaseException:
        writer.abort()
        gt.abort()
        _clean_outputs(out_dir)
        raise

    roster: dict[str, list[str]] = {}
    for learner in population:
        roster.setdefault(learner.class_id, []).append(learner.learner_id)
    roster_path = out_dir / "roster.json"
    roster_path.write_text(json.dumps(roster, indent=2, sort_keys=True) + "\n", encoding="utf-8", newline="\n")

    unique = counts["unique_events"]
    shares = {c: by_connection[c]["unique_events"] / unique for c in by_connection} if unique else {}
    manifest: dict[str, Any] = {
        "simulator_version": SIMULATOR_VERSION,
        "schema_version": SCHEMA_VERSION,
        "seed": config.seed,
        "t0": config.start,
        "t0_epoch_ms": t0_ms,
        "horizon_s": config.horizon_s,
        "params": config.describe(),
        "problem_bank": {
            "path": str(config.problems_path),
            "checksum": file_checksum(config.problems_path),
            "split": config.split,
            "problems_total": len(full_bank),
            "problems_in_split": len(bank),
            "skills_practised": list(curriculum),
            "skills_parametrised": list(skill_order),
        },
        "skills_config": {"path": str(config.skills_path), "checksum": file_checksum(config.skills_path)},
        "network": network.describe(),
        "counts": {
            "learners": len(population),
            "classes": len(roster),
            "sessions": len(session_keys),
            "unique_events": unique,
            "deliveries": writer.rows,
            "duplicate_deliveries": counts["duplicate_deliveries"],
            "late_events": counts["late_events"],
            "by_event_type": dict(sorted(by_type.items())),
            "by_connection": {c: dict(sorted(v.items())) for c, v in by_connection.items()},
            "learners_by_connection": dict(sorted(Counter(p.connection for p in population).items())),
        },
        "rates": {
            "duplicate_rate_configured": config.duplicate_rate,
            "duplicate_rate_realized": counts["duplicate_deliveries"] / unique if unique else 0.0,
            "late_rate_expected": network.expected_late_rate(shares),
            "late_rate_realized": counts["late_events"] / unique if unique else 0.0,
            "target_rate_events_per_s": config.target_rate,
            "realized_unique_events_per_s": unique / config.horizon_s,
        },
        "time_range_s": {"last_event": last_event_ms / 1000.0, "last_replay": last_replay_ms / 1000.0},
        "ground_truth": gt_stats,
        "outputs": {
            "events.parquet": {"rows": writer.rows, "checksum": file_checksum(events_path)},
            "ground_truth.jsonl": {"records": sum(gt_stats["records"].values()), "checksum": file_checksum(gt_path)},
            "roster.json": {"classes": len(roster), "checksum": file_checksum(roster_path)},
        },
        "environment": {
            "python": sys.version.split()[0],
            "pyarrow": pyarrow.__version__,
            "platform": platform.platform(),
            "cpu_count": os.cpu_count(),
        },
        "generation_seconds": round(time.perf_counter() - started, 3),
    }
    (out_dir / "manifest.json").write_text(json.dumps(manifest, indent=2, ensure_ascii=False) + "\n", encoding="utf-8", newline="\n")
    return manifest
