"""Quần thể người học: nạp cấu hình kỹ năng, gán nhóm năng lực, lớp, kết nối và tham số BKT ẩn.

Mọi ngẫu nhiên lấy từ seed qua `derive_seed`, nên cùng seed ⇒ cùng quần thể, và một
người học có cùng tham số dù quần thể lớn hay nhỏ (luồng ngẫu nhiên riêng theo chỉ số).
"""

from __future__ import annotations

import hashlib
import hmac
import random
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import yaml

from ai_math_tutor.bkt import BktParams

# Salt CÔNG KHAI chỉ cho dữ liệu mô phỏng (như salt mẫu tuần 1). source_id giả không bao giờ ra file sự kiện.
SIM_SALT = b"int4418-nhom8-sim-salt-not-secret"
APP_VERSION = "1.0.0"
CONNECTIONS = ("wifi", "cellular")
# Phân bố nền tảng theo kết nối: mạng di động chỉ có trên app điện thoại.
PLATFORMS = {"wifi": (("web", 0.5), ("android", 0.3), ("ios", 0.2)), "cellular": (("android", 0.65), ("ios", 0.35))}
MAX_LEARNERS = 1_000_000
MAX_CLASSES = 99_999


class SimulationConfigError(ValueError):
    """Cấu hình bộ mô phỏng (kỹ năng, quần thể, tham số CLI) không hợp lệ."""


def derive_seed(seed: int, *parts: object) -> int:
    """Seed con tất định cho một luồng ngẫu nhiên (độc lập nền tảng, không phụ thuộc PYTHONHASHSEED)."""
    text = "/".join([str(seed), *map(str, parts)]).encode()
    return int.from_bytes(hashlib.sha256(text).digest()[:8], "big")


@dataclass(frozen=True, slots=True)
class ParamRange:
    low: float
    high: float

    def at(self, q: float) -> float:
        return self.low + q * (self.high - self.low)


@dataclass(frozen=True, slots=True)
class SkillConfig:
    skill_id: str
    kind: str
    name_vi: str
    p_init: ParamRange
    p_learn: ParamRange
    p_guess: ParamRange
    p_slip: ParamRange


@dataclass(frozen=True, slots=True)
class AbilityGroup:
    name: str
    share: float
    quantile: ParamRange


@dataclass(frozen=True)
class SkillsConfig:
    skills: tuple[SkillConfig, ...]
    ability_groups: tuple[AbilityGroup, ...]

    def skill(self, skill_id: str) -> SkillConfig:
        for skill in self.skills:
            if skill.skill_id == skill_id:
                return skill
        raise SimulationConfigError(f"kỹ năng {skill_id} không có trong cấu hình kỹ năng")

    @property
    def skill_ids(self) -> tuple[str, ...]:
        return tuple(s.skill_id for s in self.skills)


def _range(value: Any, where: str) -> ParamRange:
    if (not isinstance(value, list) or len(value) != 2
            or not all(isinstance(v, (int, float)) and not isinstance(v, bool) for v in value)):
        raise SimulationConfigError(f"{where}: cần [thấp, cao] số thực")
    low, high = float(value[0]), float(value[1])
    if not 0.0 <= low <= high <= 1.0:
        raise SimulationConfigError(f"{where}: cần 0 ≤ thấp ≤ cao ≤ 1, nhận {value}")
    return ParamRange(low, high)


def parse_skills_config(data: Mapping[str, Any]) -> SkillsConfig:
    if not isinstance(data, Mapping) or data.get("version") != 1:
        raise SimulationConfigError("cấu hình kỹ năng cần version: 1")
    raw_groups = data.get("ability_groups")
    if not isinstance(raw_groups, Mapping) or not raw_groups:
        raise SimulationConfigError("cần ability_groups")
    groups = []
    for name, spec in raw_groups.items():
        if not isinstance(spec, Mapping) or not isinstance(spec.get("share"), (int, float)) or spec["share"] <= 0:
            raise SimulationConfigError(f"ability_groups.{name}: share phải > 0")
        groups.append(AbilityGroup(str(name), float(spec["share"]), _range(spec.get("quantile"), f"ability_groups.{name}.quantile")))
    total = sum(g.share for g in groups)
    if abs(total - 1.0) > 1e-9:
        raise SimulationConfigError(f"tổng share của ability_groups phải bằng 1, nhận {total}")

    raw_skills = data.get("skills")
    if not isinstance(raw_skills, list) or not raw_skills:
        raise SimulationConfigError("cần danh sách skills")
    skills, seen = [], set()
    for item in raw_skills:
        if not isinstance(item, Mapping) or not isinstance(item.get("skill_id"), str):
            raise SimulationConfigError(f"kỹ năng thiếu skill_id: {item!r}")
        sid = item["skill_id"]
        if sid in seen:
            raise SimulationConfigError(f"skill_id trùng: {sid}")
        seen.add(sid)
        if item.get("kind") not in ("equation", "arithmetic"):
            raise SimulationConfigError(f"{sid}: kind phải là equation hoặc arithmetic")
        bkt = item.get("bkt")
        if not isinstance(bkt, Mapping):
            raise SimulationConfigError(f"{sid}: thiếu bkt")
        ranges = {key: _range(bkt.get(key), f"{sid}.bkt.{key}") for key in ("p_init", "p_learn", "p_guess", "p_slip")}
        # Kiểm hai góc xấu nhất để mọi tham số lấy được đều thỏa ràng buộc BKT.
        try:
            BktParams(ranges["p_init"].low, ranges["p_learn"].low, ranges["p_guess"].low, ranges["p_slip"].low)
            BktParams(ranges["p_init"].high, ranges["p_learn"].high, ranges["p_guess"].high, ranges["p_slip"].high)
        except ValueError as exc:
            raise SimulationConfigError(f"{sid}: {exc}") from exc
        skills.append(SkillConfig(sid, item["kind"], str(item.get("name_vi", sid)), **ranges))
    return SkillsConfig(tuple(skills), tuple(groups))


def load_skills_config(path: Path) -> SkillsConfig:
    with Path(path).open(encoding="utf-8") as handle:
        return parse_skills_config(yaml.safe_load(handle))


@dataclass(frozen=True)
class LearnerProfile:
    """Thuộc tính tĩnh và tham số ẩn của một người học (ground truth, không vào sự kiện)."""

    index: int
    learner_id: str
    class_id: str
    ability_group: str
    connection: str
    platform: str
    app_version: str
    params: Mapping[str, BktParams]
    mastered_at_start: Mapping[str, bool]


def pseudonym(seed: int, index: int) -> str:
    source_id = f"sim-{seed}-{index:07d}"
    return "L-" + hmac.new(SIM_SALT, source_id.encode(), hashlib.sha256).hexdigest()[:16]


def class_id_for(index: int, class_size: int) -> str:
    return f"C-SIM-{index // class_size + 1:04d}"


def _weighted(rng: random.Random, options: Iterable[tuple[str, float]]) -> str:
    items = list(options)
    return rng.choices([name for name, _ in items], weights=[w for _, w in items], k=1)[0]


def build_population(
    learners: int,
    seed: int,
    skills: SkillsConfig,
    skill_ids: Iterable[str],
    class_size: int = 35,
    cellular_share: float = 0.4,
) -> list[LearnerProfile]:
    """Sinh `learners` người học với tham số BKT cho từng kỹ năng trong `skill_ids`."""
    if not 1 <= learners <= MAX_LEARNERS:
        raise SimulationConfigError(f"learners phải trong [1, {MAX_LEARNERS}]")
    if class_size < 1 or (learners - 1) // class_size + 1 > MAX_CLASSES:
        raise SimulationConfigError(f"class_size phải ≥ 1 và tạo ≤ {MAX_CLASSES} lớp")
    if not 0.0 <= cellular_share <= 1.0:
        raise SimulationConfigError("cellular_share phải trong [0, 1]")
    configs = [skills.skill(sid) for sid in skill_ids]
    groups = {g.name: g for g in skills.ability_groups}
    population, seen_ids = [], set()
    for index in range(learners):
        rng = random.Random(derive_seed(seed, "population", index))
        group = groups[_weighted(rng, ((g.name, g.share) for g in skills.ability_groups))]
        connection = "cellular" if rng.random() < cellular_share else "wifi"
        platform = _weighted(rng, PLATFORMS[connection])
        params, mastered = {}, {}
        for cfg in configs:
            q = rng.uniform(group.quantile.low, group.quantile.high)
            p = BktParams(
                p_init=cfg.p_init.at(q),
                p_learn=cfg.p_learn.at(q),
                p_guess=rng.uniform(cfg.p_guess.low, cfg.p_guess.high),
                p_slip=cfg.p_slip.at(1.0 - q),
            )
            params[cfg.skill_id] = p
            mastered[cfg.skill_id] = rng.random() < p.p_init
        learner_id = pseudonym(seed, index)
        if learner_id in seen_ids:
            raise SimulationConfigError(f"trùng bút danh ở người học {index}; đổi seed")
        seen_ids.add(learner_id)
        population.append(LearnerProfile(
            index=index,
            learner_id=learner_id,
            class_id=class_id_for(index, class_size),
            ability_group=group.name,
            connection=connection,
            platform=platform,
            app_version=APP_VERSION,
            params=params,
            mastered_at_start=mastered,
        ))
    return population
