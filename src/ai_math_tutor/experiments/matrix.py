"""Ma trận thí nghiệm: YAML (configs/experiments/*.yaml) → danh sách run có thứ tự, resume, ước lượng giờ máy.

Một **cấu hình** là bộ tham số đã chuẩn hóa của một loại run (`stream`, `hint`, `batch`, `ingest`);
`slug` là tên đọc được, tất định của cấu hình. Một **spec** = (cấu hình, seed). Hai thí nghiệm khai
báo cùng một cấu hình (vd. cấu hình mặc định nằm trong A, B, C, D và ablation) dùng chung spec: run
chạy một lần và phục vụ mọi thí nghiệm trong `groups` (cùng dữ liệu, cùng seed).

Cách mở rộng một khối (`experiments.<nhãn>`, `ablations.<tên>`, `stress.<nhãn>`):
- `grid`: tích Descartes các danh sách;
- `vary`: đổi từng yếu tố một quanh cấu hình nền (mỗi giá trị một cấu hình);
- `extra`: danh sách bộ ghi đè thêm;
- khóa tham số đặt thẳng trong khối ghi đè mặc định cho cả khối; `seeds`, `repeats` riêng khối được.
Batch baseline của C (`batch_baselines`) sinh spec `batch` phụ thuộc spec streaming nguồn cùng seed.

Resume: một spec **xong** khi `runs.jsonl` có bản ghi `success` mang tiền tố run_id của spec; bản ghi
`failed`/`timeout` là một lần thử; đủ `max_attempts` lần thử không thành công ⇒ **abandoned**.
Bản ghi `aborted` do runner bị dừng giữa run (`metrics.interrupted`) không tính là lần thử.
"""

from __future__ import annotations

import itertools
import json
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from pathlib import Path
from typing import Any

import yaml

from ai_math_tutor.batch.schedule import SCHEDULES

KINDS = ("stream", "hint", "batch", "ingest")
MODES = ("eager", "ordered", "none")
LEARNER_MODELS = ("rule", "bkt")
# Khớp simulator.network.LATE_PROFILES và hints.generator.GENERATORS (có test đối chiếu); chép lại để
# đọc kế hoạch không phải nạp bộ mô phỏng/dịch vụ gợi ý.
LATE_PROFILES = ("high", "low")
HINT_GENERATORS = ("template", "ladder")
ABLATION_PREFIX = "ablation:"
RUN_ID_MAX = 128
STAMP_LEN = len("20261027T153000Z")
# Bộ mô phỏng sinh dư một khoảng sau thời lượng run; bộ phát lại cắt ở `duration_s`.
DATASET_MARGIN_S = 30
RUN_PREFIX = {"stream": "st", "batch": "bt", "ingest": "in"}
HINT_RECORD_PREFIX = "hint-api-u"  # run_id do scripts/hint-load-test.py đặt: hint-api-u<users>-<tag>
FAILED_ATTEMPT = ("failed", "timeout")

STREAM_FIELDS = ("learners", "rate_events_per_s", "kafka_partitions", "replay_processes", "trigger", "mode",
                 "watermark", "dedup", "learner_model", "late_profile", "duplicate_rate", "duration_s", "warmup_s")
INGEST_FIELDS = ("learners", "rate_events_per_s", "kafka_partitions", "replay_processes", "late_profile",
                 "duplicate_rate", "duration_s")
# Kho bài của bộ mô phỏng: thí nghiệm chạy trên tập test 1.000 bài (kho v2); ghi đè được theo khối.
PROBLEM_DEFAULTS: dict[str, Any] = {"problem_bank": "data_sample/problem-bank-v2.jsonl", "problem_split": "test"}
PROBLEM_SPLITS = ("test", "dev")
HINT_FIELDS = ("concurrent_users", "hint_generator", "duration_s", "pacing_s")
HINT_DEFAULTS: dict[str, Any] = {"concurrent_users": 10, "hint_generator": "ladder", "duration_s": 300,
                                 "pacing_s": 1.0}
BLOCK_KEYS = {"kind", "question", "grid", "vary", "extra", "seeds", "repeats", "slices", "batch_baselines", "_section"}

# Ước lượng thời gian (giây) ngoài thời lượng đo — từ số đo trước đó trên i9-14900K/WSL2:
# khởi động Spark + query ~40 s, drain bằng heartbeat vài giây, đối soát, dọn topic/checkpoint.
STREAM_OVERHEAD_S = 120.0
RECONCILE_EVENTS_PER_S = 50_000.0
SIMULATOR_EVENTS_PER_S = 40_000.0  # 6M sự kiện sinh trong ~148 s (số đo bộ mô phỏng)
HINT_OVERHEAD_S = 45.0
BATCH_RUN_S = 90.0                 # log 10 phút: 1–2 cutoff/lịch, khởi động Spark + kiểm kê
INGEST_OVERHEAD_S = 60.0

_INTERVAL_RE = re.compile(r"^\s*([0-9]+(?:\.[0-9]+)?)\s*(s|sec|secs|second|seconds|m|min|mins|minute|minutes)?\s*$",
                          re.IGNORECASE)


class PlanError(ValueError):
    """Kế hoạch thí nghiệm sai cấu trúc hoặc giá trị ngoài miền."""


def parse_interval_s(value: Any) -> int:
    """30, "30s", "30 seconds", "2 minutes" → số giây nguyên > 0 (cùng cú pháp với `streaming.job`)."""
    if isinstance(value, bool):
        raise PlanError(f"khoảng thời gian không hợp lệ: {value!r}")
    match = _INTERVAL_RE.match(str(value))
    if not match:
        raise PlanError(f"khoảng thời gian không hợp lệ: {value!r}")
    unit = (match.group(2) or "s").lower()
    seconds = float(match.group(1)) * (60 if unit.startswith("m") else 1)
    if seconds <= 0 or seconds != int(seconds):
        raise PlanError(f"khoảng thời gian phải là số giây nguyên > 0: {value!r}")
    return int(seconds)


def _int(value: Any, name: str, low: int, high: int | None = None) -> int:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or value != int(value):
        raise PlanError(f"{name} phải là số nguyên, nhận {value!r}")
    number = int(value)
    if number < low or (high is not None and number > high):
        raise PlanError(f"{name} phải trong [{low}, {high if high is not None else '∞'}], nhận {value!r}")
    return number


def _rate(value: Any, name: str = "rate_events_per_s") -> int | float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not value > 0:
        raise PlanError(f"{name} phải là số > 0, nhận {value!r}")
    return int(value) if float(value).is_integer() else float(value)


def _choice(value: Any, name: str, choices: Sequence[str]) -> str:
    if value not in choices:
        raise PlanError(f"{name} phải thuộc {tuple(choices)}, nhận {value!r}")
    return str(value)


def _bool(value: Any, name: str) -> bool:
    # YAML 1.1 đọc on/off thành bool; chấp nhận cả chuỗi để kế hoạch viết kiểu nào cũng được.
    if isinstance(value, bool):
        return value
    if value in ("on", "off"):
        return value == "on"
    raise PlanError(f"{name} phải là true/false (hoặc on/off), nhận {value!r}")


def _fraction(value: Any, name: str) -> float:
    if isinstance(value, bool) or not isinstance(value, (int, float)) or not 0 <= value < 1:
        raise PlanError(f"{name} phải trong [0, 1), nhận {value!r}")
    return float(value)


def _problems(raw: Mapping[str, Any]) -> dict[str, str]:
    bank = raw.get("problem_bank", PROBLEM_DEFAULTS["problem_bank"])
    if not isinstance(bank, str) or not bank.endswith(".jsonl"):
        raise PlanError(f"problem_bank phải là đường dẫn .jsonl, nhận {bank!r}")
    split = _choice(raw.get("problem_split", PROBLEM_DEFAULTS["problem_split"]), "problem_split", PROBLEM_SPLITS)
    return {"problem_bank": bank, "problem_split": split}


def problems_tag(p: Mapping[str, Any]) -> str:
    """`data_sample/problem-bank-v2.jsonl` + test → `pbv2test`: kho bài nằm trong slug và khóa dữ liệu."""
    problems = {**PROBLEM_DEFAULTS, **{k: p[k] for k in PROBLEM_DEFAULTS if k in p}}
    stem = Path(str(problems["problem_bank"])).stem
    stem = stem[len("problem-bank-"):] if stem.startswith("problem-bank-") else stem
    return "pb" + re.sub(r"[^A-Za-z0-9]", "", stem) + str(problems["problem_split"])


def _num_text(value: int | float) -> str:
    """Số → chuỗi an toàn cho tên run/topic (không có '+' như 1e+06)."""
    if float(value).is_integer():
        return str(int(value))
    return repr(float(value))


# ---------------------------------------------------------------------------------------------
# Chuẩn hóa tham số từng loại run.
# ---------------------------------------------------------------------------------------------


def normalize_stream(raw: Mapping[str, Any]) -> dict[str, Any]:
    missing = [name for name in STREAM_FIELDS if name not in raw]
    if missing:
        raise PlanError(f"cấu hình stream thiếu {missing}")
    mode = _choice(raw["mode"], "mode", MODES)
    trigger = raw["trigger"]
    duration = _int(raw["duration_s"], "duration_s", 1)
    warmup = _int(raw["warmup_s"], "warmup_s", 0)
    if warmup >= duration:
        raise PlanError(f"warmup_s ({warmup}) phải < duration_s ({duration})")
    return {
        "learners": _int(raw["learners"], "learners", 1),
        "rate_events_per_s": _rate(raw["rate_events_per_s"]),
        "kafka_partitions": _int(raw["kafka_partitions"], "kafka_partitions", 1, 64),
        "replay_processes": _int(raw["replay_processes"], "replay_processes", 1, 64),
        "trigger_s": None if str(trigger).strip().lower() == "asap" else parse_interval_s(trigger),
        "mode": mode,
        # Chế độ none không có watermark: tham số này không ảnh hưởng run nên không thuộc cấu hình.
        "watermark_s": None if mode == "none" else parse_interval_s(raw["watermark"]),
        "dedup": _bool(raw["dedup"], "dedup"),
        "learner_model": _choice(raw["learner_model"], "learner_model", LEARNER_MODELS),
        "late_profile": _choice(raw["late_profile"], "late_profile", LATE_PROFILES),
        "duplicate_rate": _fraction(raw["duplicate_rate"], "duplicate_rate"),
        "duration_s": duration,
        "warmup_s": warmup,
        **_problems(raw),
    }


def normalize_ingest(raw: Mapping[str, Any]) -> dict[str, Any]:
    missing = [name for name in INGEST_FIELDS if name not in raw]
    if missing:
        raise PlanError(f"cấu hình ingest thiếu {missing}")
    return {
        "learners": _int(raw["learners"], "learners", 1),
        "rate_events_per_s": _rate(raw["rate_events_per_s"]),
        "kafka_partitions": _int(raw["kafka_partitions"], "kafka_partitions", 1, 64),
        "replay_processes": _int(raw["replay_processes"], "replay_processes", 1, 64),
        "late_profile": _choice(raw["late_profile"], "late_profile", LATE_PROFILES),
        "duplicate_rate": _fraction(raw["duplicate_rate"], "duplicate_rate"),
        "duration_s": _int(raw["duration_s"], "duration_s", 1),
        **_problems(raw),
    }


def normalize_hint(raw: Mapping[str, Any]) -> dict[str, Any]:
    pacing = raw["pacing_s"]
    if isinstance(pacing, bool) or not isinstance(pacing, (int, float)) or not pacing > 0:
        raise PlanError(f"pacing_s phải > 0, nhận {pacing!r}")
    return {
        "concurrent_users": _int(raw["concurrent_users"], "concurrent_users", 1, 10_000),
        "hint_generator": _choice(raw["hint_generator"], "hint_generator", HINT_GENERATORS),
        "duration_s": _int(raw["duration_s"], "duration_s", 1),
        "pacing_s": float(pacing),
    }


def stream_slug(p: Mapping[str, Any]) -> str:
    parts = [f"l{p['learners']}", f"r{_num_text(p['rate_events_per_s'])}", f"p{p['kafka_partitions']}",
             f"rp{p['replay_processes']}", p["mode"]]
    if p["watermark_s"] is not None:
        parts.append(f"w{p['watermark_s']}")
    parts += [p["late_profile"], f"dup{_num_text(round(p['duplicate_rate'] * 100, 6))}"]
    if not p["dedup"]:
        parts.append("nodedup")
    parts += [p["learner_model"], f"t{'asap' if p['trigger_s'] is None else p['trigger_s']}", f"{p['duration_s']}s",
              problems_tag(p)]
    return "-".join(parts)


def ingest_slug(p: Mapping[str, Any]) -> str:
    return "-".join([f"l{p['learners']}", f"r{_num_text(p['rate_events_per_s'])}", f"p{p['kafka_partitions']}",
                     f"rp{p['replay_processes']}", p["late_profile"],
                     f"dup{_num_text(round(p['duplicate_rate'] * 100, 6))}", f"{p['duration_s']}s", problems_tag(p)])


def hint_slug(p: Mapping[str, Any]) -> str:
    return f"u{p['concurrent_users']}-{p['hint_generator']}-{p['duration_s']}s-pace{_num_text(p['pacing_s'])}"


def dataset_key(params: Mapping[str, Any], seed: int) -> str:
    """Khóa cache dữ liệu mô phỏng: chỉ những tham số mà bộ mô phỏng dùng (không có partition/mode...)."""
    return "-".join([f"l{params['learners']}", f"r{_num_text(params['rate_events_per_s'])}", params["late_profile"],
                     f"dup{_num_text(round(params['duplicate_rate'] * 100, 6))}",
                     f"h{params['duration_s'] + DATASET_MARGIN_S}s", problems_tag(params), f"s{seed}"])


# ---------------------------------------------------------------------------------------------
# Spec và kế hoạch.
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RunSpec:
    kind: str
    slug: str
    seed: int
    params: Mapping[str, Any]
    experiment: str                      # nhãn của lần khai báo đầu tiên
    groups: tuple[str, ...]              # mọi thí nghiệm/ablation dùng cấu hình này
    source_slug: str | None = None       # batch: slug cấu hình streaming nguồn

    @property
    def key(self) -> tuple[str, str, int]:
        return (self.kind, self.slug, self.seed)

    @property
    def run_prefix(self) -> str:
        """Tiền tố run_id của mọi lần thử của spec (dùng để resume từ runs.jsonl)."""
        if self.kind == "hint":
            return f"{HINT_RECORD_PREFIX}{self.params['concurrent_users']}-{self.slug}-s{self.seed}-"
        return f"{RUN_PREFIX[self.kind]}-{self.slug}-s{self.seed}-"

    def run_id(self, stamp: str) -> str:
        run_id = f"{self.run_prefix}{stamp}"
        if len(run_id) > RUN_ID_MAX:
            raise PlanError(f"run_id quá dài ({len(run_id)} > {RUN_ID_MAX}): {run_id}")
        return run_id

    def hint_tag(self, run_id: str) -> str:
        """Nhãn `--tag` truyền cho scripts/hint-load-test.py để run_id nó đặt đúng bằng `run_id`."""
        prefix = f"{HINT_RECORD_PREFIX}{self.params['concurrent_users']}-"
        if self.kind != "hint" or not run_id.startswith(prefix):
            raise PlanError(f"{run_id} không phải run_id của spec hint {self.slug}")
        return run_id[len(prefix):]

    def describe(self) -> dict[str, Any]:
        return {"kind": self.kind, "config_key": self.slug, "seed": self.seed, "experiment": self.experiment,
                "groups": list(self.groups), "params": dict(self.params), "source_config_key": self.source_slug}


@dataclass
class Plan:
    path: Path
    version: str
    defaults: dict[str, Any]
    slo: dict[str, Any]
    sustainable: dict[str, Any]
    labels: list[str]                     # nhãn khối theo thứ tự chạy
    specs: list[RunSpec]
    blocks: dict[str, dict[str, Any]] = field(default_factory=dict)

    def label_specs(self, label: str) -> list[RunSpec]:
        return [spec for spec in self.specs if label in spec.groups]


def _block_params(kind: str, defaults: Mapping[str, Any], block: Mapping[str, Any]) -> dict[str, Any]:
    fields = {"stream": STREAM_FIELDS, "ingest": INGEST_FIELDS, "hint": HINT_FIELDS}[kind]
    base = dict(HINT_DEFAULTS) if kind == "hint" else {k: defaults[k] for k in fields if k in defaults}
    unknown = [k for k in block if k not in BLOCK_KEYS and k not in fields]
    if unknown:
        raise PlanError(f"khóa không hiểu trong khối {kind}: {unknown}")
    base.update({k: v for k, v in block.items() if k in fields})
    return base


def _overrides(block: Mapping[str, Any], name: str) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    grid = block.get("grid")
    if grid is not None:
        if not isinstance(grid, Mapping) or not all(isinstance(v, list) and v for v in grid.values()):
            raise PlanError(f"{name}.grid phải là mapping khóa → danh sách không rỗng")
        keys = list(grid)
        out += [dict(zip(keys, combo)) for combo in itertools.product(*(grid[k] for k in keys))]
    vary = block.get("vary")
    if vary is not None:
        if not isinstance(vary, Mapping) or not all(isinstance(v, list) and v for v in vary.values()):
            raise PlanError(f"{name}.vary phải là mapping khóa → danh sách không rỗng")
        out += [{key: value} for key, values in vary.items() for value in values]
    extra = block.get("extra")
    if extra is not None:
        if not isinstance(extra, list) or not all(isinstance(e, Mapping) for e in extra):
            raise PlanError(f"{name}.extra phải là danh sách mapping")
        out += [dict(e) for e in extra]
    return out or [{}]


def _normalize(kind: str, raw: Mapping[str, Any]) -> tuple[dict[str, Any], str]:
    if kind == "stream":
        params = normalize_stream(raw)
        return params, stream_slug(params)
    if kind == "ingest":
        params = normalize_ingest(raw)
        return params, ingest_slug(params)
    params = normalize_hint(raw)
    return params, hint_slug(params)


def _seeds(block: Mapping[str, Any], defaults: Mapping[str, Any], name: str, repeats_override: int | None) -> list[int]:
    seeds = block.get("seeds", defaults.get("seeds"))
    if not isinstance(seeds, list) or not seeds or not all(isinstance(s, int) and not isinstance(s, bool) and s >= 0
                                                            for s in seeds):
        raise PlanError(f"{name}: seeds phải là danh sách số nguyên ≥ 0")
    if len(set(seeds)) != len(seeds):
        raise PlanError(f"{name}: seeds trùng nhau")
    # --repeats chỉ ghi đè khối không tự khai báo repeats (vd. soak giữ 1 run).
    repeats = block.get("repeats", repeats_override if repeats_override is not None else defaults.get("repeats", 3))
    repeats = _int(repeats, f"{name}.repeats", 1)
    if repeats > len(seeds):
        raise PlanError(f"{name}: repeats {repeats} > số seed ({len(seeds)})")
    return seeds[:repeats]


def _matches(params: Mapping[str, Any], selector: Mapping[str, Any]) -> bool:
    for key, value in selector.items():
        if key == "trigger":
            want = None if str(value).strip().lower() == "asap" else parse_interval_s(value)
            if params.get("trigger_s") != want:
                return False
        elif key == "watermark":
            if params.get("watermark_s") != parse_interval_s(value):
                return False
        elif key == "dedup":
            if params.get("dedup") != _bool(value, "dedup"):
                return False
        elif params.get(key) != value:
            return False
    return True


def _blocks(raw: Mapping[str, Any]) -> dict[str, dict[str, Any]]:
    """Nhãn → khối, gồm thí nghiệm chính, ablation (`ablation:<tên>`) và stress."""
    blocks: dict[str, dict[str, Any]] = {}
    for section, prefix in (("experiments", ""), ("ablations", ABLATION_PREFIX), ("stress", "")):
        value = raw.get(section) or {}
        if not isinstance(value, Mapping):
            raise PlanError(f"mục {section} phải là mapping nhãn → khối")
        for name, block in value.items():
            if not isinstance(block, Mapping):
                raise PlanError(f"{section}.{name} phải là mapping")
            label = f"{prefix}{name}"
            if label in blocks:
                raise PlanError(f"nhãn trùng: {label}")
            kind = block.get("kind")
            if kind not in ("stream", "hint", "ingest"):
                raise PlanError(f"{label}.kind phải thuộc stream|hint|ingest, nhận {kind!r}")
            blocks[label] = {**block, "_section": section}
    if not blocks:
        raise PlanError("kế hoạch không có thí nghiệm nào")
    return blocks


def _ordered_labels(raw_order: Any, blocks: Mapping[str, Any]) -> list[str]:
    if raw_order is None:
        return list(blocks)
    if not isinstance(raw_order, list):
        raise PlanError("order phải là danh sách nhãn")
    labels: list[str] = []
    for item in raw_order:
        if item == "ablations":
            labels += [label for label in blocks if label.startswith(ABLATION_PREFIX)]
        elif item in blocks:
            labels.append(item)
        else:
            raise PlanError(f"order có nhãn không tồn tại: {item!r}")
    labels += [label for label in blocks if label not in labels]
    return list(dict.fromkeys(labels))


def build_plan(raw: Mapping[str, Any], path: Path, repeats_override: int | None = None) -> Plan:
    if not isinstance(raw, Mapping):
        raise PlanError(f"{path}: kế hoạch phải là mapping YAML")
    defaults = raw.get("defaults") or {}
    if not isinstance(defaults, Mapping):
        raise PlanError("defaults phải là mapping")
    blocks = _blocks(raw)
    labels = _ordered_labels(raw.get("order"), blocks)
    specs: dict[tuple[str, str, int], RunSpec] = {}

    def add(spec: RunSpec) -> None:
        existing = specs.get(spec.key)
        if existing is None:
            specs[spec.key] = spec
        elif spec.experiment not in existing.groups:
            specs[spec.key] = replace(existing, groups=(*existing.groups, spec.experiment))

    for label in labels:
        block = blocks[label]
        kind = block["kind"]
        base = _block_params(kind, defaults, block)
        seeds = _seeds(block, defaults, label, repeats_override)
        configs: list[tuple[dict[str, Any], str]] = []
        for override in _overrides(block, label):
            unknown = [k for k in override if k not in base]
            if unknown:
                raise PlanError(f"{label}: tham số không thuộc loại {kind}: {unknown}")
            configs.append(_normalize(kind, {**base, **override}))
        for params, slug in dict(((slug, (params, slug)) for params, slug in configs)).values():
            for seed in seeds:
                add(RunSpec(kind, slug, seed, params, label, (label,)))
        batch = block.get("batch_baselines")
        if batch is not None:
            if kind != "stream" or not isinstance(batch, Mapping):
                raise PlanError(f"{label}.batch_baselines chỉ dành cho khối stream, dạng mapping")
            schedules = batch.get("schedules") or []
            if not schedules or any(s not in SCHEDULES for s in schedules):
                raise PlanError(f"{label}.batch_baselines.schedules phải là tập con của {SCHEDULES}")
            selector = batch.get("source") or {}
            sources = [(p, s) for p, s in configs if _matches(p, selector)]
            if not sources:
                raise PlanError(f"{label}.batch_baselines.source không khớp cấu hình nào")
            for params, slug in dict(((s, (p, s)) for p, s in sources)).values():
                for schedule in schedules:
                    for seed in seeds:
                        add(RunSpec("batch", f"{schedule}-{slug}", seed,
                                    {"schedule": schedule, "impl": "dataframe", "source": dict(params)},
                                    label, (label,), source_slug=slug))

    plan = Plan(path=path, version=str(raw.get("version", "")), defaults=dict(defaults), slo=dict(raw.get("slo") or {}),
                sustainable={"window_s": 300, "lag_slope_ratio": 0.01, **(raw.get("sustainable") or {})},
                labels=labels, specs=list(specs.values()), blocks=blocks)
    for spec in plan.specs:
        spec.run_id("0" * STAMP_LEN)  # kiểm độ dài run_id ngay khi đọc kế hoạch
    return plan


def load_plan(path: Path, repeats_override: int | None = None) -> Plan:
    try:
        raw = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    except OSError as exc:
        raise PlanError(f"không đọc được {path}: {exc}") from exc
    except yaml.YAMLError as exc:
        raise PlanError(f"{path} không phải YAML hợp lệ: {exc}") from exc
    return build_plan(raw, Path(path), repeats_override)


def resolve_labels(plan: Plan, tokens: Iterable[str]) -> list[str]:
    """`all`, chữ cái A–E, `ablations`, `stress`, `ceiling`, `soak` hoặc nhãn đầy đủ → nhãn theo thứ tự chạy."""
    wanted: set[str] = set()
    for token in tokens:
        token = token.strip()
        if not token:
            continue
        if token == "all":
            wanted |= set(plan.labels)
        elif token == "ablations":
            wanted |= {label for label in plan.labels if label.startswith(ABLATION_PREFIX)}
        elif token == "stress":
            wanted |= {label for label in plan.labels if plan.blocks[label]["_section"] == "stress"}
        elif token == "ceiling":
            wanted |= {label for label in plan.labels if plan.blocks[label]["kind"] == "ingest"}
        elif token in plan.labels:
            wanted.add(token)
        elif len(token) == 1 and token.isalpha():
            matched = {label for label in plan.labels if label.startswith(f"{token.upper()}_")}
            if not matched:
                raise PlanError(f"không có thí nghiệm {token}")
            wanted |= matched
        elif f"{ABLATION_PREFIX}{token}" in plan.labels:
            wanted.add(f"{ABLATION_PREFIX}{token}")
        else:
            raise PlanError(f"không hiểu --exp {token!r}; có: all, A–E, ablations, stress, ceiling, {plan.labels}")
    return [label for label in plan.labels if label in wanted]


def select_specs(plan: Plan, labels: Sequence[str], only: str | None = None) -> list[RunSpec]:
    """Spec của các nhãn theo thứ tự chạy; trong một nhãn, batch chạy sau mọi run streaming."""
    chosen: list[RunSpec] = []
    seen: set[tuple[str, str, int]] = set()
    for label in labels:
        group = plan.label_specs(label)
        for spec in [s for s in group if s.kind != "batch"] + [s for s in group if s.kind == "batch"]:
            if spec.key in seen or (only and only not in spec.slug):
                continue
            seen.add(spec.key)
            chosen.append(spec)
    return chosen


# ---------------------------------------------------------------------------------------------
# Resume.
# ---------------------------------------------------------------------------------------------


def load_records(path: Path) -> list[dict[str, Any]]:
    """Mọi dòng JSON hợp lệ của runs.jsonl (dòng hỏng bị bỏ qua — tệp chỉ ghi nối, không sửa)."""
    if not Path(path).is_file():
        return []
    records = []
    for line in Path(path).read_text(encoding="utf-8").splitlines():
        if not line.strip():
            continue
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


@dataclass(frozen=True)
class SpecState:
    status: str              # done | pending | abandoned
    attempts: int            # số lần thử failed/timeout
    success_run_id: str | None
    last_run_id: str | None
    last_error: str | None


def is_failed_attempt(record: Mapping[str, Any]) -> bool:
    """failed/timeout luôn là một lần thử; aborted chỉ khi không phải do runner bị dừng giữa chừng
    (vd. kiểm tải gợi ý tự hủy vì có `state_source=default` là lỗi thật của run)."""
    status = record.get("status")
    if status in FAILED_ATTEMPT:
        return True
    return status == "aborted" and not (record.get("metrics") or {}).get("interrupted")


def spec_state(spec: RunSpec, records: Sequence[Mapping[str, Any]], max_attempts: int) -> SpecState:
    mine = [r for r in records if str(r.get("run_id", "")).startswith(spec.run_prefix)]
    success = next((r for r in reversed(mine) if r.get("status") == "success"), None)
    failures = [r for r in mine if is_failed_attempt(r)]
    last = mine[-1] if mine else None
    last_error = None
    if failures:
        metrics = failures[-1].get("metrics") or {}
        last_error = str(metrics.get("error") or metrics.get("invalid_reasons") or failures[-1].get("status"))
    if success is not None:
        status = "done"
    elif len(failures) >= max_attempts:
        status = "abandoned"
    else:
        status = "pending"
    return SpecState(status, len(failures), success and str(success["run_id"]),
                     last and str(last.get("run_id")), last_error)


# ---------------------------------------------------------------------------------------------
# Ước lượng giờ máy.
# ---------------------------------------------------------------------------------------------


def expected_events(spec: RunSpec) -> float:
    if spec.kind in ("stream", "ingest"):
        return float(spec.params["rate_events_per_s"]) * spec.params["duration_s"]
    return 0.0


def estimate_seconds(spec: RunSpec) -> float:
    """Giờ máy của một run (không gồm sinh dữ liệu lần đầu, tính riêng theo dataset)."""
    p = spec.params
    if spec.kind == "stream":
        trigger = p["trigger_s"] or 0
        return p["duration_s"] + STREAM_OVERHEAD_S + 2 * trigger + expected_events(spec) / RECONCILE_EVENTS_PER_S
    if spec.kind == "hint":
        return p["duration_s"] + HINT_OVERHEAD_S
    if spec.kind == "batch":
        return BATCH_RUN_S
    return p["duration_s"] + INGEST_OVERHEAD_S


def estimate_plan(specs: Sequence[RunSpec]) -> dict[str, Any]:
    """Giờ máy theo thí nghiệm chính (run dùng chung tính cho nhãn đầu tiên) + sinh dữ liệu + tổng."""
    by_label: dict[str, dict[str, float]] = {}
    datasets: dict[str, float] = {}
    for spec in specs:
        entry = by_label.setdefault(spec.experiment, {"runs": 0, "hours": 0.0, "shared_runs": 0})
        entry["runs"] += 1
        entry["hours"] += estimate_seconds(spec) / 3600
        entry["shared_runs"] += 1 if len(spec.groups) > 1 else 0
        if spec.kind in ("stream", "ingest"):
            datasets.setdefault(dataset_key(spec.params, spec.seed),
                                spec.params["rate_events_per_s"] * (spec.params["duration_s"] + DATASET_MARGIN_S))
    data_hours = sum(datasets.values()) / SIMULATOR_EVENTS_PER_S / 3600
    total = sum(e["hours"] for e in by_label.values()) + data_hours
    return {
        "by_experiment": {k: {**v, "hours": round(v["hours"], 2)} for k, v in by_label.items()},
        "datasets": len(datasets),
        "dataset_generation_hours": round(data_hours, 2),
        "total_runs": len(specs),
        "total_hours": round(total, 2),
    }
