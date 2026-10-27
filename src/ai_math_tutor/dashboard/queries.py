"""Đọc Redis cho dashboard (chỉ đọc) và áp ngưỡng k cho mọi số liệu nhóm.

Dashboard chỉ đọc các khóa do pipeline streaming ghi: tên khóa lấy từ `streaming.sinks.RedisKeys`,
tên trường `metrics:pipeline` từ `streaming.listener.metrics_fields`. Không có lệnh ghi nào ở đây.
Mỗi lần làm mới chỉ đọc khóa tổng hợp (`roster:` để đếm sĩ số, `agg:class:…`, `trend:…`,
`metrics:pipeline`), không quét `state:{learner}` — chi phí theo số lớp × số kỹ năng, không theo số
người học. Chế độ giáo viên đọc thêm `stuck:set:{class}` và trạng thái/gợi ý của đúng những người đó.

Bảo vệ nhóm nhỏ (docs/privacy-threat-note.md, T4: k = 5), áp cho mọi con số cấp lớp/trường:
1. Số đếm < k — kể cả 0 và khóa vắng — hiển thị "<5".
2. Số đếm ≥ k làm tròn tới bội số gần nhất của k ("≈10"). Hai lần làm mới, hay hai khối xu hướng
   liền nhau, vì vậy chỉ chênh nhau 0 hoặc ≥ k: trừ hai màn hình không cho ra "đúng 1 người vừa đổi".
3. Tỉ lệ thành thạo chỉ hiện khi số thành thạo, số có dữ liệu và phần bù (chưa thành thạo) đều ≥ k;
   tỉ lệ tính từ các số đã làm tròn nên cũng chỉ đổi khi một số đếm hiển thị đổi.
4. Tổng hàng/cột: làm tròn như (1)–(2), và chỉ hiện khi mọi ô của hàng/cột đó đều ≥ k. Có ô "<5"
   thì tổng hiện "ẩn" — nếu không, tổng − các ô đang hiện = phần của ô bị ẩn (suy ngược bằng hiệu).
   Quyết định ẩn chỉ dựa vào điều đã công khai (ô nào là "<5"), nên bản thân việc ẩn không lộ thêm gì.
"""

from __future__ import annotations

import gc
import json
import time
from collections.abc import Iterable, Iterator, Mapping, Sequence
from contextlib import contextmanager
from dataclasses import dataclass, field, fields
from typing import Literal

from redis import Redis
from redis.exceptions import ResponseError

from ai_math_tutor.streaming.listener import metrics_fields
from ai_math_tutor.streaming.processor import alert_id
from ai_math_tutor.streaming.sinks import (
    TREND_BUCKET_MS,
    TREND_TTL_S,
    RedisKeys,
    SinkError,
    parse_alert_state,
    stream_id_ms,
    trend_bucket,
)

K_MIN = 5
# Số khối 5 phút còn sống trong Redis (TTL của trend:… là 2 h).
TREND_BUCKETS = TREND_TTL_S * 1000 // TREND_BUCKET_MS
RECENT_ALERTS = 20
SCAN_COUNT = 1000
ALERT_PAGE = 100
ALERT_MAX_PAGES = 20
HINT_SUMMARY_CHARS = 160
BELOW_K = "below_k"
SHOWN = "shown"
HIDDEN = "hidden"
HIDDEN_LABEL = "ẩn"
CellState = Literal["shown", "below_k", "hidden"]
RawHash = Mapping[bytes | str, bytes | str]


class DashboardDataError(ValueError):
    """Giá trị trong Redis không đúng định dạng pipeline ghi (khóa hỏng hoặc phiên bản lệch)."""


# ---------------------------------------------------------------------------------------------
# Ngưỡng k: hàm thuần, không đụng Redis.
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class Protected:
    """Một số đếm sau khi bảo vệ: `value` là số được phép hiện (đã làm tròn) hoặc None."""

    value: int | None
    label: str
    state: CellState


@dataclass(frozen=True)
class ProtectedRate:
    value: float | None
    label: str
    state: CellState


def _check_k(k: int) -> int:
    if isinstance(k, bool) or not isinstance(k, int) or k < 2:
        raise ValueError(f"k phải là số nguyên ≥ 2, nhận {k!r}")
    return k


def round_to_k(count: int, k: int = K_MIN) -> int:
    """Bội số gần nhất của k (k lẻ không có trường hợp nằm giữa; k chẵn làm tròn lên)."""
    return ((count + k // 2) // k) * k


def protect_count(count: int, k: int = K_MIN) -> Protected:
    """Quy tắc 1–2: "<k" khi count < k, ngược lại bội số gần nhất của k (không bao giờ < k)."""
    _check_k(k)
    if isinstance(count, bool) or not isinstance(count, int) or count < 0:
        raise ValueError(f"số đếm phải là số nguyên ≥ 0, nhận {count!r}")
    if count < k:
        return Protected(None, f"<{k}", BELOW_K)
    value = max(k, round_to_k(count, k))
    return Protected(value, f"≈{value}", SHOWN)


def protect_total(cells: Sequence[int], k: int = K_MIN) -> Protected:
    """Quy tắc 4: tổng của một hàng/cột (giá trị thật của từng ô)."""
    _check_k(k)
    total = protect_count(sum(cells), k)
    if total.state == BELOW_K:
        return total
    if any(protect_count(c, k).state == BELOW_K for c in cells):
        return Protected(None, HIDDEN_LABEL, HIDDEN)
    return total


def protect_rate(numerator: int, denominator: int, k: int = K_MIN, *,
                 numerator_name: str = "thành thạo", complement_name: str = "chưa thành thạo",
                 denominator_name: str = "có dữ liệu") -> ProtectedRate:
    """Quy tắc 3: tử, mẫu và phần bù đều ≥ k; tỉ lệ = tử đã làm tròn / mẫu đã làm tròn."""
    _check_k(k)
    if numerator < 0 or denominator < 0 or numerator > denominator:
        raise ValueError(f"tỉ lệ không hợp lệ: {numerator}/{denominator}")
    if denominator < k:
        return ProtectedRate(None, f"<{k} {denominator_name}", BELOW_K)
    if numerator < k:
        return ProtectedRate(None, f"<{k} {numerator_name}", BELOW_K)
    if denominator - numerator < k:
        return ProtectedRate(None, f"<{k} {complement_name}", BELOW_K)
    shown_num = protect_count(numerator, k).value
    shown_den = protect_count(denominator, k).value
    assert shown_num is not None and shown_den is not None
    value = min(1.0, shown_num / shown_den)
    return ProtectedRate(value, f"≈{value:.0%}", SHOWN)


def rate_from_totals(numerator: Protected, denominator: Protected, raw_numerator: int, raw_denominator: int,
                     k: int = K_MIN) -> ProtectedRate:
    """Tỉ lệ toàn trường: ẩn khi một trong hai tổng bị ẩn (tỉ lệ sẽ để lộ tổng đó)."""
    if HIDDEN in (numerator.state, denominator.state):
        return ProtectedRate(None, HIDDEN_LABEL, HIDDEN)
    return protect_rate(raw_numerator, raw_denominator, k)


# ---------------------------------------------------------------------------------------------
# Giải mã giá trị Redis.
# ---------------------------------------------------------------------------------------------


def _text(value: bytes | str) -> str:
    return value.decode("utf-8") if isinstance(value, bytes) else str(value)


def _decode_hash(raw: RawHash) -> dict[str, str]:
    return {_text(k): _text(v) for k, v in raw.items()}


def _opt_int(text: str | int | None, name: str) -> int | None:
    if text is None or text == "":
        return None
    try:
        number = int(text) if isinstance(text, int) and not isinstance(text, bool) else int(str(text).strip())
    except ValueError as exc:
        raise DashboardDataError(f"{name} không phải số nguyên: {text!r}") from exc
    return number


def _req_count(values: Mapping[str, str | int], name: str) -> int:
    number = _opt_int(values.get(name), name)
    if number is None or number < 0:
        raise DashboardDataError(f"{name} thiếu hoặc âm: {values.get(name)!r}")
    return number


def _opt_float(text: str | None, name: str) -> float | None:
    if text is None or text == "":
        return None
    try:
        return float(text)
    except ValueError as exc:
        raise DashboardDataError(f"{name} không phải số: {text!r}") from exc


def glob_escape(text: str) -> str:
    """Thoát ký tự đặc biệt của mẫu SCAN MATCH (tiền tố khóa do người chạy đặt)."""
    return "".join("\\" + ch if ch in "*?[]\\" else ch for ch in text)


# ---------------------------------------------------------------------------------------------
# Chế độ lớp: sĩ số, tổng hợp theo kỹ năng, xu hướng 5 phút.
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class SkillCounts:
    """Số đếm của một (lớp, kỹ năng) đọc từ `agg:class:{class}:skill:{skill}`."""

    observed: int
    mastered: int
    stuck: int
    as_of_event_ms: int | None = None
    batch_id: int | None = None

    @classmethod
    def parse(cls, values: Mapping[str, str | int], where: str) -> SkillCounts:
        try:
            observed = _req_count(values, "observed")
            mastered = _req_count(values, "mastered")
            stuck = _req_count(values, "stuck")
            counts = cls(observed, mastered, stuck,
                         _opt_int(values.get("as_of_event_ms"), "as_of_event_ms"),
                         _opt_int(values.get("batch_id"), "batch_id"))
        except DashboardDataError as exc:
            raise DashboardDataError(f"{where}: {exc}") from exc
        if mastered > observed or stuck > observed:
            raise DashboardDataError(f"{where}: mastered/stuck lớn hơn observed ({mastered}, {stuck} > {observed})")
        return counts


EMPTY_COUNTS = SkillCounts(0, 0, 0)
# Trường của hash agg (sinks.class_aggregates + batch_id) mà dashboard dùng.
AGG_FIELDS = ("observed", "mastered", "stuck", "as_of_event_ms", "batch_id")


@dataclass
class ClassSnapshot:
    """Một lần đọc chế độ lớp. Chỉ hàm view bên dưới được chuyển số thật thành nhãn hiển thị."""

    classes: tuple[str, ...]
    skills: tuple[str, ...]
    learners: dict[str, int]
    counts: dict[tuple[str, str], SkillCounts]
    problems: list[str] = field(default_factory=list)
    commands: int = 0
    read_ms: float = 0.0

    def cell(self, class_id: str, skill_id: str) -> SkillCounts:
        return self.counts.get((class_id, skill_id), EMPTY_COUNTS)

    def as_of_event_ms(self, class_id: str | None = None) -> int | None:
        values = [c.as_of_event_ms for (cls, _), c in self.counts.items()
                  if c.as_of_event_ms is not None and (class_id is None or cls == class_id)]
        return max(values) if values else None

    def batch_id(self) -> int | None:
        values = [c.batch_id for c in self.counts.values() if c.batch_id is not None]
        return max(values) if values else None


@contextmanager
def _gc_paused() -> Iterator[None]:
    """Tạm dừng GC vòng (cyclic) trong lúc nhận vài chục nghìn đối tượng ngắn hạn của một pipeline.

    Các đối tượng này không tạo chu trình nên vẫn được giải phóng ngay bằng đếm tham chiếu; chỉ
    bỏ các lượt quét thế hệ 2 rơi vào giữa lần đọc. Đo ở 10K người học (286 lớp × 12 kỹ năng, Redis
    thật): P95 một lần làm mới 80,7 ms khi để GC, 41,1 ms khi tạm dừng. Trả lại trạng thái cũ khi xong
    (luồng khác đang tạm dừng thì luồng đó tự bật lại).
    """
    was_enabled = gc.isenabled()
    gc.disable()
    try:
        yield
    finally:
        if was_enabled:
            gc.enable()


def discover_classes(client: Redis, keys: RedisKeys, scan_count: int = SCAN_COUNT) -> list[str]:
    """Danh sách lớp = các khóa `roster:{class}` (runner nạp từ roster.json) dưới tiền tố của run."""
    head = keys.roster("")
    pattern = glob_escape(head) + "*"
    classes = {_text(key)[len(head):] for key in client.scan_iter(match=pattern, count=scan_count)}
    return sorted(c for c in classes if c)


def read_class_snapshot(client: Redis, keys: RedisKeys, classes: Sequence[str], skills: Sequence[str]) -> ClassSnapshot:
    """Một pipeline: SCARD roster mỗi lớp + HMGET agg mỗi (lớp, kỹ năng). Khóa vắng = 0.

    HMGET chỉ lấy đúng các trường cần (ở 286 lớp × 12 kỹ năng nhanh hơn HGETALL gần một nửa trên Redis
    thật vì client phải giải mã ít hơn).
    """
    started = time.perf_counter()
    classes = tuple(classes)
    skills = tuple(skills)
    with _gc_paused():
        pipe = client.pipeline(transaction=False)
        for class_id in classes:
            pipe.scard(keys.roster(class_id))
        for class_id in classes:
            for skill_id in skills:
                pipe.hmget(keys.agg(class_id, skill_id), AGG_FIELDS)
        replies = pipe.execute()
        learners = {c: int(n) for c, n in zip(classes, replies[:len(classes)])}
        snapshot = ClassSnapshot(classes, skills, learners, {}, commands=len(replies))
        pairs = [(c, s) for c in classes for s in skills]
        for (class_id, skill_id), raw in zip(pairs, replies[len(classes):]):
            if all(v is None for v in raw):
                continue
            where = keys.agg(class_id, skill_id)
            values = {name: _text(v) for name, v in zip(AGG_FIELDS, raw) if v is not None}
            try:
                snapshot.counts[(class_id, skill_id)] = SkillCounts.parse(values, where)
            except DashboardDataError as exc:
                snapshot.problems.append(str(exc))
    snapshot.read_ms = (time.perf_counter() - started) * 1000.0
    return snapshot


def heatmap_rows(snapshot: ClassSnapshot, k: int = K_MIN) -> list[dict[str, object]]:
    """Ô (lớp × kỹ năng) của heatmap: tỉ lệ thành thạo đã bảo vệ."""
    rows: list[dict[str, object]] = []
    for class_id in snapshot.classes:
        for skill_id in snapshot.skills:
            cell = snapshot.cell(class_id, skill_id)
            rate = protect_rate(cell.mastered, cell.observed, k)
            rows.append({"class_id": class_id, "skill_id": skill_id, "mastery_rate": rate.value, "label": rate.label})
    return rows


def class_learners_label(snapshot: ClassSnapshot, class_id: str, k: int = K_MIN) -> str:
    return protect_count(snapshot.learners.get(class_id, 0), k).label


def class_detail_rows(snapshot: ClassSnapshot, class_id: str, k: int = K_MIN) -> list[dict[str, object]]:
    """Bảng một lớp theo kỹ năng: số có dữ liệu, thành thạo, bế tắc, tỉ lệ (mọi số qua ngưỡng k)."""
    rows: list[dict[str, object]] = []
    for skill_id in snapshot.skills:
        cell = snapshot.cell(class_id, skill_id)
        rows.append({
            "skill_id": skill_id,
            "observed": protect_count(cell.observed, k).label,
            "mastered": protect_count(cell.mastered, k).label,
            "stuck": protect_count(cell.stuck, k).label,
            "mastery_rate": protect_rate(cell.mastered, cell.observed, k).label,
        })
    return rows


def school_rows(snapshot: ClassSnapshot, k: int = K_MIN) -> list[dict[str, object]]:
    """Tổng cột (cộng mọi lớp) theo kỹ năng; mỗi tổng qua quy tắc 4."""
    rows: list[dict[str, object]] = []
    for skill_id in snapshot.skills:
        cells = [snapshot.cell(c, skill_id) for c in snapshot.classes]
        observed = [c.observed for c in cells]
        mastered = [c.mastered for c in cells]
        total_observed = protect_total(observed, k)
        total_mastered = protect_total(mastered, k)
        rate = rate_from_totals(total_mastered, total_observed, sum(mastered), sum(observed), k)
        rows.append({
            "skill_id": skill_id,
            "observed": total_observed.label,
            "mastered": total_mastered.label,
            "stuck": protect_total([c.stuck for c in cells], k).label,
            "mastery_rate": rate.label,
        })
    return rows


def school_learners_label(snapshot: ClassSnapshot, k: int = K_MIN) -> str:
    return protect_total([snapshot.learners.get(c, 0) for c in snapshot.classes], k).label


@dataclass(frozen=True)
class TrendPoint:
    bucket_ms: int
    skill_id: str
    counts: SkillCounts


def trend_bucket_range(anchor_ms: int, buckets: int = TREND_BUCKETS) -> list[int]:
    """Các khối 5 phút có thể còn trong Redis, cũ → mới, kết thúc ở khối chứa `anchor_ms`."""
    last = trend_bucket(anchor_ms)
    return [last - i * TREND_BUCKET_MS for i in range(buckets - 1, -1, -1)]


def read_trend(client: Redis, keys: RedisKeys, class_id: str, anchor_ms: int | None,
               skills: Sequence[str]) -> tuple[list[TrendPoint], list[str]]:
    """HGETALL `trend:{class}:{bucket}` cho các khối tính từ `anchor_ms` (event time mới nhất của lớp)."""
    if anchor_ms is None:
        return [], []
    buckets = trend_bucket_range(anchor_ms)
    pipe = client.pipeline(transaction=False)
    for bucket in buckets:
        pipe.hgetall(keys.trend(class_id, bucket))
    wanted = set(skills)
    points: list[TrendPoint] = []
    problems: list[str] = []
    for bucket, raw in zip(buckets, pipe.execute()):
        for skill_id, text in sorted(_decode_hash(raw).items()):
            if skill_id not in wanted:
                continue
            where = f"{keys.trend(class_id, bucket)}[{skill_id}]"
            try:
                values = json.loads(text)
                if not isinstance(values, dict):
                    raise DashboardDataError(f"{where}: không phải JSON object")
                points.append(TrendPoint(bucket, skill_id, SkillCounts.parse(values, where)))
            except (json.JSONDecodeError, DashboardDataError) as exc:
                problems.append(str(exc) if isinstance(exc, DashboardDataError) else f"{where}: JSON hỏng")
    return points, problems


def trend_rows(points: Iterable[TrendPoint], k: int = K_MIN) -> list[dict[str, object]]:
    """Mỗi (khối, kỹ năng): tỉ lệ thành thạo và số bế tắc đã bảo vệ — cùng quy tắc với ô hiện tại."""
    rows: list[dict[str, object]] = []
    for point in points:
        rate = protect_rate(point.counts.mastered, point.counts.observed, k)
        rows.append({
            "bucket_ms": point.bucket_ms,
            "skill_id": point.skill_id,
            "mastery_rate": rate.value,
            "mastery_label": rate.label,
            "stuck": protect_count(point.counts.stuck, k).label,
        })
    return rows


# ---------------------------------------------------------------------------------------------
# Sức khỏe pipeline: hash `metrics:pipeline` do StreamingQueryListener ghi.
# ---------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class PipelineMetrics:
    """Các trường của `metrics:pipeline` (tên trùng `listener.metrics_fields`; test kiểm khớp)."""

    run_id: str | None = None
    batch_id: int | None = None
    updated_ms: int | None = None
    input_rows_per_s: float | None = None
    processed_rows_per_s: float | None = None
    batch_duration_ms: int | None = None
    foreach_ms: float | None = None
    watermark_ms: int | None = None
    lag_total: int | None = None
    lag_max_partition: int | None = None
    p95_system_ms_1m: float | None = None
    p95_staleness_ms_1m: float | None = None
    latency_samples_1m: int | None = None
    dropped_by_watermark: int | None = None
    rss_bytes: int | None = None


PIPELINE_FIELDS: tuple[str, ...] = tuple(metrics_fields({}))
_STR_FIELDS = {"run_id"}
_FLOAT_FIELDS = {"input_rows_per_s", "processed_rows_per_s", "foreach_ms", "p95_system_ms_1m", "p95_staleness_ms_1m"}


def parse_pipeline_metrics(values: Mapping[str, str]) -> PipelineMetrics:
    """Chuỗi rỗng = chưa có số đo (listener ghi "" khi thiếu); trường lạ bị bỏ qua."""
    parsed: dict[str, str | int | float | None] = {}
    for spec in fields(PipelineMetrics):
        text = values.get(spec.name)
        if spec.name in _STR_FIELDS:
            parsed[spec.name] = text or None
        elif spec.name in _FLOAT_FIELDS:
            parsed[spec.name] = _opt_float(text, spec.name)
        else:
            parsed[spec.name] = _opt_int(text, spec.name)
    return PipelineMetrics(**parsed)  # type: ignore[arg-type]  # kiểu từng trường đã chọn theo tên ở trên


def read_pipeline_metrics(client: Redis, keys: RedisKeys) -> PipelineMetrics | None:
    raw = client.hgetall(keys.metrics_pipeline)
    return parse_pipeline_metrics(_decode_hash(raw)) if raw else None


# ---------------------------------------------------------------------------------------------
# Chế độ giáo viên: người học bế tắc, gợi ý gần nhất, cảnh báo mới nhất.
# ---------------------------------------------------------------------------------------------


def lasthint_key(keys: RedisKeys, learner_id: str) -> str:
    """`lasthint:{learner}` do dịch vụ gợi ý ghi (chuỗi JSON, TTL 1 h); tên khóa từ `RedisKeys`."""
    return keys.lasthint(learner_id)


# Trường của bản ghi lasthint (hints.app) đáng hiện trong một dòng, theo thứ tự.
HINT_SUMMARY_FIELDS = ("hint_level", "problem_id", "step_index", "verified", "text_vi")


def summarize_hint(value: str | Mapping[str, str] | None) -> str | None:
    """Tóm tắt gợi ý gần nhất để hiện một dòng; chấp nhận JSON (dạng dịch vụ gợi ý ghi), chuỗi thường, hash."""
    if value is None:
        return None
    data: object = value
    if isinstance(value, str):
        try:
            data = json.loads(value)
        except json.JSONDecodeError:
            data = value
    if isinstance(data, Mapping):
        parts = []
        for name in HINT_SUMMARY_FIELDS:
            if data.get(name) not in (None, ""):
                parts.append(f"{name}={data[name]}")
        text = "; ".join(parts) if parts else json.dumps(data, ensure_ascii=False, sort_keys=True)
    else:
        text = str(data)
    return text if len(text) <= HINT_SUMMARY_CHARS else text[:HINT_SUMMARY_CHARS - 1] + "…"


@dataclass(frozen=True)
class StuckLearner:
    learner_id: str
    skill_id: str
    onset_event_id: str | None
    onset_event_ms: int | None
    stuck_for_ms: int | None
    consec_wrong: int | None
    opportunities: int | None
    last_event_time_ms: int | None
    last_hint: str | None


def _state_field(text: bytes | str | None, where: str) -> dict[str, object] | None:
    if text is None:
        return None
    try:
        value = json.loads(_text(text))
    except json.JSONDecodeError as exc:
        raise DashboardDataError(f"{where}: JSON hỏng") from exc
    if not isinstance(value, dict):
        raise DashboardDataError(f"{where}: không phải JSON object")
    return value


def _int_field(value: object) -> int | None:
    return value if isinstance(value, int) and not isinstance(value, bool) else None


def read_stuck_learners(client: Redis, keys: RedisKeys, class_id: str,
                        now_ms: int | None = None) -> tuple[list[StuckLearner], list[str]]:
    """Người học trong `stuck:set:{class}` + thời điểm bắt đầu bế tắc + gợi ý gần nhất.

    Thời điểm bắt đầu = `alert_event_time_ms` của cảnh báo `stuck` có cùng sự kiện kích hoạt
    (`alert:{id}` → stream id → mục trong `alerts:stream`). Thời lượng = bây giờ − thời điểm đó
    (bộ phát lại đóng dấu event time theo đồng hồ thật nên hai mốc so được). Thiếu một khóa (cảnh báo
    đã bị cắt khỏi stream, gợi ý hết hạn) thì trường tương ứng để trống, không lỗi.
    """
    now_ms = int(time.time() * 1000) if now_ms is None else now_ms
    members = sorted(_text(m) for m in client.smembers(keys.stuck_set(class_id)))
    problems: list[str] = []
    pairs: list[tuple[str, str]] = []
    for member in members:
        learner, sep, skill = member.partition("|")
        if not sep or not learner or not skill:
            problems.append(f"{keys.stuck_set(class_id)}: phần tử không đúng dạng learner|skill: {member!r}")
            continue
        pairs.append((learner, skill))
    if not pairs:
        return [], problems

    pipe = client.pipeline(transaction=False)
    for learner, skill in pairs:
        pipe.hget(keys.state(learner), skill)
    states: list[dict[str, object] | None] = []
    for (learner, skill), text in zip(pairs, pipe.execute()):
        try:
            states.append(_state_field(text, f"{keys.state(learner)}[{skill}]"))
        except DashboardDataError as exc:
            problems.append(str(exc))
            states.append(None)

    onset_ids = [None if s is None else s.get("stuck_onset_event_id") for s in states]
    alert_ids = [alert_id(learner, skill, "stuck", onset) if isinstance(onset, str) and onset else None
                 for (learner, skill), onset in zip(pairs, onset_ids)]
    pipe = client.pipeline(transaction=False)
    for aid in alert_ids:
        if aid is not None:
            pipe.get(keys.alert(aid))
    alert_values = iter(pipe.execute())
    stream_ids: list[str | None] = []
    for aid in alert_ids:
        if aid is None:
            stream_ids.append(None)
            continue
        raw = next(alert_values)
        if raw is None:
            stream_ids.append(None)
            continue
        try:
            stream_ids.append(parse_alert_state(raw)[1])
        except SinkError as exc:
            problems.append(f"{keys.alert(aid)}: {exc}")
            stream_ids.append(None)

    pipe = client.pipeline(transaction=False)
    for sid in stream_ids:
        if sid is not None:
            pipe.xrange(keys.alerts_stream, min=sid, max=sid, count=1)
    entries = iter(pipe.execute())
    onset_ms: list[int | None] = []
    for sid in stream_ids:
        if sid is None:
            onset_ms.append(None)
            continue
        found = next(entries)
        if not found:
            onset_ms.append(None)
            continue
        values = _decode_hash(found[0][1])
        try:
            onset_ms.append(_opt_int(values.get("alert_event_time_ms"), "alert_event_time_ms"))
        except DashboardDataError as exc:
            problems.append(f"{keys.alerts_stream} {sid}: {exc}")
            onset_ms.append(None)

    hints = read_last_hints(client, keys, sorted({learner for learner, _ in pairs}), problems)
    result: list[StuckLearner] = []
    for (learner, skill), state, onset, started in zip(pairs, states, onset_ids, onset_ms):
        result.append(StuckLearner(
            learner_id=learner,
            skill_id=skill,
            onset_event_id=onset if isinstance(onset, str) else None,
            onset_event_ms=started,
            stuck_for_ms=None if started is None else max(0, now_ms - started),
            consec_wrong=None if state is None else _int_field(state.get("consec_wrong")),
            opportunities=None if state is None else _int_field(state.get("opportunities")),
            last_event_time_ms=None if state is None else _int_field(state.get("last_event_time_ms")),
            last_hint=hints.get(learner),
        ))
    result.sort(key=lambda s: (-(s.stuck_for_ms or 0), s.learner_id, s.skill_id))
    return result, problems


def read_last_hints(client: Redis, keys: RedisKeys, learners: Sequence[str], problems: list[str]) -> dict[str, str]:
    """`lasthint:{learner}` dạng chuỗi (GET) hoặc hash (HGETALL); khóa vắng → không có mục."""
    if not learners:
        return {}
    pipe = client.pipeline(transaction=False)
    for learner in learners:
        pipe.get(lasthint_key(keys, learner))
    hints: dict[str, str] = {}
    as_hash: list[str] = []
    for learner, value in zip(learners, pipe.execute(raise_on_error=False)):
        if isinstance(value, ResponseError):  # WRONGTYPE: gợi ý lưu dạng hash
            as_hash.append(learner)
        elif value is not None:
            summary = summarize_hint(_text(value))
            if summary:
                hints[learner] = summary
    if as_hash:
        pipe = client.pipeline(transaction=False)
        for learner in as_hash:
            pipe.hgetall(lasthint_key(keys, learner))
        for learner, raw in zip(as_hash, pipe.execute(raise_on_error=False)):
            if isinstance(raw, ResponseError):
                problems.append(f"{lasthint_key(keys, learner)}: kiểu khóa không đọc được ({raw})")
            elif raw:
                summary = summarize_hint(_decode_hash(raw))
                if summary:
                    hints[learner] = summary
    return hints


@dataclass(frozen=True)
class AlertEntry:
    stream_id: str
    visible_ms: int
    alert_id: str
    alert_type: str
    learner_id: str
    class_id: str
    skill_id: str
    onset_event_id: str | None
    alert_event_time_ms: int | None


def read_recent_alerts(client: Redis, keys: RedisKeys, limit: int = RECENT_ALERTS, *,
                       page: int = ALERT_PAGE, max_pages: int = ALERT_MAX_PAGES) -> tuple[list[AlertEntry], list[str]]:
    """`limit` cảnh báo mới nhất của `alerts:stream`, khử trùng theo `alert_id` (giữ mục mới nhất)."""
    seen: set[str] = set()
    alerts: list[AlertEntry] = []
    problems: list[str] = []
    upper = "+"
    for _ in range(max_pages):
        entries = client.xrevrange(keys.alerts_stream, max=upper, min="-", count=page)
        if not entries:
            break
        for raw_id, raw_fields in entries:
            sid = _text(raw_id)
            values = _decode_hash(raw_fields)
            aid = values.get("alert_id")
            if not aid:
                problems.append(f"{keys.alerts_stream} {sid}: thiếu alert_id")
                continue
            if aid in seen:
                continue
            seen.add(aid)
            try:
                event_ms = _opt_int(values.get("alert_event_time_ms"), "alert_event_time_ms")
            except DashboardDataError as exc:
                problems.append(f"{keys.alerts_stream} {sid}: {exc}")
                event_ms = None
            alerts.append(AlertEntry(
                stream_id=sid, visible_ms=stream_id_ms(sid), alert_id=aid,
                alert_type=values.get("alert_type", ""), learner_id=values.get("learner_id", ""),
                class_id=values.get("class_id", ""), skill_id=values.get("skill_id", ""),
                onset_event_id=values.get("onset_event_id") or None, alert_event_time_ms=event_ms,
            ))
            if len(alerts) >= limit:
                return alerts, problems
        if len(entries) < page:
            break
        upper = "(" + _text(entries[-1][0])
    return alerts, problems
