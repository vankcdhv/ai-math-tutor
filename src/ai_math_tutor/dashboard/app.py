"""Dashboard lớp (Streamlit): 3 tab Lớp / Giáo viên / Pipeline, tự làm mới mỗi 2 s, chỉ đọc Redis.

Chạy trên host, chỉ nghe trên loopback:
    streamlit run src/ai_math_tutor/dashboard/app.py --server.address 127.0.0.1 --server.port 8501

Biến môi trường (hoặc `.env`):
    NHOM8_DASHBOARD_KEY_PREFIX  tiền tố khóa Redis của run (rỗng khi chạy thật; demo/test dùng tiền tố riêng)
    NHOM8_TEACHER_MODE=1        cho phép chế độ giáo viên; còn cần nhập đúng TEACHER_PASSWORD trên giao diện
    TEACHER_PASSWORD            mật khẩu chế độ giáo viên (không có ⇒ chế độ giáo viên luôn tắt)
    NHOM8_DASHBOARD_LOG_LEVEL   DEBUG ⇒ ghi mỗi lần làm mới tab Lớp (batch, ms đọc Redis) ra stderr
Cấu hình Redis (host, cổng, REDIS_PASSWORD) đọc qua `ai_math_tutor.config.load_config`.

Mỗi tab là một `st.fragment(run_every=...)`: chỉ phần đó chạy lại theo chu kỳ, không chạy lại cả trang.
Tab gắn với query param `?tab=` (mở thẳng một tab) và chỉ tab đang mở mới đọc Redis.
"""

from __future__ import annotations

import hmac
import logging
import os
import re
import sys
import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from ai_math_tutor.config import DEFAULT_DOTENV_PATH, ConfigError, load_config, read_dotenv
from ai_math_tutor.dashboard import queries
from ai_math_tutor.streaming.sinks import RedisKeys

LOGGER = logging.getLogger("ai_math_tutor.dashboard")
REFRESH = "2s"
TABS = ("Lớp", "Giáo viên", "Pipeline")
# Tín hiệu rủi ro của dashboard: một lần làm mới đọc Redis quá 1 s.
SLOW_REFRESH_MS = 1000.0
CLASS_LIST_TTL_S = 30
LOOPBACK = ("127.0.0.1", "localhost", "::1")
STALE_METRICS_S = 30
_PREFIX_RE = re.compile(r"^[A-Za-z0-9._:-]{0,64}$")
_TRUE = ("1", "true", "yes", "on")


@dataclass(frozen=True)
class DashboardSettings:
    key_prefix: str = ""
    teacher_enabled: bool = False
    teacher_password: str | None = field(default=None, repr=False)

    @property
    def teacher_available(self) -> bool:
        return self.teacher_enabled and bool(self.teacher_password)


def load_settings(env: Mapping[str, str] | None = None, dotenv_path: Path | None = DEFAULT_DOTENV_PATH) -> DashboardSettings:
    """Biến môi trường > .env, giống `load_config`. Tiền tố khóa chỉ nhận ký tự an toàn."""
    merged: dict[str, str] = read_dotenv(dotenv_path) if dotenv_path else {}
    merged.update(os.environ if env is None else env)
    prefix = merged.get("NHOM8_DASHBOARD_KEY_PREFIX", "")
    if not _PREFIX_RE.match(prefix):
        raise ConfigError(f"NHOM8_DASHBOARD_KEY_PREFIX chỉ gồm [A-Za-z0-9._:-], tối đa 64 ký tự: {prefix!r}")
    return DashboardSettings(
        key_prefix=prefix,
        teacher_enabled=merged.get("NHOM8_TEACHER_MODE", "").strip().lower() in _TRUE,
        teacher_password=merged.get("TEACHER_PASSWORD") or None,
    )


def password_matches(given: str, settings: DashboardSettings) -> bool:
    """So mật khẩu thời gian hằng; chế độ giáo viên tắt hoặc chưa đặt mật khẩu ⇒ luôn sai."""
    if not settings.teacher_available or not given:
        return False
    assert settings.teacher_password is not None
    return hmac.compare_digest(given.encode("utf-8"), settings.teacher_password.encode("utf-8"))


def bind_is_loopback(address: str | None, environment: str) -> bool:
    """Trên host chỉ cho nghe loopback; trong container (profile app) cổng publish ra 127.0.0.1 của host."""
    if environment == "container":
        return True
    return (address or "").strip() in LOOPBACK


def configure_logging(env: Mapping[str, str] | None = None) -> None:
    """Một handler stderr cho logger của dashboard (mốc epoch để so với thời điểm ghi Redis)."""
    level_name = (os.environ if env is None else env).get("NHOM8_DASHBOARD_LOG_LEVEL", "WARNING").upper()
    level = logging.getLevelName(level_name)
    if not isinstance(level, int):
        raise ConfigError(f"NHOM8_DASHBOARD_LOG_LEVEL không hợp lệ: {level_name!r}")
    LOGGER.setLevel(level)
    if not LOGGER.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(logging.Formatter("%(created).3f %(levelname)s %(name)s %(message)s"))
        LOGGER.addHandler(handler)
        LOGGER.propagate = False


def log_refresh(batch_id: int | None, read_ms: float) -> None:
    level = logging.WARNING if read_ms > SLOW_REFRESH_MS else logging.DEBUG
    LOGGER.log(level, "tab Lớp làm mới: batch=%s đọc Redis %.1f ms", batch_id, read_ms)


def format_ms(epoch_ms: int | None) -> str:
    if epoch_ms is None:
        return "—"
    return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")


def format_duration(ms: int | None) -> str:
    if ms is None:
        return "—"
    seconds = ms // 1000
    hours, rest = divmod(seconds, 3600)
    minutes, secs = divmod(rest, 60)
    return f"{hours}h{minutes:02d}m{secs:02d}s" if hours else f"{minutes}m{secs:02d}s"


def pipeline_table(metrics: queries.PipelineMetrics, now_ms: int) -> list[dict[str, str]]:
    """Các dòng bảng Pipeline (chỉ định dạng, không đổi giá trị listener đã ghi)."""
    def num(value: float | int | None, unit: str = "", digits: int = 2) -> str:
        if value is None:
            return "—"
        return f"{value:,.{digits}f}{unit}" if isinstance(value, float) else f"{value:,}{unit}"

    age = None if metrics.updated_ms is None else max(0, now_ms - metrics.updated_ms)
    rss = None if metrics.rss_bytes is None else metrics.rss_bytes / 2**20
    return [
        {"chỉ số": "run_id", "giá trị": metrics.run_id or "—"},
        {"chỉ số": "batch_id", "giá trị": num(metrics.batch_id)},
        {"chỉ số": "cập nhật (progress timestamp)", "giá trị": format_ms(metrics.updated_ms)},
        {"chỉ số": "tuổi số liệu", "giá trị": "—" if age is None else f"{age / 1000:.1f} s"},
        {"chỉ số": "input rows/s", "giá trị": num(metrics.input_rows_per_s)},
        {"chỉ số": "processed rows/s", "giá trị": num(metrics.processed_rows_per_s)},
        {"chỉ số": "batch duration", "giá trị": num(metrics.batch_duration_ms, " ms")},
        {"chỉ số": "foreachBatch", "giá trị": num(metrics.foreach_ms, " ms")},
        {"chỉ số": "watermark", "giá trị": format_ms(metrics.watermark_ms)},
        {"chỉ số": "processing lag (tổng offset)", "giá trị": num(metrics.lag_total)},
        {"chỉ số": "processing lag (partition lớn nhất)", "giá trị": num(metrics.lag_max_partition)},
        {"chỉ số": "P95 độ trễ hệ thống (1 phút)", "giá trị": num(metrics.p95_system_ms_1m, " ms", 1)},
        {"chỉ số": "P95 độ cũ trạng thái (1 phút)", "giá trị": num(metrics.p95_staleness_ms_1m, " ms", 1)},
        {"chỉ số": "số mẫu độ trễ (1 phút)", "giá trị": num(metrics.latency_samples_1m)},
        {"chỉ số": "bị loại vì watermark (batch)", "giá trị": num(metrics.dropped_by_watermark)},
        {"chỉ số": "RSS driver + JVM", "giá trị": "—" if rss is None else f"{rss:,.0f} MiB"},
    ]


# ---------------------------------------------------------------------------------------------
# Giao diện. Chỉ chạy khi Streamlit thực thi tệp (import để test không mở giao diện).
# ---------------------------------------------------------------------------------------------


def main() -> None:  # noqa: C901 — một hàm dựng trang, chia theo tab
    import altair as alt
    import pandas as pd
    import redis
    import streamlit as st

    from ai_math_tutor.simulator.population import load_skills_config
    from ai_math_tutor.streaming.processor import SKILLS_CONFIG_PATH

    st.set_page_config(page_title="Nhóm 8 — Dashboard lớp", layout="wide")
    try:
        cfg = load_config()
        settings = load_settings()
        redis_url = cfg.redis.url
        configure_logging()
    except ConfigError as exc:
        st.error(f"Cấu hình không hợp lệ: {exc}")
        st.stop()
    if not bind_is_loopback(st.get_option("server.address"), cfg.environment):
        st.error("Dashboard chỉ được nghe trên 127.0.0.1: chạy với --server.address 127.0.0.1")
        st.stop()

    @st.cache_resource
    def redis_client(url: str) -> redis.Redis:
        return redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2, health_check_interval=30)

    @st.cache_data
    def skill_catalog() -> tuple[tuple[str, ...], dict[str, str]]:
        skills_cfg = load_skills_config(SKILLS_CONFIG_PATH)
        return skills_cfg.skill_ids, {s.skill_id: s.name_vi for s in skills_cfg.skills}

    @st.cache_data(ttl=CLASS_LIST_TTL_S)
    def class_list(_client: redis.Redis, prefix: str) -> list[str]:
        return queries.discover_classes(_client, RedisKeys(prefix))

    client = redis_client(redis_url)
    keys = RedisKeys(settings.key_prefix)
    skills, skill_names = skill_catalog()

    st.title("Dashboard lớp — trạng thái người học theo thời gian thực")
    st.caption(f"Redis {cfg.redis.redacted_url} · tiền tố khóa `{settings.key_prefix or '(không)'}` · "
               f"ngưỡng k = {queries.K_MIN} · làm mới mỗi {REFRESH}")

    def show_problems(problems: list[str]) -> None:
        if problems:
            st.warning(f"{len(problems)} giá trị Redis sai định dạng bị bỏ qua: " + "; ".join(problems[:3]))

    def classes_or_stop() -> list[str] | None:
        try:
            classes = class_list(client, settings.key_prefix)
        except redis.RedisError as exc:
            st.error(f"Không đọc được Redis: {type(exc).__name__}: {exc}")
            return None
        if not classes:
            st.info("Chưa có lớp nào (chưa có khóa roster:{class}) — chạy pipeline với roster trước.")
            return None
        return classes

    # Gắn với ?tab=: mở thẳng một tab, và chỉ tab đang mở chạy (fragment của tab ẩn không đọc Redis).
    tab_class, tab_teacher, tab_pipeline = st.tabs(list(TABS), key="tab", bind="query-params")

    @st.fragment(run_every=REFRESH)
    def class_view() -> None:
        classes = classes_or_stop()
        if classes is None:
            return
        selected = st.selectbox("Lớp xem chi tiết", classes, key="class_selected")
        started = time.perf_counter()
        try:
            snapshot = queries.read_class_snapshot(client, keys, classes, skills)
            points, trend_problems = queries.read_trend(client, keys, selected, snapshot.as_of_event_ms(selected), skills)
        except redis.RedisError as exc:
            st.error(f"Không đọc được Redis: {type(exc).__name__}: {exc}")
            return
        read_ms = (time.perf_counter() - started) * 1000.0
        log_refresh(snapshot.batch_id(), read_ms)
        st.caption(f"Đọc Redis {read_ms:.1f} ms ({snapshot.commands} lệnh tổng hợp + {queries.TREND_BUCKETS} "
                   f"khối xu hướng) · batch {snapshot.batch_id() if snapshot.batch_id() is not None else '—'} · "
                   f"event time mới nhất {format_ms(snapshot.as_of_event_ms())} · lúc {time.strftime('%H:%M:%S')}")
        st.caption(f"Số đếm < {queries.K_MIN} hiện \"<{queries.K_MIN}\"; số khác làm tròn tới bội số của "
                   f"{queries.K_MIN} (≈); tổng có ô \"<{queries.K_MIN}\" hiện \"{queries.HIDDEN_LABEL}\".")
        show_problems(snapshot.problems + trend_problems)

        heat = pd.DataFrame(queries.heatmap_rows(snapshot))
        heat["kỹ năng"] = heat["skill_id"].map(lambda s: f"{s} — {skill_names.get(s, '')}")
        base = alt.Chart(heat).encode(
            x=alt.X("skill_id:N", title="Kỹ năng", sort=list(skills)),
            y=alt.Y("class_id:N", title="Lớp", sort=classes),
            tooltip=[alt.Tooltip("class_id:N", title="Lớp"), alt.Tooltip("kỹ năng:N"),
                     alt.Tooltip("label:N", title="Tỉ lệ thành thạo")],
        )
        cells = base.mark_rect().encode(color=alt.condition(
            "datum.mastery_rate !== null",
            alt.Color("mastery_rate:Q", title="Tỉ lệ thành thạo", scale=alt.Scale(domain=[0, 1], scheme="greens")),
            alt.value("#e5e5e5")))
        text = base.mark_text(fontSize=10).encode(text="label:N")
        st.subheader("Tỉ lệ thành thạo theo lớp × kỹ năng")
        st.altair_chart((cells + text).properties(height=max(120, 26 * len(classes))), width="stretch")

        left, right = st.columns(2)
        with left:
            st.subheader(f"Lớp {selected} · sĩ số {queries.class_learners_label(snapshot, selected)}")
            st.dataframe(pd.DataFrame(queries.class_detail_rows(snapshot, selected)).rename(columns={
                "skill_id": "kỹ năng", "observed": "có dữ liệu", "mastered": "thành thạo", "stuck": "bế tắc",
                "mastery_rate": "tỉ lệ thành thạo"}), hide_index=True, width="stretch")
        with right:
            st.subheader(f"Toàn bộ {len(classes)} lớp · sĩ số {queries.school_learners_label(snapshot)}")
            st.dataframe(pd.DataFrame(queries.school_rows(snapshot)).rename(columns={
                "skill_id": "kỹ năng", "observed": "có dữ liệu", "mastered": "thành thạo", "stuck": "bế tắc",
                "mastery_rate": "tỉ lệ thành thạo"}), hide_index=True, width="stretch")

        st.subheader(f"Xu hướng theo khối 5 phút — lớp {selected}")
        trend = pd.DataFrame(queries.trend_rows(points))
        if trend.empty:
            st.info("Chưa có khối xu hướng (trend:…) cho lớp này trong 2 h gần nhất.")
        else:
            trend["khối"] = pd.to_datetime(trend["bucket_ms"], unit="ms", utc=True)
            st.altair_chart(alt.Chart(trend.dropna(subset=["mastery_rate"])).mark_line(point=True).encode(
                x=alt.X("khối:T", title="Khối 5 phút (event time, UTC)"),
                y=alt.Y("mastery_rate:Q", title="Tỉ lệ thành thạo", scale=alt.Scale(domain=[0, 1])),
                color=alt.Color("skill_id:N", title="Kỹ năng"),
                tooltip=["skill_id:N", "mastery_label:N", "stuck:N"]), width="stretch")
            stuck = trend.pivot(index="khối", columns="skill_id", values="stuck").sort_index(ascending=False)
            st.caption("Số người bế tắc theo khối (cùng ngưỡng k)")
            st.dataframe(stuck, width="stretch")

    @st.fragment(run_every=REFRESH)
    def teacher_view() -> None:
        classes = classes_or_stop()
        if classes is None:
            return
        selected = st.selectbox("Lớp", classes, key="teacher_class")
        try:
            stuck, problems = queries.read_stuck_learners(client, keys, selected)
            alerts, alert_problems = queries.read_recent_alerts(client, keys)
        except redis.RedisError as exc:
            st.error(f"Không đọc được Redis: {type(exc).__name__}: {exc}")
            return
        show_problems(problems + alert_problems)
        st.subheader(f"Đang bế tắc — lớp {selected} ({len(stuck)})")
        if stuck:
            st.dataframe(pd.DataFrame([{
                "learner_id": s.learner_id, "kỹ năng": s.skill_id, "bế tắc từ": format_ms(s.onset_event_ms),
                "thời lượng": format_duration(s.stuck_for_ms), "sai liên tiếp": s.consec_wrong,
                "số cơ hội": s.opportunities, "hoạt động cuối": format_ms(s.last_event_time_ms),
                "gợi ý gần nhất": s.last_hint or "—"} for s in stuck]), hide_index=True, width="stretch")
        else:
            st.info("Không có người học nào đang bế tắc trong lớp này.")
        st.subheader(f"{queries.RECENT_ALERTS} cảnh báo mới nhất (mọi lớp, khử trùng theo alert_id)")
        if alerts:
            st.dataframe(pd.DataFrame([{
                "hiện lúc": format_ms(a.visible_ms), "loại": a.alert_type, "lớp": a.class_id,
                "learner_id": a.learner_id, "kỹ năng": a.skill_id, "event time": format_ms(a.alert_event_time_ms),
                "alert_id": a.alert_id} for a in alerts]), hide_index=True, width="stretch")
        else:
            st.info("alerts:stream chưa có cảnh báo.")

    @st.fragment(run_every=REFRESH)
    def pipeline_view() -> None:
        try:
            metrics = queries.read_pipeline_metrics(client, keys)
        except (redis.RedisError, queries.DashboardDataError) as exc:
            st.error(f"Không đọc được metrics:pipeline: {type(exc).__name__}: {exc}")
            return
        if metrics is None:
            st.info("Chưa có metrics:pipeline — job streaming chưa chạy batch nào với tiền tố này.")
            return
        now_ms = int(time.time() * 1000)
        if metrics.updated_ms is not None and now_ms - metrics.updated_ms > STALE_METRICS_S * 1000:
            st.warning(f"Số liệu cũ hơn {STALE_METRICS_S} s — job có thể đã dừng.")
        st.dataframe(pd.DataFrame(pipeline_table(metrics, now_ms)), hide_index=True, width="stretch")

    def teacher_gate() -> None:
        if not settings.teacher_available:
            st.info("Chế độ giáo viên đang tắt (cần NHOM8_TEACHER_MODE=1 và TEACHER_PASSWORD).")
            return
        if not st.session_state.get("teacher_ok"):
            given = st.text_input("Mật khẩu giáo viên", type="password", key="teacher_password_input")
            if given:
                if password_matches(given, settings):
                    st.session_state["teacher_ok"] = True
                    st.rerun()
                else:
                    st.error("Sai mật khẩu.")
            return
        if st.button("Khóa chế độ giáo viên"):
            st.session_state["teacher_ok"] = False
            st.rerun()
        teacher_view()

    with tab_class:
        if tab_class.open:
            class_view()
    with tab_teacher:
        if tab_teacher.open:
            teacher_gate()
    with tab_pipeline:
        if tab_pipeline.open:
            pipeline_view()


if __name__ == "__main__":
    main()
