"""Giao diện dashboard (Streamlit AppTest) trên fakeredis: 3 tab, learner_id chỉ hiện ở chế độ giáo viên,
chỉ chạy khi nghe loopback; cùng các hàm thuần cấu hình/xác thực/định dạng của app.py.
"""

from __future__ import annotations

import json
import logging
from pathlib import Path

import fakeredis
import pytest
import redis
import streamlit as st
from streamlit import config as st_config
from streamlit.testing.v1 import AppTest

from ai_math_tutor.config import ConfigError
from ai_math_tutor.dashboard import app, queries
from ai_math_tutor.streaming.listener import metrics_fields, progress_record
from test_dashboard_queries import T0, Pipeline, _class_states, _learners, _publish_alert

APP_PATH = Path(app.__file__)
PREFIX = "ui-test:"


# ---------------------------------------------------------------- hàm thuần


def test_settings_from_env_validate_prefix_and_teacher_flags():
    settings = app.load_settings(env={"NHOM8_DASHBOARD_KEY_PREFIX": "demo-1:", "NHOM8_TEACHER_MODE": "1",
                                      "TEACHER_PASSWORD": "s3cret"}, dotenv_path=None)
    assert settings.key_prefix == "demo-1:" and settings.teacher_available
    assert "s3cret" not in repr(settings)
    assert not app.load_settings(env={}, dotenv_path=None).teacher_available
    # Bật cờ nhưng thiếu mật khẩu ⇒ vẫn tắt; có mật khẩu nhưng không bật cờ ⇒ tắt.
    assert not app.load_settings(env={"NHOM8_TEACHER_MODE": "true"}, dotenv_path=None).teacher_available
    assert not app.load_settings(env={"TEACHER_PASSWORD": "x"}, dotenv_path=None).teacher_available
    for bad in ("a b", "x*", "k" * 65, "../"):
        with pytest.raises(ConfigError):
            app.load_settings(env={"NHOM8_DASHBOARD_KEY_PREFIX": bad}, dotenv_path=None)


def test_settings_read_dotenv_and_env_wins(tmp_path):
    dotenv = tmp_path / ".env"
    dotenv.write_text("NHOM8_TEACHER_MODE=1\nTEACHER_PASSWORD=from-file\n", encoding="utf-8")
    assert app.load_settings(env={}, dotenv_path=dotenv).teacher_password == "from-file"
    assert app.load_settings(env={"TEACHER_PASSWORD": "from-env"}, dotenv_path=dotenv).teacher_password == "from-env"


def test_password_check():
    on = app.DashboardSettings(teacher_enabled=True, teacher_password="đúng-mật-khẩu")
    assert app.password_matches("đúng-mật-khẩu", on)
    assert not app.password_matches("sai", on) and not app.password_matches("", on)
    off = app.DashboardSettings(teacher_enabled=False, teacher_password="đúng-mật-khẩu")
    assert not app.password_matches("đúng-mật-khẩu", off)


def test_bind_guard_allows_only_loopback_on_host():
    assert app.bind_is_loopback("127.0.0.1", "host") and app.bind_is_loopback("localhost", "host")
    assert not app.bind_is_loopback("0.0.0.0", "host") and not app.bind_is_loopback(None, "host")
    assert not app.bind_is_loopback("", "host")
    assert app.bind_is_loopback("0.0.0.0", "container")


def test_pipeline_table_keeps_listener_values():
    metrics = queries.PipelineMetrics(run_id="r1", batch_id=7, updated_ms=T0, input_rows_per_s=1003.25,
                                      batch_duration_ms=1500, lag_total=12, p95_system_ms_1m=412.04)
    rows = {r["chỉ số"]: r["giá trị"] for r in app.pipeline_table(metrics, now_ms=T0 + 2_500)}
    assert rows["input rows/s"] == "1,003.25" and rows["batch duration"] == "1,500 ms"
    assert rows["processing lag (tổng offset)"] == "12" and rows["P95 độ trễ hệ thống (1 phút)"] == "412.0 ms"
    assert rows["tuổi số liệu"] == "2.5 s" and rows["watermark"] == "—" and rows["RSS driver + JVM"] == "—"
    assert app.format_duration(3_725_000) == "1h02m05s" and app.format_duration(65_000) == "1m05s"


def test_refresh_log_warns_when_a_refresh_is_slow():
    with pytest.raises(ConfigError):
        app.configure_logging({"NHOM8_DASHBOARD_LOG_LEVEL": "loud"})
    app.configure_logging({"NHOM8_DASHBOARD_LOG_LEVEL": "debug"})
    records: list[logging.LogRecord] = []
    handler = logging.Handler()
    handler.emit = records.append  # type: ignore[method-assign]  # gom bản ghi để kiểm mức log
    app.LOGGER.addHandler(handler)
    try:
        app.log_refresh(12, 35.0)
        app.log_refresh(13, app.SLOW_REFRESH_MS + 1)
    finally:
        app.LOGGER.removeHandler(handler)
        app.configure_logging({})
    assert [(r.levelno, r.getMessage()) for r in records] == [
        (logging.DEBUG, "tab Lớp làm mới: batch=12 đọc Redis 35.0 ms"),
        (logging.WARNING, "tab Lớp làm mới: batch=13 đọc Redis 1001.0 ms")]


# ---------------------------------------------------------------- AppTest


@pytest.fixture
def server(tmp_path, monkeypatch):
    """Redis giả dùng chung giữa test và app; dữ liệu ghi bằng mã pipeline với tiền tố riêng."""
    fake_server = fakeredis.FakeServer()
    client = fakeredis.FakeRedis(server=fake_server)
    pipeline = Pipeline(client, tmp_path, prefix=PREFIX)
    big, small = _learners("C-SIM-0001", 12), _learners("C-SIM-0002", 3)
    pipeline.roster({"C-SIM-0001": big, "C-SIM-0002": small})
    pipeline.write({**_class_states("C-SIM-0001", big, mastered=6, stuck=2),
                    **_class_states("C-SIM-0002", small, mastered=1, stuck=1)})
    _publish_alert(client, pipeline.keys, big[-1], "linear_equation.isolate_variable", "C-SIM-0001",
                   f"e-{big[-1]}", T0)
    client.set(queries.lasthint_key(pipeline.keys, big[-1]), json.dumps({"hint_level": 2, "text_vi": "Xem lại bước 1"}))
    client.hset(pipeline.keys.metrics_pipeline, mapping=metrics_fields(progress_record(
        "ui-run", {"batchId": 3, "inputRowsPerSecond": 250.5, "batchDuration": 800}, latest_offsets=None)))

    monkeypatch.setattr(redis.Redis, "from_url", classmethod(lambda cls, url, **kw: fakeredis.FakeRedis(server=fake_server)))
    for name in ("NHOM8_ENV", "NHOM8_TEACHER_MODE", "TEACHER_PASSWORD"):
        monkeypatch.delenv(name, raising=False)
    monkeypatch.setenv("REDIS_PASSWORD", "unit-test")
    monkeypatch.setenv("NHOM8_DASHBOARD_KEY_PREFIX", PREFIX)
    st.cache_data.clear()
    st.cache_resource.clear()
    st_config.set_option("server.address", "127.0.0.1")
    yield {"learners": big + small, "pipeline": pipeline}
    st.cache_data.clear()
    st.cache_resource.clear()
    st_config.set_option("server.address", None)


def _texts(at: AppTest) -> str:
    """Mọi nội dung đã dựng (protobuf của từng phần tử, gồm cả dữ liệu bảng/biểu đồ)."""
    parts: list[str] = []

    def walk(node) -> None:
        proto = getattr(node, "proto", None)
        if proto is not None:
            parts.append(str(proto))
        for child in getattr(node, "children", {}).values():
            walk(child)

    walk(at._tree)
    for frame in at.dataframe:
        parts.append(frame.value.to_string())
    return "\n".join(parts)


def _run(tab: str | None = None, timeout: int = 60) -> AppTest:
    at = AppTest.from_file(str(APP_PATH), default_timeout=timeout)
    if tab is not None:
        at.query_params["tab"] = tab
    at.run()
    assert not at.exception, at.exception
    return at


def test_class_tab_is_default_and_shows_only_protected_numbers(server):
    at = _run()
    assert [t.label for t in at.tabs] == list(app.TABS)
    rendered = _texts(at)
    assert "<5" in rendered and "≈10" in rendered and "ẩn" in rendered
    assert "250.50" not in rendered  # tab ẩn không chạy (không đọc Redis)
    for learner in server["learners"]:
        assert learner not in rendered


def test_pipeline_and_locked_teacher_tabs_open_from_query_param(server):
    rendered = _texts(_run("Pipeline"))
    assert "250.50" in rendered and "800 ms" in rendered  # bảng Pipeline đọc metrics:pipeline
    assert "≈10" not in rendered
    rendered = _texts(_run("Giáo viên"))
    assert "Chế độ giáo viên đang tắt" in rendered
    for learner in server["learners"]:
        assert learner not in rendered


def test_teacher_mode_needs_flag_and_password_then_shows_stuck_learners(server, monkeypatch):
    monkeypatch.setenv("NHOM8_TEACHER_MODE", "1")
    monkeypatch.setenv("TEACHER_PASSWORD", "mật-khẩu-test")
    at = _run("Giáo viên")
    assert len(at.text_input) == 1
    for learner in server["learners"]:
        assert learner not in _texts(at)
    at.text_input[0].input("sai").run()
    assert any("Sai mật khẩu" in e.value for e in at.error)
    at.text_input[0].input("mật-khẩu-test").run()
    assert not at.exception, at.exception
    rendered = _texts(at)
    stuck = server["learners"][10:12]  # hai người cuối lớp 1 đang bế tắc
    assert all(learner in rendered for learner in stuck)
    assert "hint_level=2" in rendered  # gợi ý gần nhất
    assert "Đang bế tắc — lớp C-SIM-0001 (2)" in rendered


def test_app_refuses_to_serve_on_non_loopback_address(server):
    st_config.set_option("server.address", "0.0.0.0")
    at = _run()
    assert any("127.0.0.1" in e.value for e in at.error)
    assert not at.tabs
