"""Trang Học sinh (Streamlit): đóng vai người học, giải bài từng bước, xin gợi ý đã kiểm chứng.

Mỗi thao tác sinh một sự kiện LearnerEvent v0.2 gửi vào Kafka (topic của run), nên job streaming cập nhật
trạng thái của bút danh này trong Redis, dịch vụ gợi ý đọc trạng thái đó (`state_source = redis`) và dashboard
đếm người học này trong lớp. Bút danh được thêm vào `roster:{class}` của run trước sự kiện đầu tiên.

Chạy trên host, chỉ nghe trên loopback:
    streamlit run src/ai_math_tutor/student/app.py --server.address 127.0.0.1 --server.port 8502

Biến môi trường: xem `ai_math_tutor.student.backends`. Khóa API không bao giờ hiện trên trang.
"""

from __future__ import annotations

import random
import re
import time

from ai_math_tutor.config import REPO_ROOT, ConfigError, load_config
from ai_math_tutor.dashboard.app import bind_is_loopback
from ai_math_tutor.student import backends, practice

SKILLS_PATH = REPO_ROOT / "configs" / "skills.yaml"
KIND_LABELS = {"Tất cả": None, "Phương trình": "equation", "Số học": "arithmetic"}
ALL_SKILLS = "Tất cả kỹ năng"
MATH_TIMEOUT_S = 1.0
_MARKDOWN_SPECIAL = re.compile(r"([\\`*_{}\[\]<>()#+\-.!|~$])")
# Cặp $…$ của LaTeX trong đề NuminaMath: bên trong là một chữ cái đơn hoặc có phép toán. "$5 and $10" (tiền) không khớp.
_INLINE_MATH = re.compile(r"\$([A-Za-z]|[^$\s](?:[^$]*?[=+\-*/^\\(){}])[^$]*?[^$\s]?)\$")


def strip_inline_math(text: str) -> str:
    """Bỏ dấu $ quanh biểu thức LaTeX đơn giản để đề đọc được ("Solve for $x$" → "Solve for x")."""
    return _INLINE_MATH.sub(lambda m: m.group(1), text)


def plain(text: str) -> str:
    """Chữ hiển thị nguyên văn trong markdown của Streamlit (không in nghiêng `2*x*y`, không LaTeX `$`)."""
    return _MARKDOWN_SPECIAL.sub(r"\\\1", strip_inline_math(text))


def now_ms() -> int:
    return time.time_ns() // 1_000_000


# ---------------------------------------------------------------------------------------------
# Giao diện. Chỉ chạy khi Streamlit thực thi tệp (import để test không mở giao diện).
# ---------------------------------------------------------------------------------------------


def main() -> None:  # noqa: C901 — một hàm dựng trang, chia theo khối
    import redis
    import streamlit as st

    from ai_math_tutor.hints.generator import default_problem_bank_path
    from ai_math_tutor.problem_bank import Problem, ProblemBank, ProblemBankError
    from ai_math_tutor.safe_math import SafeMath
    from ai_math_tutor.simulator.population import load_skills_config
    from ai_math_tutor.streaming.sinks import RedisKeys

    st.set_page_config(page_title="Nhóm 8 — Học sinh", layout="centered")
    try:
        cfg = load_config()
        settings = backends.load_settings(cfg.kafka.topics["learner_events"])
        redis_url = cfg.redis.url
    except ConfigError as exc:
        st.error(f"Cấu hình không hợp lệ: {exc}")
        st.stop()
    if not bind_is_loopback(st.get_option("server.address"), cfg.environment):
        st.error("Trang Học sinh chỉ được nghe trên 127.0.0.1: chạy với --server.address 127.0.0.1")
        st.stop()

    @st.cache_resource
    def redis_client(url: str) -> redis.Redis:
        return redis.Redis.from_url(url, socket_timeout=2, socket_connect_timeout=2, health_check_interval=30)

    @st.cache_resource(show_spinner="Đang nạp và kiểm kho bài…")
    def practice_problems() -> tuple[Problem, ...]:
        return practice.verified_problems(ProblemBank.from_jsonl(default_problem_bank_path()))

    @st.cache_resource
    def skill_names() -> dict[str, str]:
        return {s.skill_id: s.name_vi for s in load_skills_config(SKILLS_PATH).skills}

    @st.cache_resource
    def checker() -> SafeMath:
        return SafeMath(timeout_s=MATH_TIMEOUT_S, max_pending=8, workers=1)

    @st.cache_resource
    def publisher(bootstrap: str, topic: str) -> backends.EventPublisher:
        return backends.EventPublisher(bootstrap, topic)

    client = redis_client(redis_url)
    keys = RedisKeys(settings.key_prefix)
    hints = backends.HintClient(settings.hint_url, settings.api_key)
    state = st.session_state
    state.setdefault("outbox", [])
    state.setdefault("flash", [])
    state.setdefault("sent", 0)

    def flash(level: str, text: str) -> None:
        state["flash"].append((level, text))

    def flush_outbox() -> None:
        outbox: list[tuple[bytes, bytes]] = state["outbox"]
        if not outbox:
            return
        with st.spinner("Đang gửi sự kiện vào Kafka…"):
            outcome = publisher(cfg.kafka.bootstrap_servers, settings.events_topic).publish(outbox)
        if outcome.ok:
            state["sent"] += outcome.total
            outbox.clear()
        else:
            flash("warning", f"{outcome.message} Sự kiện được giữ lại và gửi lại nguyên văn ở thao tác sau.")

    def send(events: list[dict]) -> None:
        sent_ms = now_ms()
        state["outbox"].extend(practice.encode_event(event, sent_ms) for event in events)
        flush_outbox()

    def skill_label(skill: str) -> str:
        name = skill_names().get(skill)
        return f"{skill} — {name}" if name else skill

    st.title("Học sinh — giải toán từng bước")
    st.caption(f"Sự kiện → Kafka `{settings.events_topic}` · Redis {cfg.redis.redacted_url} · tiền tố khóa "
               f"`{settings.key_prefix or '(không)'}` · dịch vụ gợi ý {settings.hint_url}")
    for level, text in state["flash"]:
        getattr(st, level)(plain(text))
    state["flash"] = []

    # --- 1. Người học -------------------------------------------------------------------------
    learner: practice.LearnerSession | None = state.get("learner")
    if learner is None:
        st.subheader("1. Người học")
        try:
            classes = backends.list_classes(client, keys)
        except redis.RedisError as exc:
            st.error(f"Không đọc được Redis: {type(exc).__name__}. Kiểm tra Redis đang chạy.")
            st.stop()
        if not classes:
            st.info("Chưa có lớp nào trong roster dưới tiền tố này. Chạy demo (make demo) để nạp roster trước.")
            st.stop()
        class_id = st.selectbox("Lớp", classes, key="class_pick")
        candidate = state.setdefault("candidate", practice.new_learner_id())
        st.markdown(f"Bút danh: `{candidate}` (ngẫu nhiên, không gắn với danh tính nào)")
        left, right = st.columns(2)
        if left.button("Tạo bút danh khác"):
            state["candidate"] = practice.new_learner_id()
            st.rerun()
        if right.button("Bắt đầu", type="primary"):
            try:
                backends.register_learner(client, keys, class_id, candidate)
            except (redis.RedisError, ValueError) as exc:
                st.error(f"Không ghi được bút danh vào roster của lớp: {type(exc).__name__}: {exc}")
                st.stop()
            state["learner"] = practice.LearnerSession.start(class_id, candidate)
            state.pop("candidate", None)
            st.rerun()
        st.stop()

    head, change = st.columns([4, 1])
    head.markdown(f"**Người học** `{learner.learner_id}` · lớp `{learner.class_id}` · phiên `{learner.session_id}`")
    if change.button("Đổi người học"):
        for name in ("learner", "attempt", "hint"):
            state.pop(name, None)
        st.rerun()

    # --- 2. Chọn bài --------------------------------------------------------------------------
    try:
        problems = practice_problems()
    except (ProblemBankError, FileNotFoundError) as exc:
        st.error(f"Không nạp được kho bài: {exc}")
        st.stop()
    attempt: practice.ProblemAttempt | None = state.get("attempt")

    def start(problem: Problem) -> None:
        new_attempt, event = practice.open_problem(learner, problem, now_ms())
        state["attempt"], state["hint"] = new_attempt, None
        send([event])
        st.rerun()

    with st.expander("2. Chọn bài", expanded=attempt is None):
        kind = KIND_LABELS[st.radio("Loại bài", list(KIND_LABELS), horizontal=True, key="kind_pick")]
        by_kind = practice.problems_matching(problems, kind=kind)
        skills = sorted({p.primary_skill for p in by_kind})
        skill = st.selectbox("Kỹ năng", [ALL_SKILLS, *skills], key="skill_pick",
                             format_func=lambda s: s if s == ALL_SKILLS else skill_label(s))
        pool = practice.problems_matching(by_kind, skill=None if skill == ALL_SKILLS else skill)
        st.caption(f"{len(pool)} bài đã qua kiểm SymPy (tập {practice.PRACTICE_SPLIT})")
        by_id = {p.problem_id: p for p in pool}
        chosen = st.selectbox("Bài", list(by_id), key="problem_pick",
                              format_func=lambda pid: f"{pid} — {(by_id[pid].statement_vi or by_id[pid].statement)[:70]}")
        left, right = st.columns(2)
        if left.button("Mở bài đã chọn", disabled=not pool):
            start(by_id[chosen])
        if right.button("Bài ngẫu nhiên", disabled=not pool):
            start(random.choice(pool))

    # --- 3. Làm bài ---------------------------------------------------------------------------
    if attempt is None:
        st.info("Chọn một bài để bắt đầu.")
    else:
        problem = attempt.problem
        st.subheader(f"3. Bài {problem.problem_id}")
        st.markdown(plain(problem.statement_vi or problem.statement))
        if problem.kind == "equation" and problem.equation:
            st.markdown(f"Phương trình: `{problem.display(problem.equation)}` · ẩn `{problem.display_variable}`")
        st.caption(f"Kỹ năng {skill_label(problem.primary_skill)} · độ khó {problem.difficulty} · "
                   f"{attempt.n_steps} bước rồi nộp đáp án")
        for index, step in enumerate(attempt.solved_steps, start=1):
            st.markdown(f"Bước {index} (đã kiểm): `{step}`")

        if attempt.completed:
            st.success(f"Hoàn thành! Đáp án `{attempt.final_answer}` · {attempt.wrong_total} lần sai · "
                       f"{attempt.hints_used} gợi ý. Chọn bài khác ở mục 2.")
        else:
            label = "Đáp án cuối" if attempt.answering else f"Bước {attempt.step} / {attempt.n_steps}"
            example = "4 hoặc {2, 3}" if attempt.answering else "2*x = 8 hoặc 3 * 12 = 36"
            with st.form("step_form", clear_on_submit=True):
                text = st.text_input(label, placeholder=f"ví dụ {example}", max_chars=200)
                submitted = st.form_submit_button("Nộp đáp án" if attempt.answering else "Kiểm tra bước")
            if submitted:
                event_ms = now_ms()
                step_before = attempt.step
                with st.spinner("Đang kiểm bằng SymPy…"):
                    result = practice.check_input(checker(), problem, text, attempt.step)
                event = practice.record_check(learner, attempt, result, event_ms)
                if result.status == practice.CORRECT and attempt.completed:
                    flash("success", "Đúng! Bạn đã giải xong bài.")
                elif result.status == practice.CORRECT:
                    flash("success", f"Đúng! Bước mẫu đã kiểm: {problem.display_steps[step_before - 1]}")
                elif result.status == practice.WRONG and result.contract_correct:
                    flash("warning", f"Phép tính đúng nhưng chưa phải bước {step_before} của lời giải. Thử lại.")
                elif result.status == practice.WRONG:
                    flash("error", "Chưa đúng. Thử lại, hoặc bấm “Xin gợi ý”.")
                else:
                    flash("warning", result.message)
                if event is not None:
                    send([event])
                st.rerun()
            st.caption(f"Sai ở bước này: {attempt.wrong_at_step} · tổng số lần sai: {attempt.wrong_total} · "
                       f"gợi ý đã nhận: {attempt.hints_used}")
            if st.button("Xin gợi ý"):
                envelope = practice.hint_envelope(learner, attempt, now_ms())
                with st.spinner("Đang hỏi dịch vụ gợi ý…"):
                    outcome = hints.request(envelope)
                if outcome.hint is None:
                    flash("error", outcome.message)
                else:
                    state["hint"] = (attempt.step, outcome.hint)
                    send([practice.record_hint(learner, attempt, envelope, outcome.hint.hint_level)])
                st.rerun()

        shown = state.get("hint")
        if shown is not None:
            step, hint = shown
            where = "đáp án cuối" if step > attempt.n_steps else f"bước {step}"
            st.markdown(f"**Gợi ý bậc {hint.hint_level}** cho {where}")
            st.info(plain(hint.text_vi))
            for expression in hint.math:
                st.code(expression, language=None)
            st.caption(f"Đã kiểm chứng: {'có' if hint.verified else 'không'} · nguồn {hint.source} · "
                       f"trạng thái người học đọc từ {hint.state_source}")

    # --- Sự kiện ------------------------------------------------------------------------------
    pending = len(state["outbox"])
    if pending:
        st.warning(f"{pending} sự kiện chưa tới Kafka.")
        if st.button("Gửi lại sự kiện"):
            flush_outbox()
            st.rerun()
    st.caption(f"Đã gửi {state['sent']} sự kiện trong phiên `{learner.session_id}`.")


if __name__ == "__main__":
    main()
