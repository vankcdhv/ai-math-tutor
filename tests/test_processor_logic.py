"""Logic thuần của processor streaming: ba chế độ áp sự kiện, cảnh báo, liệt kê sự kiện đã áp, timer.

Mọi kịch bản chạy `apply_events` qua `MemoryLearnerStore` (cùng giao kèo đọc/ghi với state Spark),
mỗi lời gọi `run` là một micro-batch; watermark truyền tay như Spark đưa vào processor.
"""

from __future__ import annotations

import hashlib
import json
import math
import random
from pathlib import Path

import pandas as pd
import pytest

from ai_math_tutor import learner_rules
from ai_math_tutor.bkt import BktParams
from ai_math_tutor.config import REPO_ROOT
from ai_math_tutor.streaming import processor as P
from ai_math_tutor.streaming.processor import (
    Event,
    LearnerMeta,
    MemoryLearnerStore,
    ProblemState,
    ProcessorError,
    SkillState,
    alert_id,
    apply_events,
    bkt_params_for,
    plan_timers,
)
from ai_math_tutor.streaming.schema import OUTPUT_COLUMNS

SAMPLE = REPO_ROOT / "data_sample" / "events-week1-sample.jsonl"
LEARNER = "L-00000000000000aa"
SKILL = "linear_equation.isolate_variable"
SKILL_B = "linear_equation.distribute"
T0 = 1_800_000_000_000


def ev(n: int, event_type: str = "submit_step", t_s: float | None = None, *, correct: bool | None = None,
       skills: tuple[str, ...] = (SKILL,), problem: str = "P-LIN-001", step: int | None = None,
       seq: int | None = None, sent_delay_s: float = 0.2) -> Event:
    t_ms = T0 + round((n if t_s is None else t_s) * 1000)
    if event_type == "submit_step" and correct is None:
        correct = True
    if event_type == "submit_step" and step is None:
        step = 1
    return Event(f"00000000-0000-0000-0000-{n:012x}", event_type, t_ms, t_ms + round(sent_delay_s * 1000),
                 seq if seq is not None else n, "C-10A1-2026", problem, skills, correct, step)


def wrong(n: int, **kw) -> Event:
    return ev(n, "submit_step", correct=False, **kw)


def right(n: int, **kw) -> Event:
    return ev(n, "submit_step", correct=True, **kw)


def updates(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["kind"] == "update"]


def alerts(rows: list[dict]) -> list[dict]:
    return [r for r in rows if r["kind"] == "alert"]


def applied_ids(rows: list[dict]) -> list[str]:
    return [i for r in updates(rows) for i in r["applied_event_ids"]]


def oracle(events: list[Event]) -> dict[str, learner_rules.SkillProfile]:
    """Tham chiếu độc lập: khử trùng event_id, sắp (event_time, session_seq, event_id), áp learner_rules."""
    unique = {e.event_id: e for e in events}
    profiles: dict[str, learner_rules.SkillProfile] = {}
    for e in sorted(unique.values(), key=lambda x: (x.event_time_ms, x.session_seq, x.event_id)):
        profiles = learner_rules.apply_event(profiles, {"event_type": e.event_type, "skill_ids": e.skill_ids,
                                                        "payload": {"is_correct": e.is_correct}})
    return profiles


def profiles_of(store: MemoryLearnerStore) -> dict[str, learner_rules.SkillProfile]:
    return {sid: s.profile for sid, s in store.skills.items()}


# ---------------------------------------------------------------- eager

def test_eager_in_order_matches_learner_rules_oracle():
    rng = random.Random(7)
    events = [ev(i, rng.choice(["submit_step", "submit_step", "wrong_answer", "complete", "request_hint",
                                "open_problem"]), correct=rng.random() < 0.5, skills=rng.choice([(SKILL,), (SKILL_B, SKILL)]))
              for i in range(1, 80)]
    store = MemoryLearnerStore(LEARNER)
    for start in range(0, len(events), 7):  # micro-batch 7 sự kiện, đúng thứ tự
        rows = store.run(events[start:start + 7], "eager", "rule", watermark_ms=0)
        assert not any(flag for r in updates(rows) for flag in r["applied_out_of_order"])
    assert profiles_of(store) == oracle(events)


def test_every_applied_event_listed_once_on_first_skill_row():
    batch = [
        ev(1, "open_problem", skills=(SKILL_B, SKILL), problem="P-LIN-002"),
        ev(2, "request_hint", skills=(SKILL_B, SKILL), problem="P-LIN-002"),
        wrong(3, skills=(SKILL_B, SKILL), problem="P-LIN-002"),
        ev(4, "wrong_answer", skills=(SKILL,), problem="P-LIN-001"),
        ev(5, "complete", skills=(SKILL,), problem="P-LIN-001"),
    ]
    rows = MemoryLearnerStore(LEARNER).run(batch, "eager", "rule", watermark_ms=0)
    by_skill = {r["skill_id"]: r for r in updates(rows)}
    assert set(by_skill) == {SKILL, SKILL_B}
    # Mỗi sự kiện (kể cả open_problem, request_hint) đúng một lần, trên dòng của skill_ids[0].
    assert by_skill[SKILL_B]["applied_event_ids"] == [batch[0].event_id, batch[1].event_id, batch[2].event_id]
    assert by_skill[SKILL]["applied_event_ids"] == [batch[3].event_id, batch[4].event_id]
    assert sorted(applied_ids(rows)) == sorted(e.event_id for e in batch)
    assert by_skill[SKILL_B]["applied_event_time_ms"] == [e.event_time_ms for e in batch[:3]]
    assert by_skill[SKILL_B]["applied_sent_time_ms"] == [e.sent_time_ms for e in batch[:3]]
    assert by_skill[SKILL_B]["applied_out_of_order"] == [False, False, False]
    # Dòng update của một sự kiện chỉ open_problem vẫn được phát (trạng thái kỹ năng chưa có cơ hội nào).
    only_open = MemoryLearnerStore(LEARNER).run([ev(9, "open_problem")], "eager", "rule", watermark_ms=0)
    assert [(r["skill_id"], r["opportunities"], r["applied_event_ids"]) for r in updates(only_open)] == [
        (SKILL, 0, [ev(9, "open_problem").event_id])]
    for row in rows:
        assert list(row) == list(OUTPUT_COLUMNS)


def test_eager_out_of_order_event_is_applied_and_flagged():
    store = MemoryLearnerStore(LEARNER)
    store.run([right(1), right(2), wrong(4)], "eager", "rule", watermark_ms=0)
    late = right(3, sent_delay_s=95)
    rows = store.run([late, right(5)], "eager", "rule", watermark_ms=T0)
    row = updates(rows)[0]
    assert row["applied_event_ids"] == [late.event_id, right(5).event_id]
    assert row["applied_out_of_order"] == [True, False]
    # Áp sai thứ tự: chuỗi đúng thành 2 (wrong(4) rồi right(3), right(5)) thay vì đạt 3 đúng liên tiếp.
    assert store.skills[SKILL].profile.streak == 2 and not store.skills[SKILL].profile.mastered
    assert oracle([right(1), right(2), right(3), wrong(4), right(5)])[SKILL].mastered


def test_state_version_increases_once_per_event_touching_the_record():
    store = MemoryLearnerStore(LEARNER)
    rows = store.run([ev(1, "open_problem"), right(2), right(3)], "eager", "rule", watermark_ms=0)
    assert updates(rows)[0]["state_version"] == 3
    progress = [r for r in rows if r["kind"] == "progress"]
    assert [(p["problem_id"], p["state_version"], p["last_step"]) for p in progress] == [("P-LIN-001", 3, 1)]
    rows = store.run([ev(4, "wrong_answer"), ev(5, "complete")], "eager", "rule", watermark_ms=0)
    assert updates(rows)[0]["state_version"] == 5
    (p,) = [r for r in rows if r["kind"] == "progress"]
    assert (p["wrong_answers"], p["completed"], p["state_version"]) == (1, True, 5)


# ---------------------------------------------------------------- cảnh báo

def test_stuck_alert_once_per_episode_then_mastery_ends_it():
    store = MemoryLearnerStore(LEARNER)
    wrongs = [wrong(i) for i in range(1, 11)]
    rows = store.run(wrongs[:5], "eager", "rule", watermark_ms=0) + store.run(wrongs[5:], "eager", "rule",
                                                                             watermark_ms=0)
    stuck = [a for a in alerts(rows) if a["alert_type"] == "stuck"]
    assert len(stuck) == 1
    assert stuck[0]["stuck_onset_event_id"] == wrongs[9].event_id
    assert stuck[0]["alert_event_time_ms"] == wrongs[9].event_time_ms
    assert stuck[0]["alert_id"] == hashlib.sha256(
        f"{LEARNER}|{SKILL}|stuck|{wrongs[9].event_id}".encode()).hexdigest()[:24]
    assert store.skills[SKILL].stuck and store.skills[SKILL].stuck_onset_event_id == wrongs[9].event_id
    # Một cảnh báo consec_wrong cho chuỗi sai liên tiếp (tại lần sai thứ 3), không lặp ở lần 4..10.
    assert [a["stuck_onset_event_id"] for a in alerts(rows) if a["alert_type"] == "consec_wrong"] == [
        wrongs[2].event_id]
    # Thêm sai sau khi đã bế tắc không phát cảnh báo bế tắc mới; 3 đúng liên tiếp kết thúc episode.
    more = store.run([wrong(11), right(12), right(13), right(14), wrong(15), wrong(16)], "eager", "rule",
                     watermark_ms=0)
    assert not [a for a in alerts(more) if a["alert_type"] == "stuck"]
    final = store.skills[SKILL]
    assert final.profile.mastered and not final.stuck
    assert final.stuck_onset_event_id == wrongs[9].event_id  # giữ mốc episode đã qua
    last_row = updates(more)[-1]
    assert last_row["stuck"] is False and last_row["mastered"] is True


def test_new_consec_wrong_run_gets_new_alert_id():
    store = MemoryLearnerStore(LEARNER)
    rows = store.run([wrong(1), wrong(2), wrong(3), right(4), wrong(5), wrong(6), wrong(7)], "eager", "rule",
                     watermark_ms=0)
    cw = [a for a in alerts(rows) if a["alert_type"] == "consec_wrong"]
    assert [a["stuck_onset_event_id"] for a in cw] == [wrong(3).event_id, wrong(7).event_id]
    assert len({a["alert_id"] for a in cw}) == 2


def test_alert_ids_are_deterministic_across_reruns_and_batching():
    events = [wrong(i) for i in range(1, 13)]
    first = MemoryLearnerStore(LEARNER).run(events, "eager", "rule", watermark_ms=0)
    store = MemoryLearnerStore(LEARNER)
    second = []
    for e in events:
        second += store.run([e], "eager", "rule", watermark_ms=0)
    ids = lambda rows: sorted((a["alert_type"], a["alert_id"]) for a in alerts(rows))  # noqa: E731
    assert ids(first) == ids(second) and len(ids(first)) == 2
    assert alert_id(LEARNER, SKILL, "stuck", events[9].event_id) in {a for _, a in ids(first)}


def test_multi_skill_event_alerts_each_skill():
    store = MemoryLearnerStore(LEARNER)
    rows = store.run([wrong(i, skills=(SKILL_B, SKILL)) for i in range(1, 4)], "eager", "rule", watermark_ms=0)
    assert sorted(a["skill_id"] for a in alerts(rows)) == [SKILL_B, SKILL]


def test_inactivity_alert_fires_when_watermark_passes_and_problem_unfinished():
    inactivity = 300_000
    store = MemoryLearnerStore(LEARNER)
    first = ev(1, "open_problem")
    store.run([first, right(2)], "eager", "rule", watermark_ms=0, inactivity_ms=inactivity)
    due = right(2).event_time_ms + inactivity
    expire = right(2).event_time_ms + P.DEFAULT_STATE_TTL_MS
    assert store.timers == {due, expire}
    assert not alerts(store.run([], "eager", "rule", watermark_ms=due - 1, inactivity_ms=inactivity))
    rows = store.run([], "eager", "rule", watermark_ms=due, inactivity_ms=inactivity)
    (alert,) = alerts(rows)
    assert (alert["alert_type"], alert["stuck_onset_event_id"], alert["alert_event_time_ms"]) == (
        "inactivity", right(2).event_id, due)
    assert alert["alert_id"] == alert_id(LEARNER, SKILL, "inactivity", right(2).event_id)
    assert store.timers == {expire} and store.meta.inactivity_due_ms is None
    # Không bắn lại; sự kiện mới hẹn lại timer; bài đã hoàn thành thì không cảnh báo.
    assert not alerts(store.run([], "eager", "rule", watermark_ms=due + 10_000, inactivity_ms=inactivity))
    done = ev(1000, "complete")
    store.run([done], "eager", "rule", watermark_ms=due + 10_000, inactivity_ms=inactivity)
    assert store.timers == {done.event_time_ms + inactivity, done.event_time_ms + P.DEFAULT_STATE_TTL_MS}
    assert not alerts(store.run([], "eager", "rule", watermark_ms=done.event_time_ms + inactivity,
                                inactivity_ms=inactivity))


def test_late_older_event_does_not_rearm_inactivity():
    store = MemoryLearnerStore(LEARNER)
    store.run([right(10)], "eager", "rule", watermark_ms=0, inactivity_ms=60_000)
    before = set(store.timers)
    store.run([right(5)], "eager", "rule", watermark_ms=0, inactivity_ms=60_000)
    assert store.timers == before and store.meta.last_seen_event_id == right(10).event_id


def test_state_expires_after_ttl_in_event_time_and_restarts_fresh():
    ttl = 3_600_000
    store = MemoryLearnerStore(LEARNER)
    store.run([right(1), right(2), wrong(3)], "eager", "rule", watermark_ms=0, expire_ms=ttl)
    expire = wrong(3).event_time_ms + ttl
    assert expire in store.timers and store.skills
    store.run([], "eager", "rule", watermark_ms=expire - 1, expire_ms=ttl)
    assert store.skills  # chưa tới hạn
    rows = store.run([], "eager", "rule", watermark_ms=expire, expire_ms=ttl)
    assert updates(rows) == []
    assert store.skills == {} and store.problems == {} and store.meta == LearnerMeta() and store.timers == set()
    # Sự kiện mới sau khi hết hạn bắt đầu hồ sơ mới.
    late_return = right(10_000)
    rows = store.run([late_return], "eager", "rule", watermark_ms=expire, expire_ms=ttl)
    assert updates(rows)[0]["opportunities"] == 1 and updates(rows)[0]["state_version"] == 1


def test_new_event_postpones_expiry_and_ordered_buffer_blocks_it():
    ttl = 60_000
    store = MemoryLearnerStore(LEARNER)
    store.run([right(1)], "eager", "rule", watermark_ms=0, expire_ms=ttl)
    store.run([right(50)], "eager", "rule", watermark_ms=0, expire_ms=ttl)
    assert store.meta.expire_due_ms == right(50).event_time_ms + ttl
    assert right(1).event_time_ms + ttl not in store.timers
    # ordered: hết hạn chỉ khi đệm rỗng (mọi sự kiện ≤ watermark đã áp) — luôn đúng vì hạn > sự kiện mới nhất.
    ordered = MemoryLearnerStore(LEARNER)
    ordered.run([right(1)], "ordered", "rule", watermark_ms=0, expire_ms=ttl)
    rows = ordered.run([], "ordered", "rule", watermark_ms=right(1).event_time_ms + ttl, expire_ms=ttl)
    assert applied_ids(rows) == [right(1).event_id]  # xả đệm và phát dòng update trước khi xóa state
    assert ordered.skills == {} and ordered.buffer == []


def test_none_mode_has_no_timers_and_prunes_seen_ids_by_event_time():
    assert P.time_mode_for("eager") == P.time_mode_for("ordered") == "EventTime"
    assert P.time_mode_for("none") == "None"
    ttl = 24_000  # tỉa mỗi ttl/24 = 1 s event time
    store = MemoryLearnerStore(LEARNER)
    store.run([right(1), right(2)], "none", "rule", watermark_ms=10**15, expire_ms=ttl)
    assert store.timers == set() and store.meta.expire_due_ms is None and store.skills  # không hết hạn
    assert set(store.seen) == {right(1).event_id, right(2).event_id}
    store.run([right(30)], "none", "rule", expire_ms=ttl)  # mới nhất 30 s ⇒ giữ id có event_time ≥ 6 s
    assert set(store.seen) == {right(30).event_id}
    # Bản trùng trong cửa sổ TTL vẫn bị bỏ; sau khi tỉa, id quá cũ không còn được nhớ.
    assert applied_ids(store.run([right(30)], "none", "rule", expire_ms=ttl)) == []
    assert store.skills[SKILL].profile.opportunities == 3


# ---------------------------------------------------------------- ordered

def test_ordered_buffers_until_watermark_and_applies_in_event_time_order():
    store = MemoryLearnerStore(LEARNER)
    a, c = right(10), wrong(30)
    rows = store.run([a, c], "ordered", "rule", watermark_ms=0)
    assert updates(rows) == [] and store.buffer == [a, c]
    assert store.timers == {a.event_time_ms, c.event_time_ms + P.DEFAULT_INACTIVITY_MS,
                            c.event_time_ms + P.DEFAULT_STATE_TTL_MS}
    b = right(20, sent_delay_s=40)  # đến muộn nhưng trong ngưỡng watermark
    rows = store.run([b], "ordered", "rule", watermark_ms=T0 + 25_000)
    assert applied_ids(rows) == [a.event_id, b.event_id]
    assert updates(rows)[0]["applied_out_of_order"] == [False, False]
    assert store.buffer == [c] and c.event_time_ms in store.timers and a.event_time_ms not in store.timers
    # Lần gọi do timer (không có dữ liệu): watermark vượt c ⇒ áp c, đệm rỗng.
    rows = store.run([], "ordered", "rule", watermark_ms=T0 + 30_000)
    assert applied_ids(rows) == [c.event_id] and store.buffer == []
    assert store.meta.buffer_due_ms is None and store.meta.buffer_size == 0
    assert profiles_of(store) == oracle([a, b, c])


def test_ordered_is_exact_when_all_events_arrive_within_watermark():
    rng = random.Random(3)
    truth = [ev(i, rng.choice(["submit_step", "wrong_answer", "complete", "request_hint"]),
                correct=rng.random() < 0.55, skills=rng.choice([(SKILL,), (SKILL_B,), (SKILL, SKILL_B)]))
             for i in range(1, 200)]
    watermark_s = 30
    # Đến theo sent_time = event_time + trễ ngẫu nhiên < W ⇒ đảo thứ tự trong cửa sổ W.
    arrivals = sorted(truth, key=lambda e: e.event_time_ms + rng.uniform(0, watermark_s - 1) * 1000)
    store = MemoryLearnerStore(LEARNER)
    max_seen = None
    applied: list[str] = []
    for start in range(0, len(arrivals), 9):
        batch = arrivals[start:start + 9]
        wm = None if max_seen is None else max_seen - watermark_s * 1000
        assert wm is None or all(e.event_time_ms > wm for e in batch)  # không sự kiện nào bị loại
        rows = store.run(batch, "ordered", "rule", watermark_ms=wm if wm is not None else 0)
        applied += applied_ids(rows)
        assert not any(f for r in updates(rows) for f in r["applied_out_of_order"])
        max_seen = max([max_seen or 0] + [e.event_time_ms for e in batch])
    applied += applied_ids(store.run([], "ordered", "rule", watermark_ms=max_seen + 1))  # drain
    assert sorted(applied) == sorted(e.event_id for e in truth) and len(applied) == len(set(applied))
    assert profiles_of(store) == oracle(truth)


def test_ordered_event_at_or_below_watermark_is_applied_immediately():
    """Sự kiện ≤ watermark tới processor (dedup tắt) không bị đệm vô hạn: áp ngay, gắn cờ nếu sai thứ tự."""
    store = MemoryLearnerStore(LEARNER)
    store.run([right(10), right(20)], "ordered", "rule", watermark_ms=T0 + 25_000)
    rows = store.run([wrong(15)], "ordered", "rule", watermark_ms=T0 + 25_000)
    assert updates(rows)[0]["applied_out_of_order"] == [True] and store.buffer == []


# ---------------------------------------------------------------- none

def test_none_mode_dedups_in_state_and_ignores_watermark_and_timers():
    store = MemoryLearnerStore(LEARNER)
    a, b = right(1), right(2)
    rows = store.run([a, a, b], "none", "rule", watermark_ms=10**15)
    assert applied_ids(rows) == [a.event_id, b.event_id]
    rows = store.run([b, right(3)], "none", "rule", watermark_ms=10**15)
    assert applied_ids(rows) == [right(3).event_id]
    assert set(store.seen) == {a.event_id, b.event_id, right(3).event_id}
    assert store.timers == set() and store.meta.inactivity_due_ms is None and not store.buffer
    assert store.skills[SKILL].profile.opportunities == 3


def test_dedup_off_applies_duplicates_twice():
    store = MemoryLearnerStore(LEARNER)
    rows = store.run([right(1), right(1)], "none", "rule", dedup=False)
    assert applied_ids(rows) == [right(1).event_id] * 2
    assert store.skills[SKILL].profile.opportunities == 2 and store.seen == {}


# ---------------------------------------------------------------- mô hình

def test_bkt_model_uses_tracker_params_and_rule_model_has_no_p_mastery():
    events = [wrong(1), right(2), ev(3, "request_hint"), right(4)]
    params = bkt_params_for(SKILL)
    expected = params.p_init
    for e in events:
        if e.event_type == "submit_step":
            expected = params.update(expected, bool(e.is_correct))
    bkt_rows = MemoryLearnerStore(LEARNER).run(events, "eager", "bkt", watermark_ms=0)
    assert math.isclose(updates(bkt_rows)[0]["p_mastery"], expected, rel_tol=1e-12)
    rule_rows = MemoryLearnerStore(LEARNER).run(events, "eager", "rule", watermark_ms=0)
    assert updates(rule_rows)[0]["p_mastery"] is None
    # Bộ đếm, thành thạo, bế tắc giống nhau giữa hai model (một định nghĩa luật).
    keys = ("opportunities", "correct", "consec_wrong", "streak", "mastered", "stuck")
    assert [r[k] for r in updates(bkt_rows) for k in keys] == [r[k] for r in updates(rule_rows) for k in keys]


def test_bkt_params_for_is_midpoint_of_configured_ranges():
    import yaml

    data = yaml.safe_load((REPO_ROOT / "configs" / "skills.yaml").read_text(encoding="utf-8"))
    mids = {}
    for skill in data["skills"]:
        b = skill["bkt"]
        mids[skill["skill_id"]] = BktParams(*(sum(b[k]) / 2 for k in ("p_init", "p_learn", "p_guess", "p_slip")))
    for skill_id, expected in mids.items():
        got = bkt_params_for(skill_id)
        assert all(math.isclose(getattr(got, k), getattr(expected, k)) for k in ("p_init", "p_learn", "p_guess",
                                                                                  "p_slip"))
    fallback = bkt_params_for("unknown.skill")
    assert math.isclose(fallback.p_init, sum(m.p_init for m in mids.values()) / len(mids))


# ---------------------------------------------------------------- mẫu tuần 1

def _sample() -> list[tuple[str, Event]]:
    """(learner_id, Event) theo thứ tự đến của mẫu tuần 1 (20 dòng, gồm 1 bản trùng)."""
    pairs = []
    for line in SAMPLE.read_text(encoding="utf-8").splitlines():
        raw = json.loads(line)
        payload = raw["payload"]
        pairs.append((raw["learner_id"], Event(
            raw["event_id"], raw["event_type"], _ms(raw["event_time"]), _ms(raw["sent_time"]), raw["session_seq"],
            raw["class_id"], raw["problem_id"], tuple(raw["skill_ids"]), payload.get("is_correct"),
            payload.get("step_index"))))
    return pairs


def _ms(text: str) -> int:
    return int(pd.Timestamp(text).value // 1_000_000)


@pytest.mark.parametrize("mode", ["eager", "ordered", "none"])
def test_week1_sample_applies_19_unique_events(mode):
    pairs = _sample()
    assert len(pairs) == 20
    if mode != "none":  # eager/ordered: bản trùng bị dropDuplicatesWithinWatermark bỏ trước processor
        pairs = list({e.event_id: (learner, e) for learner, e in reversed(pairs)}.values())[::-1]
    applied: list[str] = []
    for learner in sorted({learner for learner, _ in pairs}):
        learner_events = [e for who, e in pairs if who == learner]
        store = MemoryLearnerStore(learner)
        rows = store.run(learner_events, mode, "rule", watermark_ms=0)
        drain_ms = max(e.event_time_ms for e in learner_events) + 1  # drain: ordered xả đệm (chưa hết hạn)
        rows += store.run([], mode, "rule", watermark_ms=drain_ms)
        applied += applied_ids(rows)
        assert not any(f for r in updates(rows) for f in r["applied_out_of_order"])  # một batch, đã sắp
        assert profiles_of(store) == oracle(learner_events)
    assert len(applied) == 19 == len(set(applied))


def test_week1_late_hint_flagged_out_of_order_when_it_arrives_in_a_later_batch():
    pairs = list({e.event_id: (learner, e) for learner, e in reversed(_sample())}.values())[::-1]
    learner_b = "L-bfbe7eab44e1e1cd"
    events = [e for who, e in pairs if who == learner_b]
    late = events[-1]
    assert late.event_type == "request_hint" and late.sent_time_ms - late.event_time_ms == 95_000
    store = MemoryLearnerStore(learner_b)
    store.run(events[:-1], "eager", "rule", watermark_ms=0)
    rows = store.run([late], "eager", "rule", watermark_ms=0)
    assert updates(rows)[0]["applied_event_ids"] == [late.event_id]
    assert updates(rows)[0]["applied_out_of_order"] == [True]
    # Gợi ý không phải cơ hội nên hồ sơ cuối vẫn khớp oracle dù áp sai thứ tự.
    assert profiles_of(store) == oracle(events)


# ---------------------------------------------------------------- timer, tuple, pandas, lỗi

def test_plan_timers_keeps_due_timers_for_spark_and_registers_only_future():
    assert plan_timers(frozenset({100, 200}), frozenset({300}), 150) == ([200], [300])
    assert plan_timers(frozenset({100}), frozenset({100}), 50) == ([], [])
    with pytest.raises(ProcessorError):
        plan_timers(frozenset(), frozenset({100}), 150)


def test_state_tuples_round_trip_and_match_ddl():
    skill = SkillState(learner_rules.SkillProfile(10, 2, 1, 3, 0, False), 0.42, "e1", T0, 7)
    problem = ProblemState(2, 3, True, T0, 9)
    meta = LearnerMeta("C-10A1-2026", T0, T0 + 1, "e9", SKILL, "P-LIN-001", T0 + 300_000, T0 + 5, 2)
    event = right(1)
    for value, ddl in ((skill, P.SKILL_STATE_DDL), (problem, P.PROBLEM_STATE_DDL), (meta, P.META_STATE_DDL),
                       (event, P.EVENT_STATE_DDL)):
        assert type(value).from_tuple(value.to_tuple()) == value
        assert len(value.to_tuple()) == len(ddl.split(", "))
    assert skill.to_tuple()[7] is True  # cột stuck = is_wheel_spinning(profile)
    assert LearnerMeta.from_tuple(None) == LearnerMeta()


def test_pandas_conversion_round_trip_with_nulls():
    frame = pd.DataFrame({
        "learner_id": [LEARNER, LEARNER], "class_id": ["C-10A1-2026"] * 2, "event_id": ["a", "b"],
        "event_type": ["submit_step", "open_problem"], "event_time_ms": [T0, T0 + 1], "sent_time_ms": [T0, T0 + 2],
        "session_seq": [1, 2], "problem_id": ["P-LIN-001"] * 2,
        "skill_ids": [[SKILL], [SKILL, SKILL_B]], "is_correct": [True, None], "step_index": [1.0, float("nan")],
    })
    a, b = P.events_from_pandas(frame)
    assert (a.is_correct, a.step_index, b.is_correct, b.step_index, b.skill_ids) == (True, 1, None, None,
                                                                                     (SKILL, SKILL_B))
    rows = MemoryLearnerStore(LEARNER).run([a, b], "eager", "rule", watermark_ms=0)
    out = P.rows_to_pandas(rows)
    assert list(out.columns) == list(OUTPUT_COLUMNS) and len(out) == len(rows)
    assert out["opportunities"].dtype == object and out["skill_id"].tolist()[:2] == [SKILL, SKILL_B]


def test_invalid_inputs_raise():
    with pytest.raises(ProcessorError):
        apply_events(P.LearnerState(LEARNER), [], "late", "rule")
    with pytest.raises(ProcessorError):
        P.resolve_model("lstm")
    broken = Event("x", "submit_step", T0, T0, 1, "C-10A1-2026", "P-LIN-001", (SKILL,), None, 1)
    with pytest.raises(ProcessorError, match="is_correct"):
        apply_events(P.LearnerState(LEARNER), [broken], "eager", "rule")


def test_processor_rejects_bad_settings():
    with pytest.raises(ProcessorError):
        P.LearnerStateProcessor("late", "rule")
    with pytest.raises(ProcessorError):
        P.LearnerStateProcessor("eager", "rule", state_ttl_ms=0)
    proc = P.LearnerStateProcessor("ordered", "bkt")
    assert proc.model.uses_bkt and proc.timed

