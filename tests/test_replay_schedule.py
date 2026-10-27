"""Bộ phát lại không cần broker: lịch open-loop, chia shard giữ thứ tự, đóng dấu một lần, heartbeat, producer."""

import json
import os
import subprocess
import sys
import zlib
from collections import defaultdict
from pathlib import Path

import numpy as np
import pytest

from ai_math_tutor.event_validation import HEARTBEAT_LEARNER_ID, load_schema, validate_iter
from ai_math_tutor.ingestion.producer import (
    IdempotentProducer,
    ProducerFatalError,
    ProducerSettings,
    ProducerStopped,
)
from ai_math_tutor.ingestion.replay import (
    HEARTBEAT_KEY,
    ReplayRow,
    ReplaySettings,
    SlipHistogram,
    Stamper,
    build_heartbeat,
    heartbeat_event_ms,
    iter_window_rows,
    replay_rows,
    replay_status,
    send_heartbeats,
    shard_of,
    summarize_window,
)
from ai_math_tutor.simulator import SimulationConfig, run_simulation
from ai_math_tutor.simulator.output import iso_from_epoch_ms, iter_rows, restamp

EPOCH0_MS = 1_800_000_000_000


class VirtualClock:
    """Đồng hồ ảo: thời gian chỉ trôi khi bộ phát lại ngủ/poll hoặc khi đích gửi tốn thời gian."""

    def __init__(self) -> None:
        self.t = 100.0

    def monotonic(self) -> float:
        return self.t

    def time_ms(self) -> int:
        return EPOCH0_MS + round(self.t * 1000)

    def sleep(self, seconds: float) -> None:
        self.t += max(seconds, 0.0)


class RecordingSender:
    """Đích gửi trong bộ nhớ: ghi (thời điểm ảo, key, value, partition)."""

    def __init__(self, clock: VirtualClock, send_cost_s: float = 0.0) -> None:
        self.clock = clock
        self.send_cost_s = send_cost_s
        self.sent: list[tuple[float, bytes | None, bytes, int | None]] = []

    def send(self, value, key, partition=None):
        self.clock.t += self.send_cost_s
        self.sent.append((self.clock.t, key, value, partition))
        return 0.0

    def poll(self, timeout_s):
        self.clock.t += max(timeout_s, 0.0)
        return 0


def _value(event_id: str, learner: str = "L-00000000000000aa") -> str:
    return json.dumps({"event_id": event_id, "learner_id": learner, "event_time": "x", "sent_time": "y"})


def _row(offset: int, event_id: str, learner: str = "L-00000000000000aa", event_offset: int | None = None) -> ReplayRow:
    event_offset = offset if event_offset is None else event_offset
    return ReplayRow(offset, event_offset, offset, event_id, learner, _value(event_id, learner))


@pytest.fixture(scope="module")
def sim(tmp_path_factory):
    """Run mô phỏng nhỏ ở chế độ tốc độ, nhiều bản trùng để kiểm đóng dấu một lần."""
    out = tmp_path_factory.mktemp("sim")
    manifest = run_simulation(SimulationConfig(learners=30, horizon_s=120, seed=3, out_dir=out,
                                               target_rate=20.0, duplicate_rate=0.2))
    return out / "events.parquet", manifest


# --- Chia shard ---------------------------------------------------------------------------------

def test_shard_of_is_crc32_and_stable_across_interpreters():
    learners = [f"L-{i:016x}" for i in range(50)]
    assert [shard_of(l, 4) for l in learners] == [zlib.crc32(l.encode()) % 4 for l in learners]
    code = ("import sys; from ai_math_tutor.ingestion.replay import shard_of; "
            "print([shard_of(f'L-{i:016x}', 4) for i in range(50)], hash('L-0'))")
    outputs = []
    for seed in ("1", "2"):
        env = dict(os.environ, PYTHONHASHSEED=seed)
        outputs.append(subprocess.run([sys.executable, "-c", code], env=env, capture_output=True, text=True,
                                      check=True).stdout.split("] "))
    assert outputs[0][0] == outputs[1][0] == str([shard_of(l, 4) for l in learners])[:-1]
    assert outputs[0][1] != outputs[1][1]  # hash() của Python khác giữa tiến trình — vì vậy không dùng
    with pytest.raises(ValueError):
        shard_of("L-0000000000000001", 0)


def test_shards_partition_rows_and_keep_per_learner_file_order(sim):
    events_path, manifest = sim
    all_rows = list(iter_window_rows(events_path))
    assert len(all_rows) == manifest["counts"]["deliveries"]
    expected_order = defaultdict(list)
    for row in all_rows:
        expected_order[row.learner_id].append(row.event_id)
    window = summarize_window(events_path, None, processes=3)
    seen_learners: dict[str, int] = {}
    total = 0
    for shard in range(3):
        rows = list(iter_window_rows(events_path, shard=shard, processes=3, batch_size=97))
        assert len(rows) == window.shard_rows[shard]
        total += len(rows)
        offsets = [r.replay_offset_ms for r in rows]
        assert offsets == sorted(offsets)  # lịch trong mỗi shard không giảm
        order = defaultdict(list)
        for row in rows:
            assert seen_learners.setdefault(row.learner_id, shard) == shard  # một người học → một shard
            order[row.learner_id].append(row.event_id)
        for learner, ids in order.items():
            assert ids == expected_order[learner]
    assert total == len(all_rows)
    assert set(seen_learners) == set(expected_order)


def test_window_cutoff_counts_and_completeness(sim):
    events_path, manifest = sim
    full = summarize_window(events_path, None, processes=1)
    assert full.complete and full.rows == manifest["counts"]["deliveries"]
    assert full.unique_events == manifest["counts"]["unique_events"]
    assert full.duplicate_deliveries == manifest["counts"]["duplicate_deliveries"]
    cut = summarize_window(events_path, 60_000, processes=2)
    rows = list(iter_window_rows(events_path, cutoff_ms=60_000))
    assert not cut.complete and 0 < cut.rows == len(rows) < full.rows
    assert all(r.replay_offset_ms < 60_000 for r in rows)
    assert sum(cut.shard_rows) == cut.rows
    assert cut.unique_events == len({r.event_id for r in rows})
    assert sum(sum(d.values()) for d in cut.shard_duplicates) == cut.duplicate_deliveries
    assert cut.max_event_offset_ms == max(r.event_offset_ms for r in rows)


# --- Đóng dấu một lần ---------------------------------------------------------------------------

def test_stamp_once_per_event_id_and_duplicates_reuse_original_bytes(sim):
    events_path, manifest = sim
    window = summarize_window(events_path, None, processes=1)
    start_ms = 1_900_000_000_123
    stamper = Stamper(start_ms, window.shard_duplicates[0])
    first: dict[str, bytes] = {}
    for row in iter_window_rows(events_path):
        value = stamper.value_for(row)
        if row.event_id in first:
            assert value is first[row.event_id]  # cùng đối tượng byte đã cache, không đóng dấu lại
        else:
            first[row.event_id] = value
            assert value == restamp(row.value, start_ms, row.event_offset_ms, row.sent_offset_ms).encode()
            event = json.loads(value)
            assert event["event_time"] == iso_from_epoch_ms(start_ms + row.event_offset_ms)
            assert event["sent_time"] == iso_from_epoch_ms(start_ms + row.sent_offset_ms)
    counts = manifest["counts"]
    assert stamper.stamped == counts["unique_events"]
    assert stamper.from_cache == counts["duplicate_deliveries"] > 0
    assert stamper.cached == 0  # bản trùng cuối cùng đã gửi ⇒ cache được giải phóng
    assert 0 < stamper.max_cached < counts["duplicate_deliveries"]


def test_duplicate_is_sent_with_original_bytes_even_if_row_value_differs():
    stamper = Stamper(EPOCH0_MS, {"e1": 1})
    original = stamper.value_for(_row(0, "e1"))
    tampered = _row(500, "e1")._replace(value=_value("e1").replace('"x"', '"z"'))
    assert stamper.value_for(tampered) is original
    assert stamper.stamped == 1 and stamper.from_cache == 1


# --- Lịch open-loop ---------------------------------------------------------------------------

def test_rows_are_sent_at_replay_start_plus_offset():
    clock = VirtualClock()
    sender = RecordingSender(clock)
    start_ms = clock.time_ms() + 500
    offsets = [0, 10, 10, 250, 1000, 1000, 2500]
    rows = [_row(o, f"e{i}") for i, o in enumerate(offsets)]
    progress = replay_rows(rows, sender=sender, stamper=Stamper(start_ms, {}), replay_start_ms=start_ms, clock=clock)
    assert progress.scheduled_sent == len(rows) and not progress.stopped_early
    base = 100.0 + 0.5
    for (sent_at, key, _value_bytes, partition), offset in zip(sender.sent, offsets):
        assert sent_at == pytest.approx(base + offset / 1000, abs=0.0011)
        assert key == b"L-00000000000000aa" and partition is None
    assert progress.slip.quantile(0.95) <= 1.1
    assert progress.first_send_ms == pytest.approx(start_ms, abs=2)
    assert progress.last_send_ms == pytest.approx(start_ms + 2500, abs=2)


def test_slow_sender_falls_behind_but_keeps_order_and_records_slip():
    clock = VirtualClock()
    sender = RecordingSender(clock, send_cost_s=0.02)  # 50 tin/s trong khi lịch là 100 tin/s
    start_ms = clock.time_ms()
    rows = [_row(i * 10, f"e{i}") for i in range(100)]
    progress = replay_rows(rows, sender=sender, stamper=Stamper(start_ms, {}), replay_start_ms=start_ms, clock=clock)
    ids = [json.loads(value)["event_id"] for _, _, value, _ in sender.sent]
    assert ids == [f"e{i}" for i in range(100)]  # không bỏ, không đảo
    # Open-loop: không chờ lịch khi đã trễ ⇒ trễ tăng tuyến tính ≈ 10 ms mỗi tin.
    assert progress.slip.max_ms == pytest.approx(99 * 10 + 20, abs=1)
    assert progress.slip.quantile(0.5) == pytest.approx(510, abs=15)


def test_stop_request_interrupts_waiting():
    clock = VirtualClock()
    sender = RecordingSender(clock)
    start_ms = clock.time_ms()
    rows = [_row(0, "a"), _row(60_000, "b")]
    progress = replay_rows(rows, sender=sender, stamper=Stamper(start_ms, {}), replay_start_ms=start_ms,
                           clock=clock, should_stop=lambda: clock.t > 101.0)
    assert progress.stopped_early and progress.scheduled_sent == 1
    assert clock.t < 102.0


# --- Heartbeat --------------------------------------------------------------------------------

def test_heartbeats_reach_every_partition_with_event_time_past_end_plus_w():
    clock = VirtualClock()
    sender = RecordingSender(clock)
    end_ms, watermark_s = EPOCH0_MS + 3_600_000, 120.0
    result = send_heartbeats(sender, partitions=3, end_of_log_ms=end_ms, watermark_s=watermark_s,
                             duration_s=3.0, clock=clock)
    assert result.rounds == 3 and result.sent == 9 and result.until_satisfied is None
    assert sorted(p for _, _, _, p in sender.sent) == [0, 0, 0, 1, 1, 1, 2, 2, 2]
    beats = [json.loads(value) for _, _, value, _ in sender.sent]
    assert {key for _, key, _, _ in sender.sent} == {HEARTBEAT_KEY}
    for index, beat in enumerate(beats):
        expected_ms = end_ms + 121_000 + (index // 3) * 1000
        assert beat["event_time"] == beat["sent_time"] == iso_from_epoch_ms(expected_ms)
        assert beat["learner_id"] == HEARTBEAT_LEARNER_ID and beat["payload"] == {}
    assert result.first_event_ms == heartbeat_event_ms(end_ms, watermark_s, 0) == end_ms + 121_000
    # Watermark = max event_time − W > cuối log ngay từ vòng đầu.
    assert result.first_event_ms - watermark_s * 1000 > end_ms
    # Mỗi vòng cách nhau ~1 s.
    rounds_at = sorted({round(t, 3) for t, _, _, _ in sender.sent})
    assert np.allclose(np.diff(rounds_at), 1.0, atol=0.002)
    report = validate_iter(beats)
    assert report.errors == [] and report.heartbeats == 9 and report.unique_events == 0
    assert len({b["event_id"] for b in beats}) == 9


def test_heartbeat_until_hook_and_minimum_one_round():
    clock = VirtualClock()
    calls = {"n": 0}

    def until() -> bool:
        calls["n"] += 1
        return calls["n"] >= 2

    result = send_heartbeats(RecordingSender(clock), partitions=2, end_of_log_ms=EPOCH0_MS, watermark_s=30,
                             duration_s=60, until=until, clock=clock)
    assert result.rounds == 2 and result.until_satisfied is True
    timeout = send_heartbeats(RecordingSender(clock), partitions=2, end_of_log_ms=EPOCH0_MS, watermark_s=30,
                              duration_s=2, until=lambda: False, clock=clock)
    assert timeout.rounds == 2 and timeout.until_satisfied is False
    once = send_heartbeats(RecordingSender(clock), partitions=1, end_of_log_ms=EPOCH0_MS, watermark_s=30,
                           duration_s=0, clock=clock)
    assert once.rounds == 1 and once.sent == 1


def test_build_heartbeat_matches_contract_v0_2():
    beat = json.loads(build_heartbeat(EPOCH0_MS, event_id="00000000-0000-4000-8000-000000000001"))
    assert beat == {
        "schema_version": "0.2", "event_id": "00000000-0000-4000-8000-000000000001", "event_type": "heartbeat",
        "learner_id": "L-0000000000000000", "event_time": iso_from_epoch_ms(EPOCH0_MS),
        "sent_time": iso_from_epoch_ms(EPOCH0_MS), "payload": {},
    }
    assert set(beat) <= set(load_schema()["properties"])


# --- Histogram độ trượt -------------------------------------------------------------------------

def test_slip_histogram_quantiles_merge_and_state_roundtrip():
    rng = np.random.default_rng(1)
    values = np.concatenate([rng.exponential(2.0, 5000), [12_000.0, 30_000.0, 45_000.0]])
    left, right = SlipHistogram(), SlipHistogram()
    for index, value in enumerate(values):
        (left if index % 2 else right).add(float(value))
    left.merge(SlipHistogram.from_state(right.to_state()))
    assert left.count == len(values)
    for q in (0.5, 0.95, 0.99):
        assert left.quantile(q) == pytest.approx(np.quantile(values, q, method="inverted_cdf"), abs=0.11)
    assert left.quantile(1.0) == left.max_ms == 45_000.0
    assert left.summary()["max_ms"] == 45_000.0
    empty = SlipHistogram()
    assert empty.quantile(0.95) is None and empty.summary()["p95_ms"] is None
    empty.add(-3.0)
    assert empty.quantile(0.5) == 0.0


# --- Producer (không broker: đối tượng thay thế confluent_kafka.Producer) -------------------------

class FakeKafkaProducer:
    """Thay producer librdkafka: hàng đợi có sức chứa; poll() giao tin khi 'broker' sẵn sàng."""

    def __init__(self, config, capacity=2, broker_up_after_polls=0):
        self.config = config
        self.capacity = capacity
        self.queue: list[tuple] = []
        self.produced: list[tuple] = []
        self.polls = 0
        self.broker_up_after_polls = broker_up_after_polls

    def produce(self, topic, value=None, key=None, partition=None, on_delivery=None):
        if len(self.queue) >= self.capacity:
            raise BufferError("Local: Queue full")
        self.queue.append((topic, key, value, partition, on_delivery))

    def poll(self, timeout=0):
        self.polls += 1
        if self.polls > self.broker_up_after_polls:
            while self.queue:
                item = self.queue.pop(0)
                self.produced.append(item[:4])
                item[4](None, item)
        return 0

    def flush(self, timeout=0):
        self.poll()
        return len(self.queue)

    def list_topics(self, topic=None, timeout=0):
        raise NotImplementedError


def test_producer_config_is_idempotent_acks_all_and_tunable():
    settings = ProducerSettings("127.0.0.1:9092", linger_ms=20, batch_size=262_144, compression="lz4",
                                queue_max_messages=5000)
    config = settings.to_config()
    assert config["enable.idempotence"] is True and config["acks"] == "all"
    assert config["linger.ms"] == 20 and config["batch.size"] == 262_144
    assert config["compression.type"] == "lz4" and config["queue.buffering.max.messages"] == 5000
    seen = {}
    IdempotentProducer(settings, producer_factory=lambda c: seen.update(c) or FakeKafkaProducer(c))
    assert callable(seen["error_cb"]) and seen["enable.idempotence"] is True
    for bad in (dict(compression="brotli"), dict(linger_ms=-1), dict(batch_size=0), dict(queue_max_messages=True)):
        with pytest.raises(ValueError):
            ProducerSettings("127.0.0.1:9092", **bad)
    with pytest.raises(ValueError):
        ProducerSettings(" ")


def test_buffer_error_applies_backpressure_and_never_drops():
    fake_holder = {}
    ticks = {"t": 0.0}

    def clock():
        ticks["t"] += 0.01
        return ticks["t"]

    def factory(config):
        fake_holder["p"] = FakeKafkaProducer(config, capacity=2, broker_up_after_polls=6)
        return fake_holder["p"]

    producer = IdempotentProducer(ProducerSettings("127.0.0.1:9092"), producer_factory=factory, clock=clock,
                                  backoff_initial_s=0.001, backoff_max_s=0.004)
    blocked = [producer.send("t", f"v{i}".encode(), b"k") for i in range(5)]
    assert producer.flush(1) == 0
    fake = fake_holder["p"]
    assert [item[2] for item in fake.produced] == [f"v{i}".encode() for i in range(5)]  # đủ, đúng thứ tự
    assert producer.stats.buffer_full_events > 0 and sum(blocked) > 0
    assert producer.stats.backpressure_s == pytest.approx(sum(blocked))
    assert producer.stats.produced == producer.stats.delivered == 5 and producer.stats.delivery_errors == 0


def test_stop_during_backpressure_raises_instead_of_spinning():
    stop = {"flag": False}
    producer = IdempotentProducer(
        ProducerSettings("127.0.0.1:9092"),
        producer_factory=lambda c: FakeKafkaProducer(c, capacity=1, broker_up_after_polls=10**9),
        should_stop=lambda: stop["flag"],
    )
    producer.send("t", b"a")
    stop["flag"] = True
    with pytest.raises(ProducerStopped):
        producer.send("t", b"b")


class FakeError:
    def __init__(self, name, fatal=False):
        self._name, self._fatal = name, fatal

    def name(self):
        return self._name

    def fatal(self):
        return self._fatal

    def __str__(self):
        return f"KafkaError{{{self._name}}}"


def test_delivery_errors_are_counted_and_fatal_error_stops_sending():
    holder = {}

    def factory(config):
        holder["config"] = config
        return FakeKafkaProducer(config, capacity=100)

    producer = IdempotentProducer(ProducerSettings("127.0.0.1:9092"), producer_factory=factory)
    producer._on_delivery(FakeError("_MSG_TIMED_OUT"), None)
    producer._on_delivery(None, None)
    holder["config"]["error_cb"](FakeError("_TRANSPORT"))
    stats = producer.stats
    assert stats.delivery_errors == 1 and stats.delivered == 1
    assert stats.error_counts == {"_MSG_TIMED_OUT": 1} and stats.client_errors == {"_TRANSPORT": 1}
    holder["config"]["error_cb"](FakeError("_FATAL", fatal=True))
    with pytest.raises(ProducerFatalError):
        producer.send("t", b"x")


# --- Cấu hình / trạng thái run -------------------------------------------------------------------

def test_replay_settings_validation_and_status_rules():
    producer = ProducerSettings("127.0.0.1:9092")
    settings = ReplaySettings(Path("e.parquet"), "learner-events", producer, processes=4, duration_s=1.5)
    assert settings.cutoff_ms == 1500
    for bad in (dict(processes=0), dict(processes=True), dict(duration_s=0), dict(heartbeat_s=-1)):
        with pytest.raises(ValueError):
            ReplaySettings(Path("e.parquet"), "t", producer, **bad)
    ok = {"stopped_early": False, "delivered_all": True, "rate_ok": False, "heartbeat": {"delivery_errors": 0}}
    assert replay_status(ok, rate_check=False) == "success"
    assert replay_status(ok, rate_check=True) == "failed"
    assert replay_status({**ok, "delivered_all": False}, rate_check=False) == "failed"
    assert replay_status({**ok, "stopped_early": True}, rate_check=False) == "aborted"


def test_iter_rows_of_simulator_and_replay_reader_agree(sim):
    events_path, _ = sim
    reference = [(r["replay_offset_ms"], r["event_id"], r["value"]) for r in iter_rows(events_path)]
    assert [(r.replay_offset_ms, r.event_id, r.value) for r in iter_window_rows(events_path, batch_size=50)] == reference
