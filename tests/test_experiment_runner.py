"""Runner: bảo vệ Redis (không FLUSHDB DB 0 hay DB có khóa lạ), tín hiệu dừng ở điểm an toàn, lệnh job/bộ phát
lại theo cấu hình, cửa sổ phát lại cho batch, cache dữ liệu mô phỏng, cấu hình job thí nghiệm."""

from __future__ import annotations

import json
import os
import signal
from pathlib import Path

import fakeredis
import pyarrow.parquet as pq
import pytest

from ai_math_tutor.config import REPO_ROOT, load_config
from ai_math_tutor.experiments import matrix as M
from ai_math_tutor.experiments import runner as RUN
from ai_math_tutor.experiments.datasets import cache_valid, ensure_dataset, materialize_window
from ai_math_tutor.experiments.replay_child import watermark_passed
from ai_math_tutor.experiments.stream_job import experiment_env, split_argv
from ai_math_tutor.streaming.sinks import RedisKeys

PLAN = REPO_ROOT / "configs" / "experiments" / "plan-v1.0.yaml"


def test_redis_reset_refuses_db0_and_foreign_keys_and_never_touches_other_prefixes():
    client = fakeredis.FakeRedis()
    with pytest.raises(RUN.RunnerError, match="DB 0"):
        RUN.reset_redis(client, 0, "")
    keys = RedisKeys("")
    client.hset(keys.state("L-1"), "skill", "{}")
    client.sadd(keys.roster("C-1"), "L-1")
    client.xadd(keys.alerts_stream, {"a": "1"})
    client.hset(keys.metrics_pipeline, "batch_id", "1")
    assert RUN.foreign_keys(client) == []
    client.set("session:user-42", "x")  # khóa của ứng dụng khác
    with pytest.raises(RUN.RunnerError, match="không thuộc pipeline"):
        RUN.reset_redis(client, 9, "")
    assert client.dbsize() == 5
    client.delete("session:user-42")
    assert RUN.reset_redis(client, 9, "") == {"mode": "flushdb", "db": 9, "keys_before": 4}
    assert client.dbsize() == 0
    client.set("smoke-1:state:L-1", "x")
    client.set("other:state:L-1", "y")
    assert RUN.reset_redis(client, 0, "smoke-1:")["deleted"] == 1
    assert client.exists("other:state:L-1")


def test_every_redis_key_of_the_pipeline_is_recognized():
    keys = RedisKeys("")
    built = [keys.state("L"), keys.progress("L", "P"), keys.roster("C"), keys.agg("C", "S"), keys.stuck_set("C"),
             keys.alert("A"), keys.alerts_stream, keys.trend("C", 0), keys.metrics_pipeline]
    if hasattr(keys, "hintctx"):  # khóa của dịch vụ gợi ý dựng qua cùng lớp
        built += [keys.hintctx("L", "P", 1), keys.lasthint("L")]
    for key in built:
        assert key.startswith(RUN.PIPELINE_KEY_PREFIXES), key


def test_signal_guard_defers_inside_cleanup_and_raises_once():
    guard = RUN.SignalGuard()
    guard.check()  # chưa có tín hiệu
    with guard.deferred():
        guard._handle(signal.SIGTERM, None)
        guard.check()  # trong khối dọn dẹp: không ném
    with pytest.raises(RUN.RunnerInterrupted):
        guard.check()
    guard.check()  # đã ném một lần
    forced = RUN.SignalGuard()
    forced._handle(signal.SIGTERM, None)
    with pytest.raises(RUN.RunnerInterrupted):
        forced._handle(signal.SIGINT, None)  # tín hiệu thứ hai ngoài khối dọn dẹp: thoát ngay


@pytest.fixture
def runner(tmp_path):
    cfg = load_config()
    plan = M.load_plan(PLAN)
    opts = RUN.RunnerOptions(plan_path=PLAN, results_dir=tmp_path / "results", data_root=tmp_path / "data")
    return RUN.Runner(opts, plan, cfg)


def _ctx(runner, spec):
    work = runner.opts.work_root / spec.slug
    work.mkdir(parents=True)
    return RUN.RunContext(spec=spec, attempt=1, run_id="r", work=work, out=work, timeout_s=900)


def _argument(command: list[str], flag: str) -> str:
    return command[command.index(flag) + 1]


def test_job_and_replay_commands_follow_the_config(runner, tmp_path):
    spec = next(s for s in runner.plan.specs if s.kind == "stream" and s.params["mode"] == "ordered"
                and s.params["watermark_s"] == 600)
    topics = {"events": "exp-r", "dead": "exp-r-dl", "alerts": "exp-r-al"}
    job, replay = runner.stream_commands(_ctx(runner, spec), tmp_path, topics)
    rest = job[job.index("--") + 1:]
    assert _argument(job, "--redis-db") == "9"
    assert _argument(rest, "--mode") == "ordered" and _argument(rest, "--watermark") == "600s"
    assert _argument(rest, "--shuffle-partitions") == str(spec.params["kafka_partitions"])
    assert _argument(rest, "--input-topic") == "exp-r" and _argument(rest, "--alerts-topic") == "exp-r-al"
    assert "--redis-key-prefix" not in rest
    assert _argument(replay, "--watermark-s") == "600" and float(_argument(replay, "--heartbeat-s")) >= 1200
    assert _argument(replay, "--duration-s") == "600" and "--rate-check" in replay

    none = next(s for s in runner.plan.specs if s.kind == "stream" and s.params["mode"] == "none")
    _, replay = runner.stream_commands(_ctx(runner, none), tmp_path / "x", topics)
    assert _argument(replay, "--heartbeat-s") == "0"  # không watermark ⇒ không heartbeat

    nodedup = next(s for s in runner.plan.specs if s.kind == "stream" and not s.params["dedup"])
    trigger = next(s for s in runner.plan.specs if s.kind == "stream" and s.params["trigger_s"] == 60)
    assert _argument(runner.stream_commands(_ctx(runner, nodedup), tmp_path / "y", topics)[0], "--dedup") == "off"
    assert _argument(runner.stream_commands(_ctx(runner, trigger), tmp_path / "z", topics)[0], "--trigger") == "60 seconds"


def test_new_context_writes_marker_and_context(runner):
    spec = runner.plan.specs[0]
    ctx = runner.new_context(spec, 2)
    context = json.loads((ctx.work / RUN.CONTEXT_FILE).read_text(encoding="utf-8"))
    assert context["run_id"] == ctx.run_id and context["params"]["attempt"] == 2
    assert context["params"]["config_key"] == spec.slug and ctx.run_id.startswith(spec.run_prefix)
    entries = (runner.opts.runner_dir / "pids" / f"{ctx.run_id}.jsonl").read_text(encoding="utf-8")
    assert json.loads(entries)["role"] == "runner"
    second = runner.new_context(spec, 3)  # cùng giây ⇒ run_id mới, không ghi đè thư mục cũ
    assert second.run_id != ctx.run_id


def test_stream_job_config_override_and_argv_split(tmp_path):
    env: dict[str, str] = {}
    experiment_env(env, redis_db=9, work_dir=tmp_path)
    cfg = load_config(env=env)
    assert cfg.redis.db == 9 and cfg.paths.results == tmp_path
    assert load_config(env={}).redis.db == 0  # không có biến môi trường ⇒ DB mặc định
    with pytest.raises(ValueError):
        experiment_env({}, redis_db=-1, work_dir=tmp_path)
    assert split_argv(["--redis-db", "9", "--", "--run-id", "r"]) == (["--redis-db", "9"], ["--run-id", "r"])
    with pytest.raises(SystemExit):
        split_argv(["--run-id", "r"])


def test_watermark_hook_reads_last_progress_line(tmp_path):
    path = tmp_path / "progress.jsonl"
    assert not watermark_passed(path, 1000)
    path.write_text(json.dumps({"event": "progress", "watermark_ms": 999}) + "\n", encoding="utf-8")
    assert not watermark_passed(path, 1000)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps({"event": "progress", "watermark_ms": 1000}) + "\n")
    assert watermark_passed(path, 1000) and not watermark_passed(path, None)


def test_dataset_cache_and_replay_window(tmp_path):
    params = {"learners": 20, "rate_events_per_s": 40, "late_profile": "high", "duplicate_rate": 0.05,
              "duration_s": 30}
    sim, cached = ensure_dataset(tmp_path / "sim", params, seed=5)
    assert not cached and cache_valid(sim) and sim.name == M.dataset_key(params, 5)
    assert ensure_dataset(tmp_path / "sim", params, seed=5) == (sim, True)
    (sim / "roster.json").write_text("{}", encoding="utf-8")  # cache hỏng ⇒ sinh lại
    assert ensure_dataset(tmp_path / "sim", params, seed=5) == (sim, False)

    full = pq.read_table(sim / "events.parquet")
    window = materialize_window(sim, tmp_path / "window", cutoff_ms=20_000)
    table = pq.read_table(tmp_path / "window" / "events.parquet")
    assert 0 < table.num_rows == window["rows"] < full.num_rows
    assert max(table.column("replay_offset_ms").to_pylist()) < 20_000
    assert table.schema.metadata[b"nhom8.t0"] == full.schema.metadata[b"nhom8.t0"]  # batch đóng dấu lại theo t0
    ids = set(table.column("event_id").to_pylist())
    episodes = [json.loads(line) for line in (tmp_path / "window" / "ground_truth.jsonl").read_text().splitlines()
                if json.loads(line)["record"] == "stuck_episode"]
    assert len(episodes) == window["episodes_kept"] and all(e["onset_event_id"] in ids for e in episodes)
    first = json.loads((tmp_path / "window" / "ground_truth.jsonl").read_text().splitlines()[0])
    assert first["record"] == "run"


def test_environment_info_has_cpu_and_kernel():
    info = RUN.environment_info()
    assert info["logical_cpus"] == os.cpu_count() and info["kernel"]
    assert set(info) >= {"cpu_model", "wslconfig", "wsl_distro"}


def test_dry_run_prints_plan_and_estimate(capsys):
    assert RUN.main(["--plan", str(PLAN), "--exp", "D", "--dry-run"]) == 0
    out = capsys.readouterr().out
    assert "D_late_duplicate" in out and '"total_runs": 42' in out


def test_cli_refuses_shared_db_without_prefix():
    with pytest.raises(SystemExit):
        RUN.main(["--plan", str(PLAN), "--exp", "A", "--redis-db", "0", "--dry-run"])


def test_archive_moves_instead_of_deleting(runner, tmp_path):
    runner.opts.archive_dir = tmp_path / "hdd"
    lake = tmp_path / "lake-run"
    lake.mkdir()
    (lake / "f").write_text("x", encoding="utf-8")
    assert runner.archive_or_remove(lake, "lake").startswith("archived:")
    assert (tmp_path / "hdd" / "lake" / "lake-run" / "f").is_file() and not lake.exists()
    runner.opts.archive_dir = None
    assert runner.archive_or_remove(Path(tmp_path / "absent"), "lake") == "absent"
