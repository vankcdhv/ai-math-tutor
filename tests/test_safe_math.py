"""Worker kiểm chứng phải bị giết thật khi quá hạn và được thay để phục vụ lời gọi sau."""

import os
import threading
import time

import psutil
import pytest
import sympy

from ai_math_tutor import math_check
from ai_math_tutor.safe_math import (
    ALLOWED_FUNCTIONS,
    SafeMath,
    SafeMathError,
    SafeMathOverloaded,
    SafeMathTimeout,
)


# Hàm cấp module để worker (forkserver/spawn) import được theo tên khi unpickle.
def _spin() -> None:
    while True:  # vòng lặp CPU thuần: không nhả GIL, không phản hồi tín hiệu hợp tác
        pass


def _sleep(seconds: float) -> float:
    time.sleep(seconds)
    return seconds


def _pid() -> int:
    return os.getpid()


def _crash() -> None:
    os._exit(3)


EXTRA = {"spin": _spin, "sleep": _sleep, "pid": _pid, "crash": _crash}


@pytest.fixture
def checker():
    with SafeMath(timeout_s=0.5, max_pending=4, extra_functions=EXTRA) as instance:
        yield instance


def test_math_functions_run_in_worker(checker):
    assert checker.call("solution_set", "x^2 = 4") == sympy.FiniteSet(-2, 2)
    assert checker.call("arith_step", "48/2 = 24") is True
    assert checker.call("answer_matches", "x^2 - 5x + 6 = 0", "{2, 3}") is True
    problem = {"kind": "equation", "equation": "2*x + 3 = 11", "answer": "4", "steps": [], "misconceptions": []}
    assert checker.call("diagnose", problem, "x = -4") == {"equivalent": False, "misconception": "sign_error"}
    assert checker.call("pid") != os.getpid()


def test_worker_exceptions_keep_their_type(checker):
    with pytest.raises(math_check.MathInputError, match="số mũ"):
        checker.call("solution_set", "9^9^9 = x")
    assert checker.call("arith_step", "2 + 2 = 4") is True


@pytest.mark.parametrize("name", ["parse_expr", "__import__", "eval", "_parse_side", "VARIABLE"])
def test_only_whitelisted_functions_callable(name):
    assert name not in ALLOWED_FUNCTIONS
    with SafeMath() as instance, pytest.raises(ValueError, match="không được phép"):
        instance.call(name, "x = 1")


def test_hanging_call_is_killed_within_timeout_and_worker_replaced(checker):
    old_pid = checker.call("pid")  # worker đã sẵn sàng: thời gian khởi động không tính vào
    start = time.perf_counter()
    with pytest.raises(SafeMathTimeout):
        checker.call("spin")
    elapsed = time.perf_counter() - start
    assert elapsed <= checker.timeout_s + 0.1, f"bị giết sau {elapsed:.3f} s"
    assert not psutil.pid_exists(old_pid), "worker treo vẫn còn sống"
    new_pid = checker.call("pid")
    assert new_pid != old_pid
    assert checker.call("arith_step", "48/2 = 24") is True


def test_crashed_worker_reported_and_replaced(checker):
    with pytest.raises(SafeMathError):
        checker.call("crash")
    assert checker.call("arith_step", "1 + 1 = 2") is True


def test_unpicklable_arguments_rejected_without_losing_worker(checker):
    pid = checker.call("pid")
    with pytest.raises(SafeMathError, match="không gửi được"):
        checker.call("solution_set", lambda: "x = 1")
    assert checker.call("pid") == pid


def test_bounded_queue_rejects_when_overloaded():
    with SafeMath(timeout_s=5.0, max_pending=1, extra_functions=EXTRA) as instance:
        instance.call("pid")
        results: list[float] = []
        busy = threading.Thread(target=lambda: results.append(instance.call("sleep", 1.0)))
        busy.start()
        time.sleep(0.3)
        start = time.perf_counter()
        with pytest.raises(SafeMathOverloaded):
            instance.call("arith_step", "1 + 1 = 2")
        assert time.perf_counter() - start < 0.05
        busy.join()
        assert results == [1.0]
        assert instance.call("arith_step", "1 + 1 = 2") is True


def test_workers_run_calls_in_parallel():
    with SafeMath(timeout_s=5.0, max_pending=4, workers=2, extra_functions=EXTRA) as instance:
        warm = [threading.Thread(target=instance.call, args=("pid",)) for _ in range(2)]
        for thread in warm:
            thread.start()
        for thread in warm:
            thread.join()
        start = time.perf_counter()
        calls = [threading.Thread(target=instance.call, args=("sleep", 0.5)) for _ in range(2)]
        for thread in calls:
            thread.start()
        for thread in calls:
            thread.join()
        assert time.perf_counter() - start < 0.9


def test_closed_instance_rejects_calls_and_stops_workers():
    instance = SafeMath(extra_functions=EXTRA)
    pid = instance.call("pid")
    instance.close()
    assert not psutil.pid_exists(pid)
    with pytest.raises(SafeMathError, match="đã đóng"):
        instance.call("arith_step", "1 + 1 = 2")


def test_spawn_start_method_works():
    with SafeMath(start_method="spawn") as instance:
        assert instance.call("arith_step", "0.5*8 = 4") is True


@pytest.mark.parametrize("kwargs", [{"timeout_s": 0}, {"max_pending": 0}, {"workers": 0}])
def test_invalid_configuration_rejected(kwargs):
    with pytest.raises(ValueError):
        SafeMath(**kwargs)
