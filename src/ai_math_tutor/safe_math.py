"""Chạy hàm kiểm chứng toán trong tiến trình con có thể giết thật khi quá hạn.

`math_check` đã chặn các biểu thức tốn kém trước khi tính, nhưng SymPy vẫn là
mã bên thứ ba chạy trên đầu vào không tin cậy. Lớp này thêm thời hạn cứng:
- mỗi worker là một tiến trình riêng; lời gọi quá `timeout_s` thì worker bị
  `terminate()` (rồi `kill()` nếu cần) và được thay bằng worker mới — không có
  luồng treo nào bị bỏ lại chạy ngầm;
- số lời gọi đang chờ + đang chạy bị giới hạn bởi `max_pending`; vượt quá thì ném
  `SafeMathOverloaded` ngay (dịch vụ gợi ý trả HTTP 429) thay vì xếp hàng vô hạn;
- chỉ gọi được các hàm trong danh sách cho phép của `math_check`.

Tiến trình con dùng `forkserver` (Linux) hoặc `spawn` (nơi khác), không dùng
`fork`, nên an toàn khi tiến trình cha có nhiều luồng (FastAPI, Spark driver).
Ngoại lệ trong worker (vd. MathInputError) được ném lại nguyên kiểu ở tiến trình cha.

Dùng:
    with SafeMath(timeout_s=2.0, max_pending=8) as checker:
        checker.call("diagnose", problem, "x = 4")
"""

from __future__ import annotations

import multiprocessing
import queue
import threading
from collections.abc import Callable, Mapping
from multiprocessing.connection import Connection
from multiprocessing.reduction import ForkingPickler
from types import MappingProxyType, TracebackType
from typing import Any

from ai_math_tutor import math_check

ALLOWED_FUNCTIONS: Mapping[str, Callable[..., Any]] = MappingProxyType(
    {
        name: getattr(math_check, name)
        for name in (
            "parse_answer",
            "solution_set",
            "step_is_equivalent",
            "answer_matches",
            "arith_step",
            "diagnose",
        )
    }
)
DEFAULT_TIMEOUT_S = 2.0
DEFAULT_MAX_PENDING = 8
DEFAULT_STARTUP_TIMEOUT_S = 60.0
_KILL_GRACE_S = 0.05
_READY = "ready"


class SafeMathError(RuntimeError):
    """Lời gọi kiểm chứng không hoàn thành (worker chết, đã đóng, kết quả không gửi được)."""


class SafeMathTimeout(SafeMathError):
    """Lời gọi vượt thời hạn; worker đã bị giết và thay thế."""


class SafeMathOverloaded(SafeMathError):
    """Quá nhiều lời gọi đang chờ; từ chối ngay thay vì xếp hàng."""


def _worker_main(conn: Connection) -> None:
    """Vòng lặp của tiến trình con: nhận (hàm, tham số), trả ("ok", kết quả) hoặc ("error", ngoại lệ)."""
    conn.send(_READY)
    while True:
        try:
            request = conn.recv()
        except EOFError:
            return
        func, args = request
        try:
            reply: tuple[str, Any] = ("ok", func(*args))
        except Exception as exc:  # noqa: BLE001 — chuyển mọi lỗi của hàm về tiến trình cha
            reply = ("error", exc)
        try:
            conn.send(reply)
        except Exception as exc:  # noqa: BLE001 — kết quả/ngoại lệ không pickle được
            conn.send(("error", SafeMathError(f"không gửi được kết quả: {type(exc).__name__}: {exc}")))


def _default_start_method() -> str:
    return "forkserver" if "forkserver" in multiprocessing.get_all_start_methods() else "spawn"


class _Worker:
    def __init__(self, ctx: multiprocessing.context.BaseContext) -> None:
        parent_conn, child_conn = ctx.Pipe(duplex=True)
        self.process = ctx.Process(target=_worker_main, args=(child_conn,), name="safe-math-worker", daemon=True)
        self.process.start()
        child_conn.close()
        self.conn = parent_conn
        self.ready = False

    def wait_ready(self, timeout_s: float) -> None:
        if self.ready:
            return
        if not self.conn.poll(timeout_s):
            raise SafeMathError(f"worker không khởi động trong {timeout_s} s")
        if self.conn.recv() != _READY:
            raise SafeMathError("worker gửi tín hiệu khởi động lạ")
        self.ready = True

    def stop(self) -> None:
        """Dừng hẳn tiến trình: SIGTERM, chờ ngắn, rồi SIGKILL; luôn thu hồi (join)."""
        self.conn.close()
        if self.process.is_alive():
            self.process.terminate()
            self.process.join(_KILL_GRACE_S)
            if self.process.is_alive():
                self.process.kill()
        self.process.join()


class SafeMath:
    """Bộ gọi `math_check` qua worker giết được, có hàng đợi giới hạn. An toàn đa luồng.

    - `timeout_s`: thời hạn cứng của một lời gọi (tính từ lúc gửi cho worker).
    - `max_pending`: tối đa số lời gọi đang chạy + đang chờ worker rảnh.
    - `workers`: số tiến trình con chạy song song; worker khởi động lười ở lần gọi đầu.
    - `extra_functions`: hàm bổ sung (cấp module, pickle được) cho mục đích đặc biệt
      như kiểm thử; không bao giờ lấy tên hàm từ đầu vào người dùng để đăng ký.
    """

    def __init__(
        self,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        max_pending: int = DEFAULT_MAX_PENDING,
        workers: int = 1,
        *,
        start_method: str | None = None,
        extra_functions: Mapping[str, Callable[..., Any]] | None = None,
        startup_timeout_s: float = DEFAULT_STARTUP_TIMEOUT_S,
    ) -> None:
        if not timeout_s > 0 or not startup_timeout_s > 0:
            raise ValueError("timeout_s và startup_timeout_s phải dương")
        if workers < 1 or max_pending < 1:
            raise ValueError("workers và max_pending phải ≥ 1")
        self._functions = dict(ALLOWED_FUNCTIONS)
        for name, func in (extra_functions or {}).items():
            if not callable(func):
                raise TypeError(f"{name!r} không gọi được")
            self._functions[name] = func
        method = start_method or _default_start_method()
        self._ctx = multiprocessing.get_context(method)
        if method == "forkserver":
            # Nạp SymPy sẵn trong forkserver: worker thay thế sẵn sàng sau vài ms thay vì vài giây.
            self._ctx.set_forkserver_preload(["ai_math_tutor.math_check"])
        self._timeout_s = timeout_s
        self._startup_timeout_s = startup_timeout_s
        self._max_pending = max_pending
        self._idle: queue.SimpleQueue[_Worker | None] = queue.SimpleQueue()
        for _ in range(workers):
            self._idle.put(None)  # None = chưa có tiến trình, tạo khi cần
        self._live: set[_Worker] = set()
        self._lock = threading.Lock()
        self._pending = 0
        self._closed = False

    @property
    def timeout_s(self) -> float:
        return self._timeout_s

    def call(self, func_name: str, *args: Any) -> Any:
        """Gọi `math_check.<func_name>(*args)` trong worker; ném lại ngoại lệ gốc của hàm.

        Ném ValueError (hàm không được phép), SafeMathOverloaded, SafeMathTimeout, SafeMathError.
        """
        func = self._functions.get(func_name)
        if func is None:
            raise ValueError(f"hàm không được phép: {func_name!r}")
        try:
            payload = ForkingPickler.dumps((func, args))
        except Exception as exc:  # noqa: BLE001 — tham số không pickle được: lỗi của bên gọi
            raise SafeMathError(f"tham số không gửi được cho worker: {type(exc).__name__}: {exc}") from exc
        with self._lock:
            if self._closed:
                raise SafeMathError("SafeMath đã đóng")
            if self._pending >= self._max_pending:
                raise SafeMathOverloaded(f"quá tải: {self._pending} lời gọi đang chờ (tối đa {self._max_pending})")
            self._pending += 1
        try:
            return self._run(payload, func_name)
        finally:
            with self._lock:
                self._pending -= 1

    def _run(self, payload: bytes, func_name: str) -> Any:
        worker = self._idle.get()
        try:
            if worker is None:
                worker = self._spawn()
            try:
                worker.wait_ready(self._startup_timeout_s)
                worker.conn.send_bytes(payload)
                finished = worker.conn.poll(self._timeout_s)
                reply = worker.conn.recv() if finished else None
            except (EOFError, OSError, SafeMathError) as exc:
                self._discard(worker)
                worker = None
                if isinstance(exc, SafeMathError):
                    raise
                raise SafeMathError(f"worker dừng bất thường khi chạy {func_name}: {type(exc).__name__}") from exc
            if not finished:
                self._discard(worker)
                worker = self._spawn_replacement()
                raise SafeMathTimeout(f"{func_name} vượt {self._timeout_s} s; worker đã bị giết")
        finally:
            self._release(worker)
        status, value = reply
        if status == "error":
            raise value
        return value

    def _spawn(self) -> _Worker:
        try:
            worker = _Worker(self._ctx)
        except OSError as exc:
            raise SafeMathError(f"không tạo được worker: {exc}") from exc
        with self._lock:
            closed = self._closed
            self._live.add(worker)
        if closed:
            self._discard(worker)
            raise SafeMathError("SafeMath đã đóng")
        return worker

    def _spawn_replacement(self) -> _Worker | None:
        """Tạo worker thay thế ngay để nó khởi động trong lúc rảnh; lỗi thì để lần gọi sau thử lại."""
        try:
            return self._spawn()
        except SafeMathError:
            return None

    def _discard(self, worker: _Worker) -> None:
        with self._lock:
            self._live.discard(worker)
        worker.stop()

    def _release(self, worker: _Worker | None) -> None:
        with self._lock:
            closed = self._closed
        if closed and worker is not None:
            self._discard(worker)
            worker = None
        self._idle.put(worker)

    def close(self) -> None:
        """Dừng mọi worker. Lời gọi đang chạy nhận SafeMathError; lời gọi mới bị từ chối."""
        with self._lock:
            self._closed = True
            live, self._live = list(self._live), set()
        for worker in live:
            worker.stop()

    def __enter__(self) -> SafeMath:
        return self

    def __exit__(
        self, exc_type: type[BaseException] | None, exc: BaseException | None, tb: TracebackType | None
    ) -> None:
        self.close()
