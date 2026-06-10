import asyncio, concurrent.futures, logging, threading, time
from collections.abc import Awaitable, Callable

from ._scope import CloseScope
from ._task import create_task_group, sleep

log = logging.getLogger("microio")


class ServiceThread(threading.Thread):
    "Supervised thread with ready/failed/stop state."

    def __init__(self, *, name: str | None = None, target: Callable[["ServiceThread"], None] | None = None,
        daemon: bool = True, reraise: bool = False):
        super().__init__(name=name, daemon=daemon)
        self._target_func = target
        self.reraise = reraise
        self.ready = threading.Event()
        self.failed = threading.Event()
        self.stopped = threading.Event()
        self.scope = CloseScope()
        self.exc = None

    def started(self):
        "Mark the service ready."
        self.ready.set()

    def fail(self, exc: BaseException):
        "Mark the service failed and wake waiters."
        self.exc = exc
        self.scope.fail(exc)
        self.failed.set()
        self.ready.set()

    def wait_started(self, timeout: float | None = None):
        "Wait until started or failed."
        deadline = None if timeout is None else time.monotonic() + timeout
        while True:
            if self.failed.is_set(): raise RuntimeError(f"{self.name} failed") from self.exc
            if self.ready.wait(timeout=0.01): return
            if deadline is not None and time.monotonic() >= deadline: raise TimeoutError(f"{self.name} did not become ready")

    def stop(self, reason: str | None = "stop")->bool: return self.scope.close(reason=reason)

    def join_or_log(self, timeout: float | None = None)->bool:
        if self.ident is None and not self.is_alive(): return True
        self.join(timeout=timeout)
        if not self.is_alive(): return True
        log.error("thread did not stop: %s", self.name)
        return False

    def run_service(self):
        if self._target_func is None: raise NotImplementedError("override run_service() or pass target=")
        self._target_func(self)

    def run(self):
        try: self.run_service()
        except BaseException as exc:
            self.fail(exc)
            if self.reraise: raise
        finally:
            self.stopped.set()
            self.ready.set()


class LoopServiceThread(ServiceThread):
    "ServiceThread that owns an asyncio.Runner and loop."

    def __init__(self, *, name: str | None = None, daemon: bool = True, reraise: bool = False):
        super().__init__(name=name, daemon=daemon, reraise=reraise)
        self.loop = None
        self.task_group = None
        self._runner_task = None

    def run_service(self):
        with asyncio.Runner() as runner:
            self.loop = runner.get_loop()
            try: runner.run(self._run_main())
            except asyncio.CancelledError:
                if not self.scope.closed: raise
            finally: self.loop = None

    async def _run_main(self):
        self._runner_task = asyncio.current_task()
        try:
            async with create_task_group() as tg:
                self.task_group = tg
                tg.start_soon(self.run_async, name=f"{self.name}-main")
        finally:
            self.task_group = None
            self._runner_task = None

    async def run_async(self):
        "Override in subclasses."
        self.started()
        while not self.scope.closed: await sleep(0.05)

    def call_soon(self, fn: Callable, *args):
        loop = self.loop
        if loop is None: raise RuntimeError("loop is not running")
        loop.call_soon_threadsafe(fn, *args)

    def call_sync(self, fn: Callable, *args, timeout: float | None = None, **kwargs):
        "Run `fn` on the loop thread and return its result."
        loop = self.loop
        if loop is None: raise RuntimeError("loop is not running")
        try: running = asyncio.get_running_loop()
        except RuntimeError: running = None
        if running is loop: return fn(*args, **kwargs)
        fut = concurrent.futures.Future()
        def _run():
            if not fut.set_running_or_notify_cancel(): return
            try: fut.set_result(fn(*args, **kwargs))
            except BaseException as exc: fut.set_exception(exc)
        loop.call_soon_threadsafe(_run)
        return fut.result(timeout=timeout)

    def submit(self, coro: Awaitable)->concurrent.futures.Future:
        loop = self.loop
        if loop is None: raise RuntimeError("loop is not running")
        return asyncio.run_coroutine_threadsafe(coro, loop)

    def stop(self, reason: str | None = "stop")->bool:
        first = super().stop(reason=reason)
        if self.task_group is not None:
            self.task_group.cancel(reason)
            return first
        loop, task = self.loop, self._runner_task
        if loop is not None and task is not None and not task.done():
            try:
                try: running = asyncio.get_running_loop()
                except RuntimeError: running = None
                if running is loop and asyncio.current_task() is task: loop.call_soon(task.cancel)
                else: loop.call_soon_threadsafe(task.cancel)
            except RuntimeError: pass
        return first


class ServiceGroup:
    "Small owner for starting/stopping ServiceThread instances together."

    def __init__(self, *services): self.services = [svc for svc in services if svc is not None]

    def add(self, *services):
        self.services.extend(svc for svc in services if svc is not None)
        return self

    def start(self):
        for svc in self.services: svc.start()
        return self

    def wait_started(self, timeout: float | None = None):
        deadline = None if timeout is None else time.monotonic() + timeout
        for svc in self.services:
            rem = None if deadline is None else max(0.0, deadline - time.monotonic())
            svc.wait_started(timeout=rem)
        return self

    def stop(self):
        for svc in self.services: svc.stop()
        return self

    def join_or_log(self, timeout: float | None = None)->bool:
        ok = True
        for svc in self.services: ok = svc.join_or_log(timeout=timeout) and ok
        return ok

    def stop_join(self, timeout: float | None = None)->bool:
        self.stop()
        return self.join_or_log(timeout=timeout)
