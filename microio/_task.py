import asyncio, contextvars, inspect, threading

_scope_stack = contextvars.ContextVar("microio_cancel_scopes", default=())


class _TaskGroupCancelled(Exception): pass


async def _raise_task_group_cancel(): raise _TaskGroupCancelled


def _current_task():
    try: return asyncio.current_task()
    except RuntimeError: return None


def _cancel_task(task):
    if task is None or task.done(): return
    loop = task.get_loop()
    try: running = asyncio.get_running_loop()
    except RuntimeError: running = None
    if running is loop: task.cancel()
    else: loop.call_soon_threadsafe(task.cancel)


def _uncancel_task(task):
    if task is not None and hasattr(task, "uncancel") and task.cancelling(): task.uncancel()


class CancelScope:
    "Async task cancellation state with optional deadline."

    def __init__(self, *, deadline: float | None = None, delay: float | None = None, raise_timeout: bool = False):
        self.deadline = deadline
        self.delay = delay
        self.raise_timeout = raise_timeout
        self.reason = None
        self.cancel_called = False
        self.cancelled_caught = False
        self.timed_out = False
        self._lock = threading.RLock()
        self._tasks = set()
        self._entries = []
        self._callbacks = []
        self._timeout_handle = None

    @property
    def cancelled(self)->bool:
        with self._lock: return self.cancel_called

    @property
    def active(self)->bool:
        with self._lock: return bool(self._entries)

    def _add_cancel_callback(self, cb):
        with self._lock:
            if not self.cancel_called:
                self._callbacks.append(cb)
                return
        cb()

    def cancel(self, reason: str | None = None)->bool:
        caller = _current_task()
        with self._lock:
            if self.cancel_called: return False
            self.cancel_called = True
            self.reason = reason
            timeout_handle = self._timeout_handle
            self._timeout_handle = None
            tasks = list(self._tasks)
            callbacks = list(self._callbacks)
        if timeout_handle is not None: timeout_handle.cancel()
        for task in tasks:
            if task is not caller: _cancel_task(task)
        for cb in callbacks: cb()
        return True

    def _arm_deadline(self):
        loop = asyncio.get_running_loop()
        def _timeout():
            with self._lock: self.timed_out = True
            self.cancel("deadline")
        with self._lock:
            if self._timeout_handle is not None or (self.deadline is None and self.delay is None): return
            if self.deadline is None: self.deadline = loop.time() + self.delay
            self._timeout_handle = loop.call_soon(_timeout) if self.deadline <= loop.time() else loop.call_at(self.deadline, _timeout)

    def __enter__(self):
        token = _scope_stack.set(_scope_stack.get() + (self,))
        task = _current_task()
        with self._lock:
            if task is not None: self._tasks.add(task)
            self._entries.append((task, token))
            first = len(self._entries) == 1
        if first: self._arm_deadline()
        return self

    def _pop_entry(self, task):
        with self._lock:
            for i in range(len(self._entries) - 1, -1, -1):
                if self._entries[i][0] is task:
                    _, token = self._entries.pop(i)
                    break
            else: raise RuntimeError("CancelScope exited in a different task")
            if task is not None: self._tasks.discard(task)
            timeout_handle = self._timeout_handle if not self._entries else None
            if timeout_handle is not None: self._timeout_handle = None
        return token, timeout_handle

    def _suppress_cancelled(self, task, exc)->bool:
        with self._lock:
            if not self.cancel_called: return False
            self.cancelled_caught = True
            raise_timeout, timed_out = self.raise_timeout, self.timed_out
        _uncancel_task(task)
        if raise_timeout and timed_out: raise TimeoutError("operation timed out") from exc
        return True

    def __exit__(self, exc_type, exc, tb):
        task = _current_task()
        token, timeout_handle = self._pop_entry(task)
        _scope_stack.reset(token)
        if timeout_handle is not None: timeout_handle.cancel()
        if exc_type is not None and issubclass(exc_type, asyncio.CancelledError): return self._suppress_cancelled(task, exc)
        return False


def current_cancel_scope():
    "Return the innermost cancelled active scope, if any."
    for scope in reversed(_scope_stack.get()):
        if scope.cancelled: return scope
    return None


def checkpoint_if_cancelled():
    "Raise CancelledError if an active microio scope is cancelled."
    if current_cancel_scope() is not None: raise asyncio.CancelledError


async def checkpoint():
    "Yield once and honor active microio cancellation."
    checkpoint_if_cancelled()
    await asyncio.sleep(0)
    checkpoint_if_cancelled()


async def sleep(delay: float, result=None):
    "asyncio.sleep() with microio cancellation checkpoints."
    checkpoint_if_cancelled()
    res = await asyncio.sleep(delay, result)
    checkpoint_if_cancelled()
    return res


def move_on_after(delay: float)->CancelScope: return CancelScope(delay=delay)


def fail_after(delay: float)->CancelScope: return CancelScope(delay=delay, raise_timeout=True)


class TaskStatus:
    "Readiness handle passed to TaskGroup.start() children."

    def __init__(self, fut: asyncio.Future):
        self._fut = fut
        self.started_called = False

    def started(self, value=None):
        if self._fut.done(): raise RuntimeError("task_status.started() called twice")
        self.started_called = True
        self._fut.set_result(value)

    def _fail(self, exc: BaseException):
        if not self._fut.done(): self._fut.set_exception(exc)


class TaskGroup:
    "Small wrapper around asyncio.TaskGroup with a cancel scope."

    def __init__(self):
        self.cancel_scope = CancelScope()
        self.cancel_scope._add_cancel_callback(self._cancel_group)
        self._tg = None
        self._loop = None
        self._owner_task = None

    async def __aenter__(self):
        self._loop = asyncio.get_running_loop()
        self._owner_task = asyncio.current_task()
        self._tg = asyncio.TaskGroup()
        await self._tg.__aenter__()
        if self.cancel_scope.cancelled: self._cancel_group()
        return self

    async def __aexit__(self, exc_type, exc, tb):
        res = False
        cancelled = False
        try: res = await self._tg.__aexit__(exc_type, exc, tb)
        except* _TaskGroupCancelled: cancelled = True
        finally:
            self._tg = None
            self._loop = None
            self._owner_task = None
        if cancelled: _uncancel_task(_current_task())
        return res

    def _cancel_group(self):
        if self._tg is None or self._loop is None: return
        if _current_task() is self._owner_task: return
        def _create_cancel_task():
            try: self._tg.create_task(_raise_task_group_cancel())
            except RuntimeError: pass
        try: running = asyncio.get_running_loop()
        except RuntimeError: running = None
        if running is self._loop: _create_cancel_task()
        else: self._loop.call_soon_threadsafe(_create_cancel_task)

    def cancel(self, reason: str | None = None)->bool: return self.cancel_scope.cancel(reason=reason)

    async def _run(self, coro):
        with self.cancel_scope:
            checkpoint_if_cancelled()
            return await coro

    def create_task(self, coro, *, name: str | None = None):
        if self._tg is None: raise RuntimeError("TaskGroup is not active")
        return self._tg.create_task(self._run(coro), name=name)

    async def _call(self, fn, *args, **kwargs):
        res = fn(*args, **kwargs)
        return await res if inspect.isawaitable(res) else res

    def start_soon(self, fn, *args, name: str | None = None, **kwargs):
        if self._tg is None: raise RuntimeError("TaskGroup is not active")
        return self.create_task(self._call(fn, *args, **kwargs), name=name)

    async def _call_started(self, fn, args, kwargs, task_status: TaskStatus):
        try:
            res = fn(*args, task_status=task_status, **kwargs)
            if inspect.isawaitable(res): await res
            if not task_status.started_called: task_status._fail(RuntimeError("task exited without calling task_status.started()"))
        except BaseException as exc:
            task_status._fail(exc)
            raise

    async def start(self, fn, *args, name: str | None = None, **kwargs):
        if self._loop is None: raise RuntimeError("TaskGroup is not active")
        task_status = TaskStatus(self._loop.create_future())
        self.create_task(self._call_started(fn, args, kwargs, task_status), name=name)
        return await task_status._fut


def create_task_group()->TaskGroup: return TaskGroup()
