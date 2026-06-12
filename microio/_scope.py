import threading
from contextlib import contextmanager


class ClosedResourceError(RuntimeError): pass

class BrokenResourceError(RuntimeError): pass

class EndOfStream(Exception): pass


class CloseScope:
    "Thread-safe cooperative close/failure state."

    def __init__(self):
        self._event = threading.Event()
        self._lock = threading.Lock()
        self.reason = None
        self.exc = None

    @property
    def closed(self)->bool: return self._event.is_set()

    @property
    def failed(self)->bool: return self.exc is not None

    def close(self, reason: str | None = None, exc: BaseException | None = None)->bool:
        "Close once; return True for the first caller."
        with self._lock:
            if self._event.is_set(): return False
            self.reason = reason
            self.exc = exc
            self._event.set()
            return True

    def fail(self, exc: BaseException, reason: str | None = None)->bool:
        "Close with failure exception."
        return self.close(reason=reason or str(exc), exc=exc)

    def wait(self, timeout: float | None = None)->bool: return self._event.wait(timeout=timeout)

    def raise_if_closed(self):
        if not self.closed: return
        if self.exc is not None: raise BrokenResourceError(self.reason or str(self.exc)) from self.exc
        raise ClosedResourceError(self.reason or "closed")


class WorkTracker:
    "Thread-safe in-flight work counter with a `busy` Event view (a WaitGroup)."

    def __init__(self):
        self._lock = threading.Lock()
        self.count = 0
        self.busy = threading.Event()

    def add(self):
        "Record one unit of in-flight work."
        with self._lock:
            self.count += 1
            self.busy.set()

    def done(self):
        "Record completion of one unit of work."
        with self._lock:
            self.count -= 1
            if self.count == 0: self.busy.clear()

    @contextmanager
    def track(self):
        "Track one unit of work for the duration of the block."
        self.add()
        try: yield
        finally: self.done()
