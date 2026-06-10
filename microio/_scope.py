import threading


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
