import queue, threading
from collections.abc import Hashable


class ReplyHandle:
    "Resolve/fail/wait one RequestRegistry entry."

    def __init__(self, registry: "RequestRegistry", key: Hashable, waiter: queue.Queue):
        self.registry = registry
        self.key = key
        self.waiter = waiter

    def resolve(self, value)->bool: return self.registry.resolve(self.key, value)

    def fail(self, exc: BaseException)->bool: return self.registry.fail(self.key, exc)

    def pop(self): return self.registry.pop(self.key, waiter=self.waiter)

    def wait(self, timeout: float | None = None): return self.registry.wait(self.key, self.waiter, timeout=timeout)


class RequestRegistry:
    "Track request waiters and fail them reliably on close/crash."

    def __init__(self):
        self._lock = threading.Lock()
        self._pending = {}

    def __len__(self)->int:
        with self._lock: return len(self._pending)

    def __contains__(self, key: Hashable)->bool:
        with self._lock: return key in self._pending

    @property
    def pending(self)->dict:
        with self._lock: return dict(self._pending)

    def register(self, key: Hashable)->queue.Queue:
        "Register `key` and return its waiter queue."
        waiter = queue.Queue(maxsize=1)
        with self._lock:
            if key in self._pending: raise KeyError(f"request already pending: {key!r}")
            self._pending[key] = waiter
        return waiter

    def reply(self, key: Hashable)->ReplyHandle:
        "Register `key` and return a reply handle."
        return ReplyHandle(self, key, self.register(key))

    def pop(self, key: Hashable, waiter=None):
        "Remove `key`, optionally only if it still maps to `waiter`."
        with self._lock:
            if waiter is not None and self._pending.get(key) is not waiter: return None
            return self._pending.pop(key, None)

    def resolve(self, key: Hashable, value)->bool:
        "Resolve one pending request; return False if it is no longer pending."
        waiter = self.pop(key)
        if waiter is None: return False
        waiter.put(value)
        return True

    def fail(self, key: Hashable, exc: BaseException)->bool: return self.resolve(key, exc)

    def fail_all(self, exc: BaseException)->int:
        "Fail every pending request and return how many waiters were woken."
        with self._lock:
            waiters = list(self._pending.values())
            self._pending.clear()
        for waiter in waiters: waiter.put(exc)
        return len(waiters)

    def wait(self, key: Hashable, waiter: queue.Queue, timeout: float | None = None):
        "Wait for `waiter`, removing `key` on timeout/cancel."
        try: result = waiter.get(timeout=timeout)
        except queue.Empty as exc:
            self.pop(key, waiter=waiter)
            raise TimeoutError(f"timed out waiting for request {key!r}") from exc
        self.pop(key, waiter=waiter)
        if isinstance(result, BaseException): raise result
        return result

    def request(self, key: Hashable, send, timeout: float | None = None):
        "Register `key`, call `send(reply)`, then wait; unregister if sending fails."
        reply = self.reply(key)
        try: send(reply)
        except BaseException:
            reply.pop()
            raise
        return reply.wait(timeout=timeout)
