import asyncio, logging, threading
from collections import deque
from dataclasses import dataclass

from ._scope import BrokenResourceError, ClosedResourceError, EndOfStream
from ._task import checkpoint_if_cancelled

log = logging.getLogger("microio")
_closed = object()


@dataclass
class ChannelStats:
    sent: int = 0
    received: int = 0
    dropped: int = 0
    queued: int = 0
    closed: bool = False


class _ChannelState:
    def __init__(self, late_send: str = "raise"):
        if late_send not in {"raise", "drop"}: raise ValueError("late_send must be 'raise' or 'drop'")
        self.late_send = late_send
        self.loop = None
        self.q = None
        self.pending = deque()
        self.lock = threading.Lock()
        self.closed = False
        self.sent = 0
        self.received = 0
        self.dropped = 0

    def stats(self)->ChannelStats:
        with self.lock:
            queued = (self.q.qsize() if self.q is not None else len(self.pending))
            return ChannelStats(self.sent, self.received, self.dropped, queued, self.closed)


class ObjectSendChannel:
    "Thread-safe sender endpoint for an ObjectReceiveChannel."

    def __init__(self, state: _ChannelState): self._state = state

    def send_nowait(self, item):
        "Queue `item` from any thread."
        st = self._state
        with st.lock:
            if st.closed:
                st.dropped += 1
                if st.late_send == "drop": return None
                raise ClosedResourceError("send on closed channel")
            st.sent += 1
            loop, q = st.loop, st.q
            if loop is None or q is None:
                st.pending.append(item)
                return st.sent
        try: loop.call_soon_threadsafe(q.put_nowait, item)
        except RuntimeError:
            with st.lock:
                st.dropped += 1
                if st.late_send == "drop": return None
            raise BrokenResourceError("receiver loop is closed")
        return st.sent

    def close(self):
        "Close the channel and wake the receiver."
        return self._close(_closed)

    def fail(self, exc: BaseException):
        "Break the channel and wake the receiver with `exc`."
        return self._close(exc)

    def _close(self, item):
        st = self._state
        with st.lock:
            if st.closed: return False
            st.closed = True
            loop, q = st.loop, st.q
            if loop is None or q is None:
                st.pending.append(item)
                return True
        try: loop.call_soon_threadsafe(q.put_nowait, item)
        except RuntimeError: pass
        return True

    @property
    def closed(self)->bool: return self._state.closed

    def stats(self)->ChannelStats: return self._state.stats()


class ObjectReceiveChannel:
    "Async receiver endpoint for objects sent from other threads/tasks."

    def __init__(self, state: _ChannelState): self._state = state

    def bind(self, loop: asyncio.AbstractEventLoop | None = None):
        "Bind to an event loop; pending sends are flushed."
        st = self._state
        if loop is None: loop = asyncio.get_running_loop()
        with st.lock:
            if st.q is not None: return
            st.loop = loop
            st.q = asyncio.Queue()
            for item in st.pending: st.q.put_nowait(item)
            st.pending.clear()

    async def receive(self):
        "Receive one item, raising EndOfStream when closed."
        st = self._state
        if st.q is None: self.bind()
        checkpoint_if_cancelled()
        item = await st.q.get()
        checkpoint_if_cancelled()
        if item is _closed:
            st.q.put_nowait(_closed)
            raise EndOfStream
        if isinstance(item, BaseException):
            st.q.put_nowait(item)
            raise item
        with st.lock: st.received += 1
        return item

    def drain_nowait(self, max_items: int | None = None)->list:
        "Drain currently queued items without awaiting."
        st = self._state
        if st.q is None: return []
        out = []
        while max_items is None or len(out) < max_items:
            try: item = st.q.get_nowait()
            except asyncio.QueueEmpty: return out
            if item is _closed:
                st.q.put_nowait(_closed)
                return out
            if isinstance(item, BaseException):
                st.q.put_nowait(item)
                return out
            out.append(item)
        return out

    def close(self)->bool: return ObjectSendChannel(self._state).close()

    def fail(self, exc: BaseException)->bool: return ObjectSendChannel(self._state).fail(exc)

    def __aiter__(self): return self

    async def __anext__(self):
        try: return await self.receive()
        except EndOfStream: raise StopAsyncIteration

    @property
    def closed(self)->bool: return self._state.closed

    def stats(self)->ChannelStats: return self._state.stats()


def create_channel(*, late_send: str = "raise")->tuple[ObjectSendChannel, ObjectReceiveChannel]:
    "Create thread-safe object channel endpoints."
    state = _ChannelState(late_send=late_send)
    return ObjectSendChannel(state), ObjectReceiveChannel(state)
