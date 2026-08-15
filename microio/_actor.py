import heapq, inspect

from ._channel import create_channel
from ._scope import EndOfStream
from ._task import create_task_group


class Mailbox:
    "Thread-safe closable mailbox with async receive."

    def __init__(self, *, late_send: str = "drop"): self.send, self.receive = create_channel(late_send=late_send)

    def bind(self, loop=None): self.receive.bind(loop)

    def submit(self, item): return self.send.send_nowait(item)

    put = submit

    async def get(self): return await self.receive.receive()

    def close(self)->bool: return self.send.close()

    def fail(self, exc: BaseException)->bool: return self.send.fail(exc)

    def drain_nowait(self, max_items: int | None = None)->list: return self.receive.drain_nowait(max_items=max_items)

    def __aiter__(self): return self.receive.__aiter__()


_poke = object()
_nothing = object()

class PriorityMailbox(Mailbox):
    "Mailbox yielding the highest-priority item first (FIFO within a level), with a floor that parks items at or below it."

    def __init__(self,
        key=None, # `key(item)` returns the item's priority (default 0); higher is served first
        gate=None, # `gate(item)` sees each item as it leaves the channel, in arrival order; returning False consumes it
        **kw):
        super().__init__(**kw)
        self.key = key or (lambda item: 0)
        self.gate = gate
        self._heap, self._seq, self._floor = [], 0, None

    @property
    def floor(self): return self._floor

    @floor.setter
    def floor(self, v):
        "Yield only items with priority strictly above `v`; None lifts the floor. Set from the receive loop's own thread."
        self._floor = v
        self.send.send_nowait(_poke)  # wake a parked `get`; drops silently if closed

    def _push(self, item):
        if item is _poke: return
        if self.gate is not None and not self.gate(item): return
        self._seq += 1
        heapq.heappush(self._heap, (-self.key(item), self._seq, item))

    def _pop(self):
        if not self._heap: return _nothing
        prio = -self._heap[0][0]
        if self._floor is not None and prio <= self._floor: return _nothing
        return heapq.heappop(self._heap)[2]

    async def get(self):
        while True:
            for item in self.receive.drain_nowait(): self._push(item)
            if (got := self._pop()) is not _nothing: return got
            self._push(await self.receive.receive())

    def drain_nowait(self, max_items: int | None = None)->list:
        "Drain queued items in priority order, ignoring the floor: draining is for teardown and aborts."
        for item in self.receive.drain_nowait(): self._push(item)
        out = []
        while self._heap and (max_items is None or len(out) < max_items): out.append(heapq.heappop(self._heap)[2])
        return out

    def __aiter__(self): return self

    async def __anext__(self):
        try: return await self.get()
        except EndOfStream: raise StopAsyncIteration from None

class ActorCore:
    "Serialized async handler loop over a Mailbox."
    def __init__(
        self,
        handler,  # `handler(item)`, or `handler(item, release)` when `concurrent`
        *,
        mailbox: Mailbox | None = None,
        concurrent: bool = False # run each item as a task; `release()` lets the next item start early
    ):
        self.handler = handler
        self.concurrent = concurrent
        self.mailbox = mailbox or Mailbox()

    def bind(self, loop=None): self.mailbox.bind(loop)

    def submit(self, item): return self.mailbox.submit(item)

    def close(self)->bool: return self.mailbox.close()

    def fail(self, exc: BaseException)->bool: return self.mailbox.fail(exc)

    def drain_nowait(self, max_items: int | None = None)->list: return self.mailbox.drain_nowait(max_items=max_items)

    async def run(self, *, bind: bool = True):
        if bind: self.bind()
        if not self.concurrent:
            async for item in self.mailbox: await self.handle(item)
            return
        async with create_task_group() as tg:
            async for item in self.mailbox: await tg.start(self._handle_release, item)

    async def _handle_release(self, item, *, task_status):
        "Run `handler(item, release)`; the next item is processed once `release()` is called or the handler returns."
        def release():
            if not task_status.started_called: task_status.started()
        try:
            res = self.handler(item, release)
            if inspect.isawaitable(res): await res
        finally: release()

    async def handle(self, item):
        res = self.handler(item)
        if inspect.isawaitable(res): await res
