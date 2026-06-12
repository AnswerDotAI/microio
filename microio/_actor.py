import inspect

from ._channel import create_channel
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


class ActorCore:
    "Serialized async handler loop over a Mailbox."

    def __init__(self, handler,  # `handler(item)`, or `handler(item, release)` when `concurrent`
        *, mailbox: Mailbox | None = None,
        concurrent: bool = False):  # run each item as a task; `release()` lets the next item start early
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
