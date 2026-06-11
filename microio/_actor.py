import inspect

from ._channel import create_channel


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

    def __init__(self, handler, *, mailbox: Mailbox | None = None):
        self.handler = handler
        self.mailbox = mailbox or Mailbox()

    def bind(self, loop=None): self.mailbox.bind(loop)

    def submit(self, item): return self.mailbox.submit(item)

    def close(self)->bool: return self.mailbox.close()

    def fail(self, exc: BaseException)->bool: return self.mailbox.fail(exc)

    def drain_nowait(self, max_items: int | None = None)->list: return self.mailbox.drain_nowait(max_items=max_items)

    async def run(self, *, bind: bool = True):
        if bind: self.bind()
        async for item in self.mailbox: await self.handle(item)

    async def handle(self, item):
        res = self.handler(item)
        if inspect.isawaitable(res): await res
