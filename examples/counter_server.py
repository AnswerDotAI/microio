import itertools
from dataclasses import dataclass
from microio import EndOfStream, LoopServiceThread, ReplyHandle, RequestRegistry, create_channel, sleep


@dataclass
class Request:
    reply: ReplyHandle
    op: str
    value: int = 0
    def resolve(self, value)->bool: return self.reply.resolve(value)
    def fail(self, exc: BaseException)->bool: return self.reply.fail(exc)


class CounterServer(LoopServiceThread):
    "Tiny in-process server showing the main microio pieces together."
    def __init__(self):
        super().__init__(name="counter-server")
        # ObjectChannel splits producer and consumer endpoints. The main thread
        # owns `send`; the service loop owns `receive`.
        self.send, self.receive = create_channel()
        # RequestRegistry maps request ids to blocking waiters in the caller.
        # The service resolves or fails each waiter when work finishes.
        self.requests = RequestRegistry()
        self.ids = itertools.count(1)
        self.value = 0

    def request(self, op: str, value: int = 0, timeout: float = 2):
        "Send one request to the service loop and wait for its reply."
        req_id = next(self.ids)
        return self.requests.request(req_id, lambda reply: self.send.send_nowait(Request(reply, op, value)), timeout=timeout)

    async def run_async(self):
        # LoopServiceThread created this asyncio loop in a background thread.
        # Binding the receive endpoint lets other threads wake this loop safely.
        self.receive.bind()
        self.started()
        try:
            while not self.scope.closed:
                try: req = await self.receive.receive()
                except EndOfStream: break
                await self.dispatch(req)
        finally:
            # If the service exits while callers are waiting, wake them all.
            self.requests.fail_all(RuntimeError("counter server stopped"))

    async def dispatch(self, req: Request):
        # Handlers return values or raise. This wrapper owns reply plumbing, so
        # business logic does not repeat resolve/fail boilerplate.
        try: req.resolve(await self.handle(req))
        except BaseException as exc: req.fail(exc)

    async def handle(self, req: Request):
        if req.op == "add":
            await sleep(0.05)
            self.value += req.value
            return self.value
        if req.op == "get": return self.value
        raise ValueError(f"unknown operation: {req.op}")

    def stop(self, reason: str | None = "stop")->bool:
        # CloseScope lives at `self.scope`. `LoopServiceThread.stop()` marks it
        # closed and cancels the service task; closing the channel wakes receive().
        first = super().stop(reason)
        self.send.close()
        return first


def main():
    server = CounterServer()
    print("starting server")
    server.start()
    server.wait_started(timeout=2)

    print("add 3 ->", server.request("add", 3))
    print("add 7 ->", server.request("add", 7))
    print("get ->", server.request("get"))

    # call_sync runs a regular function on the service loop thread and returns
    # its result to this thread. Useful for small loop-owned inspections.
    print("loop-thread peek ->", server.call_sync(lambda: server.value, timeout=2))

    try: server.request("missing")
    except ValueError as exc: print("bad request ->", exc)

    server.stop()
    print("closed scope ->", server.scope.closed)
    print("joined ->", server.join_or_log(timeout=2))


if __name__ == "__main__": main()
