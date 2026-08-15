import asyncio, time

import pytest

from microio import (ActorCore, PriorityMailbox, BrokenResourceError, CancelScope, ClosedResourceError, CloseScope, EndOfStream, LoopServiceThread, Mailbox, ReplyHandle,
    RequestRegistry, ScopeGroup, ServiceGroup, ServiceThread, WorkTracker, checkpoint, create_channel, create_task_group, fail_after, move_on_after, sleep)


def test_close_scope():
    scope = CloseScope()
    assert scope.close("done") is True
    assert scope.close("again") is False
    assert scope.closed
    with pytest.raises(ClosedResourceError): scope.raise_if_closed()

    scope = CloseScope()
    err = ValueError("boom")
    assert scope.fail(err) is True
    assert scope.failed
    with pytest.raises(BrokenResourceError): scope.raise_if_closed()


def test_request_registry():
    reg = RequestRegistry()
    waiter = reg.register("a")
    assert reg.resolve("a", {"ok": True}) is True
    assert reg.wait("a", waiter, timeout=0.1) == {"ok": True}
    assert len(reg) == 0

    waiter = reg.register("b")
    with pytest.raises(TimeoutError): reg.wait("b", waiter, timeout=0.01)
    assert len(reg) == 0

    waiter = reg.register("c")
    assert reg.fail_all(RuntimeError("closed")) == 1
    with pytest.raises(RuntimeError): reg.wait("c", waiter, timeout=0.1)

    waiter = reg.register("d")
    waiter.put("manual")
    assert reg.wait("d", waiter, timeout=0.1) == "manual"
    assert "d" not in reg

    assert reg.request("e", lambda reply: reply.resolve("wrapped"), timeout=0.1) == "wrapped"
    with pytest.raises(ValueError): reg.request("f", lambda reply: (_ for _ in ()).throw(ValueError("send failed")), timeout=0.1)
    assert "f" not in reg

    reply = reg.reply("g")
    assert isinstance(reply, ReplyHandle)
    assert reply.resolve("handled") is True
    assert reply.wait(timeout=0.1) == "handled"


def test_object_channel():
    send, recv = create_channel()
    send.send_nowait("before")
    send.send_nowait("before2")

    async def _run():
        recv.bind()
        assert recv.drain_nowait(max_items=1) == ["before"]
        assert await recv.receive() == "before2"
        send.send_nowait("after")
        assert await recv.receive() == "after"
        send.close()
        with pytest.raises(EndOfStream): await recv.receive()
        seen = []
        async for item in recv: seen.append(item)
        assert seen == []

    asyncio.run(_run())
    with pytest.raises(ClosedResourceError): send.send_nowait("late")

    send, recv = create_channel(late_send="drop")
    send.close()
    assert send.send_nowait("late") is None
    assert send.stats().dropped == 1

    async def _fails():
        send, recv = create_channel()
        send.fail(ValueError("closed badly"))
        assert recv.drain_nowait() == []
        with pytest.raises(ValueError): await recv.receive()

        send, recv = create_channel()
        send.send_nowait("kept")
        send.fail(ValueError("closed badly"))
        recv.bind()
        assert await recv.receive() == "kept"
        with pytest.raises(ValueError): await recv.receive()
    asyncio.run(_fails())


def test_mailbox_actor_core():
    async def _run():
        seen = []
        async def handle(item):
            await sleep(0)
            seen.append(item)
        actor = ActorCore(handle, mailbox=Mailbox())
        actor.submit("before-bind")
        task = asyncio.create_task(actor.run())
        actor.submit("after-bind")
        await sleep(0.01)
        assert actor.drain_nowait() == []
        assert actor.close()
        await task
        assert seen == ["before-bind", "after-bind"]
        assert actor.submit("late") is None

    asyncio.run(_run())


def test_task_group_cancel_scope():
    async def _run():
        events = []
        async def worker(label):
            try:
                while True:
                    events.append(("tick", label))
                    await sleep(0.01)
            finally: events.append(("stop", label))

        async with create_task_group() as tg:
            tg.start_soon(worker, "a")
            tg.start_soon(worker, "b")
            await sleep(0.02)
            assert tg.cancel()
        assert ("stop", "a") in events and ("stop", "b") in events

        async with create_task_group() as tg:
            async def service(*, task_status):
                task_status.started("ready")
                await sleep(1)
            assert await tg.start(service) == "ready"
            tg.cancel()

        with CancelScope() as scope:
            scope.cancel()
            await checkpoint()
            raise AssertionError("checkpoint should cancel")
        assert scope.cancelled_caught

        scope = CancelScope()
        scope.cancel()
        with scope: await asyncio.sleep(1)
        assert scope.cancelled_caught

        with move_on_after(0.01) as scope: await sleep(1)
        assert scope.cancelled_caught and scope.timed_out

        with pytest.raises(TimeoutError):
            with fail_after(0.01): await sleep(1)

        with pytest.raises(ExceptionGroup):
            async with create_task_group() as tg: tg.start_soon(lambda: (_ for _ in ()).throw(ValueError("boom")))

    asyncio.run(_run())


def test_service_thread():
    def target(svc):
        svc.started()
        while not svc.scope.closed: time.sleep(0.01)

    svc = ServiceThread(name="svc", target=target)
    group = ServiceGroup(svc).start().wait_started(timeout=1)
    assert group.stop_join(timeout=1)

    def fails(svc): raise ValueError("boom")

    svc = ServiceThread(name="bad-svc", target=fails)
    svc.start()
    with pytest.raises(RuntimeError): svc.wait_started(timeout=1)
    assert svc.exc is not None
    assert svc.join_or_log(timeout=1)


def test_loop_service_thread():
    class Worker(LoopServiceThread):
        async def run_async(self):
            self.events = []
            async def child():
                try:
                    while True: await sleep(0.01)
                finally: self.events.append("child stopped")
            self.task_group.start_soon(child)
            self.started()
            while not self.scope.closed: await sleep(0.01)

    worker = Worker(name="loop-worker")
    worker.start()
    worker.wait_started(timeout=1)
    assert worker.call_sync(lambda x: x + 1, 41, timeout=1) == 42
    fut = worker.submit(asyncio.sleep(0, result=42))
    assert fut.result(timeout=1) == 42
    assert worker.stop()
    assert worker.join_or_log(timeout=1)
    assert worker.events == ["child stopped"]


def test_actor_core_concurrent_release():
    async def main():
        order = []
        async def handler(item, release):
            order.append(("start", item))
            if item == 1:
                release()
                await asyncio.sleep(0.02)
            order.append(("end", item))
        actor = ActorCore(handler, concurrent=True)
        for i in (1, 2, 3): actor.submit(i)
        actor.close()
        await actor.run()
        assert order == [("start", 1), ("start", 2), ("end", 2), ("start", 3), ("end", 3), ("end", 1)]
    asyncio.run(main())


def test_actor_core_concurrent_serialized_without_release():
    async def main():
        order = []
        async def handler(item, release):
            order.append(("start", item))
            await asyncio.sleep(0.01)
            order.append(("end", item))
        actor = ActorCore(handler, concurrent=True)
        for i in (1, 2): actor.submit(i)
        actor.close()
        await actor.run()
        assert order == [("start", 1), ("end", 1), ("start", 2), ("end", 2)]
    asyncio.run(main())


def test_cancel_scope_enter_cancelled_without_checkpoint():
    "Entering an already-cancelled scope must not leak a pending cancellation past its exit."
    async def main():
        s = CancelScope()
        s.cancel("pre")
        with s: pass            # body never reaches a checkpoint
        await asyncio.sleep(0)  # must not raise CancelledError
    asyncio.run(main())


def test_scope_group():
    async def main():
        sg = ScopeGroup()
        assert not sg.active
        assert sg.cancel("nothing") is False

        results = []
        async def work(i):
            try:
                with sg.scope(): await sleep(10)
                results.append((i, "caught"))
            except asyncio.CancelledError: results.append((i, "cancelled"))

        async with create_task_group() as tg:
            tg.start_soon(work, 1)
            tg.start_soon(work, 2)
            await sleep(0.01)
            assert sg.active
            assert sg.cancel("stop", latch=True) is True
            with sg.scope() as s: assert s.cancelled  # latched: late entrants are cancelled on entry
            sg.clear()
            with sg.scope() as s: assert not s.cancelled
        assert sorted(results) == [(1, "caught"), (2, "caught")]
        assert not sg.active
    asyncio.run(main())


def test_work_tracker():
    wt = WorkTracker()
    assert not wt.busy.is_set() and wt.count == 0
    with wt.track():
        assert wt.busy.is_set() and wt.count == 1
        with wt.track(): assert wt.count == 2
        assert wt.busy.is_set()
    assert not wt.busy.is_set() and wt.count == 0
    wt.add()
    assert wt.busy.is_set() and wt.count == 1
    wt.done()
    assert not wt.busy.is_set() and wt.count == 0


def test_priority_mailbox():
    async def _run():
        mb = PriorityMailbox(key=lambda o: o[0])
        for o in [(0,'a'), (2,'b'), (1,'c'), (2,'d')]: mb.submit(o)
        await sleep(0)
        assert [await mb.get() for _ in range(4)] == [(2,'b'), (2,'d'), (1,'c'), (0,'a')]
        mb.floor = 0
        mb.submit((0,'parked'))
        mb.submit((1,'runs'))
        assert await mb.get() == (1,'runs')
        got = []
        async def getter(): got.append(await mb.get())
        t = asyncio.create_task(getter())
        await sleep(0.01)
        assert not got, "floored item must not be yielded"
        mb.floor = None
        await sleep(0.01)
        assert got == [(0,'parked')], "lifting the floor must wake the waiter"
        await t
        mb.floor = 0
        mb.submit((0,'stranded'))
        await sleep(0)
        assert mb.drain_nowait() == [(0,'stranded')], "drain ignores the floor"
        mb.close()

    asyncio.run(_run())


def test_priority_mailbox_actor():
    async def _run():
        seen = []
        async def handle(item, release):
            release()
            seen.append(item)
        actor = ActorCore(handle, mailbox=PriorityMailbox(key=lambda o: o[0]), concurrent=True)
        task = asyncio.create_task(actor.run())
        actor.submit((0,'a'))
        await sleep(0.01)  # (0,'a') starts; anything still queued can be jumped
        for o in [(0,'c'), (1,'b')]: actor.submit(o)
        await sleep(0.05)
        actor.close()
        await task
        assert seen[0] == (0,'a'), "first item starts before later submissions can jump it"
        assert seen[1:] == [(1,'b'), (0,'c')]

    asyncio.run(_run())
