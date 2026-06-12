# microio

Small, dependency-free tools for programs that mix **threads and asyncio** — where data, cancellation, and failure have to cross the thread/event-loop boundary without races, hangs, or silent loss.

## The problem

Real programs are rarely purely async. A typical shape: one thread owns an event loop doing the interesting work, while other threads — a socket reader, a control channel, the main thread, sometimes a *signal handler* — need to:

- **feed it work** (without touching the loop from the wrong thread),
- **cancel work it's doing** (without killing the loop, and without the cancellation leaking somewhere unrelated),
- **block waiting for an answer** from it (and get woken with an error, not hang forever, if it dies),
- **know it started up properly**, and **know it actually shut down**.

The stdlib gives you the raw ingredients — `call_soon_threadsafe`, `run_coroutine_threadsafe`, `Thread`, `Queue` — and leaves all of the above as an exercise. That exercise is where deadlocks, dropped messages, zombie threads, and "it stopped responding but the process looks fine" bugs live.

[Trio](https://trio.readthedocs.io/) and [AnyIO](https://anyio.readthedocs.io/) solve concurrency beautifully, but *inside* one async world: they assume the code in control is itself async. When the thing doing the cancelling is another thread — or a SIGINT handler that must not take any locks — you're back on your own.

microio is that missing layer: ~800 lines, stdlib only, asyncio only, Python 3.11+.

## What's in the box

**Move data across the boundary**

- `create_channel()` — a sender usable from any thread (even before the loop exists), an async receiver with `async for`, and explicit close/fail semantics that *wake* the receiver rather than strand it.
- `Mailbox` / `ActorCore` — the channel wrapped into the common actor shape: thread-safe `submit()`, one-at-a-time async handling.

**Move control across the boundary**

- `CancelScope` — trio-style cancellation scopes for asyncio, cancellable **from any thread**. A scope that cancels its own region cleanly catches the cancellation at its exit; an issued-but-undelivered cancellation is retracted, never leaked into unrelated code.
- `ScopeGroup` — a live registry of cancellable regions: enter with `scope()`, cancel them *all* from anywhere with `cancel()`. The `latch` option also cancels regions entered just after the cancel — closing the classic check-then-act race. Lock-free reads, so it's safe to call from a signal handler.
- `CloseScope` — thread-safe "we are stopping, here's why" state, closable exactly once.
- `WorkTracker` — a WaitGroup: in-flight work counter with a `busy` Event any thread can check or wait on.

**Wait across the boundary**

- `RequestRegistry` — request/reply bookkeeping between threads: register, block with timeout, resolve from the reader thread, and — the part hand-rolled versions always miss — `fail_all()` so that when the connection dies, every blocked waiter gets the exception instead of hanging forever.

**Own your threads properly**

- `ServiceThread` — a supervised thread: it reports `started()` or its parent's `wait_started()` raises the real startup exception; `stop()` is durable state, not a flag a loop might miss; `join_or_log()` never silently ignores a join timeout.
- `LoopServiceThread` — a `ServiceThread` that owns an `asyncio.Runner`: `submit(coro)` and `call_sync(fn)` from any thread, structured shutdown of its child tasks.
- `ServiceGroup` — start/wait/stop/join a set of services without boilerplate.

**Structured async (the in-loop part)**

- `TaskGroup` (wrapping `asyncio.TaskGroup`) with `start_soon`, `await tg.start(...)`/`task_status.started()` readiness, and group cancellation that works from other threads; `move_on_after`, `fail_after`, `checkpoint`, `sleep`.

## Examples

### A thread feeding an event loop

```python
import asyncio, threading
from microio import create_channel

send, recv = create_channel()

def producer():                          # any thread, no loop required
    for i in range(5): send.send_nowait(i)
    send.close()                         # wakes the receiver; the async-for ends

async def main():
    threading.Thread(target=producer).start()
    async for item in recv: print(item)

asyncio.run(main())
```

### A background thread that owns a loop — with checked startup and shutdown

```python
from microio import LoopServiceThread, sleep

class Service(LoopServiceThread):
    async def run_async(self):
        self.db = await connect()              # resources live on the loop thread
        self.started()                         # parent's wait_started() returns now
        while not self.scope.closed: await sleep(0.1)

svc = Service(name="db-service")
svc.start()
svc.wait_started(timeout=5)                    # raises the real traceback if connect() failed
fut = svc.submit(svc.db.query("..."))          # run a coroutine on the service loop, from any thread
rows = fut.result(timeout=5)
svc.stop()
svc.join_or_log(timeout=2)                     # a join timeout is logged, never swallowed
```

Half of debugging multithreaded programs is finding the thread that died quietly at startup, or never exited at shutdown. `ServiceThread` makes both loud.

### Cancelling async work from another thread (or a signal handler)

```python
from microio import ScopeGroup, sleep

scopes = ScopeGroup()

async def job():
    with scopes.scope() as scope:              # registers a cancellable region
        await do_work()
    if scope.cancelled_caught: print("interrupted; cleaning up")

# meanwhile, from ANY other thread — or a SIGINT handler (no locks taken):
scopes.cancel("user interrupt", latch=True)    # latch also catches a job that is *just* starting
```

The cancellation lands inside the `with` block and is caught at its exit — the task survives, follow-up code (sending an error reply, releasing resources) still runs, and nothing leaks to other tasks.

### Serialized message handling, with an escape hatch

```python
from microio import ActorCore

async def handle(msg): await process(msg)      # one at a time, in arrival order

actor = ActorCore(handle)
actor.submit(msg)                              # thread-safe, from anywhere
await actor.run()                              # in the loop that owns the actor
```

When a handler needs to let the queue keep moving while it waits on something slow, `concurrent=True` hands each handler a release baton:

```python
async def handle(msg, release):
    prepare(msg)            # this part stays strictly ordered
    release()               # from here on, the next message may start...
    await slow_io(msg)      # ...it actually runs whenever this one suspends

actor = ActorCore(handle, concurrent=True)
```

Handlers that never call `release()` behave exactly like the serialized actor — ordering is opt-out per message, not a global mode.

### Request/reply that can't hang

```python
from microio import RequestRegistry

reg = RequestRegistry()

# requesting thread: register, send, block for the answer
reply = reg.request(msg_id, send=lambda h: sock.send(payload), timeout=10)

# reader thread, when the response arrives:
reg.resolve(msg_id, response)

# reader thread, when the connection dies:
reg.fail_all(ConnectionError("reader died"))   # every blocked requester raises instead of hanging
```

### Everything together

[`examples/counter_server.py`](examples/counter_server.py) is a complete ~90-line in-process server combining `LoopServiceThread`, channels, `RequestRegistry`, and `CloseScope`:

```bash
python examples/counter_server.py
```

## Design rules

- **Failures are loud.** Startup errors reach the parent with their traceback; join timeouts are logged; dead readers wake their waiters with the real exception.
- **Closing is a durable state with a reason**, not a one-shot flag an operation might miss. Everything closes exactly once.
- **Control may come from anywhere.** Cancellation, close, and stop are safe from other threads, and the read paths are lock-free so they're safe from signal handlers.
- **Ownership is explicit.** A loop, socket, or receiver belongs to one thread; everyone else talks to it through these primitives.

## What microio is not

- Not an AnyIO replacement: asyncio only, no networking or file APIs, no shielding, single-receiver channels. If your whole program is async, use AnyIO — it's excellent, and microio's scope/readiness design borrows directly from [its ideas][anyio-why].
- Cancellation is still asyncio cancellation: raw `await`s follow asyncio's edge-triggered rules; microio's `checkpoint()`/`sleep()` add level-triggered behavior where you opt in.

microio was extracted from a Jupyter kernel, where all of these problems show up at once: a protocol thread feeding an execution loop, Ctrl-C arriving as a signal that must cancel a coroutine on another thread, and clients that disconnect while something is blocked waiting on them. The primitives are general; that's just the crucible they were forged in.

## Development

```bash
pip install -e .[dev]
pytest -q
```

Version lives in `microio/__init__.py` as `__version__`.

[anyio-why]: https://anyio.readthedocs.io/en/stable/why.html
