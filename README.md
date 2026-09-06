# microio

Small, dependency-free tools for programs that mix threads and asyncio. They handle data transfer, cancellation, failure reporting, and service startup and shutdown across the thread/event-loop boundary.

## The problem

A program may run an event loop in one thread while a socket reader, a control channel, or the main thread needs to:

- Send work to the loop from another thread.
- Cancel that work while keeping the loop running and leaving unrelated work unaffected.
- Block waiting for an answer, and wake with an error if the service fails.
- Check that the service started successfully and that it shut down when asked.

The standard library provides `call_soon_threadsafe`, `run_coroutine_threadsafe`, `Thread`, and `Queue`. Combining them requires handling startup, cancellation, failure, and shutdown across threads. Mistakes can cause deadlocks, dropped messages, threads that never exit, or a service that stops responding while its process keeps running.

[Trio](https://trio.readthedocs.io/en/stable/reference-core.html#getting-back-into-the-trio-thread-from-another-thread) and [AnyIO](https://anyio.readthedocs.io/en/stable/threads.html) provide structured concurrency and APIs for calling into their event loops from external threads. AnyIO's blocking portals can also own a loop in a background thread and let synchronous callers start tasks, wait for readiness, and cancel them.

microio focuses on asyncio programs that need thread-callable channels and cancellation scopes, supervised service threads, and request/reply bookkeeping. These primitives can be used individually with an existing asyncio loop. The implementation is about 950 lines, uses only the standard library, and requires Python 3.11+.

microio was extracted from a Jupyter kernel. A protocol thread fed an execution loop, Ctrl-C needed to cancel a coroutine on another thread, and clients could disconnect while a request was waiting for a reply. The same coordination problems occur in other threaded services.

## Tools

### Sending data between threads and an event loop

- `create_channel()` provides a sender usable from any thread, even before the loop exists, and an async receiver supporting `async for`. Closing or failing the channel wakes the receiver.
- `Mailbox` wraps the channel with thread-safe `submit()` and async receive. `ActorCore` adds one-at-a-time async message handling.

### Cancellation and shutdown state

- `CancelScope` provides Trio-style cancellation scopes for asyncio, cancellable from any thread. It catches cancellation of its own region at scope exit and retracts issued-but-undelivered cancellation so it does not affect subsequent code.
- `ScopeGroup` tracks cancellable regions entered with `scope()`. Calling `cancel()` from another thread cancels all registered regions. With `latch=True`, it also cancels regions entered later, until `clear()` is called, covering work that starts just after the cancellation request.
- `CloseScope` records thread-safe close/failure state and its reason. Only the first close changes the state.
- `WorkTracker` is a WaitGroup-style counter for in-flight work. Its `busy` Event is set while work is in progress; any thread can check it or wait for it to become set.

### Waiting for replies from another thread

- `RequestRegistry` handles request/reply bookkeeping: register a request, block with a timeout, and resolve it from the reader thread. Call `fail_all()` when the connection dies to wake all pending waiters with the exception.

### Managing service threads

- `ServiceThread` supervises a thread's lifecycle. The service reports readiness with `started()`; startup failures reach the parent through `wait_started()`, with the original exception as the cause. `stop()` records a persistent stop request, and `join_or_log()` logs a join timeout.
- `LoopServiceThread` is a `ServiceThread` that owns an `asyncio.Runner`. It accepts `submit(coro)` and `call_sync(fn)` from other threads and manages shutdown of its child tasks.
- `ServiceGroup` starts, waits for readiness, stops, and joins a set of services.

### Structured concurrency within the event loop

- `TaskGroup` wraps `asyncio.TaskGroup` with `start_soon`, readiness reporting through `await tg.start(...)` and `task_status.started()`, and group cancellation callable from other threads.
- `move_on_after` and `fail_after` provide timeout scopes. `checkpoint` and `sleep` check for cancellation as well as yielding to the loop.

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

### A background event loop with checked startup and shutdown

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

The parent waits for an explicit readiness report before submitting work. Startup failures retain the original traceback, and a thread that fails to stop is reported by `join_or_log()`.

### Cancelling async work from another thread

```python
from microio import ScopeGroup, sleep

scopes = ScopeGroup()

async def job():
    with scopes.scope() as scope:              # registers a cancellable region
        await do_work()
    if scope.cancelled_caught: print("interrupted; cleaning up")

# from another thread:
scopes.cancel("user interrupt", latch=True)    # also cancels jobs that enter a scope later
```

Each scope catches its cancellation at the end of the `with` block. The task can then send an error reply or release resources and continue running. Tasks outside the group are unaffected. Call `scopes.clear()` when new jobs should be allowed to run again.

`ScopeGroup` reads its registry without a lock, but cancellation acquires locks in the individual scopes. Do not call it directly from a signal handler that must avoid locks. Instead, schedule the cancellation on the running loop with [`loop.call_soon_threadsafe()`](https://docs.python.org/3/library/asyncio-eventloop.html#asyncio.loop.call_soon_threadsafe). Close and stop operations also acquire locks; thread safety does not imply signal-handler safety.

### Serialized message handling with per-message concurrency

```python
from microio import ActorCore

async def handle(msg): await process(msg)      # one at a time, in arrival order

actor = ActorCore(handle)
actor.submit(msg)                              # thread-safe, from anywhere
await actor.run()                              # in the loop that owns the actor
```

With `concurrent=True`, each handler receives a `release` callback. Calling it allows the next message to start before the current handler finishes, so handlers can preserve ordering during preparation and overlap slow I/O:

```python
async def handle(msg, release):
    prepare(msg)            # this part stays strictly ordered
    release()               # allows the next message to start
    await slow_io(msg)      # yields so the next handler can run

actor = ActorCore(handle, concurrent=True)
```

If a handler never calls `release()`, the next message waits for that handler to finish. Enabling `concurrent=True` lets each handler choose when to permit overlap.

### Waking pending requesters when a connection fails

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

- Startup errors reach the parent with their traceback, and join timeouts are logged. Reader threads use `fail_all()` to wake pending requesters with the failure exception.
- Close state persists so later operations can observe it. `CloseScope` retains the reason or failure exception, and repeated close calls leave the original state unchanged.
- Cancellation, close, and stop can be requested from other threads. Signal handlers must defer operations that acquire locks, as described in the cancellation example.
- Each loop, socket, or receiver belongs to one thread. Other threads communicate with it through these primitives.

## Scope and limitations

- microio supports asyncio only. It has no networking or file APIs, no cancellation shielding, and its channels have a single receiver. If your whole program is async, use AnyIO — it's excellent, and microio's scope/readiness design borrows directly from [its ideas][anyio-why].
- Raw `await`s follow asyncio's edge-triggered cancellation rules. microio's `checkpoint()` and `sleep()` add level-triggered behavior where used: they check for cancellation on each call, even if an earlier cancellation exception was caught.

## Development

```bash
pip install -e .[dev]
pytest -q
```

The version is defined in `microio/__init__.py` as `__version__`.

[anyio-why]: https://anyio.readthedocs.io/en/stable/why.html
