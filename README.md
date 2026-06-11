# microio

`microio` is a tiny asyncio-first runtime helper library for services that own
event loops, sockets, background threads, and request/reply waiters.

It is inspired by AnyIO's practical concurrency ideas, especially the problems
called out in [Why you should be using AnyIO APIs instead of asyncio APIs][anyio-why]:

- **task readiness**: a child service should be able to report "ready" or "failed"
  before its parent continues;
- **cancel scopes**: stopping is a durable state with a reason, not a one-shot flag
  that individual operations may miss;
- **memory object streams**: producers and consumers should be split into explicit
  sender/receiver endpoints with clear close semantics;
- **thread bridges**: code outside an event-loop thread needs a safe way to submit
  work into that loop and observe failures.

`microio` is not a compatibility layer over asyncio, Trio, and Curio. It is also
not a reimplementation of AnyIO. It intentionally stays smaller:

- asyncio only;
- stdlib only;
- no generic networking/file APIs;
- cooperative level cancellation only where code uses `microio` scopes and checkpoints;
- no pytest plugin or framework-level dependency injection.

The goal is to make the common "small service runtime" patterns reliable and
testable without pulling a full concurrency abstraction into projects that already
use asyncio directly.

## What It Provides

### `TaskGroup` / `CancelScope`

`create_task_group()` wraps `asyncio.TaskGroup`. It keeps the stdlib failure
rules, and adds the missing cancellation/readiness pieces:

- `tg.start_soon(fn, *args)` starts a child task;
- `await tg.start(fn, *args)` starts a child and waits until it calls
  `task_status.started(value)`;
- `tg.cancel_scope.cancel()` or `tg.cancel()` cancels owned tasks and treats
  that as normal shutdown;
- `checkpoint()`, `checkpoint_if_cancelled()`, and `sleep()` provide cooperative
  level cancellation for code that uses `microio` primitives;
- `move_on_after(seconds)` suppresses deadline cancellation;
- `fail_after(seconds)` turns deadline cancellation into `TimeoutError`.

The group-cancel path borrows the small `asyncio_cancel_scope` trick: when a
child task or another thread asks a group to stop, `microio` injects a private
task exception into the underlying `asyncio.TaskGroup` and suppresses just that
private exception on exit.

This is still asyncio cancellation. Raw `await something()` follows asyncio's
edge-cancellation rules. Once code returns to a `microio` checkpoint, cancelled
scopes keep raising `CancelledError`, even if earlier cancellation was caught.

Shielding is not exposed. A partial shield around raw `Task.cancel()` would look
stronger than it is.

```python
from microio import create_task_group, sleep


async def worker():
    while True: await sleep(1)


async with create_task_group() as tg:
    tg.start_soon(worker)
    await sleep(0.1)
    tg.cancel()
```

### `CloseScope`

`CloseScope` is a small, thread-safe stop/failure state object. It records whether
a service is closing, why it is closing, and whether there is an exception that
should be propagated to waiters.

This is separate from `CancelScope`. `CloseScope` is for thread-safe service
lifecycle state. It does not cancel asyncio tasks for you.

### `ServiceThread` / `ServiceGroup`

`ServiceThread` is a supervised `threading.Thread`:

- child code calls `started()` after resources are ready;
- parents call `wait_started()` and get either readiness or the startup exception;
- `stop()` marks the thread's `CloseScope`;
- `join_or_log()` checks timeout results instead of ignoring them.

Use it for socket threads, protocol readers, and other owned background services.

`ServiceGroup` owns the repeated lifecycle boilerplate for a small set of service
threads:

```python
services = ServiceGroup(iopub, stdin, heartbeat).start().wait_started()
...
services.stop_join(timeout=1)
```

### `LoopServiceThread`

`LoopServiceThread` owns an `asyncio.Runner` inside a thread and exposes:

- `call_soon()` for thread-safe callbacks;
- `call_sync()` for thread-safe callbacks with a return value;
- `submit()` for coroutine submission from other threads;
- `task_group` for async work owned by the service;
- the same ready/failed/stop/join behavior as `ServiceThread`.

This is the small subset of AnyIO's thread-bridge idea that asyncio services often
need: create one loop in one thread, keep ownership clear, submit coroutine work
safely, and synchronously run small functions on the loop thread when needed.
`stop()` cancels the service task group, so owned child tasks shut down with the
service.

### `ObjectChannel`

`create_channel()` returns `(send, receive)` endpoints. A sender can be used from
other threads before or after the receiver has bound to an event loop. The receiver
is async and supports `async for`.

This is inspired by AnyIO memory object streams, but adjusted for service threads:

- the default buffer is unbounded because cross-thread producers often cannot
  await backpressure;
- close is explicit and wakes async receivers;
- receivers raise `EndOfStream` on direct receive after close;
- `fail(exc)` is explicit and wakes async receivers with the exception;
- late sends raise `ClosedResourceError` unless `late_send="drop"` is selected;
- the implementation is intentionally single-receiver and simple.

### `Mailbox` / `ActorCore`

`Mailbox` wraps an `ObjectChannel` for the common actor shape: thread-safe
`submit()`, async receive, `close()`, `fail()`, and `drain_nowait()`.

`ActorCore` is the tiny serialized consumer loop:

```python
actor = ActorCore(handle)
actor.submit(item)
await actor.run()
```

It is deliberately not tied to a thread. A service thread, a main-thread runner,
or a test can all run the same actor core.

### `RequestRegistry`

`RequestRegistry` tracks request IDs and waiters:

- register a request;
- resolve it from another thread through a `ReplyHandle`;
- wait with timeout;
- wrap the common register-send-wait pattern with `request(key, send)`;
- fail one or all pending requests on service crash/close.

This is useful for debug adapters, stdin routers, RPC clients, and any protocol
where a reader thread must wake request waiters reliably.

## Example

```python
import asyncio
from microio import LoopServiceThread, create_channel


class Worker(LoopServiceThread):
    def __init__(self):
        super().__init__(name="worker")
        self.send, self.receive = create_channel()

    async def run_async(self):
        self.receive.bind(asyncio.get_running_loop())
        self.started()
        async for item in self.receive:
            if item == "stop":
                self.stop()
                break
            print(item)


worker = Worker()
worker.start()
worker.wait_started()
worker.send.send_nowait("hello")
worker.send.send_nowait("stop")
worker.join_or_log(timeout=1)
```

## Design Rules

- Prefer explicit state over hidden magic.
- Make startup failure visible to the parent.
- Never ignore a join timeout.
- Waking pending waiters on close/crash is part of the service contract.
- Keep asyncio ownership clear: a socket or loop belongs to one service thread.

## Development

`microio` requires Python 3.11+.

```bash
pip install -e .[dev]
pytest -q
```

## Examples

Run the counter service example:

```bash
python examples/counter_server.py
```

It shows `LoopServiceThread`, `ObjectChannel`, `RequestRegistry`, and
`CloseScope` working together in one small service.

Version lives in `microio/__init__.py` as `__version__`.

[anyio-why]: https://anyio.readthedocs.io/en/stable/why.html
