# microio examples

## Counter server

Run from the `microio` project directory:

```bash
python examples/counter_server.py
```

The example starts a tiny in-process service. The main thread sends request
objects through an `ObjectChannel` to a `LoopServiceThread`, waits for replies
with a `RequestRegistry`, and shuts the service down through its `CloseScope`.
