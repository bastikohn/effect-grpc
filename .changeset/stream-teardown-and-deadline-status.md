---
"@effect-grpc/effect-grpc": patch
---

Three fixes from an adversarial review of the stream bridge and server signal
handling:

- **Server no longer hangs when a streaming handler abandons the request
  stream mid-pull.** connect's request iterable queues a `return()` issued
  while a `next()` is pending until that pull settles, so a client-streaming
  or bidi handler that stopped consuming (an `Effect.timeout`, a race) while
  the client was connected but idle blocked its own teardown — the call never
  terminated and the server could not enforce its own timeout. Request-stream
  cleanup is now issued without being awaited; the abandoned pull belongs to
  connect's own call teardown.
- **Client request pumps interrupt an in-flight pull on close.** Ending a
  client-streaming or bidi call while the request stream was awaiting its
  next element previously abandoned that pull fiber without interruption: one
  leaked fiber per call, and the stream's interrupt cleanup never ran. Both
  pumps now share one pull machine that owns its fibers and interrupts a pull
  in flight before close resolves.
- **Deadline expiry surfaces as `DEADLINE_EXCEEDED`, not `CANCELLED`.**
  connect-node enforces the incoming `grpc-timeout` by aborting the handler
  signal with a `deadline_exceeded` reason; the server previously collapsed
  every abort into `cancelled`, sending the wrong status for unary and
  client-streaming calls and — since `deadline_exceeded` is in the semconv
  server-fault set — hiding deadline expiries from server error telemetry.
  Aborts are now mapped through the signal's reason on every call shape, and
  `GrpcStatusError.deadlineExceeded` is exported alongside the other
  constructors.
