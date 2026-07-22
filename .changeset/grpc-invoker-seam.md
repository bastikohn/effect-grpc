---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcInvoker` — a four-shape invocation seam (`unary`, `serverStream`, `clientStream`, `bidiStream`) between callers and the gRPC transport, with two adapters: `GrpcInvoker.layerConnect` invokes over a connect transport in production, and `GrpcInvoker.layerInMemory` dispatches to in-process handlers for deterministic tests — no sockets, protobuf descriptors, or HTTP/2. The in-memory adapter enforces the same invocation semantics as the wire: unknown or kind-mismatched tags fail with `unimplemented`, a failed request stream replays the caller's original error while the handler observes `cancelled`, and `timeoutMs` bounds unary and client-streaming calls with `deadline_exceeded`.

`GrpcClientProtocol.layerFromTransport` / `layer` now additionally provide `GrpcInvoker`, and the invoker's connect adapter also implements the unary and server-streaming shapes in preparation for generated clients depending on the invoker alone.
