---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: the Effect RPC server protocol path is retired. The server no longer
runs `RpcServer`/`RpcGroup`/`Rpc` from `effect/unstable/rpc` — all four call
shapes (unary, server-streaming, client-streaming, bidi-streaming) now execute
on the direct connect bridge behind one unified handler seam,
`GrpcServerProtocol.GrpcHandlers` (a 4-kind handler map published by
`GrpcServerProtocol.handlersLayer`, which replaces
`GrpcStreamingHandlers`/`streamingHandlersLayer`).

Breaking API changes:

- `GrpcNodeServer.ServeAllService` loses its `group` field — drop `group:`
  from every `serveAll` call site. `GrpcServerProtocol.make` takes
  `{ registry, handlers }` and returns `{ routes }` only (no
  `RpcServer.Protocol` service, no `Scope` requirement).
- `CodegenSupport.GrpcServerContext` narrows to `{ metadata }` — the
  fabricated Effect RPC `client`/`requestId` fields are removed.
  `CodegenSupport.serverContext` now builds the context from request headers.
- `GrpcHealth` drops `Health_CheckRpc`/`Health_WatchRpc`/`HealthRpcGroup`/
  `HealthRpcs`; the health handlers are plain entries in the unified map.
  `GrpcReflection.service` likewise no longer carries an RPC group.
- Generated code no longer emits `Rpc.make` consts, `*RpcGroup`, or `*Rpcs`
  types, and no longer imports `effect/unstable/rpc` at all — regenerate your
  protos. The documented surface (`*Client`, `*Implementation`,
  `*HandlersLayer`, `*GrpcRegistry`) is unchanged, and `*Implementation`
  handler signatures are identical: user handler code is untouched.
- Methods present in a registry without a registered handler now fail with
  `unimplemented` (message `Missing handler for <tag>`) for every call shape.

Behavior notes: `GrpcMethodRegistry` is now the sole codec authority for both
directions with the same error policy as before (`invalid_argument` on request
decode, `internal` on response encode). Server-streaming responses are now
pulled through the same pull-based response pump as bidi-streaming, so
backpressure follows HTTP/2 flow control instead of a fixed 16-slot buffer,
and cancellation interrupts the handler fiber directly. OTel semconv spans,
status attributes, duration metrics, and tracestate propagation are preserved
across all four shapes.
