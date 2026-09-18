---
"@effect-grpc/effect-grpc": minor
---

Native server interceptors and a per-call handler context.

`GrpcNodeServer.serve` and `serveAll` take `interceptors`, an array of
connect `Interceptor`s installed once on the node adapter and run once per
RPC for every routed service. `CodegenSupport.GrpcServerContext` — the second
argument of every generated handler — now carries, next to `metadata`, the
method identity (`method.tag`/`method.kind`), connect's live call `signal`,
a live `remainingTimeoutMs()` read (`undefined` without a deadline, `0` once
it has elapsed), and `getContextValue(key)` for typed values interceptors
attach with `request.contextValues.set`. See
[docs/users/server-context.md](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/server-context.md).

**Compatibility.** Generated handler signatures are unchanged and handlers
that only read `metadata` keep compiling. The new fields are required on the
context the server delivers, so handwritten fixtures that build a
`GrpcServerContext` literal must now supply `method`, `signal`,
`remainingTimeoutMs` and `getContextValue`. `GrpcInvoker.layerInMemory` is
unaffected: its handlers receive `GrpcInMemoryCall`, and it runs no server
interceptors.
