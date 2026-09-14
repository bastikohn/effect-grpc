---
"@effect-grpc/effect-grpc": minor
---

Keep failures inside Effect at the library's boundaries. `GrpcStatusError` is
a `Schema.TaggedError` (with `GrpcStatusCode.GrpcErrorStatusCodeSchema` and
`GrpcMetadata.GrpcMetadataSchema` describing its fields), so it can be encoded
and decoded like any other schema class. `GrpcClientProtocol.makeTransport`
returns an `Effect<Transport>` that dies on a contradictory `tls` block instead
of throwing when the layer is described, and `metadataInterceptor` fails the
call with `invalid_argument` — the same status the per-call path reports — when
resolved metadata cannot go on the wire. `GrpcNodeServer.serve` binds through
`Effect.callback`; a bind failure is a defect as before, now raised through the
runtime rather than a rejected promise. `GrpcReflection.service` accepts
`ServeAllService<unknown>` rather than `any`.

Breaking: `makeTransport` returns an `Effect`; `yield*` it (or use
`GrpcClientProtocol.layer`, which is unchanged).
