---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: generated service handlers are a plain `Effect`, not a `Layer`.

`GrpcServerProtocol.handlersLayer` becomes `handlersEffect`, returning
`Effect<GrpcHandlers, never, R>` instead of `Layer<GrpcHandlers, never, R>`,
and `GrpcNodeServer.ServeAllService.handlers` follows. Nothing ever consumed
`GrpcHandlers` as a service, so the layer round trip only cost a build and a
context read per service. The `GrpcHandlers` context key is gone.

Regenerate your protos: the emitted `<Service>HandlersLayer` is now
`<Service>Handlers`, and `GrpcHealth.HealthHandlersLayer` is
`GrpcHealth.HealthHandlers`.

Where you provided handler dependencies with `Layer.provide(deps)`, provide
them to the whole server program — the `serveAll` effect — instead:

```ts
Effect.scoped(GrpcNodeServer.serveAll({ ... })).pipe(Effect.provide(DbLayer));
```

Do **not** provide them to the handlers effect. `Effect.provide` on a handlers
effect builds the layer in a scope that closes as soon as that effect
completes, which is immediately — so a scoped dependency is acquired,
released, and only then handed to `serveAll`, and every request runs against a
finalized resource with no error at the seam. Leaving the requirement
unprovided lets it propagate through `serveAll`, where the server's own scope
keeps finalizers running at shutdown.

Also breaking: the four `GrpcUnary/ServerStreaming/ClientStreaming/
BidiStreamingMethodEntry` interfaces collapse into one kind-parameterised
`GrpcMethodEntry<Kind, Request, Response>`; `GrpcStatusError` is a
`Data.TaggedError` (its schema, `GrpcStatusCode.errorSchema` and
`GrpcMetadata.schema` are gone), and `GrpcStatusError.unknown`,
`GrpcMethodRegistry.GrpcMethodEntryBase` and `CodegenSupport.serverContext`
are removed.
