# @effect-grpc/protoc-gen-effect-grpc

## 1.0.0-beta.4

### Minor Changes

- cfec645: Generated files compile cleanly under `noUnusedLocals`/`noUnusedParameters` and
  can no longer be shadowed by legal proto names: base64, oneof, well-known, and
  `Empty` converters moved into a `Grpc$` namespace, unused bare `type` aliases
  and helper emissions are dropped, and file usage is derived once in a single
  analysis the renderers consume. Regenerate to pick up the renamed converters.
- 60b8093: Breaking: generated service handlers are a plain `Effect`, not a `Layer`.

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

- cfec645: Breaking: generated clients and server handlers no longer run on Effect RPC.
  All four method kinds now go through the `GrpcInvoker` seam (`layerConnect` for
  the wire, `layerInMemory` for socket-free tests) and the unified
  `GrpcServerProtocol.GrpcHandlers` map. `GrpcMethodRegistry` owns tag lookup,
  merging, and the four domain/wire conversions. Regenerate your protos.

  Migration:

  - `GrpcNodeServer.ServeAllService` loses its `group` field — drop `group:` from
    every `serveAll` call site.
  - `GrpcServerProtocol.GrpcStreamingHandlers`/`streamingHandlersLayer` are
    replaced by the unified `GrpcHandlers` map.
  - `GrpcClientProtocol.GrpcStreamingClient` is removed — provide a `GrpcInvoker`
    (`layerConnect`, or `layerInMemory` for network-free tests) to run generated
    clients.
  - `GrpcClientProtocol.layer`/`layerFromTransport` no longer provide
    `RpcClient.Protocol`; migrate hand-built `RpcClient.make(...)` callers to the
    invoker.

- eb0a26b: Breaking: the public surface shrinks to what is actually load-bearing.

  - The generator drops the inert `errors`, `int64` and `methods` options; only
    `import_extension` remains (now honoured, including `ts`). Remove the other
    `opt:` entries from `buf.gen.yaml`.
  - `tracestate` is no longer propagated and `GrpcTracing` is gone; `traceparent`
    propagation and span parenting are unchanged.
  - `GrpcReflection` request/response schemas use the generated `{case, value}`
    oneof representation, `fileDescriptorProto` is `Uint8Array`, and the legacy
    `grpc.reflection.v1alpha` alias is no longer registered.
  - `GrpcHealth.make`/`layer` are nullary values, and `GrpcHealthService.statuses`
    plus `GrpcHealthOptions.initialStatuses` are removed.
  - The `@effect-grpc/codegen` package is discontinued and will not be published
    again — it was a second front-end to this generator. `buf generate` with the
    `protoc-gen-effect-grpc` plugin is the supported path; see the README for the
    `buf.yaml`/`buf.gen.yaml` recipe.

- 4ace7a5: Take the identity of a well-known method input/output from the protobuf
  descriptor instead of the name the generator printed for it. A message merely
  _named_ `GrpcGoogleProtobufTimestamp` was read back as `google.protobuf.Timestamp`
  whenever it appeared as a method input or output: the file declared
  `GrpcGoogleProtobufTimestampSchema` and its type twice and did not compile
  (TS2451/TS2300). The same lookup routed a `GrpcGoogleProtobufBoolValue` message
  through the wrapper's `{ value }` boxing converter and a `GrpcGoogleProtobufEmpty`
  message through the Empty converter, which dropped every field — the wrong
  converter, alongside the duplicate-declaration error. Method types now carry
  their descriptor's well-known kind through the model, so no naming decision can
  be mistaken for an identity one.

  **Generated well-known method types move into the `Grpc$` namespace.** The
  converters were namespaced already; their schema and type now join them, so
  `GrpcGoogleProtobufTimestamp` becomes `Grpc$GoogleProtobufTimestamp` (likewise
  `Duration`, the `*Value` wrappers, and `Empty`). `$` is legal in TypeScript
  identifiers but never in protobuf ones, so the generator's names are now
  unreachable from a `.proto` by construction, and a message of your own can share
  a file with the well-known it is named after. Regenerate, and rename any handler
  or client signature that spells one of these types.

## 1.0.0-beta.3

## 1.0.0-beta.2

### Patch Changes

- 28be2ee: Improve npm package metadata and READMEs: add `keywords`, `homepage`, and
  `author` to all published packages, and rewrite the per-package READMEs to be
  standalone (install instructions, quickstart, absolute documentation links) so
  they render usefully on npmjs.com.

## 1.0.0-beta.1

### Patch Changes

- 3afea71: Fix generated converters for messages with no fields. Empty messages now emit
  `_message`/`_value` parameters and omit the dead `const message = value as …`
  local, matching the well-known `Empty` handling. Previously the non-underscore
  forms were always emitted, tripping `noUnusedParameters`/`noUnusedLocals` in
  consumers with stricter tsconfigs.

## 1.0.0-beta.0

### Major Changes

- 8a1c3aa: Target stable Effect v4 for the 1.0 line.

### Minor Changes

- 0440966: Add client-streaming and bidi-streaming support via a direct streaming bridge.

  The Effect RPC wire protocol has no client-to-server stream, so the two new
  method kinds bypass `RpcClient`/`RpcServer` and bridge `Stream` <->
  `AsyncIterable` directly over the same connect transport and registry. Unary
  and server-streaming methods are unchanged.

  Generated clients gain per-kind signatures — client-streaming
  `(requests: Stream<I, E>, options?) => Effect<O, ClientError | E>` and bidi
  `(requests: Stream<I, E>, options?) => Stream<O, ClientError | E>` — served by
  the new `GrpcClientProtocol.GrpcStreamingClient`, which
  `layer`/`layerFromTransport` now provide alongside `RpcClient.Protocol`.
  Generated implementations extend symmetrically with
  `(requests: Stream<I, GrpcStatusError>, context)` handlers; the generated
  `*HandlersLayer` publishes them through the new
  `GrpcServerProtocol.GrpcStreamingHandlers` context key, so `serveAll` wiring is
  unchanged for users.

  Semantics: interrupting the returned `Effect`/`Stream` cancels the call; if the
  request stream fails, the call is cancelled and the caller sees the original
  error while the server observes `cancelled`; request-stream completion
  half-closes the call; streamed messages are decoded/encoded per message with
  the generated schemas. Effect RPC middleware does not apply to the direct
  streaming path.

  Breaking: `GrpcMethodEntry` gains a required `successSchema` (regenerate your
  protos), and the codegen option `ignore_unsupported_methods` is removed — all
  four gRPC method kinds are now supported and `methods` defaults to all of them.

- 94b398b: Make `GrpcStatusError` a schema-backed `Schema.TaggedErrorClass` and use the
  class itself as the generated RPC error schema. The parallel `schema` struct,
  the duck-typed `isGrpcStatusError`, and `fromEncoded` are removed. Generated
  clients now decode failures into real `GrpcStatusError` instances, discriminated
  by their `_tag` (e.g. `Effect.catchTag("GrpcStatusError", …)`), and the
  `<Service>ClientError` alias collapses to `GrpcStatusError | RpcClientError`.

  Breaking: remove uses of `GrpcStatusError.schema`, `GrpcStatusError.fromEncoded`,
  and `GrpcStatusError.isGrpcStatusError`; discriminate failures by
  `_tag === "GrpcStatusError"` (or `Effect.catchTag`) instead.

- 94b398b: Expand the supported proto matrix: nested messages and nested enums
  (generated with protobuf-es-style `Outer_Inner` names), cross-package
  imported messages (including in repeated, map, oneof, and method
  input/output positions), imported and repeated enum fields, `optional`
  scalar and enum fields with presence preserved as `undefined`, and
  64-bit integers as `bigint` by default (`int64=bigint` is now the
  default and remains accepted as an explicit option).
