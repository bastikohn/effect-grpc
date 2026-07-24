# @effect-grpc/protoc-gen-effect-grpc

## 1.0.0-beta.4

### Minor Changes

- eb87335: Namespace the converters the generator introduces itself, so legal proto names can no longer shadow them. A file containing `message Bytes` emitted the base64 helper `const fromBytes` next to the message converter `export const fromBytes`, producing a file that could not compile (TS2451) — and a `from<Message>` body that recursed into itself. The oneof and well-known converters were the same class: `message Foo_barOneof` collided with the converter for `Foo`'s `bar` oneof, and `message GrpcGoogleProtobufTimestamp` collided with the Timestamp converter, which additionally routed the well-known field through the user's message converter — wrong output, not just a wrong name.

  Base64, oneof, well-known and Empty converters now share one `Grpc$` namespace (`fromGrpc$Bytes`, `fromGrpc$FeatureRequest_contactOneof`, `fromGrpc$GoogleProtobufTimestamp`). `$` is legal in TypeScript identifiers but never in protobuf ones, so no message name can reach them. Generated schema and type names are unchanged; the renamed well-known converters are exported when the type is a method input/output, so regenerate to pick them up.

- 952f396: Generated gRPC clients now depend on the `GrpcInvoker` seam alone. All four
  method kinds resolve through it — `unary` and `serverStream` (previously routed
  through `RpcClient`/`RpcClient.Protocol`) join `clientStream` and `bidiStream`
  — so the split across `RpcClient` and the `GrpcClientProtocol.GrpcStreamingClient`
  facade is gone. Generated code no longer imports `RpcClient`/`RpcClientError`,
  and the `<Service>ClientError` alias narrows to `GrpcStatusError` alone (the
  `RpcClientError` union member is dropped from the type and every method
  signature).

  Breaking: `GrpcClientProtocol.GrpcStreamingClient` (its service class,
  `GrpcStreamingClientService` interface, and the effect that built it) is
  removed; `GrpcClientProtocol.layer` / `layerFromTransport` no longer provide it
  (they still provide `RpcClient.Protocol` and `GrpcInvoker`). Provide a
  `GrpcInvoker` — `GrpcInvoker.layerConnect` in production, or
  `GrpcInvoker.layerInMemory` for network-free tests — to run generated clients;
  regenerate your protos to pick up the new client shape. The built-in
  `GrpcReflection.ReflectionClient` migrates to the invoker and its
  `ReflectionClientError` narrows to `GrpcStatusError` accordingly.

  Breaking: `GrpcHealth.HealthClient` likewise migrates to the invoker (provide a
  `GrpcInvoker` to run it), and its `HealthClientError` narrows to
  `GrpcStatusError` alone (the `RpcClientError` union member is dropped).

  Two further consequences of routing unary and server-streaming calls through
  the invoker:

  - These calls no longer emit the intermediate `RpcClient.<tag>` wrapper span;
    only the semconv gRPC client span remains (its attributes, status, and
    duration metric are unchanged). Anyone keying trace tooling on `RpcClient.*`
    span names for unary/server-streaming calls is affected.
  - On unary and server-streaming calls, an invalid request payload or a reserved
    `x-effect-grpc-*` metadata key now surfaces as a typed `GrpcStatusError`
    (`invalid_argument`) in the error channel instead of a defect.

- 437f0d7: Breaking: the Effect RPC server protocol path is retired. The server no longer
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

### Patch Changes

- 7b92908: Derive generated-file usage once in a single analysis (`fileUsage.ts`) that renderers consume instead of re-scanning the model — imports, helpers, method partitions, well-known usage, boxed wrappers, recursive edges, and empty-message facts now have one implementation. Fixes two unused-emission defects that could fail consumers compiling with `noUnusedLocals`: unary-only service files no longer import `Stream`, and files whose messages are all empty no longer emit the unused `readField`/`compact` helpers (empty-message converters now return `{}` directly).
- a74b509: Stop emitting unused bare `type` aliases in the cross-package import block.
  The import block previously always emitted `type <Message>` and `type <Enum>`
  for every imported name, but generated code only references the bare alias for
  enums used in a field position (from-converter `as <Enum>` casts) and for
  messages used as a method input/output (client/server signatures) — field-only
  imported messages are reached exclusively through their `Schema`/`from`/`to`
  symbols. The dead alias tripped `TS6133` in consumers compiling generated
  output with `noUnusedLocals`. The `fileUsage` analysis now records which
  imported bare types are actually referenced (`usedImportedTypes`) and the
  import block gates each alias on it; the generated-output typecheck fixtures
  and the proto example packages now compile with `noUnusedLocals` to lock the
  unused-emission defect class out.

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
