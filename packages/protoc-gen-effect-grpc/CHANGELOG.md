# @effect-grpc/protoc-gen-effect-grpc

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
