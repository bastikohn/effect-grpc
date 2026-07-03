# @effect-grpc/effect-grpc

## 0.1.0

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

- ffc6e83: Invert client transport construction and add an Effect-native metadata interceptor.

  `GrpcClientProtocol.layer` now accepts the full connect-node `GrpcTransportOptions`
  (re-exported from the package) alongside `registry`, so TLS (`nodeOptions`),
  `interceptors`, compression, and `defaultTimeoutMs` are all configurable without
  depending on `@connectrpc/connect-node` directly. The transport is built through
  the new `makeTransport`, and `layerFromTransport` builds the protocol from an
  existing `Transport` — to share one transport across services, or inject a fake
  in tests. Span `server.address`/`server.port` now derive from `baseUrl` and can
  be overridden with the new `serverAddress` option.

  Add `GrpcClientProtocol.metadataInterceptor(resolve)`, which adapts an
  `Effect<GrpcMetadata, never, R>` into a connect `Interceptor`. It resolves once
  per request against the context captured when the interceptor is built — so a
  token rotated in a `Ref`/service stays current — and attaches headers as
  defaults: a header already present on the call (per-call `GrpcCallOptions.metadata`
  or the injected `traceparent`) is left untouched. Pass it via `interceptors`.

  Breaking: `GrpcClientProtocolOptions.baseUrl` is now a `string` (was `URL`); pass
  `url.toString().replace(/\/$/, "")` where you previously passed a `URL`.

- 94b398b: Make `GrpcStatusError` a schema-backed `Schema.TaggedErrorClass` and use the
  class itself as the generated RPC error schema. The parallel `schema` struct,
  the duck-typed `isGrpcStatusError`, and `fromEncoded` are removed. Generated
  clients now decode failures into real `GrpcStatusError` instances, discriminated
  by their `_tag` (e.g. `Effect.catchTag("GrpcStatusError", …)`), and the
  `<Service>ClientError` alias collapses to `GrpcStatusError | RpcClientError`.

  Breaking: remove uses of `GrpcStatusError.schema`, `GrpcStatusError.fromEncoded`,
  and `GrpcStatusError.isGrpcStatusError`; discriminate failures by
  `_tag === "GrpcStatusError"` (or `Effect.catchTag`) instead.
