# @effect-grpc/effect-grpc

## 1.0.0-beta.2

### Patch Changes

- 28be2ee: Improve npm package metadata and READMEs: add `keywords`, `homepage`, and
  `author` to all published packages, and rewrite the per-package READMEs to be
  standalone (install instructions, quickstart, absolute documentation links) so
  they render usefully on npmjs.com.

## 1.0.0-beta.1

### Minor Changes

- c41387f: Add first-class TLS/mTLS support to the server and client.

  `GrpcNodeServer.serve`/`serveAll` accept a `tls` option (`key`, `cert`, both
  PEM) and terminate TLS via `http2.createSecureServer`. Setting `clientCa`
  enables mutual TLS: the handshake requires a client certificate signed by that
  CA and rejects connections without one.

  `GrpcClientProtocol.layer`/`makeTransport` accept a `tls` option that merges
  into connect-node's `nodeOptions`: `ca` sets the trust anchor for private CAs,
  `cert`/`key` present a client certificate for mTLS, and
  `rejectUnauthorized: false` disables server verification for development.
  `tls` requires an `https://` `baseUrl` and `cert`/`key` must be passed
  together — violations fail fast with a clear error. The raw `nodeOptions`
  escape hatch keeps working; `tls` wins for the keys it sets.

  TLS handshake failures surface to callers as `GrpcStatusError` with code
  `internal`, following connect-node's error mapping.

- 9768a86: Add `GrpcAuth`: first-class bearer-token authentication for clients.

  - `BearerToken` service tag decouples token producers from consumers: any
    layer that provides `{ read: Effect<string> }` can back the interceptor.
  - `bearerInterceptor` (and `bearerInterceptorFrom` for arbitrary token
    sources) attaches `authorization: Bearer <token>` to every outgoing call,
    re-reading the token per request so rotations apply immediately. Built on
    `metadataInterceptor`, so a per-call `authorization` header wins.
  - `staticTokenLayer` provides a fixed token.
  - `refreshingTokenLayer` acquires a token once, holds it in a `Ref`, and forks
    a scoped daemon that re-mints it on an interval. Refresh failures are logged
    and skipped (the previous token stays until the next tick); bake retries for
    transient failures into the `refresh` effect.

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
