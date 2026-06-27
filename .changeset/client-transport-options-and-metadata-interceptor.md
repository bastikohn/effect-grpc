---
"@effect-grpc/effect-grpc": minor
---

Invert client transport construction and add an Effect-native metadata interceptor.

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
