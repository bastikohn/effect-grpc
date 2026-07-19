# @effect-grpc/effect-grpc

## 1.0.0-beta.3

### Minor Changes

- 713c9b6: Add `GrpcHealth`: the standard gRPC Health Checking Protocol
  (`grpc.health.v1.Health`), so load balancers, Kubernetes probes, and
  `grpc_health_probe` work out of the box.

  - `GrpcHealth.service` is a ready-made entry for `GrpcNodeServer.serveAll`
    that registers the `Health` service (`Check` unary, `Watch`
    server-streaming) next to the application services.
  - `GrpcHealth.layer()` provides the backing per-service status map. It marks
    the overall server (the empty-string service name) as `SERVING` by default;
    `initialStatuses` overrides that. Applications flip statuses through the
    `GrpcHealth.GrpcHealth` service: `set`, `clear`, `check`, `watch`, and a
    `statuses` snapshot.
  - Semantics follow the spec: `Check` returns the current status and fails
    with `not_found` for unknown services; `Watch` immediately streams the
    current status — `SERVICE_UNKNOWN` for unknown services — followed by one
    element per status change (consecutive duplicates are suppressed).
  - `GrpcHealth.HealthClient`/`HealthClientLayer` provide a client for probing
    remote servers, shaped like generated clients; `HealthGrpcRegistry` plugs
    into `GrpcClientProtocol.layer`.

- 976a7fa: Add `GrpcReflection`: the standard gRPC Server Reflection Protocol
  (`grpc.reflection.v1.ServerReflection`, plus the legacy `v1alpha` alias), so
  `grpcurl`, `grpcui`, Postman, and similar tools work against the server
  without local `.proto` files.

  - `GrpcReflection.service(services)` is a ready-made entry for
    `GrpcNodeServer.serveAll`; pass it the same services array you pass to
    `serveAll`. It answers reflection queries from the descriptors the
    generated registries already carry, so no extra codegen or runtime `.proto`
    loading is involved, and it describes itself and the health service too.
  - Semantics follow the spec: `list_services`, `file_by_filename`,
    `file_containing_symbol`, `file_containing_extension`, and
    `all_extension_numbers_of_type` are all implemented; file answers include
    the requested descriptor followed by its transitive imports; unknown names
    produce an in-band `NOT_FOUND` error response that echoes the original
    request instead of failing the stream.
  - The same handler serves both the `v1` and `v1alpha` service names — older
    tools that only probe `v1alpha` work unchanged.
  - `GrpcReflection.ReflectionClient`/`ReflectionClientLayer` provide a client
    for querying remote reflection services, shaped like generated clients;
    `ReflectionGrpcRegistry` plugs into `GrpcClientProtocol.layer`.
  - `GrpcReflection.makeIndex`/`respond` expose the pure lookup layer for
    advanced composition and testing.

- 904056d: OpenTelemetry-aligned tracing and metrics for clients and servers.

  - Span attributes keep following the current OTel RPC semantic conventions:
    `rpc.system.name` (`"grpc"`), the fully qualified `rpc.method`
    (`demo.v1.UserService/GetUser`), and the string `rpc.response.status_code`
    (`"OK"`, `"NOT_FOUND"`, ...), with `error.type` (same status name) when
    the call is an error.
  - Error classification is asymmetric per semconv: client spans/metrics treat
    every non-`OK` status as an error; server spans/metrics only the
    server-fault codes (`UNKNOWN`, `DEADLINE_EXCEEDED`, `UNIMPLEMENTED`,
    `INTERNAL`, `UNAVAILABLE`, `DATA_LOSS`). Other server outcomes record
    their status but close the span cleanly, so client-caused conditions
    (`NOT_FOUND`, `CANCELLED`, ...) do not pollute server error rates.
  - New zero-config RPC metrics via Effect `Metric`:
    `rpc.client.call.duration` and `rpc.server.call.duration` histograms
    (unit `s`, OTel-recommended boundaries), recorded for all four method
    kinds — including failure and cancellation paths — and tagged with
    `rpc.system.name`, `rpc.method`, `rpc.response.status_code`, `error.type`
    per the classification above, plus `server.address`/`server.port` on the
    client (`server.port` is a string on metrics — Effect metric attributes
    are string-only — deviating from the semconv's integer type). Exporting
    stays the app's concern (any Effect Tracer/Metric exporter works).
  - Client bidi streams that the consumer closes early (e.g. `Stream.take`)
    now record `CANCELLED` instead of `OK`.
  - Client spans end in an error state for every non-`OK` outcome, including
    cancellation via fiber interruption or an early stream close — previously
    those closed with interrupt/success exits that exporters map to OTel `OK`,
    contradicting the recorded `CANCELLED` attributes.
  - `tracestate` pass-through for all four method kinds: servers rehydrate an
    incoming `tracestate` header into the handler's context under the new
    exported `GrpcTracing.TraceState` reference (and attach it to the
    `ExternalSpan` parent's annotations); clients resolve the outgoing header
    from caller metadata first, then span-ancestry annotations, then the
    ambient reference, alongside the injected `traceparent`. Per W3C trace
    context, a `tracestate` arriving without a valid W3C `traceparent` (e.g.
    on a B3-only request) is discarded.

  See the new [observability guide](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/observability.md)
  for the full list of span/metric names and attributes and how to hook up an
  OTLP exporter.

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
