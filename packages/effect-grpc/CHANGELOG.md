# @effect-grpc/effect-grpc

## 1.0.0-beta.4

### Minor Changes

- 43eb2b3: Two call-option semantics that diverged between the connect and in-memory
  `GrpcInvoker` adapters:

  - **Binary _call_ metadata is symmetric and keyed off the `-bin` suffix.** The
    suffix is a peer's only signal that a header carries bytes, so it — not the
    JavaScript type — now drives both directions: `GrpcMetadata.toHeaders`
    base64-encodes `-bin` values, and `GrpcMetadata.fromHeaders` decodes them
    back to `Uint8Array`. A binary value previously reached the server (the sole
    receive path, via `CodegenSupport.serverContext`) as the base64 string it was
    encoded to, so it never round-tripped to its declared type — and bytes under
    a key _without_ the suffix were silently base64'd into a header the peer
    could not identify as binary. Call metadata that contradicts its key —
    `Uint8Array` under an ASCII key, `string` under a `-bin` key — now fails with
    `invalid_argument` from the shared validator, so both adapters (and the
    `metadataInterceptor`) reject it identically instead of one throwing and the
    other silently accepting. The same validator also rejects keys and values no
    header can spell (`"bad key"`, `""`, `"ünicode"`, `"x:a"`, a value containing
    CR/LF), which previously reached `Headers.append` and died as an untyped
    `TypeError`. `GrpcMetadata.isBinaryKey` is exported alongside. Repeated
    `-bin` headers, which `Headers.entries()` joins with `", "`, are split back
    into one entry per value; ASCII values are deliberately left whole, since a
    comma is legal inside one.
  - **A non-positive `timeoutMs` uniformly means _no deadline_.** The in-memory
    adapter already treated `<= 0` that way while the connect adapter forwarded
    the value to a transport, where `createDeadlineSignal` aborts a `<= 0`
    timeout the instant the call starts. (connect's own transports happen to
    clamp the value before that point, so this was latent there and observable
    only through a bare `Transport`.) Normalization now lives next to the shared
    metadata validator and the connect adapter omits the option entirely, so the
    semantic no longer depends on a transport's own clamping.
    `GrpcInMemoryCall.timeoutMs` is likewise absent for a non-positive input:
    the handler's view must match the deadline actually in force, which on the
    wire is no `grpc-timeout` header at all.

  Two consequences worth calling out:

  - **`layerInMemory` now normalizes call metadata exactly as the wire does**,
    by routing it through the same codec. A handler observes lowercased keys in
    alphabetical order, with repeated ASCII keys collapsed into one comma-joined
    value (`[["x-a","one"],["x-a","two"]]` arrives as `[["x-a","one, two"]]`) —
    which is what a real server sees, and the point of the two adapters
    promising identical semantics.
  - **`GrpcStatusError.metadata` decoded from a peer now yields `Uint8Array` for
    `-bin` keys** where it previously yielded the base64 string. This is
    user-visible on `grpc-status-details-bin`, which connect does not strip: it
    appears in `error.metadata` (now bytes) as well as, decoded, in
    `error.details`.

  The `-bin` policy covers _call_ metadata only. `GrpcStatusError` metadata is
  still encoded best-effort by `toConnectError` and is neither validated nor
  normalized, so an entry contradicting its key silently changes type in transit
  in either direction. Validating there would mean throwing while serializing an
  error, swallowing the original failure; the caveat is documented on the field
  instead.

- 06ed4ca: Add `GrpcInvoker` — a four-shape invocation seam (`unary`, `serverStream`, `clientStream`, `bidiStream`) between callers and the gRPC transport, with two adapters: `GrpcInvoker.layerConnect` invokes over a connect transport in production, and `GrpcInvoker.layerInMemory` dispatches to in-process handlers for deterministic tests — no sockets, protobuf descriptors, or HTTP/2. The in-memory adapter enforces the same invocation semantics as the wire: unknown or kind-mismatched tags fail with `unimplemented`, a failed request stream replays the caller's original error while the handler observes `cancelled`, and `timeoutMs` bounds unary and client-streaming calls with `deadline_exceeded`.

  `GrpcClientProtocol.layerFromTransport` / `layer` now additionally provide `GrpcInvoker`, and the invoker's connect adapter also implements the unary and server-streaming shapes in preparation for generated clients depending on the invoker alone.

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

- bb59a6c: Deepen the `GrpcMethodRegistry` contract: the module now owns tag lookup with cardinality validation (`lookup`), registry merging with the duplicate-tag construction invariant (`merge`), grouping by service descriptor (`groupByService`), and the four domain/wire conversions with one normalized error policy — request-payload problems fail with `invalid_argument`, response-payload problems with `internal` (`encodeRequest`/`decodeRequest`/`encodeResponse`/`decodeResponse`, backed by per-entry codecs cached separately per direction). The client invoker's four call shapes, the server's direct streaming path, and `GrpcNodeServer` no longer reimplement lookup, kind checks, codec preparation, conversion-error mapping, or duplicate detection; `internal/codec.ts` is absorbed into the registry module. The server's Effect RPC native path (unary/server-streaming) keeps its own throw-style conversion wrappers — it operates on already-encoded payloads inside the `RpcServer` message flow — but now caches its per-entry payload validator instead of rebuilding it on every request.
- 73671ba: Breaking: the Effect RPC client protocol path is retired. `GrpcClientProtocol`
  no longer implements or provides `RpcClient.Protocol` — the internal `make`
  that translated `RpcClient.Protocol` requests into connect calls is removed,
  and `GrpcClientProtocol.layer` / `layerFromTransport` narrow from
  `Layer.Layer<RpcClient.Protocol | GrpcInvoker.GrpcInvoker>` to
  `Layer.Layer<GrpcInvoker.GrpcInvoker>`.

  `GrpcInvoker` is now the single client-side seam: generated clients (and the
  built-in health/reflection clients) already resolve every call shape through
  it since the previous release, so providing `GrpcClientProtocol.layer(...)` —
  or `GrpcInvoker.layerConnect` / `GrpcInvoker.layerInMemory` directly — keeps
  working unchanged. Only code that consumed `RpcClient.Protocol` from these
  layers directly (e.g. hand-built `RpcClient.make(...)` clients) is affected;
  migrate such callers to the invoker.

  The now-dead `CodegenSupport.headersFromOptions` re-export and the internal
  `x-effect-grpc-timeout-ms` header writer are removed alongside the client
  protocol path; `CodegenSupport.GrpcCallOptions.timeoutMs` remains and is still
  honored — the invoker passes it through to connect `CallOptions`.

  Everything else in `GrpcClientProtocol` is unchanged: `makeTransport`,
  `metadataInterceptor`, the TLS options (`GrpcClientTlsOptions`),
  `GrpcClientTransportOptions`, `GrpcClientProtocolOptions`,
  `GrpcClientProtocolTransportOptions`, and the re-exported
  `GrpcTransportOptions`.

  Generated code is unaffected — no regeneration required. The generated
  `Rpc.make` / `RpcGroup.make` / `*Rpcs` exports remain because the server path
  (`GrpcServerProtocol` / `RpcServer` handler layers) still consumes them.

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

- 4e5403c: Tighten `GrpcStatusError` to the shape the gRPC wire actually supports:

  - **`code` can no longer be `"ok"`.** A handler failing with `code: "ok"` made
    both server and client record an OK span and an OK duration metric while
    `toConnectCode` still sent `UNKNOWN` to the peer — success telemetry for a
    call the caller saw fail. `code` (and the `make()` option) is now
    `GrpcStatusCode.GrpcErrorStatusCode`, the `"ok"`-free subset, backed by
    `GrpcStatusCode.errorSchema`. The full `GrpcStatusCode` union stays as a type
    for telemetry, which legitimately reports successful outcomes; both unions
    are derived from one literal list, and `fromConnectCode` now returns the
    narrow type (connect's `Code` has no `OK` member). `GrpcStatusCode.schema`,
    the wide schema, is removed — nothing decodes a status code that may be
    `"ok"`.
  - **`trailers` is gone.** It was dead API: connect exposes exactly one metadata
    channel for an error, `toConnectError` merged `metadata` and `trailers` into
    it, and `fromConnectError` always produced an empty `trailers`. Pass
    everything as `metadata` — under the gRPC protocol it is written to, and read
    back from, the response trailers.

  Breaking: drop `trailers` from `GrpcStatusError` construction, use a concrete
  failure code instead of `"ok"`, and replace `GrpcStatusCode.schema` with
  `GrpcStatusCode.errorSchema`.

### Patch Changes

- 2a9fc2b: **Overlapping pulls in the stream bridge no longer duplicate messages or leak a
  fiber past teardown.** The pull machine behind both pumps retains a single pull
  fiber, so a `next()` issued while another was in flight forked a second pull and
  overwrote that slot: `close()` then interrupted only the last fiber, and the
  abandoned pull — with its interrupt cleanup — stayed pending forever. The
  overlapping pulls also raced the shared pull and chunk iterator, so callers
  could see the same element more than once and lose others — three concurrent
  `next()` calls on a one-element stream that then fails all returned that same
  element, and the failure never surfaced at all. Pulls are
  now serialized behind a promise chain, which `close()` bypasses so teardown
  still interrupts the pull in flight instead of queueing behind it. No transport
  path reached this today, because connect-node drives both iterators
  sequentially.
- 2c1c6b1: **Client telemetry no longer drops `server.port` for default ports.** WHATWG
  `URL` normalizes a scheme's default port away, so `new URL("https://api.example.com:443").port`
  is the empty string — client spans and `rpc.client.call.duration` omitted the
  semconv `server.port` attribute for every `https://` endpoint on 443 and every
  `http://` endpoint on 80, including ones that spelled the port out explicitly.
  The port now falls back to the scheme's default (443 / 80); only a
  `serverAddress` override on some other scheme, which has no default to derive,
  still reports `server.address` alone.
- 7342d79: Centralize the Effect `Stream` <-> connect `AsyncIterable` streaming lifecycle in one internal bridge module shared by the client and server protocols. Half-close vs. cancellation detection, iterator `return`/`throw` behavior, source-failure replay, and outcome-preserving cleanup now have a single implementation and test surface.

  Also fixes a potential hang: a bidi call abandoned by the client while the server had a response pull in flight could leave connect's generator loop waiting forever, since closing the response iterator releases the handler's resources but never settles the in-flight pull.

- a2e87d8: Three fixes from an adversarial review of the stream bridge and server signal
  handling:

  - **Server no longer hangs when a streaming handler abandons the request
    stream mid-pull.** connect's request iterable queues a `return()` issued
    while a `next()` is pending until that pull settles, so a client-streaming
    or bidi handler that stopped consuming (an `Effect.timeout`, a race) while
    the client was connected but idle blocked its own teardown — the call never
    terminated and the server could not enforce its own timeout. Request-stream
    cleanup is now issued without being awaited; the abandoned pull belongs to
    connect's own call teardown.
  - **Client request pumps interrupt an in-flight pull on close.** Ending a
    client-streaming or bidi call while the request stream was awaiting its
    next element previously abandoned that pull fiber without interruption: one
    leaked fiber per call, and the stream's interrupt cleanup never ran. Both
    pumps now share one pull machine that owns its fibers and interrupts a pull
    in flight before close resolves.
  - **Deadline expiry surfaces as `DEADLINE_EXCEEDED`, not `CANCELLED`.**
    connect-node enforces the incoming `grpc-timeout` by aborting the handler
    signal with a `deadline_exceeded` reason; the server previously collapsed
    every abort into `cancelled`, sending the wrong status for unary and
    client-streaming calls and — since `deadline_exceeded` is in the semconv
    server-fault set — hiding deadline expiries from server error telemetry.
    Aborts are now mapped through the signal's reason on every call shape, and
    `GrpcStatusError.deadlineExceeded` is exported alongside the other
    constructors.

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
