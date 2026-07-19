# Observability

`effect-grpc` instruments every call — client and server, all four method
kinds — with Effect's built-in `Tracer` and `Metric` APIs, following the
OpenTelemetry semantic conventions for RPC. The library itself stays
exporter-agnostic: it never depends on `@opentelemetry/*` packages. You decide
where spans and metrics go by wiring an exporter in your app (see
[Exporting to OpenTelemetry](#exporting-to-opentelemetry)).

Everything below is on by default and zero-config. Without an exporter the
spans and metrics simply stay inside the Effect runtime; installing one later
requires no changes to your gRPC wiring.

## Spans

Span names follow the semconv `$service/$method` form, e.g.
`demo.v1.UserService/GetUser`.

| Span               | Kind     | When                                                                              |
| ------------------ | -------- | --------------------------------------------------------------------------------- |
| `$service/$method` | `client` | One per outgoing call, opened when the call starts and ended at the final status. |
| `$service/$method` | `server` | One per incoming call, parented to the caller's `traceparent` when present.       |

Attributes on both client and server spans:

| Attribute                  | Type   | Example                                          |
| -------------------------- | ------ | ------------------------------------------------ |
| `rpc.system.name`          | string | `grpc`                                           |
| `rpc.method`               | string | `demo.v1.UserService/GetUser`                    |
| `rpc.response.status_code` | string | `OK`, `NOT_FOUND`, ...                           |
| `error.type`               | string | `NOT_FOUND` — only set when the call is an error |

Client spans additionally carry `server.address` (string) and `server.port`
(number), derived from the client's `baseUrl` (override with `serverAddress`
on `GrpcClientProtocol.layer`).

Error classification follows the semconv's asymmetric rule: **client** spans
treat every non-`OK` status as an error, while **server** spans treat only
the server-fault codes `UNKNOWN`, `DEADLINE_EXCEEDED`, `UNIMPLEMENTED`,
`INTERNAL`, `UNAVAILABLE`, and `DATA_LOSS` as errors. Other server outcomes
(`NOT_FOUND`, `CANCELLED`, validation failures, ...) record their
`rpc.response.status_code` but set no `error.type` and end the span cleanly,
so client-caused conditions do not pollute server error rates.

The classification also drives the exported span status: a client span ends
in an error state for every non-`OK` outcome — including cancellation via
fiber interruption or an early stream close, whose natural Effect exits
(interrupt, success) exporters would otherwise map to OTel `OK`.

For streaming methods the span covers the whole call: it opens when the call
starts and records the status when the stream reaches its final state —
including failures mid-stream and client cancellation
(`rpc.response.status_code` = `CANCELLED`; `error.type` = `CANCELLED` on the
client span only, since cancellation is not a server fault).

## Metrics

Durations are recorded with Effect `Metric.histogram` in **seconds**, using
the OTel-recommended boundaries
(`0.005 … 10`). One observation per call, from call start to final status —
error and cancellation paths included.

| Metric                     | Instrument          | Recorded by         |
| -------------------------- | ------------------- | ------------------- |
| `rpc.client.call.duration` | histogram (seconds) | every outgoing call |
| `rpc.server.call.duration` | histogram (seconds) | every incoming call |

Both instruments carry a constant `unit` attribute of `"s"`; Effect's OTLP
exporter reads it to set the metric unit, and the Prometheus exporter skips
it as a label.

Tags (metric attributes):

| Attribute                  | On     | Example                                          |
| -------------------------- | ------ | ------------------------------------------------ |
| `rpc.system.name`          | both   | `grpc`                                           |
| `rpc.method`               | both   | `demo.v1.UserService/GetUser`                    |
| `rpc.response.status_code` | both   | `OK`, `NOT_FOUND`, ...                           |
| `error.type`               | both   | `NOT_FOUND` — only set when the call is an error |
| `server.address`           | client | `api.example.com`                                |
| `server.port`              | client | `"8443"` (stringified number)                    |

`error.type` follows the same asymmetric classification as spans: clients tag
every non-`OK` call, servers only the server-fault subset.

Known deviation: the semconv types `server.port` as an **integer**, but
Effect's metric attributes are string-only, so it is exported as a string.
Span attributes are unaffected (`server.port` is a number there). If your
backend enforces attribute types, drop or cast the label in your collector.

The two `*.call.duration` histograms are the complete set of current semconv
RPC metrics; the per-message counters and payload-size histograms defined by
earlier semconv versions are deprecated and deliberately not emitted.

## Context propagation

- **`traceparent`** — the client injects a W3C `traceparent` header derived
  from the current span into every outgoing call; a caller-provided
  `traceparent` metadata entry always wins, and noop spans (tracing disabled)
  inject nothing. The server parses incoming `traceparent` (W3C first, then
  B3) and parents its span to the resulting `ExternalSpan`.
- **`tracestate`** — Effect's tracer model has no native `tracestate` field,
  so the library threads it through the request context under the exported
  `GrpcTracing.TraceState` reference: the server rehydrates an incoming
  `tracestate` header into the handler's context — only when a valid **W3C**
  `traceparent` accompanies it; per W3C trace context, a `tracestate` on a
  request carrying only B3 headers is discarded (the span is still parented
  via B3) — and the client resolves the outgoing header in order —
  caller-provided `tracestate` metadata first, then a `TraceState` span
  annotation found in the span ancestry, then the ambient reference from the
  calling fiber's context. Net effect: `tracestate` passes through services
  built with this library (server in, client out) for all four method kinds.
  Note that exporters (e.g. `@effect/opentelemetry`) do not read this value,
  so it is propagated but not attached to exported span data.
- **W3C baggage** is not propagated: Effect's tracer model has no baggage
  concept to source it from or deliver it into. Forward baggage explicitly as
  metadata if you need it.

To read the raw `tracestate` in a handler, read the reference from the
context:

```ts
import { Effect } from "effect";
import { GrpcTracing } from "@effect-grpc/effect-grpc";

Effect.gen(function* () {
  // `string | undefined` — undefined when the caller sent no tracestate.
  const state = yield* Effect.service(GrpcTracing.TraceState);
});
```

## Exporting to OpenTelemetry

Spans and metrics are recorded through Effect's `Tracer`/`Metric` services, so
any Effect-native exporter picks them up without further configuration. Effect
v4 ships OTLP exporters in core — no extra packages needed:

```ts
import { Layer } from "effect";
import * as FetchHttpClient from "effect/unstable/http/FetchHttpClient";
import * as Otlp from "effect/unstable/observability/Otlp";
import * as OtlpSerialization from "effect/unstable/observability/OtlpSerialization";

const ObservabilityLayer = Otlp.layer({
  baseUrl: "http://localhost:4318",
  resource: { serviceName: "my-service" },
}).pipe(Layer.provide([OtlpSerialization.layerJson, FetchHttpClient.layer]));
```

Provide `ObservabilityLayer` alongside your gRPC client/server layers and all
`rpc.*` spans, duration histograms, and logs flow to your OTLP collector. Any
other setup that provides Effect's `Tracer` service (for example
`@effect/opentelemetry/Tracer` bridging to an OpenTelemetry SDK tracer) or
polls `Metric.snapshot` (for example
`effect/unstable/observability/PrometheusMetrics`) works the same way.
