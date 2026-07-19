---
"@effect-grpc/effect-grpc": minor
---

OpenTelemetry-aligned tracing and metrics for clients and servers.

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
- `tracestate` pass-through for all four method kinds: servers rehydrate an
  incoming `tracestate` header into the handler's context under the new
  exported `GrpcTracing.TraceState` reference (and attach it to the
  `ExternalSpan` parent's annotations); clients resolve the outgoing header
  from caller metadata first, then span-ancestry annotations, then the
  ambient reference, alongside the injected `traceparent`.

See the new [observability guide](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/observability.md)
for the full list of span/metric names and attributes and how to hook up an
OTLP exporter.
