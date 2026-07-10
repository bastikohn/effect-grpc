---
"@effect-grpc/effect-grpc": minor
---

OpenTelemetry-aligned tracing and metrics for clients and servers.

- Span attributes keep following the current OTel RPC semantic conventions:
  `rpc.system.name` (`"grpc"`), the fully qualified `rpc.method`
  (`demo.v1.UserService/GetUser`), and the string `rpc.response.status_code`
  (`"OK"`, `"NOT_FOUND"`, ...), with `error.type` (same status name) on
  failures.
- New zero-config RPC metrics via Effect `Metric`:
  `rpc.client.call.duration` and `rpc.server.call.duration` histograms
  (unit `s`, OTel-recommended boundaries), recorded for all four method
  kinds — including failure and cancellation paths — and tagged with
  `rpc.system.name`, `rpc.method`, `rpc.response.status_code`, `error.type`
  on failures, plus `server.address`/`server.port` on the client. Exporting
  stays the app's concern (any Effect Tracer/Metric exporter works).
- Client bidi streams that the consumer closes early (e.g. `Stream.take`)
  now record `CANCELLED` instead of `OK`.
- `tracestate` propagation: servers attach an incoming `tracestate` header to
  the `ExternalSpan` parent's annotations under the new exported
  `GrpcTracing.TraceState` context key; clients forward the nearest
  `tracestate` found in the span ancestry alongside the injected
  `traceparent`.

See the new [observability guide](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/observability.md)
for the full list of span/metric names and attributes and how to hook up an
OTLP exporter.
