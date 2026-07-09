---
"@effect-grpc/effect-grpc": minor
---

OpenTelemetry-aligned tracing and metrics for clients and servers.

- Span attributes now follow the OTel RPC semantic conventions. **Breaking
  for existing dashboards/alerts**: `rpc.system.name` is now `rpc.system`
  (`"grpc"`), the full-path `rpc.method` is split into `rpc.service`
  (`demo.v1.UserService`) and `rpc.method` (`GetUser`), and the string
  `rpc.response.status_code` is replaced by the numeric
  `rpc.grpc.status_code` (0-16). `error.type` (status name, e.g.
  `NOT_FOUND`) is still set on failures.
- New zero-config RPC metrics via Effect `Metric`: `rpc.client.duration` and
  `rpc.server.duration` histograms (seconds, OTel-recommended boundaries),
  recorded for all four method kinds — including failure and cancellation
  paths — and tagged with `rpc.system`, `rpc.service`, `rpc.method`,
  `rpc.grpc.status_code`, plus `server.address`/`server.port` on the client.
  Exporting stays the app's concern (any Effect Tracer/Metric exporter works).
- `tracestate` propagation: servers attach an incoming `tracestate` header to
  the `ExternalSpan` parent's annotations under the new exported
  `GrpcTracing.TraceState` context key; clients forward the nearest
  `tracestate` found in the span ancestry alongside the injected
  `traceparent`.

See the new [observability guide](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/observability.md)
for the full list of span/metric names and attributes and how to hook up an
OTLP exporter.
