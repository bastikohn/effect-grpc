---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcHealth`: the standard gRPC Health Checking Protocol
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
