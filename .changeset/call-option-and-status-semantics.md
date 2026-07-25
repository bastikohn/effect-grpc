---
"@effect-grpc/effect-grpc": minor
---

Breaking: `GrpcStatusError.code` can no longer be `"ok"`, and call metadata is
carried once instead of per shape. Binary metadata is now symmetric across both
invoker adapters and keyed off the `-bin` suffix, non-positive deadlines fail
with `deadline_exceeded` on both, and client telemetry keeps `server.port` for
scheme-default ports (443/80).
