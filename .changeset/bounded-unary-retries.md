---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcRetry.unary`, an explicit opt-in for application-declared retry-safe unary calls. Selected typed statuses retry within a bounded attempt count using capped exponential backoff and full jitter. Attempts, setup, and backoff share one overall deadline, including a live upstream call budget. Defects, interruption, request streams, and server-stream reconnection are not retried.
