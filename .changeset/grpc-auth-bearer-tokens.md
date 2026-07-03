---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcAuth`: first-class bearer-token authentication for clients.

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
