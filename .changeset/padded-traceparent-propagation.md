---
"@effect-grpc/effect-grpc": patch
---

Fix outbound trace propagation for 64-bit trace ids. The client now writes
propagation headers with `effect/http/HttpTraceContext` instead of hand-rolling
`traceparent`: ids that entered this process as compact B3 (16 hex chars) are
padded to the 32 hex chars W3C receivers require — previously they were
emitted un-padded and silently broke the downstream trace — and a compact B3
(`b3`) header is emitted alongside `traceparent`. Caller-supplied
`traceparent`/`b3` metadata still wins, and untraced (noop) calls still send
no propagation headers.
