---
"@effect-grpc/effect-grpc": patch
---

Target stable `effect@^4.1.2` (from `4.0.1`). Effect 4.1 releases its whole
package line together, so `@effect/platform-node` and `@effect/opentelemetry`
move to `4.1.2` in lockstep, and those packages require Node >= 22.19 — the
workspace `engines` floor is raised to match. No Effect API used by this
library changed or was deprecated in the range. The server request bridge now
relies on 4.1.2's `Stream.fromAsyncIterable` never awaiting an iterator
`return()` that is queued behind a pending pull, which replaces the hand-
rolled detached-cleanup wrapper the bridge needed on earlier versions.
