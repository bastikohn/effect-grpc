---
"@effect-grpc/effect-grpc": minor
---

Add opt-in response header/trailer observers to call options and validated native server context writers. Headers commit before the first response; trailers remain writable through normal handler finalization. Clean-completion observers, cancellation, failed calls, late writes, and unsupported in-memory response metadata now have explicit lifetimes without changing generated result types.
