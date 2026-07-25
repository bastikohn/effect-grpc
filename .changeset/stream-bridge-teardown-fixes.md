---
"@effect-grpc/effect-grpc": patch
---

Fixes in the shared `Stream` <-> `AsyncIterable` bridge, now the single home for
half-close vs. cancellation detection and outcome-preserving cleanup:
overlapping pulls no longer duplicate messages or leak a fiber past teardown; a
bidi call abandoned mid-pull no longer hangs connect's generator loop; and a
streaming handler that abandons its request stream no longer stalls the server.
