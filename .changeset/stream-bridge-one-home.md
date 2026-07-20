---
"@effect-grpc/effect-grpc": patch
---

Centralize the Effect `Stream` <-> connect `AsyncIterable` streaming lifecycle in one internal bridge module shared by the client and server protocols. Half-close vs. cancellation detection, iterator `return`/`throw` behavior, source-failure replay, and outcome-preserving cleanup now have a single implementation and test surface.

Also fixes a potential hang: a bidi call abandoned by the client while the server had a response pull in flight could leave connect's generator loop waiting forever, since closing the response iterator releases the handler's resources but never settles the in-flight pull.
