---
"@effect-grpc/effect-grpc": minor
---

Add opt-in `GrpcDeadline.callOptions` to read a server call's remaining budget at Effect execution and bound downstream RPC timeouts. Exhausted budgets fail with `deadline_exceeded` before the downstream call, and tighter caller timeouts and metadata are preserved.
