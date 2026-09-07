---
"@effect-grpc/effect-grpc": patch
---

Make `GrpcInvoker.layerInMemory` enforce call deadlines for server-streaming
and bidirectional-streaming RPCs, bringing `timeoutMs` behavior into parity
across all four RPC shapes. A positive `timeoutMs` now bounds the lifetime of
the whole streamed call — measured from invocation, not from the last message
— failing with `deadline_exceeded` and interrupting the handler (and, for
bidi calls, the request stream) so their finalizers run. A non-positive
`timeoutMs` still means no deadline.
