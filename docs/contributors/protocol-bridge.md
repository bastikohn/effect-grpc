# Protocol Bridge

`GrpcClientProtocol` translates `RpcClient.Protocol` requests into native
Connect gRPC client calls. It tracks active native calls with `AbortController`
instances and aborts them when the protocol scope finalizes or when the Effect
RPC client interrupts a request.

`GrpcServerProtocol` translates native Connect handlers into
`RpcServer.Protocol` requests. Each native call gets one client id and call
state. Cleanup is idempotent, removes abort listeners, ends the call state, and
signals the protocol `disconnects` queue.

Server-streaming uses a bounded queue between Effect RPC responses and the
native async iterable. Offers backpressure when the queue is full, and cleanup
shuts the queue down so waiting consumers do not hang.

Bridge callbacks capture the current Effect context once during protocol
construction and run async boundary effects with that context.
