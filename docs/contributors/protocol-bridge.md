# Protocol Bridge

On the client side, `GrpcInvoker` is the single seam: generated clients invoke
all four call shapes through it, and its connect adapter resolves the native
Connect client method, applies codecs and call options, and ties Effect
interruption to call cancellation with an `AbortController`.
`GrpcClientProtocol` builds the connect transport and provides the invoker
layer — there is no client-side `RpcClient.Protocol` anymore.

`GrpcServerProtocol` translates native Connect handlers into
`RpcServer.Protocol` requests. Each native call gets one client id and call
state. Cleanup is idempotent, removes abort listeners, ends the call state, and
signals the protocol `disconnects` queue.

Server-streaming uses a bounded queue between Effect RPC responses and the
native async iterable. Offers backpressure when the queue is full, and cleanup
shuts the queue down so waiting consumers do not hang.

Bridge callbacks capture the current Effect context once during protocol
construction and run async boundary effects with that context.

## Direct Streaming Bridge

The Effect RPC wire protocol has no client-to-server chunk variant
(`FromClientEncoded` is `Request | Ack | Interrupt | Ping | Eof`), so
client-streaming and bidi-streaming methods cannot flow through `RpcServer` on
the server. They use a parallel server path over the same connect transport
and registry:

- `GrpcInvoker` is the single client-side seam for all four call shapes
  (`GrpcInvoker.layerConnect` in production). For the streaming shapes it
  converts the caller's `Stream` into the `AsyncIterable` a connect client
  method expects, applying the registry's JSON codecs and converters per
  message. An `AbortController` ties Effect interruption to call cancellation.
  If the request stream fails, the call is cancelled and the original error is
  replayed to the caller.
- `GrpcServerProtocol` registers connect handlers that wrap the incoming
  `AsyncIterable` as a `Stream` (decoding per message) and run the streaming
  handler; bidi responses are pulled back out through
  `Stream.toAsyncIterableWith`, so backpressure falls out of pull semantics and
  HTTP/2 flow control.
- Generated `*HandlersLayer` functions publish streaming handlers through the
  `GrpcServerProtocol.GrpcStreamingHandlers` context key inside the handlers
  layer; `GrpcNodeServer.serveAll` builds each layer and collects the maps.
- connect-node surfaces a client cancellation server-side as a clean end of the
  request iterable plus an aborted handler signal, so the request stream fails
  with `cancelled` when the signal is aborted at end-of-stream, and the handler
  fiber is interrupted through the same signal.

Because both server paths share the transport, interceptors and metadata
behave identically; anything hung off Effect RPC middleware applies only to
the server-side RPC path (unary and server-streaming handlers) — clients never
see it.
