# Protocol Bridge

On the client side, `GrpcInvoker` is the single seam: generated clients invoke
all four call shapes through it, and its connect adapter resolves the native
Connect client method, applies codecs and call options, and ties Effect
interruption to call cancellation with an `AbortController`.
`GrpcClientProtocol` builds the connect transport and provides the invoker
layer — there is no client-side `RpcClient.Protocol` anymore.

On the server side, `GrpcServerProtocol.GrpcHandlers` is the single seam: a
map from method tag to one handler per call shape. Generated `*HandlersLayer`
functions publish their handlers through the `GrpcHandlers` context key inside
the handlers layer; `GrpcNodeServer.serveAll` builds each layer, collects the
maps, and `GrpcServerProtocol.make` registers connect routes for every
registry entry. Methods without a registered handler fail with
`unimplemented`. The `GrpcMethodRegistry` is the sole codec authority: request
messages are decoded (`invalid_argument` on failure) and response values are
encoded (`internal` on failure) per message around the handler.

Execution runs through two templates behind four thin connect adapters
(connect imposes four handler signatures — Promise vs async-generator):

- Effect-shaped calls (unary, client-streaming) run the handler effect with
  the connect `signal` bound to the running fiber, inside one server span.
  Non-server-fault failures are carried as values so the span closes cleanly
  before the error reaches connect; unary is the same template with a single
  decoded request value instead of a request stream.
- Stream-shaped calls (server-streaming, bidi-streaming) pull the handler's
  response stream through `StreamBridge.responsePump`, so demand follows
  connect's iteration and HTTP/2 flow control, and closing the pump interrupts
  the handler fiber when the client goes away. The pump spawns the handler
  fiber with the scoped server span as parent and the incoming `tracestate`
  provided, so downstream client calls inherit span context and propagation.

`internal/streamBridge.ts` owns the `Stream` <-> `AsyncIterable` termination
semantics on both sides:

- `GrpcInvoker`'s connect adapter converts the caller's `Stream` into the
  `AsyncIterable` a connect client method expects. If the request stream
  fails, the call is cancelled and the original error is replayed to the
  caller.
- The server protocol wraps the incoming request `AsyncIterable` as a `Stream`
  (decoding per message). connect-node surfaces a client cancellation
  server-side as a clean end of the request iterable plus an aborted handler
  signal, so the request stream fails with `cancelled` when the signal is
  aborted at end-of-stream, and the handler fiber is interrupted through the
  same signal.

Because every call shape shares the transport and the same handler seam,
interceptors, metadata, status mapping, and telemetry behave identically
across all four kinds.
