---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
"@effect-grpc/codegen": minor
---

Add client-streaming and bidi-streaming support via a direct streaming bridge.

The Effect RPC wire protocol has no client-to-server stream, so the two new
method kinds bypass `RpcClient`/`RpcServer` and bridge `Stream` <->
`AsyncIterable` directly over the same connect transport and registry. Unary
and server-streaming methods are unchanged.

Generated clients gain per-kind signatures — client-streaming
`(requests: Stream<I, E>, options?) => Effect<O, ClientError | E>` and bidi
`(requests: Stream<I, E>, options?) => Stream<O, ClientError | E>` — served by
the new `GrpcClientProtocol.GrpcStreamingClient`, which
`layer`/`layerFromTransport` now provide alongside `RpcClient.Protocol`.
Generated implementations extend symmetrically with
`(requests: Stream<I, GrpcStatusError>, context)` handlers; the generated
`*HandlersLayer` publishes them through the new
`GrpcServerProtocol.GrpcStreamingHandlers` context key, so `serveAll` wiring is
unchanged for users.

Semantics: interrupting the returned `Effect`/`Stream` cancels the call; if the
request stream fails, the call is cancelled and the caller sees the original
error while the server observes `cancelled`; request-stream completion
half-closes the call; streamed messages are decoded/encoded per message with
the generated schemas. Effect RPC middleware does not apply to the direct
streaming path.

Breaking: `GrpcMethodEntry` gains a required `successSchema` (regenerate your
protos), and the codegen option `ignore_unsupported_methods` is removed — all
four gRPC method kinds are now supported and `methods` defaults to all of them.
