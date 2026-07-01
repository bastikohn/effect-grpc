# @effect-grpc/effect-grpc

Runtime support for generated Effect RPC-backed native gRPC clients and
servers.

All four gRPC method kinds are supported. Unary and server-streaming methods
run through `effect/unstable/rpc`; client-streaming and bidi-streaming methods
bridge `Stream` and connect iterables directly over the same transport (the
Effect RPC protocol has no client-to-server stream). Generated RPCs use
`GrpcStatusError` as their generic error schema.

`GrpcStatusError` is a schema-backed tagged error, so generated client failures
are decoded into real `GrpcStatusError.GrpcStatusError` instances. Discriminate
them by their `_tag` (`"GrpcStatusError"`), e.g. with `Effect.catchTag`.

User metadata keys beginning with `x-effect-grpc-` are reserved for runtime
control and are rejected by generated clients.
