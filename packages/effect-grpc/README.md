# @effect-grpc/effect-grpc

Runtime support for generated Effect RPC-backed native gRPC clients and
servers.

The first prototype supports unary and server-streaming methods only. Generated
RPCs use `GrpcStatusError` as their generic error schema.

`GrpcStatusError` is a schema-backed tagged error, so generated client failures
are decoded into real `GrpcStatusError.GrpcStatusError` instances. Discriminate
them by their `_tag` (`"GrpcStatusError"`), e.g. with `Effect.catchTag`.

User metadata keys beginning with `x-effect-grpc-` are reserved for runtime
control and are rejected by generated clients.
