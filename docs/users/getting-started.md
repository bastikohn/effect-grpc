# Getting Started

`effect-grpc` is a build-time codegen plus runtime bridge for native gRPC unary
and server-streaming calls backed by `effect/unstable/rpc`.

1. Generate protobuf-es output with `protoc-gen-es`.
2. Generate Effect RPC glue with `protoc-gen-effect-grpc`.
3. Provide a generated client layer with `GrpcClientProtocol.layer`.
4. Serve generated server handlers with `GrpcNodeServer.serveAll`.

The demo packages show the current supported path:

```sh
pnpm demo:generate
pnpm demo:server
pnpm demo:client -- get-user --id 123
pnpm demo:client -- watch-users --tenant-id demo --count 3
```

Generated client methods accept `CodegenSupport.GrpcCallOptions`. User metadata
keys beginning with `x-effect-grpc-` are reserved for local runtime control and
are rejected before the native gRPC request is sent.
