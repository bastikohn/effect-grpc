# Architecture

The repository has two public packages:

- `@effect-grpc/effect-grpc` owns the native gRPC runtime bridge, status and
  metadata types, node server helper, and codegen support helpers.
- `@effect-grpc/protoc-gen-effect-grpc` owns build-time generation from
  protobuf descriptors to Effect schemas, registries, clients, and server
  handler layers.

Generated code is the contract between the packages. It imports protobuf-es
service descriptors, builds a `GrpcMethodRegistry`, and exposes a narrow client
and server facade. Generated clients invoke every method kind through the
`GrpcInvoker` seam — the single client-side entry point to the transport —
while generated server handler layers publish every method kind into the
`GrpcServerProtocol.GrpcHandlers` map — the single server-side handler seam.
Runtime code should not need to inspect `.proto` files.

Symbols exported from package roots are public. Files under `internal/*` are not
public and package exports intentionally block those subpaths.
