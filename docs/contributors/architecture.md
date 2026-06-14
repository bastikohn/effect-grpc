# Architecture

The repository has two public packages:

- `@effect-grpc/effect-grpc` owns the native gRPC runtime bridge, status and
  metadata types, node server helper, and codegen support helpers.
- `@effect-grpc/protoc-gen-effect-grpc` owns build-time generation from
  protobuf descriptors to Effect schemas, RPC groups, registries, clients, and
  server handler layers.

Generated code is the contract between the packages. It imports protobuf-es
service descriptors, builds a `GrpcMethodRegistry`, and exposes a narrow client
and server facade. Runtime code should not need to inspect `.proto` files.

Symbols exported from package roots are public. Files under `internal/*` are not
public and package exports intentionally block those subpaths.
