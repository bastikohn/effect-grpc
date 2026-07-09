---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcReflection`: the standard gRPC Server Reflection Protocol
(`grpc.reflection.v1.ServerReflection`, plus the legacy `v1alpha` alias), so
`grpcurl`, `grpcui`, Postman, and similar tools work against the server
without local `.proto` files.

- `GrpcReflection.service(services)` is a ready-made entry for
  `GrpcNodeServer.serveAll`; pass it the same services array you pass to
  `serveAll`. It answers reflection queries from the descriptors the
  generated registries already carry, so no extra codegen or runtime `.proto`
  loading is involved, and it describes itself and the health service too.
- Semantics follow the spec: `list_services`, `file_by_filename`,
  `file_containing_symbol`, `file_containing_extension`, and
  `all_extension_numbers_of_type` are all implemented; file answers include
  the requested descriptor followed by its transitive imports; unknown names
  produce an in-band `NOT_FOUND` error response that echoes the original
  request instead of failing the stream.
- The same handler serves both the `v1` and `v1alpha` service names — older
  tools that only probe `v1alpha` work unchanged.
- `GrpcReflection.ReflectionClient`/`ReflectionClientLayer` provide a client
  for querying remote reflection services, shaped like generated clients;
  `ReflectionGrpcRegistry` plugs into `GrpcClientProtocol.layer`.
- `GrpcReflection.makeIndex`/`respond` expose the pure lookup layer for
  advanced composition and testing.
