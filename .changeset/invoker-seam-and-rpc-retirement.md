---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: generated clients and server handlers no longer run on Effect RPC.
All four method kinds now go through the `GrpcInvoker` seam (`layerConnect` for
the wire, `layerInMemory` for socket-free tests) and the unified
`GrpcServerProtocol.GrpcHandlers` map. `GrpcMethodRegistry` owns tag lookup,
merging, and the four domain/wire conversions. Regenerate your protos.

Migration:

- `GrpcNodeServer.ServeAllService` loses its `group` field — drop `group:` from
  every `serveAll` call site.
- `GrpcServerProtocol.GrpcStreamingHandlers`/`streamingHandlersLayer` are
  replaced by the unified `GrpcHandlers` map.
- `GrpcClientProtocol.GrpcStreamingClient` is removed — provide a `GrpcInvoker`
  (`layerConnect`, or `layerInMemory` for network-free tests) to run generated
  clients.
- `GrpcClientProtocol.layer`/`layerFromTransport` no longer provide
  `RpcClient.Protocol`; migrate hand-built `RpcClient.make(...)` callers to the
  invoker.
