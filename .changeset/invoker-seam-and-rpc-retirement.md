---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: generated clients and server handlers no longer run on Effect RPC.
All four method kinds now go through the `GrpcInvoker` seam (`layerConnect` for
the wire, `layerInMemory` for socket-free tests) and the unified
`GrpcServerProtocol.GrpcHandlers` map. `GrpcMethodRegistry` owns tag lookup,
merging, and the four domain/wire conversions. Regenerate your protos.
