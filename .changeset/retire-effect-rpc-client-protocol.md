---
"@effect-grpc/effect-grpc": minor
---

Breaking: the Effect RPC client protocol path is retired. `GrpcClientProtocol`
no longer implements or provides `RpcClient.Protocol` — the internal `make`
that translated `RpcClient.Protocol` requests into connect calls is removed,
and `GrpcClientProtocol.layer` / `layerFromTransport` narrow from
`Layer.Layer<RpcClient.Protocol | GrpcInvoker.GrpcInvoker>` to
`Layer.Layer<GrpcInvoker.GrpcInvoker>`.

`GrpcInvoker` is now the single client-side seam: generated clients (and the
built-in health/reflection clients) already resolve every call shape through
it since the previous release, so providing `GrpcClientProtocol.layer(...)` —
or `GrpcInvoker.layerConnect` / `GrpcInvoker.layerInMemory` directly — keeps
working unchanged. Only code that consumed `RpcClient.Protocol` from these
layers directly (e.g. hand-built `RpcClient.make(...)` clients) is affected;
migrate such callers to the invoker.

The now-dead `CodegenSupport.headersFromOptions` re-export and the internal
`x-effect-grpc-timeout-ms` header writer are removed alongside the client
protocol path; `CodegenSupport.GrpcCallOptions.timeoutMs` remains and is still
honored — the invoker passes it through to connect `CallOptions`.

Everything else in `GrpcClientProtocol` is unchanged: `makeTransport`,
`metadataInterceptor`, the TLS options (`GrpcClientTlsOptions`),
`GrpcClientTransportOptions`, `GrpcClientProtocolOptions`,
`GrpcClientProtocolTransportOptions`, and the re-exported
`GrpcTransportOptions`.

Generated code is unaffected — no regeneration required. The generated
`Rpc.make` / `RpcGroup.make` / `*Rpcs` exports remain because the server path
(`GrpcServerProtocol` / `RpcServer` handler layers) still consumes them.
