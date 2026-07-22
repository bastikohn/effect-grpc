---
"@effect-grpc/protoc-gen-effect-grpc": minor
"@effect-grpc/effect-grpc": minor
---

Generated gRPC clients now depend on the `GrpcInvoker` seam alone. All four
method kinds resolve through it — `unary` and `serverStream` (previously routed
through `RpcClient`/`RpcClient.Protocol`) join `clientStream` and `bidiStream`
— so the split across `RpcClient` and the `GrpcClientProtocol.GrpcStreamingClient`
facade is gone. Generated code no longer imports `RpcClient`/`RpcClientError`,
and the `<Service>ClientError` alias narrows to `GrpcStatusError` alone (the
`RpcClientError` union member is dropped from the type and every method
signature).

Breaking: `GrpcClientProtocol.GrpcStreamingClient` (its service class,
`GrpcStreamingClientService` interface, and the effect that built it) is
removed; `GrpcClientProtocol.layer` / `layerFromTransport` no longer provide it
(they still provide `RpcClient.Protocol` and `GrpcInvoker`). Provide a
`GrpcInvoker` — `GrpcInvoker.layerConnect` in production, or
`GrpcInvoker.layerInMemory` for network-free tests — to run generated clients;
regenerate your protos to pick up the new client shape. The built-in
`GrpcReflection.ReflectionClient` migrates to the invoker and its
`ReflectionClientError` narrows to `GrpcStatusError` accordingly.

Breaking: `GrpcHealth.HealthClient` likewise migrates to the invoker (provide a
`GrpcInvoker` to run it), and its `HealthClientError` narrows to
`GrpcStatusError` alone (the `RpcClientError` union member is dropped).

Two further consequences of routing unary and server-streaming calls through
the invoker:

- These calls no longer emit the intermediate `RpcClient.<tag>` wrapper span;
  only the semconv gRPC client span remains (its attributes, status, and
  duration metric are unchanged). Anyone keying trace tooling on `RpcClient.*`
  span names for unary/server-streaming calls is affected.
- On unary and server-streaming calls, an invalid request payload or a reserved
  `x-effect-grpc-*` metadata key now surfaces as a typed `GrpcStatusError`
  (`invalid_argument`) in the error channel instead of a defect.
