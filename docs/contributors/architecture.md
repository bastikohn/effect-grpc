# Architecture

The repository has two public packages:

- `@effect-grpc/effect-grpc` owns the native gRPC runtime bridge, status and
  metadata types, node server helper, and codegen support helpers.
- `@effect-grpc/protoc-gen-effect-grpc` owns build-time generation from
  protobuf descriptors to Effect schemas, registries, clients, and server
  handlers.

Generated code is the contract between the packages. It imports protobuf-es
service descriptors, builds a `GrpcMethodRegistry`, and exposes a narrow client
and server facade. Generated clients invoke every method kind through the
`GrpcInvoker` seam — the single client-side entry point to the transport —
while generated `*Handlers` functions return every method kind as an `Effect`
of the `GrpcServerProtocol.GrpcHandlers` map — the single server-side handler
seam.
Runtime code should not need to inspect `.proto` files.

Server-side cross-cutting behavior is connect's: `GrpcNodeServer.serve` and
`serveAll` forward native `Interceptor`s to the connect node adapter, once,
and nowhere else. Handlers see one call through
`CodegenSupport.GrpcServerContext`, a per-RPC view the server protocol maps
from connect's `HandlerContext` (metadata, method identity, the live signal,
the live remaining time, and typed context values). There is no second
interceptor dispatcher and no Effect middleware pipeline.

Symbols exported from package roots are public. Files under `internal/*` are not
public and package exports intentionally block those subpaths.

## Codegen

`protoc-gen-effect-grpc` builds a small model from protobuf descriptors and then
prints deterministic TypeScript. The model is intentionally narrower than
protobuf itself. Unsupported protobuf constructs must fail before code is
emitted — do not map a field to a broader schema just to make generation
succeed. Each newly supported protobuf feature needs a descriptor/plugin fixture
test, generated snapshot coverage, converter coverage through generated output,
and E2E coverage when the feature affects transport behavior.

Generated client method errors use a named `<ServiceName>ClientError` alias to
keep method signatures readable.

## Testing

Use the demo E2E suites as the reference vertical slice for native gRPC
behavior across all four method kinds. They cover success, status failures,
metadata, trace headers, deadlines, mid-stream failures, request-stream
failures, cancellation in both directions, and protocol scope finalization.

Runtime protocol tests should cover behavior that can be asserted without a
real socket, including the codec error policy per call shape, unimplemented
methods, handler interruption when a call is aborted, and the per-call
handler context (`test/serverContext.test.ts`, against a fake connect
`HandlerContext` with its own values store and timeout getter). The
`interceptors` option itself bypasses that fake router, so it is covered only
by the native-transport suite (`examples/simple-client/test/server-context-e2e.test.ts`).
`GrpcInvoker.layerInMemory` is the network-free stand-in for the client seam,
and invoker tests assert both adapters share invocation semantics, including
the public deadline contract: a positive `timeoutMs` bounds the lifetime of
every call shape with `deadline_exceeded`. The in-memory adapter stays at the
domain level on purpose — it does not emulate HTTP/2, wire framing, or
`grpc-timeout` headers, or native server interceptors.

Generator tests should use descriptor/plugin fixtures for every unsupported
protobuf construct so codegen fails clearly instead of emitting incorrect
schemas or converters.

Package smoke must exercise packed packages, root exports, blocked internal
subpaths, package JSON imports, the plugin binary, real Buf generation, and
typechecking of generated output in a temporary consumer.

### Compatibility gates

`invokerParity.test.ts` runs shared scenarios through the in-memory invoker and
an actual HTTP/2 transport: frequent server/bidi responses cannot renew a call
budget, a paused consumer cannot keep producer resources alive past its deadline,
early termination awaits local request-source cleanup, a captured request error
retains its identity during recovery, and concurrent metadata remains isolated.
Wire cleanup is observed through a bounded server-finalization signal; a client
response cannot promise that remote cleanup has already completed.

Keep these tests alongside `grpcInvoker.test.ts`'s setup deadlines and asynchronous
scope-finalizer regressions, and the native `serverContext.test.ts` /
`server-context-e2e.test.ts` context/interceptor isolation tests. These are part of
`pnpm check:ci`; dependency and test-harness migrations must preserve their cases.
The same gate packs and installs the packages, then compiles a consumer with the
minimum supported TypeScript 5.9.3 and native TypeScript 7.0.2, checking the actual
compiler versions. The two compiler checks live in `scripts/package-smoke.mjs`.
