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
methods, and handler interruption when a call is aborted.
`GrpcInvoker.layerInMemory` is the network-free stand-in for the client seam,
and invoker tests assert both adapters share invocation semantics. Known
limitation: the in-memory adapter enforces `timeoutMs` (as `deadline_exceeded`)
only for unary and client-streaming calls — stream-shaped calls expose
`timeoutMs` on the call context but leave mid-stream deadline enforcement to
transports.

Generator tests should use descriptor/plugin fixtures for every unsupported
protobuf construct so codegen fails clearly instead of emitting incorrect
schemas or converters.

Package smoke must exercise packed packages, root exports, blocked internal
subpaths, package JSON imports, the plugin binary, real Buf generation, and
typechecking of generated output in a temporary consumer.
