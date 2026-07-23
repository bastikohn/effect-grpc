# Testing

Use the demo E2E suites as the reference vertical slice for native gRPC
behavior across all four method kinds. They cover success, status failures,
metadata, trace headers, deadlines, mid-stream failures, request-stream
failures, cancellation in both directions, and protocol scope finalization.

Runtime protocol tests should cover behavior that can be asserted without a
real socket, including the codec error policy per call shape, unimplemented
methods, and handler interruption when a call is aborted.

`GrpcInvoker` is the single client seam; `GrpcInvoker.layerInMemory` is its
network-free stand-in, and invoker tests assert both adapters share invocation
semantics. Known limitation: the in-memory adapter enforces `timeoutMs` (as
`deadline_exceeded`) only for unary and client-streaming calls — stream-shaped
calls expose `timeoutMs` on the call context but leave mid-stream deadline
enforcement to transports.

Generator tests should use descriptor/plugin fixtures for every unsupported
protobuf construct so codegen fails clearly instead of emitting incorrect
schemas or converters.

Package smoke must exercise packed packages, root exports, blocked internal
subpaths, package JSON imports, the plugin binary, real Buf generation, and
typechecking of generated output in a temporary consumer.
