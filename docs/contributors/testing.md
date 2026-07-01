# Testing

Use the demo E2E suites as the reference vertical slice for native gRPC
behavior across all four method kinds. They cover success, status failures,
metadata, trace headers, deadlines, mid-stream failures, request-stream
failures, cancellation in both directions, and protocol scope finalization.

Runtime protocol tests should cover behavior that can be asserted without a real
socket, including call-state backpressure and server protocol cleanup.

Generator tests should use descriptor/plugin fixtures for every unsupported
protobuf construct so codegen fails clearly instead of emitting incorrect
schemas or converters.

Package smoke must exercise packed packages, root exports, blocked internal
subpaths, package JSON imports, the plugin binary, real Buf generation, and
typechecking of generated output in a temporary consumer.
