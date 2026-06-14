# Codegen

`protoc-gen-effect-grpc` builds a small model from protobuf descriptors and then
prints deterministic TypeScript. The model is intentionally narrower than
protobuf itself.

Unsupported protobuf constructs must fail before code is emitted. Do not map a
field to a broader schema just to make generation succeed. Each newly supported
protobuf feature needs:

- a descriptor/plugin fixture test
- generated snapshot coverage
- converter coverage through generated output
- E2E coverage when the feature affects transport behavior

Generated client method errors use a named `<ServiceName>ClientError` alias to
keep method signatures readable.
