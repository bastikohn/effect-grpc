---
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Namespace the converters the generator introduces itself, so legal proto names can no longer shadow them. A file containing `message Bytes` emitted the base64 helper `const fromBytes` next to the message converter `export const fromBytes`, producing a file that could not compile (TS2451) — and a `from<Message>` body that recursed into itself. The oneof and well-known converters were the same class: `message Foo_barOneof` collided with the converter for `Foo`'s `bar` oneof, and `message GrpcGoogleProtobufTimestamp` collided with the Timestamp converter, which additionally routed the well-known field through the user's message converter — wrong output, not just a wrong name.

Base64, oneof, well-known and Empty converters now share one `Grpc$` namespace (`fromGrpc$Bytes`, `fromGrpc$FeatureRequest_contactOneof`, `fromGrpc$GoogleProtobufTimestamp`). `$` is legal in TypeScript identifiers but never in protobuf ones, so no message name can reach them. Generated schema and type names are unchanged; the renamed well-known converters are exported when the type is a method input/output, so regenerate to pick them up.
