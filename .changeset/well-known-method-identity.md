---
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Take the identity of a well-known method input/output from the protobuf
descriptor instead of the name the generator printed for it. A message merely
_named_ `GrpcGoogleProtobufTimestamp` was read back as `google.protobuf.Timestamp`
whenever it appeared as a method input or output: the file declared
`GrpcGoogleProtobufTimestampSchema` and its type twice and did not compile
(TS2451/TS2300). The same lookup routed a `GrpcGoogleProtobufBoolValue` message
through the wrapper's `{ value }` boxing converter and a `GrpcGoogleProtobufEmpty`
message through the Empty converter, which dropped every field — the wrong
converter, alongside the duplicate-declaration error. Method types now carry
their descriptor's well-known kind through the model, so no naming decision can
be mistaken for an identity one.

**Generated well-known method types move into the `Grpc$` namespace.** The
converters were namespaced already; their schema and type now join them, so
`GrpcGoogleProtobufTimestamp` becomes `Grpc$GoogleProtobufTimestamp` (likewise
`Duration`, the `*Value` wrappers, and `Empty`). `$` is legal in TypeScript
identifiers but never in protobuf ones, so the generator's names are now
unreachable from a `.proto` by construction, and a message of your own can share
a file with the well-known it is named after. Regenerate, and rename any handler
or client signature that spells one of these types.
