---
"@effect-grpc/protoc-gen-effect-grpc": minor
"@effect-grpc/effect-grpc": minor
---

Make `GrpcStatusError` a schema-backed `Schema.TaggedErrorClass` and use the
class itself as the generated RPC error schema. The parallel `schema` struct,
the duck-typed `isGrpcStatusError`, and `fromEncoded` are removed. Generated
clients now decode failures into real `GrpcStatusError` instances, discriminated
by their `_tag` (e.g. `Effect.catchTag("GrpcStatusError", …)`), and the
`<Service>ClientError` alias collapses to `GrpcStatusError | RpcClientError`.

Breaking: remove uses of `GrpcStatusError.schema`, `GrpcStatusError.fromEncoded`,
and `GrpcStatusError.isGrpcStatusError`; discriminate failures by
`_tag === "GrpcStatusError"` (or `Effect.catchTag`) instead.
