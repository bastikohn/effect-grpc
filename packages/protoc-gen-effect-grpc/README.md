# @effect-grpc/protoc-gen-effect-grpc

Build-time protobuf generator for `effect-grpc`.

The prototype emits one `*_effect_grpc.ts` file beside protobuf-es output. It
fails on client-streaming and bidirectional-streaming methods by default.

It supports scalar (including `optional` and 64-bit-as-`bigint`), message
(including nested and cross-package imported), enum, repeated, map, and oneof
fields, plus `google.protobuf.Timestamp` and `Duration`. It still fails fast
for unsupported shapes such as other well-known types, non-string map keys,
enum map values, enum/repeated/map oneof values, and proto2 required/default
behavior (see `docs/users/limitations.md`).
