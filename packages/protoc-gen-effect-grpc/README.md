# @effect-grpc/protoc-gen-effect-grpc

Build-time protobuf generator for `effect-grpc`.

The prototype emits one `*_effect_grpc.ts` file beside protobuf-es output. It
fails on client-streaming and bidirectional-streaming methods by default.

It supports scalar (including `optional` and 64-bit-as-`bigint`), message
(including nested and cross-package imported), enum, repeated, map, and oneof
fields, plus the common protobuf well-known types used by service APIs:
timestamps, durations, wrapper values, `Any`, `Struct`, `Value`, `ListValue`,
and `FieldMask`. It still fails fast for client/bidirectional streaming unless
configured to skip those methods, import cycles, unsupported well-known types,
and proto2 required/default behavior (see `docs/users/limitations.md`).
