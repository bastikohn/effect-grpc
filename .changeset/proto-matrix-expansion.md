---
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Expand the supported proto matrix: nested messages and nested enums
(generated with protobuf-es-style `Outer_Inner` names), cross-package
imported messages (including in repeated, map, oneof, and method
input/output positions), imported and repeated enum fields, `optional`
scalar and enum fields with presence preserved as `undefined`, and
64-bit integers as `bigint` by default (`int64=bigint` is now the
default and remains accepted as an explicit option).
