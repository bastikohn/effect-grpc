---
"@effect-grpc/effect-grpc": minor
---

Deepen the `GrpcMethodRegistry` contract: the module now owns tag lookup with cardinality validation (`lookup`), registry merging with the duplicate-tag construction invariant (`merge`), grouping by service descriptor (`groupByService`), and the four domain/wire conversions with one normalized error policy — request-payload problems fail with `invalid_argument`, response-payload problems with `internal` (`encodeRequest`/`decodeRequest`/`encodeResponse`/`decodeResponse`, backed by cached per-entry codecs). The client invoker, server protocol, and `GrpcNodeServer` no longer reimplement lookup, kind checks, codec preparation, conversion-error mapping, or duplicate detection; `internal/codec.ts` is absorbed into the registry module.
