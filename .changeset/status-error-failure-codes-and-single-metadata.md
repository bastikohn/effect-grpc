---
"@effect-grpc/effect-grpc": minor
---

Tighten `GrpcStatusError` to the shape the gRPC wire actually supports:

- **`code` can no longer be `"ok"`.** A handler failing with `code: "ok"` made
  both server and client record an OK span and an OK duration metric while
  `toConnectCode` still sent `UNKNOWN` to the peer — success telemetry for a
  call the caller saw fail. `code` (and the `make()` option) is now
  `GrpcStatusCode.GrpcErrorStatusCode`, the `"ok"`-free subset, backed by
  `GrpcStatusCode.errorSchema`. The full `GrpcStatusCode` union stays as a type
  for telemetry, which legitimately reports successful outcomes; both unions
  are derived from one literal list, and `fromConnectCode` now returns the
  narrow type (connect's `Code` has no `OK` member). `GrpcStatusCode.schema`,
  the wide schema, is removed — nothing decodes a status code that may be
  `"ok"`.
- **`trailers` is gone.** It was dead API: connect exposes exactly one metadata
  channel for an error, `toConnectError` merged `metadata` and `trailers` into
  it, and `fromConnectError` always produced an empty `trailers`. Pass
  everything as `metadata` — under the gRPC protocol it is written to, and read
  back from, the response trailers.

Breaking: drop `trailers` from `GrpcStatusError` construction, use a concrete
failure code instead of `"ok"`, and replace `GrpcStatusCode.schema` with
`GrpcStatusCode.errorSchema`.
