---
"@effect-grpc/effect-grpc": minor
---

Two call-option semantics that diverged between the connect and in-memory
`GrpcInvoker` adapters:

- **Binary _call_ metadata is symmetric and keyed off the `-bin` suffix.** The
  suffix is a peer's only signal that a header carries bytes, so it — not the
  JavaScript type — now drives both directions: `GrpcMetadata.toHeaders`
  base64-encodes `-bin` values, and `GrpcMetadata.fromHeaders` decodes them
  back to `Uint8Array`. A binary value previously reached the server (the sole
  receive path, via `CodegenSupport.serverContext`) as the base64 string it was
  encoded to, so it never round-tripped to its declared type — and bytes under
  a key _without_ the suffix were silently base64'd into a header the peer
  could not identify as binary. Call metadata that contradicts its key —
  `Uint8Array` under an ASCII key, `string` under a `-bin` key — now fails with
  `invalid_argument` from the shared validator, so both adapters (and the
  `metadataInterceptor`) reject it identically instead of one throwing and the
  other silently accepting. The same validator also rejects keys and values no
  header can spell (`"bad key"`, `""`, `"ünicode"`, `"x:a"`, a value containing
  CR/LF), which previously reached `Headers.append` and died as an untyped
  `TypeError`. `GrpcMetadata.isBinaryKey` is exported alongside. Repeated
  `-bin` headers, which `Headers.entries()` joins with `", "`, are split back
  into one entry per value; ASCII values are deliberately left whole, since a
  comma is legal inside one.
- **A non-positive `timeoutMs` uniformly means _no deadline_.** The in-memory
  adapter already treated `<= 0` that way while the connect adapter forwarded
  the value to a transport, where `createDeadlineSignal` aborts a `<= 0`
  timeout the instant the call starts. (connect's own transports happen to
  clamp the value before that point, so this was latent there and observable
  only through a bare `Transport`.) Normalization now lives next to the shared
  metadata validator and the connect adapter omits the option entirely, so the
  semantic no longer depends on a transport's own clamping.
  `GrpcInMemoryCall.timeoutMs` is likewise absent for a non-positive input:
  the handler's view must match the deadline actually in force, which on the
  wire is no `grpc-timeout` header at all.

Two consequences worth calling out:

- **`layerInMemory` now normalizes call metadata exactly as the wire does**,
  by routing it through the same codec. A handler observes lowercased keys in
  alphabetical order, with repeated ASCII keys collapsed into one comma-joined
  value (`[["x-a","one"],["x-a","two"]]` arrives as `[["x-a","one, two"]]`) —
  which is what a real server sees, and the point of the two adapters
  promising identical semantics.
- **`GrpcStatusError.metadata` decoded from a peer now yields `Uint8Array` for
  `-bin` keys** where it previously yielded the base64 string. This is
  user-visible on `grpc-status-details-bin`, which connect does not strip: it
  appears in `error.metadata` (now bytes) as well as, decoded, in
  `error.details`.

The `-bin` policy covers _call_ metadata only. `GrpcStatusError` metadata is
still encoded best-effort by `toConnectError` and is neither validated nor
normalized, so an entry contradicting its key silently changes type in transit
in either direction. Validating there would mean throwing while serializing an
error, swallowing the original failure; the caveat is documented on the field
instead.
