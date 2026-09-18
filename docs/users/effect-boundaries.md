# Effect boundary migration

`GrpcClientProtocol.makeTransport(options)` now returns
`Effect.Effect<Transport>`. Creating that Effect does not construct the transport
or validate configuration. Execute it where the transport is needed:

```ts
// Before
const transport = GrpcClientProtocol.makeTransport(options);

// After, inside Effect.gen
const transport = yield * GrpcClientProtocol.makeTransport(options);
```

`GrpcClientProtocol.layer(options)` keeps its existing signature and executes the
constructor when the layer is built. Contradictory TLS settings, such as a TLS
block with an HTTP URL or only one mTLS credential, are configuration defects in
the Effect cause; they are not typed RPC status failures. Node server binding
failures, including an occupied port, are likewise defects. Server scope closure
waits for shutdown; active HTTP/2 sessions are destroyed after the configured
`shutdownTimeoutMs` budget.

Metadata resolved by `metadataInterceptor` now uses the same `invalid_argument`
status as invalid per-call metadata, preserving normal `_tag`-based handling.

## Status-error schema values

`GrpcStatusError.GrpcStatusError` is a `Schema.TaggedError` and can be passed to
`Schema.encodeEffect` and `Schema.decodeUnknownEffect`. Its supported encoded
form is a JavaScript object containing `_tag`, `code`, `message`, `metadata`,
`details`, and an optional `cause`.

Binary metadata remains `Uint8Array` in that encoded object. Decoding produces a
`GrpcStatusError` instance that is also an `Error`; `Effect.catchTag("GrpcStatusError", …)`
continues working. The exported `GrpcErrorStatusCodeSchema` accepts failure codes
only, and `GrpcMetadataSchema` checks the tuple shape and string/byte value types.
It does not enforce the wire key/value convention; outgoing calls still do that.

`details` uses `Schema.Unknown`: arbitrary JavaScript values are permitted and
this is **not a portable JSON serialization guarantee**. For example, `Map`,
`bigint`, and cyclic values in details need an application-specific JSON codec.
Do not expect `JSON.stringify`/`JSON.parse` to preserve binary metadata or arbitrary
details. The gRPC wire continues using Connect's own status/details translation.

`cause` uses Effect's `Schema.Defect()`: an `Error` encodes as JSON-shaped
`name`/`message`/optional `cause` data, without its stack by default; an error-shaped
cause decodes to an `Error`. Arbitrary causes can be normalized or lose unsupported
properties according to that schema, so cause identity and arbitrary-value
round trips are not promised. Omitting a cause leaves it absent in the encoded
object.
