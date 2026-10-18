# Response headers and trailers

Generated client methods keep their normal Effect or Stream result. To observe
response metadata, supply synchronous `onResponseHeaders` and/or
`onResponseTrailers` callbacks in the existing call options:

```ts
yield *
  client.getUser(
    { id: "1" },
    {
      onResponseHeaders: (metadata) => {
        console.log("headers", metadata);
      },
      onResponseTrailers: (metadata) => {
        console.log("trailers", metadata);
      },
    },
  );
```

Each callback receives a decoded snapshot using the usual `GrpcMetadata` tuples:
lowercase keys, strings for ordinary values, and `Uint8Array` for `-bin` keys.
Callbacks return `undefined`; async callbacks are not supported. A callback
throw becomes an `internal` status failure and cancels an active transport.
There is no pending metadata promise to settle on cancellation.

Headers are exposed when Connect makes them available: unary calls deliver them
when the response completes, while streaming calls deliver them before response
messages. A trailers callback runs once on **clean transport completion**, after
all response messages. It does not run on failure, interruption, deadline expiry,
or early stream termination such as `Stream.take(1)`. A locally buffered last
message alone does not establish completion; the stream must be drained.

Failures retain the existing `GrpcStatusError.metadata` channel. Connect may
combine response metadata in that error; callbacks do not reconstruct separate
headers and trailers from it. Request-stream failures still return the original
source error, with its identity preserved. A callback reports transport progress;
it does not promise that a subsequent application response-schema decode succeeds.

## Writing metadata in a handler

Every native server handler context has two Effect-returning writers:

```ts
yield * context.writeResponseHeaders([["x-request-id", requestId]]);
yield * context.writeResponseTrailers([["result-bin", new Uint8Array([1, 2])]]);
```

Writers append values to Connect's existing response headers or trailers. They
validate the whole input before changing either collection, so an invalid entry
cannot leave a partial write. Metadata follows the same key, printable-ASCII,
and binary-value rules as call metadata. Protocol-owned `grpc-*`, `content-type`,
`content-length`, `transfer-encoding`, `connection`, `te`, and `trailer` keys are
rejected with `invalid_argument`.

Headers become committed immediately before the runtime yields the first
response message to Connect. For unary and client-streaming handlers, that is
when the handler finishes. Trailers remain writable through normal handler
finalization; stream finalizers can append them before the stream completes.
Both writers fail with `failed_precondition` after call completion or signal
abort, and the header writer also fails once headers are committed. Writes are
lazy Effects: their execution time determines whether the write is allowed.
Cleanup after cancellation should not attempt a metadata write.

This is the handler boundary. Native interceptors continue using Connect's own
response header/trailer collections and must respect Connect's transport lifetime.
The library does not freeze their collections or intercept unrelated writes.

## In-memory adapter

`GrpcInvoker.layerInMemory` has no native server response metadata context.
Supplying either observer returns `unimplemented` before invoking a handler or
consuming a request stream. Calls without observers retain their existing
behavior. Use native transport tests for response metadata and its lifetimes.

Handwritten `GrpcServerContext` fixtures must now supply both writer functions;
generated handler signatures and existing metadata-only handlers are unchanged.
