# Server Call Context and Interceptors

Every generated server handler receives a `CodegenSupport.GrpcServerContext`
as its second argument: a small, read-only view of the call it is serving.
Native connect server interceptors, registered once on
`GrpcNodeServer.serve`/`serveAll`, run around every call and can attach typed
values that handlers read from that context.

## The handler context

```ts
interface GrpcServerContext {
  readonly metadata: GrpcMetadata.GrpcMetadata;
  readonly method: { readonly tag: string; readonly kind: GrpcMethodKind };
  readonly signal: AbortSignal;
  readonly remainingTimeoutMs: () => number | undefined;
  readonly getContextValue: <T>(key: ContextKey<T>) => T;
}
```

A fresh context is built per RPC, after the interceptor chain has run, and a
streaming handler keeps that call's view for as long as it iterates.

- **`metadata`** — the incoming request metadata, normalized (lower-cased
  keys, `-bin` values decoded to bytes). It reflects the headers as delivered
  after upstream interceptors.
- **`method`** — the registry identity of the method: `tag` is exactly the
  registry tag (`demo.v1.UserService/GetUser`, no leading slash) and `kind`
  is one of `unary`, `server-streaming`, `client-streaming`,
  `bidi-streaming`.
- **`signal`** — connect's own call signal, the one to hand to external
  asynchronous work (an outgoing `fetch`, a database driver). It aborts when
  the client cancels and when the deadline expires, **but also when the call
  completes normally**: an abort is not by itself a cancellation. The
  runtime already maps aborts to `cancelled`/`deadline_exceeded` and
  interrupts the handler; handlers do not need to interpret the signal.
- **`remainingTimeoutMs()`** — the time left on the incoming deadline, read
  live from connect on every call: `undefined` when the call carries no
  deadline, a positive number while budget remains, `0` once it has elapsed.
  This observes the deadline; it does not enforce it (connect does) and does
  not start a second timer.
- **`getContextValue(key)`** — a typed value attached to this call by a
  server interceptor, or the key's declared default when nothing set it.
  Keys are connect's `createContextKey`; there is no second key factory.

### Propagating the remaining budget

`remainingTimeoutMs() === 0` means the upstream budget is **exhausted**. A
`timeoutMs` of zero (or any non-positive value) on an outgoing
`GrpcCallOptions` means **no deadline**. Use the opt-in
`GrpcDeadline.callOptions` helper to convert safely. It reads the context when
its Effect executes, fails with a typed `deadline_exceeded` error if the budget
is exhausted, and preserves a tighter positive caller timeout and metadata.
Without an upstream deadline, it returns your supplied options unchanged.

Complete asynchronous setup first, then read the budget immediately before
invoking the downstream RPC:

```ts
import { GrpcDeadline } from "@effect-grpc/effect-grpc";

// Inside a server handler; client is an acquired generated client service.
return Effect.gen(function* () {
  const request = yield* prepareDownstreamRequest();
  return yield* GrpcDeadline.callOptions(context, {
    timeoutMs: 500,
    metadata: [["x-request-id", requestId]],
  }).pipe(Effect.flatMap((options) => client.getUser(request, options)));
});
```

For server or bidi streams, wrap construction in `Stream.unwrap` to read at
subscription rather than when you build the stream:

```ts
return Stream.unwrap(
  GrpcDeadline.callOptions(context).pipe(
    Effect.map((options) => client.watchUsers(request, options)),
  ),
);
```

The helper works with all four method shapes. Reusing its Effect rereads the
budget; caching the returned options does not. Do not insert asynchronous
setup between reading the options and starting the RPC. A positive caller
timeout bounds that downstream call; it does not account for preceding setup.
The helper adds no timer or global policy: the downstream adapter enforces
the resulting timeout over the full call lifetime. Deadline propagation
remains explicitly opt-in.

## Server interceptors

`GrpcNodeServer.serve` and `serveAll` accept connect `Interceptor`s:

```ts
import type { Interceptor } from "@connectrpc/connect";

const log: Interceptor = (next) => async (request) => {
  console.log(`${request.method.parent.typeName}/${request.method.name}`);
  return next(request);
};

GrpcNodeServer.serveAll({
  host: "0.0.0.0",
  port: 50051,
  services: [userService, GrpcHealth.service],
  interceptors: [log],
});
```

The chain is installed once, at the connect adapter, and applies to every
service the server routes — health and reflection included; there are no
implicit exceptions. It runs **once per RPC**, not once per streamed
message: for a streaming call, `await next(request)` resolves with a
response whose `message` iterable may still be producing. To observe
streamed messages, wrap that iterable rather than awaiting the response — a
`finally` around `next(request)` is _not_ stream-completion cleanup:

```ts
const countMessages: Interceptor = (next) => async (request) => {
  const response = await next(request);
  if (!response.stream) return response;
  return {
    ...response,
    message: (async function* () {
      for await (const message of response.message) {
        count++;
        yield message;
      }
    })(),
  };
};
```

Order follows connect: the first interceptor in the array sees a request
first and its response last. Omitting the option or passing an empty array
leaves behavior unchanged, and the array you pass is never mutated.

Interceptors keep connect's Promise/`AsyncIterable` API. They are not Effect
code, and the runtime does not run them inside an Effect fiber. Callers who
assemble their own `ConnectRouter` and call `serve` with `routes` configure
interceptors the same way — the option is the only place the chain is
installed.

### Refusing a call

An interceptor that rejects a call throws a `ConnectError` at its own
boundary, without calling `next`. Handlers never run, and generated clients
receive the usual `GrpcStatusError` translation:

```ts
import { Code, ConnectError } from "@connectrpc/connect";

const requireToken: Interceptor = (next) => (request) => {
  if (!request.header.has("authorization")) {
    throw new ConnectError("missing token", Code.Unauthenticated);
  }
  return next(request);
};
```

This is not a complete authentication solution. Interceptors run after
connect has parsed (and decompressed) the request, so they do not protect
against unauthenticated payload parsing. The connect version this release
targets (2.1.1) has no earlier admission hook; an early-admission layer is a
separate design.

## Request-context keys

Attach data to a call with a key both sides import from one module. The
value here is minted on the server; do not derive a trusted value directly
from an arbitrary client header.

```ts
// requestContext.ts — one module owns the key.
import { createContextKey, type Interceptor } from "@connectrpc/connect";

export const requestIdKey = createContextKey<string | undefined>(undefined, {
  description: "server-generated request id",
});

export const attachRequestId: Interceptor = (next) => async (request) => {
  request.contextValues.set(requestIdKey, crypto.randomUUID());
  return next(request);
};
```

```ts
// A generated handler reads it back, typed.
getUser: (request, context) =>
  Effect.gen(function* () {
    const requestId = context.getContextValue(requestIdKey);
    yield* Effect.logInfo(`getUser ${request.id} (request ${requestId})`);
    // ...
  });
```

Two keys created with the same description are still distinct keys, and an
unset key returns its declared default. Values are per call: nothing set on
one call is visible to another, including a long-running stream that
outlives later calls.

### Making it an Effect service

Domain code should not depend on the gRPC context. Read the typed value in
the handler, build the application's own service value, and provide it
around the domain effect or stream with `Effect.provideService` /
`Stream.provideService`. Only that requirement is discharged; every other
dependency still flows through the generated handler builder and
`serveAll`, exactly as before:

```ts
export class CurrentRequest extends Context.Service<
  CurrentRequest,
  { readonly requestId: string | undefined; readonly method: string }
>()("app/CurrentRequest") {}

export const currentRequest = (context: CodegenSupport.GrpcServerContext) => ({
  requestId: context.getContextValue(requestIdKey),
  method: context.method.tag,
});

const implementation: UserServiceImplementation<Users> = {
  getUser: (request, context) =>
    findUser(request.id).pipe(
      Effect.provideService(CurrentRequest, currentRequest(context)),
    ),
  watchUsers: (request, context) =>
    userEvents(request.tenantId).pipe(
      Stream.provideService(CurrentRequest, currentRequest(context)),
    ),
};
```

The demo server (`examples/simple-server/src/requestContext.ts`) is the
compiled version of this pattern.

Typed context values are meant for immutable request data — an id, a
principal, a tenant. The library does not dispose values an interceptor
stores, so do not park a connection or a lock there. A request-scoped
_resource_ belongs to Effect's scoped acquisition inside the handler effect
or the response stream, where its lifetime matches the call. And, as
before, never provide server-lifetime dependencies to the short-lived
handlers effect itself — see the warning on
`GrpcServerProtocol.handlersEffect`.

## Observability boundary

The server spans and metrics described in [observability](observability.md)
are created inside the connect interception boundary. An interceptor that
refuses a call therefore refuses it _before_ a server span exists: the
client span records the status (`PERMISSION_DENIED`, ...) but there is no
server span for that call. Accepted calls keep their existing span parenting
and status recording.

## Compatibility

The new context fields are required on the context the server delivers.
Handlers that only read `metadata` keep working unchanged. Handwritten
handler tests or fixtures that construct a `GrpcServerContext` literal
themselves must now supply `method`, `signal`, `remainingTimeoutMs` and
`getContextValue`; `GrpcInvoker.layerInMemory` is unaffected — its handlers
receive the separate `GrpcInMemoryCall` context, and it does not run server
interceptors.
