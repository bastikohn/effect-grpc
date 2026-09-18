# @effect-grpc/effect-grpc

[![npm version](https://img.shields.io/npm/v/@effect-grpc/effect-grpc.svg)](https://www.npmjs.com/package/@effect-grpc/effect-grpc)
[![license](https://img.shields.io/npm/l/@effect-grpc/effect-grpc.svg)](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)

Runtime support for generated [Effect](https://effect.website)-native gRPC
clients and servers. Pairs with the build-time generator
[`@effect-grpc/protoc-gen-effect-grpc`](https://www.npmjs.com/package/@effect-grpc/protoc-gen-effect-grpc),
which turns `.proto` service definitions into typed Effect clients, server
handlers, and registries — no runtime `.proto` loading.

## Install

```sh
pnpm add @effect-grpc/effect-grpc @bufbuild/protobuf @connectrpc/connect effect
```

`@bufbuild/protobuf`, `@connectrpc/connect`, and `effect` are peer
dependencies. This package is ESM-only and requires Node.js >= 22.
TypeScript consumers require TypeScript >= 5.9.3; CI verifies packed packages
and generated clients with TypeScript 5.9.3 and 7.0.2.

> [!NOTE]
> The current prerelease line targets `effect@4.0.0-rc.115` exactly (it
> builds on unstable Effect modules). Install from the `next` dist-tag for
> Effect v4 prereleases, or `latest` for the Effect v3 line.

## Quickstart

Generate code from your `.proto` files first (see
[getting started](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/getting-started.md)).
Then serve the generated handlers:

```ts
import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";
import { GrpcNodeServer } from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlers,
} from "./generated/demo/v1/user_service_effect_grpc.js";

const program = Effect.scoped(
  GrpcNodeServer.serveAll({
    host: "127.0.0.1",
    port: 50051,
    services: [
      {
        registry: UserServiceGrpcRegistry,
        handlers: UserServiceHandlers({
          getUser: (request) =>
            Effect.succeed({ user: { id: request.id, name: "Ada" } }),
        }),
      },
    ],
  }),
);

NodeRuntime.runMain(program);
```

And call it with the generated client:

```ts
import { Effect, Layer } from "effect";
import { GrpcClientProtocol } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "./generated/demo/v1/user_service_effect_grpc.js";

const clientLayer = UserServiceClientLayer.pipe(
  Layer.provide(
    GrpcClientProtocol.layer({
      baseUrl: "http://127.0.0.1:50051",
      registry: UserServiceGrpcRegistry,
    }),
  ),
);

const program = Effect.gen(function* () {
  const client = yield* UserServiceClient;
  const { user } = yield* client.getUser({ id: "123" });
  return user;
}).pipe(Effect.provide(clientLayer));
```

## Features

- All four gRPC method kinds, bridging `Effect`/`Stream` values and connect
  calls directly over one transport — the `GrpcInvoker` seam on the client and
  a unified handlers map on the server.
- TLS and mTLS on both sides: `tls` on `GrpcNodeServer.serve`/`serveAll` and on
  `GrpcClientProtocol.layer`/`makeTransport`.
- Bearer authentication via `GrpcAuth`: a per-request `authorization` header
  interceptor plus static and auto-refreshing token layers.
- Custom client interceptors: pass connect `Interceptor`s via `interceptors`,
  or build metadata-resolving ones with
  `GrpcClientProtocol.metadataInterceptor`.
- Custom server interceptors via `interceptors` on
  `GrpcNodeServer.serve`/`serveAll`, and a per-call handler context with
  the method identity, the call signal, the remaining deadline, and typed
  values interceptors attach.
- Opt-in downstream deadline propagation with `GrpcDeadline.callOptions`:
  read the live incoming budget immediately before an RPC, preserve a tighter
  caller timeout, and fail before downstream work when the budget is exhausted.

## Error model

Generated RPCs fail with `GrpcStatusError`, a `Data.TaggedError`. Failures
coming off the wire are built from the peer's connect error by
`GrpcStatusError.fromConnectError`, and handler failures are turned back into
one by `GrpcStatusError.toConnectError`, so both ends see the same status code,
message, and metadata. Discriminate them by their `_tag`
(`"GrpcStatusError"`), e.g. with `Effect.catchTag`.

User metadata keys beginning with `x-effect-grpc-` are reserved for runtime
control and are rejected by generated clients. Metadata also follows the gRPC
`-bin` convention: `-bin` keys carry a `Uint8Array` (base64 on the wire, decoded
back to bytes for the handler), every other key a printable-ASCII `string`. A
value that contradicts its key fails the call with `invalid_argument`.

## Documentation

- [Getting started](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/getting-started.md)
  — codegen setup, TLS/mTLS, bearer authentication.
- [Server call context and interceptors](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/server-context.md)
- [Current limitations](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/limitations.md)
- [Repository](https://github.com/bastikohn/effect-grpc)

## License

[Apache-2.0](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)

## Opt-in unary retries

Declare replay safety explicitly for each unary operation you choose to retry:

```ts
import { GrpcRetry } from "@effect-grpc/effect-grpc";

const response =
  yield *
  GrpcRetry.unary((options) => client.getUser({ id: "42" }, options), {
    retrySafe: true,
    maxAttempts: 3,
    retryableCodes: ["unavailable", "resource_exhausted"],
    callOptions: { timeoutMs: 1500 },
    // In a server handler, optionally pass its live upstream budget:
    context,
  });
```

`retrySafe: true` is an application promise: replaying this operation must be safe,
for example a read or an operation protected by an application idempotency key.
The library cannot infer this from a method name or guarantee whether a failed
attempt committed server-side. Ordinary generated calls keep their existing
single-attempt behavior. This helper accepts unary Effects; it does not replay
request streams or reconnect response streams.

`maxAttempts` includes the first attempt. Only the listed typed gRPC status errors
retry; defects and interruption pass through, and attempt exhaustion preserves the
last error object. The policy requires a positive safe integer attempt count and
finite, nonnegative delay limits; invalid policies fail as defects before invoking
the callback.

Backoff doubles from `initialDelayMs` (default 100), capped at `maxDelayMs` (default
1000). Full jitter chooses a delay from zero up to that cap on each retry; set
`jitter: false` for fixed exponential delays. Randomness uses Effect's Random service.
Interruption cancels the active attempt or backoff and prevents further attempts.

Call options are sampled when the returned Effect runs. A positive `timeoutMs` is
one overall budget covering every attempt, asynchronous setup, and backoff.
Remaining time is recomputed before each attempt, and optional `context` budgets
are read live through `GrpcDeadline.callOptions`; the tighter deadline wins. An
exhausted budget fails before the next callback. Pass the callback's options to
the generated method and place asynchronous per-attempt setup inside its returned
Effect. The helper also enforces the budget if that setup is slow or never finishes.

A transport's `defaultTimeoutMs` is invisible to this callback helper and may
apply separately to each transport attempt. Supply `callOptions.timeoutMs` or a
live upstream `context` when an overall time limit is required. Without either,
attempts are still bounded in number, but elapsed time is not bounded by the helper.
