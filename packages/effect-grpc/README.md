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

## Portable clients

Import transport-independent client APIs from `@effect-grpc/effect-grpc/client`:

```ts
import { GrpcClient } from "@effect-grpc/effect-grpc/client";

const clientLayer = GrpcClient.layerFromTransport({ registry, transport });
```

`transport` is a Connect `Transport`. `GrpcClient.metadataInterceptor` attaches
Effect-resolved authentication and other metadata to that transport. The portable
entrypoint also exports the invoker, registry, status, metadata, and codegen APIs.
Generated modules now import this entrypoint, and their bytes codecs use protobuf's
portable base64 implementation. Regenerate existing code to remove older Node
imports. Browser bundles need no Node built-ins or `Buffer` polyfill.

The root entrypoint retains `GrpcClientProtocol.makeTransport`, TLS options,
`GrpcClientProtocol.layer`, and all Node server APIs. Its existing
`layerFromTransport` and `metadataInterceptor` exports remain available. Native
Node transports continue supporting all four RPC shapes. Use `GrpcWebClient` below for browser transports.

## Browser transports

`GrpcWebClient` uses `@connectrpc/connect-web` to provide generated clients with
Connect or gRPC-Web over the browser Fetch API:

```ts
import { Layer } from "effect";
import { GrpcWebClient } from "@effect-grpc/effect-grpc/client";
import {
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "./generated/user_service_effect_grpc.js";

const clientLayer = UserServiceClientLayer.pipe(
  Layer.provide(
    GrpcWebClient.layer({
      protocol: "connect", // or "grpc-web"
      baseUrl: "https://api.example.com",
      registry: UserServiceGrpcRegistry,
    }),
  ),
);
```

Both protocols support unary and server-streaming calls. Client-streaming and
bidirectional calls fail with a typed `GrpcStatusError` (`unimplemented`) before
acquiring the request stream. Native Node clients still support all four shapes.
Transport construction occurs when the layer is acquired. Relative base URLs are
supported; set `serverAddress` explicitly when telemetry needs an absolute URL.

Pass connect-web options directly, including `interceptors`, `defaultTimeoutMs`,
`useBinaryFormat`, and a custom `fetch`. `GrpcClient.metadataInterceptor` supports
Effect-resolved authentication, and per-call metadata takes precedence over its
defaults. Binary metadata remains `Uint8Array` under `-bin` keys. Positive per-call
`timeoutMs` values bound the whole call, including streaming; consuming only part
of a response stream or interrupting its Effect cancels the underlying request.

The endpoint must speak Connect or gRPC-Web (directly or through a compatible
proxy). A native gRPC-only HTTP/2 endpoint is insufficient. For cross-origin
requests, the server or proxy must answer OPTIONS requests and allow the frontend
origin, Connect's `cors.allowedMethods` and `cors.allowedHeaders`, plus application
headers such as `authorization`, `traceparent`, and binary metadata keys. Expose
`cors.exposedHeaders` and any application response headers; gRPC-Web needs the
status headers exposed to preserve RPC errors. Connect's `cors` constants come
from `@connectrpc/connect`; they do not install CORS middleware automatically.

For cookie authentication, pass `fetch: (input, init) => fetch(input, {
...init, credentials: "include" })` and configure a specific allowed origin plus
`Access-Control-Allow-Credentials: true` on the server. A browser-blocked CORS or network failure has no observable
server RPC status and becomes a typed `unknown` status error. The adapter cannot
bypass browser origin policy.

Run the real-browser suite with `pnpm exec playwright install chromium` followed
by `pnpm test:browser`. CI runs it against Chromium with two separate local origins
for both protocols, including successful and denied preflights.
