# @effect-grpc/effect-grpc

[![npm version](https://img.shields.io/npm/v/@effect-grpc/effect-grpc.svg)](https://www.npmjs.com/package/@effect-grpc/effect-grpc)
[![license](https://img.shields.io/npm/l/@effect-grpc/effect-grpc.svg)](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)

Runtime support for generated [Effect](https://effect.website)-native gRPC
clients and servers. Pairs with the build-time generator
[`@effect-grpc/protoc-gen-effect-grpc`](https://www.npmjs.com/package/@effect-grpc/protoc-gen-effect-grpc)
(or the one-shot
[`@effect-grpc/codegen`](https://www.npmjs.com/package/@effect-grpc/codegen)
CLI), which turns `.proto` service definitions into typed Effect clients,
server handler layers, and registries — no runtime `.proto` loading.

## Install

```sh
pnpm add @effect-grpc/effect-grpc @bufbuild/protobuf @connectrpc/connect effect
```

`@bufbuild/protobuf`, `@connectrpc/connect`, and `effect` are peer
dependencies. This package is ESM-only and requires Node.js >= 22.

> [!NOTE]
> The current prerelease line targets `effect@4.0.0-beta.92` exactly (it
> builds on unstable Effect modules). Install from the `next` dist-tag for
> Effect v4 betas, or `latest` for the Effect v3 line.

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
  UserServiceHandlersLayer,
} from "./generated/demo/v1/user_service_effect_grpc.js";

const program = Effect.scoped(
  GrpcNodeServer.serveAll({
    host: "127.0.0.1",
    port: 50051,
    services: [
      {
        registry: UserServiceGrpcRegistry,
        handlers: UserServiceHandlersLayer({
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

## Error model

Generated RPCs use `GrpcStatusError` as their generic error schema. It is a
schema-backed tagged error, so generated client failures are decoded into real
`GrpcStatusError.GrpcStatusError` instances. Discriminate them by their `_tag`
(`"GrpcStatusError"`), e.g. with `Effect.catchTag`.

User metadata keys beginning with `x-effect-grpc-` are reserved for runtime
control and are rejected by generated clients. Metadata also follows the gRPC
`-bin` convention: `-bin` keys carry a `Uint8Array` (base64 on the wire, decoded
back to bytes for the handler), every other key a printable-ASCII `string`. A
value that contradicts its key fails the call with `invalid_argument`.

## Documentation

- [Getting started](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/getting-started.md)
  — codegen setup, TLS/mTLS, bearer authentication.
- [Current limitations](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/limitations.md)
- [Repository](https://github.com/bastikohn/effect-grpc)

## License

[Apache-2.0](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)
