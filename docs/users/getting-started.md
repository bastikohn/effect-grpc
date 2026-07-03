# Getting Started

`effect-grpc` is a build-time codegen plus runtime bridge for native gRPC
calls. Unary and server-streaming methods are backed by `effect/unstable/rpc`;
client-streaming and bidi-streaming methods bridge `Stream` and connect
iterables directly over the same transport.

1. Generate protobuf-es output with `protoc-gen-es`.
2. Generate Effect RPC glue with `protoc-gen-effect-grpc`.
3. Provide a generated client layer with `GrpcClientProtocol.layer`.
4. Serve generated server handlers with `GrpcNodeServer.serveAll`.

The demo packages show the current supported path:

```sh
pnpm demo:generate
pnpm demo:server
pnpm demo:client -- get-user --id 123
pnpm demo:client -- watch-users --tenant-id demo --count 3
```

Generated client methods accept `CodegenSupport.GrpcCallOptions`. User metadata
keys beginning with `x-effect-grpc-` are reserved for local runtime control and
are rejected before the native gRPC request is sent.

## Bearer Authentication

`GrpcAuth` attaches `authorization: Bearer <token>` metadata to every outgoing
call. The token is resolved per request, so rotated tokens are always current;
a per-call `authorization` header still wins.

For a fixed token:

```ts
Layer.unwrap(
  Effect.gen(function* () {
    const interceptor = yield* GrpcAuth.bearerInterceptor;
    return GrpcClientProtocol.layer({
      baseUrl,
      registry,
      interceptors: [interceptor],
    });
  }),
).pipe(Layer.provide(GrpcAuth.staticTokenLayer(token)));
```

For tokens minted by an auth endpoint and re-minted on a cadence, provide the
`BearerToken` service with `refreshingTokenLayer`: it acquires once, holds the
token in a `Ref`, and forks a scoped daemon that re-mints every `interval`.
Refresh failures are logged and skipped — the previous token stays in place
until the next tick — so bake retries for transient failures into `refresh`:

```ts
GrpcAuth.refreshingTokenLayer({
  acquire: login(credentials),
  refresh: (current) => renew(current).pipe(Effect.retry(transientOnly)),
  interval: "1 hour",
});
```

Token producers and consumers only share the `GrpcAuth.BearerToken` tag, so a
custom rotation strategy is just another layer that provides
`{ read: Effect<string> }`.

## TLS and mTLS

Both sides take first-class, PEM-encoded TLS options. The server terminates
TLS when `tls` is set on `serve`/`serveAll`; adding `clientCa` enables mTLS
(client certificates are required and verified against that CA):

```ts
GrpcNodeServer.serveAll({
  host: "0.0.0.0",
  port: 50051,
  tls: {
    key: fs.readFileSync("server.key"),
    cert: fs.readFileSync("server.crt"),
    clientCa: fs.readFileSync("client-ca.crt"), // omit for plain TLS
  },
  services: [...],
})
```

The client enables TLS through an `https://` base URL. `tls` refines the
handshake: `ca` sets the trust anchor for private CAs, `cert`/`key` present a
client certificate for mTLS:

```ts
GrpcClientProtocol.layer({
  baseUrl: "https://api.example.com:50051",
  registry: UserServiceGrpcRegistry,
  tls: {
    ca: fs.readFileSync("ca.crt"), // omit to use Node's trust store
    cert: fs.readFileSync("client.crt"), // mTLS only
    key: fs.readFileSync("client.key"),
  },
});
```

TLS handshake failures surface to callers as `GrpcStatusError` with code
`internal` (connect-node's mapping). `rejectUnauthorized: false` disables
server certificate verification for development against self-signed servers.
