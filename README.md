# effect-grpc

Effect RPC-backed native gRPC prototypes for unary and server-streaming methods.

This workspace is new and intentionally small. The first prototype proves that
native gRPC paths can map through generated registries into `@effect/Rpc`
handlers and clients without introducing runtime `.proto` loading or unsupported
method kinds.

## Packages

- `@effect-grpc/effect-grpc`: runtime transport, status, metadata, and codegen
  support.
- `@effect-grpc/protoc-gen-effect-grpc`: build-time protobuf plugin.
- `@effect-grpc/simple-proto`: demo proto and generated TypeScript.
- `@effect-grpc/simple-server`: demo native gRPC server.
- `@effect-grpc/simple-client`: demo native gRPC client.

## Supported

- Unary gRPC methods.
- Server-streaming gRPC methods.
- Generic `GrpcStatusError` failures for generated RPCs.
- Build-time `.proto` code generation with Buf/protoc.

## Not Supported Yet

- Client-streaming and bidirectional-streaming methods.
- Runtime `.proto` loading or reflection.
- Typed protobuf error options.
- TLS/mTLS, gRPC-Web, health checks, retries, and custom interceptors.

## Development

```sh
pnpm install
pnpm build
pnpm test
pnpm demo:generate
```

Run the demo server in one terminal:

```sh
pnpm demo:server
```

Run demo clients in another terminal:

```sh
pnpm demo:client -- get-user --id 123
pnpm demo:client -- get-user --id missing
pnpm demo:client -- watch-users --tenant-id demo --count 3
```

The public contributor commands intentionally stay on `pnpm`. Some scripts use
Vite+ internally for linting, formatting, and workspace task orchestration.

## Code Generation

`.proto` files are consumed at build time. `@bufbuild/protoc-gen-es` generates
protobuf-es descriptors and message types, and
`protoc-gen-effect-grpc` generates Effect schemas, RPC declarations, registries,
client facades, and server handler layers.

Unsupported method kinds and protobuf field shapes fail codegen by default with
a clear error. See [limitations](docs/users/limitations.md) for the current
support policy.

## Effect Compatibility

This prototype currently targets `effect@4.0.0-beta.79`. It uses
`effect/unstable/rpc`, so compatibility is intentionally pinned. Effect beta
upgrades must update tests, generated code, and package smoke together.

## Error Model

Every generated RPC currently uses one generic `GrpcStatusError` schema. Native
Connect/gRPC errors are translated at the transport boundary and delivered to
Effect RPC callers as normal RPC failure exits.

`GrpcStatusError` is a schema-backed tagged error, so generated client failures
are decoded into real `GrpcStatusError` instances. Discriminate them by their
`_tag` (`"GrpcStatusError"`) — e.g. with `Effect.catchTag("GrpcStatusError", …)`
or a `_tag === "GrpcStatusError"` check — rather than relying on `instanceof`.

## Docs

- [Getting started](docs/users/getting-started.md)
- [Current limitations](docs/users/limitations.md)
- [Architecture](docs/contributors/architecture.md)
- [Protocol bridge](docs/contributors/protocol-bridge.md)
- [Codegen](docs/contributors/codegen.md)
- [Testing](docs/contributors/testing.md)

## Releases

Releases are managed with Changesets. Add a changeset for user-visible changes
to published packages, then merge the generated `Version packages` PR.
