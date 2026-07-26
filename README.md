# effect-grpc

[![CI](https://github.com/bastikohn/effect-grpc/actions/workflows/ci.yml/badge.svg?branch=main)](https://github.com/bastikohn/effect-grpc/actions/workflows/ci.yml)
[![npm version](https://img.shields.io/npm/v/@effect-grpc/effect-grpc.svg)](https://www.npmjs.com/package/@effect-grpc/effect-grpc)
[![npm next](https://img.shields.io/npm/v/@effect-grpc/effect-grpc/next.svg?label=npm%40next)](https://www.npmjs.com/package/@effect-grpc/effect-grpc?activeTab=versions)
[![license](https://img.shields.io/npm/l/@effect-grpc/effect-grpc.svg)](LICENSE)

Effect-native gRPC prototypes for unary, server-streaming, client-streaming,
and bidi-streaming methods.

This workspace is new and intentionally small. The first prototype proves that
native gRPC paths can map through generated registries into Effect clients and
server handlers without introducing runtime `.proto` loading.

## Packages

- `@effect-grpc/effect-grpc`: runtime transport, status, metadata, and codegen
  support.
- `@effect-grpc/protoc-gen-effect-grpc`: build-time protobuf plugin.

## Examples

Private workspace packages under `examples/`:

- `simple-proto`, `simple-server`, `simple-client`: two demo protos (a simple
  unary/server-streaming service and a feature showcase covering client- and
  bidi-streaming methods, richer field shapes and well-known types) with a
  native gRPC server and client.

## Roadmap

Shipped:

- [x] All four gRPC method kinds: unary, server-streaming, client-streaming,
      and bidi-streaming.
- [x] Build-time `.proto` code generation with Buf/protoc
      (`protoc-gen-effect-grpc`).
- [x] TLS and mTLS on server and client.
- [x] Bearer authentication via `GrpcAuth` with static and auto-refreshing
      token layers.
- [x] Custom client interceptors and per-call/default timeouts.
- [x] gRPC health checking protocol (`grpc.health.v1`) via `GrpcHealth`.
- [x] gRPC server reflection (`grpc.reflection.v1`) via `GrpcReflection`.
- [x] OpenTelemetry tracing and metrics for clients and servers
      (semconv spans and duration histograms, exporter-agnostic; see
      [observability](docs/users/observability.md)).
- [x] Published beta releases: `0.1.x` (Effect v3, npm `latest`) and
      `1.0.0-beta.x` (Effect v4, npm `next`).

Planned:

- [ ] Custom server-side interceptors.
- [ ] Client retry policies.
- [ ] gRPC-Web support.
- [ ] Track Effect v4 to a stable release and drop the beta pin.
- [ ] Stable `1.0.0` release from the main (v4) line.

Runtime `.proto` loading stays out of scope: code generation is build-time by
design.

## Development

```sh
pnpm install
pnpm build
pnpm test:unit
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
`protoc-gen-effect-grpc` generates Effect schemas, method registries, client
facades, and server handlers.

Unsupported method kinds and protobuf field shapes fail codegen with a clear
error. See [limitations](docs/users/limitations.md) for the current support
policy.

### Generating from your own `.proto` files

Codegen runs through [Buf](https://buf.build). See the
[`protoc-gen-effect-grpc` README](packages/protoc-gen-effect-grpc/README.md)
for the install line, the `buf.yaml`/`buf.gen.yaml` recipe, and the generator
options.

## Docs

- [Getting started](docs/users/getting-started.md)
- [Observability](docs/users/observability.md)
- [Current limitations](docs/users/limitations.md)
- [Architecture](docs/contributors/architecture.md)
- [Protocol bridge](docs/contributors/protocol-bridge.md)

## Releases

Releases are managed with Changesets. Add a changeset for user-visible changes
to published packages, then merge the generated `Version packages` PR.
