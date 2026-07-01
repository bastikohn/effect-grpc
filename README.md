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

### Generating from your own `.proto` files

Codegen runs through [Buf](https://buf.build). Install the plugins alongside
Buf in the package that owns your protos:

```sh
pnpm add -D @bufbuild/buf @bufbuild/protoc-gen-es @effect-grpc/protoc-gen-effect-grpc
```

Point Buf at your proto sources with a `buf.yaml`:

```yaml
version: v2
modules:
  - path: proto
lint:
  use:
    - STANDARD
```

Configure both plugins in a `buf.gen.yaml`. `protoc-gen-es` emits the
protobuf-es messages and `protoc-gen-effect-grpc` emits the Effect RPC glue:

```yaml
version: v2
clean: true
plugins:
  - local: protoc-gen-es
    out: src/generated
    opt:
      - target=ts
      - import_extension=js
  - local: protoc-gen-effect-grpc
    out: src/generated
    opt:
      - target=ts
      - import_extension=js
      - errors=grpc-status
      - methods=unary,server-streaming
```

Then run the generator. Buf resolves the `local:` plugins from `node_modules`,
so no global install is required:

```sh
# Generate into src/generated for every module in buf.yaml
pnpm exec buf generate

# Generate from an explicit input directory
pnpm exec buf generate proto

# Lint protos before generating
pnpm exec buf lint
```

A typical project wires this into a `generate` script (the demo proto uses
`"generate": "buf generate"`), so consumers run `pnpm --filter <pkg> generate`.

#### `protoc-gen-effect-grpc` options

Pass these under `opt:` in `buf.gen.yaml` (or as `--effect-grpc_opt` flags when
invoking `protoc` directly):

| Option                       | Values                                    | Default                  | Description                                          |
| ---------------------------- | ----------------------------------------- | ------------------------ | ---------------------------------------------------- |
| `import_extension`           | `js`, `ts`                                | `js`                     | Extension used in generated import paths.            |
| `errors`                     | `grpc-status`                             | `grpc-status`            | Error model for generated RPCs.                      |
| `methods`                    | comma list of `unary`, `server-streaming` | `unary,server-streaming` | Method kinds to emit.                                |
| `ignore_unsupported_methods` | `true`, `false`                           | `false`                  | Skip unsupported methods instead of failing codegen. |
| `int64`                      | `bigint`                                  | `bigint`                 | TypeScript representation for 64-bit integers.       |

Unknown options and unsupported values fail codegen with a clear error.

## Effect Compatibility

This prototype currently targets `effect@4.0.0-beta.92`. It uses
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
