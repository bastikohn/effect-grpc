# @effect-grpc/protoc-gen-effect-grpc

[![npm version](https://img.shields.io/npm/v/@effect-grpc/protoc-gen-effect-grpc.svg)](https://www.npmjs.com/package/@effect-grpc/protoc-gen-effect-grpc)
[![license](https://img.shields.io/npm/l/@effect-grpc/protoc-gen-effect-grpc.svg)](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)

Build-time protobuf generator for
[`effect-grpc`](https://github.com/bastikohn/effect-grpc). It emits one
`*_effect_grpc.ts` file beside protobuf-es output, containing typed
[Effect](https://effect.website) clients, server handler layers, and
registries for the runtime package
[`@effect-grpc/effect-grpc`](https://www.npmjs.com/package/@effect-grpc/effect-grpc).

All four gRPC method kinds are generated. Clients invoke every kind through
the `GrpcInvoker` seam; on the server, unary and server-streaming handlers run
as Effect RPCs while client-streaming and bidi-streaming handlers use the
direct streaming bridge.

If you don't already use [Buf](https://buf.build) or protoc, consider
[`@effect-grpc/codegen`](https://www.npmjs.com/package/@effect-grpc/codegen)
instead — a self-contained CLI that runs protobuf-es and this generator in a
single `npx` command.

## Install

```sh
pnpm add -D @bufbuild/buf @bufbuild/protoc-gen-es @effect-grpc/protoc-gen-effect-grpc
```

Requires Node.js >= 22.

## Usage with Buf

Configure both plugins in a `buf.gen.yaml`. `protoc-gen-es` emits the
protobuf-es messages and `protoc-gen-effect-grpc` emits the effect-grpc glue:

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
```

Buf resolves the `local:` plugins from `node_modules`, so no global install is
required:

```sh
pnpm exec buf generate
```

## Options

Pass these under `opt:` in `buf.gen.yaml` (or as `--effect-grpc_opt` flags when
invoking `protoc` directly):

| Option             | Values                                                                          | Default       | Description                                    |
| ------------------ | ------------------------------------------------------------------------------- | ------------- | ---------------------------------------------- |
| `import_extension` | `js`, `ts`                                                                      | `js`          | Extension used in generated import paths.      |
| `errors`           | `grpc-status`                                                                   | `grpc-status` | Error model for generated RPCs.                |
| `methods`          | comma list of `unary`, `server-streaming`, `client-streaming`, `bidi-streaming` | all kinds     | Method kinds to emit.                          |
| `int64`            | `bigint`                                                                        | `bigint`      | TypeScript representation for 64-bit integers. |

Unknown options and unsupported values fail codegen with a clear error.

## Supported protobuf shapes

Scalar fields (including `optional` and 64-bit-as-`bigint`), messages
(including nested and cross-package imported), enums, repeated, map, and oneof
fields, plus the common protobuf well-known types used by service APIs:
timestamps, durations, wrapper values, `Any`, `Struct`, `Value`, `ListValue`,
and `FieldMask`. Codegen fails fast for import cycles, unsupported well-known
types, and proto2 required/default behavior — see
[current limitations](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/limitations.md).

## Documentation

- [Getting started](https://github.com/bastikohn/effect-grpc/blob/main/docs/users/getting-started.md)
- [Repository](https://github.com/bastikohn/effect-grpc)

## License

[Apache-2.0](https://github.com/bastikohn/effect-grpc/blob/main/LICENSE)
