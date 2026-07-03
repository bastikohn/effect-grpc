# @effect-grpc/codegen

Self-contained CLI for `effect-grpc` code generation. It compiles `.proto`
files with the package-local `@bufbuild/buf` binary, runs protobuf-es, and then
runs the `@effect-grpc/protoc-gen-effect-grpc` generator in a single step — no
global Buf or protoc installation, no `buf.gen.yaml`.

```sh
npx @effect-grpc/codegen -i ./protos/*.proto -o ./generated/
```

For projects that already use Buf, keep using `@effect-grpc/protoc-gen-effect-grpc`
as a `buf.gen.yaml` plugin. This package is the alternative for consumers who
want a one-shot `npx` command instead.

## Usage

```
effect-grpc-codegen -i <glob...> -o <dir> [options]

  -i, --input <glob>                 .proto file or glob (repeatable)
  -o, --output <dir>                 output directory
  -I, --proto-path <dir>             import root (repeatable)
      --clean                        delete <dir> before writing
      --import-extension <js|ts>     generated import extension (default: js)
      --errors <grpc-status>         error model (default: grpc-status)
      --methods <list>               comma list of unary,server-streaming,
                                     client-streaming,bidi-streaming
      --int64 <bigint>               64-bit integer representation (default: bigint)
```

The options map 1:1 to the plugin options documented in the root README. The
first `-I, --proto-path` value is used as the Buf module root; if omitted, the
current directory is used.

## How it works

1. Expand the `-i` globs into a concrete file list.
2. Compile the `.proto` sources into a `FileDescriptorSet` with the bundled
   `@bufbuild/buf` CLI.
3. Wrap the descriptors in a `CodeGeneratorRequest`, with the user's files in
   `fileToGenerate` and the options serialized into `parameter`.
4. Drive protobuf-es in-process to emit `*_pb.ts`.
5. Drive the existing effect-grpc plugin in-process via `plugin.run(request)` —
   same logic, unsupported-shape guards, and output as the Buf path.
6. Write both `CodeGeneratorResponse` file sets under `-o`.

The pipeline is also available programmatically:

```ts
import { NodeContext } from "@effect/platform-node";
import { generate } from "@effect-grpc/codegen";
import { Effect } from "effect";

await Effect.runPromise(
  generate({
    inputs: ["proto/demo/v1/user_service.proto"],
    outDir: "src/generated",
    plugin: { methods: ["unary"] },
  }).pipe(Effect.provide(NodeContext.layer)),
);
```
