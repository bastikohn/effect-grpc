import { Buffer } from "node:buffer";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { fromBinary } from "@bufbuild/protobuf";
import type { FileDescriptorSet } from "@bufbuild/protobuf/wkt";
import { FileDescriptorSetSchema } from "@bufbuild/protobuf/wkt";
import { Effect, Stream } from "effect";
import { ChildProcess } from "effect/unstable/process";
import type { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";

import { BufBuildError, CodegenError, formatUnknown } from "./errors.js";

export interface CompileInput {
  /** Resolved `.proto` file paths to generate from (the `-i` globs, expanded). */
  readonly files: ReadonlyArray<string>;
  /** Import roots used to resolve `import "..."` statements (the `-I` flags). */
  readonly importPaths: ReadonlyArray<string>;
}

export interface CompileResult {
  /**
   * The compiled descriptors for the input files *and* their transitive
   * imports. This becomes `CodeGeneratorRequest.protoFile`.
   */
  readonly set: FileDescriptorSet;
  /**
   * The descriptor `name`s the user asked to generate (a subset of `set.file`).
   * These map 1:1 to `CodeGeneratorRequest.fileToGenerate`, so they must match
   * the `name` field the compiler assigned to each input file (i.e. the path
   * relative to its import root, e.g. `demo/v1/user_service.proto`).
   */
  readonly fileToGenerate: ReadonlyArray<string>;
}

const resolveBufBin = (input: CompileInput, root: string) =>
  Effect.suspend(() => {
    try {
      return Effect.succeed(
        fileURLToPath(import.meta.resolve("@bufbuild/buf/bin/buf")),
      );
    } catch (cause) {
      return Effect.fail(
        new BufBuildError({
          message: `could not resolve @bufbuild/buf: ${formatUnknown(cause)}`,
          root,
          files: input.files,
          cause,
        }),
      );
    }
  });

const concatChunks = (chunks: ReadonlyArray<Uint8Array>): Buffer =>
  Buffer.concat(chunks.map((chunk) => Buffer.from(chunk)));

const decodeDescriptorSet = (bytes: Uint8Array) =>
  Effect.suspend(() => {
    try {
      return Effect.succeed(fromBinary(FileDescriptorSetSchema, bytes));
    } catch (cause) {
      return Effect.fail(
        new CodegenError({
          message: `could not decode buf descriptor set: ${formatUnknown(cause)}`,
          cause,
        }),
      );
    }
  });

/**
 * Compile `.proto` sources into a {@link FileDescriptorSet}.
 *
 * This is the one integration boundary of the package: the job Buf/protoc
 * normally does before invoking the plugin. The CLI brings its own
 * `@bufbuild/buf` binary, so consumers do not need a global Buf or protoc
 * installation.
 */
export const compileProtos = (
  input: CompileInput,
): Effect.Effect<
  CompileResult,
  BufBuildError | CodegenError,
  ChildProcessSpawner
> =>
  Effect.gen(function* () {
    const root = input.importPaths[0] ?? ".";
    const rootPath = path.resolve(root);
    const fileToGenerate: Array<string> = [];
    for (const file of input.files) {
      const relative = path.relative(rootPath, path.resolve(file));
      if (relative === ".." || relative.startsWith(`..${path.sep}`)) {
        return yield* Effect.fail(
          new CodegenError({
            message: `${file} is outside the proto root ${rootPath}. Pass -I/--proto-path pointing at the directory that contains this proto file.`,
          }),
        );
      }
      fileToGenerate.push(relative.split(path.sep).join("/"));
    }
    const bufBin = yield* resolveBufBin(input, root);
    const args = [
      bufBin,
      "build",
      rootPath,
      "--as-file-descriptor-set",
      ...input.files.flatMap((file) => ["--path", path.resolve(file)]),
      "-o",
      "-#format=binpb",
    ];
    const handle = yield* ChildProcess.make(process.execPath, args).pipe(
      Effect.mapError(
        (cause) =>
          new BufBuildError({
            message: `could not start buf: ${formatUnknown(cause)}`,
            root,
            files: input.files,
            cause,
          }),
      ),
    );
    const output = yield* Effect.all(
      {
        stdout: Stream.runCollect(handle.stdout),
        stderr: Stream.runCollect(handle.stderr),
        exitCode: handle.exitCode,
      },
      { concurrency: "unbounded" },
    ).pipe(
      Effect.mapError(
        (cause) =>
          new BufBuildError({
            message: `buf failed: ${formatUnknown(cause)}`,
            root,
            files: input.files,
            cause,
          }),
      ),
    );
    const stderr = concatChunks(output.stderr).toString("utf8");
    const exitCode = Number(output.exitCode);

    if (exitCode !== 0) {
      return yield* Effect.fail(
        new BufBuildError({
          message: `buf build failed with exit code ${exitCode}${
            stderr.trim() === "" ? "" : `: ${stderr.trim()}`
          }`,
          root,
          files: input.files,
          exitCode,
          stderr,
        }),
      );
    }

    const set = yield* decodeDescriptorSet(concatChunks(output.stdout));
    return { set, fileToGenerate };
  }).pipe(Effect.scoped);
