import type { CodeGeneratorResponse } from "@bufbuild/protobuf/wkt";
import { Effect, FileSystem, Path } from "effect";

import { CodegenError, formatUnknown } from "./errors.js";

/**
 * Write a {@link CodeGeneratorResponse} to disk under `outDir`.
 *
 * Surfaces a plugin-reported `error` as a failed Effect so no partial output is
 * written, and honors `clean` to mirror `buf.gen.yaml`'s `clean: true`.
 */
export const writeResponse = (
  response: CodeGeneratorResponse,
  outDir: string,
  clean: boolean,
): Effect.Effect<
  ReadonlyArray<string>,
  CodegenError,
  FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    if (response.error !== undefined && response.error !== "") {
      return yield* Effect.fail(
        new CodegenError({ message: `codegen failed: ${response.error}` }),
      );
    }

    if (clean) {
      yield* fs.remove(outDir, { recursive: true, force: true }).pipe(
        Effect.mapError(
          (cause) =>
            new CodegenError({
              message: `could not clean output directory ${outDir}: ${formatUnknown(cause)}`,
              cause,
            }),
        ),
      );
    }

    const written: Array<string> = [];
    for (const file of response.file) {
      const target = path.join(outDir, file.name);
      yield* fs.makeDirectory(path.dirname(target), { recursive: true }).pipe(
        Effect.mapError(
          (cause) =>
            new CodegenError({
              message: `could not create output directory for ${target}: ${formatUnknown(cause)}`,
              cause,
            }),
        ),
      );
      yield* fs.writeFileString(target, file.content ?? "").pipe(
        Effect.mapError(
          (cause) =>
            new CodegenError({
              message: `could not write ${target}: ${formatUnknown(cause)}`,
              cause,
            }),
        ),
      );
      written.push(target);
    }

    return written;
  });
