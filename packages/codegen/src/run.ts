import { plugin } from "@effect-grpc/protoc-gen-effect-grpc";
import type { CodeGeneratorRequest } from "@bufbuild/protobuf/wkt";
import type { CodeGeneratorResponse } from "@bufbuild/protobuf/wkt";
import { Effect, FileSystem, Path } from "effect";
import type { ChildProcessSpawner } from "effect/unstable/process/ChildProcessSpawner";

import { compileProtos } from "./compile.js";
import { CodegenError, formatUnknown } from "./errors.js";
import {
  defaultPluginOptions,
  toParameterString,
  type PluginOptions,
} from "./options.js";
import { buildCodeGeneratorRequest } from "./request.js";
import { writeResponse } from "./write.js";

export interface GenerateOptions {
  /** Resolved `.proto` file paths (the expanded `-i` globs). */
  readonly inputs: ReadonlyArray<string>;
  /** Output directory (the `-o` flag). */
  readonly outDir: string;
  /** Import roots for resolving `import "..."` (the `-I` flags). */
  readonly importPaths?: ReadonlyArray<string>;
  /** Delete `outDir` before writing. Mirrors `buf.gen.yaml` `clean: true`. */
  readonly clean?: boolean;
  /** Generator options forwarded to the plugin. */
  readonly plugin?: Partial<PluginOptions>;
}

export interface GenerateResult {
  readonly files: ReadonlyArray<string>;
}

const runPlugin = (
  request: CodeGeneratorRequest,
): Effect.Effect<CodeGeneratorResponse, CodegenError> =>
  Effect.suspend(() => {
    try {
      return Effect.succeed(plugin.run(request));
    } catch (cause) {
      return Effect.fail(
        new CodegenError({
          message: `codegen failed: ${formatUnknown(cause)}`,
          cause,
        }),
      );
    }
  });

/**
 * Run the full pipeline: compile protos → build the request → drive the
 * existing `protoc-gen-effect-grpc` plugin in-process → write the response.
 */
export const generate = (
  options: GenerateOptions,
): Effect.Effect<
  GenerateResult,
  CodegenError,
  ChildProcessSpawner | FileSystem.FileSystem | Path.Path
> =>
  Effect.gen(function* () {
    if (options.inputs.length === 0) {
      return yield* Effect.fail(
        new CodegenError({ message: "no .proto inputs matched (-i)" }),
      );
    }

    const pluginOptions: PluginOptions = {
      ...defaultPluginOptions,
      ...options.plugin,
    };

    const { set, fileToGenerate } = yield* compileProtos({
      files: options.inputs,
      importPaths: options.importPaths ?? [],
    }).pipe(
      Effect.mapError(
        (cause) =>
          new CodegenError({
            message: cause.message,
            cause,
          }),
      ),
    );

    const request = buildCodeGeneratorRequest(
      set,
      fileToGenerate,
      toParameterString(pluginOptions),
    );

    const response = yield* runPlugin(request);
    const files = yield* writeResponse(
      response,
      options.outDir,
      options.clean ?? false,
    );

    return { files };
  });
