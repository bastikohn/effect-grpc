import { glob } from "node:fs/promises";

import { Console, Effect } from "effect";
import { Command, Flag } from "effect/unstable/cli";

import { CodegenError, formatUnknown } from "./errors.js";
import { generate } from "./run.js";

const methodKinds = [
  "unary",
  "server-streaming",
  "client-streaming",
  "bidi-streaming",
] as const;
type Method = (typeof methodKinds)[number];

const parseMethods = (
  value: string,
): Effect.Effect<ReadonlyArray<Method>, CodegenError> =>
  Effect.suspend(() => {
    const methods = value
      .split(",")
      .map((item) => item.trim())
      .filter(Boolean);
    const unsupported = methods.find(
      (method) => !methodKinds.includes(method as Method),
    );

    if (unsupported !== undefined) {
      return Effect.fail(
        new CodegenError({
          message: `Unsupported methods value: ${unsupported}`,
        }),
      );
    }

    return Effect.succeed(methods as ReadonlyArray<Method>);
  });

const expandInputGlobs = (
  patterns: ReadonlyArray<string>,
): Effect.Effect<ReadonlyArray<string>, CodegenError> =>
  Effect.tryPromise({
    try: async () => {
      const matches = new Set<string>();
      for (const pattern of patterns) {
        for await (const match of glob(pattern)) {
          if (match.endsWith(".proto")) {
            matches.add(match);
          }
        }
      }
      return [...matches].sort();
    },
    catch: (cause) =>
      new CodegenError({
        message: `could not expand input globs: ${formatUnknown(cause)}`,
        cause,
      }),
  });

export const codegenCommand = Command.make(
  "effect-grpc-codegen",
  {
    input: Flag.string("input").pipe(
      Flag.withAlias("i"),
      Flag.atLeast(1),
      Flag.withDescription(".proto file or glob"),
    ),
    output: Flag.string("output").pipe(
      Flag.withAlias("o"),
      Flag.withDescription("output directory"),
    ),
    protoPath: Flag.string("proto-path").pipe(
      Flag.withAlias("I"),
      Flag.atMost(Number.MAX_SAFE_INTEGER),
      Flag.withDescription("import root"),
    ),
    clean: Flag.boolean("clean").pipe(
      Flag.withDefault(false),
      Flag.withDescription("delete output directory before writing"),
    ),
    importExtension: Flag.choice("import-extension", ["js", "ts"]).pipe(
      Flag.withDefault("js"),
      Flag.withDescription("generated import extension"),
    ),
    errors: Flag.choice("errors", ["grpc-status"]).pipe(
      Flag.withDefault("grpc-status"),
      Flag.withDescription("error model"),
    ),
    methods: Flag.string("methods").pipe(
      Flag.withDefault(methodKinds.join(",")),
      Flag.withDescription(`comma list of ${methodKinds.join(",")}`),
    ),
    int64: Flag.choice("int64", ["bigint"]).pipe(
      Flag.withDefault("bigint"),
      Flag.withDescription("64-bit integer representation"),
    ),
  },
  (config) =>
    Effect.gen(function* () {
      const inputs = yield* expandInputGlobs(config.input);
      const methods = yield* parseMethods(config.methods);
      const { files } = yield* generate({
        inputs,
        outDir: config.output,
        importPaths: config.protoPath,
        clean: config.clean,
        plugin: {
          importExtension: config.importExtension,
          errors: config.errors,
          methods,
          int64: config.int64,
        },
      });

      yield* Console.log(
        `effect-grpc-codegen: wrote ${files.length} file(s) to ${config.output}`,
      );
    }),
).pipe(
  Command.withDescription(
    "Compile .proto files and run the effect-grpc generator.",
  ),
);
