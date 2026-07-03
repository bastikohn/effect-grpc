import { glob } from "node:fs/promises";

import { Command, Options } from "@effect/cli";
import { Console, Effect } from "effect";

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
    input: Options.text("input").pipe(
      Options.withAlias("i"),
      Options.atLeast(1),
      Options.withDescription(".proto file or glob"),
    ),
    output: Options.text("output").pipe(
      Options.withAlias("o"),
      Options.withDescription("output directory"),
    ),
    protoPath: Options.text("proto-path").pipe(
      Options.withAlias("I"),
      Options.repeated,
      Options.withDescription("import root"),
    ),
    clean: Options.boolean("clean").pipe(
      Options.withDescription("delete output directory before writing"),
    ),
    importExtension: Options.choice("import-extension", ["js", "ts"]).pipe(
      Options.withDefault("js" as const),
      Options.withDescription("generated import extension"),
    ),
    errors: Options.choice("errors", ["grpc-status"]).pipe(
      Options.withDefault("grpc-status" as const),
      Options.withDescription("error model"),
    ),
    methods: Options.text("methods").pipe(
      Options.withDefault(methodKinds.join(",")),
      Options.withDescription(`comma list of ${methodKinds.join(",")}`),
    ),
    int64: Options.choice("int64", ["bigint"]).pipe(
      Options.withDefault("bigint" as const),
      Options.withDescription("64-bit integer representation"),
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
