#!/usr/bin/env node
import { CliConfig, Command, ValidationError } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Layer } from "effect";

import { codegenCommand } from "./cli.js";

const setFailureExitCode = Effect.sync(() => {
  process.exitCode = 1;
});

const reportCodegenError = (message: string) =>
  Effect.gen(function* () {
    yield* Console.error(`effect-grpc-codegen: ${message}`);
    yield* setFailureExitCode;
  });

const cli = Command.run(codegenCommand, {
  name: "effect-grpc-codegen",
  version: "0.1.0-alpha.0",
});

cli(process.argv).pipe(
  Effect.catchTag("CodegenError", (error) => reportCodegenError(error.message)),
  Effect.catchIf(ValidationError.isValidationError, () => setFailureExitCode),
  // Case-sensitive flags keep `-i` (input) and `-I` (proto root) distinct,
  // matching protoc's conventions.
  Effect.provide(
    Layer.merge(NodeContext.layer, CliConfig.layer({ isCaseSensitive: true })),
  ),
  NodeRuntime.runMain,
);
