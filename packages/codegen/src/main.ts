#!/usr/bin/env node
import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect } from "effect";
import { CliError, Command } from "effect/unstable/cli";

import { codegenCommand } from "./cli.js";

const setFailureExitCode = Effect.sync(() => {
  process.exitCode = 1;
});

const reportCodegenError = (message: string) =>
  Effect.gen(function* () {
    yield* Console.error(`effect-grpc-codegen: ${message}`);
    yield* setFailureExitCode;
  });

Command.run(codegenCommand, { version: "0.1.0-alpha.0" }).pipe(
  Effect.catchTag("CodegenError", (error) => reportCodegenError(error.message)),
  Effect.catch((error) =>
    CliError.isCliError(error) ? setFailureExitCode : Effect.fail(error),
  ),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
