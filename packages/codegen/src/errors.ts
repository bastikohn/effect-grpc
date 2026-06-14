import { Data } from "effect";

export class BufBuildError extends Data.TaggedError("BufBuildError")<{
  readonly message: string;
  readonly root: string;
  readonly files: ReadonlyArray<string>;
  readonly exitCode?: number;
  readonly stderr?: string;
  readonly cause?: unknown;
}> {}

export class CodegenError extends Data.TaggedError("CodegenError")<{
  readonly message: string;
  readonly cause?: unknown;
}> {}

export const formatUnknown = (cause: unknown): string =>
  cause instanceof Error ? cause.message : String(cause);
