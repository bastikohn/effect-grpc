import type { ResponseExitEncoded } from "@effect/rpc/RpcMessage";
import { Option, Schema } from "effect";

import * as GrpcStatusError from "../GrpcStatusError.js";

const encodeError = Schema.encodeUnknownSync(GrpcStatusError.GrpcStatusError);
const decodeError = Schema.decodeUnknownOption(GrpcStatusError.GrpcStatusError);

type CauseEncoded = Schema.CauseEncoded<unknown, unknown>;

export const successExit = (
  requestId: string,
  value: unknown,
): ResponseExitEncoded => ({
  _tag: "Exit",
  requestId,
  exit: {
    _tag: "Success",
    value,
  },
});

export const failureExit = (
  requestId: string,
  error: GrpcStatusError.GrpcStatusError,
): ResponseExitEncoded => ({
  _tag: "Exit",
  requestId,
  exit: {
    _tag: "Failure",
    cause: {
      _tag: "Fail",
      error: encodeError(error),
    },
  },
});

// The encoded cause is a tree (`Sequential`/`Parallel` nodes); flatten it so
// the terminal causes can be inspected in priority order.
const flattenCause = (cause: CauseEncoded): Array<CauseEncoded> => {
  switch (cause._tag) {
    case "Sequential":
    case "Parallel":
      return [...flattenCause(cause.left), ...flattenCause(cause.right)];
    default:
      return [cause];
  }
};

export const errorFromExit = (
  exit: ResponseExitEncoded["exit"],
): GrpcStatusError.GrpcStatusError => {
  if (exit._tag === "Success") {
    return GrpcStatusError.internal("Unexpected successful RPC exit");
  }
  const causes = flattenCause(exit.cause);
  const failure = causes.find((item) => item._tag === "Fail");
  if (failure?._tag === "Fail") {
    return Option.getOrElse(decodeError(failure.error), () =>
      GrpcStatusError.internal("Malformed gRPC status error", failure.error),
    );
  }
  const interrupt = causes.find((item) => item._tag === "Interrupt");
  if (interrupt) {
    return GrpcStatusError.cancelled("RPC interrupted");
  }
  const die = causes.find((item) => item._tag === "Die");
  return GrpcStatusError.internal(
    "RPC handler defect",
    die?._tag === "Die" ? die.defect : undefined,
  );
};
