import { Option, Schema } from "effect";
import type { ResponseExitEncoded } from "effect/unstable/rpc/RpcMessage";

import * as GrpcStatusError from "../GrpcStatusError.js";

const errorCodec = Schema.toCodecJson(GrpcStatusError.GrpcStatusError);
const encodeError = Schema.encodeUnknownSync(errorCodec);
const decodeError = Schema.decodeUnknownOption(errorCodec);

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
    cause: [
      {
        _tag: "Fail",
        error: encodeError(error),
      },
    ],
  },
});

export const errorFromExit = (
  exit: ResponseExitEncoded["exit"],
): GrpcStatusError.GrpcStatusError => {
  if (exit._tag === "Success") {
    return GrpcStatusError.internal("Unexpected successful RPC exit");
  }
  const failure = exit.cause.find((item) => item._tag === "Fail");
  if (failure?._tag === "Fail") {
    return Option.getOrElse(decodeError(failure.error), () =>
      GrpcStatusError.internal("Malformed gRPC status error", failure.error),
    );
  }
  const interrupt = exit.cause.find((item) => item._tag === "Interrupt");
  if (interrupt) {
    return GrpcStatusError.cancelled("RPC interrupted");
  }
  const die = exit.cause.find((item) => item._tag === "Die");
  return GrpcStatusError.internal("RPC handler defect", die?.defect);
};
