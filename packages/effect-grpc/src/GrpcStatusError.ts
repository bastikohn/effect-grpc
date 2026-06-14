import { ConnectError } from "@connectrpc/connect";
import { Schema } from "effect";

import * as GrpcMetadata from "./GrpcMetadata.js";
import * as GrpcStatusCode from "./GrpcStatusCode.js";
import type { GrpcStatusCode as GrpcStatusCodeType } from "./GrpcStatusCode.js";

export class GrpcStatusError extends Schema.TaggedErrorClass<GrpcStatusError>()(
  "GrpcStatusError",
  {
    code: GrpcStatusCode.schema,
    message: Schema.String,
    metadata: GrpcMetadata.schema,
    trailers: GrpcMetadata.schema,
    details: Schema.Array(Schema.Unknown),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

export const make = (options: {
  readonly code: GrpcStatusCodeType;
  readonly message: string;
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  readonly trailers?: GrpcMetadata.GrpcMetadata;
  readonly details?: ReadonlyArray<unknown>;
  readonly cause?: unknown;
}) =>
  new GrpcStatusError({
    code: options.code,
    message: options.message,
    metadata: options.metadata ?? GrpcMetadata.empty,
    trailers: options.trailers ?? GrpcMetadata.empty,
    details: options.details ?? [],
    ...(options.cause === undefined ? {} : { cause: options.cause }),
  });

export const unknown = (cause: unknown) =>
  make({
    code: "unknown",
    message: cause instanceof Error ? cause.message : String(cause),
    cause,
  });

export const internal = (message: string, cause?: unknown) =>
  make({ code: "internal", message, cause });

export const notFound = (message: string) =>
  make({ code: "not_found", message });

export const invalidArgument = (message: string, cause?: unknown) =>
  make({ code: "invalid_argument", message, cause });

export const unavailable = (message: string, cause?: unknown) =>
  make({ code: "unavailable", message, cause });

export const unimplemented = (message: string) =>
  make({ code: "unimplemented", message });

export const cancelled = (message: string, cause?: unknown) =>
  make({ code: "cancelled", message, cause });

export const fromConnectError = (cause: unknown): GrpcStatusError => {
  const error = ConnectError.from(cause);
  return make({
    code: GrpcStatusCode.fromConnectCode(error.code),
    message: error.rawMessage,
    metadata: GrpcMetadata.fromHeaders(error.metadata),
    details: error.details,
    cause,
  });
};

export const toConnectError = (error: GrpcStatusError): ConnectError =>
  new ConnectError(
    error.message,
    GrpcStatusCode.toConnectCode(error.code),
    GrpcMetadata.toHeaders([...error.metadata, ...error.trailers]),
    error.details as ConstructorParameters<typeof ConnectError>[3],
    error.cause,
  );
