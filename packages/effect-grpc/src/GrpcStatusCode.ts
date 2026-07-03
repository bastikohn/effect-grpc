import { Code } from "@connectrpc/connect";
import { Schema } from "effect";

export type GrpcStatusCode =
  | "ok"
  | "cancelled"
  | "unknown"
  | "invalid_argument"
  | "deadline_exceeded"
  | "not_found"
  | "already_exists"
  | "permission_denied"
  | "resource_exhausted"
  | "failed_precondition"
  | "aborted"
  | "out_of_range"
  | "unimplemented"
  | "internal"
  | "unavailable"
  | "data_loss"
  | "unauthenticated";

export const schema = Schema.Literal(
  "ok",
  "cancelled",
  "unknown",
  "invalid_argument",
  "deadline_exceeded",
  "not_found",
  "already_exists",
  "permission_denied",
  "resource_exhausted",
  "failed_precondition",
  "aborted",
  "out_of_range",
  "unimplemented",
  "internal",
  "unavailable",
  "data_loss",
  "unauthenticated",
);

export const fromConnectCode = (code: Code): GrpcStatusCode => {
  switch (code) {
    case Code.Canceled:
      return "cancelled";
    case Code.Unknown:
      return "unknown";
    case Code.InvalidArgument:
      return "invalid_argument";
    case Code.DeadlineExceeded:
      return "deadline_exceeded";
    case Code.NotFound:
      return "not_found";
    case Code.AlreadyExists:
      return "already_exists";
    case Code.PermissionDenied:
      return "permission_denied";
    case Code.ResourceExhausted:
      return "resource_exhausted";
    case Code.FailedPrecondition:
      return "failed_precondition";
    case Code.Aborted:
      return "aborted";
    case Code.OutOfRange:
      return "out_of_range";
    case Code.Unimplemented:
      return "unimplemented";
    case Code.Internal:
      return "internal";
    case Code.Unavailable:
      return "unavailable";
    case Code.DataLoss:
      return "data_loss";
    case Code.Unauthenticated:
      return "unauthenticated";
  }
};

export const toConnectCode = (code: GrpcStatusCode): Code => {
  switch (code) {
    case "cancelled":
      return Code.Canceled;
    case "invalid_argument":
      return Code.InvalidArgument;
    case "deadline_exceeded":
      return Code.DeadlineExceeded;
    case "not_found":
      return Code.NotFound;
    case "already_exists":
      return Code.AlreadyExists;
    case "permission_denied":
      return Code.PermissionDenied;
    case "resource_exhausted":
      return Code.ResourceExhausted;
    case "failed_precondition":
      return Code.FailedPrecondition;
    case "aborted":
      return Code.Aborted;
    case "out_of_range":
      return Code.OutOfRange;
    case "unimplemented":
      return Code.Unimplemented;
    case "internal":
      return Code.Internal;
    case "unavailable":
      return Code.Unavailable;
    case "data_loss":
      return Code.DataLoss;
    case "unauthenticated":
      return Code.Unauthenticated;
    case "ok":
    case "unknown":
      return Code.Unknown;
  }
};
