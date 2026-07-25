import { Code } from "@connectrpc/connect";

/**
 * Source of truth for both unions and both directions: every failure code
 * paired with its connect `Code`. `"ok"` is added on top for the
 * outcome-reporting union below.
 */
const toConnect = {
  cancelled: Code.Canceled,
  unknown: Code.Unknown,
  invalid_argument: Code.InvalidArgument,
  deadline_exceeded: Code.DeadlineExceeded,
  not_found: Code.NotFound,
  already_exists: Code.AlreadyExists,
  permission_denied: Code.PermissionDenied,
  resource_exhausted: Code.ResourceExhausted,
  failed_precondition: Code.FailedPrecondition,
  aborted: Code.Aborted,
  out_of_range: Code.OutOfRange,
  unimplemented: Code.Unimplemented,
  internal: Code.Internal,
  unavailable: Code.Unavailable,
  data_loss: Code.DataLoss,
  unauthenticated: Code.Unauthenticated,
} as const;

/**
 * A status code that denotes a failure. `GrpcStatusError` carries this rather
 * than {@link GrpcStatusCode}: a failure reported as `"ok"` would record
 * success telemetry while the peer still sees the call fail as `UNKNOWN`.
 */
export type GrpcErrorStatusCode = keyof typeof toConnect;

/**
 * Any call outcome, success included. Used for telemetry, which legitimately
 * reports `"ok"`.
 */
export type GrpcStatusCode = "ok" | GrpcErrorStatusCode;

const fromConnect = new Map<Code, GrpcErrorStatusCode>(
  Object.entries(toConnect).map(([name, code]) => [
    code,
    name as GrpcErrorStatusCode,
  ]),
);

/** Connect's `Code` has no `OK` member, so this never yields `"ok"`. */
export const fromConnectCode = (code: Code): GrpcErrorStatusCode =>
  fromConnect.get(code) ?? "unknown";

/** `"ok"` has no connect counterpart, so it maps to `Unknown` like the peer sees it. */
export const toConnectCode = (code: GrpcStatusCode): Code =>
  code === "ok" ? Code.Unknown : toConnect[code];
