import { Effect } from "effect";

import type { GrpcCallOptions } from "../CodegenSupport.js";
import * as GrpcStatusError from "../GrpcStatusError.js";
import { metadataViolation } from "./metadata.js";

/**
 * Call-option semantics shared by both adapters, so the connect and in-memory
 * implementations report identical status codes and messages for the same
 * inputs instead of drifting apart.
 */

/** Unknown or kind-mismatched tag. */
export const unknownTag = (tag: string): GrpcStatusError.GrpcStatusError =>
  GrpcStatusError.unimplemented(`Unknown gRPC RPC tag: ${tag}`);

/**
 * Fails with `invalid_argument` when the call metadata cannot go on the wire
 * (see {@link metadataViolation}). Both adapters run this up front so an
 * unsendable entry is a uniform typed error — not a connect-side throw that
 * surfaces as a defect on streaming shapes and a generic `unknown` on unary
 * shapes, nor a value the in-memory adapter would silently accept.
 */
export const validateCallMetadata = (
  options: GrpcCallOptions | undefined,
): Effect.Effect<void, GrpcStatusError.GrpcStatusError> =>
  Effect.suspend(() => {
    const violation = metadataViolation(options?.metadata);
    return violation === undefined
      ? Effect.void
      : Effect.fail(GrpcStatusError.invalidArgument(violation));
  });

/**
 * The deadline actually in force, or `undefined` for none. A non-positive
 * `timeoutMs` uniformly means *no deadline*: connect's `createDeadlineSignal`
 * aborts a `<= 0` timeout the instant the call starts, so the connect adapter
 * omits the option entirely rather than trusting a transport to normalize it.
 */
export const callTimeoutMs = (
  options: GrpcCallOptions | undefined,
): number | undefined =>
  options?.timeoutMs !== undefined && options.timeoutMs > 0
    ? options.timeoutMs
    : undefined;
