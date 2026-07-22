import { Effect } from "effect";

import type { GrpcCallOptions } from "../CodegenSupport.js";
import * as GrpcStatusError from "../GrpcStatusError.js";
import { reservedHeaderPrefix, reservedMetadataKey } from "./metadata.js";

/**
 * Invocation errors shared by both adapters, so the connect and in-memory
 * implementations report identical status codes and messages for the same
 * inputs instead of drifting apart.
 */

/** Unknown or kind-mismatched tag. */
export const unknownTag = (tag: string): GrpcStatusError.GrpcStatusError =>
  GrpcStatusError.unimplemented(`Unknown gRPC RPC tag: ${tag}`);

/**
 * Fails with `invalid_argument` when the call metadata carries a reserved
 * `x-effect-grpc-*` key. Both adapters run this up front so a reserved key is
 * a uniform typed error — not a connect-side throw that surfaces as a defect
 * on streaming shapes and a generic `unknown` on unary shapes, nor a value
 * the in-memory adapter would silently accept.
 */
export const validateCallMetadata = (
  options: GrpcCallOptions | undefined,
): Effect.Effect<void, GrpcStatusError.GrpcStatusError> =>
  Effect.suspend(() => {
    const reserved = reservedMetadataKey(options?.metadata);
    return reserved === undefined
      ? Effect.void
      : Effect.fail(
          GrpcStatusError.invalidArgument(
            `Reserved gRPC metadata key: ${reserved}. Keys beginning with ${reservedHeaderPrefix} are used internally by effect-grpc.`,
          ),
        );
  });
