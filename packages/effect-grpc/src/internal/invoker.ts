import { Effect } from "effect";

import type { GrpcCallOptions } from "../CodegenSupport.js";
import * as GrpcMetadata from "../GrpcMetadata.js";
import * as GrpcStatusError from "../GrpcStatusError.js";

/**
 * Call-option semantics shared by both adapters, so the connect and in-memory
 * implementations report identical status codes and messages for the same
 * inputs instead of drifting apart.
 */

const reservedHeaderPrefix = "x-effect-grpc-";

/**
 * gRPC's metadata charset, matching `@grpc/grpc-js`. Keys are compared
 * lowercased; ASCII values are limited to printable characters.
 *
 * These are not the platform's rules and cannot be delegated to it: HTTP's
 * token charset is strictly wider than gRPC's, so `Headers.append` happily
 * accepts `x-parity$q`, `a\tb` and non-ASCII bytes that gRPC forbids. A
 * conforming peer drops or rejects those, which is a far worse failure than
 * a local `invalid_argument`.
 */
const legalKey = /^[0-9a-z_.\-]+$/;
const legalAsciiValue = /^[ -~]*$/;

/**
 * The first metadata entry that cannot be put on the wire, as a ready-made
 * message, or `undefined` when every entry is sendable. Four rules, one
 * traversal:
 *
 * - `x-effect-grpc-*` is the library's own header namespace.
 * - Keys and ASCII values must be spellable as gRPC metadata (see
 *   {@link legalKey}).
 * - A value must match what its key declares (see
 *   {@link GrpcMetadata.isBinaryKey}). Without this, bytes under an ASCII key
 *   would reach the peer as base64 it cannot identify as binary, and a string
 *   under a `-bin` key would come back from {@link GrpcMetadata.fromHeaders}
 *   as arbitrary decoded bytes.
 * - Whatever is left still has to survive the encode path, the narrower of
 *   the two checks being whichever rejects first: `Headers.append` throws a
 *   `TypeError` on a key or value no header can carry, and both adapters
 *   reach it — the in-memory one through its wire round trip — so the throw
 *   has to be turned into a typed `invalid_argument` here.
 */
export const metadataViolation = (
  metadata: GrpcMetadata.GrpcMetadata | undefined,
): string | undefined => {
  const entries = metadata ?? GrpcMetadata.empty;
  for (const [key, value] of entries) {
    const lower = key.toLowerCase();
    if (lower.startsWith(reservedHeaderPrefix)) {
      return `Reserved gRPC metadata key: ${key}. Keys beginning with ${reservedHeaderPrefix} are used internally by effect-grpc.`;
    }
    if (!legalKey.test(lower)) {
      return `Invalid gRPC metadata key: ${JSON.stringify(key)}. Keys are non-empty and limited to letters, digits, and \`_.-\`.`;
    }
    const binary = GrpcMetadata.isBinaryKey(lower);
    if (binary !== value instanceof Uint8Array) {
      return binary
        ? `Binary gRPC metadata key ${key} requires a Uint8Array value.`
        : `gRPC metadata key ${key} requires a string value; binary metadata needs a -bin key suffix.`;
    }
    if (!binary && !legalAsciiValue.test(value as string)) {
      return `Invalid gRPC metadata value for key ${key}. ASCII metadata is limited to printable characters (0x20-0x7E); use a -bin key for anything else.`;
    }
  }
  try {
    GrpcMetadata.toHeaders(entries);
    return undefined;
  } catch (cause) {
    return `Invalid gRPC metadata: ${cause instanceof Error ? cause.message : String(cause)}`;
  }
};

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
