import * as GrpcMetadata from "../GrpcMetadata.js";

const reservedHeaderPrefix = "x-effect-grpc-";

/**
 * gRPC's metadata charset, matching `@grpc/grpc-js`. Keys are compared
 * lowercased; ASCII values are limited to printable characters, which is
 * exactly what keeps a value out of `Headers`' own (later, untyped) rejection
 * of control characters and stray CR/LF.
 */
const legalKey = /^[0-9a-z_.\-]+$/;
const legalAsciiValue = /^[ -~]*$/;

/**
 * The first metadata entry that cannot be put on the wire, as a ready-made
 * message, or `undefined` when every entry is sendable. Three rules, one
 * traversal:
 *
 * - `x-effect-grpc-*` is the library's own header namespace.
 * - Keys and ASCII values must be spellable as a header. `Headers.append`
 *   throws a `TypeError` on the rest, which both adapters now reach — the
 *   in-memory one through its wire round trip — so the check has to live here
 *   for the failure to stay a typed `invalid_argument`.
 * - A value must match what its key declares (see
 *   {@link GrpcMetadata.isBinaryKey}). Without this, bytes under an ASCII key
 *   would reach the peer as base64 it cannot identify as binary, and a string
 *   under a `-bin` key would come back from
 *   {@link GrpcMetadata.fromHeaders} as arbitrary decoded bytes.
 */
export const metadataViolation = (
  metadata: GrpcMetadata.GrpcMetadata | undefined,
): string | undefined => {
  for (const [key, value] of metadata ?? GrpcMetadata.empty) {
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
  return undefined;
};
