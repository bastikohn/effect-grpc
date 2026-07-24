import { Schema } from "effect";

export type GrpcMetadataValue = string | Uint8Array;

export type GrpcMetadata = ReadonlyArray<
  readonly [key: string, value: GrpcMetadataValue]
>;

export const schema = Schema.Array(
  Schema.Tuple([
    Schema.String,
    Schema.Union([Schema.String, Schema.Uint8Array]),
  ]),
);

export const empty: GrpcMetadata = [];

/**
 * gRPC's binary-metadata marker. The `-bin` suffix is the only signal a peer
 * has that a header carries bytes rather than ASCII, so it — not the
 * JavaScript type of the value — decides how metadata is encoded and decoded
 * on both sides: `-bin` keys carry a `Uint8Array` (base64 on the wire), every
 * other key carries a `string`. Call metadata that contradicts its key is
 * rejected with `invalid_argument` before the call leaves.
 */
export const isBinaryKey = (key: string): boolean =>
  key.toLowerCase().endsWith("-bin");

/**
 * Decodes incoming headers into metadata, restoring `-bin` values to the
 * `Uint8Array` their key declares so a binary value round-trips to its
 * declared type.
 *
 * `Headers.entries()` joins repeated keys with `", "`. Only `-bin` values are
 * split back apart on that separator — gRPC defines comma-concatenated base64
 * for binary metadata, whereas an ASCII value may legitimately contain a
 * comma and must be left alone.
 */
export const fromHeaders = (
  headers: Headers | ReadonlyArray<readonly [string, string]>,
): GrpcMetadata =>
  (headers instanceof Headers
    ? Array.from(headers.entries())
    : headers
  ).flatMap(([rawKey, value]): GrpcMetadata => {
    const key = rawKey.toLowerCase();
    if (!isBinaryKey(key)) return [[key, value] as const];
    // Empty chunks are an artefact of the join, not values — except when the
    // whole value is empty, which is a legal zero-length binary value.
    const chunks = value.split(",").filter((chunk) => chunk.trim() !== "");
    return chunks.length === 0
      ? [[key, base64ToBytes("")] as const]
      : chunks.map((chunk) => [key, base64ToBytes(chunk)] as const);
  });

/** Encodes metadata for the wire: `-bin` values as base64, others verbatim. */
export const toHeaders = (metadata: GrpcMetadata): Headers => {
  const headers = new Headers();
  for (const [key, value] of metadata) {
    headers.append(key, encodeValue(key, value));
  }
  return headers;
};

/**
 * `Buffer.from` accepts both halves of the union at runtime, so one branch
 * covers every combination the key allows: bytes under a `-bin` key, and the
 * unvalidated leftovers of the error path (`GrpcStatusError` trailers are not
 * call metadata) where base64 still beats stringifying a `Uint8Array`.
 */
const encodeValue = (key: string, value: GrpcMetadataValue): string =>
  typeof value === "string" && !isBinaryKey(key)
    ? value
    : Buffer.from(value as Uint8Array).toString("base64");

const base64ToBytes = (value: string): Uint8Array =>
  new Uint8Array(Buffer.from(value.trim(), "base64"));
