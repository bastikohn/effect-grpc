import * as GrpcMetadata from "../GrpcMetadata.js";

export const reservedHeaderPrefix = "x-effect-grpc-";

export interface CallOptions {
  readonly metadata?: GrpcMetadata.GrpcMetadata;
}

export const headersFromCallOptions = (
  options?: CallOptions,
): ReadonlyArray<readonly [string, string]> => {
  const entries: Array<readonly [string, string]> = [];
  for (const [key, value] of options?.metadata ?? GrpcMetadata.empty) {
    if (isReservedHeader(key)) {
      throw new Error(
        `Reserved gRPC metadata key: ${key}. Keys beginning with ${reservedHeaderPrefix} are used internally by effect-grpc.`,
      );
    }
    entries.push([
      key,
      value instanceof Uint8Array
        ? Buffer.from(value).toString("base64")
        : value,
    ]);
  }
  return entries;
};

/**
 * The first reserved key present in the metadata, or `undefined` if none.
 * Lets callers reject reserved metadata with a typed error before header
 * construction, rather than relying on {@link headersFromCallOptions}'s throw.
 */
export const reservedMetadataKey = (
  metadata: GrpcMetadata.GrpcMetadata | undefined,
): string | undefined => {
  for (const [key] of metadata ?? GrpcMetadata.empty) {
    if (isReservedHeader(key)) return key;
  }
  return undefined;
};

const isReservedHeader = (key: string) =>
  key.toLowerCase().startsWith(reservedHeaderPrefix);
