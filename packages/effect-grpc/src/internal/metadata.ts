import * as GrpcMetadata from "../GrpcMetadata.js";

export const timeoutHeader = "x-effect-grpc-timeout-ms";
export const reservedHeaderPrefix = "x-effect-grpc-";

export interface CallOptions {
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  readonly timeoutMs?: number;
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
  if (options?.timeoutMs !== undefined) {
    entries.push([timeoutHeader, String(options.timeoutMs)]);
  }
  return entries;
};

export const readTimeoutMs = (
  headers: ReadonlyArray<readonly [string, string]>,
): number | undefined => {
  const value = headers.find(
    ([key]) => key.toLowerCase() === timeoutHeader,
  )?.[1];
  if (value === undefined) return undefined;
  const timeoutMs = Number(value);
  return Number.isFinite(timeoutMs) ? timeoutMs : undefined;
};

export const stripInternalHeaders = (
  headers: ReadonlyArray<readonly [string, string]>,
): ReadonlyArray<readonly [string, string]> =>
  headers.filter(([key]) => key.toLowerCase() !== timeoutHeader);

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
