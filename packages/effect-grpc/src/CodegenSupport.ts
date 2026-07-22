import * as GrpcMetadata from "./GrpcMetadata.js";

export interface GrpcCallOptions {
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  readonly timeoutMs?: number;
}

export interface GrpcServerContext {
  readonly metadata: GrpcMetadata.GrpcMetadata;
}

/**
 * Builds the per-call context server handlers receive: the request metadata,
 * extracted from the incoming headers.
 */
export const serverContext = (
  headers: Headers | ReadonlyArray<readonly [string, string]>,
): GrpcServerContext => ({
  metadata: GrpcMetadata.fromHeaders(headers),
});
