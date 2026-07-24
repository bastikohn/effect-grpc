import * as GrpcMetadata from "./GrpcMetadata.js";

export interface GrpcCallOptions {
  /**
   * Metadata sent with the call. `-bin` keys carry a `Uint8Array` (base64 on
   * the wire), every other key a `string`; anything else fails the call with
   * `invalid_argument` (see {@link GrpcMetadata.isBinaryKey}).
   */
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  /**
   * Deadline for the call, in milliseconds. A non-positive value means *no
   * deadline* rather than one that has already expired, on every adapter.
   */
  readonly timeoutMs?: number;
}

export interface GrpcServerContext {
  readonly metadata: GrpcMetadata.GrpcMetadata;
}

/**
 * Builds the per-call context server handlers receive: the request metadata,
 * decoded from the incoming headers — `-bin` values arrive as `Uint8Array`,
 * matching what the caller sent.
 */
export const serverContext = (
  headers: Headers | ReadonlyArray<readonly [string, string]>,
): GrpcServerContext => ({
  metadata: GrpcMetadata.fromHeaders(headers),
});
