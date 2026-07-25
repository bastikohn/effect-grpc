import type * as GrpcMetadata from "./GrpcMetadata.js";

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
 * Reads a field off a wire message, tolerating an absent or non-object
 * message. Every generated registry converter needs exactly this, so it is
 * exported here rather than emitted into each generated file.
 */
export const readField = (message: unknown, field: string): unknown =>
  typeof message === "object" && message !== null
    ? (message as Record<string, unknown>)[field]
    : undefined;
