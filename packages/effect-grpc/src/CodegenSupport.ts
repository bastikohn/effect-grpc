import type { ContextKey } from "@connectrpc/connect";
import type { Effect } from "effect";

import type * as GrpcMetadata from "./GrpcMetadata.js";
import type { GrpcMethodKind } from "./GrpcMethodRegistry.js";
import type { GrpcStatusError } from "./GrpcStatusError.js";

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
  /**
   * Synchronous observer of a decoded response-header snapshot. Connect
   * exposes unary headers at completion, streaming headers before messages.
   * Throwing fails the call with `internal`. In-memory calls reject observers.
   */
  readonly onResponseHeaders?: (
    metadata: GrpcMetadata.GrpcMetadata,
  ) => undefined;
  /**
   * Called only on clean transport completion. Not called on failure,
   * cancellation, or early stream termination; failures retain their
   * existing `GrpcStatusError.metadata` channel. Must be synchronous.
   */
  readonly onResponseTrailers?: (
    metadata: GrpcMetadata.GrpcMetadata,
  ) => undefined;
}

/**
 * A handler's view of the call it is serving. Built fresh per RPC by the
 * server protocol from connect's `HandlerContext`. Metadata and method
 * identity are snapshots; the signal, remaining time and context values
 * reflect the live call.
 */
export interface GrpcServerContext {
  /** Incoming metadata, normalized (lower-cased keys, `-bin` values decoded). */
  readonly metadata: GrpcMetadata.GrpcMetadata;
  /** The registry identity of the method: its tag (`pkg.Service/Method`) and kind. */
  readonly method: {
    readonly tag: string;
    readonly kind: GrpcMethodKind;
  };
  /**
   * connect's own call signal, for handing to external asynchronous work.
   * It aborts on client cancellation and deadline expiry — but also when the
   * call completes normally — so an abort is not by itself a cancellation.
   * The protocol already maps aborts to the right status; handlers need not.
   */
  readonly signal: AbortSignal;
  /**
   * The time left on the incoming deadline, read live on every call:
   * `undefined` when the call has no deadline, `0` once it has elapsed.
   * Observation only — the deadline is enforced by connect, not by this
   * value. Note that `0` here means *exhausted*, while `timeoutMs: 0` on an
   * outgoing `GrpcCallOptions` means *no deadline*: never forward it blindly.
   */
  readonly remainingTimeoutMs: () => number | undefined;
  /**
   * A typed value attached to this call by a server interceptor via
   * `request.contextValues.set(key, value)`; an unset key yields the key's
   * declared default. Keys are connect's `createContextKey`, so the same key
   * module serves interceptors and handlers alike.
   */
  readonly getContextValue: <T>(key: ContextKey<T>) => T;
  /** Append validated response headers before the first response is yielded. */
  readonly writeResponseHeaders: (
    metadata: GrpcMetadata.GrpcMetadata,
  ) => Effect.Effect<void, GrpcStatusError>;
  /** Append validated trailers until handler completion, including finalizers. */
  readonly writeResponseTrailers: (
    metadata: GrpcMetadata.GrpcMetadata,
  ) => Effect.Effect<void, GrpcStatusError>;
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
