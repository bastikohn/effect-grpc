import { Context } from "effect";

/**
 * Context reference carrying a W3C `tracestate` header value.
 *
 * Effect spans have no dedicated `tracestate` field, so this library threads
 * the value through the request context (and, additionally, through span
 * annotations):
 *
 * - On the server, an incoming `tracestate` header is provided under this
 *   reference into the handler's context, and also attached to the
 *   `Tracer.ExternalSpan` parent's `annotations`.
 * - On the client, the outgoing `tracestate` header is resolved in order:
 *   a caller-provided `tracestate` metadata entry always wins, then the
 *   span ancestry is searched for an annotation under this key, then the
 *   ambient reference from the calling fiber's context is used.
 *
 * Together this makes `tracestate` pass through services built with this
 * library (server in, client out) for every method kind. Read the ambient
 * value from a handler when you need the raw header:
 *
 * ```ts
 * const state = yield* Effect.service(GrpcTracing.TraceState);
 * ```
 */
export const TraceState = Context.Reference<string | undefined>(
  "@effect-grpc/effect-grpc/GrpcTracing/TraceState",
  { defaultValue: () => undefined },
);
