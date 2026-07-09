import { Context } from "effect";

/**
 * Context key carrying a W3C `tracestate` header value through Effect's
 * tracer model.
 *
 * Effect spans have no dedicated `tracestate` field, so this library threads
 * the value through span annotations instead:
 *
 * - On the server, an incoming `tracestate` header is attached to the
 *   `Tracer.ExternalSpan` parent's `annotations` context under this key.
 * - On the client, the span ancestry is searched for this annotation and the
 *   first value found is forwarded as the outgoing `tracestate` header
 *   (alongside the injected `traceparent`). A caller-provided `tracestate`
 *   metadata entry always wins.
 *
 * Together this makes `tracestate` pass through services built with this
 * library (server in, client out). Read it from a handler's parent span when
 * you need the raw value:
 *
 * ```ts
 * const state = Context.get(externalSpan.annotations, GrpcTracing.TraceState);
 * ```
 */
export const TraceState = Context.Reference<string | undefined>(
  "@effect-grpc/effect-grpc/GrpcTracing/TraceState",
  { defaultValue: () => undefined },
);
