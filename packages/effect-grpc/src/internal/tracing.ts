import { Context, Exit, Metric, Option } from "effect";
import * as Tracer from "effect/Tracer";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpTraceContext from "effect/unstable/http/HttpTraceContext";

import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "../GrpcStatusCode.js";
import * as GrpcStatusError from "../GrpcStatusError.js";
import { TraceState } from "../GrpcTracing.js";
import * as GrpcMetrics from "./metrics.js";

export interface TraceContextFields {
  readonly traceId: string;
  readonly spanId: string;
  readonly sampled: boolean;
}

/** Span name per OTel RPC semconv: the full RPC path `$service/$method`. */
export const spanName = (entry: GrpcMethodEntry): string => entry.tag;

export const clientSpanOptions = (
  entry: GrpcMethodEntry,
  serverAddress?: URL,
): Tracer.SpanOptionsNoTrace => ({
  kind: "client",
  attributes: {
    ...rpcAttributes(entry),
    ...(serverAddress ? serverSpanAttributes(serverAddress) : {}),
  },
});

export const serverSpanOptions = (
  entry: GrpcMethodEntry,
  parent?: Tracer.ExternalSpan,
): Tracer.SpanOptionsNoTrace => ({
  kind: "server",
  ...(parent ? { parent } : {}),
  attributes: rpcAttributes(entry),
});

export const traceFields = (span: TraceContextFields): TraceContextFields => ({
  traceId: span.traceId,
  spanId: span.spanId,
  sampled: span.sampled,
});

export const traceparent = (span: TraceContextFields): string =>
  `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;

/**
 * Decodes incoming propagation headers into the parent `ExternalSpan`.
 * A W3C `tracestate` header is attached to the span's annotations under
 * {@link TraceState} so it can be forwarded on downstream calls.
 */
export const externalSpanFromHeaders = (
  headers: ReadonlyArray<readonly [string, string]>,
): Tracer.ExternalSpan | undefined => {
  const decoded = Headers.fromInput(headers);
  const parent = Option.getOrUndefined(HttpTraceContext.fromHeaders(decoded));
  if (parent === undefined) return undefined;
  const state = traceStateFromDecoded(decoded);
  return state === undefined
    ? parent
    : Tracer.externalSpan({
        traceId: parent.traceId,
        spanId: parent.spanId,
        sampled: parent.sampled,
        annotations: Context.add(parent.annotations, TraceState, state),
      });
};

/**
 * Extracts the W3C `tracestate` value from incoming propagation headers.
 * Per W3C trace context, `tracestate` is only meaningful alongside a valid
 * W3C `traceparent`, so it is discarded when that header fails to decode —
 * including requests that carry only B3 propagation headers.
 */
export const traceStateFromHeaders = (
  headers: ReadonlyArray<readonly [string, string]>,
): string | undefined => traceStateFromDecoded(Headers.fromInput(headers));

const traceStateFromDecoded = (
  decoded: Headers.Headers,
): string | undefined => {
  // Deliberately W3C-only (not the W3C -> B3 fallback used for parenting).
  if (Option.isNone(HttpTraceContext.w3c(decoded))) return undefined;
  const state = decoded["tracestate"];
  return state === undefined || state === "" ? undefined : state;
};

/**
 * Finds the nearest `tracestate` value carried in the span ancestry (see
 * {@link TraceState}). Effect spans do not model `tracestate` natively, so
 * only values attached by {@link externalSpanFromHeaders} (or by the app via
 * span annotations) are visible.
 */
export const findTraceState = (span: Tracer.AnySpan): string | undefined => {
  let current: Tracer.AnySpan | undefined = span;
  while (current !== undefined) {
    const state = Context.get(current.annotations, TraceState);
    if (state !== undefined) return state;
    current =
      current._tag === "Span"
        ? Option.getOrUndefined(current.parent)
        : undefined;
  }
  return undefined;
};

/**
 * Records the final status of a call exactly once: annotates the span with
 * the semconv status attributes and observes the call duration histogram.
 */
export type StatusRecorder = (code: GrpcStatusCode) => void;

export const clientCallRecorder = (options: {
  readonly entry: GrpcMethodEntry;
  readonly span: Tracer.Span;
  readonly context: Context.Context<never>;
  readonly serverAddress?: URL | undefined;
}): StatusRecorder =>
  callRecorder(
    options.span,
    GrpcMetrics.clientDuration,
    {
      ...rpcAttributes(options.entry),
      ...(options.serverAddress
        ? serverMetricAttributes(options.serverAddress)
        : {}),
    },
    options.context,
    clientStatusAttributes,
  );

export const serverCallRecorder = (options: {
  readonly entry: GrpcMethodEntry;
  readonly span: Tracer.Span;
  readonly context: Context.Context<never>;
}): StatusRecorder =>
  callRecorder(
    options.span,
    GrpcMetrics.serverDuration,
    rpcAttributes(options.entry),
    options.context,
    serverStatusAttributes,
  );

const callRecorder = (
  span: Tracer.Span,
  duration: Metric.Histogram<number>,
  attributes: Record<string, string>,
  context: Context.Context<never>,
  statusAttributes: (code: GrpcStatusCode) => Record<string, string>,
): StatusRecorder => {
  const start = performance.now();
  let recorded = false;
  return (code) => {
    if (recorded) return;
    recorded = true;
    for (const [key, value] of Object.entries(statusAttributes(code))) {
      span.attribute(key, value);
    }
    Metric.withAttributes(duration, {
      ...attributes,
      ...statusAttributes(code),
    }).updateUnsafe((performance.now() - start) / 1000, context);
  };
};

/**
 * Exit used to close a client span scope. Per semconv every non-`ok` client
 * status is an error, so the span ends in a failed exit whenever a non-`ok`
 * status was recorded — even when the call surfaced as interruption or an
 * early stream close, whose natural exits exporters would map to OK.
 */
export const clientSpanExit = (
  code: GrpcStatusCode | undefined,
): Exit.Exit<void, GrpcStatusError.GrpcStatusError> =>
  code === undefined || code === "ok"
    ? Exit.void
    : Exit.fail(
        GrpcStatusError.make({
          code,
          message: `RPC failed with status ${code}`,
        }),
      );

/**
 * Per OTel gRPC semconv, only this subset of status codes marks a SERVER span
 * (or metric) as an error — conditions the server itself is responsible for.
 * Client-caused or routine outcomes (`cancelled`, `not_found`, ...) keep the
 * status attribute but are not errors from the server's point of view.
 * Clients treat every non-`ok` code as an error.
 */
const serverErrorCodes: ReadonlySet<GrpcStatusCode> = new Set([
  "unknown",
  "deadline_exceeded",
  "unimplemented",
  "internal",
  "unavailable",
  "data_loss",
]);

/** Whether a server span should end in an error state for this status code. */
export const isServerError = (code: GrpcStatusCode): boolean =>
  serverErrorCodes.has(code);

export const clientStatusAttributes = (
  code: GrpcStatusCode,
): Record<string, string> =>
  code === "ok"
    ? { "rpc.response.status_code": statusCodeString(code) }
    : {
        "rpc.response.status_code": statusCodeString(code),
        "error.type": statusCodeString(code),
      };

export const serverStatusAttributes = (
  code: GrpcStatusCode,
): Record<string, string> =>
  isServerError(code)
    ? {
        "rpc.response.status_code": statusCodeString(code),
        "error.type": statusCodeString(code),
      }
    : { "rpc.response.status_code": statusCodeString(code) };

/** gRPC status code name per OTel semconv, e.g. `"OK"`, `"NOT_FOUND"`. */
export const statusCodeString = (code: GrpcStatusCode): string =>
  code.toUpperCase();

const rpcAttributes = (entry: GrpcMethodEntry): Record<string, string> => ({
  "rpc.system.name": "grpc",
  // Fully qualified logical method name, e.g. `demo.v1.UserService/GetUser`.
  "rpc.method": entry.tag,
});

const serverSpanAttributes = (
  baseUrl: URL,
): Record<string, string | number> => {
  const port = serverPort(baseUrl);
  return {
    "server.address": serverAddress(baseUrl),
    ...(port === undefined ? {} : { "server.port": port }),
  };
};

/**
 * The span attributes above, as metric tags. Effect's metric attributes are
 * string-only, so semconv's integer `server.port` is stringified here.
 */
const serverMetricAttributes = (baseUrl: URL): Record<string, string> => {
  const port = serverPort(baseUrl);
  return {
    "server.address": serverAddress(baseUrl),
    ...(port === undefined ? {} : { "server.port": String(port) }),
  };
};

const serverAddress = (baseUrl: URL): string =>
  baseUrl.hostname || baseUrl.host || normalizeUrl(baseUrl);

/** Ports WHATWG `URL` normalizes away — see {@link serverPort}. */
const defaultPorts = new Map([
  ["https:", 443],
  ["http:", 80],
]);

/**
 * Effective port of the target. `URL` drops a scheme's default port, so
 * `new URL("https://api.example.com:443").port` is `""` and semconv's
 * `server.port` would vanish for the most common endpoints; the scheme
 * supplies it instead. Only the two schemes a gRPC `baseUrl` can use are
 * mapped, so a `serverAddress` override on any other scheme has no default
 * here and reports `server.address` alone.
 */
const serverPort = (baseUrl: URL): number | undefined =>
  baseUrl.port === ""
    ? defaultPorts.get(baseUrl.protocol)
    : Number(baseUrl.port);

const normalizeUrl = (baseUrl: URL): string =>
  baseUrl.toString().replace(/\/$/, "");
