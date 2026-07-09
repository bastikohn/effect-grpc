import { Context, Metric, Option } from "effect";
import * as Tracer from "effect/Tracer";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpTraceContext from "effect/unstable/http/HttpTraceContext";

import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "../GrpcStatusCode.js";
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
  const state = decoded["tracestate"];
  return state === undefined || state === ""
    ? parent
    : Tracer.externalSpan({
        traceId: parent.traceId,
        spanId: parent.spanId,
        sampled: parent.sampled,
        annotations: Context.add(parent.annotations, TraceState, state),
      });
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
  );

const callRecorder = (
  span: Tracer.Span,
  duration: Metric.Histogram<number>,
  attributes: Record<string, string>,
  context: Context.Context<never>,
): StatusRecorder => {
  const start = performance.now();
  let recorded = false;
  return (code) => {
    if (recorded) return;
    recorded = true;
    annotateSpanStatus(span, code);
    Metric.withAttributes(duration, {
      ...attributes,
      "rpc.grpc.status_code": String(statusCodeNumber(code)),
    }).updateUnsafe((performance.now() - start) / 1000, context);
  };
};

export const annotateSpanStatus = (
  span: Tracer.Span,
  code: GrpcStatusCode,
): void => {
  for (const [key, value] of Object.entries(statusAttributes(code))) {
    span.attribute(key, value);
  }
};

export const statusAttributes = (
  code: GrpcStatusCode,
): Record<string, string | number> =>
  code === "ok"
    ? { "rpc.grpc.status_code": statusCodeNumber(code) }
    : {
        "rpc.grpc.status_code": statusCodeNumber(code),
        "error.type": code.toUpperCase(),
      };

/** Numeric gRPC status code per the gRPC wire protocol (0-16). */
export const statusCodeNumber = (code: GrpcStatusCode): number =>
  grpcStatusNumbers[code];

const grpcStatusNumbers: Record<GrpcStatusCode, number> = {
  ok: 0,
  cancelled: 1,
  unknown: 2,
  invalid_argument: 3,
  deadline_exceeded: 4,
  not_found: 5,
  already_exists: 6,
  permission_denied: 7,
  resource_exhausted: 8,
  failed_precondition: 9,
  aborted: 10,
  out_of_range: 11,
  unimplemented: 12,
  internal: 13,
  unavailable: 14,
  data_loss: 15,
  unauthenticated: 16,
};

const rpcAttributes = (entry: GrpcMethodEntry): Record<string, string> => {
  const [service, method] = serviceAndMethod(entry);
  return {
    "rpc.system": "grpc",
    "rpc.service": service,
    "rpc.method": method,
  };
};

const serviceAndMethod = (
  entry: GrpcMethodEntry,
): readonly [service: string, method: string] => {
  const index = entry.tag.lastIndexOf("/");
  return index > 0
    ? [entry.tag.slice(0, index), entry.tag.slice(index + 1)]
    : [entry.tag, entry.tag];
};

const serverSpanAttributes = (
  baseUrl: URL,
): Record<string, string | number> => {
  const port = serverPort(baseUrl);
  return port === undefined
    ? { "server.address": serverAddress(baseUrl) }
    : { "server.address": serverAddress(baseUrl), "server.port": port };
};

const serverMetricAttributes = (baseUrl: URL): Record<string, string> => {
  const port = serverPort(baseUrl);
  return port === undefined
    ? { "server.address": serverAddress(baseUrl) }
    : {
        "server.address": serverAddress(baseUrl),
        "server.port": String(port),
      };
};

const serverAddress = (baseUrl: URL): string =>
  baseUrl.hostname || baseUrl.host || normalizeUrl(baseUrl);

const serverPort = (baseUrl: URL): number | undefined => {
  const port = Number(baseUrl.port);
  return Number.isInteger(port) && port > 0 ? port : undefined;
};

const normalizeUrl = (baseUrl: URL): string =>
  baseUrl.toString().replace(/\/$/, "");
