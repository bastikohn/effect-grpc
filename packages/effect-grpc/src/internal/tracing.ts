import { Context, Exit, Metric, Option } from "effect";
import * as Tracer from "effect/Tracer";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpTraceContext from "effect/unstable/http/HttpTraceContext";

import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "../GrpcStatusCode.js";
import * as GrpcStatusError from "../GrpcStatusError.js";

/** Span name per OTel RPC semconv: the full RPC path `$service/$method`. */
export const spanName = (entry: GrpcMethodEntry): string => entry.tag;

export const clientSpanOptions = (
  entry: GrpcMethodEntry,
  serverAddress?: URL,
): Tracer.SpanOptionsNoTrace => ({
  kind: "client",
  attributes: {
    ...rpcAttributes(entry),
    ...(serverAddress ? serverAttributes(serverAddress, (port) => port) : {}),
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

export const traceparent = (span: Tracer.AnySpan): string =>
  `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;

/** Decodes incoming propagation headers into the parent `ExternalSpan`. */
export const externalSpanFromHeaders = (
  headers: ReadonlyArray<readonly [string, string]>,
): Tracer.ExternalSpan | undefined =>
  Option.getOrUndefined(
    HttpTraceContext.fromHeaders(Headers.fromInput(headers)),
  );

/**
 * Records the final status of a call exactly once: annotates the span with
 * the semconv status attributes and observes the call duration histogram.
 */
export type StatusRecorder = (code: GrpcStatusCode) => void;

/**
 * OpenTelemetry-recommended histogram bucket boundaries for RPC durations
 * measured in seconds. The `unit` attribute is read by Effect's OTLP exporter
 * to set the metric unit (and skipped as a label by the Prometheus exporter).
 */
const durationOptions = {
  boundaries: [
    0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
  ],
  attributes: { unit: "s" },
};

/**
 * `rpc.client.call.duration`: duration of outbound gRPC calls in seconds,
 * from call start to final status. Tagged with `rpc.system.name`,
 * `rpc.method`, `rpc.response.status_code`, `error.type` on failure, and
 * `server.address`/`server.port` where available.
 */
const clientDuration = Metric.histogram("rpc.client.call.duration", {
  description: "Duration of outbound gRPC calls, in seconds.",
  ...durationOptions,
});

/**
 * `rpc.server.call.duration`: duration of inbound gRPC calls in seconds,
 * from call start to final status. Tagged with `rpc.system.name`,
 * `rpc.method`, `rpc.response.status_code`, and `error.type` for
 * server-fault codes only (see {@link isServerError}).
 */
const serverDuration = Metric.histogram("rpc.server.call.duration", {
  description: "Duration of inbound gRPC calls, in seconds.",
  ...durationOptions,
});

export const clientCallRecorder = (options: {
  readonly entry: GrpcMethodEntry;
  readonly span: Tracer.Span;
  readonly context: Context.Context<never>;
  readonly serverAddress?: URL | undefined;
}): StatusRecorder =>
  callRecorder(
    options.span,
    clientDuration,
    {
      ...rpcAttributes(options.entry),
      ...(options.serverAddress
        ? serverAttributes(options.serverAddress, String)
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
    serverDuration,
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

const clientStatusAttributes = (code: GrpcStatusCode): Record<string, string> =>
  code === "ok"
    ? { "rpc.response.status_code": statusCodeString(code) }
    : {
        "rpc.response.status_code": statusCodeString(code),
        "error.type": statusCodeString(code),
      };

const serverStatusAttributes = (code: GrpcStatusCode): Record<string, string> =>
  isServerError(code)
    ? {
        "rpc.response.status_code": statusCodeString(code),
        "error.type": statusCodeString(code),
      }
    : { "rpc.response.status_code": statusCodeString(code) };

/** gRPC status code name per OTel semconv, e.g. `"OK"`, `"NOT_FOUND"`. */
const statusCodeString = (code: GrpcStatusCode): string => code.toUpperCase();

const rpcAttributes = (entry: GrpcMethodEntry): Record<string, string> => ({
  "rpc.system.name": "grpc",
  // Fully qualified logical method name, e.g. `demo.v1.UserService/GetUser`.
  "rpc.method": entry.tag,
});

/**
 * semconv's `server.*` attributes for the target. `port` renders the port:
 * semconv types it as an integer, which spans carry as such, but Effect's
 * metric attributes are string-only.
 */
const serverAttributes = <P extends string | number>(
  baseUrl: URL,
  port: (value: number) => P,
): Record<string, string | P> => {
  const value = serverPort(baseUrl);
  return {
    "server.address": serverAddress(baseUrl),
    ...(value === undefined ? {} : { "server.port": port(value) }),
  };
};

/**
 * The target's address. `hostname` is non-empty for the `http:`/`https:` URLs
 * `createGrpcTransport` builds, but a `serverAddress` override — and the
 * `layerConnect` path, where the caller brings its own `Transport` — is
 * unconstrained by scheme: `new URL("unix:/var/run/grpc.sock").hostname` is
 * `""`, which would emit the attribute present but blank. Fall back to the
 * whole URL so a non-special scheme still reports something addressable.
 */
const serverAddress = (baseUrl: URL): string =>
  baseUrl.hostname || baseUrl.host || baseUrl.toString().replace(/\/$/, "");

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
