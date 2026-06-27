import type * as Tracer from "effect/Tracer";

import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "../GrpcStatusCode.js";

export interface TraceContextFields {
  readonly traceId: string;
  readonly spanId: string;
  readonly sampled: boolean;
}

export const spanName = (entry: GrpcMethodEntry): string => entry.tag;

export const clientSpanOptions = (
  entry: GrpcMethodEntry,
  serverAddress?: URL,
): Tracer.SpanOptionsNoTrace => ({
  kind: "client",
  attributes: {
    ...commonAttributes(entry),
    ...(serverAddress ? serverAttributes(serverAddress) : {}),
  },
});

export const serverSpanOptions = (
  entry: GrpcMethodEntry,
  parent?: Tracer.ExternalSpan,
): Tracer.SpanOptionsNoTrace => ({
  kind: "server",
  ...(parent ? { parent } : {}),
  attributes: commonAttributes(entry),
});

export const traceFields = (span: TraceContextFields): TraceContextFields => ({
  traceId: span.traceId,
  spanId: span.spanId,
  sampled: span.sampled,
});

export const traceparent = (span: TraceContextFields): string =>
  `00-${span.traceId}-${span.spanId}-${span.sampled ? "01" : "00"}`;

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
): Record<string, string> => {
  const status = statusCode(code);
  return code === "ok"
    ? { "rpc.response.status_code": status }
    : {
        "rpc.response.status_code": status,
        "error.type": status,
      };
};

const commonAttributes = (entry: GrpcMethodEntry): Record<string, string> => ({
  "rpc.system.name": "grpc",
  "rpc.method": entry.tag,
});

const serverAttributes = (baseUrl: URL): Record<string, string | number> => {
  const address = baseUrl.hostname || baseUrl.host || normalizeUrl(baseUrl);
  const port = Number(baseUrl.port);
  return Number.isInteger(port) && port > 0
    ? { "server.address": address, "server.port": port }
    : { "server.address": address };
};

const normalizeUrl = (baseUrl: URL): string =>
  baseUrl.toString().replace(/\/$/, "");

const statusCode = (code: GrpcStatusCode): string => code.toUpperCase();
