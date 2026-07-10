import { Metric } from "effect";

/**
 * OpenTelemetry-recommended histogram bucket boundaries for RPC durations
 * measured in seconds.
 */
const durationBoundaries: ReadonlyArray<number> = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

/**
 * The `unit` attribute is read by Effect's OTLP exporter to set the metric
 * unit (and skipped as a label by the Prometheus exporter).
 */
const durationAttributes = { unit: "s" };

/**
 * `rpc.client.call.duration`: duration of outbound gRPC calls in seconds,
 * from call start to final status. Tagged with `rpc.system.name`,
 * `rpc.method`, `rpc.response.status_code`, `error.type` on failure, and
 * `server.address`/`server.port` where available.
 */
export const clientDuration = Metric.histogram("rpc.client.call.duration", {
  description: "Duration of outbound gRPC calls, in seconds.",
  boundaries: durationBoundaries,
  attributes: durationAttributes,
});

/**
 * `rpc.server.call.duration`: duration of inbound gRPC calls in seconds,
 * from call start to final status. Tagged with `rpc.system.name`,
 * `rpc.method`, `rpc.response.status_code`, and `error.type` on failure.
 */
export const serverDuration = Metric.histogram("rpc.server.call.duration", {
  description: "Duration of inbound gRPC calls, in seconds.",
  boundaries: durationBoundaries,
  attributes: durationAttributes,
});
