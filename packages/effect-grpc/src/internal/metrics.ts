import { Metric } from "effect";

/**
 * OpenTelemetry-recommended histogram bucket boundaries for RPC durations
 * measured in seconds.
 */
const durationBoundaries: ReadonlyArray<number> = [
  0.005, 0.01, 0.025, 0.05, 0.075, 0.1, 0.25, 0.5, 0.75, 1, 2.5, 5, 7.5, 10,
];

/**
 * `rpc.client.duration`: duration of outbound gRPC calls in seconds, from
 * call start to final status. Tagged with `rpc.system`, `rpc.service`,
 * `rpc.method`, `rpc.grpc.status_code`, and `server.address`/`server.port`
 * where available.
 */
export const clientDuration = Metric.histogram("rpc.client.duration", {
  description: "Duration of outbound gRPC calls, in seconds.",
  boundaries: durationBoundaries,
});

/**
 * `rpc.server.duration`: duration of inbound gRPC calls in seconds, from
 * call start to final status. Tagged with `rpc.system`, `rpc.service`,
 * `rpc.method`, and `rpc.grpc.status_code`.
 */
export const serverDuration = Metric.histogram("rpc.server.duration", {
  description: "Duration of inbound gRPC calls, in seconds.",
  boundaries: durationBoundaries,
});
