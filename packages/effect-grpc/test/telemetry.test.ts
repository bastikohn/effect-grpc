import type {
  ConnectRouter,
  HandlerContext,
  Transport,
} from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Layer,
  Metric,
  Option,
  Schema,
  Stream,
} from "effect";
import * as Tracer from "effect/Tracer";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { TraceState } from "../src/GrpcTracing.js";
import {
  externalSpanFromHeaders,
  traceStateFromHeaders,
} from "../src/internal/tracing.js";

const TRACEPARENT_PATTERN = /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/;

type HeadersImport = ConstructorParameters<typeof Headers>[0];

describe("client telemetry", () => {
  it("forwards tracestate and records duration for client-streaming calls", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      stream: async function* (input) {
        let count = 0;
        for await (const _ of input) {
          count++;
        }
        yield { received: count };
      },
    });
    const parent = Tracer.externalSpan({
      traceId: "0123456789abcdef0123456789abcdef",
      spanId: "0123456789abcdef",
      sampled: true,
      annotations: Context.add(Context.empty(), TraceState, "vendor=xyz"),
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const response = yield* invoker.clientStream(
            clientStreamingEntry.tag,
            Stream.make({ id: "1" }, { id: "2" }),
          );
          const metrics = yield* Metric.snapshot;
          return { response, metrics };
        }).pipe(
          Effect.withParentSpan(parent),
          Effect.provide(clientLayer(transport)),
        ),
      ),
    );

    expect(result.response).toEqual({ received: 2 });
    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=xyz");

    const span = telemetry.expectSpan(clientStreamingEntry.tag);
    expect(span.kind).toBe("client");
    expect(span.attributes.get("rpc.response.status_code")).toBe("OK");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.system.name": "grpc",
      "rpc.method": "demo.v1.TelemetryService/Upload",
      "rpc.response.status_code": "OK",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("falls back to the ambient TraceState reference when the span ancestry has none", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });

    await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          return yield* invoker.unary(unaryEntry.tag, {});
        }).pipe(
          Effect.provide(clientLayer(transport)),
          Effect.provideService(TraceState, "vendor=ambient"),
        ),
      ),
    );

    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=ambient");
  });

  it("falls back to the ambient TraceState reference on streaming calls", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      stream: async function* (input) {
        for await (const _ of input) {
          // drain
        }
        yield { received: true };
      },
    });

    await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          return yield* invoker.clientStream(
            clientStreamingEntry.tag,
            Stream.make({ id: "1" }),
          );
        }).pipe(
          Effect.provide(clientLayer(transport)),
          Effect.provideService(TraceState, "vendor=ambient"),
        ),
      ),
    );

    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=ambient");
  });

  it("records failed client-streaming calls with the failure status", async () => {
    const telemetry = makeTestTelemetry();
    const { transport } = fakeTransport({
      stream: () => {
        throw new ConnectError("nope", Code.PermissionDenied);
      },
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const error = yield* invoker
            .clientStream(clientStreamingEntry.tag, Stream.make({ id: "1" }))
            .pipe(Effect.flip);
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(result.error).toMatchObject({ code: "permission_denied" });
    const span = telemetry.expectSpan(clientStreamingEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe(
      "PERMISSION_DENIED",
    );
    expect(span.attributes.get("error.type")).toBe("PERMISSION_DENIED");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Upload",
      "rpc.response.status_code": "PERMISSION_DENIED",
      "error.type": "PERMISSION_DENIED",
    });
  });

  it("records OK when a bidi response stream completes naturally", async () => {
    const telemetry = makeTestTelemetry();
    const { transport } = fakeTransport({
      stream: async function* (input) {
        for await (const request of input) {
          yield request;
        }
      },
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const responses = yield* invoker
            .bidiStream(
              bidiStreamingEntry.tag,
              Stream.make({ id: "1" }, { id: "2" }),
            )
            .pipe(Stream.runCollect);
          const metrics = yield* Metric.snapshot;
          return { responses, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(result.responses).toHaveLength(2);
    const span = telemetry.expectSpan(bidiStreamingEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("OK");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Chat",
      "rpc.response.status_code": "OK",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("ends the client span as an error when a unary call is interrupted", async () => {
    const telemetry = makeTestTelemetry();
    const { transport } = fakeTransport({
      unary: (_header, signal) =>
        new Promise((_resolve, reject) => {
          signal?.addEventListener(
            "abort",
            () => reject(new ConnectError("cancelled", Code.Canceled)),
            { once: true },
          );
        }),
    });

    await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const fiber = yield* invoker
            .unary(unaryEntry.tag, {})
            .pipe(Effect.forkChild);
          // Let the call reach the in-flight transport request.
          yield* Effect.promise<void>(
            (): Promise<void> =>
              new Promise((resolve) => setTimeout(resolve, 10)),
          );
          yield* Fiber.interrupt(fiber);
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("CANCELLED");
    expect(span.attributes.get("error.type")).toBe("CANCELLED");
    // Interruption must not end the span with an interrupt-only exit, which
    // exporters map to OK; per semconv a CANCELLED client span is an error.
    // An interrupted exit is also `Failure`, so assert the cause carries a
    // real failure — the distinction the OTLP exporter makes.
    const exit = spanEndExit(span);
    expect(exit._tag).toBe("Failure");
    if (exit._tag === "Failure") {
      expect(Cause.hasInterruptsOnly(exit.cause)).toBe(false);
    }
  });

  it("records CANCELLED when the consumer stops a bidi stream early", async () => {
    const telemetry = makeTestTelemetry();
    const { transport } = fakeTransport({
      stream: async function* (input) {
        for await (const request of input) {
          yield request;
        }
      },
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const responses = yield* invoker
            .bidiStream(
              bidiStreamingEntry.tag,
              Stream.make({ id: "1" }, { id: "2" }, { id: "3" }),
            )
            .pipe(Stream.take(1), Stream.runCollect);
          const metrics = yield* Metric.snapshot;
          return { responses, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(result.responses).toHaveLength(1);
    const span = telemetry.expectSpan(bidiStreamingEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("CANCELLED");
    // The stream scope closes successfully on an early consumer close, but
    // per semconv the CANCELLED client span must still end as an error.
    expect(spanEndExit(span)._tag).toBe("Failure");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Chat",
      "rpc.response.status_code": "CANCELLED",
      "error.type": "CANCELLED",
    });
    expect(durations[0]?.count).toBe(1);
  });

  // Generated clients resolve every call shape through the `GrpcInvoker` seam
  // (its connect adapter's `withCallSpanEffect` / `withCallSpanStream`). These
  // cases assert the unary and server-streaming shapes carry the same spans,
  // status, metrics, and trace headers as the streaming shapes above.
  const callInvokerUnary = (
    tag: string,
    callOptions?: Parameters<GrpcInvoker.GrpcInvokerService["unary"]>[2],
  ) =>
    Effect.gen(function* () {
      const invoker = yield* GrpcInvoker.GrpcInvoker;
      return yield* invoker.unary(tag, {}, callOptions);
    });

  it("records semconv span attributes, injects trace headers, and observes duration on unary success", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });
    const parent = Tracer.externalSpan({
      traceId: "0123456789abcdef0123456789abcdef",
      spanId: "0123456789abcdef",
      sampled: true,
      annotations: Context.add(Context.empty(), TraceState, "vendor=abc"),
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const response = yield* callInvokerUnary(unaryEntry.tag);
          const metrics = yield* Metric.snapshot;
          return { response, metrics };
        }).pipe(
          Effect.withParentSpan(parent),
          Effect.provide(clientLayer(transport)),
        ),
      ),
    );

    expect(result.response).toEqual({ ok: true });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.kind).toBe("client");
    expect(span.attributes.get("rpc.system.name")).toBe("grpc");
    expect(span.attributes.get("rpc.method")).toBe(
      "demo.v1.TelemetryService/Get",
    );
    expect(span.attributes.get("server.address")).toBe("api.example.com");
    expect(span.attributes.get("server.port")).toBe(8443);
    expect(span.attributes.get("rpc.response.status_code")).toBe("OK");
    expect(span.attributes.get("error.type")).toBeUndefined();
    expect(spanEndExit(span)._tag).toBe("Success");

    const traceparent = headers[0]?.get("traceparent");
    expect(traceparent).toMatch(TRACEPARENT_PATTERN);
    expect(traceparent).toBe(`00-${span.traceId}-${span.spanId}-01`);
    expect(span.traceId).toBe(parent.traceId);
    expect(headers[0]?.get("tracestate")).toBe("vendor=abc");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toEqual({
      unit: "s",
      "rpc.system.name": "grpc",
      "rpc.method": "demo.v1.TelemetryService/Get",
      "server.address": "api.example.com",
      "server.port": "8443",
      "rpc.response.status_code": "OK",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("respects a caller-provided traceparent", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });
    const provided = "00-11111111111111111111111111111111-2222222222222222-01";

    await Effect.runPromise(
      telemetry.provide(
        callInvokerUnary(unaryEntry.tag, {
          metadata: [["traceparent", provided]],
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(headers[0]?.get("traceparent")).toBe(provided);
    expect(headers[0]?.get("tracestate")).toBeNull();
  });

  it("does not inject a traceparent for noop spans", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });

    await Effect.runPromise(
      telemetry.provide(
        callInvokerUnary(unaryEntry.tag).pipe(
          Effect.provide(clientLayer(transport)),
          Effect.withTracerEnabled(false),
        ),
      ),
    );

    expect(headers[0]?.get("traceparent")).toBeNull();
    expect(headers[0]?.get("tracestate")).toBeNull();
  });

  it("records the status code and error.type on unary failure", async () => {
    const telemetry = makeTestTelemetry();
    const { transport } = fakeTransport({
      unary: () => {
        throw new ConnectError("missing", Code.NotFound);
      },
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const error = yield* callInvokerUnary(unaryEntry.tag).pipe(
            Effect.flip,
          );
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(result.error).toMatchObject({ code: "not_found" });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    expect(span.attributes.get("error.type")).toBe("NOT_FOUND");
    expect(spanEndExit(span)._tag).toBe("Failure");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "NOT_FOUND",
      "error.type": "NOT_FOUND",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("forwards trace headers and records duration for server-streaming calls", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      stream: async function* () {
        yield { seq: 1 };
        yield { seq: 2 };
      },
    });
    const parent = Tracer.externalSpan({
      traceId: "0123456789abcdef0123456789abcdef",
      spanId: "0123456789abcdef",
      sampled: true,
      annotations: Context.add(Context.empty(), TraceState, "vendor=xyz"),
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const responses = yield* invoker
            .serverStream(serverStreamingEntry.tag, {})
            .pipe(Stream.runCollect);
          const metrics = yield* Metric.snapshot;
          return { responses, metrics };
        }).pipe(
          Effect.withParentSpan(parent),
          Effect.provide(clientLayer(transport)),
        ),
      ),
    );

    expect(result.responses).toHaveLength(2);
    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=xyz");

    const span = telemetry.expectSpan(serverStreamingEntry.tag);
    expect(span.kind).toBe("client");
    expect(span.attributes.get("rpc.method")).toBe(
      "demo.v1.TelemetryService/Watch",
    );
    expect(span.attributes.get("rpc.response.status_code")).toBe("OK");

    const durations = durationMetrics(
      result.metrics,
      "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Watch",
      "rpc.response.status_code": "OK",
    });
    expect(durations[0]?.count).toBe(1);
  });
});

describe("server telemetry", () => {
  const traceId = "0af7651916cd43dd8448eb211c80319c";
  const parentSpanId = "b7ad6b7169203331";
  const incomingHeaders = {
    traceparent: `00-${traceId}-${parentSpanId}-01`,
    tracestate: "vendor=abc",
  };

  it("parents the unary span to the incoming traceparent and records duration", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                unaryEntry.tag,
                { kind: "unary", handler: () => Effect.succeed({ ok: true }) },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const response = yield* Effect.promise(() =>
            (
              implementation.get as (
                request: unknown,
                context: HandlerContext,
              ) => Promise<unknown>
            )({}, handlerContext(incomingHeaders)),
          );
          const metrics = yield* Metric.snapshot;
          return { response, metrics };
        }),
      ),
    );

    expect(result.response).toEqual({ ok: true });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.kind).toBe("server");
    expect(span.attributes.get("rpc.system.name")).toBe("grpc");
    expect(span.attributes.get("rpc.method")).toBe(
      "demo.v1.TelemetryService/Get",
    );
    expect(span.attributes.get("rpc.response.status_code")).toBe("OK");
    expect(span.traceId).toBe(traceId);

    const parent = Option.getOrThrow(span.parent);
    expect(parent._tag).toBe("ExternalSpan");
    expect(parent.traceId).toBe(traceId);
    expect(parent.spanId).toBe(parentSpanId);
    expect(Context.get(parent.annotations, TraceState)).toBe("vendor=abc");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toEqual({
      unit: "s",
      "rpc.system.name": "grpc",
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "OK",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("forwards incoming tracestate to downstream client calls made from a unary handler", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const downstream = yield* Effect.provide(
            Effect.service(GrpcInvoker.GrpcInvoker),
            clientLayer(transport),
          );
          // The handler runs inside the request fiber with the incoming
          // `tracestate` provided from the headers, so downstream client
          // calls pick it up for header injection.
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                unaryEntry.tag,
                {
                  kind: "unary",
                  handler: () =>
                    downstream.unary(unaryEntry.tag, {}).pipe(Effect.orDie),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);
          return yield* Effect.promise(() =>
            (
              implementation.get as (
                request: unknown,
                context: HandlerContext,
              ) => Promise<unknown>
            )({}, handlerContext(incomingHeaders)),
          );
        }),
      ),
    );

    expect(result).toEqual({ ok: true });
    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=abc");
  });

  it("forwards incoming tracestate to downstream client calls made from a server-streaming handler", async () => {
    const telemetry = makeTestTelemetry();
    const { transport, headers } = fakeTransport({
      unary: () => ({ ok: true }),
    });

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const downstream = yield* Effect.provide(
            Effect.service(GrpcInvoker.GrpcInvoker),
            clientLayer(transport),
          );
          // The handler fiber is spawned by the response pump; this pins the
          // pump context rehydration: the scoped server span parents the
          // downstream span and the incoming `tracestate` is injected.
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [serverStreamingEntry.tag, serverStreamingEntry],
            ]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                serverStreamingEntry.tag,
                {
                  kind: "server-streaming",
                  handler: () =>
                    Stream.fromEffect(
                      downstream.unary(unaryEntry.tag, {}).pipe(Effect.orDie),
                    ),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);
          return yield* Effect.promise(async () => {
            const responses: Array<unknown> = [];
            for await (const value of (
              implementation.watch as (
                request: unknown,
                context: HandlerContext,
              ) => AsyncIterable<unknown>
            )({}, handlerContext(incomingHeaders))) {
              responses.push(value);
            }
            return responses;
          });
        }),
      ),
    );

    expect(result).toEqual([{ ok: true }]);
    expect(headers[0]?.get("traceparent")).toMatch(TRACEPARENT_PATTERN);
    expect(headers[0]?.get("tracestate")).toBe("vendor=abc");

    const serverSpan = telemetry.expectSpan(serverStreamingEntry.tag);
    expect(serverSpan.kind).toBe("server");
    expect(serverSpan.traceId).toBe(traceId);
    // The downstream client span must be parented to the server span.
    const clientSpan = telemetry.expectSpan(unaryEntry.tag);
    expect(clientSpan.kind).toBe("client");
    expect(clientSpan.traceId).toBe(traceId);
  });

  it("records NOT_FOUND without error.type on unary failure (not a server fault)", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                unaryEntry.tag,
                {
                  kind: "unary",
                  handler: () =>
                    Effect.fail(GrpcStatusError.notFound("missing")),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const error = yield* Effect.promise(async () => {
            try {
              await (
                implementation.get as (
                  request: unknown,
                  context: HandlerContext,
                ) => Promise<unknown>
              )({}, handlerContext());
            } catch (cause) {
              return cause;
            }
            throw new Error("Expected unary handler to fail");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      ),
    );

    expect(result.error).toMatchObject({ rawMessage: "missing" });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    // Per semconv, server spans mark only server-fault codes as errors.
    expect(span.attributes.get("error.type")).toBeUndefined();

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "NOT_FOUND",
    });
    expect(durations[0]?.attributes?.["error.type"]).toBeUndefined();
    expect(durations[0]?.count).toBe(1);
  });

  it("records error.type on unary failure with a server-fault code", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                unaryEntry.tag,
                {
                  kind: "unary",
                  handler: () => Effect.fail(GrpcStatusError.internal("boom")),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const error = yield* Effect.promise(async () => {
            try {
              await (
                implementation.get as (
                  request: unknown,
                  context: HandlerContext,
                ) => Promise<unknown>
              )({}, handlerContext());
            } catch (cause) {
              return cause;
            }
            throw new Error("Expected unary handler to fail");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      ),
    );

    expect(result.error).toMatchObject({ rawMessage: "boom" });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("INTERNAL");
    expect(span.attributes.get("error.type")).toBe("INTERNAL");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "INTERNAL",
      "error.type": "INTERNAL",
    });
    expect(durations[0]?.count).toBe(1);
  });

  // Regression pin for the client-abort span path: on abort, the connect
  // signal must interrupt only the handler body — the spanned effect has to
  // survive long enough to record `cancelled` while the span is open. The
  // OTLP exporter serializes a span inside `end()`, so a status attribute
  // written after an interrupt-torn span end never leaves the process.
  const abortedEffectCall = (
    entry: GrpcMethodEntry,
    handler: (
      interrupted: Deferred.Deferred<boolean>,
    ) => GrpcServerProtocol.GrpcHandler,
    call: (
      implementation: Record<string, unknown>,
      context: HandlerContext,
    ) => Promise<unknown>,
  ) =>
    Effect.gen(function* () {
      const interrupted = yield* Deferred.make<boolean>();
      const { routes } = yield* GrpcServerProtocol.make({
        registry: new Map([[entry.tag, entry]]),
        handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
          [entry.tag, handler(interrupted)],
        ]),
      });
      const implementation = captureImplementation(routes);
      const abort = new AbortController();

      const error = yield* Effect.promise(async () => {
        const pending = call(
          implementation,
          handlerContext(undefined, abort.signal),
        );
        await new Promise((resolve) => setTimeout(resolve, 10));
        abort.abort();
        try {
          await pending;
        } catch (cause) {
          return GrpcStatusError.fromConnectError(cause);
        }
        throw new Error("Expected the aborted call to fail");
      });
      const handlerInterrupted = yield* Deferred.await(interrupted);
      const metrics = yield* Metric.snapshot;
      return { error, handlerInterrupted, metrics };
    });

  const expectCancelledSpanEnd = (
    telemetry: ReturnType<typeof makeTestTelemetry>,
    tag: string,
  ) => {
    const span = telemetry.expectSpan(tag);
    const end = telemetry.endState(span);
    // The status must already be on the span when it ends...
    expect(end.attributesAtEnd.get("rpc.response.status_code")).toBe(
      "CANCELLED",
    );
    // ...and nothing may be written after the end — post-end attributes are
    // exactly what real exporters drop.
    expect(end.attributesAfterEnd).toEqual([]);
    // Per semconv, `cancelled` is not a server fault.
    expect(end.attributesAtEnd.get("error.type")).toBeUndefined();
    // The span must close cleanly with the recorded status, not with an
    // interrupt-only exit (which exporters map to an attributeless close).
    expect(spanEndExit(span)._tag).toBe("Success");
  };

  it("records CANCELLED while the span is open when the client aborts a unary call", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        abortedEffectCall(
          unaryEntry,
          (interrupted) => ({
            kind: "unary",
            handler: () =>
              Effect.never.pipe(
                Effect.onInterrupt(() =>
                  Deferred.succeed(interrupted, true).pipe(Effect.asVoid),
                ),
              ),
          }),
          (implementation, context) =>
            (
              implementation.get as (
                request: unknown,
                context: HandlerContext,
              ) => Promise<unknown>
            )({}, context),
        ),
      ),
    );

    expect(result.error).toMatchObject({ code: "cancelled" });
    expect(result.handlerInterrupted).toBe(true);
    expectCancelledSpanEnd(telemetry, unaryEntry.tag);

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "CANCELLED",
    });
    expect(durations[0]?.attributes?.["error.type"]).toBeUndefined();
  });

  it("records CANCELLED while the span is open when the client aborts a client-streaming call", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        abortedEffectCall(
          clientStreamingEntry,
          (interrupted) => ({
            kind: "client-streaming",
            handler: () =>
              Effect.never.pipe(
                Effect.onInterrupt(() =>
                  Deferred.succeed(interrupted, true).pipe(Effect.asVoid),
                ),
              ),
          }),
          (implementation, context) =>
            (
              implementation.upload as (
                requests: AsyncIterable<unknown>,
                context: HandlerContext,
              ) => Promise<unknown>
            )(
              (async function* () {
                yield { id: "1" };
              })(),
              context,
            ),
        ),
      ),
    );

    expect(result.error).toMatchObject({ code: "cancelled" });
    expect(result.handlerInterrupted).toBe(true);
    expectCancelledSpanEnd(telemetry, clientStreamingEntry.tag);

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Upload",
      "rpc.response.status_code": "CANCELLED",
    });
  });

  // Regression pin for the pump path: a handler stream failing with an
  // interrupt-only cause (the handler interrupting itself, not a client
  // abort) must map to CANCELLED like the effect-shaped calls do — before
  // the fix the pump squashed the cause into a generic error that mapped to
  // INTERNAL with an error span.
  it("maps a handler-side interrupt on a server stream to CANCELLED, not INTERNAL", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [serverStreamingEntry.tag, serverStreamingEntry],
            ]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                serverStreamingEntry.tag,
                {
                  kind: "server-streaming",
                  handler: () =>
                    Stream.make({ sequence: 1 }).pipe(
                      Stream.concat(Stream.fromEffect(Effect.interrupt)),
                    ),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const outcome = yield* Effect.promise(async () => {
            const received: Array<unknown> = [];
            try {
              for await (const value of (
                implementation.watch as (
                  request: unknown,
                  context: HandlerContext,
                ) => AsyncIterable<unknown>
              )({}, handlerContext())) {
                received.push(value);
              }
            } catch (cause) {
              return {
                received,
                error: GrpcStatusError.fromConnectError(cause),
              };
            }
            throw new Error("Expected the interrupted handler stream to fail");
          });
          const metrics = yield* Metric.snapshot;
          return { outcome, metrics };
        }),
      ),
    );

    expect(result.outcome.received).toEqual([{ sequence: 1 }]);
    expect(result.outcome.error).toMatchObject({ code: "cancelled" });
    expectCancelledSpanEnd(telemetry, serverStreamingEntry.tag);

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Watch",
      "rpc.response.status_code": "CANCELLED",
    });
    expect(durations[0]?.attributes?.["error.type"]).toBeUndefined();
  });

  it("records mid-stream bidi failures with the failure status", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[bidiStreamingEntry.tag, bidiStreamingEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                bidiStreamingEntry.tag,
                {
                  kind: "bidi-streaming",
                  handler: (requests) =>
                    Stream.mapEffect(requests, (request) =>
                      (request as { readonly id: string }).id === "boom"
                        ? Effect.fail(GrpcStatusError.notFound("boom"))
                        : Effect.succeed(request),
                    ),
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const error = yield* Effect.promise(async () => {
            try {
              for await (const value of (
                implementation.chat as (
                  request: AsyncIterable<unknown>,
                  context: HandlerContext,
                ) => AsyncIterable<unknown>
              )(
                (async function* () {
                  yield { id: "1" };
                  yield { id: "boom" };
                })(),
                handlerContext(incomingHeaders),
              )) {
                void value;
              }
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected bidi handler failure");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      ),
    );

    expect(result.error).toMatchObject({ code: "not_found" });
    const span = telemetry.expectSpan(bidiStreamingEntry.tag);
    expect(span.kind).toBe("server");
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    // Per semconv, server spans mark only server-fault codes as errors.
    expect(span.attributes.get("error.type")).toBeUndefined();
    const parent = Option.getOrThrow(span.parent);
    expect(parent.traceId).toBe(traceId);

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Chat",
      "rpc.response.status_code": "NOT_FOUND",
    });
    expect(durations[0]?.attributes?.["error.type"]).toBeUndefined();
    expect(durations[0]?.count).toBe(1);
  });

  // Regression pin for `handlersLayer`: the layer captures the whole
  // build-time context, and before the fix it was provided *over* the
  // per-call context — a handler built under a startup span then observed
  // that (already ended) span instead of the gRPC server span, breaking
  // child-span parenting and incoming trace propagation.
  it("keeps request-local tracing over build-time context captured by handlersLayer", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          // Build the handlers layer the way `serveAll` does during startup:
          // under an ambient (bootstrap) span, with a build-time dependency.
          const handlersContext = yield* Layer.build(
            GrpcServerProtocol.handlersLayer({
              [unaryEntry.tag]: {
                kind: "unary",
                handler: () =>
                  Effect.gen(function* () {
                    const dep = yield* Effect.service(BuildDep);
                    yield* Effect.void.pipe(
                      Effect.withSpan("handler-child-unary"),
                    );
                    return { origin: dep.origin };
                  }),
              },
              [serverStreamingEntry.tag]: {
                kind: "server-streaming",
                handler: () =>
                  Stream.fromEffect(
                    Effect.void.pipe(
                      Effect.withSpan("handler-child-stream"),
                      Effect.as({ ok: true }),
                    ),
                  ),
              },
            }),
          ).pipe(
            Effect.scoped,
            Effect.withSpan("bootstrap"),
            Effect.provideService(BuildDep, { origin: "build" }),
          );

          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [unaryEntry.tag, unaryEntry],
              [serverStreamingEntry.tag, serverStreamingEntry],
            ]),
            handlers: Context.get(
              handlersContext,
              GrpcServerProtocol.GrpcHandlers,
            ),
          });
          const implementation = captureImplementation(routes);

          const unaryResponse = yield* Effect.promise(() =>
            (
              implementation.get as (
                request: unknown,
                context: HandlerContext,
              ) => Promise<unknown>
            )({}, handlerContext(incomingHeaders)),
          );
          yield* Effect.promise(async () => {
            for await (const value of (
              implementation.watch as (
                request: unknown,
                context: HandlerContext,
              ) => AsyncIterable<unknown>
            )({}, handlerContext(incomingHeaders))) {
              void value;
            }
          });
          return unaryResponse;
        }),
      ),
    );

    // The build-time dependency must still resolve for the handler...
    expect(result).toEqual({ origin: "build" });
    // ...while spans created by the handler parent to the per-call server
    // span on the incoming trace, not to the bootstrap span.
    const unaryServerSpan = telemetry.expectSpan(unaryEntry.tag);
    const unaryChild = telemetry.expectSpan("handler-child-unary");
    expect(unaryChild.traceId).toBe(traceId);
    expect(Option.getOrThrow(unaryChild.parent).spanId).toBe(
      unaryServerSpan.spanId,
    );

    const streamServerSpan = telemetry.expectSpan(serverStreamingEntry.tag);
    const streamChild = telemetry.expectSpan("handler-child-stream");
    expect(streamChild.traceId).toBe(traceId);
    expect(Option.getOrThrow(streamChild.parent).spanId).toBe(
      streamServerSpan.spanId,
    );
  });

  // Regression pin for the stream-call boundary: the bidi adapter invokes
  // user handler code eagerly, so a synchronous throw used to escape before
  // the try/finally that closes the span scope — surfacing as UNKNOWN to the
  // client and leaking the server span.
  it("maps a synchronously throwing bidi handler to INTERNAL and still closes the span", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[bidiStreamingEntry.tag, bidiStreamingEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                bidiStreamingEntry.tag,
                {
                  kind: "bidi-streaming",
                  handler: () => {
                    throw new Error("sync defect");
                  },
                },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);

          const error = yield* Effect.promise(async () => {
            try {
              for await (const value of (
                implementation.chat as (
                  requests: AsyncIterable<unknown>,
                  context: HandlerContext,
                ) => AsyncIterable<unknown>
              )(
                (async function* () {
                  yield { id: "1" };
                })(),
                handlerContext(),
              )) {
                void value;
              }
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected the throwing bidi handler to fail");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      ),
    );

    expect(result.error).toMatchObject({ code: "internal" });
    const span = telemetry.expectSpan(bidiStreamingEntry.tag);
    // `endState` throws when the span never ended (the leaked-scope case).
    const end = telemetry.endState(span);
    expect(end.attributesAtEnd.get("rpc.response.status_code")).toBe(
      "INTERNAL",
    );
    expect(end.attributesAtEnd.get("error.type")).toBe("INTERNAL");
    expect(end.attributesAfterEnd).toEqual([]);
    // A server-fault code ends the span in an error state.
    expect(spanEndExit(span)._tag).toBe("Failure");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Chat",
      "rpc.response.status_code": "INTERNAL",
      "error.type": "INTERNAL",
    });
  });

  // connect-node enforces the incoming `grpc-timeout` by aborting the handler
  // signal with a deadline_exceeded ConnectError as the abort reason, while a
  // plain client cancel carries no such reason. Regression pins: the server
  // must surface DEADLINE_EXCEEDED — a server-fault status per the repo's
  // semconv subset — instead of collapsing every abort into CANCELLED.
  const deadlineAbort = () => {
    const controller = new AbortController();
    const expire = () =>
      controller.abort(
        new ConnectError("the operation timed out", Code.DeadlineExceeded),
      );
    return { signal: controller.signal, expire };
  };

  it("records DEADLINE_EXCEEDED when connect's deadline aborts a unary call", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [unaryEntry.tag, { kind: "unary", handler: () => Effect.never }],
            ]),
          });
          const implementation = captureImplementation(routes);
          const { signal, expire } = deadlineAbort();

          const error = yield* Effect.promise(async () => {
            const pending = (
              implementation.get as (
                request: unknown,
                context: HandlerContext,
              ) => Promise<unknown>
            )({}, handlerContext(undefined, signal));
            await new Promise((resolve) => setTimeout(resolve, 10));
            expire();
            try {
              await pending;
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected the deadline expiry to fail the call");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      ),
    );

    expect(result.error).toMatchObject({ code: "deadline_exceeded" });
    const span = telemetry.expectSpan(unaryEntry.tag);
    const end = telemetry.endState(span);
    expect(end.attributesAtEnd.get("rpc.response.status_code")).toBe(
      "DEADLINE_EXCEEDED",
    );
    expect(end.attributesAtEnd.get("error.type")).toBe("DEADLINE_EXCEEDED");
    expect(end.attributesAfterEnd).toEqual([]);
    // deadline_exceeded is a server fault: the span ends in an error state.
    expect(spanEndExit(span)._tag).toBe("Failure");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "DEADLINE_EXCEEDED",
      "error.type": "DEADLINE_EXCEEDED",
    });
  });

  it("records DEADLINE_EXCEEDED when connect's deadline aborts a server stream", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [serverStreamingEntry.tag, serverStreamingEntry],
            ]),
            handlers: new Map<string, GrpcServerProtocol.GrpcHandler>([
              [
                serverStreamingEntry.tag,
                { kind: "server-streaming", handler: () => Stream.never },
              ],
            ]),
          });
          const implementation = captureImplementation(routes);
          const { signal, expire } = deadlineAbort();
          setTimeout(expire, 10);

          // On expiry the pump closes the handler and the generator ends
          // cleanly — connect itself writes the deadline trailer.
          const received = yield* Effect.promise(async () => {
            const values: Array<unknown> = [];
            for await (const value of (
              implementation.watch as (
                request: unknown,
                context: HandlerContext,
              ) => AsyncIterable<unknown>
            )({}, handlerContext(undefined, signal))) {
              values.push(value);
            }
            return values;
          });
          const metrics = yield* Metric.snapshot;
          return { received, metrics };
        }),
      ),
    );

    expect(result.received).toEqual([]);
    const span = telemetry.expectSpan(serverStreamingEntry.tag);
    const end = telemetry.endState(span);
    expect(end.attributesAtEnd.get("rpc.response.status_code")).toBe(
      "DEADLINE_EXCEEDED",
    );
    expect(end.attributesAtEnd.get("error.type")).toBe("DEADLINE_EXCEEDED");
    expect(end.attributesAfterEnd).toEqual([]);
    // deadline_exceeded is a server fault: the span ends in an error state.
    expect(spanEndExit(span)._tag).toBe("Failure");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Watch",
      "rpc.response.status_code": "DEADLINE_EXCEEDED",
      "error.type": "DEADLINE_EXCEEDED",
    });
  });
});

describe("tracestate decoding", () => {
  const w3cHeaders: ReadonlyArray<readonly [string, string]> = [
    ["traceparent", "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"],
    ["tracestate", "vendor=abc"],
  ];
  const b3Headers: ReadonlyArray<readonly [string, string]> = [
    ["b3", "0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-1"],
    ["tracestate", "vendor=abc"],
  ];

  it("keeps tracestate alongside a valid W3C traceparent", () => {
    expect(traceStateFromHeaders(w3cHeaders)).toBe("vendor=abc");
    const parent = externalSpanFromHeaders(w3cHeaders);
    expect(parent).toBeDefined();
    expect(Context.get(parent!.annotations, TraceState)).toBe("vendor=abc");
  });

  it("discards tracestate on B3-only requests per W3C trace context", () => {
    expect(traceStateFromHeaders(b3Headers)).toBeUndefined();
    // The span is still parented via B3 — only the tracestate is dropped.
    const parent = externalSpanFromHeaders(b3Headers);
    expect(parent).toBeDefined();
    expect(parent!.traceId).toBe("0af7651916cd43dd8448eb211c80319c");
    expect(Context.get(parent!.annotations, TraceState)).toBeUndefined();
  });

  it("discards tracestate without any propagation headers", () => {
    expect(
      traceStateFromHeaders([["tracestate", "vendor=abc"]]),
    ).toBeUndefined();
  });
});

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/**
 * Attribute activity around a span's end. Real exporters (Effect's OTLP
 * tracer serializes the span inside `end()`) drop attributes written after
 * the span has ended, while Effect's in-memory span silently accepts them —
 * so status assertions must check `attributesAtEnd`, not `span.attributes`.
 */
interface SpanEndState {
  readonly attributesAtEnd: ReadonlyMap<string, unknown>;
  readonly attributesAfterEnd: ReadonlyArray<string>;
}

const makeTestTelemetry = () => {
  const spans: Array<Tracer.Span> = [];
  const endStates = new Map<
    Tracer.Span,
    {
      attributesAtEnd: ReadonlyMap<string, unknown> | undefined;
      readonly attributesAfterEnd: Array<string>;
    }
  >();
  const native = Context.get(Context.empty(), Tracer.Tracer);
  const tracer = Tracer.make({
    span(options) {
      const span = native.span(options);
      // Mirror what the OTLP exporter observes: snapshot the attributes at
      // the moment `end()` runs and record any attribute written afterwards.
      const state = {
        attributesAtEnd: undefined as ReadonlyMap<string, unknown> | undefined,
        attributesAfterEnd: [] as Array<string>,
      };
      endStates.set(span, state);
      const originalEnd = span.end.bind(span);
      const originalAttribute = span.attribute.bind(span);
      span.end = (endTime, exit) => {
        state.attributesAtEnd ??= new Map(span.attributes);
        originalEnd(endTime, exit);
      };
      span.attribute = (key, value) => {
        if (state.attributesAtEnd !== undefined) {
          state.attributesAfterEnd.push(key);
        }
        originalAttribute(key, value);
      };
      spans.push(span);
      return span;
    },
  });
  const registry = new Map<string, never>();
  const provide = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    effect.pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.provideService(Metric.MetricRegistry, registry as never),
    );
  const expectSpan = (name: string): Tracer.Span => {
    const span = spans.find((candidate) => candidate.name === name);
    if (!span) {
      throw new Error(`Expected a span named ${name}`);
    }
    return span;
  };
  const endState = (span: Tracer.Span): SpanEndState => {
    const state = endStates.get(span);
    if (!state || state.attributesAtEnd === undefined) {
      throw new Error(`Expected span ${span.name} to be ended`);
    }
    return state as SpanEndState;
  };
  return { spans, provide, expectSpan, endState };
};

/** The exit a span was ended with; throws when the span is still open. */
const spanEndExit = (span: Tracer.Span): Exit.Exit<unknown, unknown> => {
  const status = (
    span as unknown as {
      readonly status:
        | { readonly _tag: "Started" }
        | {
            readonly _tag: "Ended";
            readonly exit: Exit.Exit<unknown, unknown>;
          };
    }
  ).status;
  if (status._tag !== "Ended") {
    throw new Error(`Expected span ${span.name} to be ended`);
  }
  return status.exit;
};

const durationMetrics = (
  metrics: ReadonlyArray<{
    readonly id: string;
    readonly attributes: Readonly<Record<string, string>> | undefined;
    readonly state: unknown;
  }>,
  id: string,
): Array<{
  readonly attributes: Readonly<Record<string, string>> | undefined;
  readonly count: number;
}> =>
  metrics
    .filter((metric) => metric.id === id)
    .map((metric) => ({
      attributes: metric.attributes,
      count: (metric.state as { readonly count: number }).count,
    }));

const fakeTransport = (behavior: {
  readonly unary?: (header: Headers, signal?: AbortSignal) => unknown;
  readonly stream?: (
    input: AsyncIterable<unknown>,
    header: Headers,
  ) => AsyncIterable<unknown>;
}): { transport: Transport; headers: Array<Headers> } => {
  const headers: Array<Headers> = [];
  const transport = {
    async unary(
      _method: unknown,
      signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      header: HeadersImport | undefined,
      _input: unknown,
    ) {
      const captured = new Headers(header);
      headers.push(captured);
      const message = await behavior.unary!(captured, signal);
      return {
        stream: false,
        message,
        header: new Headers(),
        trailer: new Headers(),
      };
    },
    async stream(
      _method: unknown,
      _signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      header: HeadersImport | undefined,
      input: AsyncIterable<unknown>,
    ) {
      const captured = new Headers(header);
      headers.push(captured);
      const message = behavior.stream!(input, captured);
      return {
        stream: true,
        message,
        header: new Headers(),
        trailer: new Headers(),
      };
    },
  } as unknown as Transport;
  return { transport, headers };
};

const clientLayer = (transport: Transport) =>
  GrpcClientProtocol.layerFromTransport({
    registry: new Map([
      [unaryEntry.tag, unaryEntry],
      [serverStreamingEntry.tag, serverStreamingEntry],
      [clientStreamingEntry.tag, clientStreamingEntry],
      [bidiStreamingEntry.tag, bidiStreamingEntry],
    ]),
    transport,
    serverAddress: new URL("http://api.example.com:8443"),
  });

/** Build-time handler dependency for the `handlersLayer` regression test. */
const BuildDep = Context.Service<{ readonly origin: string }>(
  "effect-grpc-test/BuildDep",
);

const testService = {
  typeName: "demo.v1.TelemetryService",
  methods: [
    { methodKind: "unary", localName: "get" },
    { methodKind: "server_streaming", localName: "watch" },
    { methodKind: "client_streaming", localName: "upload" },
    { methodKind: "bidi_streaming", localName: "chat" },
  ],
} as unknown as GrpcMethodEntry["service"];

const unaryEntry: GrpcMethodEntry = {
  kind: "unary",
  tag: "demo.v1.TelemetryService/Get",
  service: testService,
  localName: "get",
  payloadSchema: Schema.Unknown,
  successSchema: Schema.Unknown,
  toGrpcRequest: (value) => value as never,
  fromGrpcRequest: (message) => message,
  toGrpcResponse: (value) => value as never,
  fromGrpcResponse: (message) => message,
};

const serverStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "server-streaming",
  tag: "demo.v1.TelemetryService/Watch",
  localName: "watch",
};

const clientStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "client-streaming",
  tag: "demo.v1.TelemetryService/Upload",
  localName: "upload",
};

const bidiStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "bidi-streaming",
  tag: "demo.v1.TelemetryService/Chat",
  localName: "chat",
};

const captureImplementation = (
  routes: (router: ConnectRouter) => ConnectRouter,
) => {
  let implementation: Record<string, unknown> | undefined;
  const router = {
    service(_service: unknown, serviceImplementation: unknown) {
      implementation = serviceImplementation as typeof implementation;
      return router;
    },
  };

  routes(router as unknown as ConnectRouter);

  if (!implementation) {
    throw new Error("Expected routes to register a service implementation");
  }
  return implementation;
};

const handlerContext = (
  headers?: HeadersImport,
  signal?: AbortSignal,
): HandlerContext =>
  ({
    requestHeader: new Headers(headers),
    signal: signal ?? new AbortController().signal,
  }) as HandlerContext;
