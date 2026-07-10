import type {
  ConnectRouter,
  HandlerContext,
  Transport,
} from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import {
  Context,
  Deferred,
  Effect,
  Metric,
  Option,
  Schema,
  Stream,
} from "effect";
import * as Tracer from "effect/Tracer";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { TraceState } from "../src/GrpcTracing.js";
import { failureExit, successExit } from "../src/internal/status.js";

const TRACEPARENT_PATTERN = /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/;

type HeadersImport = ConstructorParameters<typeof Headers>[0];

describe("client telemetry", () => {
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
          const response = yield* callUnary(unaryEntry.tag);
          const metrics = yield* Metric.snapshot;
          return { response, metrics };
        }).pipe(
          Effect.withParentSpan(parent),
          Effect.provide(clientLayer(transport)),
        ),
      ),
    );

    expect(result.response).toMatchObject({ _tag: "Exit" });
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
        callUnary(unaryEntry.tag, [["traceparent", provided]]).pipe(
          Effect.provide(clientLayer(transport)),
        ),
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
        callUnary(unaryEntry.tag).pipe(
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
          const response = yield* callUnary(unaryEntry.tag);
          const metrics = yield* Metric.snapshot;
          return { response, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      ),
    );

    expect(result.response).toMatchObject({ _tag: "Exit" });
    if (
      result.response._tag !== "Exit" ||
      result.response.exit._tag !== "Failure"
    ) {
      throw new Error("Expected failure exit");
    }
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    expect(span.attributes.get("error.type")).toBe("NOT_FOUND");

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
          const client = yield* GrpcClientProtocol.GrpcStreamingClient;
          const response = yield* client.clientStreaming(
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
          const client = yield* GrpcClientProtocol.GrpcStreamingClient;
          const error = yield* client
            .clientStreaming(clientStreamingEntry.tag, Stream.make({ id: "1" }))
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
          const client = yield* GrpcClientProtocol.GrpcStreamingClient;
          const responses = yield* client
            .bidiStreaming(
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
          const client = yield* GrpcClientProtocol.GrpcStreamingClient;
          const responses = yield* client
            .bidiStreaming(
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
        Effect.scoped(
          Effect.gen(function* () {
            const { protocol, routes } = yield* GrpcServerProtocol.make({
              registry: new Map([[unaryEntry.tag, unaryEntry]]),
            });
            const implementation = captureImplementation(routes);

            yield* protocol
              .run((clientId, data) =>
                data._tag === "Request"
                  ? protocol.send(clientId, successExit(data.id, { ok: true }))
                  : Effect.void,
              )
              .pipe(Effect.forkScoped);

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

  it("records the status code and error.type on unary handler failure", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.scoped(
          Effect.gen(function* () {
            const { protocol, routes } = yield* GrpcServerProtocol.make({
              registry: new Map([[unaryEntry.tag, unaryEntry]]),
            });
            const implementation = captureImplementation(routes);

            yield* protocol
              .run((clientId, data) =>
                data._tag === "Request"
                  ? protocol.send(
                      clientId,
                      failureExit(data.id, GrpcStatusError.notFound("missing")),
                    )
                  : Effect.void,
              )
              .pipe(Effect.forkScoped);

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
      ),
    );

    expect(result.error).toMatchObject({ rawMessage: "missing" });
    const span = telemetry.expectSpan(unaryEntry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    expect(span.attributes.get("error.type")).toBe("NOT_FOUND");

    const durations = durationMetrics(
      result.metrics,
      "rpc.server.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": "demo.v1.TelemetryService/Get",
      "rpc.response.status_code": "NOT_FOUND",
      "error.type": "NOT_FOUND",
    });
    expect(durations[0]?.count).toBe(1);
  });

  it("records mid-stream bidi failures with the failure status", async () => {
    const telemetry = makeTestTelemetry();

    const result = await Effect.runPromise(
      telemetry.provide(
        Effect.scoped(
          Effect.gen(function* () {
            const { routes } = yield* GrpcServerProtocol.make({
              registry: new Map([[bidiStreamingEntry.tag, bidiStreamingEntry]]),
              streamingHandlers: new Map<
                string,
                GrpcServerProtocol.GrpcStreamingHandler
              >([
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
      ),
    );

    expect(result.error).toMatchObject({ code: "not_found" });
    const span = telemetry.expectSpan(bidiStreamingEntry.tag);
    expect(span.kind).toBe("server");
    expect(span.attributes.get("rpc.response.status_code")).toBe("NOT_FOUND");
    expect(span.attributes.get("error.type")).toBe("NOT_FOUND");
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
      "error.type": "NOT_FOUND",
    });
    expect(durations[0]?.count).toBe(1);
  });
});

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

const makeTestTelemetry = () => {
  const spans: Array<Tracer.Span> = [];
  const native = Context.get(Context.empty(), Tracer.Tracer);
  const tracer = Tracer.make({
    span(options) {
      const span = native.span(options);
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
  return { spans, provide, expectSpan };
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
  readonly unary?: (header: Headers) => unknown;
  readonly stream?: (
    input: AsyncIterable<unknown>,
    header: Headers,
  ) => AsyncIterable<unknown>;
}): { transport: Transport; headers: Array<Headers> } => {
  const headers: Array<Headers> = [];
  const transport = {
    async unary(
      _method: unknown,
      _signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      header: HeadersImport | undefined,
      _input: unknown,
    ) {
      const captured = new Headers(header);
      headers.push(captured);
      const message = await behavior.unary!(captured);
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
      [clientStreamingEntry.tag, clientStreamingEntry],
      [bidiStreamingEntry.tag, bidiStreamingEntry],
    ]),
    transport,
    serverAddress: new URL("http://api.example.com:8443"),
  });

const callUnary = (
  tag: string,
  headers: ReadonlyArray<[string, string]> = [],
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const protocol = yield* RpcClient.Protocol;
      const received = yield* Deferred.make<FromServerEncoded>();

      yield* protocol
        .run(0, (message) =>
          Deferred.succeed(received, message).pipe(Effect.asVoid),
        )
        .pipe(Effect.forkScoped);
      yield* Effect.yieldNow;

      yield* protocol.send(0, {
        _tag: "Request",
        id: "1",
        tag,
        payload: {},
        headers,
      });

      return yield* Deferred.await(received);
    }),
  );

const testService = {
  typeName: "demo.v1.TelemetryService",
  methods: [
    { methodKind: "unary", localName: "get" },
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

const handlerContext = (headers?: HeadersImport): HandlerContext =>
  ({
    requestHeader: new Headers(headers),
    signal: new AbortController().signal,
  }) as HandlerContext;
