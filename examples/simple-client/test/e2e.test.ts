import * as OtelTracer from "@effect/opentelemetry/OtelTracer";
import { assert, describe, it } from "@effect/vitest";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import { Deferred, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";

import {
  GrpcClientProtocol,
  GrpcNodeServer,
  GrpcStatusError,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceClientService,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

import { freePort, withServer as serve } from "./support.ts";

const defaultImplementation: UserServiceImplementation = {
  getUser: (request) =>
    request.id === "missing"
      ? Effect.fail(GrpcStatusError.notFound(`User not found: ${request.id}`))
      : Effect.succeed({
          user: {
            id: request.id,
            name: `User ${request.id}`,
          },
        }),
  watchUsers: (request) =>
    Stream.range(1, request.count).pipe(
      Stream.map((sequence) => ({
        id: `${request.tenantId}-${sequence}`,
        name: `User ${sequence}`,
        action: sequence % 2 === 0 ? "updated" : "created",
        sequence,
      })),
    ),
};

// Every test boots a real node http2 server and talks to it over real
// sockets, so they all run under `it.live`: the TestClock would freeze the
// connect transport's deadlines and the server startup sleep.
describe("simple demo e2e", () => {
  it.live("calls get-user successfully", () =>
    Effect.gen(function* () {
      const response = yield* withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "123" });
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      );

      assert.deepStrictEqual(response, {
        user: { id: "123", name: "User 123" },
      });
    }),
  );

  it.live("maps missing get-user to not_found", () =>
    Effect.gen(function* () {
      const error = yield* withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "missing" }).pipe(Effect.flip);
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      );

      assert.deepInclude(error, {
        _tag: "GrpcStatusError",
        code: "not_found",
        message: "User not found: missing",
      });
    }),
  );

  it.live("streams watch-users results", () =>
    Effect.gen(function* () {
      const events = yield* withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client
            .watchUsers({ tenantId: "demo", count: 3 })
            .pipe(Stream.runCollect);
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      );

      assert.deepStrictEqual(events, [
        { id: "demo-1", name: "User 1", action: "created", sequence: 1 },
        { id: "demo-2", name: "User 2", action: "updated", sequence: 2 },
        { id: "demo-3", name: "User 3", action: "created", sequence: 3 },
      ]);
    }),
  );

  it.live(
    "passes request metadata and Effect trace context to the server handler",
    () =>
      Effect.gen(function* () {
        const seenMetadata =
          yield* Deferred.make<ReadonlyArray<readonly [string, unknown]>>();
        const seenTrace = yield* Deferred.make<string>();
        const clientTrace = yield* Deferred.make<string>();
        const implementation: UserServiceImplementation = {
          ...defaultImplementation,
          getUser: (request, context) =>
            Effect.gen(function* () {
              const span = yield* Effect.currentSpan.pipe(Effect.orDie);
              yield* Deferred.succeed(seenTrace, span.traceId);
              yield* Deferred.succeed(seenMetadata, context.metadata);
              return yield* defaultImplementation.getUser(request, context);
            }),
        };

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              yield* Effect.gen(function* () {
                const span = yield* Effect.currentSpan.pipe(Effect.orDie);
                yield* Deferred.succeed(clientTrace, span.traceId);
                yield* client.getUser(
                  { id: "123" },
                  { metadata: [["x-demo", "metadata"]] },
                );
              }).pipe(Effect.withSpan("client-call"));
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { implementation },
        );

        const clientTraceId = yield* Deferred.await(clientTrace);
        const metadata = yield* Deferred.await(seenMetadata);
        const serverTraceId = yield* Deferred.await(seenTrace);

        assert.deepInclude(metadata, ["x-demo", "metadata"]);
        assert.strictEqual(serverTraceId, clientTraceId);
      }),
  );

  it.live("exports native gRPC protocol spans through OpenTelemetry", () => {
    const exporter = new InMemorySpanExporter();
    const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });

    return Effect.gen(function* () {
      yield* withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            yield* client.getUser({ id: "123" });
            yield* client.getUser({ id: "missing" }).pipe(Effect.flip);
            yield* client
              .watchUsers({ tenantId: "demo", count: 1 })
              .pipe(Stream.runDrain, Effect.flip);
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        {
          implementation: {
            ...defaultImplementation,
            watchUsers: () => Stream.fail(GrpcStatusError.unavailable("down")),
          },
        },
      ).pipe(Effect.provide(otelTestLayer(provider)));
      yield* Effect.promise(() => provider.forceFlush());

      const spans = exporter.getFinishedSpans();
      const successClient = protocolSpan(
        spans,
        "demo.v1.UserService/GetUser",
        SpanKind.CLIENT,
        "OK",
      );
      const successServer = protocolSpan(
        spans,
        "demo.v1.UserService/GetUser",
        SpanKind.SERVER,
        "OK",
      );
      const failedClient = protocolSpan(
        spans,
        "demo.v1.UserService/GetUser",
        SpanKind.CLIENT,
        "NOT_FOUND",
      );
      const failedServer = protocolSpan(
        spans,
        "demo.v1.UserService/GetUser",
        SpanKind.SERVER,
        "NOT_FOUND",
      );
      const failedStreamServer = protocolSpan(
        spans,
        "demo.v1.UserService/WatchUsers",
        SpanKind.SERVER,
        "UNAVAILABLE",
      );

      assert.deepInclude(successClient.attributes, {
        "rpc.system.name": "grpc",
        "rpc.method": "demo.v1.UserService/GetUser",
        "rpc.response.status_code": "OK",
        "server.address": "127.0.0.1",
      });
      assert.typeOf(successClient.attributes["server.port"], "number");
      assert.deepInclude(successServer.attributes, {
        "rpc.system.name": "grpc",
        "rpc.method": "demo.v1.UserService/GetUser",
        "rpc.response.status_code": "OK",
      });
      assert.strictEqual(
        successServer.parentSpanContext?.spanId,
        successClient.spanContext().spanId,
      );
      assert.strictEqual(
        successServer.spanContext().traceId,
        successClient.spanContext().traceId,
      );

      assert.deepInclude(failedClient.attributes, {
        "rpc.response.status_code": "NOT_FOUND",
        "error.type": "NOT_FOUND",
      });
      // NOT_FOUND is not a server fault: the server span records the status
      // but is not marked as an error (per semconv, servers only flag
      // UNKNOWN, DEADLINE_EXCEEDED, UNIMPLEMENTED, INTERNAL, UNAVAILABLE,
      // and DATA_LOSS).
      assert.deepInclude(failedServer.attributes, {
        "rpc.response.status_code": "NOT_FOUND",
      });
      assert.isUndefined(failedServer.attributes["error.type"]);
      // UNAVAILABLE is a server fault and stays an error on the server span.
      assert.deepInclude(failedStreamServer.attributes, {
        "rpc.response.status_code": "UNAVAILABLE",
        "error.type": "UNAVAILABLE",
      });
      assert.strictEqual(failedClient.status.code, SpanStatusCode.ERROR);
      assert.notStrictEqual(failedServer.status.code, SpanStatusCode.ERROR);
      assert.strictEqual(failedStreamServer.status.code, SpanStatusCode.ERROR);
    }).pipe(Effect.ensuring(Effect.promise(() => provider.shutdown())));
  });

  it.live("maps client deadlines to deadline_exceeded", () =>
    Effect.gen(function* () {
      const error = yield* withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client
              .getUser({ id: "slow" }, { timeoutMs: 5 })
              .pipe(Effect.flip);
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        {
          implementation: {
            ...defaultImplementation,
            getUser: () =>
              Effect.sleep("1 second").pipe(
                Effect.as({ user: { id: "slow", name: "Slow User" } }),
              ),
          },
        },
      );

      assert.deepInclude(error, {
        _tag: "GrpcStatusError",
        code: "deadline_exceeded",
      });
    }),
  );

  it.live("maps server-stream failure before the first chunk", () =>
    Effect.gen(function* () {
      const error = yield* withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client
              .watchUsers({ tenantId: "demo", count: 3 })
              .pipe(Stream.runCollect, Effect.flip);
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        {
          implementation: {
            ...defaultImplementation,
            watchUsers: () => Stream.fail(GrpcStatusError.unavailable("down")),
          },
        },
      );

      assert.deepInclude(error, {
        _tag: "GrpcStatusError",
        code: "unavailable",
        message: "down",
      });
    }),
  );

  it.live("maps server-stream failure after at least one chunk", () =>
    Effect.gen(function* () {
      const result = yield* withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            const values: Array<unknown> = [];
            const error = yield* client
              .watchUsers({ tenantId: "demo", count: 3 })
              .pipe(
                Stream.runForEach((event) =>
                  Effect.sync(() => {
                    values.push(event);
                  }),
                ),
                Effect.flip,
              );
            return { values, error };
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        {
          implementation: {
            ...defaultImplementation,
            watchUsers: () =>
              Stream.make({
                id: "demo-1",
                name: "User 1",
                action: "created",
                sequence: 1,
              }).pipe(
                Stream.concat(Stream.fail(GrpcStatusError.unavailable("down"))),
              ),
          },
        },
      );

      assert.deepStrictEqual(result.values, [
        { id: "demo-1", name: "User 1", action: "created", sequence: 1 },
      ]);
      assert.deepInclude(result.error, {
        _tag: "GrpcStatusError",
        code: "unavailable",
        message: "down",
      });
    }),
  );

  // Signal `started`, block until torn down, then signal `cancelled` from the
  // finalizer. Both termination modes below assert the same fact — the
  // server-side call is cancelled — so they are parameterised over the call
  // shape rather than spelled out per shape.
  const hang = (
    started: Deferred.Deferred<void>,
    cancelled: Deferred.Deferred<void>,
  ) =>
    Deferred.succeed(started, undefined).pipe(
      Effect.andThen(Effect.never),
      Effect.ensuring(Deferred.succeed(cancelled, undefined)),
    );

  const hangingCall = {
    unary: {
      implementation: (hanging: Effect.Effect<never>) => ({
        ...defaultImplementation,
        getUser: () => hanging,
      }),
      call: (client: UserServiceClientService) =>
        client.getUser({ id: "hang" }),
    },
    "server stream": {
      implementation: (hanging: Effect.Effect<never>) => ({
        ...defaultImplementation,
        watchUsers: () => Stream.fromEffect(hanging),
      }),
      call: (client: UserServiceClientService) =>
        client.watchUsers({ tenantId: "demo", count: 1 }).pipe(Stream.runDrain),
    },
  } as const;

  // `it.live.each` hands each case to the test whole (vitest's `it.for`), so
  // the shapes are plain values rather than argument tuples.
  const shapes = ["unary", "server stream"] as const;

  it.live.each(shapes)(
    "client-side Effect interruption cancels native %s calls",
    (shape) =>
      Effect.gen(function* () {
        const { call, implementation } = hangingCall[shape];
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              const fiber = yield* call(client).pipe(Effect.forkChild);
              yield* Deferred.await(started).pipe(Effect.timeout("1 second"));
              // Give connect-node a beat to flush the request before the
              // interrupt races it; without this the abort can arrive first
              // and the server never starts the call.
              yield* Effect.sleep("20 millis");
              yield* Fiber.interrupt(fiber);
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { implementation: implementation(hang(started, cancelled)) },
        );
      }),
  );

  it.live.each(shapes)(
    "client protocol scope finalization cancels active native %s calls",
    (shape) =>
      Effect.gen(function* () {
        const { call, implementation } = hangingCall[shape];
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              // The client layer lives in the inner scope; closing it is what
              // must cancel the in-flight call, so `cancelled` is awaited
              // outside that scope.
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const client = yield* UserServiceClient;
                  yield* call(client).pipe(Effect.forkScoped);
                  yield* Deferred.await(started).pipe(
                    Effect.timeout("1 second"),
                  );
                }),
              ).pipe(Effect.provide(clientLayer(baseUrl)));
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }),
          { implementation: implementation(hang(started, cancelled)) },
        );
      }),
  );

  it.live(
    "handles concurrent unary calls, server streams, and isolated stream cancellation",
    () =>
      Effect.gen(function* () {
        const cancelStarted = yield* Deferred.make<void>();
        const cancelFinalized = yield* Deferred.make<void>();
        const implementation: UserServiceImplementation = {
          getUser: (request) =>
            request.id.startsWith("missing-")
              ? Effect.fail(
                  GrpcStatusError.notFound(`User not found: ${request.id}`),
                )
              : Effect.succeed({
                  user: {
                    id: request.id,
                    name: `User ${request.id}`,
                  },
                }),
          watchUsers: (request) =>
            request.tenantId === "cancel"
              ? Stream.fromEffect(
                  Deferred.succeed(cancelStarted, undefined),
                ).pipe(
                  Stream.drain,
                  Stream.concat(Stream.never),
                  Stream.ensuring(Deferred.succeed(cancelFinalized, undefined)),
                )
              : Stream.range(1, request.count).pipe(
                  Stream.tap(() => Effect.sleep("5 millis")),
                  Stream.map((sequence) => ({
                    id: `${request.tenantId}-${sequence}`,
                    name: `User ${sequence}`,
                    action: sequence % 2 === 0 ? "updated" : "created",
                    sequence,
                  })),
                ),
        };

        const result = yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              const unaryIds = Array.from({ length: 100 }, (_, index) =>
                index % 10 === 0 ? `missing-${index}` : `user-${index}`,
              );
              const unaryResults = yield* Effect.all(
                unaryIds.map((id) =>
                  client.getUser({ id }).pipe(
                    Effect.match({
                      onFailure: (error) => ({
                        _tag: "failure" as const,
                        code:
                          error._tag === "GrpcStatusError"
                            ? error.code
                            : "client",
                      }),
                      onSuccess: (response) => ({
                        _tag: "success" as const,
                        id: response.user?.id,
                      }),
                    }),
                  ),
                ),
                { concurrency: "unbounded" },
              );

              const streamResults = yield* Effect.all(
                Array.from({ length: 20 }, (_, index) =>
                  client
                    .watchUsers({ tenantId: `tenant-${index}`, count: 5 })
                    .pipe(
                      Stream.runCollect,
                      Effect.map((events) => Array.from(events)),
                    ),
                ),
                { concurrency: "unbounded" },
              );

              const cancelFiber = yield* client
                .watchUsers({ tenantId: "cancel", count: 1 }, { timeoutMs: 20 })
                .pipe(Stream.runDrain, Effect.exit, Effect.forkDetach);
              yield* Deferred.await(cancelStarted).pipe(
                Effect.timeout("1 second"),
              );

              const cancelExit = yield* Fiber.join(cancelFiber);
              yield* Deferred.await(cancelFinalized).pipe(
                Effect.timeout("1 second"),
              );
              const afterCancel = yield* client
                .watchUsers({ tenantId: "after-cancel", count: 2 })
                .pipe(
                  Stream.runCollect,
                  Effect.map((events) => Array.from(events)),
                );

              return { afterCancel, cancelExit, unaryResults, streamResults };
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { implementation },
        );

        assert.lengthOf(
          result.unaryResults.filter((item) => item._tag === "success"),
          90,
        );
        assert.lengthOf(
          result.unaryResults.filter(
            (item) => item._tag === "failure" && item.code === "not_found",
          ),
          10,
        );
        assert.strictEqual(result.cancelExit._tag, "Failure");
        assert.lengthOf(result.afterCancel, 2);
        assert.deepInclude(result.afterCancel[0], { id: "after-cancel-1" });
        assert.lengthOf(result.streamResults, 20);
        for (const [index, events] of result.streamResults.entries()) {
          assert.lengthOf(events, 5);
          assert.deepInclude(events[0], { id: `tenant-${index}-1` });
          assert.deepInclude(events[4], { id: `tenant-${index}-5` });
        }
      }),
  );

  it.live("server shutdown does not hang with active server streams", () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const serverScope = yield* Scope.make();
      const port = yield* freePort;

      const implementation: UserServiceImplementation = {
        ...defaultImplementation,
        watchUsers: () =>
          Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
            Stream.drain,
            Stream.concat(Stream.never),
          ),
      };

      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        shutdownTimeoutMs: 20,
        services: [
          {
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlers(implementation),
          },
        ],
      }).pipe(Effect.forkScoped, Scope.provide(serverScope));
      yield* Effect.sleep("50 millis");

      yield* Effect.gen(function* () {
        const client = yield* UserServiceClient;
        yield* client
          .watchUsers({ tenantId: "hang", count: 1 })
          .pipe(Stream.runDrain, Effect.exit, Effect.forkScoped);
        yield* Deferred.await(started).pipe(Effect.timeout("1 second"));
        yield* Scope.close(serverScope, Exit.void).pipe(
          Effect.timeout("1 second"),
        );
      }).pipe(Effect.provide(clientLayer(new URL(`http://127.0.0.1:${port}`))));
    }),
  );
});

const withServer = <A, E, R>(
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
  options?: {
    readonly implementation?: UserServiceImplementation;
  },
): Effect.Effect<A, E, R> =>
  serve(
    {
      services: [
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers(
            options?.implementation ?? defaultImplementation,
          ),
        },
      ],
    },
    use,
  );

const clientLayer = (baseUrl: URL) =>
  UserServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        defaultTimeoutMs: 1_000,
        registry: UserServiceGrpcRegistry,
      }),
    ),
  );

const otelTestLayer = (provider: BasicTracerProvider) =>
  OtelTracer.layerWithoutOtelTracer.pipe(
    Layer.provide(
      Layer.succeed(
        OtelTracer.OtelTracer,
        provider.getTracer("effect-grpc-test"),
      ),
    ),
  );

const protocolSpan = (
  spans: ReadonlyArray<ReadableSpan>,
  name: string,
  kind: SpanKind,
  statusCode: string,
): ReadableSpan => {
  const span = spans.find(
    (span) =>
      span.name === name &&
      span.kind === kind &&
      span.attributes["rpc.response.status_code"] === statusCode,
  );
  assert.isDefined(span);
  return span!;
};
