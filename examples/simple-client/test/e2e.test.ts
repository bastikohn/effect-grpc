import * as net from "node:net";
import * as OtelTracer from "@effect/opentelemetry/Tracer";
import { SpanKind, SpanStatusCode } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
  type ReadableSpan,
} from "@opentelemetry/sdk-trace-base";
import { Deferred, Effect, Exit, Fiber, Layer, Scope, Stream } from "effect";
import { describe, expect, it } from "vitest";

import {
  GrpcClientProtocol,
  GrpcNodeServer,
  GrpcStatusError,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

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

describe("simple demo e2e", () => {
  it("calls get-user successfully", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "123" });
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(response).toEqual({ user: { id: "123", name: "User 123" } });
  });

  it("maps missing get-user to not_found", async () => {
    const error = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "missing" }).pipe(
            Effect.match({
              onFailure: (error) => error,
              onSuccess: () => {
                throw new Error("Expected getUser to fail");
              },
            }),
          );
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "not_found",
      message: "User not found: missing",
    });
  });

  it("streams watch-users results", async () => {
    const events = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client
            .watchUsers({ tenantId: "demo", count: 3 })
            .pipe(Stream.runCollect);
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(events).toEqual([
      { id: "demo-1", name: "User 1", action: "created", sequence: 1 },
      { id: "demo-2", name: "User 2", action: "updated", sequence: 2 },
      { id: "demo-3", name: "User 3", action: "created", sequence: 3 },
    ]);
  });

  it("passes request metadata and Effect trace context to the server handler", async () => {
    const result = await Effect.runPromise(
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

        return {
          clientTraceId: yield* Deferred.await(clientTrace),
          metadata: yield* Deferred.await(seenMetadata),
          serverTraceId: yield* Deferred.await(seenTrace),
        };
      }),
    );

    expect(result.metadata).toContainEqual(["x-demo", "metadata"]);
    expect(result.serverTraceId).toBe(result.clientTraceId);
  });

  it("exports native gRPC protocol spans through OpenTelemetry", async () => {
    const exporter = new InMemorySpanExporter();
    const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });

    try {
      await Effect.runPromise(
        withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              yield* client.getUser({ id: "123" });
              yield* client.getUser({ id: "missing" }).pipe(
                Effect.match({
                  onFailure: () => undefined,
                  onSuccess: () => {
                    throw new Error("Expected getUser to fail");
                  },
                }),
              );
              yield* client.watchUsers({ tenantId: "demo", count: 1 }).pipe(
                Stream.runDrain,
                Effect.match({
                  onFailure: () => undefined,
                  onSuccess: () => {
                    throw new Error("Expected watchUsers to fail");
                  },
                }),
              );
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          {
            implementation: {
              ...defaultImplementation,
              watchUsers: () =>
                Stream.fail(GrpcStatusError.unavailable("down")),
            },
          },
        ).pipe(Effect.provide(otelTestLayer(provider))),
      );
      await provider.forceFlush();

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

      expect(successClient.attributes).toMatchObject({
        "rpc.system.name": "grpc",
        "rpc.method": "demo.v1.UserService/GetUser",
        "rpc.response.status_code": "OK",
        "server.address": "127.0.0.1",
      });
      expect(successClient.attributes["server.port"]).toEqual(
        expect.any(Number),
      );
      expect(successServer.attributes).toMatchObject({
        "rpc.system.name": "grpc",
        "rpc.method": "demo.v1.UserService/GetUser",
        "rpc.response.status_code": "OK",
      });
      expect(successServer.parentSpanContext?.spanId).toBe(
        successClient.spanContext().spanId,
      );
      expect(successServer.spanContext().traceId).toBe(
        successClient.spanContext().traceId,
      );

      expect(failedClient.attributes).toMatchObject({
        "rpc.response.status_code": "NOT_FOUND",
        "error.type": "NOT_FOUND",
      });
      expect(failedServer.attributes).toMatchObject({
        "rpc.response.status_code": "NOT_FOUND",
        "error.type": "NOT_FOUND",
      });
      expect(failedStreamServer.attributes).toMatchObject({
        "rpc.response.status_code": "UNAVAILABLE",
        "error.type": "UNAVAILABLE",
      });
      expect(failedClient.status.code).toBe(SpanStatusCode.ERROR);
      expect(failedServer.status.code).toBe(SpanStatusCode.ERROR);
      expect(failedStreamServer.status.code).toBe(SpanStatusCode.ERROR);
    } finally {
      await provider.shutdown();
    }
  });

  it("maps client deadlines to deadline_exceeded", async () => {
    const error = await Effect.runPromise(
      withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client.getUser({ id: "slow" }, { timeoutMs: 5 }).pipe(
              Effect.match({
                onFailure: (error) => error,
                onSuccess: () => {
                  throw new Error("Expected getUser to time out");
                },
              }),
            );
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
      ),
    );

    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "deadline_exceeded",
    });
  });

  it("maps server-stream failure before the first chunk", async () => {
    const error = await Effect.runPromise(
      withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client
              .watchUsers({ tenantId: "demo", count: 3 })
              .pipe(
                Stream.runCollect,
                Effect.match({
                  onFailure: (error) => error,
                  onSuccess: () => {
                    throw new Error("Expected watchUsers to fail");
                  },
                }),
              );
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        {
          implementation: {
            ...defaultImplementation,
            watchUsers: () => Stream.fail(GrpcStatusError.unavailable("down")),
          },
        },
      ),
    );

    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "unavailable",
      message: "down",
    });
  });

  it("maps server-stream failure after at least one chunk", async () => {
    const result = await Effect.runPromise(
      withServer(
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
                Effect.match({
                  onFailure: (error) => error,
                  onSuccess: () => {
                    throw new Error("Expected watchUsers to fail");
                  },
                }),
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
      ),
    );

    expect(result.values).toEqual([
      { id: "demo-1", name: "User 1", action: "created", sequence: 1 },
    ]);
    expect(result.error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "unavailable",
      message: "down",
    });
  });

  it("client-side Effect interruption cancels native unary calls", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const implementation: UserServiceImplementation = {
          ...defaultImplementation,
          getUser: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(cancelled, undefined)),
            ),
        };

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              const fiber = yield* client
                .getUser({ id: "hang" })
                .pipe(Effect.forkChild);
              yield* Deferred.await(started).pipe(Effect.timeout("1 second"));
              yield* Effect.sleep("20 millis");
              yield* Fiber.interrupt(fiber);
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { implementation },
        );
      }),
    );
  });

  it("client protocol scope finalization cancels active native unary calls", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const implementation: UserServiceImplementation = {
          ...defaultImplementation,
          getUser: () =>
            Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(cancelled, undefined)),
            ),
        };

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const client = yield* UserServiceClient;
                  yield* client.getUser({ id: "hang" }).pipe(Effect.forkScoped);
                  yield* Deferred.await(started).pipe(
                    Effect.timeout("1 second"),
                  );
                }),
              ).pipe(Effect.provide(clientLayer(baseUrl)));
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }),
          { implementation },
        );
      }),
    );
  });

  it("client-side Effect interruption cancels native server streams", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const implementation: UserServiceImplementation = {
          ...defaultImplementation,
          watchUsers: () =>
            Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
              Stream.ensuring(Deferred.succeed(cancelled, undefined)),
            ),
        };

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* UserServiceClient;
              const fiber = yield* client
                .watchUsers({ tenantId: "demo", count: 1 })
                .pipe(Stream.runDrain, Effect.forkChild);
              yield* Deferred.await(started).pipe(Effect.timeout("1 second"));
              yield* Effect.sleep("20 millis");
              yield* Fiber.interrupt(fiber);
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { implementation },
        );
      }),
    );
  });

  it("client protocol scope finalization cancels active native server streams", async () => {
    await Effect.runPromise(
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>();
        const cancelled = yield* Deferred.make<void>();
        const implementation: UserServiceImplementation = {
          ...defaultImplementation,
          watchUsers: () =>
            Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
              Stream.drain,
              Stream.concat(Stream.never),
              Stream.ensuring(Deferred.succeed(cancelled, undefined)),
            ),
        };

        yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              yield* Effect.scoped(
                Effect.gen(function* () {
                  const client = yield* UserServiceClient;
                  yield* client
                    .watchUsers({ tenantId: "demo", count: 1 })
                    .pipe(Stream.runDrain, Effect.forkScoped);
                  yield* Deferred.await(started).pipe(
                    Effect.timeout("1 second"),
                  );
                }),
              ).pipe(Effect.provide(clientLayer(baseUrl)));
              yield* Deferred.await(cancelled).pipe(Effect.timeout("1 second"));
            }),
          { implementation },
        );
      }),
    );
  });

  it("handles concurrent unary calls, server streams, and isolated stream cancellation", async () => {
    const result = await Effect.runPromise(
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

        return yield* withServer(
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
      }),
    );

    expect(
      result.unaryResults.filter((item) => item._tag === "success"),
    ).toHaveLength(90);
    expect(
      result.unaryResults.filter(
        (item) => item._tag === "failure" && item.code === "not_found",
      ),
    ).toHaveLength(10);
    expect(result.cancelExit._tag).toBe("Failure");
    expect(result.afterCancel).toHaveLength(2);
    expect(result.afterCancel[0]).toMatchObject({ id: "after-cancel-1" });
    expect(result.streamResults).toHaveLength(20);
    for (const [index, events] of result.streamResults.entries()) {
      expect(events).toHaveLength(5);
      expect(events[0]).toMatchObject({ id: `tenant-${index}-1` });
      expect(events[4]).toMatchObject({ id: `tenant-${index}-5` });
    }
  });

  it("server shutdown does not hang with active server streams", async () => {
    await Effect.runPromise(
      Effect.scoped(
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
                group: UserServiceRpcGroup,
                registry: UserServiceGrpcRegistry,
                handlers: UserServiceHandlersLayer(implementation),
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
          }).pipe(
            Effect.provide(clientLayer(new URL(`http://127.0.0.1:${port}`))),
          );
        }),
      ),
    );
  });
});

const withServer = <A, E, R>(
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
  options?: {
    readonly implementation?: UserServiceImplementation;
  },
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [
          {
            group: UserServiceRpcGroup,
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlersLayer(
              options?.implementation ?? defaultImplementation,
            ),
          },
        ],
      }).pipe(Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      return yield* use(new URL(`http://127.0.0.1:${port}`));
    }),
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
  expect(span).toBeDefined();
  return span!;
};

const freePort = Effect.promise(
  () =>
    new Promise<number>((resolve, reject) => {
      const server = net.createServer();
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === "object") {
            resolve(address.port);
          } else {
            reject(new Error("Unable to allocate a local port"));
          }
        });
      });
    }),
);
