import {
  Code,
  ConnectError,
  createContextKey,
  type Interceptor,
} from "@connectrpc/connect";
import * as OtelTracer from "@effect/opentelemetry/OtelTracer";
import { SpanKind } from "@opentelemetry/api";
import {
  BasicTracerProvider,
  InMemorySpanExporter,
  SimpleSpanProcessor,
} from "@opentelemetry/sdk-trace-base";
import { Cause, Deferred, Effect, Fiber, Layer, Option, Stream } from "effect";
import { describe, expect, it } from "vitest";

import {
  GrpcClientProtocol,
  GrpcMethodRegistry,
  GrpcNodeServer,
  GrpcServerProtocol,
  type GrpcStatusError,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceClientService,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlers,
  type FeatureShowcaseServiceClientService,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";

import { freePort, withServer } from "./support.ts";

// Native server interceptors and the per-call handler context, over the real
// gRPC transport. `UserService` supplies the unary and server-streaming
// shapes, `FeatureShowcaseService` the client- and bidi-streaming ones.

const requestIdKey = createContextKey<string | undefined>(undefined, {
  description: "request id",
});
const unsetKey = createContextKey<string | undefined>(undefined, {
  description: "never set",
});

describe("server interceptors", () => {
  it.each(shapes)(
    "runs the chain once per %s call, outermost first, in registration order",
    async (shape) => {
      const trace: Array<string> = [];

      const responses = await Effect.runPromise(
        serve(
          { interceptors: [tracing("outer", trace), tracing("inner", trace)] },
          calls[shape],
        ),
      );

      expect(responses).toHaveLength(messageCount[shape]);
      expect(trace).toEqual([
        "outer:request",
        "inner:request",
        "inner:response",
        "outer:response",
      ]);
    },
  );

  it("observes streamed messages through the response, not through re-invocation", async () => {
    const counts = { invocations: 0, messages: 0 };

    const responses = await Effect.runPromise(
      serve(
        { interceptors: [countMessages(counts)] },
        calls["server-streaming"],
      ),
    );

    expect(responses).toHaveLength(3);
    expect(counts).toEqual({ invocations: 1, messages: 3 });
  });

  it("leaves behavior unchanged when omitted or empty, and never mutates the caller's array", async () => {
    const trace: Array<string> = [];
    const interceptors: ReadonlyArray<Interceptor> = Object.freeze([
      tracing("only", trace),
    ]);

    const results = await Effect.runPromise(
      Effect.all([
        serve({}, calls.unary),
        serve({ interceptors: [] }, calls.unary),
        serve({ interceptors }, calls.unary),
      ]),
    );

    expect(results[0]).toEqual(results[1]);
    expect(results[2]).toEqual(results[0]);
    expect(trace).toEqual(["only:request", "only:response"]);
    expect(interceptors).toHaveLength(1);
  });

  it("plumbs interceptors through serve as well as serveAll", async () => {
    const trace: Array<string> = [];

    const name = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const port = yield* freePort;
          const { routes } = yield* GrpcServerProtocol.make({
            registry: UserServiceGrpcRegistry,
            handlers: yield* UserServiceHandlers({
              ...userImplementation,
              getUser: (request, context) =>
                Effect.succeed({
                  user: {
                    id: request.id,
                    name: context.getContextValue(requestIdKey) ?? "unset",
                  },
                }),
            }),
          });
          yield* GrpcNodeServer.serve({
            host: "127.0.0.1",
            port,
            routes,
            interceptors: [
              tracing("only", trace),
              attachRequestId(() => "req-serve"),
            ],
          }).pipe(Effect.forkScoped);
          yield* Effect.sleep("50 millis");

          return yield* Effect.gen(function* () {
            const client = yield* UserServiceClient;
            const { user } = yield* client.getUser({ id: "1" });
            return user?.name;
          }).pipe(
            Effect.provide(clientLayer(new URL(`http://127.0.0.1:${port}`))),
          );
        }),
      ),
    );

    expect(name).toBe("req-serve");
    expect(trace).toEqual(["only:request", "only:response"]);
  });

  it.each(shapes)(
    "refuses a %s call at the interceptor boundary with the interceptor's status",
    async (shape) => {
      let reached = false;
      const touch = <A, E, R>(self: Effect.Effect<A, E, R>) =>
        Effect.suspend(() => {
          reached = true;
          return self;
        });

      const error = await Effect.runPromise(
        serve(
          {
            interceptors: [
              () => () => {
                throw new ConnectError("no entry", Code.PermissionDenied);
              },
            ],
            user: {
              getUser: (request, context) =>
                touch(userImplementation.getUser(request, context)),
              watchUsers: (request, context) =>
                Stream.unwrap(
                  touch(
                    Effect.succeed(
                      userImplementation.watchUsers(request, context),
                    ),
                  ),
                ),
            },
            features: {
              uploadNotes: (requests, context) =>
                touch(featureImplementation.uploadNotes(requests, context)),
              chat: (requests, context) =>
                Stream.unwrap(
                  touch(
                    Effect.succeed(
                      featureImplementation.chat(requests, context),
                    ),
                  ),
                ),
            },
          },
          calls[shape].pipe(Effect.flip),
        ),
      );

      expect(error).toMatchObject({
        _tag: "GrpcStatusError",
        code: "permission_denied",
        message: "no entry",
      });
      expect(reached).toBe(false);
    },
  );
});

describe("server call context", () => {
  it("delivers each call its own typed values, even while another call completes", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const streamStarted = yield* Deferred.make<void>();
        const unaryDone = yield* Deferred.make<void>();
        let sequence = 0;

        return yield* serve(
          {
            interceptors: [attachRequestId(() => `req-${++sequence}`)],
            user: {
              getUser: (request, context) =>
                Effect.succeed({
                  user: {
                    id: request.id,
                    name: context.getContextValue(requestIdKey) ?? "unset",
                  },
                }),
              // Reads its id, waits for the unary call to come and go, and
              // reads again: the second read must still be its own.
              watchUsers: (_request, context) =>
                Stream.fromEffect(
                  Effect.gen(function* () {
                    const first = context.getContextValue(requestIdKey);
                    yield* Deferred.succeed(streamStarted, undefined);
                    yield* Deferred.await(unaryDone);
                    return {
                      id: first ?? "unset",
                      name: context.getContextValue(requestIdKey) ?? "unset",
                      action: context.getContextValue(unsetKey) ?? "default",
                      sequence: 1,
                    };
                  }),
                ),
            },
          },
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            const stream = yield* client
              .watchUsers({ tenantId: "t", count: 1 })
              .pipe(Stream.runCollect, Effect.forkChild);
            yield* Deferred.await(streamStarted).pipe(
              Effect.timeout("2 seconds"),
            );
            const { user } = yield* client.getUser({ id: "1" });
            yield* Deferred.succeed(unaryDone, undefined);
            const events = yield* Fiber.join(stream);
            return { unary: user?.name, events };
          }),
        );
      }),
    );

    expect(result.unary).toBe("req-2");
    expect(result.events).toEqual([
      { id: "req-1", name: "req-1", action: "default", sequence: 1 },
    ]);
  });

  it("hands a unary handler connect's live call signal", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const seenSignal = yield* Deferred.make<AbortSignal>();
        const seenMethod = yield* Deferred.make<string>();
        const released = yield* Deferred.make<void>();

        yield* serve(
          {
            user: {
              getUser: (_request, context) =>
                Effect.gen(function* () {
                  yield* Deferred.succeed(seenSignal, context.signal);
                  yield* Deferred.succeed(seenMethod, context.method.tag);
                  return yield* Effect.never;
                }).pipe(Effect.ensuring(Deferred.succeed(released, undefined))),
            },
          },
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            const call = yield* client
              .getUser({ id: "hang" })
              .pipe(Effect.forkChild);
            yield* Deferred.await(seenSignal).pipe(Effect.timeout("2 seconds"));
            yield* Effect.sleep("20 millis");
            yield* Fiber.interrupt(call);
          }),
        );

        const signal = yield* Deferred.await(seenSignal);
        const reason = yield* abortOf(signal);
        yield* Deferred.await(released).pipe(Effect.timeout("2 seconds"));
        return { method: yield* Deferred.await(seenMethod), reason };
      }),
    );

    expect(result.method).toBe("demo.v1.UserService/GetUser");
    expect(isDeadline(result.reason)).toBe(false);
  });

  it("reads the remaining deadline live on a delayed stream and sees the call end", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const seenSignal = yield* Deferred.make<AbortSignal>();
        const reads = yield* Deferred.make<{
          readonly atStart: number | undefined;
          readonly afterSetup: number | undefined;
          readonly remaining: () => number | undefined;
        }>();

        const exit = yield* serve(
          {
            user: {
              // A handler whose stream is only built after some setup work,
              // then never ends: only the client's deadline can end it.
              watchUsers: (_request, context) =>
                Stream.unwrap(
                  Effect.gen(function* () {
                    const atStart = context.remainingTimeoutMs();
                    yield* Effect.sleep("30 millis");
                    yield* Deferred.succeed(reads, {
                      atStart,
                      afterSetup: context.remainingTimeoutMs(),
                      remaining: context.remainingTimeoutMs,
                    });
                    yield* Deferred.succeed(seenSignal, context.signal);
                    return Stream.make({
                      id: "1",
                      name: "n",
                      action: "a",
                      sequence: 1,
                    }).pipe(Stream.concat(Stream.never));
                  }),
                ),
            },
          },
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client
              .watchUsers({ tenantId: "t", count: 1 }, { timeoutMs: 300 })
              .pipe(Stream.runCollect, Effect.exit);
          }),
        );

        // The client's own deadline timer and the server's race by design —
        // the server may see its deadline fire or the client's cancellation
        // arrive first — so only the abort itself is asserted here, not its
        // reason. The budget is read again once the deadline has certainly
        // passed.
        yield* abortOf(yield* Deferred.await(seenSignal));
        yield* Effect.sleep("350 millis");
        const { remaining, ...before } = yield* Deferred.await(reads);
        return { exit, ...before, afterDeadline: remaining() };
      }),
    );

    expect(result.exit._tag).toBe("Failure");
    if (result.exit._tag === "Failure") {
      expect(
        Option.getOrUndefined(Cause.findErrorOption(result.exit.cause)),
      ).toMatchObject({ code: "deadline_exceeded" });
    }
    expect(result.atStart).toBeDefined();
    expect(result.atStart!).toBeLessThanOrEqual(300);
    expect(result.afterSetup!).toBeLessThan(result.atStart!);
    expect(result.afterDeadline).toBe(0);
  });

  it("finalizes an intercepted bidi handler once when the client stops early", async () => {
    const counts = { invocations: 0, messages: 0 };

    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const seenSignal = yield* Deferred.make<AbortSignal>();
        const finished = yield* Deferred.make<void>();
        let finalized = 0;

        const echoes = yield* serve(
          {
            interceptors: [countMessages(counts)],
            features: {
              chat: (requests, context) =>
                Stream.fromEffect(
                  Deferred.succeed(seenSignal, context.signal),
                ).pipe(
                  Stream.drain,
                  Stream.concat(featureImplementation.chat(requests, context)),
                  Stream.ensuring(
                    Effect.suspend(() => {
                      finalized++;
                      return Deferred.succeed(finished, undefined);
                    }),
                  ),
                ),
            },
          },
          Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            const echoes = yield* client
              .chat(Stream.forever(Stream.make({ text: "ping", sequence: 1 })))
              .pipe(Stream.take(2), Stream.runCollect);
            yield* Deferred.await(finished).pipe(Effect.timeout("2 seconds"));
            return echoes;
          }),
        );

        yield* abortOf(yield* Deferred.await(seenSignal));
        return { echoes, finalized };
      }),
    );

    expect(result.echoes).toHaveLength(2);
    expect(result.finalized).toBe(1);
    expect(counts.invocations).toBe(1);
    expect(counts.messages).toBeGreaterThanOrEqual(2);
  });

  it("keeps span parenting for accepted calls and creates no server span for refused ones", async () => {
    const exporter = new InMemorySpanExporter();
    const provider = new BasicTracerProvider({
      spanProcessors: [new SimpleSpanProcessor(exporter)],
    });
    const deny: Interceptor = (next) => (request) => {
      if (request.header.get("x-deny") === "1") {
        throw new ConnectError("no entry", Code.PermissionDenied);
      }
      return next(request);
    };

    try {
      await Effect.runPromise(
        serve(
          { interceptors: [deny] },
          Effect.gen(function* () {
            const client = yield* UserServiceClient;
            yield* client.getUser({ id: "1" });
            yield* client
              .getUser({ id: "1" }, { metadata: [["x-deny", "1"]] })
              .pipe(Effect.flip);
          }).pipe(Effect.withSpan("caller")),
        ).pipe(
          Effect.provide(
            OtelTracer.layerWithoutOtelTracer.pipe(
              Layer.provide(
                Layer.succeed(
                  OtelTracer.OtelTracer,
                  provider.getTracer("effect-grpc-test"),
                ),
              ),
            ),
          ),
        ),
      );
      await provider.forceFlush();

      const spans = exporter
        .getFinishedSpans()
        .filter((span) => span.name === "demo.v1.UserService/GetUser");
      const server = spans.filter((span) => span.kind === SpanKind.SERVER);
      const client = spans.filter((span) => span.kind === SpanKind.CLIENT);

      expect(server).toHaveLength(1);
      expect(server[0]!.attributes["rpc.response.status_code"]).toBe("OK");
      expect(
        client.map((span) => span.attributes["rpc.response.status_code"]),
      ).toEqual(["OK", "PERMISSION_DENIED"]);
      expect(server[0]!.parentSpanContext?.spanId).toBe(
        client[0]!.spanContext().spanId,
      );
    } finally {
      await provider.shutdown();
    }
  });
});

const shapes = [
  "unary",
  "server-streaming",
  "client-streaming",
  "bidi-streaming",
] as const;

type Clients = {
  readonly user: UserServiceClientService;
  readonly features: FeatureShowcaseServiceClientService;
};

/** Messages each shape's exchange yields — three for every streamed side. */
const messageCount = {
  unary: 1,
  "server-streaming": 3,
  "client-streaming": 3,
  "bidi-streaming": 3,
} as const;

/** One exchange per call shape, through the generated clients. */
const calls: Record<
  (typeof shapes)[number],
  Effect.Effect<
    ReadonlyArray<unknown>,
    GrpcStatusError.GrpcStatusError,
    UserServiceClient | FeatureShowcaseServiceClient
  >
> = {
  unary: withClients(({ user }) =>
    user.getUser({ id: "1" }).pipe(Effect.map((response) => [response])),
  ),
  "server-streaming": withClients(({ user }) =>
    user.watchUsers({ tenantId: "t", count: 3 }).pipe(Stream.runCollect),
  ),
  "client-streaming": withClients(({ features }) =>
    features
      .uploadNotes(Stream.make({ text: "a" }, { text: "b" }, { text: "c" }))
      .pipe(Effect.map((summary) => summary.joined.split(","))),
  ),
  "bidi-streaming": withClients(({ features }) =>
    features
      .chat(
        Stream.make(
          { text: "a", sequence: 1 },
          { text: "b", sequence: 2 },
          { text: "c", sequence: 3 },
        ),
      )
      .pipe(Stream.runCollect),
  ),
};

function withClients<A, E>(
  use: (clients: Clients) => Effect.Effect<A, E>,
): Effect.Effect<A, E, UserServiceClient | FeatureShowcaseServiceClient> {
  return Effect.gen(function* () {
    return yield* use({
      user: yield* UserServiceClient,
      features: yield* FeatureShowcaseServiceClient,
    });
  });
}

const userImplementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({ user: { id: request.id, name: "Ada" } }),
  watchUsers: (request) =>
    Stream.range(1, request.count).pipe(
      Stream.map((sequence) => ({
        id: `${request.tenantId}-${sequence}`,
        name: "Ada",
        action: "created",
        sequence,
      })),
    ),
};

const featureImplementation: FeatureShowcaseServiceImplementation = {
  describe: (request) => Effect.succeed({ request, summary: "" }),
  uploadNotes: (requests) =>
    Stream.runCollect(requests).pipe(
      Effect.map((notes) => ({
        count: notes.length,
        joined: notes.map((note) => note.text).join(","),
      })),
    ),
  chat: (requests) =>
    Stream.map(requests, (message) => ({
      text: `echo:${message.text}`,
      sequence: message.sequence + 1,
    })),
};

/** Serves both demo services, with overrides, and runs `use` against them. */
const serve = <A, E>(
  options: {
    readonly interceptors?: ReadonlyArray<Interceptor>;
    readonly user?: Partial<UserServiceImplementation>;
    readonly features?: Partial<FeatureShowcaseServiceImplementation>;
  },
  use: Effect.Effect<A, E, UserServiceClient | FeatureShowcaseServiceClient>,
): Effect.Effect<A, E> =>
  withServer(
    {
      interceptors: options.interceptors,
      services: [
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers({
            ...userImplementation,
            ...options.user,
          }),
        },
        {
          registry: FeatureShowcaseServiceGrpcRegistry,
          handlers: FeatureShowcaseServiceHandlers({
            ...featureImplementation,
            ...options.features,
          }),
        },
      ],
    },
    (baseUrl) => use.pipe(Effect.provide(clientLayer(baseUrl))),
  );

const clientLayer = (baseUrl: URL) =>
  Layer.mergeAll(
    UserServiceClientLayer,
    FeatureShowcaseServiceClientLayer,
  ).pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        defaultTimeoutMs: 5_000,
        registry: GrpcMethodRegistry.merge([
          UserServiceGrpcRegistry,
          FeatureShowcaseServiceGrpcRegistry,
        ]),
      }),
    ),
  );

/** Appends `name:request` on the way in and `name:response` on the way out. */
const tracing =
  (name: string, trace: Array<string>): Interceptor =>
  (next) =>
  async (request) => {
    trace.push(`${name}:request`);
    const response = await next(request);
    trace.push(`${name}:response`);
    return response;
  };

/** Sets the request id key from `mint` before the handler runs. */
const attachRequestId =
  (mint: () => string): Interceptor =>
  (next) =>
  (request) => {
    request.contextValues.set(requestIdKey, mint());
    return next(request);
  };

/**
 * Counts invocations of the interceptor itself and, for stream responses,
 * the messages that flow through — connect's streaming interception pattern:
 * wrap the response's message iterable rather than awaiting the response.
 */
const countMessages =
  (counts: { invocations: number; messages: number }): Interceptor =>
  (next) =>
  async (request) => {
    counts.invocations++;
    const response = await next(request);
    if (!response.stream) return response;
    return {
      ...response,
      message: (async function* () {
        for await (const message of response.message) {
          counts.messages++;
          yield message;
        }
      })(),
    };
  };

/** Resolves with the signal's abort reason once it aborts. */
const abortOf = (signal: AbortSignal) =>
  Effect.promise<unknown>(
    () =>
      new Promise((resolve) => {
        if (signal.aborted) {
          resolve(signal.reason);
          return;
        }
        signal.addEventListener("abort", () => resolve(signal.reason), {
          once: true,
        });
      }),
  ).pipe(Effect.timeout("2 seconds"));

const isDeadline = (reason: unknown): boolean =>
  reason instanceof ConnectError && reason.code === Code.DeadlineExceeded;
