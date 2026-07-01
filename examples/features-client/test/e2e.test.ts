import * as net from "node:net";

import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import {
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  Layer,
  Option,
  Stream,
} from "effect";
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
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlersLayer,
  FeatureShowcaseServiceRpcGroup,
  type FeatureRequest,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc";
import { FeatureShowcaseService } from "@effect-grpc/features-proto/generated/features/v1/showcase_pb";

const featureRequest = (): FeatureRequest => ({
  tags: ["alpha", "beta"],
  scores: [10, 20],
  notes: [{ text: "generated feature demo" }],
  state: 1,
  owner: { id: "user-1", name: "Ada" },
  labels: { env: "demo" },
  counts: { attempts: 1 },
  reviewers: { primary: { id: "reviewer-1", role: "owner" } },
  createdAt: new Date(1_500),
  ttl: Duration.nanos(2_250_000_001n),
  payload: new Uint8Array([1, 2, 3]),
  sequence: 42n,
  contact: { case: "contactUser", value: { id: "user-1", role: "owner" } },
});

const implementation: FeatureShowcaseServiceImplementation = {
  describe: (request) =>
    Effect.succeed({
      request,
      summary: summary(request),
    }),
  uploadNotes: (requests) =>
    requests.pipe(
      Stream.mapEffect((note) =>
        note.text === "boom"
          ? Effect.fail(
              GrpcStatusError.make({
                code: "failed_precondition",
                message: "boom note",
              }),
            )
          : Effect.succeed(note.text),
      ),
      Stream.runCollect,
      Effect.map((texts) => ({
        count: texts.length,
        joined: texts.join(","),
      })),
    ),
  chat: (requests) =>
    requests.pipe(
      Stream.mapEffect((message) =>
        message.text === "boom"
          ? Effect.fail(
              GrpcStatusError.make({
                code: "failed_precondition",
                message: "boom message",
              }),
            )
          : Effect.succeed({
              text: `echo:${message.text}`,
              sequence: message.sequence + 1,
            }),
      ),
    ),
};

const userImplementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: {
        id: request.id,
        name: "Secondary User",
      },
    }),
  watchUsers: (request) =>
    Stream.make({
      id: request.tenantId,
      name: "Secondary User",
      action: "created",
      sequence: 1,
    }),
};

describe("features demo e2e", () => {
  it("round-trips the supported feature matrix through the Effect client", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          return yield* client.describe(featureRequest());
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(response.summary).toBe(
      "owner=Ada tags=2 notes=1 labels=1 payload=3 sequence=42 contact=contactUser",
    );
    expectRuntimeRequest(response.request);
  });

  it("serves native gRPC calls from a non-Effect connect client", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.promise(async () => {
          const client = createClient(
            FeatureShowcaseService,
            createGrpcTransport({
              baseUrl: baseUrl.toString().replace(/\/$/, ""),
            }),
          );

          return client.describe({
            tags: ["alpha"],
            scores: [7],
            notes: [{ text: "direct gRPC" }],
            state: 99 as never,
            owner: { id: "user-2", name: "Grace" },
            labels: { env: "interop" },
            counts: { attempts: 2 },
            reviewers: { secondary: { id: "reviewer-2", role: "reviewer" } },
            createdAt: { seconds: 1n, nanos: 500_000_000 },
            ttl: { seconds: 2n, nanos: 250_000_001 },
            payload: new Uint8Array([4, 5, 6]),
            sequence: 99n,
            contact: { case: "contactEmail", value: "grace@example.com" },
          });
        }),
      ),
    );

    expect(response.summary).toBe(
      "owner=Grace tags=1 notes=1 labels=1 payload=3 sequence=99 contact=contactEmail",
    );
    expect(response.request?.state).toBe(99);
    expect(response.request?.createdAt).toMatchObject({
      seconds: 1n,
      nanos: 500_000_000,
    });
    expect(response.request?.ttl).toMatchObject({
      seconds: 2n,
      nanos: 250_000_001,
    });
    expect(response.request?.payload).toEqual(new Uint8Array([4, 5, 6]));
    expect(response.request?.sequence).toBe(99n);
    expect(response.request?.contact).toEqual({
      case: "contactEmail",
      value: "grace@example.com",
    });
  });

  it("routes requests to generated services after the first service", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "secondary" });
        }).pipe(Effect.provide(userClientLayer(baseUrl))),
      ),
    );

    expect(response.user).toEqual({
      id: "secondary",
      name: "Secondary User",
    });
  });

  it("round-trips a client-streaming upload through the Effect client", async () => {
    const uploaded = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          return yield* client.uploadNotes(
            Stream.make({ text: "alpha" }, { text: "beta" }, { text: "gamma" }),
          );
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(uploaded).toEqual({ count: 3, joined: "alpha,beta,gamma" });
  });

  it("round-trips a bidi chat through the Effect client", async () => {
    const echoes = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          return yield* Stream.runCollect(
            client.chat(
              Stream.make(
                { text: "hi", sequence: 1 },
                { text: "there", sequence: 2 },
              ),
            ),
          );
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(echoes).toEqual([
      { text: "echo:hi", sequence: 2 },
      { text: "echo:there", sequence: 3 },
    ]);
  });

  it("propagates a mid-stream server failure to the client-streaming caller", async () => {
    const error = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          return yield* client
            .uploadNotes(
              Stream.make({ text: "ok" }, { text: "boom" }, { text: "late" }),
            )
            .pipe(Effect.flip);
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "failed_precondition",
      message: "boom note",
    });
  });

  it("fails the bidi response stream when the server fails mid-stream", async () => {
    const result = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          const collected: Array<{ text: string; sequence: number }> = [];
          const error = yield* client
            .chat(
              Stream.make(
                { text: "hi", sequence: 1 },
                { text: "boom", sequence: 2 },
              ),
            )
            .pipe(
              Stream.tap((message) =>
                Effect.sync(() => collected.push(message)),
              ),
              Stream.runDrain,
              Effect.flip,
            );
          return { collected, error };
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(result.collected).toEqual([{ text: "echo:hi", sequence: 2 }]);
    expect(result.error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "failed_precondition",
      message: "boom message",
    });
  });

  it("cancels the call and surfaces the original error when the request stream fails", async () => {
    const failure = { _tag: "UploadSourceFailure" as const };
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const observed =
          yield* Deferred.make<
            Exit.Exit<unknown, GrpcStatusError.GrpcStatusError>
          >();
        const received = yield* Deferred.make<void>();
        const uploadNotes: FeatureShowcaseServiceImplementation["uploadNotes"] =
          (requests) =>
            requests.pipe(
              Stream.tap(() => Deferred.succeed(received, undefined)),
              Stream.runCollect,
              // Keep the handler busy after the stream ends so the
              // cancellation is observable even if connect-node delivers the
              // abort a beat after the request stream closes.
              Effect.andThen(Effect.sleep("500 millis")),
              Effect.as({ count: 0, joined: "" }),
              Effect.onExit((exit) => Deferred.succeed(observed, exit)),
            );
        const exit = yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* FeatureShowcaseServiceClient;
              return yield* client.uploadNotes(
                Stream.concat(
                  Stream.make({ text: "first" }),
                  // Fail only after the server has consumed the first note, so
                  // the cancellation is observable server-side.
                  Stream.fromEffect(
                    Deferred.await(received).pipe(
                      Effect.andThen(Effect.fail(failure)),
                    ),
                  ),
                ),
              );
            }).pipe(Effect.provide(clientLayer(baseUrl)), Effect.exit),
          { uploadNotes },
        );
        const serverExit = yield* Deferred.await(observed);
        return { exit, serverExit };
      }),
    );

    expect(result.exit._tag).toBe("Failure");
    if (result.exit._tag === "Failure") {
      expect(Cause.squash(result.exit.cause)).toBe(failure);
    }
    // The server observes the cancellation either as a failed request stream
    // or as an interruption of the handler fiber.
    expect(result.serverExit._tag).toBe("Failure");
    if (result.serverExit._tag === "Failure") {
      const error = Cause.findErrorOption(result.serverExit.cause);
      expect(
        Cause.hasInterrupts(result.serverExit.cause) ||
          (Option.isSome(error) && error.value.code === "cancelled"),
      ).toBe(true);
    }
  });

  it("stops the server handler when the bidi consumer stops early", async () => {
    const echoes = await Effect.runPromise(
      Effect.gen(function* () {
        const finished = yield* Deferred.make<void>();
        const chat: FeatureShowcaseServiceImplementation["chat"] = (requests) =>
          requests.pipe(
            Stream.map((message) => ({
              text: `echo:${message.text}`,
              sequence: message.sequence + 1,
            })),
            Stream.ensuring(Deferred.succeed(finished, undefined)),
          );
        return yield* withServer(
          (baseUrl) =>
            Effect.gen(function* () {
              const client = yield* FeatureShowcaseServiceClient;
              const echoes = yield* client
                .chat(
                  Stream.forever(Stream.make({ text: "ping", sequence: 1 })),
                )
                .pipe(Stream.take(2), Stream.runCollect);
              // The handler must terminate through cancellation while the
              // server is still running.
              yield* Deferred.await(finished);
              return echoes;
            }).pipe(Effect.provide(clientLayer(baseUrl))),
          { chat },
        );
      }),
    );

    expect(echoes).toEqual([
      { text: "echo:ping", sequence: 2 },
      { text: "echo:ping", sequence: 2 },
    ]);
  });

  it("serves native connect streaming clients", async () => {
    const result = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.promise(async () => {
          const client = createClient(
            FeatureShowcaseService,
            createGrpcTransport({
              baseUrl: baseUrl.toString().replace(/\/$/, ""),
            }),
          );

          const uploaded = await client.uploadNotes(
            (async function* () {
              yield { text: "native" };
              yield { text: "grpc" };
            })(),
          );
          const echoes: Array<{ text: string; sequence: number }> = [];
          for await (const message of client.chat(
            (async function* () {
              yield { text: "hello", sequence: 41 };
            })(),
          )) {
            echoes.push({ text: message.text, sequence: message.sequence });
          }
          return {
            uploaded: { count: uploaded.count, joined: uploaded.joined },
            echoes,
          };
        }),
      ),
    );

    expect(result.uploaded).toEqual({ count: 2, joined: "native,grpc" });
    expect(result.echoes).toEqual([{ text: "echo:hello", sequence: 42 }]);
  });
});

const summary = (request: FeatureRequest) =>
  [
    `owner=${request.owner?.name ?? "unknown"}`,
    `tags=${request.tags.length}`,
    `notes=${request.notes.length}`,
    `labels=${Object.keys(request.labels).length}`,
    `payload=${request.payload.length}`,
    `sequence=${request.sequence}`,
    `contact=${request.contact.case ?? "none"}`,
  ].join(" ");

const expectRuntimeRequest = (request: FeatureRequest | undefined) => {
  expect(request).toBeDefined();
  expect(request).toMatchObject({
    tags: ["alpha", "beta"],
    scores: [10, 20],
    notes: [{ text: "generated feature demo" }],
    state: 1,
    owner: { id: "user-1", name: "Ada" },
    labels: { env: "demo" },
    counts: { attempts: 1 },
    reviewers: { primary: { id: "reviewer-1", role: "owner" } },
    sequence: 42n,
    contact: { case: "contactUser", value: { id: "user-1", role: "owner" } },
  });
  expect(request?.createdAt?.getTime()).toBe(1_500);
  expect(Duration.toNanosUnsafe(request!.ttl!)).toBe(2_250_000_001n);
  expect(request?.payload).toEqual(new Uint8Array([1, 2, 3]));
};

const withServer = <A, E, R>(
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
  overrides?: Partial<FeatureShowcaseServiceImplementation>,
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [
          {
            group: FeatureShowcaseServiceRpcGroup,
            registry: FeatureShowcaseServiceGrpcRegistry,
            handlers: FeatureShowcaseServiceHandlersLayer({
              ...implementation,
              ...overrides,
            }),
          },
          {
            group: UserServiceRpcGroup,
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlersLayer(userImplementation),
          },
        ],
      }).pipe(Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      return yield* use(new URL(`http://127.0.0.1:${port}`));
    }),
  );

const clientLayer = (baseUrl: URL) =>
  FeatureShowcaseServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        defaultTimeoutMs: 1_000,
        registry: FeatureShowcaseServiceGrpcRegistry,
      }),
    ),
  );

const userClientLayer = (baseUrl: URL) =>
  UserServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        defaultTimeoutMs: 1_000,
        registry: UserServiceGrpcRegistry,
      }),
    ),
  );

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
