import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { assert, describe, it } from "@effect/vitest";
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

import { GrpcClientProtocol, GrpcStatusError } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlers,
  type FeatureRequest,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";
import { FeatureShowcaseService } from "@effect-grpc/simple-proto/generated/features/v1/showcase_pb";

import { withServer as serve } from "./support.ts";

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
  it.live(
    "round-trips the supported feature matrix through the Effect client",
    () =>
      Effect.gen(function* () {
        const response = yield* withServer((baseUrl) =>
          Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            return yield* client.describe(featureRequest());
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        );

        assert.strictEqual(
          response.summary,
          "owner=Ada tags=2 notes=1 labels=1 payload=3 sequence=42 contact=contactUser",
        );
        assertRuntimeRequest(response.request);
      }),
  );

  it.live("serves native gRPC calls from a non-Effect connect client", () =>
    Effect.gen(function* () {
      const response = yield* withServer((baseUrl) =>
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
      );

      assert.strictEqual(
        response.summary,
        "owner=Grace tags=1 notes=1 labels=1 payload=3 sequence=99 contact=contactEmail",
      );
      assert.strictEqual(response.request?.state, 99);
      assert.deepInclude(response.request?.createdAt, {
        seconds: 1n,
        nanos: 500_000_000,
      });
      assert.deepInclude(response.request?.ttl, {
        seconds: 2n,
        nanos: 250_000_001,
      });
      assert.deepStrictEqual(
        response.request?.payload,
        new Uint8Array([4, 5, 6]),
      );
      assert.strictEqual(response.request?.sequence, 99n);
      assert.deepStrictEqual(response.request?.contact, {
        case: "contactEmail",
        value: "grace@example.com",
      });
    }),
  );

  it.live("routes requests to generated services after the first service", () =>
    Effect.gen(function* () {
      const response = yield* withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "secondary" });
        }).pipe(Effect.provide(userClientLayer(baseUrl))),
      );

      assert.deepStrictEqual(response.user, {
        id: "secondary",
        name: "Secondary User",
      });
    }),
  );

  it.live(
    "round-trips a client-streaming upload through the Effect client",
    () =>
      Effect.gen(function* () {
        const uploaded = yield* withServer((baseUrl) =>
          Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            return yield* client.uploadNotes(
              Stream.make(
                { text: "alpha" },
                { text: "beta" },
                { text: "gamma" },
              ),
            );
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        );

        assert.deepStrictEqual(uploaded, {
          count: 3,
          joined: "alpha,beta,gamma",
        });
      }),
  );

  it.live("round-trips a bidi chat through the Effect client", () =>
    Effect.gen(function* () {
      const echoes = yield* withServer((baseUrl) =>
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
      );

      assert.deepStrictEqual(echoes, [
        { text: "echo:hi", sequence: 2 },
        { text: "echo:there", sequence: 3 },
      ]);
    }),
  );

  it.live(
    "propagates a mid-stream server failure to the client-streaming caller",
    () =>
      Effect.gen(function* () {
        const error = yield* withServer((baseUrl) =>
          Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            return yield* client
              .uploadNotes(
                Stream.make({ text: "ok" }, { text: "boom" }, { text: "late" }),
              )
              .pipe(Effect.flip);
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        );

        assert.deepInclude(error, {
          _tag: "GrpcStatusError",
          code: "failed_precondition",
          message: "boom note",
        });
      }),
  );

  it.live(
    "fails the bidi response stream when the server fails mid-stream",
    () =>
      Effect.gen(function* () {
        const result = yield* withServer((baseUrl) =>
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
        );

        assert.deepStrictEqual(result.collected, [
          { text: "echo:hi", sequence: 2 },
        ]);
        assert.deepInclude(result.error, {
          _tag: "GrpcStatusError",
          code: "failed_precondition",
          message: "boom message",
        });
      }),
  );

  it.live(
    "cancels the call and surfaces the original error when the request stream fails",
    () =>
      Effect.gen(function* () {
        const failure = { _tag: "UploadSourceFailure" as const };
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

        assert.strictEqual(exit._tag, "Failure");
        if (exit._tag === "Failure") {
          assert.strictEqual(Cause.squash(exit.cause), failure);
        }
        // The server observes the cancellation either as a failed request stream
        // or as an interruption of the handler fiber.
        assert.strictEqual(serverExit._tag, "Failure");
        if (serverExit._tag === "Failure") {
          const error = Cause.findErrorOption(serverExit.cause);
          assert.isTrue(
            Cause.hasInterrupts(serverExit.cause) ||
              (Option.isSome(error) && error.value.code === "cancelled"),
          );
        }
      }),
  );

  it.live("stops the server handler when the bidi consumer stops early", () =>
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
      const echoes = yield* withServer(
        (baseUrl) =>
          Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            const echoes = yield* client
              .chat(Stream.forever(Stream.make({ text: "ping", sequence: 1 })))
              .pipe(Stream.take(2), Stream.runCollect);
            // The handler must terminate through cancellation while the
            // server is still running.
            yield* Deferred.await(finished);
            return echoes;
          }).pipe(Effect.provide(clientLayer(baseUrl))),
        { chat },
      );

      assert.deepStrictEqual(echoes, [
        { text: "echo:ping", sequence: 2 },
        { text: "echo:ping", sequence: 2 },
      ]);
    }),
  );

  it.live("serves native connect streaming clients", () =>
    Effect.gen(function* () {
      const result = yield* withServer((baseUrl) =>
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
      );

      assert.deepStrictEqual(result.uploaded, {
        count: 2,
        joined: "native,grpc",
      });
      assert.deepStrictEqual(result.echoes, [
        { text: "echo:hello", sequence: 42 },
      ]);
    }),
  );
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

const assertRuntimeRequest = (request: FeatureRequest | undefined) => {
  assert.isDefined(request);
  assert.deepInclude(request, {
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
  assert.strictEqual(request?.createdAt?.getTime(), 1_500);
  assert.strictEqual(Duration.toNanosUnsafe(request!.ttl!), 2_250_000_001n);
  assert.deepStrictEqual(request?.payload, new Uint8Array([1, 2, 3]));
};

const withServer = <A, E, R>(
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
  overrides?: Partial<FeatureShowcaseServiceImplementation>,
): Effect.Effect<A, E, R> =>
  serve(
    {
      services: [
        {
          registry: FeatureShowcaseServiceGrpcRegistry,
          handlers: FeatureShowcaseServiceHandlers({
            ...implementation,
            ...overrides,
          }),
        },
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers(userImplementation),
        },
      ],
    },
    use,
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
