import type { Transport } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Stream } from "effect";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcNodeServer from "../src/GrpcNodeServer.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { UserServiceGrpcRegistry } from "../../../examples/simple-proto/src/generated/demo/v1/user_service_effect_grpc.js";
import { FeatureShowcaseServiceGrpcRegistry } from "../../../examples/simple-proto/src/generated/features/v1/showcase_effect_grpc.js";
import { freePort } from "./support/serverHarness.js";

/**
 * Adapter parity over a real listener. Health and generated service registries
 * supply actual protobuf descriptors and codecs; shared scenarios travel
 * HTTP/2 end to end or invoke the same handlers through the in-memory adapter.
 */
const CHECK = "grpc.health.v1.Health/Check";
const SERVING = { status: "SERVING" };

/** The entries under test, isolated from the transport's own headers. */
const underTest = (metadata: GrpcMetadata.GrpcMetadata) =>
  metadata.filter(([key]) => key.startsWith("x-parity"));

const callMetadata: GrpcMetadata.GrpcMetadata = [
  ["X-Parity-Ascii", "plain, with a comma"],
  ["x-parity-token-bin", new Uint8Array([0, 1, 250, 255])],
  ["x-parity-dup", "one"],
  ["x-parity-dup", "two"],
];

/** What `callMetadata` looks like once it has been through headers. */
const delivered: GrpcMetadata.GrpcMetadata = [
  ["x-parity-ascii", "plain, with a comma"],
  ["x-parity-dup", "one, two"],
  ["x-parity-token-bin", new Uint8Array([0, 1, 250, 255])],
];

describe("GrpcInvoker adapter parity", () => {
  // Boots a real HTTP/2 listener and waits for it in wall-clock time.
  it.live(
    "delivers identical metadata to a wire handler and an in-memory handler",
    () =>
      Effect.gen(function* () {
        const wire = yield* onTheWire((invoker) =>
          invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
        );
        const memory = yield* inMemory((invoker) =>
          invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
        );

        assert.deepStrictEqual(wire.response, SERVING);
        assert.deepStrictEqual(memory.response, SERVING);
        assert.deepStrictEqual(underTest(memory.seen!), underTest(wire.seen!));
        // Pinned explicitly, so a codec that drifts on *both* sides still fails:
        // keys lowercased and ordered, repeated ASCII keys joined, `-bin` decoded.
        assert.deepStrictEqual(underTest(wire.seen!), delivered);
      }),
  );
});

describe("GrpcInvoker (connect call options)", () => {
  // connect's own transports happen to clamp `timeoutMs <= 0` to "no
  // deadline" before `createDeadlineSignal` sees it, so the wire test above
  // cannot distinguish forwarding a zero from dropping it. A bare `Transport`
  // — which `layerConnect` accepts — can: `createDeadlineSignal` aborts a
  // `<= 0` timeout the instant the call starts.
  it.effect(
    "omits a non-positive timeoutMs instead of forwarding an expired deadline",
    () =>
      Effect.gen(function* () {
        const forwarded: Array<number | undefined> = [];
        const transport = {
          unary: (_method: unknown, _signal: unknown, timeoutMs?: number) => {
            forwarded.push(timeoutMs);
            return Promise.resolve({
              header: new Headers(),
              trailer: new Headers(),
              message: { status: 1 },
            });
          },
        } as unknown as Transport;

        yield* GrpcInvoker.GrpcInvoker.pipe(
          Effect.flatMap((invoker) =>
            Effect.all([
              invoker.unary(CHECK, { service: "" }, { timeoutMs: 0 }),
              invoker.unary(CHECK, { service: "" }, { timeoutMs: -1 }),
              invoker.unary(CHECK, { service: "" }, { timeoutMs: 5_000 }),
            ]),
          ),
          Effect.provide(
            GrpcInvoker.layerConnect({
              registry: GrpcHealth.HealthGrpcRegistry,
              transport,
            }),
          ),
        );

        assert.deepStrictEqual(forwarded, [undefined, undefined, 5_000]);
      }),
  );
});

interface Observed<A> {
  readonly response: A;
  readonly seen: GrpcMetadata.GrpcMetadata | undefined;
}

const registry = new Map([
  ...GrpcHealth.HealthGrpcRegistry,
  ...UserServiceGrpcRegistry,
  ...FeatureShowcaseServiceGrpcRegistry,
]);
type Adapter = "memory" | "wire";
// The shared scenarios only read incoming metadata, which both contexts expose.
type SharedHandlers = GrpcInvoker.GrpcInMemoryHandlers &
  Record<string, GrpcServerProtocol.GrpcHandler>;

const withAdapter = <A, E>(
  adapter: Adapter,
  handlers: SharedHandlers,
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E>,
): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      if (adapter === "memory") {
        return yield* GrpcInvoker.GrpcInvoker.pipe(
          Effect.flatMap(use),
          Effect.provide(GrpcInvoker.layerInMemory(handlers)),
        );
      }
      const { routes } = yield* GrpcServerProtocol.make({
        registry,
        handlers: new Map(Object.entries(handlers)),
      });
      const port = yield* freePort;
      yield* GrpcNodeServer.serve({ host: "127.0.0.1", port, routes }).pipe(
        Effect.forkScoped,
      );
      yield* Effect.sleep("50 millis");
      return yield* GrpcInvoker.GrpcInvoker.pipe(
        Effect.flatMap(use),
        Effect.provide(
          GrpcClientProtocol.layer({
            baseUrl: `http://127.0.0.1:${port}`,
            registry,
          }),
        ),
      );
    }),
  );

const observeMetadata = <A, E>(
  adapter: Adapter,
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E>,
): Effect.Effect<Observed<A>, E> =>
  Effect.gen(function* () {
    let seen: GrpcMetadata.GrpcMetadata | undefined;
    const response = yield* withAdapter(
      adapter,
      {
        [CHECK]: {
          kind: "unary",
          handler: (_request, context) =>
            Effect.sync(() => {
              seen = context.metadata;
              return SERVING;
            }),
        },
      },
      use,
    );
    return { response, seen };
  });
const onTheWire = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E>,
) => observeMetadata("wire", use);
const inMemory = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E>,
) => observeMetadata("memory", use);

const WATCH = "demo.v1.UserService/WatchUsers";
const CHAT = "features.v1.FeatureShowcaseService/Chat";
const GET = "demo.v1.UserService/GetUser";
const event = { id: "1", name: "n", action: "update", sequence: 1 };
const echo = { text: "echo", sequence: 1 };

for (const adapter of ["memory", "wire"] as const) {
  describe(`${adapter} streaming compatibility`, () => {
    for (const kind of ["server-streaming", "bidi-streaming"] as const) {
      const tag = kind === "server-streaming" ? WATCH : CHAT;
      const response = kind === "server-streaming" ? event : echo;
      const invoke = (
        invoker: GrpcInvoker.GrpcInvokerService,
        timeoutMs: number,
      ) =>
        kind === "server-streaming"
          ? invoker.serverStream(
              tag,
              { tenantId: "t", count: 1 },
              { timeoutMs },
            )
          : invoker.bidiStream(tag, Stream.empty, { timeoutMs });

      it.live(`bounds frequent ${kind} responses by one call deadline`, () =>
        Effect.gen(function* () {
          const finalized = yield* Deferred.make<void>();
          let releases = 0;
          let received = 0;
          yield* withAdapter(
            adapter,
            {
              [tag]: {
                kind,
                handler: () =>
                  Stream.tick(5).pipe(
                    Stream.map(() => response),
                    Stream.ensuring(
                      Effect.sync(() => {
                        releases++;
                      }).pipe(
                        Effect.andThen(Deferred.succeed(finalized, undefined)),
                      ),
                    ),
                  ),
              },
            },
            (invoker) =>
              Effect.gen(function* () {
                const error = yield* invoke(invoker, 100).pipe(
                  Stream.runForEach(() =>
                    Effect.sync(() => {
                      received++;
                    }),
                  ),
                  Effect.flip,
                );
                assert.instanceOf(error, GrpcStatusError.GrpcStatusError);
                assert.strictEqual(error.code, "deadline_exceeded");
                assert.isAbove(received, 0);
                // The wire client cannot acknowledge remote cleanup; wait for its signal.
                if (adapter === "memory") assert.strictEqual(releases, 1);
                yield* Deferred.await(finalized).pipe(
                  Effect.timeout("1 second"),
                );
                assert.strictEqual(releases, 1);
              }),
          );
        }),
      );

      it.live(
        `finalizes a paused ${kind} producer without another consumer pull`,
        () =>
          Effect.gen(function* () {
            const finalized = yield* Deferred.make<void>();
            let releases = 0;
            yield* withAdapter(
              adapter,
              {
                [tag]: {
                  kind,
                  handler: () =>
                    Stream.make(response).pipe(
                      Stream.concat(Stream.never),
                      Stream.ensuring(
                        Effect.sync(() => {
                          releases++;
                        }).pipe(
                          Effect.andThen(
                            Deferred.succeed(finalized, undefined),
                          ),
                        ),
                      ),
                    ),
                },
              },
              (invoker) =>
                Effect.gen(function* () {
                  const pull = yield* Stream.toPull(invoke(invoker, 100));
                  assert.lengthOf(yield* pull, 1);
                  // Intentionally stop pulling: the producer must still be finalized.
                  yield* Deferred.await(finalized).pipe(
                    Effect.timeout("1 second"),
                  );
                  assert.strictEqual(releases, 1);
                  const error = yield* Effect.flip(pull);
                  assert.instanceOf(error, GrpcStatusError.GrpcStatusError);
                  assert.strictEqual(
                    (error as GrpcStatusError.GrpcStatusError).code,
                    "deadline_exceeded",
                  );
                }).pipe(Effect.scoped),
            );
          }),
      );
    }

    it.live(
      "awaits local bidi request cleanup when the response consumer stops early",
      () =>
        Effect.gen(function* () {
          let sourceReleases = 0;
          const finalized = yield* Deferred.make<void>();
          yield* withAdapter(
            adapter,
            {
              [CHAT]: {
                kind: "bidi-streaming",
                handler: (requests) =>
                  requests.pipe(
                    Stream.ensuring(Deferred.succeed(finalized, undefined)),
                  ),
              },
            },
            (invoker) =>
              Effect.gen(function* () {
                const requests = Stream.make(echo).pipe(
                  Stream.concat(Stream.never),
                  Stream.ensuring(
                    Effect.sleep(20).pipe(
                      Effect.andThen(
                        Effect.sync(() => {
                          sourceReleases++;
                        }),
                      ),
                    ),
                  ),
                );
                const responses = yield* invoker
                  .bidiStream(CHAT, requests)
                  .pipe(Stream.take(1), Stream.runCollect);
                assert.deepStrictEqual(responses, [echo]);
                assert.strictEqual(sourceReleases, 1);
                yield* Deferred.await(finalized).pipe(
                  Effect.timeout("1 second"),
                );
                assert.strictEqual(sourceReleases, 1);
              }),
          );
        }),
    );

    it.live(
      "preserves a captured bidi request error while the handler recovers until deadline",
      () =>
        Effect.gen(function* () {
          const original = new Error("request source failed");
          const started = yield* Deferred.make<void>();
          const finalized = yield* Deferred.make<void>();
          let sourceReleases = 0;
          yield* withAdapter(
            adapter,
            {
              [CHAT]: {
                kind: "bidi-streaming",
                handler: (requests) =>
                  Stream.fromEffect(Deferred.succeed(started, undefined)).pipe(
                    Stream.drain,
                    Stream.concat(requests),
                    Stream.catch(() => Stream.never),
                    Stream.ensuring(Deferred.succeed(finalized, undefined)),
                  ),
              },
            },
            (invoker) =>
              Effect.gen(function* () {
                // Start the wire handler before failing; otherwise a request
                // failure could cancel before the server owns any resources.
                const requests = Stream.make(echo).pipe(
                  Stream.concat(
                    Stream.fromEffect(Deferred.await(started)).pipe(
                      Stream.drain,
                    ),
                  ),
                  Stream.concat(Stream.fail(original)),
                  Stream.ensuring(
                    Effect.sync(() => {
                      sourceReleases++;
                    }),
                  ),
                );
                const error = yield* invoker
                  .bidiStream(CHAT, requests, { timeoutMs: 100 })
                  .pipe(Stream.runDrain, Effect.flip);
                assert.strictEqual(error, original);
                assert.strictEqual(sourceReleases, 1);
                yield* Deferred.await(finalized).pipe(
                  Effect.timeout("1 second"),
                );
              }),
          );
        }),
    );

    it.live(
      "isolates request metadata for calls that overlap inside the handler",
      () =>
        Effect.gen(function* () {
          const arrived = yield* Deferred.make<void>();
          let calls = 0;
          yield* withAdapter(
            adapter,
            {
              [GET]: {
                kind: "unary",
                handler: (_request, context) =>
                  Effect.gen(function* () {
                    if (++calls === 2)
                      yield* Deferred.succeed(arrived, undefined);
                    yield* Deferred.await(arrived).pipe(
                      Effect.timeout("1 second"),
                      Effect.orDie,
                    );
                    const id = context.metadata.find(
                      ([key]) => key === "x-parity-id",
                    )?.[1];
                    return { user: { id: "1", name: String(id) } };
                  }),
              },
            },
            (invoker) =>
              Effect.gen(function* () {
                const values = yield* Effect.all(
                  ["first", "second"].map((id) =>
                    invoker.unary(
                      GET,
                      { id: "1" },
                      { metadata: [["x-parity-id", id]] },
                    ),
                  ),
                  { concurrency: "unbounded" },
                );
                assert.deepStrictEqual(
                  values,
                  ["first", "second"].map((name) => ({
                    user: { id: "1", name },
                  })),
                );
              }),
          );
        }),
    );
  });
}
