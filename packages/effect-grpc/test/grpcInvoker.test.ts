import type { Transport } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import {
  Channel,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Metric,
  Ref,
  Scope,
  Stream,
} from "effect";
import { TestClock } from "effect/testing";
import * as Tracer from "effect/Tracer";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { methodEntries } from "./support/serverHarness.js";

const withInvoker = <A, E>(
  handlers: GrpcInvoker.GrpcInMemoryHandlers,
  body: (
    invoker: GrpcInvoker.GrpcInvokerService,
  ) => Effect.Effect<A, E, GrpcInvoker.GrpcInvoker>,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    const invoker = yield* GrpcInvoker.GrpcInvoker;
    return yield* body(invoker);
  }).pipe(Effect.provide(GrpcInvoker.layerInMemory(handlers)));

describe("GrpcInvoker (in-memory adapter)", () => {
  it.effect("round trips all four call shapes with domain values", () =>
    Effect.gen(function* () {
      const result = yield* withInvoker(
        {
          "test.Svc/Unary": {
            kind: "unary",
            handler: (request) => Effect.succeed({ echoed: request }),
          },
          "test.Svc/ServerStream": {
            kind: "server-streaming",
            handler: (request) => Stream.make(request, request),
          },
          "test.Svc/ClientStream": {
            kind: "client-streaming",
            handler: (requests) =>
              Stream.runCollect(requests).pipe(
                Effect.map((values) => ({ count: values.length })),
              ),
          },
          "test.Svc/BidiStream": {
            kind: "bidi-streaming",
            handler: (requests) =>
              Stream.map(requests, (value) => ({ doubled: value })),
          },
        },
        (invoker) =>
          Effect.gen(function* () {
            const unary = yield* invoker.unary("test.Svc/Unary", "hi");
            const server = yield* Stream.runCollect(
              invoker.serverStream("test.Svc/ServerStream", "s"),
            );
            const client = yield* invoker.clientStream(
              "test.Svc/ClientStream",
              Stream.make(1, 2, 3),
            );
            const bidi = yield* Stream.runCollect(
              invoker.bidiStream("test.Svc/BidiStream", Stream.make(1, 2)),
            );
            return { unary, server, client, bidi };
          }),
      );

      assert.deepStrictEqual(result, {
        unary: { echoed: "hi" },
        server: ["s", "s"],
        client: { count: 3 },
        bidi: [{ doubled: 1 }, { doubled: 2 }],
      });
    }),
  );

  it.effect(
    "fails with a stable unimplemented status for unknown tags and kind mismatches",
    () =>
      Effect.gen(function* () {
        const codes = yield* withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: (request) => Effect.succeed(request),
            },
          },
          (invoker) =>
            Effect.gen(function* () {
              const unknown = yield* Effect.flip(
                invoker.unary("test.Svc/Missing", {}),
              );
              const mismatch = yield* Effect.flip(
                invoker.clientStream("test.Svc/Unary", Stream.empty),
              );
              const unknownStream = yield* Effect.flip(
                Stream.runCollect(invoker.serverStream("test.Svc/Missing", {})),
              );
              const unknownBidi = yield* Effect.flip(
                Stream.runCollect(
                  invoker.bidiStream("test.Svc/Missing", Stream.empty),
                ),
              );
              return [unknown, mismatch, unknownStream, unknownBidi];
            }),
        );

        for (const error of codes) {
          assert.instanceOf(error, GrpcStatusError.GrpcStatusError);
          assert.strictEqual(
            (error as GrpcStatusError.GrpcStatusError).code,
            "unimplemented",
          );
          assert.include(
            (error as GrpcStatusError.GrpcStatusError).message,
            "Unknown gRPC RPC tag",
          );
        }
      }),
  );

  it.effect("exposes normalized metadata and timeout on the call context", () =>
    Effect.gen(function* () {
      let seen: GrpcInvoker.GrpcInMemoryCall | undefined;
      yield* withInvoker(
        {
          "test.Svc/Unary": {
            kind: "unary",
            handler: (request, call) =>
              Effect.sync(() => {
                seen = call;
                return request;
              }),
          },
        },
        (invoker) =>
          invoker.unary("test.Svc/Unary", "x", {
            metadata: [["x-test", "42"]],
            timeoutMs: 5000,
          }),
      );

      assert.deepStrictEqual(seen, {
        tag: "test.Svc/Unary",
        metadata: [["x-test", "42"]],
        timeoutMs: 5000,
      });
    }),
  );

  it.effect("bounds all four call shapes with deadline_exceeded", () =>
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(
        withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: () => Effect.never,
            },
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: () => Stream.never,
            },
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: () => Effect.never,
            },
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: () => Stream.never,
            },
          },
          (invoker) =>
            Effect.gen(function* () {
              const unary = yield* Effect.flip(
                invoker.unary("test.Svc/Unary", {}, { timeoutMs: 20 }),
              );
              const server = yield* Effect.flip(
                Stream.runCollect(
                  invoker.serverStream(
                    "test.Svc/ServerStream",
                    {},
                    {
                      timeoutMs: 20,
                    },
                  ),
                ),
              );
              const client = yield* Effect.flip(
                invoker.clientStream("test.Svc/ClientStream", Stream.empty, {
                  timeoutMs: 20,
                }),
              );
              const bidi = yield* Effect.flip(
                Stream.runCollect(
                  invoker.bidiStream("test.Svc/BidiStream", Stream.empty, {
                    timeoutMs: 20,
                  }),
                ),
              );
              return [unary, server, client, bidi];
            }),
        ),
      );
      // Four sequential 20ms calls each consume their own deadline or wait.
      yield* TestClock.adjust(80);
      const errors = yield* Fiber.join(fiber);
      assert.lengthOf(errors, 4);
      for (const error of errors) {
        assert.instanceOf(error, GrpcStatusError.GrpcStatusError);
        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).message,
          "RPC deadline exceeded",
        );
      }
    }),
  );
  // A gRPC deadline is `call start + timeoutMs`, not an inactivity window: a
  // stream that keeps emitting faster than the deadline must still be cut off
  // at the deadline. `Stream.timeout` resets per pull and would never fire.
  it.live(
    "measures a streaming deadline from call start, not from the last item",
    () =>
      Effect.gen(function* () {
        let received = 0;
        const error = yield* withInvoker(
          {
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: () => Stream.tick(5),
            },
          },
          (invoker) =>
            Effect.flip(
              Stream.runForEach(
                invoker.serverStream(
                  "test.Svc/ServerStream",
                  {},
                  {
                    timeoutMs: 60,
                  },
                ),
                () =>
                  Effect.sync(() => {
                    received += 1;
                  }),
              ),
            ),
        );
        assert.isAbove(received, 0);
        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
      }),
  );
  it.live(
    "finalizes a server-streaming handler when the deadline expires",
    () =>
      Effect.gen(function* () {
        let finalized = 0;
        const error = yield* withInvoker(
          {
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: () =>
                Stream.never.pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      finalized += 1;
                    }),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.flip(
              Stream.runCollect(
                invoker.serverStream(
                  "test.Svc/ServerStream",
                  {},
                  {
                    timeoutMs: 20,
                  },
                ),
              ),
            ),
        );
        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
        assert.strictEqual(finalized, 1);
      }),
  );
  // A bidi call has two live stream lifetimes; the deadline must end both.
  it.live(
    "finalizes both the request and response streams when a bidi deadline expires",
    () =>
      Effect.gen(function* () {
        let requestsFinalized = 0;
        let responsesFinalized = 0;
        const error = yield* withInvoker(
          {
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) =>
                requests.pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      responsesFinalized += 1;
                    }),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.flip(
              Stream.runCollect(
                invoker.bidiStream(
                  "test.Svc/BidiStream",
                  Stream.never.pipe(
                    Stream.ensuring(
                      Effect.sync(() => {
                        requestsFinalized += 1;
                      }),
                    ),
                  ),
                  { timeoutMs: 20 },
                ),
              ),
            ),
        );
        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
        assert.strictEqual(requestsFinalized, 1);
        assert.strictEqual(responsesFinalized, 1);
      }),
  );
  // A pull-based consumer may hold the stream open without pulling — e.g.
  // while it processes the previous response. The deadline must still tear the
  // producer down at expiry, not at the consumer's next pull.
  it.live(
    "interrupts a server-streaming producer at the deadline while the consumer is not pulling",
    () =>
      Effect.gen(function* () {
        let finalized = 0;
        const result = yield* withInvoker(
          {
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: () =>
                Stream.make("first").pipe(
                  Stream.concat(Stream.never),
                  Stream.ensuring(
                    Effect.sync(() => {
                      finalized += 1;
                    }),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.scoped(
              Effect.gen(function* () {
                const pull = yield* Stream.toPull(
                  invoker.serverStream(
                    "test.Svc/ServerStream",
                    {},
                    {
                      timeoutMs: 30,
                    },
                  ),
                );
                const first = yield* pull;
                yield* Effect.sleep(100);
                const finalizedBeforeNextPull = finalized;
                const error = yield* Effect.flip(pull);
                return { first, finalizedBeforeNextPull, error };
              }),
            ),
        );
        assert.deepStrictEqual(result.first, ["first"]);
        assert.strictEqual(result.finalizedBeforeNextPull, 1);
        assert.strictEqual(
          (result.error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
        assert.strictEqual(finalized, 1);
      }),
  );
  it.live(
    "interrupts both bidi streams at the deadline while the consumer is not pulling",
    () =>
      Effect.gen(function* () {
        let requestsFinalized = 0;
        let responsesFinalized = 0;
        const result = yield* withInvoker(
          {
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) =>
                requests.pipe(
                  Stream.ensuring(
                    Effect.sync(() => {
                      responsesFinalized += 1;
                    }),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.scoped(
              Effect.gen(function* () {
                const pull = yield* Stream.toPull(
                  invoker.bidiStream(
                    "test.Svc/BidiStream",
                    Stream.make(1).pipe(
                      Stream.concat(Stream.never),
                      Stream.ensuring(
                        Effect.sync(() => {
                          requestsFinalized += 1;
                        }),
                      ),
                    ),
                    { timeoutMs: 30 },
                  ),
                );
                const first = yield* pull;
                yield* Effect.sleep(100);
                const finalizedBeforeNextPull = [
                  requestsFinalized,
                  responsesFinalized,
                ];
                const error = yield* Effect.flip(pull);
                return { first, finalizedBeforeNextPull, error };
              }),
            ),
        );
        assert.deepStrictEqual(result.first, [1]);
        assert.deepStrictEqual(result.finalizedBeforeNextPull, [1, 1]);
        assert.strictEqual(
          (result.error as GrpcStatusError.GrpcStatusError).code,
          "deadline_exceeded",
        );
        assert.deepStrictEqual([requestsFinalized, responsesFinalized], [1, 1]);
      }),
  );
  for (const shape of ["server-streaming", "bidi-streaming"] as const) {
    for (const phase of [
      "pull",
      "delayed setup",
      "never-ending setup",
    ] as const) {
      it.live(
        `awaits asynchronous ${shape} cleanup during ${phase} before returning the deadline failure`,
        () =>
          Effect.gen(function* () {
            let cleanupStarted = 0;
            let cleanupCompleted = 0;
            const handler = () =>
              Stream.fromChannel(
                Channel.fromTransform((_upstream, scope) =>
                  Effect.gen(function* () {
                    yield* Scope.addFinalizer(
                      scope,
                      Effect.sync(() => {
                        cleanupStarted += 1;
                      }).pipe(
                        Effect.andThen(Effect.sleep(80)),
                        Effect.andThen(
                          Effect.sync(() => {
                            cleanupCompleted += 1;
                          }),
                        ),
                      ),
                    );
                    if (phase === "delayed setup") yield* Effect.sleep(200);
                    if (phase === "never-ending setup") yield* Effect.never;
                    return Effect.never;
                  }),
                ),
              );
            const error = yield* withInvoker(
              { "test.Svc/Stream": { kind: shape, handler } },
              (invoker) =>
                Effect.flip(
                  Stream.runDrain(
                    shape === "server-streaming"
                      ? invoker.serverStream(
                          "test.Svc/Stream",
                          {},
                          { timeoutMs: 20 },
                        )
                      : invoker.bidiStream("test.Svc/Stream", Stream.empty, {
                          timeoutMs: 20,
                        }),
                  ),
                ),
            );
            assert.strictEqual(
              (error as GrpcStatusError.GrpcStatusError).code,
              "deadline_exceeded",
            );
            assert.strictEqual(cleanupStarted, 1);
            assert.strictEqual(cleanupCompleted, 1);
          }),
      );
    }
  }
  // Once the caller's request stream has failed, that failure owns the call —
  // as for client-streaming, a deadline that expires while the handler is
  // still recovering from the resulting `cancelled` must not displace it.
  it.live(
    "restores a captured request-stream error when the bidi deadline expires during recovery",
    () =>
      Effect.gen(function* () {
        const boom = new Error("source boom");
        const error = yield* withInvoker(
          {
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) =>
                requests.pipe(
                  Stream.catch(() =>
                    Stream.fromEffect(Effect.sleep(1000)).pipe(Stream.drain),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.flip(
              Stream.runCollect(
                invoker.bidiStream("test.Svc/BidiStream", Stream.fail(boom), {
                  timeoutMs: 30,
                }),
              ),
            ),
        );
        assert.strictEqual(error, boom);
      }),
  );
  // A stream's setup (`Stream.fromPull`'s effect, the acquisition behind
  // `Stream.toPull`) can itself be slow or block forever. The deadline is
  // measured from call start, so it must cover setup as well as pulls: a
  // handler whose setup outlives the deadline emits nothing, and the setup
  // work it started is interrupted rather than left to finish.
  describe("bounds stream setup by the deadline", () => {
    // Independent of the deadline under test, so a regression that leaves
    // setup unbounded fails the test instead of hanging the suite.
    const watchdog = <A, E>(effect: Effect.Effect<A, E>) =>
      Effect.timeoutOrElse(effect, {
        duration: 2000,
        orElse: () =>
          Effect.die(new Error("test watchdog: call never expired")),
      });
    const setups = [
      ["delayed", () => Effect.sleep(200)],
      ["never-completing", () => Effect.never],
    ] as const;
    for (const [label, setup] of setups) {
      it.live(
        `fails a server-streaming call with ${label} setup with deadline_exceeded`,
        () =>
          Effect.gen(function* () {
            let interrupted = 0;
            const error = yield* withInvoker(
              {
                "test.Svc/ServerStream": {
                  kind: "server-streaming",
                  handler: () =>
                    Stream.fromPull(
                      setup().pipe(
                        Effect.onInterrupt(() =>
                          Effect.sync(() => {
                            interrupted += 1;
                          }),
                        ),
                        Effect.as(Effect.succeed(["late"] as const)),
                      ),
                    ),
                },
              },
              (invoker) =>
                watchdog(
                  Effect.flip(
                    Stream.runCollect(
                      invoker
                        .serverStream(
                          "test.Svc/ServerStream",
                          {},
                          { timeoutMs: 20 },
                        )
                        .pipe(Stream.take(1)),
                    ),
                  ),
                ),
            );
            assert.strictEqual(
              (error as GrpcStatusError.GrpcStatusError).code,
              "deadline_exceeded",
            );
            assert.strictEqual(interrupted, 1);
          }),
      );
      it.live(
        `fails a bidi-streaming call with ${label} setup with deadline_exceeded`,
        () =>
          Effect.gen(function* () {
            let interrupted = 0;
            let requestsFinalized = 0;
            const error = yield* withInvoker(
              {
                "test.Svc/BidiStream": {
                  kind: "bidi-streaming",
                  handler: () =>
                    Stream.fromPull(
                      setup().pipe(
                        Effect.onInterrupt(() =>
                          Effect.sync(() => {
                            interrupted += 1;
                          }),
                        ),
                        Effect.as(Effect.succeed(["late"] as const)),
                      ),
                    ),
                },
              },
              (invoker) =>
                watchdog(
                  Effect.flip(
                    Stream.runCollect(
                      invoker
                        .bidiStream(
                          "test.Svc/BidiStream",
                          Stream.make(1).pipe(
                            Stream.concat(Stream.never),
                            Stream.ensuring(
                              Effect.sync(() => {
                                requestsFinalized += 1;
                              }),
                            ),
                          ),
                          { timeoutMs: 20 },
                        )
                        .pipe(Stream.take(1)),
                    ),
                  ),
                ),
            );
            assert.strictEqual(
              (error as GrpcStatusError.GrpcStatusError).code,
              "deadline_exceeded",
            );
            assert.strictEqual(interrupted, 1);
            // The handler never pulled the request stream, so nothing there was
            // ever started; the deadline must not leave it dangling either.
            assert.strictEqual(requestsFinalized, 0);
          }),
      );
    }
  });
  // The deadline sits inside source replay, so a request stream failure that
  // lands before the deadline still reaches the caller as its own error,
  // never as `deadline_exceeded`.
  it.live(
    "keeps the caller's request-stream error ahead of a pending bidi deadline",
    () =>
      Effect.gen(function* () {
        const boom = new Error("source boom");
        const error = yield* withInvoker(
          {
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) => requests,
            },
          },
          (invoker) =>
            Effect.flip(
              Stream.runCollect(
                invoker.bidiStream(
                  "test.Svc/BidiStream",
                  Stream.fromEffect(Effect.sleep(5)).pipe(
                    Stream.concat(Stream.fail(boom)),
                  ),
                  { timeoutMs: 1000 },
                ),
              ),
            ),
        );
        assert.strictEqual(error, boom);
      }),
  );
  // `timeoutMs <= 0` uniformly means *no deadline* — the connect adapter drops
  // the option (see `invokerParity.test.ts`), so the in-memory one must neither
  // enforce it nor put it on the call context, or a zero would turn every call
  // into an instant `deadline_exceeded`.
  it.effect("treats a non-positive timeoutMs as no deadline", () =>
    Effect.gen(function* () {
      const seen: Array<GrpcInvoker.GrpcInMemoryCall> = [];
      const observe = (call: GrpcInvoker.GrpcInMemoryCall) =>
        Effect.sync(() => {
          seen.push(call);
        });
      const fiber = yield* Effect.forkChild(
        withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: (request, call) =>
                observe(call).pipe(
                  Effect.andThen(Effect.sleep(20)),
                  Effect.as(request),
                ),
            },
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: (request, call) =>
                Stream.fromEffect(observe(call)).pipe(
                  Stream.drain,
                  Stream.concat(
                    Stream.fromEffect(
                      Effect.sleep(20).pipe(Effect.as(request)),
                    ),
                  ),
                ),
            },
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: (_requests, call) =>
                observe(call).pipe(
                  Effect.andThen(Effect.sleep(20)),
                  Effect.as("client response"),
                ),
            },
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (_requests, call) =>
                Stream.fromEffect(observe(call)).pipe(
                  Stream.drain,
                  Stream.concat(
                    Stream.fromEffect(
                      Effect.sleep(20).pipe(Effect.as("bidi response")),
                    ),
                  ),
                ),
            },
          },
          (invoker) =>
            Effect.all([
              invoker.unary("test.Svc/Unary", "unary response", {
                timeoutMs: 0,
              }),
              Stream.runCollect(
                invoker.serverStream(
                  "test.Svc/ServerStream",
                  "server response",
                  {
                    timeoutMs: 0,
                  },
                ),
              ),
              invoker.clientStream("test.Svc/ClientStream", Stream.empty, {
                timeoutMs: 0,
              }),
              Stream.runCollect(
                invoker.bidiStream("test.Svc/BidiStream", Stream.empty, {
                  timeoutMs: 0,
                }),
              ),
            ]),
        ),
      );
      // Four sequential 20ms calls each consume their own deadline or wait.
      yield* TestClock.adjust(80);
      const result = yield* Fiber.join(fiber);
      assert.deepStrictEqual(result, [
        "unary response",
        ["server response"],
        "client response",
        ["bidi response"],
      ]);
      assert.deepStrictEqual(seen, [
        { tag: "test.Svc/Unary", metadata: [] },
        { tag: "test.Svc/ServerStream", metadata: [] },
        { tag: "test.Svc/ClientStream", metadata: [] },
        { tag: "test.Svc/BidiStream", metadata: [] },
      ]);
    }),
  );
  it.effect("terminates the handler when the caller interrupts", () =>
    Effect.gen(function* () {
      const observed = yield* withInvoker(
        {
          "test.Svc/Unary": {
            kind: "unary",
            handler: () => Effect.never,
          },
        },
        (invoker) =>
          Effect.gen(function* () {
            const interrupted = yield* Deferred.make<boolean>();
            const call = invoker
              .unary("test.Svc/Unary", {})
              .pipe(
                Effect.onInterrupt(() => Deferred.succeed(interrupted, true)),
              );
            const fiber = yield* Effect.forkChild(call);
            yield* Effect.yieldNow;
            yield* Fiber.interrupt(fiber);
            return yield* Deferred.await(interrupted);
          }),
      );

      assert.isTrue(observed);
    }),
  );

  it.effect(
    "replays the caller's original error while the handler observes cancelled",
    () =>
      Effect.gen(function* () {
        const boom = new Error("source boom");
        let handlerSaw: GrpcStatusError.GrpcStatusError | undefined;

        const result = yield* withInvoker(
          {
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: (requests) =>
                Stream.runCollect(requests).pipe(
                  Effect.asVoid,
                  Effect.catch((error) => {
                    handlerSaw = error;
                    return Effect.fail(error);
                  }),
                ),
            },
          },
          (invoker) =>
            Effect.flip(
              invoker.clientStream(
                "test.Svc/ClientStream",
                Stream.make(1).pipe(Stream.concat(Stream.fail(boom))),
              ),
            ),
        );

        assert.strictEqual(result, boom);
        assert.strictEqual(handlerSaw?.code, "cancelled");
      }),
  );

  it.effect(
    "replays the caller's original error when streaming handlers recover",
    () =>
      Effect.gen(function* () {
        const boom = new Error("source boom");
        const failures = yield* withInvoker(
          {
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: (requests) =>
                Stream.runDrain(requests).pipe(
                  Effect.catch(() => Effect.succeed("recovered")),
                ),
            },
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) =>
                requests.pipe(Stream.catch(() => Stream.make("recovered"))),
            },
          },
          (invoker) =>
            Effect.all([
              Effect.flip(
                invoker.clientStream(
                  "test.Svc/ClientStream",
                  Stream.fail(boom),
                ),
              ),
              Effect.flip(
                Stream.runCollect(
                  invoker.bidiStream("test.Svc/BidiStream", Stream.fail(boom)),
                ),
              ),
            ]),
        );

        assert.deepStrictEqual(failures, [boom, boom]);
      }),
  );

  it.effect(
    "rejects reserved metadata keys with invalid_argument on every shape",
    () =>
      Effect.gen(function* () {
        const reserved = { metadata: [["x-effect-grpc-foo", "bar"]] as const };
        const codes = yield* withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: (request) => Effect.succeed(request),
            },
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: (request) => Stream.make(request),
            },
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: () => Effect.succeed("ok"),
            },
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: (requests) => requests,
            },
          },
          (invoker) =>
            Effect.gen(function* () {
              const unary = yield* Effect.flip(
                invoker.unary("test.Svc/Unary", {}, reserved),
              );
              const server = yield* Effect.flip(
                Stream.runCollect(
                  invoker.serverStream("test.Svc/ServerStream", {}, reserved),
                ),
              );
              const client = yield* Effect.flip(
                invoker.clientStream(
                  "test.Svc/ClientStream",
                  Stream.empty,
                  reserved,
                ),
              );
              const bidi = yield* Effect.flip(
                Stream.runCollect(
                  invoker.bidiStream(
                    "test.Svc/BidiStream",
                    Stream.empty,
                    reserved,
                  ),
                ),
              );
              return [unary, server, client, bidi].map(
                (error) => (error as GrpcStatusError.GrpcStatusError).code,
              );
            }),
        );

        assert.deepStrictEqual(codes, [
          "invalid_argument",
          "invalid_argument",
          "invalid_argument",
          "invalid_argument",
        ]);
      }),
  );

  it.effect(
    "constructs unary and server-streaming handlers lazily after metadata validation",
    () =>
      Effect.gen(function* () {
        let unaryCalls = 0;
        let serverCalls = 0;
        const reserved = { metadata: [["x-effect-grpc-foo", "bar"]] as const };

        const result = yield* withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: () => {
                unaryCalls += 1;
                return Effect.succeed("unary");
              },
            },
            "test.Svc/ServerStream": {
              kind: "server-streaming",
              handler: () => {
                serverCalls += 1;
                return Stream.make("server");
              },
            },
          },
          (invoker) => {
            const unary = invoker.unary("test.Svc/Unary", {}, reserved);
            const server = invoker.serverStream(
              "test.Svc/ServerStream",
              {},
              reserved,
            );
            const afterConstruction = [unaryCalls, serverCalls];

            return Effect.gen(function* () {
              yield* Effect.flip(unary);
              yield* Effect.flip(Stream.runCollect(server));
              return {
                afterConstruction,
                afterValidation: [unaryCalls, serverCalls],
              };
            });
          },
        );

        assert.deepStrictEqual(result, {
          afterConstruction: [0, 0],
          afterValidation: [0, 0],
        });
      }),
  );

  it.effect("keeps source-failure capture execution-local across re-runs", () =>
    Effect.gen(function* () {
      const boom = new Error("first-run boom");
      const runs = yield* Ref.make(0);

      // One effect run twice: the request stream fails only on the first run.
      // A shared (non-execution-local) failure capture would poison the second.
      const call = withInvoker(
        {
          "test.Svc/ClientStream": {
            kind: "client-streaming",
            handler: (requests) =>
              Stream.runDrain(requests).pipe(Effect.as("done")),
          },
        },
        (invoker) =>
          invoker.clientStream(
            "test.Svc/ClientStream",
            Stream.unwrap(
              Ref.updateAndGet(runs, (n) => n + 1).pipe(
                Effect.map((n) => (n === 1 ? Stream.fail(boom) : Stream.empty)),
              ),
            ),
          ),
      );

      const first = yield* Effect.flip(call);
      const second = yield* call;

      assert.strictEqual(first, boom);
      assert.strictEqual(second, "done");
    }),
  );

  it.effect(
    "finalizes the handler stream when a bidi consumer stops early",
    () =>
      Effect.gen(function* () {
        let finalized = 0;
        const first = yield* withInvoker(
          {
            "test.Svc/BidiStream": {
              kind: "bidi-streaming",
              handler: () =>
                Stream.make("a").pipe(
                  Stream.concat(Stream.never),
                  Stream.ensuring(
                    Effect.sync(() => {
                      finalized += 1;
                    }),
                  ),
                ),
            },
          },
          (invoker) =>
            Stream.runCollect(
              invoker
                .bidiStream("test.Svc/BidiStream", Stream.empty)
                .pipe(Stream.take(1)),
            ),
        );

        assert.deepStrictEqual(first, ["a"]);
        assert.strictEqual(finalized, 1);
      }),
  );
});

describe("GrpcInvoker (connect adapter)", () => {
  it.effect(
    "fails with a stable unimplemented status for unknown tags on all shapes",
    () =>
      Effect.gen(function* () {
        const codes = yield* Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          return yield* Effect.gen(function* () {
            const unary = yield* Effect.flip(
              invoker.unary("missing.Svc/A", {}),
            );
            const server = yield* Effect.flip(
              Stream.runCollect(invoker.serverStream("missing.Svc/B", {})),
            );
            const client = yield* Effect.flip(
              invoker.clientStream("missing.Svc/C", Stream.empty),
            );
            const bidi = yield* Effect.flip(
              Stream.runCollect(
                invoker.bidiStream("missing.Svc/D", Stream.empty),
              ),
            );
            return [unary, server, client, bidi].map(
              (error) => (error as GrpcStatusError.GrpcStatusError).code,
            );
          });
        }).pipe(
          Effect.provide(
            GrpcClientProtocol.layer({
              registry: new Map(),
              baseUrl: "http://127.0.0.1:1",
            }),
          ),
        );

        assert.deepStrictEqual(codes, [
          "unimplemented",
          "unimplemented",
          "unimplemented",
          "unimplemented",
        ]);
      }),
  );

  it.effect(
    "fails with unimplemented when a tag is invoked as the wrong kind",
    () =>
      Effect.gen(function* () {
        // `lookup` only reads `kind`, so a minimal entry exercises kind validation
        // without a transport round trip.
        const registry = new Map([
          ["test.Svc/Unary", { kind: "unary" } as never],
        ]);
        const error = yield* GrpcInvoker.GrpcInvoker.pipe(
          Effect.flatMap((invoker) =>
            Effect.flip(
              Stream.runCollect(invoker.serverStream("test.Svc/Unary", {})),
            ),
          ),
          Effect.provide(
            GrpcClientProtocol.layer({
              registry,
              baseUrl: "http://127.0.0.1:1",
            }),
          ),
        );

        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "unimplemented",
        );
      }),
  );

  it.effect(
    "rejects reserved metadata with invalid_argument before touching the transport",
    () =>
      Effect.gen(function* () {
        // Metadata is validated before method resolution, so a minimal entry
        // reaches the check without a network call.
        const registry = new Map([
          ["test.Svc/Unary", { kind: "unary" } as never],
        ]);
        const error = yield* GrpcInvoker.GrpcInvoker.pipe(
          Effect.flatMap((invoker) =>
            Effect.flip(
              invoker.unary(
                "test.Svc/Unary",
                {},
                {
                  metadata: [["x-effect-grpc-foo", "bar"]],
                },
              ),
            ),
          ),
          Effect.provide(
            GrpcClientProtocol.layer({
              registry,
              baseUrl: "http://127.0.0.1:1",
            }),
          ),
        );

        assert.strictEqual(
          (error as GrpcStatusError.GrpcStatusError).code,
          "invalid_argument",
        );
      }),
  );

  it.effect(
    "dies with a named defect when an entry's localName is not on the service",
    () =>
      Effect.gen(function* () {
        // `localName` is carried verbatim by registry entries (the built-in
        // services hand-write it), so a mismatch is a wiring defect, not a status.
        const entry = { ...unaryEntry, localName: "nope" };
        const spans = captureSpanEnds();
        const result = yield* Effect.gen(function* () {
          const exit = yield* Effect.exit(
            GrpcInvoker.GrpcInvoker.pipe(
              Effect.flatMap((invoker) => invoker.unary(entry.tag, {})),
              Effect.provide(
                GrpcClientProtocol.layer({
                  registry: new Map([[entry.tag, entry]]),
                  baseUrl: "http://127.0.0.1:1",
                }),
              ),
            ),
          );
          return { exit, metrics: yield* Metric.snapshot };
        }).pipe(spans.provide);

        assert.isTrue(Exit.isFailure(result.exit));
        assert.include(String(result.exit), "has no method 'nope'");

        // The defect kills the fiber, so the call has to be recorded before the
        // throw or the span would close OK, attributeless and without a duration
        // observation — telemetry showing a healthy call that never happened.
        const span = spans.expect(entry.tag);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "UNIMPLEMENTED",
        );
        assert.strictEqual(span.attributes.get("error.type"), "UNIMPLEMENTED");
        assert.isTrue(Exit.isFailure(span.exit));

        const durations = result.metrics.filter(
          (metric) => metric.id === "rpc.client.call.duration",
        );
        assert.lengthOf(durations, 1);
        assert.deepInclude(durations[0]?.attributes ?? {}, {
          "rpc.method": entry.tag,
          "rpc.response.status_code": "UNIMPLEMENTED",
          "error.type": "UNIMPLEMENTED",
        });
      }),
  );

  it.effect(
    "maps synchronous server and bidi transport throws to typed status errors",
    () =>
      Effect.gen(function* () {
        const transport = {
          stream() {
            throw new ConnectError("transport unavailable", Code.Unavailable);
          },
        } as unknown as Transport;

        const errors = yield* Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const server = yield* Effect.flip(
            Stream.runCollect(
              invoker.serverStream(serverStreamingEntry.tag, {}),
            ),
          );
          const bidi = yield* Effect.flip(
            Stream.runCollect(
              invoker.bidiStream(bidiStreamingEntry.tag, Stream.empty),
            ),
          );
          return [server, bidi];
        }).pipe(
          Effect.provide(
            GrpcInvoker.layerConnect({
              registry: new Map([
                [serverStreamingEntry.tag, serverStreamingEntry],
                [bidiStreamingEntry.tag, bidiStreamingEntry],
              ]),
              transport,
            }),
          ),
        );

        for (const error of errors) {
          assert.instanceOf(error, GrpcStatusError.GrpcStatusError);
          assert.strictEqual(error.code, "unavailable");
        }
      }),
  );
});

/**
 * Snapshots each span's attributes and exit as it ends, and isolates the
 * metric registry so a snapshot sees only the call under test.
 */
const captureSpanEnds = () => {
  const ended = new Map<
    string,
    {
      readonly attributes: ReadonlyMap<string, unknown>;
      readonly exit: Exit.Exit<unknown, unknown>;
    }
  >();
  const native = Context.get(Context.empty(), Tracer.Tracer);
  const tracer = Tracer.make({
    span(options) {
      const span = native.span(options);
      const end = span.end.bind(span);
      span.end = (endTime, exit) => {
        ended.set(span.name, { attributes: new Map(span.attributes), exit });
        end(endTime, exit);
      };
      return span;
    },
  });
  const registry = new Map<string, never>();
  return {
    provide: <A, E, R>(
      effect: Effect.Effect<A, E, R>,
    ): Effect.Effect<A, E, R> =>
      effect.pipe(
        Effect.provideService(Tracer.Tracer, tracer),
        Effect.provideService(Metric.MetricRegistry, registry as never),
      ),
    expect: (name: string) => {
      const span = ended.get(name);
      if (!span) {
        throw new Error(`Expected an ended span named ${name}`);
      }
      return span;
    },
  };
};

const {
  unary: unaryEntry,
  serverStreaming: serverStreamingEntry,
  bidiStreaming: bidiStreamingEntry,
} = methodEntries("test.Svc");
