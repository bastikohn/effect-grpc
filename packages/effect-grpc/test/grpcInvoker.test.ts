import type { Transport } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import {
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Metric,
  Ref,
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

  it.effect(
    "bounds unary and client-streaming calls with deadline_exceeded",
    () =>
      Effect.gen(function* () {
        const fiber = yield* Effect.forkChild(
          withInvoker(
            {
              "test.Svc/Unary": {
                kind: "unary",
                handler: () => Effect.never,
              },
              "test.Svc/ClientStream": {
                kind: "client-streaming",
                handler: () => Effect.never,
              },
            },
            (invoker) =>
              Effect.gen(function* () {
                const unary = yield* Effect.flip(
                  invoker.unary("test.Svc/Unary", {}, { timeoutMs: 20 }),
                );
                const client = yield* Effect.flip(
                  invoker.clientStream("test.Svc/ClientStream", Stream.empty, {
                    timeoutMs: 20,
                  }),
                );
                return [unary, client];
              }),
          ),
        );
        // The two calls run back to back, so both 20ms deadlines have to pass.
        yield* TestClock.adjust(40);
        const codes = yield* Fiber.join(fiber);

        for (const error of codes) {
          assert.strictEqual(
            (error as GrpcStatusError.GrpcStatusError).code,
            "deadline_exceeded",
          );
        }
      }),
  );

  // `timeoutMs <= 0` uniformly means *no deadline* — the connect adapter drops
  // the option (see `invokerParity.test.ts`), so the in-memory one must neither
  // enforce it nor put it on the call context, or a zero would turn every call
  // into an instant `deadline_exceeded`.
  it.effect("treats a non-positive timeoutMs as no deadline", () =>
    Effect.gen(function* () {
      let seen: GrpcInvoker.GrpcInMemoryCall | undefined;
      const fiber = yield* Effect.forkChild(
        withInvoker(
          {
            "test.Svc/Unary": {
              kind: "unary",
              handler: (request, call) =>
                Effect.sync(() => {
                  seen = call;
                }).pipe(Effect.andThen(Effect.sleep(20)), Effect.as(request)),
            },
            "test.Svc/ClientStream": {
              kind: "client-streaming",
              handler: () =>
                Effect.sleep(20).pipe(Effect.as("client response")),
            },
          },
          (invoker) =>
            Effect.all([
              invoker.unary("test.Svc/Unary", "unary response", {
                timeoutMs: 0,
              }),
              invoker.clientStream("test.Svc/ClientStream", Stream.empty, {
                timeoutMs: 0,
              }),
            ]),
        ),
      );
      // The two handlers sleep back to back, so both 20ms waits have to pass.
      yield* TestClock.adjust(40);
      const result = yield* Fiber.join(fiber);

      assert.deepStrictEqual(result, ["unary response", "client response"]);
      assert.deepStrictEqual(seen, { tag: "test.Svc/Unary", metadata: [] });
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
