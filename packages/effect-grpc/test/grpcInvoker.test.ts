import type { Transport } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
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
import * as Tracer from "effect/Tracer";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { methodEntries } from "./support/serverHarness.js";

const withInvokerEffect = <A, E>(
  handlers: GrpcInvoker.GrpcInMemoryHandlers,
  body: (
    invoker: GrpcInvoker.GrpcInvokerService,
  ) => Effect.Effect<A, E, GrpcInvoker.GrpcInvoker>,
): Effect.Effect<A, E> =>
  Effect.gen(function* () {
    const invoker = yield* GrpcInvoker.GrpcInvoker;
    return yield* body(invoker);
  }).pipe(Effect.provide(GrpcInvoker.layerInMemory(handlers)));

const withInvoker = <A, E>(
  handlers: GrpcInvoker.GrpcInMemoryHandlers,
  body: (
    invoker: GrpcInvoker.GrpcInvokerService,
  ) => Effect.Effect<A, E, GrpcInvoker.GrpcInvoker>,
): Promise<A> => Effect.runPromise(withInvokerEffect(handlers, body));

describe("GrpcInvoker (in-memory adapter)", () => {
  it("round trips all four call shapes with domain values", async () => {
    const result = await withInvoker(
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

    expect(result).toEqual({
      unary: { echoed: "hi" },
      server: ["s", "s"],
      client: { count: 3 },
      bidi: [{ doubled: 1 }, { doubled: 2 }],
    });
  });

  it("fails with a stable unimplemented status for unknown tags and kind mismatches", async () => {
    const codes = await withInvoker(
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
      expect(error).toBeInstanceOf(GrpcStatusError.GrpcStatusError);
      expect((error as GrpcStatusError.GrpcStatusError).code).toBe(
        "unimplemented",
      );
      expect((error as GrpcStatusError.GrpcStatusError).message).toContain(
        "Unknown gRPC RPC tag",
      );
    }
  });

  it("exposes normalized metadata and timeout on the call context", async () => {
    let seen: GrpcInvoker.GrpcInMemoryCall | undefined;
    await withInvoker(
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

    expect(seen).toEqual({
      tag: "test.Svc/Unary",
      metadata: [["x-test", "42"]],
      timeoutMs: 5000,
    });
  });

  it("bounds unary and client-streaming calls with deadline_exceeded", async () => {
    const codes = await withInvoker(
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
    );

    for (const error of codes) {
      expect((error as GrpcStatusError.GrpcStatusError).code).toBe(
        "deadline_exceeded",
      );
    }
  });

  it.each([0, -1])(
    "does not impose a deadline for timeoutMs=%s",
    async (timeoutMs) => {
      const result = await withInvoker(
        {
          "test.Svc/Unary": {
            kind: "unary",
            handler: (request) => Effect.sleep(1).pipe(Effect.as(request)),
          },
          "test.Svc/ClientStream": {
            kind: "client-streaming",
            handler: () => Effect.sleep(1).pipe(Effect.as("client response")),
          },
        },
        (invoker) =>
          Effect.all([
            invoker.unary("test.Svc/Unary", "unary response", { timeoutMs }),
            invoker.clientStream("test.Svc/ClientStream", Stream.empty, {
              timeoutMs,
            }),
          ]),
      );

      expect(result).toEqual(["unary response", "client response"]);
    },
  );

  it("terminates the handler when the caller interrupts", async () => {
    const observed = await withInvoker(
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

    expect(observed).toBe(true);
  });

  it("replays the caller's original error while the handler observes cancelled", async () => {
    const boom = new Error("source boom");
    let handlerSaw: GrpcStatusError.GrpcStatusError | undefined;

    const result = await withInvoker(
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

    expect(result).toBe(boom);
    expect(handlerSaw?.code).toBe("cancelled");
  });

  it("replays the caller's original error when streaming handlers recover", async () => {
    const boom = new Error("source boom");
    const failures = await withInvoker(
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
            invoker.clientStream("test.Svc/ClientStream", Stream.fail(boom)),
          ),
          Effect.flip(
            Stream.runCollect(
              invoker.bidiStream("test.Svc/BidiStream", Stream.fail(boom)),
            ),
          ),
        ]),
    );

    expect(failures).toEqual([boom, boom]);
  });

  it("rejects reserved metadata keys with invalid_argument on every shape", async () => {
    const reserved = { metadata: [["x-effect-grpc-foo", "bar"]] as const };
    const codes = await withInvoker(
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
              invoker.bidiStream("test.Svc/BidiStream", Stream.empty, reserved),
            ),
          );
          return [unary, server, client, bidi].map(
            (error) => (error as GrpcStatusError.GrpcStatusError).code,
          );
        }),
    );

    expect(codes).toEqual([
      "invalid_argument",
      "invalid_argument",
      "invalid_argument",
      "invalid_argument",
    ]);
  });

  it("constructs unary and server-streaming handlers lazily after metadata validation", async () => {
    let unaryCalls = 0;
    let serverCalls = 0;
    const reserved = { metadata: [["x-effect-grpc-foo", "bar"]] as const };

    const result = await withInvoker(
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

    expect(result).toEqual({
      afterConstruction: [0, 0],
      afterValidation: [0, 0],
    });
  });

  it("keeps source-failure capture execution-local across re-runs", async () => {
    const boom = new Error("first-run boom");
    const runs = Effect.runSync(Ref.make(0));

    // One effect run twice: the request stream fails only on the first run.
    // A shared (non-execution-local) failure capture would poison the second.
    const call = withInvokerEffect(
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

    const first = await Effect.runPromise(Effect.flip(call));
    const second = await Effect.runPromise(call);

    expect(first).toBe(boom);
    expect(second).toBe("done");
  });

  it("finalizes the handler stream when a bidi consumer stops early", async () => {
    let finalized = 0;
    const first = await withInvoker(
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

    expect(first).toEqual(["a"]);
    expect(finalized).toBe(1);
  });
});

describe("GrpcInvoker (connect adapter)", () => {
  it("fails with a stable unimplemented status for unknown tags on all shapes", async () => {
    const codes = await Effect.runPromise(
      Effect.gen(function* () {
        const invoker = yield* GrpcInvoker.GrpcInvoker;
        return yield* Effect.gen(function* () {
          const unary = yield* Effect.flip(invoker.unary("missing.Svc/A", {}));
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
          GrpcInvoker.layerConnect({
            registry: new Map(),
            transport: GrpcClientProtocol.makeTransport({
              baseUrl: "http://127.0.0.1:1",
            }),
          }),
        ),
      ),
    );

    expect(codes).toEqual([
      "unimplemented",
      "unimplemented",
      "unimplemented",
      "unimplemented",
    ]);
  });

  it("fails with unimplemented when a tag is invoked as the wrong kind", async () => {
    // `lookup` only reads `kind`, so a minimal entry exercises kind validation
    // without a transport round trip.
    const registry = new Map([["test.Svc/Unary", { kind: "unary" } as never]]);
    const error = await Effect.runPromise(
      GrpcInvoker.GrpcInvoker.pipe(
        Effect.flatMap((invoker) =>
          Effect.flip(
            Stream.runCollect(invoker.serverStream("test.Svc/Unary", {})),
          ),
        ),
        Effect.provide(
          GrpcInvoker.layerConnect({
            registry,
            transport: GrpcClientProtocol.makeTransport({
              baseUrl: "http://127.0.0.1:1",
            }),
          }),
        ),
      ),
    );

    expect((error as GrpcStatusError.GrpcStatusError).code).toBe(
      "unimplemented",
    );
  });

  it("rejects reserved metadata with invalid_argument before touching the transport", async () => {
    // Metadata is validated before method resolution, so a minimal entry
    // reaches the check without a network call.
    const registry = new Map([["test.Svc/Unary", { kind: "unary" } as never]]);
    const error = await Effect.runPromise(
      GrpcInvoker.GrpcInvoker.pipe(
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
          GrpcInvoker.layerConnect({
            registry,
            transport: GrpcClientProtocol.makeTransport({
              baseUrl: "http://127.0.0.1:1",
            }),
          }),
        ),
      ),
    );

    expect((error as GrpcStatusError.GrpcStatusError).code).toBe(
      "invalid_argument",
    );
  });

  it("dies with a named defect when an entry's localName is not on the service", async () => {
    // `localName` is carried verbatim by registry entries (the built-in
    // services hand-write it), so a mismatch is a wiring defect, not a status.
    const entry = { ...unaryEntry, localName: "nope" };
    const spans = captureSpanEnds();
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const exit = yield* Effect.exit(
          GrpcInvoker.GrpcInvoker.pipe(
            Effect.flatMap((invoker) => invoker.unary(entry.tag, {})),
            Effect.provide(
              GrpcInvoker.layerConnect({
                registry: new Map([[entry.tag, entry]]),
                transport: GrpcClientProtocol.makeTransport({
                  baseUrl: "http://127.0.0.1:1",
                }),
              }),
            ),
          ),
        );
        return { exit, metrics: yield* Metric.snapshot };
      }).pipe(spans.provide),
    );

    expect(Exit.isFailure(result.exit)).toBe(true);
    expect(String(result.exit)).toContain("has no method 'nope'");

    // The defect kills the fiber, so the call has to be recorded before the
    // throw or the span would close OK, attributeless and without a duration
    // observation — telemetry showing a healthy call that never happened.
    const span = spans.expect(entry.tag);
    expect(span.attributes.get("rpc.response.status_code")).toBe(
      "UNIMPLEMENTED",
    );
    expect(span.attributes.get("error.type")).toBe("UNIMPLEMENTED");
    expect(Exit.isFailure(span.exit)).toBe(true);

    const durations = result.metrics.filter(
      (metric) => metric.id === "rpc.client.call.duration",
    );
    expect(durations).toHaveLength(1);
    expect(durations[0]?.attributes).toMatchObject({
      "rpc.method": entry.tag,
      "rpc.response.status_code": "UNIMPLEMENTED",
      "error.type": "UNIMPLEMENTED",
    });
  });

  it("maps synchronous server and bidi transport throws to typed status errors", async () => {
    const transport = {
      stream() {
        throw new ConnectError("transport unavailable", Code.Unavailable);
      },
    } as unknown as Transport;

    const errors = await Effect.runPromise(
      Effect.gen(function* () {
        const invoker = yield* GrpcInvoker.GrpcInvoker;
        const server = yield* Effect.flip(
          Stream.runCollect(invoker.serverStream(serverStreamingEntry.tag, {})),
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
      ),
    );

    for (const error of errors) {
      expect(error).toBeInstanceOf(GrpcStatusError.GrpcStatusError);
      expect(error.code).toBe("unavailable");
    }
  });
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
