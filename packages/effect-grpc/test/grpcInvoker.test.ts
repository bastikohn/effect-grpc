import { Deferred, Effect, Fiber, Stream } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

const withInvoker = <A, E>(
  handlers: GrpcInvoker.GrpcInMemoryHandlers,
  body: (
    invoker: GrpcInvoker.GrpcInvokerService,
  ) => Effect.Effect<A, E, GrpcInvoker.GrpcInvoker>,
): Promise<A> =>
  Effect.runPromise(
    Effect.gen(function* () {
      const invoker = yield* GrpcInvoker.GrpcInvoker;
      return yield* body(invoker);
    }).pipe(Effect.provide(GrpcInvoker.layerInMemory(handlers))),
  );

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
});
