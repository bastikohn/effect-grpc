import { createContextKey, createContextValues } from "@connectrpc/connect";
import type { HandlerContext } from "@connectrpc/connect";
import { Deferred, Effect, Stream } from "effect";
import { describe, expect, it } from "vitest";

import type { GrpcServerContext } from "../src/CodegenSupport.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import {
  captureImplementation,
  handlerContext,
  methodEntries,
} from "./support/serverHarness.js";

const {
  unary: unaryEntry,
  serverStreaming: serverStreamingEntry,
  clientStreaming: clientStreamingEntry,
  bidiStreaming: bidiStreamingEntry,
} = methodEntries("demo.v1.ContextService");

const requestIdKey = createContextKey<string | undefined>(undefined, {
  description: "request id",
});

describe("GrpcServerContext", () => {
  it.each([
    unaryEntry,
    serverStreamingEntry,
    clientStreamingEntry,
    bidiStreamingEntry,
  ])(
    "delivers method identity, metadata, the call signal and typed values to a $kind handler",
    async (entry) => {
      const values = createContextValues().set(requestIdKey, "req-1");
      const context = handlerContext({
        headers: new Headers({ "X-Demo": "42" }),
        values,
      });

      const seen = await Effect.runPromise(
        Effect.gen(function* () {
          const seen = yield* Deferred.make<GrpcServerContext>();
          const implementation = yield* implementationOf(entry, (context) =>
            Deferred.succeed(seen, context).pipe(Effect.as({ ok: true })),
          );
          yield* drive(implementation, entry, context);
          return yield* Deferred.await(seen);
        }),
      );

      expect(seen.method).toEqual({ tag: entry.tag, kind: entry.kind });
      expect(seen.metadata).toEqual([["x-demo", "42"]]);
      expect(seen.signal).toBe(context.signal);
      expect(seen.getContextValue(requestIdKey)).toBe("req-1");
    },
  );

  it("reads the remaining deadline live, clamping an elapsed budget to zero", async () => {
    let remaining: number | undefined = 1_000;
    const context = handlerContext({ timeoutMs: () => remaining });
    const noDeadline = handlerContext();

    const reads = await Effect.runPromise(
      Effect.gen(function* () {
        const implementation = yield* implementationOf(unaryEntry, (context) =>
          // The response is JSON-encoded, so "no deadline" travels as `null`.
          Effect.sync(() => {
            const first = context.remainingTimeoutMs() ?? null;
            remaining = 250;
            const second = context.remainingTimeoutMs() ?? null;
            remaining = -5;
            const third = context.remainingTimeoutMs() ?? null;
            return { first, second, third };
          }),
        );
        const withDeadline = yield* drive(implementation, unaryEntry, context);
        const without = yield* drive(implementation, unaryEntry, noDeadline);
        return { withDeadline, without };
      }),
    );

    expect(reads.withDeadline).toEqual({ first: 1_000, second: 250, third: 0 });
    expect(reads.without).toEqual({ first: null, second: null, third: null });
  });

  it("answers an unset key with its default and keeps same-description keys apart", async () => {
    const keyA = createContextKey("a-default", { description: "shared" });
    const keyB = createContextKey("b-default", { description: "shared" });
    const values = createContextValues().set(keyA, "a-set");

    const read = await Effect.runPromise(
      Effect.gen(function* () {
        const implementation = yield* implementationOf(unaryEntry, (context) =>
          Effect.succeed({
            a: context.getContextValue(keyA),
            b: context.getContextValue(keyB),
            requestId: context.getContextValue(requestIdKey) ?? null,
          }),
        );
        return yield* drive(
          implementation,
          unaryEntry,
          handlerContext({ values }),
        );
      }),
    );

    expect(read).toEqual({ a: "a-set", b: "b-default", requestId: null });
  });

  it("keeps concurrent calls' stores, signals, metadata and budgets apart", async () => {
    const first = handlerContext({
      headers: { "x-call": "first" },
      timeoutMs: () => 100,
      values: createContextValues().set(requestIdKey, "req-first"),
    });
    const second = handlerContext({
      headers: { "x-call": "second" },
      timeoutMs: () => 200,
      values: createContextValues().set(requestIdKey, "req-second"),
    });

    const seen = await Effect.runPromise(
      Effect.gen(function* () {
        // Both handlers observe their context, then wait for each other before
        // completing, so the two calls are provably in flight at once.
        const arrived = yield* Deferred.make<void>();
        const contexts: Array<GrpcServerContext> = [];
        const implementation = yield* implementationOf(
          serverStreamingEntry,
          (context) =>
            Effect.gen(function* () {
              if (contexts.push(context) === 2) {
                yield* Deferred.succeed(arrived, undefined);
              }
              yield* Deferred.await(arrived);
              return context.getContextValue(requestIdKey);
            }),
        );
        const ids = yield* Effect.all(
          [
            drive(implementation, serverStreamingEntry, first),
            drive(implementation, serverStreamingEntry, second),
          ],
          { concurrency: "unbounded" },
        );
        return { ids, contexts };
      }),
    );

    expect(seen.ids).toEqual([["req-first"], ["req-second"]]);
    const byId = (id: string) =>
      seen.contexts.find(
        (context) => context.getContextValue(requestIdKey) === id,
      )!;
    expect(byId("req-first").signal).toBe(first.signal);
    expect(byId("req-second").signal).toBe(second.signal);
    expect(byId("req-first").metadata).toEqual([["x-call", "first"]]);
    expect(byId("req-second").metadata).toEqual([["x-call", "second"]]);
    expect(byId("req-first").remainingTimeoutMs()).toBe(100);
    expect(byId("req-second").remainingTimeoutMs()).toBe(200);
  });

  it("lets nothing survive from one call into the next", async () => {
    const read = await Effect.runPromise(
      Effect.gen(function* () {
        const implementation = yield* implementationOf(unaryEntry, (context) =>
          Effect.succeed({
            requestId: context.getContextValue(requestIdKey) ?? null,
            metadata: context.metadata,
            remaining: context.remainingTimeoutMs() ?? null,
          }),
        );
        const primed = yield* drive(
          implementation,
          unaryEntry,
          handlerContext({
            headers: { "x-call": "primed" },
            timeoutMs: () => 50,
            values: createContextValues().set(requestIdKey, "req-primed"),
          }),
        );
        const fresh = yield* drive(
          implementation,
          unaryEntry,
          handlerContext(),
        );
        return { primed, fresh };
      }),
    );

    expect(read.primed).toEqual({
      requestId: "req-primed",
      metadata: [["x-call", "primed"]],
      remaining: 50,
    });
    expect(read.fresh).toEqual({
      requestId: null,
      metadata: [],
      remaining: null,
    });
  });
});

type Method = (
  request: unknown,
  context: HandlerContext,
) => Promise<unknown> | AsyncIterable<unknown>;

/**
 * A single-method server whose handler runs `observe` on its context and
 * answers with the result: as the response value for effect-shaped kinds, as
 * the only streamed element for stream-shaped kinds.
 */
const implementationOf = (
  entry: GrpcMethodEntry,
  observe: (context: GrpcServerContext) => Effect.Effect<unknown>,
) =>
  GrpcServerProtocol.make({
    registry: new Map([[entry.tag, entry]]),
    handlers: new Map([
      [
        entry.tag,
        {
          kind: entry.kind,
          handler: (_input: unknown, context: GrpcServerContext) =>
            entry.kind === "unary" || entry.kind === "client-streaming"
              ? observe(context)
              : Stream.fromEffect(observe(context)),
        } as GrpcServerProtocol.GrpcHandler,
      ],
    ]),
  }).pipe(
    Effect.map(
      ({ routes }) => captureImplementation(routes)[entry.localName] as Method,
    ),
  );

/** Calls the method the way connect would and collects its response(s). */
const drive = (
  method: Method,
  entry: GrpcMethodEntry,
  context: HandlerContext,
): Effect.Effect<unknown> =>
  Effect.promise(async () => {
    const request =
      entry.kind === "unary" || entry.kind === "server-streaming"
        ? { id: "1" }
        : (async function* () {
            yield { id: "1" };
          })();
    const response = method(request, context);
    if (Symbol.asyncIterator in response) {
      const values: Array<unknown> = [];
      for await (const value of response) values.push(value);
      return values;
    }
    return response;
  });
