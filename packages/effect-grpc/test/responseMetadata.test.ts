import { Effect, Stream } from "effect";
import { describe, expect, it } from "vitest";
import type {
  GrpcCallOptions,
  GrpcServerContext,
} from "../src/CodegenSupport.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import {
  captureImplementation,
  handlerContext,
  methodEntries,
} from "./support/serverHarness.js";

const entries = methodEntries("test.Response");
const implementation = async (handler: GrpcServerProtocol.GrpcHandler) => {
  const entry =
    handler.kind === "unary" ? entries.unary : entries.serverStreaming;
  const { routes } = await Effect.runPromise(
    GrpcServerProtocol.make({
      registry: new Map([[entry.tag, entry]]),
      handlers: new Map([[entry.tag, handler]]),
    }),
  );
  return captureImplementation(routes)[entry.localName] as (
    request: unknown,
    context: ReturnType<typeof handlerContext>,
  ) => Promise<unknown> | AsyncIterable<unknown>;
};

describe("server response metadata lifetime", () => {
  it("commits headers before the first response, permits trailers until finalization, then rejects writes", async () => {
    let context!: GrpcServerContext;
    const native = handlerContext();
    const handler = await implementation({
      kind: "server-streaming",
      handler: (_, call) => {
        context = call;
        return Stream.make(1, 2).pipe(
          Stream.ensuring(
            call
              .writeResponseTrailers([["x-finalized", "yes"]])
              .pipe(Effect.orDie),
          ),
        );
      },
    });
    const responses = handler({}, native) as AsyncIterable<unknown>;
    const iterator = responses[Symbol.asyncIterator]();
    expect(await iterator.next()).toEqual({ done: false, value: 1 });
    const lateHeader = await Effect.runPromise(
      Effect.flip(context.writeResponseHeaders([["x-late", "no"]])),
    );
    expect(lateHeader.code).toBe("failed_precondition");
    await Effect.runPromise(
      context.writeResponseTrailers([["x-trailer", "yes"]]),
    );
    expect(await iterator.next()).toEqual({ done: false, value: 2 });
    expect(await iterator.next()).toEqual({ done: true, value: undefined });
    expect(native.responseTrailer.get("x-finalized")).toBe("yes");
    expect(native.responseTrailer.get("x-trailer")).toBe("yes");
    expect(native.responseHeader.has("x-late")).toBe(false);
    for (const write of [
      context.writeResponseHeaders,
      context.writeResponseTrailers,
    ]) {
      const error = await Effect.runPromise(
        Effect.flip(write([["x-after", "no"]])),
      );
      expect(error.code).toBe("failed_precondition");
    }
  });

  it("rejects both writers when cancellation has aborted the context", async () => {
    let context!: GrpcServerContext;
    const controller = new AbortController();
    const handler = await implementation({
      kind: "server-streaming",
      handler: (_, call) => {
        context = call;
        return Stream.succeed(1).pipe(Stream.concat(Stream.never));
      },
    });
    const iterator = (
      handler(
        {},
        handlerContext({ signal: controller.signal }),
      ) as AsyncIterable<unknown>
    )[Symbol.asyncIterator]();
    await iterator.next();
    controller.abort();
    for (const write of [
      context.writeResponseHeaders,
      context.writeResponseTrailers,
    ]) {
      expect(
        (await Effect.runPromise(Effect.flip(write([["x-after", "no"]])))).code,
      ).toBe("failed_precondition");
    }
    await iterator.return?.();
  });

  it("validates a whole write before mutating headers, including reserved protocol keys", async () => {
    const native = handlerContext();
    const handler = await implementation({
      kind: "unary",
      handler: (_, context) =>
        Effect.gen(function* () {
          for (const metadata of [
            [
              ["x-first", "value"],
              ["invalid-bin", "not bytes"],
            ],
            [["grpc-status", "0"]],
            [["content-type", "text/plain"]],
          ] as const) {
            const error = yield* Effect.flip(
              context.writeResponseHeaders(metadata),
            ).pipe(Effect.orDie);
            expect(error.code).toBe("invalid_argument");
          }
          return { ok: true };
        }),
    });
    await handler({}, native);
    expect([...native.responseHeader]).toEqual([]);
  });
});

describe("in-memory response metadata boundary", () => {
  it("rejects observers for all shapes before the handler or request stream starts", async () => {
    let called = 0;
    const touch = Effect.sync(() => {
      called++;
    });
    const requests = Stream.fromEffect(touch);
    const options: GrpcCallOptions = {
      onResponseHeaders: () => {
        called++;
      },
    };
    const errors = await Effect.runPromise(
      Effect.gen(function* () {
        const invoker = yield* GrpcInvoker.GrpcInvoker;
        return yield* Effect.all([
          invoker.unary("unary", {}, options).pipe(Effect.flip),
          invoker.clientStream("client", requests, options).pipe(Effect.flip),
          invoker
            .serverStream("server", {}, options)
            .pipe(Stream.runDrain, Effect.flip),
          invoker
            .bidiStream("bidi", requests, options)
            .pipe(Stream.runDrain, Effect.flip),
        ]);
      }).pipe(
        Effect.provide(
          GrpcInvoker.layerInMemory({
            unary: { kind: "unary", handler: () => touch },
            client: { kind: "client-streaming", handler: () => touch },
            server: {
              kind: "server-streaming",
              handler: () => Stream.fromEffect(touch),
            },
            bidi: {
              kind: "bidi-streaming",
              handler: () => Stream.fromEffect(touch),
            },
          }),
        ),
      ),
    );
    expect(errors.map((error) => error.code)).toEqual(
      Array(4).fill("unimplemented"),
    );
    expect(called).toBe(0);
  });
});
