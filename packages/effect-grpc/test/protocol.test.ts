import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Deferred, Effect, Fiber, Queue, Ref, Schema } from "effect";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { FromServerEncoded } from "effect/unstable/rpc/RpcMessage";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import * as CallState from "../src/internal/callState.js";
import { failureExit, successExit } from "../src/internal/status.js";

describe("GrpcClientProtocol", () => {
  it("maps an unknown registry tag to unimplemented", async () => {
    const response = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const protocol = yield* RpcClient.Protocol;
          const received = yield* Deferred.make<FromServerEncoded>();

          yield* protocol
            .run(0, (message) =>
              Deferred.succeed(received, message).pipe(Effect.asVoid),
            )
            .pipe(Effect.forkScoped);
          yield* Effect.yieldNow;

          yield* protocol.send(0, {
            _tag: "Request",
            id: "1",
            tag: "missing.Service/Call",
            payload: {},
            headers: [],
          });

          return yield* Deferred.await(received);
        }),
      ).pipe(
        Effect.provide(
          GrpcClientProtocol.layer({
            baseUrl: "http://127.0.0.1:1",
            registry: new Map(),
          }),
        ),
      ),
    );

    expect(response?._tag).toBe("Exit");
    if (response?._tag !== "Exit" || response.exit._tag !== "Failure") {
      throw new Error("Expected failure exit");
    }
    expect(response.exit.cause[0]).toMatchObject({
      _tag: "Fail",
      error: { code: "unimplemented" },
    });
  });
});

describe("metadataInterceptor", () => {
  it("adds metadata as defaults, lets per-call win, and re-reads per call", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const token = yield* Ref.make("t1");
        const interceptor = yield* GrpcClientProtocol.metadataInterceptor(
          Ref.get(token).pipe(
            Effect.map((t) => [["authorization", `Bearer ${t}`]] as const),
          ),
        );

        const invoke = (header: Headers) =>
          Effect.promise(async () => {
            const next = ((req: { header: Headers }) =>
              Promise.resolve(req)) as unknown as Parameters<
              typeof interceptor
            >[0];
            await interceptor(next)({ header } as never);
            return header.get("authorization");
          });

        const fresh = yield* invoke(new Headers());
        const perCall = yield* invoke(
          new Headers({ authorization: "Bearer explicit" }),
        );
        yield* Ref.set(token, "t2");
        const rotated = yield* invoke(new Headers());
        return { fresh, perCall, rotated };
      }),
    );

    expect(result).toEqual({
      fresh: "Bearer t1",
      perCall: "Bearer explicit",
      rotated: "Bearer t2",
    });
  });
});

describe("GrpcServerProtocol", () => {
  it("signals disconnects and clears client ids after normal unary completion", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { protocol, routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
          });
          const implementation = captureUnaryImplementation(routes);

          yield* protocol
            .run((clientId, data) =>
              data._tag === "Request"
                ? protocol.send(clientId, successExit(data.id, { ok: true }))
                : Effect.void,
            )
            .pipe(Effect.forkScoped);

          const response = yield* Effect.promise(() =>
            implementation.get({}, handlerContext()),
          );
          const disconnected = yield* Queue.take(protocol.disconnects);
          const clientIds = yield* protocol.clientIds;
          return { response, disconnected, clientIds };
        }),
      ),
    );

    expect(result.response).toEqual({ ok: true });
    expect(result.disconnected).toBe(0);
    expect(result.clientIds.size).toBe(0);
  });

  it("signals disconnects and clears client ids after unary handler failure", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { protocol, routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[unaryEntry.tag, unaryEntry]]),
          });
          const implementation = captureUnaryImplementation(routes);

          yield* protocol
            .run((clientId, data) =>
              data._tag === "Request"
                ? protocol.send(
                    clientId,
                    failureExit(data.id, GrpcStatusError.notFound("missing")),
                  )
                : Effect.void,
            )
            .pipe(Effect.forkScoped);

          const error = yield* Effect.promise(async () => {
            try {
              await implementation.get({}, handlerContext());
            } catch (cause) {
              return cause;
            }
            throw new Error("Expected unary handler to fail");
          });
          const disconnected = yield* Queue.take(protocol.disconnects);
          const clientIds = yield* protocol.clientIds;
          return { error, disconnected, clientIds };
        }),
      ),
    );

    expect(result.error).toMatchObject({
      rawMessage: "missing",
    });
    expect(result.disconnected).toBe(0);
    expect(result.clientIds.size).toBe(0);
  });

  it("maps request converter failures to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entry = {
            ...unaryEntry,
            fromGrpcRequest: () => {
              throw new Error("bad request");
            },
          } satisfies GrpcMethodEntry;
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[entry.tag, entry]]),
          });
          const implementation = captureUnaryImplementation(routes);

          return yield* Effect.promise(async () => {
            try {
              await implementation.get({}, handlerContext());
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected request conversion to fail");
          });
        }),
      ),
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
  });

  it("maps encoded request payload validation failures to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entry = {
            ...unaryEntry,
            payloadSchema: Schema.Struct({ id: Schema.String }),
            fromGrpcRequest: () => ({ id: 123 }),
          } satisfies GrpcMethodEntry;
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[entry.tag, entry]]),
          });
          const implementation = captureUnaryImplementation(routes);

          return yield* Effect.promise(async () => {
            try {
              await implementation.get({}, handlerContext());
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected request validation to fail");
          });
        }),
      ),
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
  });

  it("maps unary response converter failures to internal", async () => {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entry = {
            ...unaryEntry,
            toGrpcResponse: () => {
              throw new Error("bad response");
            },
          } satisfies GrpcMethodEntry;
          const { protocol, routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[entry.tag, entry]]),
          });
          const implementation = captureUnaryImplementation(routes);

          yield* protocol
            .run((clientId, data) =>
              data._tag === "Request"
                ? protocol.send(clientId, successExit(data.id, { ok: true }))
                : Effect.void,
            )
            .pipe(Effect.forkScoped);

          return yield* Effect.promise(async () => {
            try {
              await implementation.get({}, handlerContext());
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected response conversion to fail");
          });
        }),
      ),
    );

    expect(error).toMatchObject({
      code: "internal",
      message: "Invalid gRPC response payload",
    });
  });

  it("maps server-streaming response converter failures to internal and cleans up", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entry = {
            ...serverStreamingEntry,
            toGrpcResponse: () => {
              throw new Error("bad stream response");
            },
          } satisfies GrpcMethodEntry;
          const { protocol, routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[entry.tag, entry]]),
          });
          const implementation = captureServerStreamingImplementation(routes);

          yield* protocol
            .run((clientId, data) =>
              data._tag === "Request"
                ? protocol.send(clientId, {
                    _tag: "Chunk",
                    requestId: data.id,
                    values: [{ ok: true }],
                  })
                : Effect.void,
            )
            .pipe(Effect.forkScoped);

          const error = yield* Effect.promise(async () => {
            try {
              for await (const value of implementation.watch(
                {},
                handlerContext(),
              )) {
                void value;
                // Consume until the converter throws.
              }
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected streaming response conversion to fail");
          });
          const disconnected = yield* Queue.take(protocol.disconnects);
          const clientIds = yield* protocol.clientIds;
          return { error, disconnected, clientIds };
        }),
      ),
    );

    expect(result.error).toMatchObject({
      code: "internal",
      message: "Invalid gRPC response payload",
    });
    expect(result.disconnected).toBe(0);
    expect(result.clientIds.size).toBe(0);
  });
});

describe("call state", () => {
  it("queues server-streaming chunks and terminal exits", async () => {
    const values = await Effect.runPromise(
      Effect.gen(function* () {
        const state = yield* CallState.makeServerStreaming;
        yield* state.offer({
          _tag: "Chunk",
          requestId: "0",
          values: [{ id: "1" }],
        });
        yield* state.offer({
          _tag: "Exit",
          requestId: "0",
          exit: { _tag: "Success", value: undefined },
        });
        yield* state.end;
        return [
          yield* state.take,
          yield* state.take,
          yield* state.take,
        ] as const;
      }),
    );

    expect(values[0]?._tag).toBe("Chunk");
    expect(values[1]?._tag).toBe("Exit");
    expect(values[2]).toBeUndefined();
  });

  it("backpressures server-streaming offers when the buffer is full", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const state = yield* CallState.makeServerStreaming;
        const offered = yield* Deferred.make<void>();
        const chunk: FromServerEncoded = {
          _tag: "Chunk",
          requestId: "0",
          values: ["value"],
        };

        for (let index = 0; index < 16; index++) {
          yield* state.offer(chunk);
        }

        const fiber = yield* state
          .offer(chunk)
          .pipe(Effect.andThen(Deferred.succeed(offered, undefined)))
          .pipe(Effect.forkChild);
        const beforeDrain = yield* Deferred.await(offered).pipe(
          Effect.as("offered" as const),
          Effect.race(Effect.sleep("20 millis").pipe(Effect.as("blocked"))),
        );

        yield* state.take;
        yield* Fiber.join(fiber);

        return beforeDrain;
      }),
    );

    expect(result).toBe("blocked");
  });

  it("turns unary chunks into a protocol defect", async () => {
    const response = await Effect.runPromise(
      Effect.gen(function* () {
        const state = yield* CallState.makeUnary;
        yield* state.offer({
          _tag: "Chunk",
          requestId: "0",
          values: ["unexpected"],
        });
        return yield* state.awaitExit;
      }),
    );

    expect(response._tag).toBe("Defect");
  });
});

const unaryEntry: GrpcMethodEntry = {
  kind: "unary",
  tag: "demo.v1.TestService/Get",
  service: {} as GrpcMethodEntry["service"],
  localName: "get",
  payloadSchema: Schema.Unknown,
  toGrpcRequest: (value) => value as never,
  fromGrpcRequest: (message) => message,
  toGrpcResponse: (value) => value as never,
  fromGrpcResponse: (message) => message,
};

const serverStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "server-streaming",
  tag: "demo.v1.TestService/Watch",
  localName: "watch",
};

const captureUnaryImplementation = (
  routes: (router: ConnectRouter) => ConnectRouter,
): Record<
  string,
  (request: unknown, context: HandlerContext) => Promise<unknown>
> =>
  captureImplementation(routes) as Record<
    string,
    (request: unknown, context: HandlerContext) => Promise<unknown>
  >;

const captureServerStreamingImplementation = (
  routes: (router: ConnectRouter) => ConnectRouter,
): Record<
  string,
  (request: unknown, context: HandlerContext) => AsyncIterable<unknown>
> =>
  captureImplementation(routes) as Record<
    string,
    (request: unknown, context: HandlerContext) => AsyncIterable<unknown>
  >;

const captureImplementation = (
  routes: (router: ConnectRouter) => ConnectRouter,
) => {
  let implementation:
    | Record<
        string,
        (
          request: unknown,
          context: HandlerContext,
        ) => Promise<unknown> | AsyncIterable<unknown>
      >
    | undefined;
  const router = {
    service(_service: unknown, serviceImplementation: unknown) {
      implementation = serviceImplementation as typeof implementation;
      return router;
    },
  };

  routes(router as unknown as ConnectRouter);

  if (!implementation) {
    throw new Error("Expected routes to register a service implementation");
  }
  return implementation;
};

const handlerContext = (): HandlerContext =>
  ({
    requestHeader: new Headers(),
    signal: new AbortController().signal,
  }) as HandlerContext;
