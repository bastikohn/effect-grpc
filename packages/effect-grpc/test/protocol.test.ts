import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Chunk, Deferred, Effect, Fiber, Ref, Schema, Stream } from "effect";
import * as RpcClient from "@effect/rpc/RpcClient";
import type { FromServerEncoded } from "@effect/rpc/RpcMessage";
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
            .run((message) =>
              Deferred.succeed(received, message).pipe(Effect.asVoid),
            )
            .pipe(Effect.forkScoped);
          yield* Effect.yieldNow();

          yield* protocol.send({
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
    expect(response.exit.cause).toMatchObject({
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
          const disconnected = yield* protocol.disconnects.take;
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
          const disconnected = yield* protocol.disconnects.take;
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
          const disconnected = yield* protocol.disconnects.take;
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

describe("GrpcServerProtocol streaming bridge", () => {
  it("bridges client-streaming requests to the handler", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [clientStreamingEntry.tag, clientStreamingEntry],
            ]),
            streamingHandlers: new Map<
              string,
              GrpcServerProtocol.GrpcStreamingHandler
            >([
              [
                clientStreamingEntry.tag,
                {
                  kind: "client-streaming",
                  handler: (requests) =>
                    Stream.runCollect(requests).pipe(
                      Effect.map((items) => ({
                        items: Chunk.toReadonlyArray(items),
                      })),
                    ),
                },
              ],
            ]),
          });
          const implementation = captureUnaryImplementation(routes);

          return yield* Effect.promise(() =>
            implementation.upload(
              (async function* () {
                yield { id: "1" };
                yield { id: "2" };
              })() as never,
              handlerContext(),
            ),
          );
        }),
      ),
    );

    expect(result).toEqual({ items: [{ id: "1" }, { id: "2" }] });
  });

  it("bridges bidi streams and maps mid-stream handler failures", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[bidiStreamingEntry.tag, bidiStreamingEntry]]),
            streamingHandlers: new Map<
              string,
              GrpcServerProtocol.GrpcStreamingHandler
            >([
              [
                bidiStreamingEntry.tag,
                {
                  kind: "bidi-streaming",
                  handler: (requests) =>
                    Stream.mapEffect(requests, (request) =>
                      (request as { readonly id: string }).id === "boom"
                        ? Effect.fail(GrpcStatusError.notFound("boom"))
                        : Effect.succeed(request),
                    ),
                },
              ],
            ]),
          });
          const implementation = captureServerStreamingImplementation(routes);

          return yield* Effect.promise(async () => {
            const received: Array<unknown> = [];
            try {
              for await (const value of implementation.chat(
                (async function* () {
                  yield { id: "1" };
                  yield { id: "boom" };
                })() as never,
                handlerContext(),
              )) {
                received.push(value);
              }
            } catch (cause) {
              return {
                received,
                error: GrpcStatusError.fromConnectError(cause),
              };
            }
            throw new Error("Expected bidi handler failure");
          });
        }),
      ),
    );

    expect(result.received).toEqual([{ id: "1" }]);
    expect(result.error).toMatchObject({
      code: "not_found",
      message: "boom",
    });
  });

  it("rejects streaming methods without a registered handler as unimplemented", async () => {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([
              [clientStreamingEntry.tag, clientStreamingEntry],
            ]),
          });
          const implementation = captureUnaryImplementation(routes);

          return yield* Effect.promise(async () => {
            try {
              await implementation.upload(
                (async function* () {})() as never,
                handlerContext(),
              );
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected missing handler to fail");
          });
        }),
      ),
    );

    expect(error).toMatchObject({
      code: "unimplemented",
      message: "Missing streaming handler for demo.v1.TestService/Upload",
    });
  });

  it("maps invalid streamed request payloads to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const entry = {
            ...clientStreamingEntry,
            payloadSchema: Schema.Struct({ id: Schema.String }),
          } satisfies GrpcMethodEntry;
          const { routes } = yield* GrpcServerProtocol.make({
            registry: new Map([[entry.tag, entry]]),
            streamingHandlers: new Map<
              string,
              GrpcServerProtocol.GrpcStreamingHandler
            >([
              [
                entry.tag,
                {
                  kind: "client-streaming",
                  handler: (requests) => Stream.runDrain(requests),
                },
              ],
            ]),
          });
          const implementation = captureUnaryImplementation(routes);

          return yield* Effect.promise(async () => {
            try {
              await implementation.upload(
                (async function* () {
                  yield { id: 42 };
                })() as never,
                handlerContext(),
              );
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected streamed payload validation to fail");
          });
        }),
      ),
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
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
          .pipe(Effect.fork);
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
  successSchema: Schema.Unknown,
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

const clientStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "client-streaming",
  tag: "demo.v1.TestService/Upload",
  localName: "upload",
};

const bidiStreamingEntry: GrpcMethodEntry = {
  ...unaryEntry,
  kind: "bidi-streaming",
  tag: "demo.v1.TestService/Chat",
  localName: "chat",
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
