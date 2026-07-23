import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Deferred, Effect, Ref, Schema, Stream } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

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
  it("completes unary calls through the handlers map", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[unaryEntry.tag, unaryEntry]]),
          handlers: handlers(unaryEntry.tag, {
            kind: "unary",
            handler: (request, context) =>
              Effect.succeed({
                echoed: request,
                metadata: context.metadata,
              }),
          }),
        });
        const implementation = captureUnaryImplementation(routes);

        return yield* Effect.promise(() =>
          implementation.get(
            { id: "1" },
            handlerContext({ headers: new Headers({ "x-demo": "42" }) }),
          ),
        );
      }),
    );

    expect(result).toEqual({
      echoed: { id: "1" },
      metadata: [["x-demo", "42"]],
    });
  });

  it("maps unary handler failures to connect errors", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[unaryEntry.tag, unaryEntry]]),
          handlers: handlers(unaryEntry.tag, {
            kind: "unary",
            handler: () => Effect.fail(GrpcStatusError.notFound("missing")),
          }),
        });
        const implementation = captureUnaryImplementation(routes);

        return yield* Effect.promise(async () => {
          try {
            await implementation.get({}, handlerContext());
          } catch (cause) {
            return GrpcStatusError.fromConnectError(cause);
          }
          throw new Error("Expected unary handler to fail");
        });
      }),
    );

    expect(error).toMatchObject({
      code: "not_found",
      message: "missing",
    });
  });

  it("rejects unary methods without a registered handler as unimplemented", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[unaryEntry.tag, unaryEntry]]),
        });
        const implementation = captureUnaryImplementation(routes);

        return yield* Effect.promise(async () => {
          try {
            await implementation.get({}, handlerContext());
          } catch (cause) {
            return GrpcStatusError.fromConnectError(cause);
          }
          throw new Error("Expected missing handler to fail");
        });
      }),
    );

    expect(error).toMatchObject({
      code: "unimplemented",
      message: "Missing handler for demo.v1.TestService/Get",
    });
  });

  it("rejects server-streaming methods without a registered handler as unimplemented", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[serverStreamingEntry.tag, serverStreamingEntry]]),
        });
        const implementation = captureServerStreamingImplementation(routes);

        return yield* Effect.promise(async () => {
          try {
            for await (const value of implementation.watch(
              {},
              handlerContext(),
            )) {
              void value;
            }
          } catch (cause) {
            return GrpcStatusError.fromConnectError(cause);
          }
          throw new Error("Expected missing handler to fail");
        });
      }),
    );

    expect(error).toMatchObject({
      code: "unimplemented",
      message: "Missing handler for demo.v1.TestService/Watch",
    });
  });

  it("treats a handler registered under the wrong kind as unimplemented", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[unaryEntry.tag, unaryEntry]]),
          // Registered under the unary tag but with a streaming shape — the
          // kind guard must fast-fail instead of invoking the wrong shape.
          handlers: handlers(unaryEntry.tag, {
            kind: "server-streaming",
            handler: () => Stream.empty,
          }),
        });
        const implementation = captureUnaryImplementation(routes);

        return yield* Effect.promise(async () => {
          try {
            await implementation.get({}, handlerContext());
          } catch (cause) {
            return GrpcStatusError.fromConnectError(cause);
          }
          throw new Error("Expected wrong-kind handler to fail");
        });
      }),
    );

    expect(error).toMatchObject({
      code: "unimplemented",
      message: "Missing handler for demo.v1.TestService/Get",
    });
  });

  it("maps request converter failures to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const entry = {
          ...unaryEntry,
          fromGrpcRequest: () => {
            throw new Error("bad request");
          },
        } satisfies GrpcMethodEntry;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          handlers: handlers(entry.tag, {
            kind: "unary",
            handler: (request) => Effect.succeed(request),
          }),
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
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
  });

  it("maps encoded request payload validation failures to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const entry = {
          ...unaryEntry,
          payloadSchema: Schema.Struct({ id: Schema.String }),
          fromGrpcRequest: () => ({ id: 123 }),
        } satisfies GrpcMethodEntry;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          handlers: handlers(entry.tag, {
            kind: "unary",
            handler: (request) => Effect.succeed(request),
          }),
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
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
  });

  it("maps unary response converter failures to internal", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const entry = {
          ...unaryEntry,
          toGrpcResponse: () => {
            throw new Error("bad response");
          },
        } satisfies GrpcMethodEntry;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          handlers: handlers(entry.tag, {
            kind: "unary",
            handler: () => Effect.succeed({ ok: true }),
          }),
        });
        const implementation = captureUnaryImplementation(routes);

        return yield* Effect.promise(async () => {
          try {
            await implementation.get({}, handlerContext());
          } catch (cause) {
            return GrpcStatusError.fromConnectError(cause);
          }
          throw new Error("Expected response conversion to fail");
        });
      }),
    );

    expect(error).toMatchObject({
      code: "internal",
      message: "Invalid gRPC response payload",
    });
  });

  it("streams server-streaming responses and completes", async () => {
    const received = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[serverStreamingEntry.tag, serverStreamingEntry]]),
          handlers: handlers(serverStreamingEntry.tag, {
            kind: "server-streaming",
            handler: (request) =>
              Stream.make(
                { ...(request as object), sequence: 1 },
                { ...(request as object), sequence: 2 },
              ),
          }),
        });
        const implementation = captureServerStreamingImplementation(routes);

        return yield* Effect.promise(async () => {
          const values: Array<unknown> = [];
          for await (const value of implementation.watch(
            { id: "7" },
            handlerContext(),
          )) {
            values.push(value);
          }
          return values;
        });
      }),
    );

    expect(received).toEqual([
      { id: "7", sequence: 1 },
      { id: "7", sequence: 2 },
    ]);
  });

  it("maps server-streaming handler failures mid-stream", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[serverStreamingEntry.tag, serverStreamingEntry]]),
          handlers: handlers(serverStreamingEntry.tag, {
            kind: "server-streaming",
            handler: () =>
              Stream.make({ sequence: 1 }).pipe(
                Stream.concat(
                  Stream.fail(GrpcStatusError.unavailable("stream broke")),
                ),
              ),
          }),
        });
        const implementation = captureServerStreamingImplementation(routes);

        return yield* Effect.promise(async () => {
          const received: Array<unknown> = [];
          try {
            for await (const value of implementation.watch(
              {},
              handlerContext(),
            )) {
              received.push(value);
            }
          } catch (cause) {
            return { received, error: GrpcStatusError.fromConnectError(cause) };
          }
          throw new Error("Expected server-streaming handler failure");
        });
      }),
    );

    expect(result.received).toEqual([{ sequence: 1 }]);
    expect(result.error).toMatchObject({
      code: "unavailable",
      message: "stream broke",
    });
  });

  it("maps server-streaming response converter failures to internal", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const entry = {
          ...serverStreamingEntry,
          toGrpcResponse: () => {
            throw new Error("bad stream response");
          },
        } satisfies GrpcMethodEntry;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          handlers: handlers(entry.tag, {
            kind: "server-streaming",
            handler: () => Stream.make({ ok: true }),
          }),
        });
        const implementation = captureServerStreamingImplementation(routes);

        return yield* Effect.promise(async () => {
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
      }),
    );

    expect(error).toMatchObject({
      code: "internal",
      message: "Invalid gRPC response payload",
    });
  });

  it("interrupts the server-streaming handler when the call is aborted", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const interrupted = yield* Deferred.make<boolean>();
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[serverStreamingEntry.tag, serverStreamingEntry]]),
          handlers: handlers(serverStreamingEntry.tag, {
            kind: "server-streaming",
            handler: () =>
              Stream.make({ sequence: 1 }).pipe(
                Stream.concat(
                  Stream.fromEffect(
                    Effect.never.pipe(
                      Effect.onInterrupt(() =>
                        Deferred.succeed(interrupted, true).pipe(Effect.asVoid),
                      ),
                    ),
                  ),
                ),
              ),
          }),
        });
        const implementation = captureServerStreamingImplementation(routes);
        const abort = new AbortController();

        const received = yield* Effect.promise(async () => {
          const responses = implementation.watch(
            {},
            handlerContext({ signal: abort.signal }),
          );
          const iterator = responses[Symbol.asyncIterator]();
          const first = await iterator.next();
          // Leave a pull pending on the never-ending handler, then abort the
          // call the way connect-node surfaces a client cancellation. The
          // pending pull must settle and the handler fiber be interrupted.
          const pending = iterator.next();
          await new Promise((resolve) => setTimeout(resolve, 10));
          abort.abort();
          const end = await pending;
          return { first: first.value, endDone: end.done };
        });
        const handlerInterrupted = yield* Deferred.await(interrupted);
        return { received, handlerInterrupted };
      }),
    );

    expect(result.received).toEqual({ first: { sequence: 1 }, endDone: true });
    expect(result.handlerInterrupted).toBe(true);
  });
});

describe("GrpcServerProtocol streaming bridge", () => {
  it("bridges client-streaming requests to the handler", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[clientStreamingEntry.tag, clientStreamingEntry]]),
          handlers: handlers(clientStreamingEntry.tag, {
            kind: "client-streaming",
            handler: (requests) =>
              Stream.runCollect(requests).pipe(
                Effect.map((items) => ({ items })),
              ),
          }),
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
    );

    expect(result).toEqual({ items: [{ id: "1" }, { id: "2" }] });
  });

  // Regression pin for the request-stream teardown hang: connect's request
  // iterable queues a `return()` issued while a `next()` is pending until
  // that pull settles. A handler that stops consuming mid-pull (here via
  // `Effect.timeoutOrElse`) while the client is connected but idle must still
  // complete — before the fix the call never settled and the server could not
  // enforce its own timeout.
  it("lets a handler abandon the request stream mid-pull while the client is idle", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[clientStreamingEntry.tag, clientStreamingEntry]]),
          handlers: handlers(clientStreamingEntry.tag, {
            kind: "client-streaming",
            handler: (requests) =>
              Stream.runDrain(requests).pipe(
                Effect.as({ drained: true }),
                Effect.timeoutOrElse({
                  duration: 50,
                  orElse: () => Effect.succeed({ timedOut: true }),
                }),
              ),
          }),
        });
        const implementation = captureUnaryImplementation(routes);

        // connect's strict-queueing semantics for an idle client: the first
        // message arrives, the next pull stays pending forever, and a
        // return() issued behind it never settles.
        const idleRequests: AsyncIterable<unknown> = {
          [Symbol.asyncIterator]: () => {
            let first = true;
            return {
              next: () => {
                if (first) {
                  first = false;
                  return Promise.resolve({ done: false, value: { id: "1" } });
                }
                return new Promise<IteratorResult<unknown>>(() => {});
              },
              return: () => new Promise<IteratorResult<unknown>>(() => {}),
            };
          },
        };

        return yield* Effect.promise(() =>
          implementation.upload(idleRequests as never, handlerContext()),
        );
      }),
    );

    expect(result).toEqual({ timedOut: true });
  });

  it("bridges bidi streams and maps mid-stream handler failures", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[bidiStreamingEntry.tag, bidiStreamingEntry]]),
          handlers: handlers(bidiStreamingEntry.tag, {
            kind: "bidi-streaming",
            handler: (requests) =>
              Stream.mapEffect(requests, (request) =>
                (request as { readonly id: string }).id === "boom"
                  ? Effect.fail(GrpcStatusError.notFound("boom"))
                  : Effect.succeed(request),
              ),
          }),
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
    );

    expect(result.received).toEqual([{ id: "1" }]);
    expect(result.error).toMatchObject({
      code: "not_found",
      message: "boom",
    });
  });

  it("rejects streaming methods without a registered handler as unimplemented", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[clientStreamingEntry.tag, clientStreamingEntry]]),
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
    );

    expect(error).toMatchObject({
      code: "unimplemented",
      message: "Missing handler for demo.v1.TestService/Upload",
    });
  });

  it("maps invalid streamed request payloads to invalid_argument", async () => {
    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const entry = {
          ...clientStreamingEntry,
          payloadSchema: Schema.Struct({ id: Schema.String }),
        } satisfies GrpcMethodEntry;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          handlers: handlers(entry.tag, {
            kind: "client-streaming",
            handler: (requests) => Stream.runDrain(requests),
          }),
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
    );

    expect(error).toMatchObject({
      code: "invalid_argument",
      message: "Invalid gRPC request payload",
    });
  });
});

const handlers = (
  tag: string,
  handler: GrpcServerProtocol.GrpcHandler,
): GrpcServerProtocol.GrpcHandlers => new Map([[tag, handler]]);

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

const handlerContext = (options?: {
  readonly headers?: Headers;
  readonly signal?: AbortSignal;
}): HandlerContext =>
  ({
    requestHeader: options?.headers ?? new Headers(),
    signal: options?.signal ?? new AbortController().signal,
  }) as HandlerContext;
