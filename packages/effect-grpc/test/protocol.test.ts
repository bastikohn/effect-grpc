import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import { Context, Deferred, Effect, Layer, Ref, Schema, Stream } from "effect";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcNodeServer from "../src/GrpcNodeServer.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import type { ServiceImplementation } from "./support/serverHarness.js";
import {
  captureImplementation,
  freePort,
  handlerContext,
  methodEntries,
} from "./support/serverHarness.js";

describe("metadataInterceptor", () => {
  it.effect(
    "adds metadata as defaults, lets per-call win, and re-reads per call",
    () =>
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

        assert.deepStrictEqual(
          { fresh, perCall, rotated },
          {
            fresh: "Bearer t1",
            perCall: "Bearer explicit",
            rotated: "Bearer t2",
          },
        );
      }),
  );
});

describe("GrpcServerProtocol", () => {
  it.effect("completes unary calls through the handlers map", () =>
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

      const result = yield* Effect.promise(() =>
        implementation.get(
          { id: "1" },
          handlerContext({ headers: new Headers({ "x-demo": "42" }) }),
        ),
      );

      assert.deepStrictEqual(result, {
        echoed: { id: "1" },
        metadata: [["x-demo", "42"]],
      });
    }),
  );

  it.effect("maps unary handler failures to connect errors", () =>
    Effect.gen(function* () {
      const { routes } = yield* GrpcServerProtocol.make({
        registry: new Map([[unaryEntry.tag, unaryEntry]]),
        handlers: handlers(unaryEntry.tag, {
          kind: "unary",
          handler: () => Effect.fail(GrpcStatusError.notFound("missing")),
        }),
      });
      const implementation = captureUnaryImplementation(routes);

      const error = yield* Effect.flip(
        Effect.tryPromise({
          try: () => implementation.get({}, handlerContext()),
          catch: GrpcStatusError.fromConnectError,
        }),
      );

      assert.strictEqual(error.code, "not_found");
      assert.strictEqual(error.message, "missing");
    }),
  );

  // Every `handlers.get(tag)` miss — no handler at all, or one registered
  // under a different kind — is the same `unimplemented` status on every call
  // shape. The wrong-kind row is the only coverage of the kind guard.
  it.effect.each([
    ["a unary", unaryEntry, undefined],
    ["a server-streaming", serverStreamingEntry, undefined],
    ["a client-streaming", clientStreamingEntry, undefined],
    [
      "a wrong-kind",
      unaryEntry,
      {
        kind: "server-streaming",
        handler: () => Stream.empty,
      } satisfies GrpcServerProtocol.GrpcHandler,
    ],
  ] as ReadonlyArray<
    readonly [
      string,
      GrpcMethodEntry,
      GrpcServerProtocol.GrpcHandler | undefined,
    ]
  >)(
    "rejects %s method without a handler as unimplemented",
    ([_shape, entry, handler]) =>
      Effect.gen(function* () {
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[entry.tag, entry]]),
          // The wrong-kind row registers a handler under the unary tag with a
          // streaming shape: the guard must fast-fail instead of invoking it.
          handlers: handler ? handlers(entry.tag, handler) : undefined,
        });

        const error = yield* failureOf(captureImplementation(routes), entry);

        assert.deepStrictEqual(
          { code: error.code, message: error.message },
          {
            code: "unimplemented",
            message: `Missing handler for ${entry.tag}`,
          },
        );
      }),
  );

  // The registry's error policy as connect sees it: a request payload the
  // codec rejects is the caller's fault, a response the converter cannot
  // produce is ours. One row per execution template (effect and stream), the
  // codes themselves being asserted per direction in `methodRegistry.test.ts`.
  it.effect.each([
    [
      "an unconvertible request payload",
      {
        ...unaryEntry,
        payloadSchema: Schema.Struct({ id: Schema.String }),
        fromGrpcRequest: () => ({ id: 123 }),
      } satisfies GrpcMethodEntry,
      {
        kind: "unary",
        handler: (request) => Effect.succeed(request),
      } satisfies GrpcServerProtocol.GrpcHandler,
      { code: "invalid_argument", message: "Invalid gRPC request payload" },
    ],
    [
      "a throwing streamed response converter",
      {
        ...serverStreamingEntry,
        toGrpcResponse: () => {
          throw new Error("bad stream response");
        },
      } satisfies GrpcMethodEntry,
      {
        kind: "server-streaming",
        handler: () => Stream.make({ ok: true }),
      } satisfies GrpcServerProtocol.GrpcHandler,
      { code: "internal", message: "Invalid gRPC response payload" },
    ],
  ] as ReadonlyArray<
    readonly [
      string,
      GrpcMethodEntry,
      GrpcServerProtocol.GrpcHandler,
      { readonly code: string; readonly message: string },
    ]
  >)("fails %s with a typed status", ([_name, entry, handler, expected]) =>
    Effect.gen(function* () {
      const { routes } = yield* GrpcServerProtocol.make({
        registry: new Map([[entry.tag, entry]]),
        handlers: handlers(entry.tag, handler),
      });

      const error = yield* failureOf(captureImplementation(routes), entry);

      assert.deepStrictEqual(
        { code: error.code, message: error.message },
        expected,
      );
    }),
  );

  it.effect("streams server-streaming responses and completes", () =>
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

      const received = yield* Effect.promise(async () => {
        const values: Array<unknown> = [];
        for await (const value of implementation.watch(
          { id: "7" },
          handlerContext(),
        )) {
          values.push(value);
        }
        return values;
      });

      assert.deepStrictEqual(received, [
        { id: "7", sequence: 1 },
        { id: "7", sequence: 2 },
      ]);
    }),
  );

  it.effect("maps server-streaming handler failures mid-stream", () =>
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

      const received: Array<unknown> = [];
      const error = yield* Effect.flip(
        Effect.tryPromise({
          try: async () => {
            for await (const value of implementation.watch(
              {},
              handlerContext(),
            )) {
              received.push(value);
            }
          },
          catch: GrpcStatusError.fromConnectError,
        }),
      );

      assert.deepStrictEqual(received, [{ sequence: 1 }]);
      assert.strictEqual(error.code, "unavailable");
      assert.strictEqual(error.message, "stream broke");
    }),
  );

  // Real time: the abort is issued behind a pending pull after a
  // `setTimeout`-backed pause, which the TestClock cannot advance.
  it.live(
    "interrupts the server-streaming handler when the call is aborted",
    () =>
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

        assert.deepStrictEqual(received, {
          first: { sequence: 1 },
          endDone: true,
        });
        assert.isTrue(handlerInterrupted);
      }),
  );
});

describe("GrpcServerProtocol streaming bridge", () => {
  it.effect("bridges client-streaming requests to the handler", () =>
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

      const result = yield* Effect.promise(() =>
        implementation.upload(
          (async function* () {
            yield { id: "1" };
            yield { id: "2" };
          })() as never,
          handlerContext(),
        ),
      );

      assert.deepStrictEqual(result, { items: [{ id: "1" }, { id: "2" }] });
    }),
  );

  // Regression pin for the request-stream teardown hang: connect's request
  // iterable queues a `return()` issued while a `next()` is pending until
  // that pull settles. A handler that stops consuming mid-pull (here via
  // `Effect.timeoutOrElse`) while the client is connected but idle must still
  // complete — before the fix the call never settled and the server could not
  // enforce its own timeout.
  //
  // Real time: the handler's timeout runs inside the context captured by
  // `GrpcServerProtocol.make`, against a client that never wakes up.
  it.live(
    "lets a handler abandon the request stream mid-pull while the client is idle",
    () =>
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

        const result = yield* Effect.promise(() =>
          implementation.upload(idleRequests as never, handlerContext()),
        );

        assert.deepStrictEqual(result, { timedOut: true });
      }),
  );

  it.effect("bridges bidi streams and maps mid-stream handler failures", () =>
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

      const received: Array<unknown> = [];
      const error = yield* Effect.flip(
        Effect.tryPromise({
          try: async () => {
            for await (const value of implementation.chat(
              (async function* () {
                yield { id: "1" };
                yield { id: "boom" };
              })() as never,
              handlerContext(),
            )) {
              received.push(value);
            }
          },
          catch: GrpcStatusError.fromConnectError,
        }),
      );

      assert.deepStrictEqual(received, [{ id: "1" }]);
      assert.strictEqual(error.code, "not_found");
      assert.strictEqual(error.message, "boom");
    }),
  );

  it.effect("maps invalid streamed request payloads to invalid_argument", () =>
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

      const error = yield* Effect.flip(
        Effect.tryPromise({
          try: () =>
            implementation.upload(
              (async function* () {
                yield { id: 42 };
              })() as never,
              handlerContext(),
            ),
          catch: GrpcStatusError.fromConnectError,
        }),
      );

      assert.strictEqual(error.code, "invalid_argument");
      assert.strictEqual(error.message, "Invalid gRPC request payload");
    }),
  );
});

/**
 * Where handler dependencies are provided decides whether they are still
 * alive when a request arrives. `handlersEffect` completes as soon as it has
 * read the context, so providing to it is *not* the migration from the old
 * `handlersLayer`; providing to the whole server program is.
 */
describe("handlersEffect dependency scoping", () => {
  const CHECK = "grpc.health.v1.Health/Check";

  interface Pool {
    released: boolean;
  }
  const Pool = Context.Service<Pool>("effect-grpc-test/Pool");

  /** A scoped dependency that records its lifecycle in `events`. */
  const poolLayer = (events: Array<string>) =>
    Layer.effect(
      Pool,
      Effect.acquireRelease(
        Effect.sync((): Pool => {
          events.push("acquire");
          return { released: false };
        }),
        (pool) =>
          Effect.sync(() => {
            pool.released = true;
            events.push("release");
          }),
      ),
    );

  /** One unary handler reporting whether the dependency is live at call time. */
  const poolHandlers = (tag: string) =>
    GrpcServerProtocol.handlersEffect({
      [tag]: {
        kind: "unary",
        handler: () =>
          Effect.map(Effect.service(Pool), (pool) => ({
            status: pool.released ? "NOT_SERVING" : "SERVING",
          })),
      },
    });

  // Real time: a real http2 server and a connect transport on loopback.
  it.live(
    "keeps a dependency provided to the whole server program live until shutdown",
    () =>
      Effect.gen(function* () {
        const events: Array<string> = [];

        // The server's scope is closed explicitly here so that its finalizer
        // can be observed below, after the program has shut down.
        const result = yield* Effect.scoped(
          Effect.gen(function* () {
            const port = yield* freePort;
            // The documented wiring: `R` propagates through `serveAll`, and
            // the layer is provided to the server program, whose scope is the
            // server's lifetime.
            yield* Effect.forkScoped(
              GrpcNodeServer.serveAll({
                host: "127.0.0.1",
                port,
                services: [
                  {
                    registry: GrpcHealth.HealthGrpcRegistry,
                    handlers: poolHandlers(CHECK),
                  },
                ],
              }).pipe(Effect.provide(poolLayer(events))),
            );
            yield* Effect.sleep("50 millis");

            const response = yield* GrpcInvoker.GrpcInvoker.pipe(
              Effect.flatMap((invoker) =>
                invoker.unary(CHECK, { service: "" }),
              ),
              Effect.provide(
                GrpcClientProtocol.layer({
                  baseUrl: `http://127.0.0.1:${port}`,
                  registry: GrpcHealth.HealthGrpcRegistry,
                }),
              ),
            );
            return { response, duringCall: [...events] };
          }),
        );

        // The handler ran against a live resource, acquired exactly once.
        assert.deepStrictEqual(result.response, { status: "SERVING" });
        assert.deepStrictEqual(result.duringCall, ["acquire"]);
        // ... and the finalizer ran only once the server's scope closed.
        assert.deepStrictEqual(events, ["acquire", "release"]);
      }),
  );

  // The hazard the changeset and `handlersEffect`'s doc comment warn about,
  // pinned so it cannot silently become the recommended path again.
  it.effect(
    "releases a dependency provided to the handlers effect before the first call",
    () =>
      Effect.gen(function* () {
        const events: Array<string> = [];

        const built = yield* poolHandlers(unaryEntry.tag).pipe(
          Effect.provide(poolLayer(events)),
        );
        const afterBuild = [...events];

        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([[unaryEntry.tag, unaryEntry]]),
          handlers: built,
        });
        const implementation = captureUnaryImplementation(routes);
        const response = yield* Effect.promise(() =>
          implementation.get({}, handlerContext()),
        );

        // `Effect.provide` closed the layer's scope the instant the handlers
        // map was built — before the server was even wired up.
        assert.deepStrictEqual(afterBuild, ["acquire", "release"]);
        // So every request runs against a finalized resource, with no error
        // at the seam to say so.
        assert.deepStrictEqual(response, { status: "NOT_SERVING" });
      }),
  );
});

const handlers = (
  tag: string,
  handler: GrpcServerProtocol.GrpcHandler,
): GrpcServerProtocol.GrpcHandlers => new Map([[tag, handler]]);

/**
 * Calls `entry`'s method on a captured implementation the way connect does —
 * a message for message-request kinds, an async generator for stream-request
 * ones — and returns the gRPC status it fails with. Stream responses are
 * *drained to completion*: the failures under test are raised mid-iteration,
 * so a call that is merely started would pass vacuously. A call that
 * completes instead fails the returned effect with its result.
 */
const failureOf = (
  implementation: ServiceImplementation,
  entry: GrpcMethodEntry,
): Effect.Effect<GrpcStatusError.GrpcStatusError, void> =>
  Effect.flip(
    Effect.tryPromise({
      try: async () => {
        const call = (implementation[entry.localName] as Method)(
          entry.kind === "unary" || entry.kind === "server-streaming"
            ? {}
            : (async function* () {
                yield {};
              })(),
          handlerContext(),
        );
        if (entry.kind === "unary" || entry.kind === "client-streaming") {
          await (call as Promise<unknown>);
        } else {
          for await (const _ of call as AsyncIterable<unknown>) void _;
        }
      },
      catch: GrpcStatusError.fromConnectError,
    }),
  );

type Method = (
  request: unknown,
  context: HandlerContext,
) => Promise<unknown> | AsyncIterable<unknown>;

const {
  unary: unaryEntry,
  serverStreaming: serverStreamingEntry,
  clientStreaming: clientStreamingEntry,
  bidiStreaming: bidiStreamingEntry,
} = methodEntries("demo.v1.TestService");

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
