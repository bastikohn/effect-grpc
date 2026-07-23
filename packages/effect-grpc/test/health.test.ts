import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Context, Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

/** Wire values of `HealthCheckResponse.ServingStatus`. */
const SERVING = 1;
const NOT_SERVING = 2;
const SERVICE_UNKNOWN = 3;

describe("GrpcHealth service", () => {
  it("marks the overall server as serving by default", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make();
        const overall = yield* health.check();
        const snapshot = yield* health.statuses;
        return { overall, snapshot };
      }),
    );

    expect(result.overall).toBe("SERVING");
    expect(result.snapshot).toEqual(new Map([["", "SERVING"]]));
  });

  it("unregisters services on clear", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make();
        yield* health.set("demo.v1.UserService", "SERVING");
        yield* health.clear("demo.v1.UserService");
        const error = yield* Effect.flip(health.check("demo.v1.UserService"));
        const watched = yield* Stream.runCollect(
          Stream.take(health.watch("demo.v1.UserService"), 1),
        );
        return { error, watched };
      }),
    );

    expect(result.error).toMatchObject({
      code: "not_found",
      message: "unknown service: demo.v1.UserService",
    });
    expect(result.watched).toEqual(["SERVICE_UNKNOWN"]);
  });
});

describe("grpc.health.v1.Health over the server protocol", () => {
  it("Check returns the status of a serving service", async () => {
    const response = await withHealthServer((harness) =>
      Effect.gen(function* () {
        yield* harness.health.set("demo.v1.UserService", "SERVING");
        return yield* Effect.promise(() =>
          harness.check({ service: "demo.v1.UserService" }),
        );
      }),
    );

    expect(response).toEqual({ status: SERVING });
  });

  it("Check with the empty service name reports overall server health", async () => {
    const result = await withHealthServer((harness) =>
      Effect.gen(function* () {
        const initial = yield* Effect.promise(() =>
          harness.check({ service: "" }),
        );
        yield* harness.health.set("", "NOT_SERVING");
        const drained = yield* Effect.promise(() =>
          harness.check({ service: "" }),
        );
        return { initial, drained };
      }),
    );

    expect(result.initial).toEqual({ status: SERVING });
    expect(result.drained).toEqual({ status: NOT_SERVING });
  });

  it("Check fails with not_found for unknown services", async () => {
    const error = await withHealthServer((harness) =>
      Effect.promise(async () => {
        try {
          await harness.check({ service: "demo.v1.Missing" });
        } catch (cause) {
          return GrpcStatusError.fromConnectError(cause);
        }
        throw new Error("Expected Check to fail for an unknown service");
      }),
    );

    expect(error).toMatchObject({
      code: "not_found",
      message: "unknown service: demo.v1.Missing",
    });
  });

  it("Watch emits the current status immediately", async () => {
    const result = await withHealthServer((harness) =>
      Effect.gen(function* () {
        yield* harness.health.set("demo.v1.UserService", "SERVING");
        const known = watchIterator(harness, "demo.v1.UserService");
        const unknown = watchIterator(harness, "demo.v1.Missing");
        try {
          return {
            known: yield* Effect.promise(() => known.next()),
            unknown: yield* Effect.promise(() => unknown.next()),
          };
        } finally {
          yield* closeIterator(known);
          yield* closeIterator(unknown);
        }
      }),
    );

    expect(result.known.value).toEqual({ status: SERVING });
    expect(result.unknown.value).toEqual({ status: SERVICE_UNKNOWN });
  });

  it("Watch streams status changes and suppresses duplicates", async () => {
    const received = await withHealthServer((harness) =>
      Effect.gen(function* () {
        yield* harness.health.set("demo.v1.UserService", "SERVING");
        const iterator = watchIterator(harness, "demo.v1.UserService");
        try {
          const first = yield* Effect.promise(() => iterator.next());
          // A no-op update must not produce an element; the next pull only
          // resolves with the real change that follows it.
          yield* harness.health.set("demo.v1.UserService", "SERVING");
          yield* harness.health.set("demo.v1.UserService", "NOT_SERVING");
          const second = yield* Effect.promise(() => iterator.next());
          return [first.value, second.value];
        } finally {
          yield* closeIterator(iterator);
        }
      }),
    );

    expect(received).toEqual([{ status: SERVING }, { status: NOT_SERVING }]);
  });
});

interface HealthHarness {
  readonly health: GrpcHealth.GrpcHealthService;
  readonly check: (request: unknown) => Promise<unknown>;
  readonly watch: (request: unknown) => AsyncIterable<unknown>;
}

/**
 * Runs the real `Health` handlers behind the server protocol: handlers layer,
 * handlers map, and connect route implementation, without a TCP listener.
 */
const withHealthServer = <A>(
  test: (harness: HealthHarness) => Effect.Effect<A>,
  options?: GrpcHealth.GrpcHealthOptions,
): Promise<A> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const context = yield* Layer.build(
          GrpcHealth.HealthHandlersLayer.pipe(
            Layer.provideMerge(GrpcHealth.layer(options)),
          ),
        );
        const { routes } = yield* GrpcServerProtocol.make({
          registry: GrpcHealth.HealthGrpcRegistry,
          handlers: Context.get(context, GrpcServerProtocol.GrpcHandlers),
        });

        const implementation = captureImplementation(routes);
        return yield* test({
          health: Context.get(context, GrpcHealth.GrpcHealth),
          check: (request) =>
            implementation["check"]!(
              request,
              handlerContext(),
            ) as Promise<unknown>,
          watch: (request) =>
            implementation["watch"]!(
              request,
              handlerContext(),
            ) as AsyncIterable<unknown>,
        });
      }),
    ),
  );

const watchIterator = (
  harness: HealthHarness,
  service: string,
): AsyncIterator<unknown> => harness.watch({ service })[Symbol.asyncIterator]();

const closeIterator = (iterator: AsyncIterator<unknown>) =>
  Effect.promise(async () => {
    await iterator.return?.(undefined as never);
  });

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
