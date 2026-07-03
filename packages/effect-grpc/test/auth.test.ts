import type { Interceptor } from "@connectrpc/connect";
import { Deferred, Effect, Layer, Ref } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcAuth from "../src/GrpcAuth.js";

/** Runs `interceptor` against a pass-through `next`, returns `authorization`. */
const invoke = (interceptor: Interceptor, header: Headers) =>
  Effect.promise(async () => {
    const next = ((req: { header: Headers }) =>
      Promise.resolve(req)) as unknown as Parameters<Interceptor>[0];
    await interceptor(next)({ header } as never);
    return header.get("authorization");
  });

describe("bearerMetadata", () => {
  it("maps a token to an authorization header", () => {
    expect(GrpcAuth.bearerMetadata("t1")).toEqual([
      ["authorization", "Bearer t1"],
    ]);
  });
});

describe("bearerInterceptor", () => {
  it("reads the BearerToken service per request and lets per-call win", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const token = yield* Ref.make("t1");
        const interceptor = yield* GrpcAuth.bearerInterceptor.pipe(
          Effect.provideService(GrpcAuth.BearerToken, {
            read: Ref.get(token),
          }),
        );

        const fresh = yield* invoke(interceptor, new Headers());
        const perCall = yield* invoke(
          interceptor,
          new Headers({ authorization: "Bearer explicit" }),
        );
        yield* Ref.set(token, "t2");
        const rotated = yield* invoke(interceptor, new Headers());
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

describe("staticTokenLayer", () => {
  it("always yields the fixed token", async () => {
    const token = await Effect.runPromise(
      Effect.gen(function* () {
        const service = yield* GrpcAuth.BearerToken;
        return yield* service.read;
      }).pipe(Effect.provide(GrpcAuth.staticTokenLayer("fixed"))),
    );

    expect(token).toBe("fixed");
  });
});

describe("refreshingTokenLayer", () => {
  const readToken = Effect.gen(function* () {
    const service = yield* GrpcAuth.BearerToken;
    return yield* service.read;
  });

  /** Polls until the token matches, so the test never races the daemon. */
  const awaitToken = (
    expected: string,
  ): Effect.Effect<string, never, GrpcAuth.BearerToken> =>
    readToken.pipe(
      Effect.flatMap((token) =>
        token === expected
          ? Effect.succeed(token)
          : Effect.sleep("5 millis").pipe(
              Effect.andThen(() => awaitToken(expected)),
            ),
      ),
    );

  it("acquires once and re-mints on the interval", async () => {
    const result = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const context = yield* Layer.build(
            GrpcAuth.refreshingTokenLayer({
              acquire: Effect.succeed("initial"),
              refresh: (current) => Effect.succeed(`${current}+`),
              interval: "10 millis",
            }),
          );

          const first = yield* readToken.pipe(Effect.provideContext(context));
          const rotated = yield* awaitToken("initial+").pipe(
            Effect.provideContext(context),
          );
          return { first, rotated };
        }),
      ),
    );

    expect(result).toEqual({ first: "initial", rotated: "initial+" });
  });

  it("keeps the previous token and the daemon alive when a refresh fails", async () => {
    const rotated = await Effect.runPromise(
      Effect.scoped(
        Effect.gen(function* () {
          const failedOnce = yield* Deferred.make<void>();
          const calls = yield* Ref.make(0);
          const context = yield* Layer.build(
            GrpcAuth.refreshingTokenLayer({
              acquire: Effect.succeed("v1"),
              refresh: () =>
                Ref.updateAndGet(calls, (n) => n + 1).pipe(
                  Effect.flatMap((attempt) =>
                    attempt === 1
                      ? Deferred.succeed(failedOnce, undefined).pipe(
                          Effect.andThen(Effect.fail(new Error("transient"))),
                        )
                      : Effect.succeed("v2"),
                  ),
                ),
              interval: "10 millis",
            }),
          );

          // The failed first cycle must leave the initial token in place.
          yield* Deferred.await(failedOnce);
          const afterFailure = yield* readToken.pipe(
            Effect.provideContext(context),
          );
          expect(afterFailure).toBe("v1");

          // The next cycle succeeds; the daemon survived the failure.
          return yield* awaitToken("v2").pipe(Effect.provideContext(context));
        }),
      ),
    );

    expect(rotated).toBe("v2");
  });
});
