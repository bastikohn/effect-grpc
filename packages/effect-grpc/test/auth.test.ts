import type { Interceptor } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import { Deferred, Effect, Layer, Ref } from "effect";
import { TestClock } from "effect/testing";

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
    assert.deepStrictEqual(GrpcAuth.bearerMetadata("t1"), [
      ["authorization", "Bearer t1"],
    ]);
  });
});

describe("bearerInterceptor", () => {
  it.effect(
    "reads the BearerToken service per request and lets per-call win",
    () =>
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

describe("staticTokenLayer", () => {
  it.effect("always yields the fixed token", () =>
    Effect.gen(function* () {
      const service = yield* GrpcAuth.BearerToken;
      const token = yield* service.read;

      assert.strictEqual(token, "fixed");
    }).pipe(Effect.provide(GrpcAuth.staticTokenLayer("fixed"))),
  );
});

describe("refreshingTokenLayer", () => {
  const readToken = Effect.gen(function* () {
    const service = yield* GrpcAuth.BearerToken;
    return yield* service.read;
  });

  // The daemon sleeps on the TestClock, so every refresh cycle is driven by
  // `TestClock.adjust(interval)`: no polling, no racing the daemon.
  const interval = "10 millis";

  it.effect("acquires once and re-mints on the interval", () =>
    Effect.gen(function* () {
      const context = yield* Layer.build(
        GrpcAuth.refreshingTokenLayer({
          acquire: Effect.succeed("initial"),
          refresh: (current) => Effect.succeed(`${current}+`),
          interval,
        }),
      );

      const first = yield* readToken.pipe(Effect.provideContext(context));
      yield* TestClock.adjust(interval);
      const rotated = yield* readToken.pipe(Effect.provideContext(context));

      assert.deepStrictEqual(
        { first, rotated },
        { first: "initial", rotated: "initial+" },
      );
    }),
  );

  it.effect(
    "keeps the previous token and the daemon alive when a refresh fails",
    () =>
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
            interval,
          }),
        );

        // The failed first cycle must leave the initial token in place.
        yield* TestClock.adjust(interval);
        yield* Deferred.await(failedOnce);
        const afterFailure = yield* readToken.pipe(
          Effect.provideContext(context),
        );
        assert.strictEqual(afterFailure, "v1");

        // The next cycle succeeds; the daemon survived the failure.
        yield* TestClock.adjust(interval);
        const rotated = yield* readToken.pipe(Effect.provideContext(context));
        assert.strictEqual(rotated, "v2");
      }),
  );
});
