import { Cause, Clock, Effect, Exit, Fiber, Random } from "effect";
import { TestClock } from "effect/testing";
import { describe, expect, it } from "vitest";

import type { GrpcCallOptions } from "../src/CodegenSupport.js";
import * as GrpcRetry from "../src/GrpcRetry.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

const policy = {
  retrySafe: true,
  maxAttempts: 3,
  retryableCodes: ["unavailable"],
  initialDelayMs: 100,
  maxDelayMs: 200,
  jitter: false,
} as const;

const advance = <A, E>(effect: Effect.Effect<A, E>, milliseconds = 1000) =>
  Effect.runPromise(
    Effect.gen(function* () {
      const fiber = yield* Effect.forkChild(effect);
      yield* TestClock.adjust(milliseconds);
      return yield* Fiber.join(fiber);
    }).pipe(Effect.provide(TestClock.layer())),
  );

describe("GrpcRetry.unary", () => {
  it("shares one budget across attempt setup and capped exponential backoff", async () => {
    const seen: Array<{ time: number; options: GrpcCallOptions }> = [];
    const metadata = [["x-id", "stable"]] as const;
    const result = await advance(
      GrpcRetry.unary(
        (options) =>
          Effect.gen(function* () {
            seen.push({ time: yield* Clock.currentTimeMillis, options });
            yield* Effect.sleep(20);
            return seen.length < 3
              ? yield* Effect.fail(GrpcStatusError.unavailable("retry"))
              : "done";
          }),
        { ...policy, callOptions: { timeoutMs: 500, metadata } },
      ),
    );
    expect(result).toBe("done");
    expect(seen.map(({ time }) => time)).toEqual([0, 120, 340]);
    expect(seen.map(({ options }) => options.timeoutMs)).toEqual([
      500, 380, 160,
    ]);
    expect(seen.every(({ options }) => options.metadata === metadata)).toBe(
      true,
    );
  });

  it("expires during backoff without starting another attempt", async () => {
    let calls = 0;
    const error = await advance(
      GrpcRetry.unary(
        () => {
          calls += 1;
          return Effect.fail(GrpcStatusError.unavailable("retry"));
        },
        { ...policy, maxAttempts: 10, callOptions: { timeoutMs: 150 } },
      ).pipe(Effect.flip),
    );
    expect(error.code).toBe("deadline_exceeded");
    expect(calls).toBe(2);
  });

  it("bounds asynchronous setup even when the callback ignores timeout options", async () => {
    let calls = 0;
    let finalized = 0;
    const error = await advance(
      GrpcRetry.unary(
        () => {
          calls += 1;
          return Effect.never.pipe(
            Effect.ensuring(
              Effect.sync(() => {
                finalized += 1;
              }),
            ),
          );
        },
        { ...policy, callOptions: { timeoutMs: 50 } },
      ).pipe(Effect.flip),
    );
    expect(error.code).toBe("deadline_exceeded");
    expect(calls).toBe(1);
    expect(finalized).toBe(1);
  });

  it("uses live upstream remaining time after external setup and on every retry", async () => {
    const budgets: Array<number | undefined> = [];
    await advance(
      Effect.gen(function* () {
        const clock = yield* Clock.Clock;
        const context = {
          remainingTimeoutMs: () =>
            Math.max(0, 200 - clock.currentTimeMillisUnsafe()),
        };
        yield* Effect.sleep(40);
        return yield* GrpcRetry.unary(
          (options) => {
            budgets.push(options.timeoutMs);
            return budgets.length === 1
              ? Effect.fail(GrpcStatusError.unavailable("retry"))
              : Effect.succeed("ok");
          },
          {
            ...policy,
            initialDelayMs: 20,
            context,
            callOptions: { timeoutMs: 500 },
          },
        );
      }),
    );
    expect(budgets).toEqual([160, 140]);
  });

  it("preserves a tighter caller deadline and rejects exhausted upstream before invocation", async () => {
    let calls = 0;
    const invoke = (options: GrpcCallOptions) => {
      calls += 1;
      return Effect.succeed(options.timeoutMs);
    };
    expect(
      await advance(
        GrpcRetry.unary(invoke, {
          ...policy,
          context: { remainingTimeoutMs: () => 500 },
          callOptions: { timeoutMs: 20 },
        }),
      ),
    ).toBe(20);
    const error = await advance(
      GrpcRetry.unary(invoke, {
        ...policy,
        context: { remainingTimeoutMs: () => 0 },
      }).pipe(Effect.flip),
    );
    expect(error.code).toBe("deadline_exceeded");
    expect(calls).toBe(1);
  });

  it("honors an upstream budget shortened after the first attempt", async () => {
    let calls = 0;
    let upstream = 500;
    const error = await advance(
      GrpcRetry.unary(
        () => {
          calls += 1;
          upstream = 10;
          return Effect.fail(GrpcStatusError.unavailable("retry"));
        },
        { ...policy, context: { remainingTimeoutMs: () => upstream } },
      ).pipe(Effect.flip),
      20,
    );
    expect(error.code).toBe("deadline_exceeded");
    expect(calls).toBe(1);
  });

  it.each(["invalid_argument", "unavailable"] as const)(
    "preserves final %s error identity with bounded attempts",
    async (code) => {
      let calls = 0;
      const error = GrpcStatusError.make({ code, message: "same error" });
      const result = await advance(
        GrpcRetry.unary(() => {
          calls += 1;
          return Effect.fail(error);
        }, policy).pipe(Effect.flip),
      );
      expect(result).toBe(error);
      expect(calls).toBe(code === "unavailable" ? 3 : 1);
    },
  );

  it("only retries the explicitly selected status codes", async () => {
    let calls = 0;
    const result = await advance(
      GrpcRetry.unary(
        () => {
          calls += 1;
          return calls < 3
            ? Effect.fail(
                GrpcStatusError.make({
                  code: "resource_exhausted",
                  message: "busy",
                }),
              )
            : Effect.succeed("ok");
        },
        { ...policy, retryableCodes: ["resource_exhausted"] },
      ),
    );
    expect(result).toBe("ok");
    expect(calls).toBe(3);
  });

  it("samples call options lazily and resets attempt state for each execution", async () => {
    let calls = 0;
    const options = { timeoutMs: 50 };
    const retry = GrpcRetry.unary(
      (current) => {
        calls += 1;
        return Effect.succeed(current.timeoutMs);
      },
      { ...policy, callOptions: options },
    );
    expect(calls).toBe(0);
    options.timeoutMs = 30;
    expect(await advance(retry)).toBe(30);
    options.timeoutMs = 10;
    expect(await advance(retry)).toBe(10);
    expect(calls).toBe(2);
  });

  it.each([undefined, 0, -10, NaN])(
    "preserves an unbounded call budget %s",
    async (timeoutMs) => {
      const seen: Array<number | undefined> = [];
      await advance(
        GrpcRetry.unary(
          (options) => {
            seen.push(options.timeoutMs);
            return seen.length < 2
              ? Effect.fail(GrpcStatusError.unavailable("retry"))
              : Effect.succeed("ok");
          },
          { ...policy, callOptions: { timeoutMs } },
        ),
      );
      expect(seen).toEqual([timeoutMs, timeoutMs]);
    },
  );

  it("allows exactly one attempt when configured", async () => {
    let calls = 0;
    const error = GrpcStatusError.unavailable("final");
    expect(
      await advance(
        GrpcRetry.unary(
          () => {
            calls += 1;
            return Effect.fail(error);
          },
          { ...policy, maxAttempts: 1 },
        ).pipe(Effect.flip),
      ),
    ).toBe(error);
    expect(calls).toBe(1);
  });

  it("does not retry defects", async () => {
    let calls = 0;
    const defect = new Error("bug");
    const exit = await advance(
      Effect.exit(
        GrpcRetry.unary(() => {
          calls += 1;
          throw defect;
        }, policy),
      ),
    );
    expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
    expect(calls).toBe(1);
  });

  it.each(["attempt", "backoff"] as const)(
    "interrupts %s without another invocation",
    async (phase) => {
      let calls = 0;
      let finalized = 0;
      await Effect.runPromise(
        Effect.gen(function* () {
          const retry = GrpcRetry.unary(() => {
            calls += 1;
            return phase === "attempt"
              ? Effect.never.pipe(
                  Effect.ensuring(
                    Effect.sync(() => {
                      finalized += 1;
                    }),
                  ),
                )
              : Effect.fail(GrpcStatusError.unavailable("retry"));
          }, policy);
          const fiber = yield* Effect.forkChild(retry);
          yield* TestClock.adjust(1);
          yield* Fiber.interrupt(fiber);
          yield* TestClock.adjust(1000);
        }).pipe(Effect.provide(TestClock.layer())),
      );
      expect(calls).toBe(1);
      expect(finalized).toBe(phase === "attempt" ? 1 : 0);
    },
  );

  it("uses reproducible full jitter within capped backoff windows", async () => {
    const run = async (seed: string) => {
      const times: Array<number> = [];
      await advance(
        GrpcRetry.unary(
          () =>
            Effect.gen(function* () {
              times.push(yield* Clock.currentTimeMillis);
              return times.length < 5
                ? yield* Effect.fail(GrpcStatusError.unavailable("retry"))
                : "ok";
            }),
          { ...policy, maxAttempts: 5, maxDelayMs: 150, jitter: true },
        ).pipe(Random.withSeed(seed)),
      );
      return times.slice(1).map((time, index) => time - times[index]);
    };
    const delays = await run("retry-seed");
    expect(delays).toEqual(await run("retry-seed"));
    expect(delays).not.toEqual(await run("other-seed"));
    delays.forEach((delay, index) => {
      expect(delay).toBeGreaterThanOrEqual(0);
      expect(delay).toBeLessThan(index === 0 ? 100 : 150);
    });
  });

  it.each([
    { maxAttempts: 0 },
    { maxAttempts: Infinity },
    { maxAttempts: NaN },
    { maxAttempts: 1.5 },
    { maxAttempts: Number.MAX_VALUE },
    { initialDelayMs: -1 },
    { maxDelayMs: Infinity },
  ])(
    "rejects invalid policy %j as a defect before invoking",
    async (invalid) => {
      let calls = 0;
      const exit = await advance(
        Effect.exit(
          GrpcRetry.unary(
            () => {
              calls += 1;
              return Effect.succeed("no");
            },
            { ...policy, ...invalid },
          ),
        ),
      );
      expect(Exit.isFailure(exit) && Cause.hasDies(exit.cause)).toBe(true);
      expect(calls).toBe(0);
    },
  );
});
