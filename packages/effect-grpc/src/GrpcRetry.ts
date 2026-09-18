import { Clock, Effect, Random, Result } from "effect";

import type { GrpcCallOptions, GrpcServerContext } from "./CodegenSupport.js";
import * as GrpcDeadline from "./GrpcDeadline.js";
import type { GrpcErrorStatusCode } from "./GrpcStatusCode.js";
import * as GrpcStatusError from "./GrpcStatusError.js";

export interface UnaryRetryOptions {
  /** Application promise: replaying this unary operation is safe. */
  readonly retrySafe: true;
  /** Maximum total attempts, including the first; a positive integer. */
  readonly maxAttempts: number;
  readonly retryableCodes: ReadonlyArray<GrpcErrorStatusCode>;
  /** Options sampled when the effect runs; timeoutMs is one overall budget. */
  readonly callOptions?: GrpcCallOptions;
  readonly context?: Pick<GrpcServerContext, "remainingTimeoutMs">;
  /** Exponential backoff starts here. Default 100 milliseconds. */
  readonly initialDelayMs?: number;
  /** Backoff cap. Default 1000 milliseconds. */
  readonly maxDelayMs?: number;
  /** Full jitter in [0, capped backoff). Default true. */
  readonly jitter?: boolean;
}

/**
 * Explicit opt-in for retry-safe unary effects. Pass the provided options to
 * the generated unary method; put asynchronous per-attempt setup inside the
 * returned effect. Only selected typed statuses retry: defects and interruption
 * pass through. Attempts and backoff share one deadline and remain cancellable.
 */
export const unary = <A, R>(
  invoke: (
    options: GrpcCallOptions,
  ) => Effect.Effect<A, GrpcStatusError.GrpcStatusError, R>,
  options: UnaryRetryOptions,
): Effect.Effect<A, GrpcStatusError.GrpcStatusError, R> =>
  Effect.gen(function* () {
    const {
      retrySafe,
      maxAttempts,
      retryableCodes,
      context = { remainingTimeoutMs: () => undefined },
      initialDelayMs = 100,
      maxDelayMs = 1000,
      jitter = true,
    } = options;
    if (
      retrySafe !== true ||
      !Number.isSafeInteger(maxAttempts) ||
      maxAttempts < 1 ||
      !Number.isFinite(initialDelayMs) ||
      initialDelayMs < 0 ||
      !Number.isFinite(maxDelayMs) ||
      maxDelayMs < 0
    ) {
      return yield* Effect.die(new RangeError("Invalid unary retry policy"));
    }
    const started = yield* Clock.currentTimeMillis;
    const initial = yield* GrpcDeadline.callOptions(context, {
      ...options.callOptions,
    });
    const deadline =
      initial.timeoutMs !== undefined && initial.timeoutMs > 0
        ? started + initial.timeoutMs
        : undefined;
    const remainingOptions = Effect.gen(function* () {
      const remaining =
        deadline === undefined
          ? undefined
          : deadline - (yield* Clock.currentTimeMillis);
      if (remaining !== undefined && remaining <= 0)
        return yield* Effect.fail(expired());
      return yield* GrpcDeadline.callOptions(context, {
        ...initial,
        timeoutMs: remaining ?? initial.timeoutMs,
      });
    });
    const run = Effect.gen(function* () {
      let backoff = Math.min(maxDelayMs, initialDelayMs);
      for (let attempt = 1; ; attempt += 1) {
        const callOptions = yield* remainingOptions;
        const result = yield* Effect.result(
          withBudget(
            Effect.suspend(() => invoke(callOptions)),
            callOptions.timeoutMs,
          ),
        );
        if (Result.isSuccess(result)) return result.success;
        if (
          attempt === maxAttempts ||
          !retryableCodes.includes(result.failure.code)
        ) {
          return yield* Effect.fail(result.failure);
        }
        const remaining = yield* remainingOptions;
        const delay = jitter ? backoff * (yield* Random.next) : backoff;
        yield* withBudget(Effect.sleep(delay), remaining.timeoutMs);
        backoff = Math.min(maxDelayMs, backoff * 2);
      }
    });
    return yield* withBudget(run, initial.timeoutMs);
  });

const expired = () =>
  GrpcStatusError.deadlineExceeded("Unary retry deadline exhausted");

const withBudget = <A, E, R>(
  effect: Effect.Effect<A, E, R>,
  timeoutMs: number | undefined,
) =>
  timeoutMs === undefined || !(timeoutMs > 0)
    ? effect
    : Effect.timeoutOrElse(effect, {
        duration: timeoutMs,
        orElse: () => Effect.fail(expired()),
      });
