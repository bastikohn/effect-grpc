import { Effect, Stream } from "effect";
import { describe, expect, it, vi } from "vitest";

import type { GrpcCallOptions } from "../src/CodegenSupport.js";
import * as GrpcDeadline from "../src/GrpcDeadline.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

describe("GrpcDeadline.callOptions", () => {
  it("reads lazily and rereads the same effect on every execution", () => {
    const remainingTimeoutMs = vi
      .fn()
      .mockReturnValueOnce(100)
      .mockReturnValueOnce(25);
    const options = GrpcDeadline.callOptions({ remainingTimeoutMs });
    expect(remainingTimeoutMs).not.toHaveBeenCalled();
    expect(Effect.runSync(options).timeoutMs).toBe(100);
    expect(Effect.runSync(options).timeoutMs).toBe(25);
    expect(remainingTimeoutMs).toHaveBeenCalledTimes(2);
  });

  it.each([undefined, 0, -1, 10, 100, 200])(
    "bounds caller timeout %s and preserves metadata without mutating options",
    (timeoutMs) => {
      const options: GrpcCallOptions = {
        ...(timeoutMs === undefined ? {} : { timeoutMs }),
        metadata: [["x-request", "value"]],
      };
      const bounded = Effect.runSync(
        GrpcDeadline.callOptions({ remainingTimeoutMs: () => 100 }, options),
      );
      expect(bounded.timeoutMs).toBe(timeoutMs === 10 ? 10 : 100);
      expect(bounded.metadata).toBe(options.metadata);
      expect(options.timeoutMs).toBe(timeoutMs);
    },
  );

  it.each([undefined, 0, -1, 50])(
    "preserves caller timeout %s when upstream has no deadline",
    (timeoutMs) => {
      const options: GrpcCallOptions =
        timeoutMs === undefined ? {} : { timeoutMs };
      expect(
        Effect.runSync(
          GrpcDeadline.callOptions(
            { remainingTimeoutMs: () => undefined },
            options,
          ),
        ),
      ).toBe(options);
    },
  );

  it.each([0, -1])(
    "fails with a typed deadline error at %s before invoking downstream work",
    (remaining) => {
      const downstream = vi.fn(() => Effect.succeed("response"));
      const error = Effect.runSync(
        GrpcDeadline.callOptions({ remainingTimeoutMs: () => remaining }).pipe(
          Effect.flatMap(downstream),
          Effect.flip,
        ),
      );
      expect(error).toBeInstanceOf(GrpcStatusError.GrpcStatusError);
      expect(error._tag).toBe("GrpcStatusError");
      expect(error.code).toBe("deadline_exceeded");
      expect(downstream).not.toHaveBeenCalled();
    },
  );

  it("deducts time spent in setup before constructing the downstream call", async () => {
    let now = 0;
    const deadline = 100;
    const options = GrpcDeadline.callOptions({
      remainingTimeoutMs: () => Math.max(0, deadline - now),
    });
    const downstream = vi.fn((callOptions: GrpcCallOptions) =>
      Effect.succeed(callOptions.timeoutMs),
    );
    const result = await Effect.runPromise(
      Effect.promise(async () => {
        // A controlled server clock makes elapsed asynchronous setup exact.
        await Promise.resolve();
        now += 40;
      }).pipe(Effect.andThen(options), Effect.flatMap(downstream)),
    );
    expect(result).toBe(60);
    expect(downstream).toHaveBeenCalledTimes(1);
  });

  it("reads at stream subscription and rejects expired subscriptions before construction", async () => {
    let remaining = 100;
    const downstream = vi.fn((options: GrpcCallOptions) =>
      Stream.succeed(options.timeoutMs),
    );
    const stream = Stream.unwrap(
      GrpcDeadline.callOptions({
        remainingTimeoutMs: () => remaining,
      }).pipe(Effect.map(downstream)),
    );
    expect(downstream).not.toHaveBeenCalled();
    remaining = 30;
    expect(await Effect.runPromise(Stream.runCollect(stream))).toEqual([30]);
    remaining = 0;
    const error = await Effect.runPromise(
      Stream.runCollect(stream).pipe(Effect.flip),
    );
    expect(error.code).toBe("deadline_exceeded");
    expect(downstream).toHaveBeenCalledTimes(1);
  });
});
