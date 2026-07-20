import { Context, Effect, Ref, Stream } from "effect";
import { describe, expect, it } from "vitest";

import * as StreamBridge from "../src/internal/streamBridge.js";

const noop = () => {};

const tick = () => new Promise((resolve) => setTimeout(resolve, 10));

describe("requestPump", () => {
  it("delivers all values and completes cleanly without aborting", async () => {
    let aborted = 0;
    const pump = StreamBridge.requestPump(
      Stream.make(1, 2, 3),
      Context.empty(),
      () => {
        aborted += 1;
      },
    );

    const values: Array<unknown> = [];
    for await (const value of pump.iterable) {
      values.push(value);
    }

    expect(values).toEqual([1, 2, 3]);
    expect(aborted).toBe(0);
    expect(pump.failure()).toBeUndefined();
  });

  it("aborts the call and preserves the original error when the source fails", async () => {
    const boom = new Error("source boom");
    let aborted = 0;
    const pump = StreamBridge.requestPump(
      Stream.make(1).pipe(Stream.concat(Stream.fail(boom))),
      Context.empty(),
      () => {
        aborted += 1;
      },
    );

    const iterator = pump.iterable[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({ done: false, value: 1 });
    await expect(iterator.next()).rejects.toBe(boom);

    expect(aborted).toBe(1);
    expect(pump.failure()).toEqual({ error: boom });
  });

  it("treats connect's iterator throw as cleanup, not a request failure", async () => {
    let finalized = 0;
    let aborted = 0;
    const pump = StreamBridge.requestPump(
      Stream.make(1).pipe(
        Stream.concat(Stream.never),
        Stream.ensuring(
          Effect.sync(() => {
            finalized += 1;
          }),
        ),
      ),
      Context.empty(),
      () => {
        aborted += 1;
      },
    );

    const iterator = pump.iterable[Symbol.asyncIterator]();
    await iterator.next();
    await expect(iterator.throw?.(new Error("connect abort"))).resolves.toEqual(
      { done: true, value: undefined },
    );

    expect(finalized).toBe(1);
    expect(aborted).toBe(0);
    expect(pump.failure()).toBeUndefined();

    await pump.close();
    expect(finalized).toBe(1);
  });

  it("close resolves even when stream cleanup fails", async () => {
    const pump = StreamBridge.requestPump(
      Stream.make(1).pipe(
        Stream.concat(Stream.never),
        Stream.ensuring(Effect.die(new Error("cleanup boom"))),
      ),
      Context.empty(),
      noop,
    );

    const iterator = pump.iterable[Symbol.asyncIterator]();
    await iterator.next();
    await expect(pump.close()).resolves.toBeUndefined();
  });

  it("pulls lazily so a slow consumer applies backpressure", async () => {
    const counter = Effect.runSync(Ref.make(0));
    const source = Stream.fromIterableEffectRepeat(
      Ref.modify(counter, (n) => [[n + 1], n + 1] as const),
    );
    const pump = StreamBridge.requestPump(source, Context.empty(), noop);

    const iterator = pump.iterable[Symbol.asyncIterator]();
    await expect(iterator.next()).resolves.toEqual({ done: false, value: 1 });
    await expect(iterator.next()).resolves.toEqual({ done: false, value: 2 });

    expect(Effect.runSync(Ref.get(counter))).toBe(2);
    await pump.close();
  });
});

describe("responseStream", () => {
  const failingResponses = (cause: unknown): AsyncIterable<unknown> =>
    (async function* () {
      yield "r1";
      throw cause;
    })();

  it("replays the original source failure instead of the wire error", async () => {
    const boom = new Error("source boom");
    const pump = StreamBridge.requestPump(
      Stream.fail(boom),
      Context.empty(),
      noop,
    );
    await pump.iterable[Symbol.asyncIterator]()
      .next()
      .catch(() => undefined);

    const error = await Effect.runPromise(
      Effect.flip(
        Stream.runCollect(
          StreamBridge.responseStream(
            failingResponses(new Error("wire cancelled")),
            pump,
            (cause) => ({ mapped: cause }),
          ),
        ),
      ),
    );

    expect(error).toBe(boom);
  });

  it("maps wire errors when the request stream did not fail", async () => {
    const wire = new Error("wire boom");
    const pump = StreamBridge.requestPump(Stream.empty, Context.empty(), noop);

    const error = await Effect.runPromise(
      Effect.flip(
        Stream.runCollect(
          StreamBridge.responseStream(
            failingResponses(wire),
            pump,
            (cause) => ({
              mapped: cause,
            }),
          ),
        ),
      ),
    );

    expect(error).toEqual({ mapped: wire });
  });
});

describe("requestStream", () => {
  const options = (
    requests: AsyncIterable<unknown>,
    signal: AbortSignal,
  ): Parameters<typeof StreamBridge.requestStream>[0] => ({
    requests,
    signal,
    onError: (cause) => ({ mapped: cause }),
    onCancelled: () => "cancelled",
  });

  it("treats a clean end with a live signal as a half-close", async () => {
    const controller = new AbortController();
    const requests = (async function* () {
      yield 1;
      yield 2;
    })();

    const values = await Effect.runPromise(
      Stream.runCollect(
        StreamBridge.requestStream(options(requests, controller.signal)),
      ),
    );

    expect(values).toEqual([1, 2]);
  });

  it("turns a clean end with an aborted signal into a cancellation", async () => {
    const controller = new AbortController();
    const requests = (async function* () {
      yield 1;
      controller.abort();
    })();

    const error = await Effect.runPromise(
      Effect.flip(
        Stream.runCollect(
          StreamBridge.requestStream(options(requests, controller.signal)),
        ),
      ),
    );

    expect(error).toBe("cancelled");
  });

  it("maps request iterable failures through onError", async () => {
    const boom = new Error("request boom");
    const controller = new AbortController();
    const requests = (async function* () {
      yield 1;
      throw boom;
    })();

    const error = await Effect.runPromise(
      Effect.flip(
        Stream.runCollect(
          StreamBridge.requestStream(options(requests, controller.signal)),
        ),
      ),
    );

    expect(error).toEqual({ mapped: boom });
  });
});

describe("responsePump", () => {
  it("interrupts an in-flight handler pull before close resolves", async () => {
    let resume!: () => void;
    let resolveStarted!: () => void;
    let effects = 0;
    const started = new Promise<void>((resolve) => {
      resolveStarted = resolve;
    });
    const controller = new AbortController();
    const pump = StreamBridge.responsePump(
      Stream.fromEffect(
        Effect.callback<void>((complete) => {
          resume = () => {
            complete(
              Effect.sync(() => {
                effects += 1;
              }),
            );
          };
          resolveStarted();
        }),
      ),
      Context.empty(),
      controller.signal,
    );

    const pending = pump.next();
    await started;
    await pump.close();
    resume();
    await tick();

    await expect(pending).resolves.toEqual({ done: true, value: undefined });
    expect(effects).toBe(0);
  });

  it("settles a pending pull with a clean end and closes the handler once when the signal aborts", async () => {
    let finalized = 0;
    const controller = new AbortController();
    const pump = StreamBridge.responsePump(
      Stream.never.pipe(
        Stream.ensuring(
          Effect.sync(() => {
            finalized += 1;
          }),
        ),
      ),
      Context.empty(),
      controller.signal,
    );

    const pending = pump.next();
    await tick();
    controller.abort();

    // A clean `{ done: true }` — not a rejection — so connect sees a normal
    // end of stream rather than a thrown generator.
    await expect(pending).resolves.toEqual({ done: true, value: undefined });
    await pump.close();
    expect(finalized).toBe(1);
  });

  it("does not start the handler when the signal is already aborted", async () => {
    let started = 0;
    const controller = new AbortController();
    // connect can abort during the server's span setup, so the pump is often
    // built around a signal that has already fired. A listener added after the
    // fact never runs, so `next()` must still settle from the initial state.
    controller.abort();
    const pump = StreamBridge.responsePump(
      Stream.fromEffect(
        Effect.sync(() => {
          started += 1;
        }),
      ),
      Context.empty(),
      controller.signal,
    );

    await expect(pump.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
    await pump.close();
    await tick();
    expect(started).toBe(0);
  });

  it("delivers values and close() terminates the handler exactly once", async () => {
    let finalized = 0;
    const controller = new AbortController();
    const pump = StreamBridge.responsePump(
      Stream.make("a").pipe(
        Stream.concat(Stream.never),
        Stream.ensuring(
          Effect.sync(() => {
            finalized += 1;
          }),
        ),
      ),
      Context.empty(),
      controller.signal,
    );

    await expect(pump.next()).resolves.toEqual({ done: false, value: "a" });
    await pump.close();
    expect(finalized).toBe(1);

    await pump.close();
    controller.abort();
    await tick();
    expect(finalized).toBe(1);
  });

  it("close resolves even when handler cleanup fails", async () => {
    const controller = new AbortController();
    const pump = StreamBridge.responsePump(
      Stream.make("a").pipe(
        Stream.concat(Stream.never),
        Stream.ensuring(Effect.die(new Error("cleanup boom"))),
      ),
      Context.empty(),
      controller.signal,
    );

    await pump.next();
    await expect(pump.close()).resolves.toBeUndefined();
  });
});
