import { assert, describe, it } from "@effect/vitest";
import { Context, Effect, Fiber, Ref, Stream } from "effect";
import { TestClock } from "effect/testing";

import * as StreamBridge from "../src/internal/streamBridge.js";

const noop = () => {};

const tick = Effect.sleep(10);

/** Resolves to `"pending"` instead of hanging when `promise` never settles. */
const settled = <A>(promise: Promise<A>): Effect.Effect<A | "pending"> =>
  Effect.promise(() => promise).pipe(
    Effect.timeoutOrElse({
      duration: 100,
      orElse: () => Effect.succeed("pending" as const),
    }),
  );

/** One pull, with the iterator's rejection surfaced in the error channel. */
const next = (
  iterator: AsyncIterator<unknown>,
): Effect.Effect<IteratorResult<unknown>, unknown> =>
  Effect.tryPromise({ try: () => iterator.next(), catch: (error) => error });

describe("requestPump", () => {
  it.effect("delivers all values and completes cleanly without aborting", () =>
    Effect.gen(function* () {
      let aborted = 0;
      const pump = StreamBridge.requestPump(
        Stream.make(1, 2, 3),
        Context.empty(),
        () => {
          aborted += 1;
        },
      );

      const values: Array<unknown> = [];
      yield* Effect.promise(async () => {
        for await (const value of pump.iterable) {
          values.push(value);
        }
      });

      assert.deepStrictEqual(values, [1, 2, 3]);
      assert.strictEqual(aborted, 0);
      assert.isUndefined(pump.failure());
    }),
  );

  it.effect(
    "aborts the call and preserves the original error when the source fails",
    () =>
      Effect.gen(function* () {
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
        const first = yield* next(iterator);
        assert.deepStrictEqual(first, { done: false, value: 1 });
        const error = yield* Effect.flip(next(iterator));
        assert.strictEqual(error, boom);

        assert.strictEqual(aborted, 1);
        assert.deepStrictEqual(pump.failure(), { error: boom });
      }),
  );

  // Regression pin: the chain that serializes pulls must not carry a rejection
  // forward. A retained failed link would replay the first failure to every
  // later `next()` without running a pull — on the plainly sequential path, so
  // connect would stop re-issuing the cancel that a failed request stream owes
  // the server.
  it.effect(
    "re-pulls after a failure instead of replaying it down the chain",
    () =>
      Effect.gen(function* () {
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
        const first = yield* next(iterator);
        assert.deepStrictEqual(first, { done: false, value: 1 });
        const second = yield* Effect.flip(next(iterator));
        assert.strictEqual(second, boom);
        const third = yield* Effect.flip(next(iterator));
        assert.strictEqual(third, boom);

        assert.strictEqual(aborted, 2);
      }),
  );

  it.effect(
    "treats connect's iterator throw as cleanup, not a request failure",
    () =>
      Effect.gen(function* () {
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
        yield* Effect.promise(() => iterator.next());
        const thrown = yield* Effect.promise(() =>
          Promise.resolve(iterator.throw?.(new Error("connect abort"))),
        );
        assert.deepStrictEqual(thrown, { done: true, value: undefined });

        assert.strictEqual(finalized, 1);
        assert.strictEqual(aborted, 0);
        assert.isUndefined(pump.failure());

        yield* Effect.promise(() => pump.close());
        assert.strictEqual(finalized, 1);
      }),
  );

  // Regression pin: Effect's AsyncIterable bridge abandons an in-flight pull
  // on return() without interrupting it. The pump must own its pull fibers so
  // ending a call interrupts the user's request stream — otherwise the pull
  // (and its interrupt cleanup) leaks for every call that ends while the
  // stream awaits its next element, the normal state for a bidi stream.
  it.live("interrupts an in-flight request pull before close resolves", () =>
    Effect.gen(function* () {
      let interrupted = 0;
      const pump = StreamBridge.requestPump(
        Stream.fromEffect(
          Effect.never.pipe(
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted += 1;
              }),
            ),
          ),
        ),
        Context.empty(),
        noop,
      );

      const iterator = pump.iterable[Symbol.asyncIterator]();
      const pending = iterator.next();
      yield* tick;
      yield* Effect.promise(() => pump.close());

      assert.strictEqual(interrupted, 1);
      const result = yield* Effect.promise(() => pending);
      assert.deepStrictEqual(result, { done: true, value: undefined });
    }),
  );

  // Regression pin: overlapping `next()` calls used to race the shared chunk
  // iterator, so callers saw duplicated and dropped elements. Serializing must
  // deliver the stream intact — a pump that simply answered the overlapping
  // caller with `done` would pass every teardown assertion below while
  // truncating the request stream after its first element.
  it.effect("delivers every element when pulls overlap", () =>
    Effect.gen(function* () {
      const pump = StreamBridge.requestPump(
        Stream.fromIterable([1, 2, 3, 4]).pipe(Stream.rechunk(1)),
        Context.empty(),
        noop,
      );

      const iterator = pump.iterable[Symbol.asyncIterator]();
      const results = yield* Effect.promise(() =>
        Promise.all([
          iterator.next(),
          iterator.next(),
          iterator.next(),
          iterator.next(),
          iterator.next(),
        ]),
      );

      assert.deepStrictEqual(results, [
        { done: false, value: 1 },
        { done: false, value: 2 },
        { done: false, value: 3 },
        { done: false, value: 4 },
        { done: true, value: undefined },
      ]);
    }),
  );

  // Regression pin: the pump retains a single pull fiber. A `next()` issued
  // while another is in flight used to fork a second pull, overwrite that slot
  // and race the shared iterator — close() then interrupted only the last
  // fiber, leaving the first pull (and its interrupt cleanup) pending forever.
  it.live("serializes overlapping pulls so close() leaves none behind", () =>
    Effect.gen(function* () {
      let started = 0;
      let interrupted = 0;
      const pump = StreamBridge.requestPump(
        Stream.fromEffect(
          Effect.sync(() => {
            started += 1;
          }).pipe(
            Effect.andThen(Effect.never),
            Effect.onInterrupt(() =>
              Effect.sync(() => {
                interrupted += 1;
              }),
            ),
          ),
        ),
        Context.empty(),
        noop,
      );

      const iterator = pump.iterable[Symbol.asyncIterator]();
      const first = iterator.next();
      const second = iterator.next();
      yield* tick;
      yield* Effect.promise(() => pump.close());

      assert.strictEqual(started, 1);
      assert.strictEqual(interrupted, 1);
      const firstResult = yield* settled(first);
      assert.deepStrictEqual(firstResult, { done: true, value: undefined });
      const secondResult = yield* settled(second);
      assert.deepStrictEqual(secondResult, { done: true, value: undefined });
    }),
  );

  it.effect("close resolves even when stream cleanup fails", () =>
    Effect.gen(function* () {
      const pump = StreamBridge.requestPump(
        Stream.make(1).pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(Effect.die(new Error("cleanup boom"))),
        ),
        Context.empty(),
        noop,
      );

      const iterator = pump.iterable[Symbol.asyncIterator]();
      yield* Effect.promise(() => iterator.next());
      const closed = yield* Effect.promise(() => pump.close());
      assert.isUndefined(closed);
    }),
  );

  it.effect("pulls lazily so a slow consumer applies backpressure", () =>
    Effect.gen(function* () {
      const counter = yield* Ref.make(0);
      const source = Stream.fromIterableEffectRepeat(
        Ref.modify(counter, (n) => [[n + 1], n + 1] as const),
      );
      const pump = StreamBridge.requestPump(source, Context.empty(), noop);

      const iterator = pump.iterable[Symbol.asyncIterator]();
      const first = yield* Effect.promise(() => iterator.next());
      assert.deepStrictEqual(first, { done: false, value: 1 });
      const second = yield* Effect.promise(() => iterator.next());
      assert.deepStrictEqual(second, { done: false, value: 2 });

      const pulled = yield* Ref.get(counter);
      assert.strictEqual(pulled, 2);
      yield* Effect.promise(() => pump.close());
    }),
  );
});

describe("responseStream", () => {
  const failingResponses = (cause: unknown): AsyncIterable<unknown> =>
    (async function* () {
      yield "r1";
      throw cause;
    })();

  it.effect(
    "replays the original source failure instead of the wire error",
    () =>
      Effect.gen(function* () {
        const boom = new Error("source boom");
        const pump = StreamBridge.requestPump(
          Stream.fail(boom),
          Context.empty(),
          noop,
        );
        yield* Effect.ignore(next(pump.iterable[Symbol.asyncIterator]()));

        // Typed as the mapped error by the bridge, but the replayed value is the
        // raw source failure — compare it as such.
        const error: unknown = yield* Effect.flip(
          Stream.runCollect(
            StreamBridge.responseStream(
              failingResponses(new Error("wire cancelled")),
              pump,
              (cause) => ({ mapped: cause }),
            ),
          ),
        );

        assert.strictEqual(error, boom);
      }),
  );

  it.effect("maps wire errors when the request stream did not fail", () =>
    Effect.gen(function* () {
      const wire = new Error("wire boom");
      const pump = StreamBridge.requestPump(
        Stream.empty,
        Context.empty(),
        noop,
      );

      const error = yield* Effect.flip(
        Stream.runCollect(
          StreamBridge.responseStream(
            failingResponses(wire),
            pump,
            (cause) => ({
              mapped: cause,
            }),
          ),
        ),
      );

      assert.deepStrictEqual(error, { mapped: wire });
    }),
  );
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

  it.effect("treats a clean end with a live signal as a half-close", () =>
    Effect.gen(function* () {
      const controller = new AbortController();
      const requests = (async function* () {
        yield 1;
        yield 2;
      })();

      const values = yield* Stream.runCollect(
        StreamBridge.requestStream(options(requests, controller.signal)),
      );

      assert.deepStrictEqual(values, [1, 2]);
    }),
  );

  it.effect(
    "turns a clean end with an aborted signal into a cancellation",
    () =>
      Effect.gen(function* () {
        const controller = new AbortController();
        const requests = (async function* () {
          yield 1;
          controller.abort();
        })();

        const error = yield* Effect.flip(
          Stream.runCollect(
            StreamBridge.requestStream(options(requests, controller.signal)),
          ),
        );

        assert.strictEqual(error, "cancelled");
      }),
  );

  it.effect("maps request iterable failures through onError", () =>
    Effect.gen(function* () {
      const boom = new Error("request boom");
      const controller = new AbortController();
      const requests = (async function* () {
        yield 1;
        throw boom;
      })();

      const error = yield* Effect.flip(
        Stream.runCollect(
          StreamBridge.requestStream(options(requests, controller.signal)),
        ),
      );

      assert.deepStrictEqual(error, { mapped: boom });
    }),
  );

  // Regression pin: connect's request iterable strictly queues a `return()`
  // issued while a `next()` is pending until that pull settles. A handler
  // tearing down its consumption mid-pull (timeout, race) with an idle client
  // must not await that queued cleanup — before the fix the teardown (and the
  // whole call) hung until the client sent a message or went away.
  it.effect(
    "does not block teardown on a return() queued behind a pending pull",
    () =>
      Effect.gen(function* () {
        const controller = new AbortController();
        let returned = 0;
        const requests: AsyncIterable<unknown> = {
          [Symbol.asyncIterator]: () => ({
            next: () => new Promise<IteratorResult<unknown>>(() => {}),
            return: () => {
              returned += 1;
              return new Promise<IteratorResult<unknown>>(() => {});
            },
          }),
        };

        const fiber = yield* Effect.forkChild(
          Stream.runDrain(
            StreamBridge.requestStream(options(requests, controller.signal)),
          ).pipe(
            Effect.timeoutOrElse({
              duration: 100,
              orElse: () => Effect.succeed("teardown completed"),
            }),
          ),
        );
        yield* TestClock.adjust(100);
        const result = yield* Fiber.join(fiber);

        assert.strictEqual(result, "teardown completed");
        // The cleanup is still issued — just not awaited.
        assert.strictEqual(returned, 1);
      }),
  );
});

describe("responsePump", () => {
  it.live("interrupts an in-flight handler pull before close resolves", () =>
    Effect.gen(function* () {
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
      yield* Effect.promise(() => started);
      yield* Effect.promise(() => pump.close());
      resume();
      yield* tick;

      const result = yield* Effect.promise(() => pending);
      assert.deepStrictEqual(result, { done: true, value: undefined });
      assert.strictEqual(effects, 0);
    }),
  );

  it.live(
    "settles a pending pull with a clean end and closes the handler once when the signal aborts",
    () =>
      Effect.gen(function* () {
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
        yield* tick;
        controller.abort();

        // A clean `{ done: true }` — not a rejection — so connect sees a normal
        // end of stream rather than a thrown generator.
        const result = yield* Effect.promise(() => pending);
        assert.deepStrictEqual(result, { done: true, value: undefined });
        yield* Effect.promise(() => pump.close());
        assert.strictEqual(finalized, 1);
      }),
  );

  it.live("does not start the handler when the signal is already aborted", () =>
    Effect.gen(function* () {
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

      const result = yield* Effect.promise(() => pump.next());
      assert.deepStrictEqual(result, { done: true, value: undefined });
      yield* Effect.promise(() => pump.close());
      yield* tick;
      assert.strictEqual(started, 0);
    }),
  );

  it.live(
    "delivers values and close() terminates the handler exactly once",
    () =>
      Effect.gen(function* () {
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

        const first = yield* Effect.promise(() => pump.next());
        assert.deepStrictEqual(first, { done: false, value: "a" });
        yield* Effect.promise(() => pump.close());
        assert.strictEqual(finalized, 1);

        yield* Effect.promise(() => pump.close());
        controller.abort();
        yield* tick;
        assert.strictEqual(finalized, 1);
      }),
  );

  it.effect("close resolves even when handler cleanup fails", () =>
    Effect.gen(function* () {
      const controller = new AbortController();
      const pump = StreamBridge.responsePump(
        Stream.make("a").pipe(
          Stream.concat(Stream.never),
          Stream.ensuring(Effect.die(new Error("cleanup boom"))),
        ),
        Context.empty(),
        controller.signal,
      );

      yield* Effect.promise(() => pump.next());
      const closed = yield* Effect.promise(() => pump.close());
      assert.isUndefined(closed);
    }),
  );
});
