import {
  Cause,
  Context,
  Effect,
  Exit,
  Fiber,
  Pull,
  Scope,
  Stream,
} from "effect";

/**
 * One home for the Effect `Stream` <-> connect `AsyncIterable` lifecycle.
 *
 * Mapping values between the two worlds is trivial; preserving the meaning
 * of stream *termination* is not, because the two sides interpret the same
 * iterator events differently:
 *
 * - Half-close: a request iterator ending normally means "client done
 *   sending" — the call may still be live and produce responses.
 * - Cancellation: connect-node surfaces a client cancellation server-side as
 *   a clean end of the request iterable plus an aborted handler signal, so a
 *   clean EOF alone is not proof of a half-close.
 * - Source failure: gRPC has no channel for an arbitrary client stream
 *   error. The client can only cancel the call; the original error must be
 *   replayed to the local caller while the server observes `cancelled`.
 * - Abort via `throw`: connect aborts a request stream by calling the
 *   iterator's `throw`; that is cleanup, not a request failure.
 * - Cleanup is outcome-preserving: closing iterators and removing listeners
 *   can never replace the call's real result.
 *
 * The four bridges below own these rules — `requestPump`/`responseStream`
 * for the client, `requestStream`/`responsePump` for the server. Callers own
 * codecs, gRPC status normalization, tracing, and connect wiring.
 */

/** A recorded request-stream failure. Wrapped so `undefined` errors survive. */
export interface SourceFailure {
  readonly error: unknown;
}

/**
 * Carries the `Cause` of a handler-stream failure across the promise boundary
 * of {@link responsePump}. `next()` rejects with this wrapper instead of a
 * squashed error so the caller can map the real cause — an interrupt-only
 * cause (handler interrupted itself -> `cancelled`) is indistinguishable from
 * a generic defect once squashed.
 */
export class PumpFailure {
  constructor(readonly cause: Cause.Cause<unknown>) {}
}

/**
 * Client request side: pumps an Effect `Stream` into the `AsyncIterable`
 * connect consumes as the request stream of a client-streaming or
 * bidi-streaming call.
 */
export interface RequestPump {
  /** Request stream handed to connect. */
  readonly iterable: AsyncIterable<unknown>;
  /**
   * Interrupts the underlying stream. Safe to call more than once; cleanup
   * errors are swallowed because the call outcome is already determined.
   */
  readonly close: () => Promise<void>;
  /** The original source failure, when the request stream failed locally. */
  readonly failure: () => SourceFailure | undefined;
}

export const requestPump = (
  requests: Stream.Stream<unknown, unknown>,
  context: Context.Context<never>,
  abortCall: () => void,
): RequestPump => {
  let failure: SourceFailure | undefined;
  const pull = ownedPull(requests, context, (cause) => {
    // gRPC has no channel for client-side failures other than cancelling
    // the call: remember the original error so the caller sees it while
    // the server observes `cancelled`.
    const error: unknown = Cause.squash(cause);
    failure ??= { error };
    abortCall();
    throw error;
  });
  const close = async (): Promise<IteratorResult<unknown>> => {
    await pull.close();
    return { done: true, value: undefined };
  };
  return {
    iterable: {
      // connect aborts the request stream via `throw`; resolving done
      // reports clean completion while the underlying stream is interrupted
      // by the iterator close.
      [Symbol.asyncIterator]: () => ({
        next: pull.next,
        return: close,
        throw: close,
      }),
    },
    close: async () => {
      await pull.close();
    },
    failure: () => failure,
  };
};

/**
 * Client response side: bridges the `AsyncIterable` connect returns for a
 * bidi-streaming call back into an Effect `Stream`, replaying the original
 * request-stream failure — the wire only carries `cancelled`.
 */
export const responseStream = <E>(
  responses: AsyncIterable<unknown>,
  pump: RequestPump,
  onError: (cause: unknown) => E,
): Stream.Stream<unknown, E> =>
  Stream.fromAsyncIterable(responses, (cause): E => {
    const failure = pump.failure();
    return failure ? (failure.error as E) : onError(cause);
  });

/**
 * Server request side: bridges connect's request `AsyncIterable` into the
 * Effect `Stream` a streaming handler consumes, distinguishing a half-close
 * from a cancellation.
 */
export const requestStream = <E>(options: {
  readonly requests: AsyncIterable<unknown>;
  readonly signal: AbortSignal;
  readonly onError: (cause: unknown) => E;
  readonly onCancelled: () => E;
}): Stream.Stream<unknown, E> =>
  Stream.fromAsyncIterable(
    detachedCleanup(options.requests),
    options.onError,
  ).pipe(
    // connect-node surfaces a client cancellation as a clean end of the
    // request iterable plus an aborted handler signal; distinguish it from a
    // half-close so handlers do not treat a truncated stream as complete.
    Stream.concat(
      Stream.suspend(() =>
        options.signal.aborted
          ? Stream.fail(options.onCancelled())
          : Stream.empty,
      ),
    ),
  );

/**
 * connect's request iterable strictly queues a `return()` issued while a
 * `next()` is pending until that pull settles — but a handler that stops
 * consuming mid-pull (timeout, race) tears the stream down exactly then, and
 * awaiting the queued cleanup would block the handler's interruption (and
 * with it the whole call) until the client sends another message or goes
 * away. Issue the cleanup without awaiting it: the abandoned pull belongs to
 * connect's own call teardown.
 */
const detachedCleanup = (
  iterable: AsyncIterable<unknown>,
): AsyncIterable<unknown> => ({
  [Symbol.asyncIterator]: () => {
    const iterator = iterable[Symbol.asyncIterator]();
    return {
      next: () => iterator.next(),
      return: () => {
        void Promise.resolve(iterator.return?.(undefined)).catch(
          () => undefined,
        );
        return Promise.resolve<IteratorResult<unknown>>({
          done: true,
          value: undefined,
        });
      },
    };
  },
});

/**
 * Server response side: pumps a handler's Effect `Stream` into the iterator
 * connect drives for a bidi-streaming response, closing the handler when the
 * client goes away.
 */
export interface ResponsePump {
  /**
   * Pull one response; connect's generator loop drives this. Rejects with a
   * {@link PumpFailure} carrying the handler stream's `Cause` when it fails.
   */
  readonly next: () => Promise<IteratorResult<unknown>>;
  /**
   * Removes the abort listener and closes the handler stream. Safe to call
   * more than once; cleanup errors are swallowed because the call outcome is
   * already determined.
   */
  readonly close: () => Promise<void>;
}

export const responsePump = (
  responses: Stream.Stream<unknown, unknown>,
  context: Context.Context<never>,
  signal: AbortSignal,
): ResponsePump => {
  const pull = ownedPull(responses, context, (cause) => {
    throw new PumpFailure(cause);
  });
  const close = (): Promise<void> => {
    signal.removeEventListener("abort", onAbort);
    return pull.close();
  };
  const onAbort = () => {
    void close();
  };

  if (signal.aborted) {
    void close();
  } else {
    signal.addEventListener("abort", onAbort, { once: true });
  }
  return { next: pull.next, close };
};

/**
 * The pull machine under both pumps. Effect's own AsyncIterable bridge hides
 * its pull fibers and abandons a pull in flight on return(), so own the pull:
 * spawn each one in an owned scope and retain the active fiber until it
 * completes or close() interrupts it — teardown must reach into the stream
 * (its finalizers, its interrupt cleanup), not walk away from it.
 *
 * A failed pull is delegated to `onFailure`, which must throw; after close(),
 * `next()` reports a clean end.
 */
const ownedPull = (
  stream: Stream.Stream<unknown, unknown>,
  context: Context.Context<never>,
  onFailure: (cause: Cause.Cause<unknown>) => never,
): {
  readonly next: () => Promise<IteratorResult<unknown>>;
  readonly close: () => Promise<void>;
} => {
  const done: IteratorResult<unknown> = { done: true, value: undefined };
  const scope = Scope.makeUnsafe();
  const runFork = Effect.runForkWith(Context.add(context, Scope.Scope, scope));
  let active: Fiber.Fiber<unknown, unknown> | undefined;
  let current: Iterator<unknown> | undefined;
  let pull: Pull.Pull<ReadonlyArray<unknown>, unknown> | undefined;
  let closed = false;
  let closing: Promise<void> | undefined;

  const run = async <A, E>(
    effect: Effect.Effect<A, E, Scope.Scope>,
  ): Promise<Exit.Exit<A, E>> => {
    const fiber = runFork(effect);
    active = fiber;
    const exit = await Effect.runPromise(Fiber.await(fiber));
    if (active === fiber) active = undefined;
    return exit;
  };

  const close = (): Promise<void> => {
    if (closing) return closing;
    closed = true;
    const fiber = active;
    closing = (async () => {
      if (fiber) await Effect.runPromise(Fiber.interrupt(fiber));
      await Effect.runPromise(Scope.close(scope, Exit.void));
    })().catch(() => undefined);
    return closing;
  };

  return {
    next: async () => {
      if (closed) return done;
      if (current) {
        const result = current.next();
        if (!result.done) return result;
        current = undefined;
      }
      if (!pull) {
        const initialized = await run(Stream.toPull(stream));
        if (closed) return done;
        if (Exit.isFailure(initialized)) return onFailure(initialized.cause);
        pull = initialized.value;
      }
      const exit = await run(pull);
      if (closed) return done;
      if (Exit.isSuccess(exit)) {
        current = exit.value[Symbol.iterator]();
        return current.next();
      }
      if (Pull.isDoneCause(exit.cause)) return done;
      return onFailure(exit.cause);
    },
    close,
  };
};
