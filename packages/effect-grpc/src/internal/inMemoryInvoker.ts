import { Channel, Effect, Exit, Fiber, Scope, Stream } from "effect";

import type { GrpcCallOptions } from "../CodegenSupport.js";
import type {
  GrpcInMemoryCall,
  GrpcInMemoryHandler,
  GrpcInMemoryHandlers,
  GrpcInvokerService,
} from "../GrpcInvoker.js";
import * as GrpcMetadata from "../GrpcMetadata.js";
import * as GrpcStatusError from "../GrpcStatusError.js";
import { callTimeoutMs, unknownTag, validateCallMetadata } from "./invoker.js";

/**
 * Test {@link GrpcInvokerService}: dispatches to in-process handlers with the
 * same invocation semantics as the connect adapter, but at domain-value level
 * — no sockets, protobuf descriptors, or HTTP/2. Interruption and stream
 * finalization propagate naturally because caller and handler share a fiber.
 */
export const makeInMemory = (
  handlers: GrpcInMemoryHandlers,
): GrpcInvokerService => {
  const lookup = <K extends GrpcInMemoryHandler["kind"]>(
    tag: string,
    kind: K,
  ): Extract<GrpcInMemoryHandler, { readonly kind: K }> | undefined => {
    const handler = handlers[tag];
    return handler && handler.kind === kind
      ? (handler as Extract<GrpcInMemoryHandler, { readonly kind: K }>)
      : undefined;
  };

  const callContext = (
    tag: string,
    options: GrpcCallOptions | undefined,
  ): GrpcInMemoryCall => {
    const timeoutMs = callTimeoutMs(options);
    return {
      tag,
      // Round-tripped through the wire codec so the handler observes exactly
      // what a server would: lowercased keys, `-bin` values decoded back to
      // bytes, repeated keys joined the way `Headers` joins them.
      metadata: GrpcMetadata.fromHeaders(
        GrpcMetadata.toHeaders(options?.metadata ?? GrpcMetadata.empty),
      ),
      // A non-positive timeout puts no `grpc-timeout` header on the wire, so
      // the handler must see no deadline rather than a value that is not in
      // force.
      ...(timeoutMs === undefined ? {} : { timeoutMs }),
    };
  };

  const withDeadline = (
    effect: Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>,
    timeoutMs: number | undefined,
  ) =>
    timeoutMs === undefined
      ? effect
      : Effect.timeoutOrElse(effect, {
          duration: timeoutMs,
          orElse: () => Effect.fail(deadlineExceeded()),
        });

  // The stream counterpart of `withDeadline`: one timer for the whole call,
  // started before the handler's stream is even set up. `Stream.timeout`
  // would not do — it restarts per pull, turning the deadline into an
  // inactivity window that a chatty stream never trips. Nor would
  // `Stream.interruptWhen`: it only surfaces the timer's failure on the
  // consumer's next pull, so a consumer holding the stream open while it
  // processes a response would leave the producer running past the deadline.
  // Instead the producer gets a scope of its own that the timer closes at
  // expiry, whether or not anyone is pulling. Setup and every pull run in
  // fibers that scope owns, so closing it interrupts whichever is in flight
  // — a setup that is slow or never completes included — then the producer's
  // finalizers run, and everything from then on fails with
  // `deadline_exceeded`.
  const withStreamDeadline = <A, E>(
    stream: Stream.Stream<A, E>,
    timeoutMs: number | undefined,
  ): Stream.Stream<A, E | GrpcStatusError.GrpcStatusError> =>
    timeoutMs === undefined
      ? stream
      : Stream.fromChannel(
          Channel.fromTransform((_upstream, scope) =>
            Effect.gen(function* () {
              const producer = yield* Scope.fork(scope);
              let expired: GrpcStatusError.GrpcStatusError | undefined;
              // Bound to the call's scope, so completion or an early
              // consumer close cancels the timer along with everything else.
              yield* Effect.forkIn(
                Effect.sleep(timeoutMs).pipe(
                  Effect.andThen(
                    Effect.suspend(() => {
                      expired = deadlineExceeded();
                      return Scope.close(producer, Exit.fail(expired));
                    }),
                  ),
                ),
                scope,
                { startImmediately: true },
              );
              // Runs `effect` in a fiber the producer scope owns, so closing
              // the scope interrupts it before the producer's finalizers run.
              // `expired` is set before the close, so an interrupted run
              // reports the deadline, not a bare interruption.
              const owned = <X, X2, R>(
                effect: Effect.Effect<X, X2, R>,
              ): Effect.Effect<X, X2 | GrpcStatusError.GrpcStatusError, R> =>
                Effect.suspend(() =>
                  expired
                    ? Effect.fail(expired)
                    : Effect.forkIn(effect, producer).pipe(
                        Effect.flatMap(Fiber.join),
                        Effect.catchCause(
                          (
                            cause,
                          ): Effect.Effect<
                            never,
                            X2 | GrpcStatusError.GrpcStatusError
                          > =>
                            expired
                              ? Effect.fail(expired)
                              : Effect.failCause(cause),
                        ),
                      ),
                );
              // Setup — `Stream.toPull` runs the handler stream's own
              // acquisition, which may be slow or never finish — is owned the
              // same way as every pull after it, with the timer already
              // running.
              const pull = yield* owned(
                Stream.toPull(stream).pipe(Scope.provide(producer)),
              );
              return owned(pull);
            }),
          ),
        );

  const unary: GrpcInvokerService["unary"] = (tag, request, options) => {
    const method = lookup(tag, "unary");
    if (!method) return Effect.fail(unknownTag(tag));
    return validateCallMetadata(options).pipe(
      Effect.andThen(
        Effect.suspend(() =>
          withDeadline(
            method.handler(request, callContext(tag, options)),
            callTimeoutMs(options),
          ),
        ),
      ),
    );
  };

  const serverStream: GrpcInvokerService["serverStream"] = (
    tag,
    request,
    options,
  ) => {
    const method = lookup(tag, "server-streaming");
    if (!method) return Stream.fail(unknownTag(tag));
    return Stream.unwrap(
      validateCallMetadata(options).pipe(
        Effect.map(() =>
          withStreamDeadline(
            method.handler(request, callContext(tag, options)),
            callTimeoutMs(options),
          ),
        ),
      ),
    );
  };

  const clientStream: GrpcInvokerService["clientStream"] = <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => {
    const method = lookup(tag, "client-streaming");
    if (!method) return Effect.fail(unknownTag(tag));
    // Execution-local failure capture: like the wire, the handler observes a
    // failed request stream as `cancelled` while the caller gets its
    // original error replayed.
    return validateCallMetadata(options).pipe(
      Effect.andThen(
        Effect.suspend(() => {
          const replay = sourceReplay<A, E>(requests);
          return withDeadline(
            method.handler(replay.requests, callContext(tag, options)),
            callTimeoutMs(options),
          ).pipe(
            Effect.mapError(replay.restore),
            Effect.tap(() => replay.failIfCaptured),
          );
        }),
      ),
    );
  };

  const bidiStream: GrpcInvokerService["bidiStream"] = <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => {
    const method = lookup(tag, "bidi-streaming");
    if (!method) return Stream.fail(unknownTag(tag));
    return Stream.unwrap(
      validateCallMetadata(options).pipe(
        Effect.as(
          Stream.suspend(() => {
            const replay = sourceReplay<A, E>(requests);
            // As for client-streaming, the deadline sits inside source
            // replay: it bounds the handler — request and response work
            // alike — while a request-stream failure already captured still
            // reaches the caller as its own error, not as `deadline_exceeded`.
            return withStreamDeadline(
              method.handler(replay.requests, callContext(tag, options)),
              callTimeoutMs(options),
            ).pipe(
              Stream.mapError(replay.restore),
              Stream.mapEffect((value) =>
                replay.failIfCaptured.pipe(Effect.as(value)),
              ),
              Stream.concat(
                Stream.fromEffect(replay.failIfCaptured).pipe(Stream.drain),
              ),
            );
          }),
        ),
      ),
    );
  };

  return { unary, serverStream, clientStream, bidiStream };
};

/** The one status every shape fails with when its deadline expires. */
const deadlineExceeded = () =>
  GrpcStatusError.deadlineExceeded("RPC deadline exceeded");

/**
 * Mirrors the wire's source-failure policy: gRPC has no channel for an
 * arbitrary client stream error, so the server side sees `cancelled` and the
 * caller's original error is replayed once the call fails.
 */
const sourceReplay = <A, E>(requests: Stream.Stream<A, E>) => {
  let failure: { readonly error: E } | undefined;
  return {
    requests: Stream.mapError(requests, (error) => {
      failure ??= { error };
      return GrpcStatusError.cancelled("RPC cancelled", error);
    }),
    restore: (
      error: GrpcStatusError.GrpcStatusError,
    ): GrpcStatusError.GrpcStatusError | E => (failure ? failure.error : error),
    failIfCaptured: Effect.suspend(() =>
      failure ? Effect.fail(failure.error) : Effect.void,
    ),
  };
};
