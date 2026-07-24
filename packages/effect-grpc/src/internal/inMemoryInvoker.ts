import { Effect, Stream } from "effect";

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
          orElse: () =>
            Effect.fail(
              GrpcStatusError.make({
                code: "deadline_exceeded",
                message: "RPC deadline exceeded",
              }),
            ),
        });

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
        Effect.map(() => method.handler(request, callContext(tag, options))),
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
            return method
              .handler(replay.requests, callContext(tag, options))
              .pipe(
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
