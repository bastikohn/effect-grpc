import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import {
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Queue,
  Schema,
  Scope,
  Stream,
} from "effect";
import * as Tracer from "effect/Tracer";
import { ServerClient } from "effect/unstable/rpc/Rpc";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";
import { RequestId } from "effect/unstable/rpc/RpcMessage";

import type { GrpcServerContext } from "./CodegenSupport.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import * as CallState from "./internal/callState.js";
import { entryCodecs } from "./internal/codec.js";
import { eof, requestId } from "./internal/effectRpc.js";
import { errorFromExit } from "./internal/status.js";
import * as GrpcTracing from "./internal/tracing.js";

export interface GrpcServerProtocolOptions {
  readonly registry: GrpcMethodRegistry;
  readonly streamingHandlers?: GrpcStreamingHandlers;
}

/**
 * Handlers for client-streaming and bidi-streaming methods. Effect RPC's wire
 * protocol has no client-to-server chunk variant, so these methods bypass
 * `RpcServer` and bridge connect's `AsyncIterable` requests to `Stream`
 * directly.
 */
export interface GrpcClientStreamingHandler<R = never> {
  readonly kind: "client-streaming";
  readonly handler: (
    requests: Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
    context: GrpcServerContext,
  ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError, R>;
}

export interface GrpcBidiStreamingHandler<R = never> {
  readonly kind: "bidi-streaming";
  readonly handler: (
    requests: Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
    context: GrpcServerContext,
  ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError, R>;
}

export type GrpcStreamingHandler<R = never> =
  | GrpcClientStreamingHandler<R>
  | GrpcBidiStreamingHandler<R>;

export type GrpcStreamingHandlers = ReadonlyMap<string, GrpcStreamingHandler>;

/**
 * Carries the streaming handlers of a generated service inside its handlers
 * layer, so `GrpcNodeServer.serveAll` can collect them without changing the
 * user-facing service wiring.
 */
export const GrpcStreamingHandlers = Context.Service<GrpcStreamingHandlers>(
  "@effect-grpc/effect-grpc/GrpcStreamingHandlers",
);

/**
 * Builds the layer generated `*HandlersLayer` functions use to publish
 * streaming handlers. Captures the context so handler requirements `R` are
 * resolved where the layer is built.
 */
export const streamingHandlersLayer = <R = never>(
  handlers: Record<string, GrpcStreamingHandler<R>>,
): Layer.Layer<GrpcStreamingHandlers, never, R> =>
  Layer.effect(
    GrpcStreamingHandlers,
    Effect.gen(function* () {
      const context = yield* Effect.context<R>();
      return new Map(
        Object.entries(handlers).map(([tag, handler]) => [
          tag,
          bindStreamingHandler(handler, context),
        ]),
      );
    }),
  );

const bindStreamingHandler = <R>(
  entry: GrpcStreamingHandler<R>,
  context: Context.Context<R>,
): GrpcStreamingHandler =>
  entry.kind === "client-streaming"
    ? {
        kind: entry.kind,
        handler: (requests, serverContext) =>
          Effect.provideContext(
            entry.handler(requests, serverContext),
            context,
          ),
      }
    : {
        kind: entry.kind,
        handler: (requests, serverContext) =>
          Stream.provideContext(
            entry.handler(requests, serverContext),
            context,
          ),
      };

export interface GrpcServerProtocolResult {
  readonly protocol: RpcServer.Protocol["Service"];
  readonly routes: (router: ConnectRouter) => ConnectRouter;
}

export const make = (
  options: GrpcServerProtocolOptions,
): Effect.Effect<GrpcServerProtocolResult, never, Scope.Scope> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<never>();
    const run = Effect.runPromiseWith(context);
    const serverRecorder = (entry: GrpcMethodEntry, span: Tracer.Span) =>
      GrpcTracing.serverCallRecorder({ entry, span, context });
    const calls = new Map<number, CallState.CallState>();
    const clientIds = new Set<number>();
    const disconnects = yield* Queue.unbounded<number>();
    let nextClientId = 0;
    let writeRequest: (
      clientId: number,
      data: FromClientEncoded,
    ) => Effect.Effect<void> = () => Effect.void;

    const protocol = yield* RpcServer.Protocol.make((writeRequest_) => {
      writeRequest = writeRequest_;
      return Effect.succeed({
        disconnects,
        send(clientId, response) {
          const call = calls.get(clientId);
          return call ? call.offer(response) : Effect.void;
        },
        end(clientId) {
          const call = calls.get(clientId);
          return call ? call.end : Effect.void;
        },
        clientIds: Effect.sync(() => clientIds),
        initialMessage: Effect.succeed(Option.none()),
        supportsAck: false,
        supportsTransferables: false,
        supportsSpanPropagation: true,
      });
    });

    const allocate = (state: CallState.CallState) => {
      const clientId = nextClientId++;
      clientIds.add(clientId);
      calls.set(clientId, state);
      return clientId;
    };

    const cleanup = (
      clientId: number,
      state: CallState.CallState,
      context: HandlerContext,
      onAbort: () => void,
    ) => {
      let released = false;
      let disconnected = false;
      const signalDisconnect = () => {
        if (disconnected) return Promise.resolve();
        disconnected = true;
        return run(Queue.offer(disconnects, clientId).pipe(Effect.asVoid));
      };
      const release = async () => {
        if (released) return;
        released = true;
        context.signal.removeEventListener("abort", onAbort);
        calls.delete(clientId);
        clientIds.delete(clientId);
        await run(state.end);
        await signalDisconnect();
      };
      return { release, signalDisconnect };
    };

    const interrupt = (
      clientId: number,
      signalDisconnect: () => Promise<void>,
    ) =>
      run(
        writeRequest(clientId, {
          _tag: "Interrupt",
          requestId,
        }).pipe(
          Effect.andThen(Effect.promise(signalDisconnect)),
          Effect.asVoid,
        ),
      );

    const sendNativeRequest = (
      clientId: number,
      entry: GrpcMethodEntry,
      request: unknown,
      headers: ReadonlyArray<[string, string]>,
      span: Tracer.Span,
    ) => {
      const payload = fromGrpcRequest(entry, request);
      validatePayload(entry, payload);
      return run(
        writeRequest(clientId, {
          _tag: "Request",
          id: requestId,
          tag: entry.tag,
          payload,
          headers,
          ...GrpcTracing.traceFields(span),
        }).pipe(Effect.withParentSpan(span)),
      );
    };

    const endNativeRequest = (clientId: number) =>
      run(writeRequest(clientId, eof));

    const handleUnary = async (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
    ): Promise<unknown> => {
      const headers = Array.from(context.requestHeader.entries());
      return run(
        Effect.gen(function* () {
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
          return yield* Effect.promise(() =>
            handleUnaryNative(entry, request, context, headers, span),
          );
        }).pipe(
          Effect.withSpan(
            GrpcTracing.spanName(entry),
            GrpcTracing.serverSpanOptions(
              entry,
              GrpcTracing.externalSpanFromHeaders(headers),
            ),
          ),
        ),
      );
    };

    const handleUnaryNative = async (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
      headers: ReadonlyArray<[string, string]>,
      span: Tracer.Span,
    ): Promise<unknown> => {
      const state = await run(CallState.makeUnary);
      const clientId = allocate(state);
      let completed = false;
      let signalDisconnect = () => Promise.resolve();
      const recordStatus = serverRecorder(entry, span);
      const onAbort = () => {
        void interrupt(clientId, signalDisconnect);
      };
      const call = cleanup(clientId, state, context, onAbort);
      signalDisconnect = call.signalDisconnect;
      context.signal.addEventListener("abort", onAbort, { once: true });

      try {
        await sendNativeRequest(clientId, entry, request, headers, span);
        const response = await run(state.awaitExit);
        completed = true;
        if (response._tag === "Defect") {
          throw GrpcStatusError.toConnectError(
            GrpcStatusError.internal("RPC handler defect", response.defect),
          );
        }
        if (response.exit._tag === "Failure") {
          throw GrpcStatusError.toConnectError(errorFromExit(response.exit));
        }
        const grpcResponse = toGrpcResponse(entry, response.exit.value);
        recordStatus("ok");
        return grpcResponse;
      } catch (cause) {
        const error = GrpcStatusError.fromConnectError(cause);
        recordStatus(error.code);
        throw GrpcStatusError.toConnectError(error);
      } finally {
        try {
          if (!completed) {
            await interrupt(clientId, call.signalDisconnect);
          }
          await endNativeRequest(clientId);
        } finally {
          await call.release();
        }
      }
    };

    const handleServerStreaming = async function* (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
    ): AsyncIterable<unknown> {
      const headers = Array.from(context.requestHeader.entries());
      const spanScope = await run(Scope.make());
      const span = await run(
        Effect.makeSpanScoped(
          GrpcTracing.spanName(entry),
          GrpcTracing.serverSpanOptions(
            entry,
            GrpcTracing.externalSpanFromHeaders(headers),
          ),
        ).pipe(Scope.provide(spanScope)),
      );
      const state = await run(CallState.makeServerStreaming);
      const clientId = allocate(state);
      let completed = false;
      let signalDisconnect = () => Promise.resolve();
      let spanExit: Exit.Exit<void, GrpcStatusError.GrpcStatusError> =
        Exit.void;
      const recordStatus = serverRecorder(entry, span);
      const recordFailure = (error: GrpcStatusError.GrpcStatusError) => {
        recordStatus(error.code);
        spanExit = Exit.fail(error);
      };
      const onAbort = () => {
        void interrupt(clientId, signalDisconnect);
      };
      const call = cleanup(clientId, state, context, onAbort);
      signalDisconnect = call.signalDisconnect;
      context.signal.addEventListener("abort", onAbort, { once: true });

      try {
        await sendNativeRequest(clientId, entry, request, headers, span);
        while (true) {
          const response = await run(state.take);
          if (!response) {
            completed = true;
            recordStatus("ok");
            return;
          }
          switch (response._tag) {
            case "Chunk":
              for (const value of response.values) {
                yield toGrpcResponse(entry, value);
              }
              break;
            case "Exit":
              completed = true;
              if (response.exit._tag === "Failure") {
                throw GrpcStatusError.toConnectError(
                  errorFromExit(response.exit),
                );
              }
              recordStatus("ok");
              return;
            case "Defect":
              completed = true;
              throw GrpcStatusError.toConnectError(
                GrpcStatusError.internal("RPC handler defect", response.defect),
              );
            case "ClientProtocolError":
              completed = true;
              throw GrpcStatusError.toConnectError(
                GrpcStatusError.internal("RPC protocol error", response.error),
              );
            case "Pong":
              break;
          }
        }
      } catch (cause) {
        const error = GrpcStatusError.fromConnectError(cause);
        recordFailure(error);
        throw GrpcStatusError.toConnectError(error);
      } finally {
        try {
          if (!completed) {
            if (spanExit._tag === "Success") {
              recordFailure(GrpcStatusError.cancelled("RPC cancelled"));
            }
            await interrupt(clientId, call.signalDisconnect);
          }
          await endNativeRequest(clientId);
        } finally {
          try {
            await call.release();
          } finally {
            await run(Scope.close(spanScope, spanExit));
          }
        }
      }
    };

    const handleClientStreaming = async (
      entry: GrpcMethodEntry,
      streaming: GrpcClientStreamingHandler,
      requests: AsyncIterable<unknown>,
      handlerContext: HandlerContext,
    ): Promise<unknown> => {
      const headers = Array.from(handlerContext.requestHeader.entries());
      let record: GrpcTracing.StatusRecorder | undefined;
      try {
        return await run(
          Effect.gen(function* () {
            const span = yield* Effect.currentSpan.pipe(Effect.orDie);
            const recordStatus = serverRecorder(entry, span);
            record = recordStatus;
            const result = yield* streaming
              .handler(
                streamingRequests(entry, requests, handlerContext.signal),
                streamingServerContext(headers),
              )
              .pipe(
                Effect.flatMap((value) =>
                  encodeStreamingResponse(entry, value),
                ),
                Effect.exit,
              );
            if (result._tag === "Failure") {
              const error = streamingCauseError(result.cause);
              recordStatus(error.code);
              return yield* Effect.fail(error);
            }
            recordStatus("ok");
            return result.value;
          }).pipe(
            Effect.withSpan(
              GrpcTracing.spanName(entry),
              GrpcTracing.serverSpanOptions(
                entry,
                GrpcTracing.externalSpanFromHeaders(headers),
              ),
            ),
          ),
          { signal: handlerContext.signal },
        );
      } catch (cause) {
        const error = streamingRejectionError(cause, handlerContext.signal);
        record?.(error.code);
        throw GrpcStatusError.toConnectError(error);
      }
    };

    const handleBidiStreaming = async function* (
      entry: GrpcMethodEntry,
      streaming: GrpcBidiStreamingHandler,
      requests: AsyncIterable<unknown>,
      handlerContext: HandlerContext,
    ): AsyncIterable<unknown> {
      const headers = Array.from(handlerContext.requestHeader.entries());
      const spanScope = await run(Scope.make());
      const span = await run(
        Effect.makeSpanScoped(
          GrpcTracing.spanName(entry),
          GrpcTracing.serverSpanOptions(
            entry,
            GrpcTracing.externalSpanFromHeaders(headers),
          ),
        ).pipe(Scope.provide(spanScope)),
      );
      const recordStatus = serverRecorder(entry, span);
      let spanExit: Exit.Exit<void, GrpcStatusError.GrpcStatusError> =
        Exit.void;
      let completed = false;
      const responses = streaming
        .handler(
          streamingRequests(entry, requests, handlerContext.signal),
          streamingServerContext(headers),
        )
        .pipe(
          Stream.mapEffect((value) => encodeStreamingResponse(entry, value)),
        );
      const iterator = Stream.toAsyncIterableWith(
        responses,
        Context.add(context, Tracer.ParentSpan, span),
      )[Symbol.asyncIterator]();
      // Closing the iterator interrupts the handler fiber, so a pending pull
      // settles when the client goes away mid-stream.
      const onAbort = () => {
        void Promise.resolve(iterator.return?.(undefined as never)).catch(
          () => undefined,
        );
      };
      handlerContext.signal.addEventListener("abort", onAbort, { once: true });

      try {
        while (true) {
          const next = await iterator.next();
          if (next.done) break;
          yield next.value;
        }
        completed = true;
        recordStatus(handlerContext.signal.aborted ? "cancelled" : "ok");
      } catch (cause) {
        completed = true;
        const error = streamingRejectionError(cause, handlerContext.signal);
        recordStatus(error.code);
        spanExit = Exit.fail(error);
        throw GrpcStatusError.toConnectError(error);
      } finally {
        handlerContext.signal.removeEventListener("abort", onAbort);
        if (!completed) {
          const error = GrpcStatusError.cancelled("RPC cancelled");
          recordStatus(error.code);
          spanExit = Exit.fail(error);
        }
        try {
          await Promise.resolve(iterator.return?.(undefined as never)).catch(
            () => undefined,
          );
        } finally {
          await run(Scope.close(spanScope, spanExit));
        }
      }
    };

    const streamingImplementation = (entry: GrpcMethodEntry) => {
      const streaming = (
        options.streamingHandlers ?? emptyStreamingHandlers
      ).get(entry.tag);
      if (!streaming || streaming.kind !== entry.kind) {
        return missingStreamingImplementation(entry);
      }
      return streaming.kind === "client-streaming"
        ? (requests: AsyncIterable<unknown>, context: HandlerContext) =>
            handleClientStreaming(entry, streaming, requests, context)
        : (requests: AsyncIterable<unknown>, context: HandlerContext) =>
            handleBidiStreaming(entry, streaming, requests, context);
    };

    const routes = (router: ConnectRouter) => {
      for (const [service, entries] of groupByService(options.registry)) {
        const implementation: Record<string, unknown> = {};
        for (const entry of entries) {
          switch (entry.kind) {
            case "unary":
              implementation[entry.localName] = (
                request: unknown,
                context: HandlerContext,
              ) => handleUnary(entry, request, context);
              break;
            case "server-streaming":
              implementation[entry.localName] = (
                request: unknown,
                context: HandlerContext,
              ) => handleServerStreaming(entry, request, context);
              break;
            case "client-streaming":
            case "bidi-streaming":
              implementation[entry.localName] = streamingImplementation(entry);
              break;
          }
        }
        router.service(service as never, implementation as never);
      }
      return router;
    };

    return { protocol, routes };
  });

const emptyStreamingHandlers: GrpcStreamingHandlers = new Map();

const streamingServerContext = (
  headers: ReadonlyArray<readonly [string, string]>,
): GrpcServerContext => ({
  client: new ServerClient(0),
  requestId: RequestId(0n),
  metadata: GrpcMetadata.fromHeaders(headers),
});

const streamingRequests = (
  entry: GrpcMethodEntry,
  requests: AsyncIterable<unknown>,
  signal: AbortSignal,
): Stream.Stream<unknown, GrpcStatusError.GrpcStatusError> => {
  const codecs = entryCodecs(entry);
  return Stream.fromAsyncIterable(requests, (cause) =>
    GrpcStatusError.fromConnectError(cause),
  ).pipe(
    Stream.mapEffect((message) =>
      Effect.try({
        try: () =>
          codecs.decodePayload(entry.fromGrpcRequest(message as never)),
        catch: (cause) =>
          GrpcStatusError.invalidArgument(
            "Invalid gRPC request payload",
            cause,
          ),
      }),
    ),
    // connect-node surfaces a client cancellation as a clean end of the
    // request iterable plus an aborted handler signal; distinguish it from a
    // half-close so handlers do not treat a truncated stream as complete.
    Stream.concat(
      Stream.suspend(() =>
        signal.aborted
          ? Stream.fail(GrpcStatusError.cancelled("RPC cancelled"))
          : Stream.empty,
      ),
    ),
  );
};

const encodeStreamingResponse = (entry: GrpcMethodEntry, value: unknown) =>
  Effect.try({
    try: () => entry.toGrpcResponse(entryCodecs(entry).encodeSuccess(value)),
    catch: (cause) =>
      GrpcStatusError.internal("Invalid gRPC response payload", cause),
  });

const streamingCauseError = (
  cause: Cause.Cause<GrpcStatusError.GrpcStatusError>,
): GrpcStatusError.GrpcStatusError =>
  Option.getOrElse(Cause.findErrorOption(cause), () =>
    Cause.hasInterrupts(cause)
      ? GrpcStatusError.cancelled("RPC cancelled")
      : GrpcStatusError.internal("RPC handler defect", Cause.squash(cause)),
  );

const streamingRejectionError = (
  cause: unknown,
  signal: AbortSignal,
): GrpcStatusError.GrpcStatusError =>
  cause instanceof GrpcStatusError.GrpcStatusError
    ? cause
    : signal.aborted
      ? GrpcStatusError.cancelled("RPC cancelled", cause)
      : GrpcStatusError.internal("RPC handler defect", cause);

const missingStreamingImplementation = (entry: GrpcMethodEntry) => {
  const error = () =>
    GrpcStatusError.toConnectError(
      GrpcStatusError.unimplemented(
        `Missing streaming handler for ${entry.tag}`,
      ),
    );
  return entry.kind === "client-streaming"
    ? () => Promise.reject(error())
    : (): AsyncIterable<unknown> => ({
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(error()),
        }),
      });
};

const groupByService = (registry: GrpcMethodRegistry) => {
  const groups = new Map<GrpcMethodEntry["service"], Array<GrpcMethodEntry>>();
  for (const entry of registry.values()) {
    const group = groups.get(entry.service);
    if (group) {
      group.push(entry);
    } else {
      groups.set(entry.service, [entry]);
    }
  }
  return groups;
};

const fromGrpcRequest = (entry: GrpcMethodEntry, request: unknown) => {
  try {
    return entry.fromGrpcRequest(request as never);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
    );
  }
};

const validatePayload = (entry: GrpcMethodEntry, payload: unknown) => {
  try {
    Schema.decodeUnknownSync(Schema.toCodecJson(entry.payloadSchema))(payload);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
    );
  }
};

const toGrpcResponse = (entry: GrpcMethodEntry, value: unknown) => {
  try {
    return entry.toGrpcResponse(value);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.internal("Invalid gRPC response payload", cause),
    );
  }
};
