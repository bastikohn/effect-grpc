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
import { TraceState } from "./GrpcTracing.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import * as MethodRegistry from "./GrpcMethodRegistry.js";
import * as CallState from "./internal/callState.js";
import { eof, requestId } from "./internal/effectRpc.js";
import { errorFromExit } from "./internal/status.js";
import * as StreamBridge from "./internal/streamBridge.js";
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
      // `RpcServer` spawns the handler fiber with this request fiber's
      // context, so providing the reference here rehydrates the incoming
      // `tracestate` into the handler's context, where downstream client
      // calls pick it up for header injection.
      const traceState = GrpcTracing.traceStateFromHeaders(headers);
      const message = writeRequest(clientId, {
        _tag: "Request",
        id: requestId,
        tag: entry.tag,
        payload,
        headers,
        ...GrpcTracing.traceFields(span),
      }).pipe(Effect.withParentSpan(span));
      return run(
        traceState === undefined
          ? message
          : Effect.provideService(message, TraceState, traceState),
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
      const outcome = await run(
        Effect.gen(function* () {
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
          const result = yield* Effect.promise(() =>
            handleUnaryNative(entry, request, context, headers, span),
          );
          // Per semconv, a server span ends in an error state only for
          // server-fault codes; other failures still record their status
          // attributes but close the span cleanly.
          if (!result.ok && GrpcTracing.isServerError(result.error.code)) {
            return yield* Effect.fail(result.error);
          }
          return result;
        }).pipe(
          Effect.withSpan(
            GrpcTracing.spanName(entry),
            GrpcTracing.serverSpanOptions(
              entry,
              GrpcTracing.externalSpanFromHeaders(headers),
            ),
          ),
          Effect.catch((error) =>
            Effect.succeed<ServerCallOutcome>({ ok: false, error }),
          ),
        ),
      );
      if (!outcome.ok) {
        throw GrpcStatusError.toConnectError(outcome.error);
      }
      return outcome.value;
    };

    const handleUnaryNative = async (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
      headers: ReadonlyArray<[string, string]>,
      span: Tracer.Span,
    ): Promise<ServerCallOutcome> => {
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
        return { ok: true, value: grpcResponse };
      } catch (cause) {
        const error = GrpcStatusError.fromConnectError(cause);
        recordStatus(error.code);
        return { ok: false, error };
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
      let failed = false;
      const recordStatus = serverRecorder(entry, span);
      const recordFailure = (error: GrpcStatusError.GrpcStatusError) => {
        failed = true;
        recordStatus(error.code);
        // Per semconv, only server-fault codes end the server span in an
        // error state; other failures record attributes but close cleanly.
        if (GrpcTracing.isServerError(error.code)) {
          spanExit = Exit.fail(error);
        }
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
            if (!failed) {
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
      const traceState = GrpcTracing.traceStateFromHeaders(headers);
      let record: GrpcTracing.StatusRecorder | undefined;
      let outcome: ServerCallOutcome;
      try {
        outcome = await run(
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
                  MethodRegistry.encodeResponse(entry, value),
                ),
                Effect.exit,
              );
            if (result._tag === "Failure") {
              const error = streamingCauseError(result.cause);
              recordStatus(error.code);
              // Per semconv, only server-fault codes end the server span in
              // an error state.
              if (GrpcTracing.isServerError(error.code)) {
                return yield* Effect.fail(error);
              }
              return { ok: false, error } satisfies ServerCallOutcome;
            }
            recordStatus("ok");
            return {
              ok: true,
              value: result.value,
            } satisfies ServerCallOutcome;
          }).pipe(
            Effect.withSpan(
              GrpcTracing.spanName(entry),
              GrpcTracing.serverSpanOptions(
                entry,
                GrpcTracing.externalSpanFromHeaders(headers),
              ),
            ),
            (effect) =>
              traceState === undefined
                ? effect
                : Effect.provideService(effect, TraceState, traceState),
            Effect.catch((error) =>
              Effect.succeed<ServerCallOutcome>({ ok: false, error }),
            ),
          ),
          { signal: handlerContext.signal },
        );
      } catch (cause) {
        const error = streamingRejectionError(cause, handlerContext.signal);
        record?.(error.code);
        throw GrpcStatusError.toConnectError(error);
      }
      if (!outcome.ok) {
        throw GrpcStatusError.toConnectError(outcome.error);
      }
      return outcome.value;
    };

    const handleBidiStreaming = async function* (
      entry: GrpcMethodEntry,
      streaming: GrpcBidiStreamingHandler,
      requests: AsyncIterable<unknown>,
      handlerContext: HandlerContext,
    ): AsyncIterable<unknown> {
      const headers = Array.from(handlerContext.requestHeader.entries());
      const traceState = GrpcTracing.traceStateFromHeaders(headers);
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
          Stream.mapEffect((value) =>
            MethodRegistry.encodeResponse(entry, value),
          ),
        );
      const handlerFiberContext = Context.add(
        traceState === undefined
          ? context
          : Context.add(context, TraceState, traceState),
        Tracer.ParentSpan,
        span,
      );
      // Closing the pump interrupts the handler fiber, so a pending pull
      // settles when the client goes away mid-stream.
      const pump = StreamBridge.responsePump(
        responses,
        handlerFiberContext,
        handlerContext.signal,
      );

      try {
        while (true) {
          const next = await pump.next();
          if (next.done) break;
          yield next.value;
        }
        completed = true;
        recordStatus(handlerContext.signal.aborted ? "cancelled" : "ok");
      } catch (cause) {
        completed = true;
        const error = streamingRejectionError(cause, handlerContext.signal);
        recordStatus(error.code);
        // Per semconv, only server-fault codes end the server span in an
        // error state; a cancelled or otherwise client-caused end closes
        // the span cleanly with the status attributes recorded.
        if (GrpcTracing.isServerError(error.code)) {
          spanExit = Exit.fail(error);
        }
        throw GrpcStatusError.toConnectError(error);
      } finally {
        if (!completed) {
          recordStatus("cancelled");
        }
        try {
          await pump.close();
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
      for (const [service, entries] of MethodRegistry.groupByService(
        options.registry,
      )) {
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

/**
 * Result of a spanned server call. Failures are carried as values so the
 * span can close cleanly for non-server-fault codes while the error is still
 * thrown to connect after the span has ended.
 */
type ServerCallOutcome =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly error: GrpcStatusError.GrpcStatusError };

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
): Stream.Stream<unknown, GrpcStatusError.GrpcStatusError> =>
  StreamBridge.requestStream({
    requests,
    signal,
    onError: (cause) => GrpcStatusError.fromConnectError(cause),
    onCancelled: () => GrpcStatusError.cancelled("RPC cancelled"),
  }).pipe(
    Stream.mapEffect((message) => MethodRegistry.decodeRequest(entry, message)),
  );

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
