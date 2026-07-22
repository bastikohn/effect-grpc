import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import {
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Option,
  Scope,
  Stream,
} from "effect";
import * as Tracer from "effect/Tracer";

import * as CodegenSupport from "./CodegenSupport.js";
import type { GrpcServerContext } from "./CodegenSupport.js";
import { TraceState } from "./GrpcTracing.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import * as MethodRegistry from "./GrpcMethodRegistry.js";
import * as StreamBridge from "./internal/streamBridge.js";
import * as GrpcTracing from "./internal/tracing.js";

export interface GrpcServerProtocolOptions {
  readonly registry: GrpcMethodRegistry;
  readonly handlers?: GrpcHandlers;
}

/**
 * The single server-side handler seam: one entry per method tag, covering all
 * four gRPC call shapes. Effect-shaped kinds (unary, client-streaming) return
 * an `Effect`; stream-shaped kinds (server-streaming, bidi-streaming) return a
 * `Stream`. Values are domain values — the protocol owns codecs via the
 * method registry.
 */
export interface GrpcUnaryHandler<R = never> {
  readonly kind: "unary";
  readonly handler: (
    request: unknown,
    context: GrpcServerContext,
  ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError, R>;
}

export interface GrpcServerStreamingHandler<R = never> {
  readonly kind: "server-streaming";
  readonly handler: (
    request: unknown,
    context: GrpcServerContext,
  ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError, R>;
}

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

export type GrpcHandler<R = never> =
  | GrpcUnaryHandler<R>
  | GrpcServerStreamingHandler<R>
  | GrpcClientStreamingHandler<R>
  | GrpcBidiStreamingHandler<R>;

export type GrpcHandlers = ReadonlyMap<string, GrpcHandler>;

/**
 * Carries the handlers of a generated service inside its handlers layer, so
 * `GrpcNodeServer.serveAll` can collect them without changing the
 * user-facing service wiring.
 */
export const GrpcHandlers = Context.Service<GrpcHandlers>(
  "@effect-grpc/effect-grpc/GrpcHandlers",
);

/**
 * Builds the layer generated `*HandlersLayer` functions use to publish their
 * handlers. Captures the context so handler requirements `R` are resolved
 * where the layer is built.
 */
export const handlersLayer = <R = never>(
  handlers: Record<string, GrpcHandler<R>>,
): Layer.Layer<GrpcHandlers, never, R> =>
  Layer.effect(
    GrpcHandlers,
    Effect.gen(function* () {
      const context = yield* Effect.context<R>();
      return new Map(
        Object.entries(handlers).map(([tag, handler]) => [
          tag,
          bindHandler(handler, context),
        ]),
      );
    }),
  );

const bindHandler = <R>(
  entry: GrpcHandler<R>,
  context: Context.Context<R>,
): GrpcHandler => {
  switch (entry.kind) {
    case "unary":
      return {
        kind: entry.kind,
        handler: (request, serverContext) =>
          Effect.provideContext(entry.handler(request, serverContext), context),
      };
    case "server-streaming":
      return {
        kind: entry.kind,
        handler: (request, serverContext) =>
          Stream.provideContext(entry.handler(request, serverContext), context),
      };
    case "client-streaming":
      return {
        kind: entry.kind,
        handler: (requests, serverContext) =>
          Effect.provideContext(
            entry.handler(requests, serverContext),
            context,
          ),
      };
    case "bidi-streaming":
      return {
        kind: entry.kind,
        handler: (requests, serverContext) =>
          Stream.provideContext(
            entry.handler(requests, serverContext),
            context,
          ),
      };
  }
};

export interface GrpcServerProtocolResult {
  readonly routes: (router: ConnectRouter) => ConnectRouter;
}

export const make = (
  options: GrpcServerProtocolOptions,
): Effect.Effect<GrpcServerProtocolResult> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<never>();
    const run = Effect.runPromiseWith(context);
    const serverRecorder = (entry: GrpcMethodEntry, span: Tracer.Span) =>
      GrpcTracing.serverCallRecorder({ entry, span, context });
    const handlers = options.handlers ?? emptyHandlers;

    /**
     * Execution template for effect-shaped calls (unary, client-streaming):
     * one server span, semconv status recording, and non-server-fault
     * failures carried as values so the span closes cleanly before the error
     * reaches connect. The connect `signal` interrupts only the handler body
     * (raced against {@link abortCancelled}), never the surrounding spanned
     * effect: a client abort must record its `cancelled` status while the
     * span is still open — exporters serialize a span when it ends, so
     * attributes written after an interrupt-torn span end are lost.
     */
    const handleEffectCall = async (
      entry: GrpcMethodEntry,
      handlerContext: HandlerContext,
      body: (
        serverContext: GrpcServerContext,
      ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>,
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
            const result = yield* Effect.raceFirst(
              body(CodegenSupport.serverContext(headers)),
              abortCancelled(handlerContext.signal),
            ).pipe(Effect.exit);
            if (result._tag === "Failure") {
              const error = causeError(result.cause);
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
        );
      } catch (cause) {
        const error = rejectionError(cause, handlerContext.signal);
        record?.(error.code);
        throw GrpcStatusError.toConnectError(error);
      }
      if (!outcome.ok) {
        throw GrpcStatusError.toConnectError(outcome.error);
      }
      return outcome.value;
    };

    /**
     * Execution template for stream-shaped calls (server-streaming,
     * bidi-streaming): a scoped server span, semconv status recording, and
     * the response stream pulled through `StreamBridge.responsePump` so
     * demand follows connect's iteration (HTTP/2 flow control) and the
     * handler fiber is interrupted when the client goes away.
     */
    const handleStreamCall = async function* (
      entry: GrpcMethodEntry,
      handlerContext: HandlerContext,
      body: (
        serverContext: GrpcServerContext,
      ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
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
      const responses = body(CodegenSupport.serverContext(headers));
      // The pump spawns the handler fiber with this context, so the scoped
      // span parents the handler's spans and the incoming `tracestate` is
      // rehydrated for downstream client calls to pick up.
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
        // The pump surfaces the handler stream's real `Cause` so the shared
        // mapper sees interrupts as interrupts (-> `cancelled`), not as a
        // squashed generic error (-> `internal`).
        const error =
          cause instanceof StreamBridge.PumpFailure
            ? causeError(cause.cause)
            : rejectionError(cause, handlerContext.signal);
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

    // The four connect adapters below stay separate because connect imposes
    // four handler signatures (Promise vs async-generator, message vs
    // iterable); each one is a thin binding of an entry and its handler onto
    // one of the two execution templates.

    const unaryImplementation =
      (entry: GrpcMethodEntry, handler: GrpcUnaryHandler) =>
      (request: unknown, handlerContext: HandlerContext) =>
        handleEffectCall(entry, handlerContext, (serverContext) =>
          MethodRegistry.decodeRequest(entry, request).pipe(
            Effect.flatMap((decoded) =>
              handler.handler(decoded, serverContext),
            ),
            Effect.flatMap((value) =>
              MethodRegistry.encodeResponse(entry, value),
            ),
          ),
        );

    const serverStreamingImplementation =
      (entry: GrpcMethodEntry, handler: GrpcServerStreamingHandler) =>
      (request: unknown, handlerContext: HandlerContext) =>
        handleStreamCall(entry, handlerContext, (serverContext) =>
          Stream.unwrap(
            MethodRegistry.decodeRequest(entry, request).pipe(
              Effect.map((decoded) => handler.handler(decoded, serverContext)),
            ),
          ).pipe(
            Stream.mapEffect((value) =>
              MethodRegistry.encodeResponse(entry, value),
            ),
          ),
        );

    const clientStreamingImplementation =
      (entry: GrpcMethodEntry, handler: GrpcClientStreamingHandler) =>
      (requests: AsyncIterable<unknown>, handlerContext: HandlerContext) =>
        handleEffectCall(entry, handlerContext, (serverContext) =>
          handler
            .handler(
              decodedRequestStream(entry, requests, handlerContext.signal),
              serverContext,
            )
            .pipe(
              Effect.flatMap((value) =>
                MethodRegistry.encodeResponse(entry, value),
              ),
            ),
        );

    const bidiStreamingImplementation =
      (entry: GrpcMethodEntry, handler: GrpcBidiStreamingHandler) =>
      (requests: AsyncIterable<unknown>, handlerContext: HandlerContext) =>
        handleStreamCall(entry, handlerContext, (serverContext) =>
          handler
            .handler(
              decodedRequestStream(entry, requests, handlerContext.signal),
              serverContext,
            )
            .pipe(
              Stream.mapEffect((value) =>
                MethodRegistry.encodeResponse(entry, value),
              ),
            ),
        );

    const methodImplementation = (entry: GrpcMethodEntry) => {
      const handler = handlers.get(entry.tag);
      if (!handler || handler.kind !== entry.kind) {
        return missingImplementation(entry);
      }
      switch (handler.kind) {
        case "unary":
          return unaryImplementation(entry, handler);
        case "server-streaming":
          return serverStreamingImplementation(entry, handler);
        case "client-streaming":
          return clientStreamingImplementation(entry, handler);
        case "bidi-streaming":
          return bidiStreamingImplementation(entry, handler);
      }
    };

    const routes = (router: ConnectRouter) => {
      for (const [service, entries] of MethodRegistry.groupByService(
        options.registry,
      )) {
        const implementation: Record<string, unknown> = {};
        for (const entry of entries) {
          implementation[entry.localName] = methodImplementation(entry);
        }
        router.service(service as never, implementation as never);
      }
      return router;
    };

    return { routes };
  });

const emptyHandlers: GrpcHandlers = new Map();

/**
 * Result of a spanned server call. Failures are carried as values so the
 * span can close cleanly for non-server-fault codes while the error is still
 * thrown to connect after the span has ended.
 */
type ServerCallOutcome =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly error: GrpcStatusError.GrpcStatusError };

const decodedRequestStream = (
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

/**
 * Fails with `cancelled` when the connect signal aborts, and never otherwise.
 * Raced against the handler body in `handleEffectCall` so a client abort
 * interrupts only the body fiber: the surrounding spanned effect survives to
 * record the `cancelled` status while the span is still open and to close the
 * span cleanly instead of tearing it down with an interrupt.
 */
const abortCancelled = (
  signal: AbortSignal,
): Effect.Effect<never, GrpcStatusError.GrpcStatusError> =>
  Effect.callback((resume) => {
    const onAbort = () =>
      resume(Effect.fail(GrpcStatusError.cancelled("RPC cancelled")));
    if (signal.aborted) {
      onAbort();
      return;
    }
    signal.addEventListener("abort", onAbort, { once: true });
    return Effect.sync(() => signal.removeEventListener("abort", onAbort));
  });

/**
 * The single cause -> gRPC status mapper for every call shape: effect-shaped
 * calls run their handler's exit cause through it directly, and stream-shaped
 * calls feed it the cause surfaced by `StreamBridge.responsePump` (as a
 * {@link StreamBridge.PumpFailure}). An interrupt-only cause — a handler
 * interrupting itself — maps to `cancelled`, a `GrpcStatusError` failure
 * keeps its code, and everything else is a server fault (`internal`).
 */
const causeError = (
  cause: Cause.Cause<unknown>,
): GrpcStatusError.GrpcStatusError => {
  const failure = Option.getOrUndefined(Cause.findErrorOption(cause));
  if (failure !== undefined) {
    return failure instanceof GrpcStatusError.GrpcStatusError
      ? failure
      : GrpcStatusError.internal("RPC handler defect", failure);
  }
  return Cause.hasInterrupts(cause)
    ? GrpcStatusError.cancelled("RPC cancelled")
    : GrpcStatusError.internal("RPC handler defect", Cause.squash(cause));
};

const rejectionError = (
  cause: unknown,
  signal: AbortSignal,
): GrpcStatusError.GrpcStatusError =>
  cause instanceof GrpcStatusError.GrpcStatusError
    ? cause
    : signal.aborted
      ? GrpcStatusError.cancelled("RPC cancelled", cause)
      : GrpcStatusError.internal("RPC handler defect", cause);

const missingImplementation = (entry: GrpcMethodEntry) => {
  const error = () =>
    GrpcStatusError.toConnectError(
      GrpcStatusError.unimplemented(`Missing handler for ${entry.tag}`),
    );
  return entry.kind === "unary" || entry.kind === "client-streaming"
    ? () => Promise.reject(error())
    : (): AsyncIterable<unknown> => ({
        [Symbol.asyncIterator]: () => ({
          next: () => Promise.reject(error()),
        }),
      });
};
