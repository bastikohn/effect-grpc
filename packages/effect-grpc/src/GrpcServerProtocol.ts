import {
  Code,
  ConnectError,
  type ConnectRouter,
  type HandlerContext,
} from "@connectrpc/connect";
import { Cause, Context, Effect, Exit, Option, Scope, Stream } from "effect";
import * as Tracer from "effect/Tracer";

import type { GrpcServerContext } from "./CodegenSupport.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import type {
  GrpcMethodEntry,
  GrpcMethodKind,
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
 * Builds what generated `*Handlers` functions return. Captures the context so
 * handler requirements `R` are resolved where the effect runs; request-local
 * services provided per call (the server span) take precedence over the
 * capture.
 *
 * **Do not provide handler dependencies to this effect.** It completes as soon
 * as it has read the context, so `Effect.provide(handlersEffect(...), deps)`
 * builds `deps` in a scope that closes immediately: a scoped dependency is
 * acquired, released, and only then handed to `serveAll`, and every request
 * then runs against a finalized resource with no error at the seam. Leave `R`
 * unprovided so the requirement propagates through `GrpcNodeServer.serveAll`,
 * and provide it to the whole server program — whose scope is the server's
 * lifetime — instead.
 */
export const handlersEffect = <R = never>(
  handlers: Record<string, GrpcHandler<R>>,
): Effect.Effect<GrpcHandlers, never, R> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<R>();
    return new Map(
      Object.entries(handlers).map(([tag, handler]) => [
        tag,
        bindHandler(handler, context),
      ]),
    );
  });

const bindHandler = <R>(
  entry: GrpcHandler<R>,
  context: Context.Context<R>,
): GrpcHandler => {
  // The captured context carries the whole ambient build-time context, not
  // just `R` — merge it *beneath* the per-call context so request-local
  // services (the server span) stay authoritative over whatever happened to
  // be in scope where the handlers effect ran.
  const merge = (callContext: Context.Context<never>) =>
    Context.merge(context, callContext);
  // The two `updateContext` overloads have the same shape; which one applies
  // is decided by whether the kind returns an `Effect` or a `Stream`.
  const update = (
    isEffectKind(entry.kind) ? Effect.updateContext : Stream.updateContext
  ) as (self: unknown, f: typeof merge) => unknown;
  return {
    kind: entry.kind,
    handler: (request: never, serverContext: GrpcServerContext) =>
      update(entry.handler(request, serverContext), merge),
  } as GrpcHandler;
};

/** Whether a call kind is carried by an `Effect` rather than a `Stream`. */
const isEffectKind = (kind: GrpcMethodKind): boolean =>
  kind === "unary" || kind === "client-streaming";

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
     * (raced against {@link abortFailure}), never the surrounding spanned
     * effect: a signal abort must record its status while the
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
      let record: GrpcTracing.StatusRecorder | undefined;
      let outcome: ServerCallOutcome;
      try {
        outcome = await run(
          Effect.gen(function* () {
            const span = yield* Effect.currentSpan.pipe(Effect.orDie);
            const recordStatus = serverRecorder(entry, span);
            record = recordStatus;
            const result = yield* Effect.raceFirst(
              body({ metadata: GrpcMetadata.fromHeaders(headers) }),
              abortFailure(handlerContext.signal),
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
      // The stream adapters invoke user handler code eagerly, so `body` can
      // throw synchronously. `Stream.suspend` moves that throw into the
      // stream's cause channel, where the pump normalizes it (-> INTERNAL)
      // and the `finally` below still closes the span scope.
      const responses = Stream.suspend(() =>
        body({ metadata: GrpcMetadata.fromHeaders(headers) }),
      );
      // The pump spawns the handler fiber with this context, so the scoped
      // span parents the handler's spans.
      const handlerFiberContext = Context.add(context, Tracer.ParentSpan, span);
      // Closing the pump interrupts the handler fiber, so a pending pull
      // settles when the client goes away mid-stream.
      const pump = StreamBridge.responsePump(
        responses,
        handlerFiberContext,
        handlerContext.signal,
      );

      // Records the status of a signal-aborted or abandoned call: a deadline
      // expiry is a server fault and must also end the span in an error
      // state, while a client cancellation closes it cleanly.
      const recordAbort = () => {
        const error = abortError(handlerContext.signal);
        recordStatus(error.code);
        if (GrpcTracing.isServerError(error.code)) {
          spanExit = Exit.fail(error);
        }
      };

      try {
        while (true) {
          const next = await pump.next();
          if (next.done) break;
          yield next.value;
        }
        completed = true;
        if (handlerContext.signal.aborted) {
          recordAbort();
        } else {
          recordStatus("ok");
        }
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
          recordAbort();
        }
        try {
          await pump.close();
        } finally {
          await run(Scope.close(spanScope, spanExit));
        }
      }
    };

    // connect imposes four handler signatures, but the only real axis is how
    // the response is shaped (Promise vs async-generator); the request axis
    // is folded into {@link handlerInput}, which yields either the decoded
    // message or the decoded request stream.

    const effectImplementation =
      (entry: GrpcMethodEntry, handler: GrpcHandler) =>
      (request: unknown, handlerContext: HandlerContext) =>
        handleEffectCall(entry, handlerContext, (serverContext) =>
          handlerInput(entry, request, handlerContext.signal).pipe(
            Effect.flatMap((input) =>
              (handler.handler as EffectHandler)(input, serverContext),
            ),
            Effect.flatMap((value) =>
              MethodRegistry.encodeResponse(entry, value),
            ),
          ),
        );

    const streamImplementation =
      (entry: GrpcMethodEntry, handler: GrpcHandler) =>
      (request: unknown, handlerContext: HandlerContext) =>
        handleStreamCall(entry, handlerContext, (serverContext) =>
          Stream.unwrap(
            handlerInput(entry, request, handlerContext.signal).pipe(
              Effect.map((input) =>
                (handler.handler as StreamHandler)(input, serverContext),
              ),
            ),
          ).pipe(
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
      return isEffectKind(handler.kind)
        ? effectImplementation(entry, handler)
        : streamImplementation(entry, handler);
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

type EffectHandler = (
  input: unknown,
  context: GrpcServerContext,
) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>;

type StreamHandler = (
  input: unknown,
  context: GrpcServerContext,
) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>;

/**
 * The value a handler is called with: message-request kinds get the decoded
 * request, stream-request kinds the decoded request stream.
 */
const handlerInput = (
  entry: GrpcMethodEntry,
  request: unknown,
  signal: AbortSignal,
): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError> =>
  entry.kind === "unary" || entry.kind === "server-streaming"
    ? MethodRegistry.decodeRequest(entry, request)
    : Effect.succeed(
        decodedRequestStream(entry, request as AsyncIterable<unknown>, signal),
      );

const decodedRequestStream = (
  entry: GrpcMethodEntry,
  requests: AsyncIterable<unknown>,
  signal: AbortSignal,
): Stream.Stream<unknown, GrpcStatusError.GrpcStatusError> =>
  StreamBridge.requestStream({
    requests,
    signal,
    onError: (cause) => GrpcStatusError.fromConnectError(cause),
    onCancelled: () => abortError(signal),
  }).pipe(
    Stream.mapEffect((message) => MethodRegistry.decodeRequest(entry, message)),
  );

/**
 * The gRPC status for an aborted connect signal. connect-node enforces the
 * incoming `grpc-timeout` by aborting the handler signal with a
 * `deadline_exceeded` `ConnectError` as the abort reason; any other abort is
 * a client cancellation.
 */
const abortError = (
  signal: AbortSignal,
  cause?: unknown,
): GrpcStatusError.GrpcStatusError => {
  const reason: unknown = signal.reason;
  return reason instanceof ConnectError && reason.code === Code.DeadlineExceeded
    ? GrpcStatusError.deadlineExceeded("RPC deadline exceeded", cause ?? reason)
    : GrpcStatusError.cancelled("RPC cancelled", cause);
};

/**
 * Fails with the signal's abort status ({@link abortError}) when the connect
 * signal aborts, and never otherwise. Raced against the handler body in
 * `handleEffectCall` so a signal abort interrupts only the body fiber: the
 * surrounding spanned effect survives to record the status while the span is
 * still open and to close the span with the right exit instead of tearing it
 * down with an interrupt.
 */
const abortFailure = (
  signal: AbortSignal,
): Effect.Effect<never, GrpcStatusError.GrpcStatusError> =>
  Effect.callback((resume) => {
    const onAbort = () => resume(Effect.fail(abortError(signal)));
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
      ? abortError(signal, cause)
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
