import { createClient, type CallOptions } from "@connectrpc/connect";
import { Effect, Scope, Stream } from "effect";
import type * as Tracer from "effect/Tracer";

import type { GrpcCallOptions } from "../CodegenSupport.js";
import type {
  GrpcConnectInvokerOptions,
  GrpcInvokerService,
} from "../GrpcInvoker.js";
import * as GrpcMetadata from "../GrpcMetadata.js";
import * as MethodRegistry from "../GrpcMethodRegistry.js";
import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "../GrpcStatusCode.js";
import * as GrpcStatusError from "../GrpcStatusError.js";
import { callTimeoutMs, unknownTag, validateCallMetadata } from "./invoker.js";
import * as StreamBridge from "./streamBridge.js";
import * as GrpcTracing from "./tracing.js";

/**
 * Production {@link GrpcInvokerService}: resolves the native connect client
 * method, translates normalized call options, delegates stream mechanics to
 * `streamBridge`, and maps connect failures to gRPC status. Tracing records
 * one semantic call outcome per invocation.
 */
export const makeConnect = (
  options: GrpcConnectInvokerOptions,
): Effect.Effect<GrpcInvokerService> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<never>();
    const transport = options.transport;

    // One connect client per service descriptor, built on first use. The
    // transport is fixed for the invoker, so the cache is too.
    const clients = new Map<
      GrpcMethodEntry["service"],
      Record<string, unknown>
    >();

    /**
     * @throws `Error` when the entry names a method the service descriptor
     * does not declare. Registry entries carry `localName` verbatim — the
     * built-in services hand-write it — so a mismatch is a wiring defect, not
     * a status a caller could act on. The call is still recorded as
     * `unimplemented` first: the defect kills the fiber, which would
     * otherwise close the span OK, attributeless and without a duration
     * observation, hiding the broken wiring from telemetry entirely.
     */
    const resolveMethod = (
      entry: GrpcMethodEntry,
      record: GrpcTracing.StatusRecorder,
    ) => {
      let client = clients.get(entry.service);
      if (!client) {
        client = createClient(entry.service, transport) as Record<
          string,
          unknown
        >;
        clients.set(entry.service, client);
      }
      const method = client[entry.localName];
      if (typeof method !== "function") {
        record("unimplemented");
        throw new Error(
          `gRPC client for ${entry.service.typeName} has no method '${entry.localName}' (tag ${entry.tag})`,
        );
      }
      return method.bind(client) as (
        input: unknown,
        options?: CallOptions,
      ) => unknown;
    };

    const openRequests = (
      entry: GrpcMethodEntry,
      requests: Stream.Stream<unknown, unknown>,
      controller: AbortController,
    ) =>
      StreamBridge.requestPump(
        Stream.mapEffect(requests, (value) =>
          MethodRegistry.encodeRequest(entry, value),
        ),
        context,
        () => controller.abort(),
      );

    // `Effect.suspend` keeps the status recorder execution-local, so
    // re-running the returned effect cannot share mutable call state across
    // executions. The span is scope-managed (not `Effect.withSpan`) so it
    // can close with an exit computed from the recorded status: per semconv
    // every non-OK client status is an error — including cancellation, which
    // surfaces as interruption and would otherwise end the span with an
    // interrupt-only exit that exporters map to OK.
    const withCallSpanEffect = <A, E>(
      entry: GrpcMethodEntry,
      body: (call: CallSpan) => Effect.Effect<A, E>,
    ): Effect.Effect<A, E> =>
      Effect.suspend(() => {
        let finalCode: GrpcStatusCode | undefined;
        return Effect.uninterruptibleMask((restore) =>
          Effect.gen(function* () {
            const spanScope = yield* Scope.make();
            const call = yield* makeCallSpan(
              entry,
              spanScope,
              (code) => (finalCode ??= code),
            );
            return yield* restore(
              body(call).pipe(
                Effect.onInterrupt(() =>
                  Effect.sync(() => call.record("cancelled")),
                ),
                Effect.withParentSpan(call.span),
              ),
            ).pipe(
              Effect.onExit(() =>
                Scope.close(spanScope, GrpcTracing.clientSpanExit(finalCode)),
              ),
            );
          }),
        );
      });

    // Stream variant: the span closes when the stream scope does, with an
    // exit computed from the recorded status — an early consumer close ends
    // the scope with a successful exit, but per semconv the resulting
    // CANCELLED status is an error on the client span. The setup effect is
    // uninterruptible so the span scope cannot leak between creation and
    // finalizer registration.
    const withCallSpanStream = <E>(
      entry: GrpcMethodEntry,
      body: (
        call: CallSpan,
      ) => Effect.Effect<Stream.Stream<unknown, E>, E, Scope.Scope>,
    ): Stream.Stream<unknown, E> =>
      Stream.unwrap(
        Effect.uninterruptible(
          Effect.gen(function* () {
            let finalCode: GrpcStatusCode | undefined;
            const spanScope = yield* Scope.make();
            const call = yield* makeCallSpan(
              entry,
              spanScope,
              (code) => (finalCode ??= code),
            );
            // Registered before the body's finalizers, so it runs after
            // them (LIFO) and closes the span with the final status.
            yield* Effect.addFinalizer(() =>
              Scope.close(spanScope, GrpcTracing.clientSpanExit(finalCode)),
            );
            // Body failures have already recorded their status; surface
            // them as a failing stream so consumption drives span closure.
            return yield* body(call).pipe(
              Effect.catch((error) =>
                Effect.succeed(Stream.fail(error) as Stream.Stream<unknown, E>),
              ),
            );
          }),
        ),
      );

    const makeCallSpan = (
      entry: GrpcMethodEntry,
      spanScope: Scope.Scope,
      onCode: (code: GrpcStatusCode) => void,
    ): Effect.Effect<CallSpan> =>
      Effect.gen(function* () {
        const span = yield* Effect.makeSpanScoped(
          GrpcTracing.spanName(entry),
          GrpcTracing.clientSpanOptions(entry, options.serverAddress),
        ).pipe(Scope.provide(spanScope));
        const baseRecord = GrpcTracing.clientCallRecorder({
          entry,
          span,
          context,
          serverAddress: options.serverAddress,
        });
        const record: GrpcTracing.StatusRecorder = (code) => {
          onCode(code);
          baseRecord(code);
        };
        return { span, record };
      });

    // Reject unsendable metadata up front — a reserved key, a key or value
    // no header can spell, a value contradicting its key's `-bin` suffix — so
    // it is a recorded `invalid_argument` on every shape rather than a
    // header-construction throw (a defect on streaming shapes, a generic
    // `unknown` on unary).
    const recordMetadata = (
      options: GrpcCallOptions | undefined,
      record: GrpcTracing.StatusRecorder,
    ) => validateCallMetadata(options).pipe(recorded(record));

    /**
     * Records a failing step's status on its way out. Every step of every
     * shape has to do this before propagating, so it is spelled once.
     */
    const recorded =
      (record: GrpcTracing.StatusRecorder) =>
      <A>(
        effect: Effect.Effect<A, GrpcStatusError.GrpcStatusError>,
      ): Effect.Effect<A, GrpcStatusError.GrpcStatusError> =>
        Effect.mapError(effect, (error) => {
          record(error.code);
          return error;
        });

    /** {@link recorded} for the response half of a stream-shaped call. */
    const recordedStream =
      (record: GrpcTracing.StatusRecorder) =>
      <A>(
        stream: Stream.Stream<A, GrpcStatusError.GrpcStatusError>,
      ): Stream.Stream<A, GrpcStatusError.GrpcStatusError> =>
        Stream.mapError(stream, (error) => {
          record(error.code);
          return error;
        });

    const invokeStream = (
      invoke: () => AsyncIterable<unknown>,
      record: GrpcTracing.StatusRecorder,
    ) =>
      Effect.try({
        try: invoke,
        catch: (cause) => {
          const error = GrpcStatusError.fromConnectError(cause);
          record(error.code);
          return error;
        },
      });

    const unary: GrpcInvokerService["unary"] = (tag, request, callOptions) => {
      const entry = MethodRegistry.lookup(options.registry, tag, "unary");
      if (!entry) return Effect.fail(unknownTag(tag));
      return withCallSpanEffect(entry, ({ span, record }) =>
        Effect.gen(function* () {
          yield* recordMetadata(callOptions, record);
          const method = resolveMethod(entry, record);
          const grpcRequest = yield* MethodRegistry.encodeRequest(
            entry,
            request,
          ).pipe(recorded(record));
          const value = yield* Effect.tryPromise({
            try: (signal) =>
              method(
                grpcRequest,
                callOptionsFor(callOptions, span, signal),
              ) as Promise<unknown>,
            catch: (cause) => {
              const error = GrpcStatusError.fromConnectError(cause);
              record(error.code);
              return error;
            },
          });
          const response = yield* MethodRegistry.decodeResponse(
            entry,
            value,
          ).pipe(recorded(record));
          record("ok");
          return response;
        }),
      );
    };

    const serverStream: GrpcInvokerService["serverStream"] = (
      tag,
      request,
      callOptions,
    ) => {
      const entry = MethodRegistry.lookup(
        options.registry,
        tag,
        "server-streaming",
      );
      if (!entry) return Stream.fail(unknownTag(tag));
      return withCallSpanStream<GrpcStatusError.GrpcStatusError>(
        entry,
        ({ span, record }) =>
          Effect.gen(function* () {
            yield* recordMetadata(callOptions, record);
            const method = resolveMethod(entry, record);
            const grpcRequest = yield* MethodRegistry.encodeRequest(
              entry,
              request,
            ).pipe(recorded(record));
            const controller = new AbortController();
            // An early consumer close aborts a live call, so the
            // finalizer's `cancelled` is correct for what remains; natural
            // completion and failures have already recorded first.
            yield* Effect.addFinalizer(() =>
              Effect.sync(() => {
                controller.abort();
                record("cancelled");
              }),
            );
            const responses = yield* invokeStream(
              () =>
                method(
                  grpcRequest,
                  callOptionsFor(callOptions, span, controller.signal),
                ) as AsyncIterable<unknown>,
              record,
            );
            return Stream.fromAsyncIterable(responses, (cause) =>
              GrpcStatusError.fromConnectError(cause),
            ).pipe(
              Stream.mapEffect((message) =>
                MethodRegistry.decodeResponse(entry, message),
              ),
              recordedStream(record),
              Stream.onEnd(Effect.sync(() => record("ok"))),
            );
          }),
      );
    };

    const clientStream: GrpcInvokerService["clientStream"] = <A, E>(
      tag: string,
      requests: Stream.Stream<A, E>,
      callOptions?: GrpcCallOptions,
    ) => {
      const entry = MethodRegistry.lookup(
        options.registry,
        tag,
        "client-streaming",
      );
      if (!entry) return Effect.fail(unknownTag(tag));
      return withCallSpanEffect<unknown, GrpcStatusError.GrpcStatusError | E>(
        entry,
        ({ span, record }) =>
          Effect.gen(function* () {
            yield* recordMetadata(callOptions, record);
            const method = resolveMethod(entry, record);
            const controller = new AbortController();
            const pump = openRequests(
              entry,
              requests as Stream.Stream<unknown, unknown>,
              controller,
            );
            const result = yield* Effect.promise(
              async (signal): Promise<CallResult> => {
                const abort = () => controller.abort();
                signal.addEventListener("abort", abort, { once: true });
                try {
                  const call = method(
                    pump.iterable,
                    callOptionsFor(callOptions, span, controller.signal),
                  ) as Promise<unknown>;
                  return { ok: true, value: await call };
                } catch (cause) {
                  return { ok: false, cause };
                } finally {
                  signal.removeEventListener("abort", abort);
                  await pump.close();
                }
              },
            );
            if (!result.ok) {
              const failure = pump.failure();
              if (failure) {
                record(streamingFailureCode(failure.error));
                return yield* Effect.fail(failure.error as E);
              }
              const error = GrpcStatusError.fromConnectError(result.cause);
              record(error.code);
              return yield* Effect.fail(error);
            }
            const response = yield* MethodRegistry.decodeResponse(
              entry,
              result.value,
            ).pipe(recorded(record));
            record("ok");
            return response;
          }),
      );
    };

    const bidiStream: GrpcInvokerService["bidiStream"] = <A, E>(
      tag: string,
      requests: Stream.Stream<A, E>,
      callOptions?: GrpcCallOptions,
    ) => {
      const entry = MethodRegistry.lookup(
        options.registry,
        tag,
        "bidi-streaming",
      );
      if (!entry) return Stream.fail(unknownTag(tag));
      return withCallSpanStream<GrpcStatusError.GrpcStatusError | E>(
        entry,
        ({ span, record }) =>
          Effect.gen(function* () {
            yield* recordMetadata(callOptions, record);
            const method = resolveMethod(entry, record);
            const controller = new AbortController();
            const pump = openRequests(
              entry,
              requests as Stream.Stream<unknown, unknown>,
              controller,
            );
            // `record` keeps only the first status. Natural completion and
            // failures record below; any earlier scope close (consumer
            // short-circuiting via `Stream.take`, interruption) aborts a
            // live call, so the finalizer's `cancelled` is correct for what
            // remains.
            yield* Effect.addFinalizer(() =>
              Effect.promise(async () => {
                controller.abort();
                await pump.close();
                record("cancelled");
              }),
            );
            const responses = yield* invokeStream(
              () =>
                method(
                  pump.iterable,
                  callOptionsFor(callOptions, span, controller.signal),
                ) as AsyncIterable<unknown>,
              record,
            );
            return StreamBridge.responseStream(
              responses,
              pump,
              (cause): GrpcStatusError.GrpcStatusError | E =>
                GrpcStatusError.fromConnectError(cause),
            ).pipe(
              Stream.mapEffect((message) =>
                MethodRegistry.decodeResponse(entry, message),
              ),
              Stream.mapError((error) => {
                record(streamingFailureCode(error));
                return error;
              }),
              Stream.onEnd(Effect.sync(() => record("ok"))),
            );
          }),
      );
    };

    return { unary, serverStream, clientStream, bidiStream };
  });

interface CallSpan {
  readonly span: Tracer.Span;
  readonly record: GrpcTracing.StatusRecorder;
}

type CallResult =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly cause: unknown };

const streamingFailureCode = (error: unknown): GrpcStatusCode =>
  error instanceof GrpcStatusError.GrpcStatusError ? error.code : "cancelled";

const callOptionsFor = (
  options: GrpcCallOptions | undefined,
  span: Tracer.Span,
  signal: AbortSignal,
): CallOptions => {
  // Metadata has already been validated by `recordMetadata`, so the codec
  // cannot be handed a value its key forbids.
  const headers = GrpcMetadata.toHeaders(
    options?.metadata ?? GrpcMetadata.empty,
  );
  if (
    !headers.has("traceparent") &&
    span.traceId !== "noop" &&
    span.spanId !== "noop"
  ) {
    headers.set("traceparent", GrpcTracing.traceparent(span));
  }
  const timeoutMs = callTimeoutMs(options);
  return { headers, signal, ...(timeoutMs === undefined ? {} : { timeoutMs }) };
};
