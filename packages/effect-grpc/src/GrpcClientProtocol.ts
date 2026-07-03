import type { CallOptions, Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import type { GrpcTransportOptions } from "@connectrpc/connect-node";
import * as RpcClient from "@effect/rpc/RpcClient";
import type {
  FromClientEncoded,
  FromServerEncoded,
} from "@effect/rpc/RpcMessage";
import {
  Cause,
  Context,
  Effect,
  Exit,
  Layer,
  Runtime,
  Scope,
  Stream,
} from "effect";
import type * as Tracer from "effect/Tracer";

import type { GrpcCallOptions } from "./CodegenSupport.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "./GrpcStatusCode.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import { entryCodecs } from "./internal/codec.js";
import { getClient } from "./internal/connect.js";
import {
  headersFromCallOptions,
  readTimeoutMs,
  stripInternalHeaders,
} from "./internal/metadata.js";
import { failureExit, successExit } from "./internal/status.js";
import * as GrpcTracing from "./internal/tracing.js";

export type { GrpcTransportOptions } from "@connectrpc/connect-node";

export interface GrpcClientProtocolOptions extends GrpcTransportOptions {
  readonly registry: GrpcMethodRegistry;
  /**
   * Overrides the address reported in client span attributes
   * (`server.address` / `server.port`). Defaults to `baseUrl`.
   */
  readonly serverAddress?: URL;
}

export interface GrpcClientProtocolTransportOptions {
  readonly registry: GrpcMethodRegistry;
  /** A transport from {@link makeTransport}, or any connect `Transport`. */
  readonly transport: Transport;
  /** Address reported in client span attributes. Telemetry only. */
  readonly serverAddress?: URL;
}

/**
 * Builds the gRPC transport used by the client protocol. Wraps connect-node's
 * `createGrpcTransport` so callers configure TLS (`nodeOptions`), interceptors,
 * compression, and timeouts without depending on `@connectrpc/connect-node`.
 */
export const makeTransport = (options: GrpcTransportOptions): Transport =>
  createGrpcTransport(options);

/**
 * Adapts an Effect that resolves gRPC metadata into a connect `Interceptor`,
 * so cross-cutting headers (e.g. `authorization: Bearer <token>`) can be
 * attached to every outgoing call while staying in Effect.
 *
 * `resolve` runs once per request against the context captured when the
 * interceptor is built, so reading a `Ref`/service yields the current value —
 * e.g. a token rotated by a background refresher. Its requirements `R` must be
 * satisfied where the interceptor is built (typically the same scope as the
 * service it reads).
 *
 * Resolved metadata is treated as defaults: a header already present on the
 * call — per-call `GrpcCallOptions.metadata`, or the injected `traceparent` —
 * is left untouched. Reserved `x-effect-grpc-*` keys are rejected, as on the
 * per-call path.
 *
 * Pass the result via `interceptors` on {@link layer} or {@link makeTransport}.
 */
export const metadataInterceptor = <R>(
  resolve: Effect.Effect<GrpcMetadata.GrpcMetadata, never, R>,
): Effect.Effect<Interceptor, never, R> =>
  Effect.runtime<R>().pipe(
    Effect.map((runtime): Interceptor => {
      const run = Runtime.runPromise(runtime);
      return (next) => async (req) => {
        const metadata = await run(resolve);
        const present = new Set<string>();
        req.header.forEach((_value, key) => present.add(key.toLowerCase()));
        for (const [key, value] of headersFromCallOptions({ metadata })) {
          if (present.has(key.toLowerCase())) continue;
          req.header.append(key, value);
        }
        return next(req);
      };
    }),
  );

/**
 * Streaming client used by generated code for client-streaming and
 * bidi-streaming methods. Effect RPC's wire protocol has no client-to-server
 * chunk variant, so these methods bridge `Stream` <-> `AsyncIterable` directly
 * over the same connect transport instead of going through `RpcClient`.
 */
export interface GrpcStreamingClientService {
  readonly clientStreaming: <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError | E>;
  readonly bidiStreaming: <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError | E>;
}

export class GrpcStreamingClient extends Context.Tag(
  "@effect-grpc/effect-grpc/GrpcStreamingClient",
)<GrpcStreamingClient, GrpcStreamingClientService>() {}

/**
 * Builds the protocol layer. The common case: pass `baseUrl` plus any
 * connect-node options (`nodeOptions`, `interceptors`, `defaultTimeoutMs`, ...).
 */
export const layer = (
  options: GrpcClientProtocolOptions,
): Layer.Layer<RpcClient.Protocol | GrpcStreamingClient> =>
  layerFromTransport({
    registry: options.registry,
    transport: makeTransport(options),
    serverAddress: options.serverAddress ?? new URL(options.baseUrl),
  });

/**
 * Builds the protocol layer from an existing transport. Use this to share one
 * transport across services, or to inject a fake transport in tests.
 */
export const layerFromTransport = (
  options: GrpcClientProtocolTransportOptions,
): Layer.Layer<RpcClient.Protocol | GrpcStreamingClient> =>
  Layer.mergeAll(
    Layer.scoped(RpcClient.Protocol, make(options)),
    Layer.effect(GrpcStreamingClient, makeStreaming(options)),
  );

const make = (
  options: GrpcClientProtocolTransportOptions,
): Effect.Effect<RpcClient.Protocol["Type"], never, Scope.Scope> =>
  RpcClient.Protocol.make((writeResponse) =>
    Effect.gen(function* () {
      const runtime = yield* Effect.runtime<never>();
      const run = Runtime.runPromise(runtime);
      const transport = options.transport;
      const activeCalls = new Map<string, AbortController>();

      yield* Effect.addFinalizer(() =>
        Effect.sync(() => {
          for (const controller of activeCalls.values()) {
            controller.abort();
          }
          activeCalls.clear();
        }),
      );

      const send = (message: FromClientEncoded): Effect.Effect<void> => {
        switch (message._tag) {
          case "Request":
            return sendRequest(message);
          case "Interrupt": {
            activeCalls.get(message.requestId)?.abort();
            activeCalls.delete(message.requestId);
            return Effect.void;
          }
          case "Ack":
          case "Eof":
          case "Ping":
            return Effect.void;
        }
      };

      const sendRequest = (
        request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
      ): Effect.Effect<void> => {
        const entry = options.registry.get(request.tag);
        if (!entry) {
          return writeResponse(
            failureExit(
              request.id,
              GrpcStatusError.unimplemented(
                `Unknown gRPC RPC tag: ${request.tag}`,
              ),
            ),
          );
        }

        const controller = new AbortController();
        activeCalls.set(request.id, controller);

        // Delivering the terminal exit resumes the caller, which immediately
        // interrupts this send fiber. Finish all span bookkeeping first and
        // write the terminal message only after the span has ended.
        let terminal: FromServerEncoded | undefined;
        return Effect.gen(function* () {
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
          const result = yield* Effect.promise(async (signal) => {
            const abort = () => controller.abort();
            signal.addEventListener("abort", abort, { once: true });
            try {
              return await invoke(entry, request, controller.signal, span);
            } finally {
              signal.removeEventListener("abort", abort);
              activeCalls.delete(request.id);
            }
          });
          terminal = result.terminal;
          GrpcTracing.annotateSpanStatus(span, result.status);
          if (result.status !== "ok") {
            yield* Effect.fail(result.status);
          }
        }).pipe(
          Effect.withSpan(
            GrpcTracing.spanName(entry),
            GrpcTracing.clientSpanOptions(entry, options.serverAddress),
          ),
          Effect.exit,
          Effect.flatMap(() =>
            terminal === undefined ? Effect.void : writeResponse(terminal),
          ),
        );
      };

      interface InvokeResult {
        readonly status: GrpcStatusCode;
        readonly terminal: FromServerEncoded;
      }

      const invoke = async (
        entry: GrpcMethodEntry,
        request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
        signal: AbortSignal,
        span: Tracer.Span,
      ): Promise<InvokeResult> => {
        const headers = headersWithTrace(request, span);
        const headerTimeoutMs = readTimeoutMs(request.headers);
        const callOptions: CallOptions = {
          headers: new Headers(headers.map(([key, value]) => [key, value])),
          signal,
          ...(headerTimeoutMs !== undefined
            ? { timeoutMs: headerTimeoutMs }
            : {}),
        };
        const client = getClient(transport, entry.service) as Record<
          string,
          unknown
        >;
        const method = client[entry.localName];
        if (typeof method !== "function") {
          return {
            status: "unimplemented",
            terminal: failureExit(
              request.id,
              GrpcStatusError.unimplemented(
                `gRPC client is missing method ${entry.localName}`,
              ),
            ),
          };
        }

        try {
          const grpcRequest = entry.toGrpcRequest(request.payload);
          if (entry.kind === "unary") {
            const response = await method.call(
              client,
              grpcRequest,
              callOptions,
            );
            return {
              status: "ok",
              terminal: successExit(
                request.id,
                entry.fromGrpcResponse(response as never),
              ),
            };
          }

          const responses = method.call(client, grpcRequest, callOptions) as
            | AsyncIterable<unknown>
            | Promise<AsyncIterable<unknown>>;
          for await (const response of await responses) {
            await run(
              writeResponse({
                _tag: "Chunk",
                requestId: request.id,
                values: [entry.fromGrpcResponse(response as never)],
              }),
            );
          }
          return { status: "ok", terminal: successExit(request.id, null) };
        } catch (cause) {
          const error = GrpcStatusError.fromConnectError(cause);
          return {
            status: error.code,
            terminal: failureExit(request.id, error),
          };
        }
      };

      return {
        send,
        supportsAck: false,
        supportsTransferables: false,
      };
    }),
  );

interface StreamingCallState {
  failure?: { readonly error: unknown };
}

type StreamingCallResult =
  | { readonly ok: true; readonly value: unknown }
  | { readonly ok: false; readonly cause: unknown };

const makeStreaming = (
  options: GrpcClientProtocolTransportOptions,
): Effect.Effect<GrpcStreamingClientService> =>
  Effect.gen(function* () {
    const runtime = yield* Effect.runtime<never>();
    const transport = options.transport;

    const resolveMethod = (entry: GrpcMethodEntry) => {
      const client = getClient(transport, entry.service) as Record<
        string,
        unknown
      >;
      const method = client[entry.localName];
      return typeof method === "function"
        ? (method.bind(client) as (
            requests: AsyncIterable<unknown>,
            options?: CallOptions,
          ) => unknown)
        : undefined;
    };

    const encodeRequest = (entry: GrpcMethodEntry) => {
      const codecs = entryCodecs(entry);
      return (value: unknown) =>
        Effect.try({
          try: () => entry.toGrpcRequest(codecs.encodePayload(value)),
          catch: (cause) =>
            GrpcStatusError.invalidArgument(
              "Invalid gRPC request payload",
              cause,
            ),
        });
    };

    const decodeResponse = (entry: GrpcMethodEntry, message: unknown) =>
      Effect.try({
        try: () =>
          entryCodecs(entry).decodeSuccess(
            entry.fromGrpcResponse(message as never),
          ),
        catch: (cause) =>
          GrpcStatusError.internal("Invalid gRPC response payload", cause),
      });

    const openRequests = (
      entry: GrpcMethodEntry,
      requests: Stream.Stream<unknown, unknown>,
      state: StreamingCallState,
      controller: AbortController,
    ) => {
      const iterator = Stream.toAsyncIterableRuntime(
        Stream.mapEffect(requests, encodeRequest(entry)),
        runtime,
      )[Symbol.asyncIterator]();
      const next = async (): Promise<IteratorResult<unknown>> => {
        try {
          return await iterator.next();
        } catch (error) {
          // gRPC has no channel for client-side failures other than cancelling
          // the call: remember the original error (unwrapped from the runtime's
          // `FiberFailure`) so the caller sees it while the server observes
          // `cancelled`.
          state.failure ??= {
            error: Runtime.isFiberFailure(error)
              ? Cause.squash(error[Runtime.FiberFailureCauseId])
              : error,
          };
          controller.abort();
          throw error;
        }
      };
      const close = async (): Promise<IteratorResult<unknown>> => {
        try {
          await iterator.return?.(undefined as never);
        } catch {
          // Cleanup only; the call outcome is already determined.
        }
        return { done: true, value: undefined };
      };
      return {
        iterable: {
          // connect aborts the request stream via `throw`; resolving done
          // reports clean completion while the scope close interrupts the
          // underlying stream.
          [Symbol.asyncIterator]: () => ({ next, return: close, throw: close }),
        } satisfies AsyncIterable<unknown>,
        close,
      };
    };

    const clientStreaming = <A, E>(
      tag: string,
      requests: Stream.Stream<A, E>,
      callOptions?: GrpcCallOptions,
    ): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError | E> => {
      const entry = options.registry.get(tag);
      if (!entry || entry.kind !== "client-streaming") {
        return Effect.fail(
          GrpcStatusError.unimplemented(`Unknown gRPC RPC tag: ${tag}`),
        );
      }
      return Effect.gen(function* () {
        const span = yield* Effect.currentSpan.pipe(Effect.orDie);
        const record = GrpcTracing.statusRecorder(span);
        const method = resolveMethod(entry);
        if (!method) {
          const error = GrpcStatusError.unimplemented(
            `gRPC client is missing method ${entry.localName}`,
          );
          record(error.code);
          return yield* Effect.fail(error);
        }
        const state: StreamingCallState = {};
        const controller = new AbortController();
        const pump = openRequests(
          entry,
          requests as Stream.Stream<unknown, unknown>,
          state,
          controller,
        );
        const result = yield* Effect.promise(
          async (signal): Promise<StreamingCallResult> => {
            const abort = () => controller.abort();
            signal.addEventListener("abort", abort, { once: true });
            try {
              const call = method(
                pump.iterable,
                streamingCallOptions(callOptions, span, controller.signal),
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
          if (state.failure) {
            record(streamingFailureCode(state.failure.error));
            return yield* Effect.fail(state.failure.error as E);
          }
          const error = GrpcStatusError.fromConnectError(result.cause);
          record(error.code);
          return yield* Effect.fail(error);
        }
        const response = yield* decodeResponse(entry, result.value).pipe(
          Effect.mapError((error) => {
            record(error.code);
            return error;
          }),
        );
        record("ok");
        return response;
      }).pipe(
        Effect.withSpan(
          GrpcTracing.spanName(entry),
          GrpcTracing.clientSpanOptions(entry, options.serverAddress),
        ),
      );
    };

    const bidiStreaming = <A, E>(
      tag: string,
      requests: Stream.Stream<A, E>,
      callOptions?: GrpcCallOptions,
    ): Stream.Stream<unknown, GrpcStatusError.GrpcStatusError | E> => {
      const entry = options.registry.get(tag);
      if (!entry || entry.kind !== "bidi-streaming") {
        return Stream.fail(
          GrpcStatusError.unimplemented(`Unknown gRPC RPC tag: ${tag}`),
        );
      }
      return Stream.unwrapScoped(
        Effect.gen(function* () {
          const span = yield* Effect.makeSpanScoped(
            GrpcTracing.spanName(entry),
            GrpcTracing.clientSpanOptions(entry, options.serverAddress),
          );
          const record = GrpcTracing.statusRecorder(span);
          const method = resolveMethod(entry);
          if (!method) {
            const error = GrpcStatusError.unimplemented(
              `gRPC client is missing method ${entry.localName}`,
            );
            record(error.code);
            return Stream.fail(error);
          }
          const state: StreamingCallState = {};
          const controller = new AbortController();
          const pump = openRequests(
            entry,
            requests as Stream.Stream<unknown, unknown>,
            state,
            controller,
          );
          yield* Effect.addFinalizer((exit) =>
            Effect.promise(async () => {
              controller.abort();
              await pump.close();
              record(Exit.isInterrupted(exit) ? "cancelled" : "ok");
            }),
          );
          const responses = method(
            pump.iterable,
            streamingCallOptions(callOptions, span, controller.signal),
          ) as AsyncIterable<unknown>;
          return Stream.fromAsyncIterable(
            responses,
            (cause): GrpcStatusError.GrpcStatusError | E =>
              state.failure
                ? (state.failure.error as E)
                : GrpcStatusError.fromConnectError(cause),
          ).pipe(
            Stream.mapEffect((message) => decodeResponse(entry, message)),
            Stream.mapError((error) => {
              record(streamingFailureCode(error));
              return error;
            }),
          );
        }),
      );
    };

    return { clientStreaming, bidiStreaming };
  });

const streamingFailureCode = (error: unknown): GrpcStatusCode =>
  error instanceof GrpcStatusError.GrpcStatusError ? error.code : "cancelled";

const streamingCallOptions = (
  options: GrpcCallOptions | undefined,
  span: Tracer.Span,
  signal: AbortSignal,
): CallOptions => {
  const headers = new Headers(
    headersFromCallOptions({ metadata: options?.metadata }).map(
      ([key, value]) => [key, value] as [string, string],
    ),
  );
  if (
    !headers.has("traceparent") &&
    span.traceId !== "noop" &&
    span.spanId !== "noop"
  ) {
    headers.set("traceparent", GrpcTracing.traceparent(span));
  }
  return {
    headers,
    signal,
    ...(options?.timeoutMs !== undefined
      ? { timeoutMs: options.timeoutMs }
      : {}),
  };
};

const headersWithTrace = (
  request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
  span: Tracer.Span,
): ReadonlyArray<readonly [string, string]> => {
  const headers = stripInternalHeaders(request.headers);
  if (headers.some(([key]) => key.toLowerCase() === "traceparent")) {
    return headers;
  }
  if (span.traceId !== "noop" && span.spanId !== "noop") {
    return [...headers, ["traceparent", GrpcTracing.traceparent(span)]];
  }
  if (request.traceId === undefined || request.spanId === undefined) {
    return headers;
  }
  return [
    ...headers,
    [
      "traceparent",
      GrpcTracing.traceparent({
        traceId: request.traceId,
        spanId: request.spanId,
        sampled: request.sampled ?? true,
      }),
    ],
  ];
};
