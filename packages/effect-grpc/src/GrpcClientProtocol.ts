import type { CallOptions, Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import type { GrpcTransportOptions } from "@connectrpc/connect-node";
import { Context, Effect, Layer, Scope, Stream } from "effect";
import type * as Tracer from "effect/Tracer";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";

import type { GrpcCallOptions } from "./CodegenSupport.js";
import * as GrpcInvoker from "./GrpcInvoker.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "./GrpcStatusCode.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import { TraceState } from "./GrpcTracing.js";
import { getClient } from "./internal/connect.js";
import {
  headersFromCallOptions,
  readTimeoutMs,
  stripInternalHeaders,
} from "./internal/metadata.js";
import { failureExit, successExit } from "./internal/status.js";
import * as GrpcTracing from "./internal/tracing.js";

export type { GrpcTransportOptions } from "@connectrpc/connect-node";

/**
 * First-class TLS configuration for {@link makeTransport} / {@link layer}.
 * All material is PEM-encoded. Requires an `https://` base URL; the options
 * are merged into connect-node's `nodeOptions` (and win over any TLS keys set
 * there directly).
 */
export interface GrpcClientTlsOptions {
  /**
   * PEM CA bundle used to verify the server certificate. Defaults to Node's
   * trust store — needed whenever the server certificate is not publicly
   * trusted (self-signed, private CA).
   */
  readonly ca?: string | Buffer;
  /** PEM client certificate (chain) presented to the server for mTLS. Requires `key`. */
  readonly cert?: string | Buffer;
  /** PEM private key for `cert`. Requires `cert`. */
  readonly key?: string | Buffer;
  /**
   * Set to `false` to skip server certificate verification. Development
   * only — this disables the authentication half of TLS.
   */
  readonly rejectUnauthorized?: boolean;
}

/** {@link GrpcTransportOptions} plus first-class TLS/mTLS configuration. */
export interface GrpcClientTransportOptions extends GrpcTransportOptions {
  /** TLS/mTLS configuration. Requires an `https://` `baseUrl`. */
  readonly tls?: GrpcClientTlsOptions;
}

export interface GrpcClientProtocolOptions extends GrpcClientTransportOptions {
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
 * `createGrpcTransport` so callers configure TLS (`tls` or raw `nodeOptions`),
 * interceptors, compression, and timeouts without depending on
 * `@connectrpc/connect-node`.
 *
 * Whether the connection uses TLS is decided by the `baseUrl` scheme
 * (`https://` vs `http://`); `tls` refines the handshake — trust anchor,
 * client certificate for mTLS — and therefore requires `https://`.
 */
export const makeTransport = (
  options: GrpcClientTransportOptions,
): Transport => {
  const { tls, ...transportOptions } = options;
  if (tls === undefined) {
    return createGrpcTransport(transportOptions);
  }
  if (new URL(options.baseUrl).protocol !== "https:") {
    throw new Error(
      `GrpcClientProtocol: 'tls' requires an https:// baseUrl, got '${options.baseUrl}'`,
    );
  }
  if ((tls.cert === undefined) !== (tls.key === undefined)) {
    throw new Error(
      "GrpcClientProtocol: mTLS requires both 'cert' and 'key' (got only one)",
    );
  }
  return createGrpcTransport({
    ...transportOptions,
    nodeOptions: {
      ...transportOptions.nodeOptions,
      ...(tls.ca !== undefined ? { ca: tls.ca } : {}),
      ...(tls.cert !== undefined ? { cert: tls.cert, key: tls.key } : {}),
      ...(tls.rejectUnauthorized !== undefined
        ? { rejectUnauthorized: tls.rejectUnauthorized }
        : {}),
    },
  });
};

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
  Effect.context<R>().pipe(
    Effect.map((context): Interceptor => {
      const run = Effect.runPromiseWith(context);
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
 * chunk variant, so these methods bypass `RpcClient` — a thin facade over the
 * corresponding {@link GrpcInvoker.GrpcInvoker} call shapes, kept until
 * generated clients depend on the invoker directly.
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

export class GrpcStreamingClient extends Context.Service<
  GrpcStreamingClient,
  GrpcStreamingClientService
>()("@effect-grpc/effect-grpc/GrpcStreamingClient") {}

/**
 * Builds the protocol layer. The common case: pass `baseUrl` plus any
 * connect-node options (`nodeOptions`, `interceptors`, `defaultTimeoutMs`, ...).
 */
export const layer = (
  options: GrpcClientProtocolOptions,
): Layer.Layer<
  RpcClient.Protocol | GrpcStreamingClient | GrpcInvoker.GrpcInvoker
> =>
  layerFromTransport({
    registry: options.registry,
    transport: makeTransport(options),
    serverAddress: options.serverAddress ?? new URL(options.baseUrl),
  });

/**
 * Builds the protocol layer from an existing transport. Use this to share one
 * transport across services, or to substitute the invocation seam in tests —
 * the provided {@link GrpcInvoker.GrpcInvoker} is the connect adapter.
 */
export const layerFromTransport = (
  options: GrpcClientProtocolTransportOptions,
): Layer.Layer<
  RpcClient.Protocol | GrpcStreamingClient | GrpcInvoker.GrpcInvoker
> => {
  const invoker = GrpcInvoker.layerConnect(options);
  return Layer.mergeAll(
    Layer.effect(RpcClient.Protocol, make(options)),
    Layer.effect(GrpcStreamingClient, makeStreaming).pipe(
      Layer.provide(invoker),
    ),
    invoker,
  );
};

const make = (
  options: GrpcClientProtocolTransportOptions,
): Effect.Effect<RpcClient.Protocol["Service"], never, Scope.Scope> =>
  RpcClient.Protocol.make((writeResponse) =>
    Effect.gen(function* () {
      const context = yield* Effect.context<never>();
      const run = Effect.runPromiseWith(context);
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

      const callKey = (clientId: number, requestId: string) =>
        `${clientId}:${requestId}`;

      const send = (
        clientId: number,
        message: FromClientEncoded,
      ): Effect.Effect<void> => {
        switch (message._tag) {
          case "Request":
            return sendRequest(message, clientId);
          case "Interrupt": {
            const key = callKey(clientId, message.requestId);
            activeCalls.get(key)?.abort();
            activeCalls.delete(key);
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
        clientId: number,
      ): Effect.Effect<void> => {
        const entry = options.registry.get(request.tag);
        if (!entry) {
          return writeResponse(
            clientId,
            failureExit(
              request.id,
              GrpcStatusError.unimplemented(
                `Unknown gRPC RPC tag: ${request.tag}`,
              ),
            ),
          );
        }

        // `Effect.suspend` keeps the abort controller and status recorder
        // execution-local, so re-running the returned effect cannot share
        // mutable call state across executions. The span is scope-managed
        // (not `Effect.withSpan`) so it can close with an exit computed from
        // the recorded status: per semconv every non-OK client status is an
        // error — including cancellation, which surfaces as interruption and
        // would otherwise end the span with an interrupt-only exit that
        // exporters map to OK.
        return Effect.suspend(() => {
          const controller = new AbortController();
          const key = callKey(clientId, request.id);
          activeCalls.set(key, controller);

          let finalCode: GrpcStatusCode | undefined;
          return Effect.uninterruptibleMask((restore) =>
            Effect.gen(function* () {
              const spanScope = yield* Scope.make();
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
                finalCode ??= code;
                baseRecord(code);
              };
              return yield* restore(
                Effect.gen(function* () {
                  const ambientTraceState = yield* Effect.service(TraceState);
                  const status = yield* Effect.promise(async (signal) => {
                    const abort = () => controller.abort();
                    signal.addEventListener("abort", abort, { once: true });
                    try {
                      return await invoke(
                        entry,
                        request,
                        clientId,
                        controller.signal,
                        span,
                        ambientTraceState,
                      );
                    } finally {
                      signal.removeEventListener("abort", abort);
                      activeCalls.delete(key);
                    }
                  });
                  record(status);
                  if (status !== "ok") {
                    yield* Effect.fail(status);
                  }
                }).pipe(
                  Effect.onInterrupt(() =>
                    Effect.sync(() => record("cancelled")),
                  ),
                  Effect.withParentSpan(span),
                ),
              ).pipe(
                Effect.onExit(() =>
                  Scope.close(spanScope, GrpcTracing.clientSpanExit(finalCode)),
                ),
              );
            }),
          ).pipe(Effect.catch(() => Effect.void));
        });
      };

      const invoke = async (
        entry: GrpcMethodEntry,
        request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
        clientId: number,
        signal: AbortSignal,
        span: Tracer.Span,
        ambientTraceState: string | undefined,
      ): Promise<GrpcStatusCode> => {
        const headers = headersWithTrace(request, span, ambientTraceState);
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
          await run(
            writeResponse(
              clientId,
              failureExit(
                request.id,
                GrpcStatusError.unimplemented(
                  `gRPC client is missing method ${entry.localName}`,
                ),
              ),
            ),
          );
          return "unimplemented";
        }

        try {
          const grpcRequest = entry.toGrpcRequest(request.payload);
          if (entry.kind === "unary") {
            const response = await method.call(
              client,
              grpcRequest,
              callOptions,
            );
            await run(
              writeResponse(
                clientId,
                successExit(
                  request.id,
                  entry.fromGrpcResponse(response as never),
                ),
              ),
            );
            return "ok";
          }

          const responses = method.call(client, grpcRequest, callOptions) as
            | AsyncIterable<unknown>
            | Promise<AsyncIterable<unknown>>;
          for await (const response of await responses) {
            await run(
              writeResponse(clientId, {
                _tag: "Chunk",
                requestId: request.id,
                values: [entry.fromGrpcResponse(response as never)],
              }),
            );
          }
          await run(writeResponse(clientId, successExit(request.id, null)));
          return "ok";
        } catch (cause) {
          const error = GrpcStatusError.fromConnectError(cause);
          await run(writeResponse(clientId, failureExit(request.id, error)));
          return error.code;
        }
      };

      return {
        send,
        supportsAck: false,
        supportsTransferables: false,
      };
    }),
  );

/**
 * Facade over the invocation seam: generated code still resolves streaming
 * calls through {@link GrpcStreamingClient}, which delegates to the
 * corresponding {@link GrpcInvoker.GrpcInvoker} call shapes.
 */
const makeStreaming: Effect.Effect<
  GrpcStreamingClientService,
  never,
  GrpcInvoker.GrpcInvoker
> = Effect.gen(function* () {
  const invoker = yield* GrpcInvoker.GrpcInvoker;
  return {
    clientStreaming: invoker.clientStream,
    bidiStreaming: invoker.bidiStream,
  };
});

const headersWithTrace = (
  request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
  span: Tracer.Span,
  ambientTraceState: string | undefined,
): ReadonlyArray<readonly [string, string]> => {
  const headers = stripInternalHeaders(request.headers);
  if (headers.some(([key]) => key.toLowerCase() === "traceparent")) {
    return headers;
  }
  if (span.traceId !== "noop" && span.spanId !== "noop") {
    const injected: Array<readonly [string, string]> = [
      ["traceparent", GrpcTracing.traceparent(span)],
    ];
    // Span-annotation values win over the ambient `TraceState` reference
    // rehydrated from the incoming request by the server protocol.
    const traceState = GrpcTracing.findTraceState(span) ?? ambientTraceState;
    if (
      traceState !== undefined &&
      !headers.some(([key]) => key.toLowerCase() === "tracestate")
    ) {
      injected.push(["tracestate", traceState]);
    }
    return [...headers, ...injected];
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
