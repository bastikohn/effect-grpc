import type { CallOptions, Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import type { GrpcTransportOptions } from "@connectrpc/connect-node";
import { Effect, Layer, Scope } from "effect";
import type * as Tracer from "effect/Tracer";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";

import * as GrpcMetadata from "./GrpcMetadata.js";
import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "./GrpcStatusCode.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
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
 * Builds the protocol layer. The common case: pass `baseUrl` plus any
 * connect-node options (`nodeOptions`, `interceptors`, `defaultTimeoutMs`, ...).
 */
export const layer = (
  options: GrpcClientProtocolOptions,
): Layer.Layer<RpcClient.Protocol> =>
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
): Layer.Layer<RpcClient.Protocol> =>
  Layer.effect(RpcClient.Protocol, make(options));

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

        const controller = new AbortController();
        const key = callKey(clientId, request.id);
        activeCalls.set(key, controller);

        return Effect.gen(function* () {
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
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
              );
            } finally {
              signal.removeEventListener("abort", abort);
              activeCalls.delete(key);
            }
          });
          GrpcTracing.annotateSpanStatus(span, status);
          if (status !== "ok") {
            yield* Effect.fail(status);
          }
        }).pipe(
          Effect.withSpan(
            GrpcTracing.spanName(entry),
            GrpcTracing.clientSpanOptions(entry, options.serverAddress),
          ),
          Effect.catch(() => Effect.void),
        );
      };

      const invoke = async (
        entry: GrpcMethodEntry,
        request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
        clientId: number,
        signal: AbortSignal,
        span: Tracer.Span,
      ): Promise<GrpcStatusCode> => {
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
