import type { CallOptions } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { Effect, Layer, Scope } from "effect";
import * as RpcClient from "effect/unstable/rpc/RpcClient";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";

import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import { getClient } from "./internal/connect.js";
import { readTimeoutMs, stripInternalHeaders } from "./internal/metadata.js";
import { failureExit, successExit } from "./internal/status.js";

export interface GrpcClientProtocolOptions {
  readonly baseUrl: URL;
  readonly registry: GrpcMethodRegistry;
  readonly defaultTimeoutMs?: number;
}

export const layer = (
  options: GrpcClientProtocolOptions,
): Layer.Layer<RpcClient.Protocol> =>
  Layer.effect(RpcClient.Protocol, make(options));

const make = (
  options: GrpcClientProtocolOptions,
): Effect.Effect<RpcClient.Protocol["Service"], never, Scope.Scope> =>
  RpcClient.Protocol.make((writeResponse) =>
    Effect.gen(function* () {
      const context = yield* Effect.context<never>();
      const run = Effect.runPromiseWith(context);
      const transport = createGrpcTransport({
        baseUrl: options.baseUrl.toString().replace(/\/$/, ""),
        defaultTimeoutMs: options.defaultTimeoutMs,
      });
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

        return Effect.promise(async (signal) => {
          const abort = () => controller.abort();
          signal.addEventListener("abort", abort, { once: true });
          try {
            await invoke(entry, request, clientId, controller.signal);
          } finally {
            signal.removeEventListener("abort", abort);
            activeCalls.delete(key);
          }
        });
      };

      const invoke = async (
        entry: GrpcMethodEntry,
        request: Extract<FromClientEncoded, { readonly _tag: "Request" }>,
        clientId: number,
        signal: AbortSignal,
      ) => {
        const headers = headersWithTrace(request);
        const callOptions: CallOptions = {
          headers: new Headers(headers.map(([key, value]) => [key, value])),
          signal,
          timeoutMs: readTimeoutMs(request.headers) ?? options.defaultTimeoutMs,
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
          return;
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
            return;
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
        } catch (cause) {
          await run(
            writeResponse(
              clientId,
              failureExit(request.id, GrpcStatusError.fromConnectError(cause)),
            ),
          );
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
): ReadonlyArray<readonly [string, string]> => {
  const headers = stripInternalHeaders(request.headers);
  if (
    request.traceId === undefined ||
    request.spanId === undefined ||
    headers.some(([key]) => key.toLowerCase() === "traceparent")
  ) {
    return headers;
  }
  return [
    ...headers,
    [
      "traceparent",
      `00-${request.traceId}-${request.spanId}-${request.sampled ? "01" : "00"}`,
    ],
  ];
};
