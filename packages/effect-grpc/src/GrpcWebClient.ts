import {
  createConnectTransport,
  createGrpcWebTransport,
  type ConnectTransportOptions,
  type GrpcWebTransportOptions,
} from "@connectrpc/connect-web";
import { Effect, Layer, Stream } from "effect";

import { GrpcInvoker } from "./GrpcInvoker.js";
import type { GrpcMethodRegistry } from "./GrpcMethodRegistry.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import { makeConnect } from "./internal/connectInvoker.js";

/** Browser transport configuration, using connect-web's native options. */
export type GrpcWebClientOptions = (
  | ({ readonly protocol: "connect" } & ConnectTransportOptions)
  | ({ readonly protocol: "grpc-web" } & GrpcWebTransportOptions)
) & {
  readonly registry: GrpcMethodRegistry;
  /** Optional address used only for client telemetry; relative base URLs work. */
  readonly serverAddress?: URL;
};

/**
 * Provides generated clients with unary and server-streaming browser calls.
 * Client and bidirectional streaming fail with `unimplemented` before their
 * request streams are acquired. Native Node invokers retain all four shapes.
 *
 * Transport construction is deferred until layer acquisition. `fetch`,
 * interceptors, credentials and protocol settings use connect-web unchanged.
 */
export const layer = (
  options: GrpcWebClientOptions,
): Layer.Layer<GrpcInvoker> =>
  Layer.effect(
    GrpcInvoker,
    Effect.sync(() =>
      options.protocol === "connect"
        ? createConnectTransport(options)
        : createGrpcWebTransport(options),
    ).pipe(
      Effect.flatMap((transport) =>
        makeConnect({
          transport,
          registry: options.registry,
          serverAddress: options.serverAddress,
        }),
      ),
      Effect.map((invoker) => ({
        ...invoker,
        clientStream: () => Effect.fail(unsupported()),
        bidiStream: () => Stream.fail(unsupported()),
      })),
    ),
  );

const unsupported = () =>
  GrpcStatusError.unimplemented(
    "Browser clients support only unary and server-streaming calls",
  );
