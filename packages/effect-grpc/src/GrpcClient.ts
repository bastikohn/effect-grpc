import type { Interceptor } from "@connectrpc/connect";
import { Effect, Layer } from "effect";

import * as GrpcInvoker from "./GrpcInvoker.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import { metadataViolation } from "./internal/invoker.js";

/** Transport-independent client options. */
export type GrpcClientProtocolTransportOptions =
  GrpcInvoker.GrpcConnectInvokerOptions;

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
 * is left untouched. Reserved `x-effect-grpc-*` keys, and values contradicting
 * their key's `-bin` suffix, are rejected as on the per-call path — here as a
 * throw, since a connect interceptor has no typed error channel.
 *
 * Pass the result via `interceptors` when constructing your Connect transport.
 */
export const metadataInterceptor = <R>(
  resolve: Effect.Effect<GrpcMetadata.GrpcMetadata, never, R>,
): Effect.Effect<Interceptor, never, R> =>
  Effect.context<R>().pipe(
    Effect.map((context): Interceptor => {
      const run = Effect.runPromiseWith(context);
      return (next) => async (req) => {
        const metadata = await run(resolve);
        const violation = metadataViolation(metadata);
        if (violation !== undefined) throw new Error(violation);
        const present = new Set<string>();
        req.header.forEach((_value, key) => present.add(key.toLowerCase()));
        GrpcMetadata.toHeaders(metadata).forEach((value, key) => {
          if (!present.has(key)) req.header.append(key, value);
        });
        return next(req);
      };
    }),
  );

/**
 * Builds the client layer from an existing transport. Use this to share one
 * transport across services, or to substitute the invocation seam in tests —
 * the provided {@link GrpcInvoker.GrpcInvoker} is the connect adapter.
 */
export const layerFromTransport = (
  options: GrpcClientProtocolTransportOptions,
): Layer.Layer<GrpcInvoker.GrpcInvoker> => GrpcInvoker.layerConnect(options);
