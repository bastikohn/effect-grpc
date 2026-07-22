import type { Interceptor, Transport } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import type { GrpcTransportOptions } from "@connectrpc/connect-node";
import { Effect, Layer } from "effect";

import * as GrpcInvoker from "./GrpcInvoker.js";
import * as GrpcMetadata from "./GrpcMetadata.js";
import type { GrpcMethodRegistry } from "./GrpcMethodRegistry.js";
import { headersFromCallOptions } from "./internal/metadata.js";

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

/**
 * Options for {@link layerFromTransport}: the same shape the connect invoker
 * takes, so the two never drift.
 */
export type GrpcClientProtocolTransportOptions =
  GrpcInvoker.GrpcConnectInvokerOptions;

/**
 * Builds the gRPC transport used by the client layer. Wraps connect-node's
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
 * Builds the client layer, providing the {@link GrpcInvoker.GrpcInvoker}
 * generated clients depend on. The common case: pass `baseUrl` plus any
 * connect-node options (`nodeOptions`, `interceptors`, `defaultTimeoutMs`, ...).
 */
export const layer = (
  options: GrpcClientProtocolOptions,
): Layer.Layer<GrpcInvoker.GrpcInvoker> =>
  layerFromTransport({
    registry: options.registry,
    transport: makeTransport(options),
    serverAddress: options.serverAddress ?? new URL(options.baseUrl),
  });

/**
 * Builds the client layer from an existing transport. Use this to share one
 * transport across services, or to substitute the invocation seam in tests —
 * the provided {@link GrpcInvoker.GrpcInvoker} is the connect adapter.
 */
export const layerFromTransport = (
  options: GrpcClientProtocolTransportOptions,
): Layer.Layer<GrpcInvoker.GrpcInvoker> => GrpcInvoker.layerConnect(options);
