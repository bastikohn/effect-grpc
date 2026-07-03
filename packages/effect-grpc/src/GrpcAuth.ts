import type { Interceptor } from "@connectrpc/connect";
import { Context, type Duration, Effect, Layer, Ref, Schedule } from "effect";

import * as GrpcClientProtocol from "./GrpcClientProtocol.js";
import * as GrpcMetadata from "./GrpcMetadata.js";

/**
 * Holds the current bearer token for outgoing calls. Implementations decide
 * where tokens come from — a static credential, an auth RPC, a file — and how
 * they rotate; `read` must always yield the freshest value, because
 * {@link bearerInterceptor} reads it once per request.
 *
 * Defined as its own tag so transports (which read the token) and auth layers
 * (which produce it) can depend on it without importing each other.
 */
export interface BearerTokenService {
  readonly read: Effect.Effect<string>;
}

export class BearerToken extends Context.Service<
  BearerToken,
  BearerTokenService
>()("@effect-grpc/effect-grpc/BearerToken") {}

/** Map a bearer token to `authorization: Bearer <token>` gRPC metadata. */
export const bearerMetadata = (token: string): GrpcMetadata.GrpcMetadata =>
  GrpcMetadata.fromHeaders([["authorization", `Bearer ${token}`]]);

/**
 * A connect `Interceptor` that attaches `authorization: Bearer <token>` to
 * every outgoing call, resolving the token per request from an arbitrary
 * source. Like {@link GrpcClientProtocol.metadataInterceptor}, the resolved
 * header is a default: a per-call `authorization` header wins.
 *
 * Pass the result via `interceptors` on {@link GrpcClientProtocol.layer} or
 * {@link GrpcClientProtocol.makeTransport}.
 */
export const bearerInterceptorFrom = <R>(
  token: Effect.Effect<string, never, R>,
): Effect.Effect<Interceptor, never, R> =>
  GrpcClientProtocol.metadataInterceptor(Effect.map(token, bearerMetadata));

/**
 * {@link bearerInterceptorFrom} wired to the {@link BearerToken} service: the
 * token is re-read on every request, so a token rotated by a background
 * refresher (e.g. {@link refreshingTokenLayer}) is always current.
 */
export const bearerInterceptor: Effect.Effect<Interceptor, never, BearerToken> =
  bearerInterceptorFrom(
    Effect.gen(function* () {
      const service = yield* BearerToken;
      return yield* service.read;
    }),
  );

/** {@link BearerToken} layer for a fixed token that never rotates. */
export const staticTokenLayer = (token: string): Layer.Layer<BearerToken> =>
  Layer.succeed(BearerToken, { read: Effect.succeed(token) });

export interface RefreshingTokenOptions<E, R, E2, R2> {
  /** Mints the initial token. A failure here fails the layer build. */
  readonly acquire: Effect.Effect<string, E, R>;
  /**
   * Re-mints from the current token on every `interval` tick. Bake retries
   * for transient failures into this effect; a failure is logged and skipped,
   * keeping the previous token until the next tick — it never kills the
   * refresh daemon or the layer.
   */
  readonly refresh: (current: string) => Effect.Effect<string, E2, R2>;
  /**
   * Re-mint cadence. Prefer a fraction of the token lifetime over a window
   * that only opens shortly before expiry: spare cycles keep one badly-timed
   * outage from turning into dead RPCs.
   */
  readonly interval: Duration.Input;
}

/**
 * Live {@link BearerToken}: acquire a token once, hold it in a `Ref`, and fork
 * a scoped daemon that re-mints it on every `interval` tick. The daemon lives
 * exactly as long as the layer's scope, and {@link bearerInterceptor} picks up
 * each rotation on the next request.
 */
export const refreshingTokenLayer = <E, R, E2, R2>(
  options: RefreshingTokenOptions<E, R, E2, R2>,
): Layer.Layer<BearerToken, E, R | R2> =>
  Layer.effect(
    BearerToken,
    Effect.gen(function* () {
      const first = yield* options.acquire;
      const ref = yield* Ref.make(first);
      const refreshOnce = Ref.get(ref).pipe(
        Effect.flatMap(options.refresh),
        Effect.flatMap((token) => Ref.set(ref, token)),
        Effect.tapError((error) =>
          Effect.logWarning("bearer token refresh failed", error),
        ),
        // Never let a failed cycle kill the daemon; try again next interval.
        Effect.ignore,
      );
      yield* Effect.forkScoped(
        Effect.repeat(
          Effect.sleep(options.interval).pipe(Effect.andThen(refreshOnce)),
          Schedule.forever,
        ),
      );
      return { read: Ref.get(ref) };
    }),
  );
