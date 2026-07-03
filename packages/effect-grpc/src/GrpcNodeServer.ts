import * as http2 from "node:http2";
import type { ConnectRouter } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { Context, Effect, Layer, Option, Scope } from "effect";
import type * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "./GrpcServerProtocol.js";

/**
 * First-class TLS configuration for {@link serve} / {@link serveAll}. All
 * material is PEM-encoded. When present, the server terminates TLS itself via
 * `http2.createSecureServer`; when absent, it speaks plaintext h2c.
 */
export interface GrpcServerTlsOptions {
  /** PEM private key for the server certificate. */
  readonly key: string | Buffer;
  /** PEM server certificate (chain). */
  readonly cert: string | Buffer;
  /**
   * PEM CA bundle used to verify client certificates. Setting it enables
   * mTLS: the handshake requires a client certificate signed by this CA and
   * rejects connections without one.
   */
  readonly clientCa?: string | Buffer;
}

export interface ServeOptions {
  readonly host: string;
  readonly port: number;
  readonly routes: (router: ConnectRouter) => ConnectRouter | void;
  readonly shutdownTimeoutMs?: number;
  /** Terminate TLS (and optionally require client certificates, i.e. mTLS). */
  readonly tls?: GrpcServerTlsOptions;
}

export interface ServeAllService<R = never> {
  readonly group: RpcGroup.RpcGroup<any>;
  readonly registry: GrpcMethodRegistry;
  readonly handlers: Layer.Layer<any, never, R>;
}

export interface ServeAllOptions<
  Services extends ReadonlyArray<ServeAllService<any>>,
> extends Omit<ServeOptions, "routes"> {
  readonly services: Services;
}

type ServiceRequirements<Services extends ReadonlyArray<ServeAllService<any>>> =
  Services[number] extends ServeAllService<infer R> ? R : never;

export const serveAll = <
  const Services extends ReadonlyArray<ServeAllService<any>>,
>(
  options: ServeAllOptions<Services>,
): Effect.Effect<never, never, Scope.Scope | ServiceRequirements<Services>> =>
  Effect.gen(function* () {
    // Handlers layers are built individually so each service's streaming
    // handlers (carried inside the layer) can be collected without one
    // service's map overriding another's.
    const contexts = yield* Effect.forEach(options.services, (service) =>
      Layer.build(service.handlers),
    );
    const { protocol, routes } = yield* GrpcServerProtocol.make({
      registry: mergeRegistries(
        options.services.map((service) => service.registry),
      ),
      streamingHandlers: mergeStreamingHandlers(contexts),
    });

    const [firstService, ...remainingServices] = options.services;
    if (firstService !== undefined) {
      const group = remainingServices.reduce(
        (group, service) => group.merge(service.group),
        firstService.group,
      );
      yield* RpcServer.make(group).pipe(
        Effect.provideService(RpcServer.Protocol, protocol),
        Effect.provideContext(contexts.reduce(Context.merge)),
        Effect.forkScoped,
      );
    }

    return yield* serve({
      host: options.host,
      port: options.port,
      shutdownTimeoutMs: options.shutdownTimeoutMs,
      tls: options.tls,
      routes,
    });
  });

export const serve = (
  options: ServeOptions,
): Effect.Effect<never, never, Scope.Scope> =>
  Effect.gen(function* () {
    const handler = connectNodeAdapter({
      routes: (router) => {
        options.routes(router);
      },
    });
    const server = yield* Effect.acquireRelease(
      Effect.promise(
        () =>
          new Promise<NodeHttp2Server>((resolve, reject) => {
            const sessions = new Set<http2.ServerHttp2Session>();
            const server =
              options.tls === undefined
                ? http2.createServer(handler)
                : http2.createSecureServer(
                    secureServerOptions(options.tls),
                    handler,
                  );
            (
              server as unknown as {
                [kSessions]: Set<http2.ServerHttp2Session>;
              }
            )[kSessions] = sessions;
            server.on("session", (session) => {
              sessions.add(session);
              session.once("close", () => {
                sessions.delete(session);
              });
            });
            const onError = (error: Error) => {
              server.off("listening", onListening);
              reject(error);
            };
            const onListening = () => {
              server.off("error", onError);
              resolve(server);
            };
            server.once("error", onError);
            server.once("listening", onListening);
            server.listen(options.port, options.host);
          }),
      ),
      (server) => Effect.promise(() => closeServer(server, options)),
    );

    yield* Effect.logInfo(
      `gRPC server listening on ${options.host}:${options.port}${
        options.tls === undefined
          ? ""
          : options.tls.clientCa === undefined
            ? " (TLS)"
            : " (mTLS)"
      }`,
    );
    yield* Effect.never.pipe(
      Effect.ensuring(
        Effect.logInfo(
          `gRPC server stopped on ${options.host}:${options.port}`,
        ),
      ),
    );
    return server as never;
  });

const mergeStreamingHandlers = (
  contexts: ReadonlyArray<Context.Context<unknown>>,
): GrpcServerProtocol.GrpcStreamingHandlers => {
  const merged = new Map<string, GrpcServerProtocol.GrpcStreamingHandler>();
  for (const context of contexts) {
    const handlers = Context.getOption(
      context,
      GrpcServerProtocol.GrpcStreamingHandlers,
    );
    if (Option.isSome(handlers)) {
      for (const [tag, handler] of handlers.value) {
        merged.set(tag, handler);
      }
    }
  }
  return merged;
};

const mergeRegistries = (
  registries: ReadonlyArray<GrpcMethodRegistry>,
): GrpcMethodRegistry => {
  const merged = new Map<string, GrpcMethodEntry>();
  for (const registry of registries) {
    for (const [tag, entry] of registry) {
      if (merged.has(tag)) {
        throw new Error(`Duplicate gRPC RPC tag: ${tag}`);
      }
      merged.set(tag, entry);
    }
  }
  return merged;
};

type NodeHttp2Server = http2.Http2Server | http2.Http2SecureServer;

/**
 * Maps the first-class TLS options onto Node's secure-server options.
 * `clientCa` switches on mutual TLS: request a client certificate and reject
 * handshakes whose certificate does not chain to the given CA.
 */
const secureServerOptions = (
  tls: GrpcServerTlsOptions,
): http2.SecureServerOptions => ({
  key: tls.key,
  cert: tls.cert,
  ...(tls.clientCa !== undefined
    ? { ca: tls.clientCa, requestCert: true, rejectUnauthorized: true }
    : {}),
});

const kSessions = Symbol("effectGrpcHttp2Sessions");

const serverSessions = (server: NodeHttp2Server) =>
  (
    server as unknown as {
      readonly [kSessions]?: Set<http2.ServerHttp2Session>;
    }
  )[kSessions] ?? new Set<http2.ServerHttp2Session>();

const closeServer = (server: NodeHttp2Server, options: ServeOptions) =>
  new Promise<void>((resolve) => {
    let resolved = false;
    const sessions = serverSessions(server);
    const resolveOnce = () => {
      if (resolved) return;
      resolved = true;
      clearTimeout(forceDestroy);
      resolve();
    };
    const forceDestroy = setTimeout(() => {
      for (const session of sessions) {
        if (!session.closed && !session.destroyed) {
          session.destroy();
        }
      }
      resolveOnce();
    }, options.shutdownTimeoutMs ?? 5_000);
    forceDestroy.unref();

    for (const session of sessions) {
      if (!session.closed && !session.destroyed) {
        session.close();
      }
    }
    server.close(resolveOnce);
  });
