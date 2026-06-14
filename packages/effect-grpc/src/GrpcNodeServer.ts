import * as http2 from "node:http2";
import type { ConnectRouter } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { Effect, Layer, Scope } from "effect";
import type * as RpcGroup from "effect/unstable/rpc/RpcGroup";
import * as RpcServer from "effect/unstable/rpc/RpcServer";

import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "./GrpcServerProtocol.js";

export interface ServeOptions {
  readonly host: string;
  readonly port: number;
  readonly routes: (router: ConnectRouter) => ConnectRouter | void;
  readonly shutdownTimeoutMs?: number;
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
    const { protocol, routes } = yield* GrpcServerProtocol.make({
      registry: mergeRegistries(
        options.services.map((service) => service.registry),
      ),
    });

    const [firstService, ...remainingServices] = options.services;
    if (firstService !== undefined) {
      const group = remainingServices.reduce(
        (group, service) => group.merge(service.group),
        firstService.group,
      );
      const handlers = Layer.mergeAll(
        firstService.handlers,
        ...remainingServices.map((service) => service.handlers),
      );
      yield* RpcServer.make(group).pipe(
        Effect.provideService(RpcServer.Protocol, protocol),
        Effect.provide(handlers),
        Effect.forkScoped,
      );
    }

    return yield* serve({
      host: options.host,
      port: options.port,
      shutdownTimeoutMs: options.shutdownTimeoutMs,
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
          new Promise<http2.Http2Server>((resolve, reject) => {
            const sessions = new Set<http2.ServerHttp2Session>();
            const server = http2.createServer(handler);
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
      `gRPC server listening on ${options.host}:${options.port}`,
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

const kSessions = Symbol("effectGrpcHttp2Sessions");

const serverSessions = (server: http2.Http2Server) =>
  (
    server as unknown as {
      readonly [kSessions]?: Set<http2.ServerHttp2Session>;
    }
  )[kSessions] ?? new Set<http2.ServerHttp2Session>();

const closeServer = (server: http2.Http2Server, options: ServeOptions) =>
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
