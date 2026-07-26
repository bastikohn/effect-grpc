import * as net from "node:net";
import { Effect } from "effect";

import { GrpcNodeServer } from "@effect-grpc/effect-grpc";

export const freePort = Effect.promise(
  () =>
    new Promise<number>((resolve, reject) => {
      const server = net.createServer();
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === "object") {
            resolve(address.port);
          } else {
            reject(new Error("Unable to allocate a local port"));
          }
        });
      });
    }),
);

/**
 * Serve `services` on a free local port and run `use` against its base URL.
 * The health and reflection suites deliberately keep their own harnesses: they
 * build client layers internally and inject server-side context, so `use` has a
 * different shape there.
 */
export const withServer = <A, E, R>(
  options: {
    readonly services: ReadonlyArray<GrpcNodeServer.ServeAllService>;
    readonly tls?: GrpcNodeServer.GrpcServerTlsOptions;
  },
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        tls: options.tls,
        services: options.services,
      }).pipe(Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      const scheme = options.tls ? "https" : "http";
      return yield* use(new URL(`${scheme}://127.0.0.1:${port}`));
    }),
  );
