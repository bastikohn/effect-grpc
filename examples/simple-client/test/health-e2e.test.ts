import * as net from "node:net";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import {
  GrpcClientProtocol,
  GrpcHealth,
  GrpcMethodRegistry,
  GrpcNodeServer,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: { id: request.id, name: `User ${request.id}` },
    }),
  watchUsers: () => Stream.empty,
};

describe("grpc.health.v1 e2e", () => {
  it("Check reports server and service statuses over the wire", async () => {
    const result = await Effect.runPromise(
      withHealthServer((health) =>
        Effect.gen(function* () {
          const client = yield* GrpcHealth.HealthClient;

          const overall = yield* client.check({ service: "" });
          yield* health.set("demo.v1.UserService", "SERVING");
          const service = yield* client.check({
            service: "demo.v1.UserService",
          });
          const missing = yield* client
            .check({ service: "demo.v1.Missing" })
            .pipe(Effect.flip);

          return { overall, service, missing };
        }),
      ),
    );

    expect(result.overall).toEqual({ status: "SERVING" });
    expect(result.service).toEqual({ status: "SERVING" });
    expect(result.missing).toMatchObject({
      _tag: "GrpcStatusError",
      code: "not_found",
      message: "unknown service: demo.v1.Missing",
    });
  });

  it("Watch emits the current status and streams changes", async () => {
    const statuses = await Effect.runPromise(
      withHealthServer((health) =>
        Effect.gen(function* () {
          const client = yield* GrpcHealth.HealthClient;
          yield* health.set("demo.v1.UserService", "SERVING");

          return yield* client.watch({ service: "demo.v1.UserService" }).pipe(
            // Flip the server-side status once the initial emission arrives,
            // so the second element is a genuine status change.
            Stream.tap((response) =>
              response.status === "SERVING"
                ? health.set("demo.v1.UserService", "NOT_SERVING")
                : Effect.void,
            ),
            Stream.take(2),
            Stream.runCollect,
          );
        }),
      ),
    );

    expect(statuses).toEqual([
      { status: "SERVING" },
      { status: "NOT_SERVING" },
    ]);
  });

  it("Watch reports SERVICE_UNKNOWN until the service registers", async () => {
    const statuses = await Effect.runPromise(
      withHealthServer((health) =>
        Effect.gen(function* () {
          const client = yield* GrpcHealth.HealthClient;

          return yield* client.watch({ service: "demo.v1.Late" }).pipe(
            Stream.tap((response) =>
              response.status === "SERVICE_UNKNOWN"
                ? health.set("demo.v1.Late", "SERVING")
                : Effect.void,
            ),
            Stream.take(2),
            Stream.runCollect,
          );
        }),
      ),
    );

    expect(statuses).toEqual([
      { status: "SERVICE_UNKNOWN" },
      { status: "SERVING" },
    ]);
  });
});

const clientRegistry: GrpcMethodRegistry.GrpcMethodRegistry = new Map([
  ...UserServiceGrpcRegistry,
  ...GrpcHealth.HealthGrpcRegistry,
]);

const withHealthServer = <A, E>(
  use: (
    health: GrpcHealth.GrpcHealthService,
  ) => Effect.Effect<A, E, GrpcHealth.HealthClient>,
): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      const health = yield* GrpcHealth.make();

      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [
          {
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlersLayer(implementation),
          },
          GrpcHealth.service,
        ],
      }).pipe(
        Effect.provideService(GrpcHealth.GrpcHealth, health),
        Effect.forkScoped,
      );
      yield* Effect.sleep("50 millis");

      return yield* use(health).pipe(
        Effect.provide(
          GrpcHealth.HealthClientLayer.pipe(
            Layer.provide(
              GrpcClientProtocol.layer({
                baseUrl: `http://127.0.0.1:${port}`,
                defaultTimeoutMs: 1_000,
                registry: clientRegistry,
              }),
            ),
          ),
        ),
      );
    }),
  );

const freePort = Effect.promise(
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
