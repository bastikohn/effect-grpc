import * as http2 from "node:http2";
import * as net from "node:net";
import { assert, describe, it } from "@effect/vitest";
import { Cause, Deferred, Effect, Exit, Fiber } from "effect";

import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcNodeServer from "../src/GrpcNodeServer.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import { freePort } from "./support/serverHarness.js";

const listen = (port: number) =>
  Effect.acquireRelease(
    Effect.promise(
      () =>
        new Promise<net.Server>((resolve, reject) => {
          const server = net.createServer();
          server.once("error", reject);
          server.listen(port, "127.0.0.1", () => {
            server.off("error", reject);
            resolve(server);
          });
        }),
    ),
    (server) =>
      Effect.promise(
        () => new Promise<void>((resolve) => server.close(() => resolve())),
      ),
  );

// Probe actual readiness. No fixed sleep can establish that a server has bound.
const connect = (port: number) =>
  Effect.acquireRelease(
    Effect.promise(async () => {
      for (let attempt = 0; ; attempt++) {
        const session = http2.connect(`http://127.0.0.1:${port}`);
        try {
          await new Promise<void>((resolve, reject) => {
            session.once("connect", resolve);
            session.once("error", reject);
          });
          return session;
        } catch (error) {
          session.destroy();
          if (attempt === 100) throw error;
          await new Promise((resolve) => setTimeout(resolve, 5));
        }
      }
    }),
    (session) => Effect.sync(() => session.destroy()),
  );

const awaitClose = (session: http2.ClientHttp2Session) =>
  Effect.promise(() =>
    session.destroyed
      ? Promise.resolve()
      : new Promise<void>((resolve) => session.once("close", resolve)),
  ).pipe(Effect.timeout("1 second"));

describe("Node server binding and cleanup", () => {
  it.live(
    "classifies an occupied port as a defect without closing its existing listener",
    () =>
      Effect.gen(function* () {
        const occupied = yield* listen(0);
        const port = (occupied.address() as net.AddressInfo).port;
        const exit = yield* Effect.exit(
          Effect.scoped(
            GrpcNodeServer.serve({
              host: "127.0.0.1",
              port,
              routes: (router) => router,
            }),
          ),
        );
        assert.isTrue(Exit.isFailure(exit));
        if (Exit.isFailure(exit)) {
          assert.isTrue(Cause.hasDies(exit.cause));
          assert.isFalse(Cause.hasFails(exit.cause));
          assert.match(Cause.pretty(exit.cause), /EADDRINUSE/);
        }
        assert.isTrue(occupied.listening);
      }),
  );

  it.live(
    "binds successfully and releases idle sessions and the port on shutdown",
    () =>
      Effect.gen(function* () {
        const port = yield* freePort;
        const server = yield* GrpcNodeServer.serve({
          host: "127.0.0.1",
          port,
          shutdownTimeoutMs: 25,
          routes: (router) => router,
        }).pipe(Effect.scoped, Effect.forkChild);
        const session = yield* connect(port);
        yield* Fiber.interrupt(server);
        yield* awaitClose(session);
        const rebound = yield* listen(port);
        assert.isTrue(rebound.listening);
      }),
  );

  it.live(
    "forces an active session closed after the graceful shutdown budget",
    () =>
      Effect.gen(function* () {
        const entered = yield* Deferred.make<void>();
        const finalized = yield* Deferred.make<void>();
        const { routes } = yield* GrpcServerProtocol.make({
          registry: GrpcHealth.HealthGrpcRegistry,
          handlers: new Map([
            [
              "grpc.health.v1.Health/Check",
              {
                kind: "unary",
                handler: () =>
                  Deferred.succeed(entered, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(Deferred.succeed(finalized, undefined)),
                  ),
              },
            ],
          ]),
        });
        const port = yield* freePort;
        const server = yield* GrpcNodeServer.serve({
          host: "127.0.0.1",
          port,
          shutdownTimeoutMs: 25,
          routes,
        }).pipe(Effect.scoped, Effect.forkChild);
        const session = yield* connect(port);
        const request = session.request({
          ":method": "POST",
          ":path": "/grpc.health.v1.Health/Check",
          "content-type": "application/grpc",
          te: "trailers",
        });
        request.on("error", () => {});
        request.end(new Uint8Array(5));
        yield* Deferred.await(entered).pipe(Effect.timeout("1 second"));
        yield* Fiber.interrupt(server);
        yield* awaitClose(session);
        yield* Deferred.await(finalized).pipe(Effect.timeout("1 second"));
        assert.isTrue(session.destroyed);
        const rebound = yield* listen(port);
        assert.isTrue(rebound.listening);
      }),
  );
});
