import type { HandlerContext } from "@connectrpc/connect";
import { Effect, Stream } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import {
  captureImplementation,
  handlerContext,
} from "./support/serverHarness.js";

describe("GrpcHealth service", () => {
  it("reports the overall server status under the empty service name", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make;
        const initial = yield* health.check();
        yield* health.set("", "NOT_SERVING");
        return { initial, drained: yield* health.check("") };
      }),
    );

    // The server starts serving, and `""` is a settable service like any other.
    expect(result).toEqual({ initial: "SERVING", drained: "NOT_SERVING" });
  });

  it("unregisters services on clear", async () => {
    const result = await Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make;
        yield* health.set("demo.v1.UserService", "SERVING");
        yield* health.clear("demo.v1.UserService");
        const error = yield* Effect.flip(health.check("demo.v1.UserService"));
        const watched = yield* Stream.runCollect(
          Stream.take(health.watch("demo.v1.UserService"), 1),
        );
        return { error, watched };
      }),
    );

    expect(result.error).toMatchObject({
      code: "not_found",
      message: "unknown service: demo.v1.UserService",
    });
    expect(result.watched).toEqual(["SERVICE_UNKNOWN"]);
  });

  // The one thing the wire tests cannot see: `health-e2e.test.ts` asserts the
  // status *name* on both ends, so a consistently wrong pair of converters
  // would still round trip. This pins the encoded value itself.
  it("puts the domain status name on the wire as its numeric enum value", async () => {
    const response = await Effect.runPromise(
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make;
        const { routes } = yield* GrpcServerProtocol.make({
          registry: GrpcHealth.HealthGrpcRegistry,
          handlers: yield* GrpcHealth.HealthHandlers.pipe(
            Effect.provideService(GrpcHealth.GrpcHealth, health),
          ),
        });
        const check = captureImplementation(routes)["check"] as (
          request: unknown,
          context: HandlerContext,
        ) => Promise<unknown>;

        return yield* Effect.promise(() =>
          check({ service: "" }, handlerContext()),
        );
      }),
    );

    // `HealthCheckResponse.ServingStatus.SERVING` is 1.
    expect(response).toEqual({ status: 1 });
  });
});
