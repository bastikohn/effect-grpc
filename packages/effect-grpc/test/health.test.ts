import type { HandlerContext } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Stream } from "effect";

import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as HealthPb from "../src/internal/healthPb.js";
import {
  captureImplementation,
  handlerContext,
} from "./support/serverHarness.js";

describe("GrpcHealth service", () => {
  it.effect(
    "reports the overall server status under the empty service name",
    () =>
      Effect.gen(function* () {
        const health = yield* GrpcHealth.make;
        const initial = yield* health.check();
        yield* health.set("", "NOT_SERVING");
        const drained = yield* health.check("");

        // The server starts serving, and `""` is a settable service like any other.
        assert.deepStrictEqual(
          { initial, drained },
          { initial: "SERVING", drained: "NOT_SERVING" },
        );
      }),
  );

  it.effect("unregisters services on clear", () =>
    Effect.gen(function* () {
      const health = yield* GrpcHealth.make;
      yield* health.set("demo.v1.UserService", "SERVING");
      yield* health.clear("demo.v1.UserService");
      const error = yield* Effect.flip(health.check("demo.v1.UserService"));
      const watched = yield* Stream.runCollect(
        Stream.take(health.watch("demo.v1.UserService"), 1),
      );

      assert.strictEqual(error.code, "not_found");
      assert.strictEqual(error.message, "unknown service: demo.v1.UserService");
      assert.deepStrictEqual(watched, ["SERVICE_UNKNOWN"]);
    }),
  );

  // The one thing the wire tests cannot see: `health-e2e.test.ts` asserts the
  // status *name* on both ends, so a consistently wrong pair of converters
  // would still round trip. This pins the encoded value itself.
  it.effect(
    "puts the domain status name on the wire as its numeric enum value",
    () =>
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

        const response = yield* Effect.promise(() =>
          check({ service: "" }, handlerContext()),
        );

        // `HealthCheckResponse.ServingStatus.SERVING` is 1.
        assert.deepStrictEqual(response, { status: 1 });
      }),
  );

  // The wire value of a status is its position in a single array read by both
  // converters, so a reordering stays self-consistent and round trips. The
  // canonical numbering lives in the vendored descriptor; cross-check it there
  // rather than restating it as a second literal list.
  it("agrees with the descriptor on every serving status number", () => {
    const entry = GrpcHealth.HealthGrpcRegistry.get(
      "grpc.health.v1.Health/Check",
    )!;
    const servingStatus = HealthPb.Health.file.messages
      .find((message) => message.name === "HealthCheckResponse")!
      .nestedEnums.find((nested) => nested.name === "ServingStatus")!;

    assert.lengthOf(servingStatus.values, 4);
    assert.deepStrictEqual(
      servingStatus.values.map((value) => ({
        name: value.name,
        encoded: entry.toGrpcResponse({ status: value.name }),
        decoded: entry.fromGrpcResponse({ status: value.number } as never),
      })),
      servingStatus.values.map((value) => ({
        name: value.name,
        encoded: { status: value.number },
        decoded: { status: value.name },
      })),
    );
  });
});
