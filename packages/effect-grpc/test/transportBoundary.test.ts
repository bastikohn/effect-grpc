import { assert, describe, it } from "@effect/vitest";
import { Cause, Effect, Exit } from "effect";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";

describe("makeTransport execution boundary", () => {
  it.effect(
    "does not read options or construct a transport until executed",
    () =>
      Effect.gen(function* () {
        let reads = 0;
        const make = GrpcClientProtocol.makeTransport({
          get baseUrl() {
            reads++;
            return "http://127.0.0.1:1";
          },
        });
        assert.strictEqual(reads, 0);
        const transport = yield* make;
        assert.isAbove(reads, 0);
        assert.isFunction(transport.unary);
        assert.isFunction(transport.stream);
      }),
  );

  it.effect.each([
    { baseUrl: "http://127.0.0.1:1", tls: { ca: "certificate" } },
    { baseUrl: "https://127.0.0.1:1", tls: { cert: "certificate" } },
    { baseUrl: "not a URL", tls: { ca: "certificate" } },
  ])("classifies invalid configuration as a defect: %s", (options) =>
    Effect.gen(function* () {
      // Calling the constructor itself must not throw.
      const make = GrpcClientProtocol.makeTransport(options);
      const exit = yield* Effect.exit(make);
      assert.isTrue(Exit.isFailure(exit));
      if (Exit.isFailure(exit)) {
        assert.isTrue(Cause.hasDies(exit.cause));
        assert.isFalse(Cause.hasFails(exit.cause));
      }
    }),
  );
});
