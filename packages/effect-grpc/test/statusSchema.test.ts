import { assert, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

import * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcStatusCode from "../src/GrpcStatusCode.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";

const encode = Schema.encodeSync(GrpcStatusError.GrpcStatusError);
const decode = Schema.decodeUnknownSync(GrpcStatusError.GrpcStatusError);

describe("GrpcStatusError schema contract", () => {
  it.effect.each([false, true])(
    "round trips JavaScript values with cause=%s",
    (withCause) =>
      Effect.gen(function* () {
        const original = GrpcStatusError.make({
          code: "unavailable",
          message: "retry later",
          metadata: withCause
            ? [
                ["trace-bin", new Uint8Array([0, 255])],
                ["x-id", "42"],
              ]
            : [],
          details: withCause
            ? [{ reason: "busy" }, new Map([["attempt", 2n]])]
            : [],
          ...(withCause ? { cause: new Error("upstream failed") } : {}),
        });
        const encoded = encode(original);
        assert.strictEqual(encoded._tag, "GrpcStatusError");
        assert.deepStrictEqual(encoded.metadata, original.metadata);
        assert.deepStrictEqual(encoded.details, original.details);
        const decoded = decode(encoded);
        assert.instanceOf(decoded, GrpcStatusError.GrpcStatusError);
        assert.instanceOf(decoded, Error);
        assert.strictEqual(decoded.code, original.code);
        assert.strictEqual(decoded.message, original.message);
        assert.deepStrictEqual(decoded.metadata, original.metadata);
        assert.deepStrictEqual(decoded.details, original.details);
        if (withCause) {
          assert.instanceOf(decoded.cause, Error);
          assert.strictEqual(
            (decoded.cause as Error).message,
            "upstream failed",
          );
          assert.deepStrictEqual(encoded.cause, {
            name: "Error",
            message: "upstream failed",
          });
        } else {
          assert.notProperty(encoded, "cause");
          assert.isUndefined(decoded.cause);
        }
        const handled = yield* Effect.fail(decoded).pipe(
          Effect.catchTag("GrpcStatusError", (error) =>
            Effect.succeed(error.code),
          ),
        );
        assert.strictEqual(handled, "unavailable");
      }),
  );

  it("decodes a JSON-shaped cause according to Schema.Defect", () => {
    const decoded = decode({
      _tag: "GrpcStatusError",
      code: "internal",
      message: "failure",
      metadata: [],
      details: [],
      cause: { name: "TypeError", message: "bad input" },
    });
    assert.instanceOf(decoded.cause, Error);
    assert.strictEqual((decoded.cause as Error).name, "TypeError");
    assert.strictEqual((decoded.cause as Error).message, "bad input");
  });

  it.each(["ok", "INVALID_ARGUMENT", "not_a_code", 14])(
    "rejects invalid error status %s",
    (code) => {
      assert.throws(() =>
        decode({
          _tag: "GrpcStatusError",
          code,
          message: "failure",
          metadata: [],
          details: [],
        }),
      );
      assert.throws(() =>
        Schema.decodeUnknownSync(GrpcStatusCode.GrpcErrorStatusCodeSchema)(
          code,
        ),
      );
    },
  );

  it.each([
    { metadata: [["trace-bin", [0, 255]]] },
    { metadata: [[42, "value"]] },
    { metadata: [["x-key"]] },
    { details: {} },
    { _tag: "OtherError" },
  ])("rejects malformed fields %s", (fields) => {
    assert.throws(() =>
      decode({
        _tag: "GrpcStatusError",
        code: "internal",
        message: "failure",
        metadata: [],
        details: [],
        ...fields,
      }),
    );
  });

  it("exports the same binary metadata schema used by status errors", () => {
    const metadata = [["trace-bin", new Uint8Array([1, 2])]] as const;
    assert.deepStrictEqual(
      Schema.decodeUnknownSync(GrpcMetadata.GrpcMetadataSchema)(
        Schema.encodeSync(GrpcMetadata.GrpcMetadataSchema)(metadata),
      ),
      metadata,
    );
  });
});
