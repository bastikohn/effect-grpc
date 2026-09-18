import { ConnectError, Code } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";

import * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcStatusCode from "../src/GrpcStatusCode.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { metadataViolation } from "../src/internal/invoker.js";

describe("GrpcStatusCode", () => {
  it("converts to and from Connect codes", () => {
    assert.strictEqual(
      GrpcStatusCode.fromConnectCode(Code.NotFound),
      "not_found",
    );
    assert.strictEqual(
      GrpcStatusCode.fromConnectCode(Code.InvalidArgument),
      "invalid_argument",
    );
    assert.strictEqual(
      GrpcStatusCode.toConnectCode("unavailable"),
      Code.Unavailable,
    );
  });
});

describe("GrpcStatusError", () => {
  it("converts ConnectError to generic status error", () => {
    const error = GrpcStatusError.fromConnectError(
      new ConnectError("missing", Code.NotFound, {
        "x-demo": "1",
      }),
    );

    assert.strictEqual(error.code, "not_found");
    assert.strictEqual(error.message, "missing");
    assert.deepInclude(error.metadata, ["x-demo", "1"]);
  });

  it("converts generic status error to ConnectError", () => {
    const error = GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("bad id"),
    );

    assert.strictEqual(error.code, Code.InvalidArgument);
    assert.strictEqual(error.rawMessage, "bad id");
  });

  it("preserves Connect details when converting generic status errors", () => {
    const details = [
      {
        desc: {
          typeName: "demo.v1.ErrorDetail",
        },
        value: { reason: "bad id" },
      },
    ];
    const error = GrpcStatusError.toConnectError(
      GrpcStatusError.make({
        code: "invalid_argument",
        message: "bad id",
        details,
      }),
    );

    assert.deepStrictEqual<unknown>(error.details, details);
  });
});

describe("GrpcMetadata", () => {
  it("round trips string headers", () => {
    const metadata = GrpcMetadata.fromHeaders(
      new Headers([
        ["x-demo", "1"],
        ["x-other", "2"],
      ]),
    );

    const headers = GrpcMetadata.toHeaders(metadata);

    assert.strictEqual(headers.get("x-demo"), "1");
    assert.strictEqual(headers.get("x-other"), "2");
  });

  it("round trips -bin values through base64 back to bytes", () => {
    const bytes = new Uint8Array([0, 1, 250, 255]);
    const headers = GrpcMetadata.toHeaders([["x-trace-bin", bytes]]);

    assert.strictEqual(headers.get("x-trace-bin"), "AAH6/w==");
    assert.deepStrictEqual(GrpcMetadata.fromHeaders(headers), [
      ["x-trace-bin", bytes],
    ]);
  });

  it("splits repeated -bin values but never an ASCII value containing a comma", () => {
    const headers = new Headers();
    headers.append("x-trace-bin", "AQI=");
    headers.append("x-trace-bin", "Aw==");
    headers.append("x-list", "a,b");

    assert.deepStrictEqual(GrpcMetadata.fromHeaders(headers), [
      ["x-list", "a,b"],
      ["x-trace-bin", new Uint8Array([1, 2])],
      ["x-trace-bin", new Uint8Array([3])],
    ]);
  });

  it("drops join artefacts but keeps an empty binary value", () => {
    assert.deepStrictEqual(
      GrpcMetadata.fromHeaders([["x-trace-bin", "AQI=,,Aw=="]]),
      [
        ["x-trace-bin", new Uint8Array([1, 2])],
        ["x-trace-bin", new Uint8Array([3])],
      ],
    );
    assert.deepStrictEqual(GrpcMetadata.fromHeaders([["x-trace-bin", ""]]), [
      ["x-trace-bin", new Uint8Array([])],
    ]);
  });

  it("reports the first unsendable call-metadata entry", () => {
    assert.include(
      metadataViolation([["x-effect-grpc-custom", "value"]]),
      "Reserved gRPC metadata key: x-effect-grpc-custom",
    );
    assert.include(
      metadataViolation([["x-trace", new Uint8Array([1])]]),
      "requires a string value",
    );
    assert.include(
      metadataViolation([["x-trace-bin", "not-bytes"]]),
      "requires a Uint8Array value",
    );
    // Header syntax: `Headers.append` would throw a `TypeError` on each of
    // these, which is a defect rather than a status on both adapters.
    for (const key of ["bad key", "", "ünicode", "x:a"]) {
      assert.include(
        metadataViolation([[key, "v"]]),
        "Invalid gRPC metadata key",
      );
    }
    // ...and gRPC's charset is narrower than HTTP's, so the encode path alone
    // would let these onto the wire for a conforming peer to drop or reject,
    // instead of failing the call locally with `invalid_argument`.
    for (const key of ["x-parity$q", "foo!", "key#1", "a%b", "a&b", "a|b"]) {
      assert.include(
        metadataViolation([[key, "v"]]),
        "Invalid gRPC metadata key",
      );
    }
    for (const value of ["a\nb", "héllo", "a\tb", "a\u000Bb"]) {
      assert.include(
        metadataViolation([["x-trace", value]]),
        "Invalid gRPC metadata value",
      );
    }
    assert.isUndefined(
      metadataViolation([
        ["x-trace", "ok"],
        ["x-trace-bin", new Uint8Array([1])],
      ]),
    );
  });
});
