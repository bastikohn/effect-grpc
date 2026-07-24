import { ConnectError, Code } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";

import * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcStatusCode from "../src/GrpcStatusCode.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { metadataViolation } from "../src/internal/metadata.js";

describe("GrpcStatusCode", () => {
  it("converts to and from Connect codes", () => {
    expect(GrpcStatusCode.fromConnectCode(Code.NotFound)).toBe("not_found");
    expect(GrpcStatusCode.fromConnectCode(Code.InvalidArgument)).toBe(
      "invalid_argument",
    );
    expect(GrpcStatusCode.toConnectCode("unavailable")).toBe(Code.Unavailable);
  });
});

describe("GrpcStatusError", () => {
  it("converts ConnectError to generic status error", () => {
    const error = GrpcStatusError.fromConnectError(
      new ConnectError("missing", Code.NotFound, {
        "x-demo": "1",
      }),
    );

    expect(error.code).toBe("not_found");
    expect(error.message).toBe("missing");
    expect(error.metadata).toContainEqual(["x-demo", "1"]);
  });

  it("converts generic status error to ConnectError", () => {
    const error = GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("bad id"),
    );

    expect(error.code).toBe(Code.InvalidArgument);
    expect(error.rawMessage).toBe("bad id");
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

    expect(error.details).toEqual(details);
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

    expect(headers.get("x-demo")).toBe("1");
    expect(headers.get("x-other")).toBe("2");
  });

  it("round trips -bin values through base64 back to bytes", () => {
    const bytes = new Uint8Array([0, 1, 250, 255]);
    const headers = GrpcMetadata.toHeaders([["x-trace-bin", bytes]]);

    expect(headers.get("x-trace-bin")).toBe("AAH6/w==");
    expect(GrpcMetadata.fromHeaders(headers)).toEqual([["x-trace-bin", bytes]]);
  });

  it("splits repeated -bin values but never an ASCII value containing a comma", () => {
    const headers = new Headers();
    headers.append("x-trace-bin", "AQI=");
    headers.append("x-trace-bin", "Aw==");
    headers.append("x-list", "a,b");

    expect(GrpcMetadata.fromHeaders(headers)).toEqual([
      ["x-list", "a,b"],
      ["x-trace-bin", new Uint8Array([1, 2])],
      ["x-trace-bin", new Uint8Array([3])],
    ]);
  });

  it("drops join artefacts but keeps an empty binary value", () => {
    expect(GrpcMetadata.fromHeaders([["x-trace-bin", "AQI=,,Aw=="]])).toEqual([
      ["x-trace-bin", new Uint8Array([1, 2])],
      ["x-trace-bin", new Uint8Array([3])],
    ]);
    expect(GrpcMetadata.fromHeaders([["x-trace-bin", ""]])).toEqual([
      ["x-trace-bin", new Uint8Array([])],
    ]);
  });

  it("reports the first unsendable call-metadata entry", () => {
    expect(metadataViolation([["x-effect-grpc-custom", "value"]])).toContain(
      "Reserved gRPC metadata key: x-effect-grpc-custom",
    );
    expect(metadataViolation([["x-trace", new Uint8Array([1])]])).toContain(
      "requires a string value",
    );
    expect(metadataViolation([["x-trace-bin", "not-bytes"]])).toContain(
      "requires a Uint8Array value",
    );
    // Header syntax: `Headers.append` would throw a `TypeError` on each of
    // these, which is a defect rather than a status on both adapters.
    for (const key of ["bad key", "", "ünicode", "x:a"]) {
      expect(metadataViolation([[key, "v"]])).toContain(
        "Invalid gRPC metadata key",
      );
    }
    expect(metadataViolation([["x-trace", "a\nb"]])).toContain(
      "Invalid gRPC metadata value",
    );
    expect(
      metadataViolation([
        ["x-trace", "ok"],
        ["x-trace-bin", new Uint8Array([1])],
      ]),
    ).toBeUndefined();
  });
});
