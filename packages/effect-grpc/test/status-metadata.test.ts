import { ConnectError, Code } from "@connectrpc/connect";
import { describe, expect, it } from "vitest";

import * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcStatusCode from "../src/GrpcStatusCode.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import { headersFromCallOptions } from "../src/internal/metadata.js";
import { errorFromExit, failureExit } from "../src/internal/status.js";

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

  it("does not synthesize a cause when encoded failures omit one", () => {
    const exit = failureExit("1", GrpcStatusError.notFound("missing")).exit;
    const error = errorFromExit(exit);

    expect(error).toBeInstanceOf(GrpcStatusError.GrpcStatusError);
    expect(error.cause).toBeUndefined();
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

  it("rejects user metadata in the effect-grpc internal namespace", () => {
    expect(() =>
      headersFromCallOptions({
        metadata: [["x-effect-grpc-custom", "value"]],
      }),
    ).toThrow("Reserved gRPC metadata key: x-effect-grpc-custom");
  });
});
