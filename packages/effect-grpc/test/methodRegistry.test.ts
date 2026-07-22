import type { DescService } from "@bufbuild/protobuf";
import { Effect, Schema } from "effect";
import { describe, expect, it } from "vitest";

import type {
  GrpcMethodEntry,
  GrpcMethodKind,
} from "../src/GrpcMethodRegistry.js";
import * as MethodRegistry from "../src/GrpcMethodRegistry.js";
import type * as GrpcStatusError from "../src/GrpcStatusError.js";

const service = { typeName: "test.Svc" } as DescService;

const entry = (tag: string, kind: GrpcMethodKind): GrpcMethodEntry =>
  ({
    kind,
    tag,
    service,
    localName: tag.split("/")[1] ?? tag,
    payloadSchema: Schema.String,
    successSchema: Schema.Number,
    toGrpcRequest: (encoded: unknown) => ({ value: encoded }),
    fromGrpcRequest: (message: unknown) =>
      (message as { readonly value: unknown }).value,
    toGrpcResponse: (encoded: unknown) => ({ value: encoded }),
    fromGrpcResponse: (message: unknown) =>
      (message as { readonly value: unknown }).value,
  }) as unknown as GrpcMethodEntry;

const registryOf = (...entries: ReadonlyArray<GrpcMethodEntry>) =>
  new Map(entries.map((e) => [e.tag, e]));

describe("lookup", () => {
  it("returns the entry only when tag and kind match", () => {
    const registry = registryOf(entry("test.Svc/Unary", "unary"));

    expect(
      MethodRegistry.lookup(registry, "test.Svc/Unary", "unary")?.tag,
    ).toBe("test.Svc/Unary");
    expect(
      MethodRegistry.lookup(registry, "test.Svc/Unary", "bidi-streaming"),
    ).toBeUndefined();
    expect(
      MethodRegistry.lookup(registry, "test.Svc/Missing", "unary"),
    ).toBeUndefined();
  });
});

describe("merge", () => {
  it("combines registries and rejects duplicate tags", () => {
    const a = registryOf(entry("test.Svc/A", "unary"));
    const b = registryOf(entry("test.Svc/B", "bidi-streaming"));

    const merged = MethodRegistry.merge([a, b]);
    expect([...merged.keys()].sort()).toEqual(["test.Svc/A", "test.Svc/B"]);

    expect(() => MethodRegistry.merge([a, a])).toThrow(
      "Duplicate gRPC RPC tag: test.Svc/A",
    );
  });
});

describe("groupByService", () => {
  it("groups entries by service descriptor", () => {
    const other = { typeName: "test.Other" } as DescService;
    const foreign = {
      ...(entry("test.Other/C", "unary") as object),
      service: other,
    } as GrpcMethodEntry;
    const registry = registryOf(
      entry("test.Svc/A", "unary"),
      entry("test.Svc/B", "server-streaming"),
      foreign,
    );

    const groups = MethodRegistry.groupByService(registry);
    expect(groups.get(service)?.map((e) => e.tag)).toEqual([
      "test.Svc/A",
      "test.Svc/B",
    ]);
    expect(groups.get(other)?.map((e) => e.tag)).toEqual(["test.Other/C"]);
  });
});

describe("conversions", () => {
  const unary = entry("test.Svc/Unary", "unary");

  it("round trips domain values through the wire converters", async () => {
    const results = await Effect.runPromise(
      Effect.all([
        MethodRegistry.encodeRequest(unary, "req"),
        MethodRegistry.decodeRequest(unary, { value: "req" }),
        MethodRegistry.encodeResponse(unary, 42),
        MethodRegistry.decodeResponse(unary, { value: 42 }),
      ]),
    );

    expect(results).toEqual([{ value: "req" }, "req", { value: 42 }, 42]);
  });

  it("normalizes request failures to invalid_argument and response failures to internal", async () => {
    const codes = await Effect.runPromise(
      Effect.all([
        Effect.flip(MethodRegistry.encodeRequest(unary, 42)),
        Effect.flip(MethodRegistry.decodeRequest(unary, { value: 42 })),
        Effect.flip(MethodRegistry.encodeResponse(unary, "not a number")),
        Effect.flip(
          MethodRegistry.decodeResponse(unary, { value: "not a number" }),
        ),
      ]).pipe(
        Effect.map((errors) =>
          errors.map(
            (error) => (error as GrpcStatusError.GrpcStatusError).code,
          ),
        ),
      ),
    );

    expect(codes).toEqual([
      "invalid_argument",
      "invalid_argument",
      "internal",
      "internal",
    ]);
  });
});
