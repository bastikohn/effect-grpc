import type { DescService } from "@bufbuild/protobuf";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Schema } from "effect";

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

    assert.strictEqual(
      MethodRegistry.lookup(registry, "test.Svc/Unary", "unary")?.tag,
      "test.Svc/Unary",
    );
    assert.isUndefined(
      MethodRegistry.lookup(registry, "test.Svc/Unary", "bidi-streaming"),
    );
    assert.isUndefined(
      MethodRegistry.lookup(registry, "test.Svc/Missing", "unary"),
    );
  });
});

describe("merge", () => {
  it("combines registries and rejects duplicate tags", () => {
    const a = registryOf(entry("test.Svc/A", "unary"));
    const b = registryOf(entry("test.Svc/B", "bidi-streaming"));

    const merged = MethodRegistry.merge([a, b]);
    assert.deepStrictEqual([...merged.keys()].sort(), [
      "test.Svc/A",
      "test.Svc/B",
    ]);

    assert.throws(
      () => MethodRegistry.merge([a, a]),
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
    assert.deepStrictEqual(
      groups.get(service)?.map((e) => e.tag),
      ["test.Svc/A", "test.Svc/B"],
    );
    assert.deepStrictEqual(
      groups.get(other)?.map((e) => e.tag),
      ["test.Other/C"],
    );
  });
});

describe("conversions", () => {
  const unary = entry("test.Svc/Unary", "unary");

  it.effect("round trips domain values through the wire converters", () =>
    Effect.gen(function* () {
      const results = yield* Effect.all([
        MethodRegistry.encodeRequest(unary, "req"),
        MethodRegistry.decodeRequest(unary, { value: "req" }),
        MethodRegistry.encodeResponse(unary, 42),
        MethodRegistry.decodeResponse(unary, { value: 42 }),
      ]);

      assert.deepStrictEqual(results, [
        { value: "req" },
        "req",
        { value: 42 },
        42,
      ]);
    }),
  );

  it.effect(
    "normalizes request failures to invalid_argument and response failures to internal",
    () =>
      Effect.gen(function* () {
        const codes = yield* Effect.all([
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
        );

        assert.deepStrictEqual(codes, [
          "invalid_argument",
          "invalid_argument",
          "internal",
          "internal",
        ]);
      }),
  );
});
