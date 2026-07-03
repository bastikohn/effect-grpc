import { pathToFileURL } from "node:url";

import { describe, expect, it } from "vitest";

import {
  generateProtoFeature,
  type GeneratedProtoFeature,
  typecheckProtoFeature,
} from "./protoFeatureFixtures.js";
import { Schema } from "../../effect-grpc/node_modules/effect/dist/index.js";

interface ConverterEntry {
  readonly fromGrpcRequest: (message: unknown) => unknown;
  readonly toGrpcRequest: (message: unknown) => unknown;
}

const registryEntry = async (
  feature: GeneratedProtoFeature,
  registryName: string,
  tag: string,
) => {
  const generated = await import(pathToFileURL(feature.effectFile).href);
  const registry = generated[registryName] as Map<string, ConverterEntry>;
  const entry = registry.get(tag);
  if (!entry) throw new Error(`missing registry entry: ${tag}`);
  return { entry, generated };
};

const expectSnapshot = (
  name: string,
  options?: Parameters<typeof generateProtoFeature>[1],
) => {
  const feature = generateProtoFeature(name, options);
  expect([...feature.files.entries()]).toMatchSnapshot();
};

describe("proto feature fixtures", () => {
  it("snapshots generated output", () => {
    expectSnapshot("repeated_scalars");
    expectSnapshot("repeated_messages");
    expectSnapshot("enums");
    expectSnapshot("imported_messages", {
      primary: "imported_messages.proto",
      protoFiles: ["imported_common.proto", "imported_messages.proto"],
    });
    expectSnapshot("int64_bigint");
    expectSnapshot("well_known_types");
    expectSnapshot("field_shapes");
    expectSnapshot("maps");
    expectSnapshot("oneofs");
    expectSnapshot("nested_messages");
    expectSnapshot("cross_package", {
      primary: "cross_package.proto",
      protoFiles: ["cross_package_common.proto", "cross_package.proto"],
    });
    expectSnapshot("optional_scalars");
    expectSnapshot("empty_messages");
  }, 120_000);

  it("typechecks generated output", () => {
    typecheckProtoFeature(
      generateProtoFeature("repeated_scalars"),
      [
        'import { create } from "@bufbuild/protobuf";',
        'import { RepeatedScalarsSchema as RepeatedScalarsPbSchema } from "./repeated_scalars_pb.js";',
        "import {",
        "  RepeatedScalarFeatureGrpcRegistry,",
        "  type RepeatedScalars,",
        '} from "./repeated_scalars_effect_grpc.js";',
        "",
        "const value: RepeatedScalars = {",
        '  tags: ["alpha"],',
        "  flags: [true],",
        "  blobs: [new Uint8Array([1, 2])],",
        "  scores: [1],",
        "  counts: [2],",
        "  ratios: [1.5],",
        "  weights: [2.5],",
        "};",
        'const entry = RepeatedScalarFeatureGrpcRegistry.get("features.v1.RepeatedScalarFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(RepeatedScalarsPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("repeated_messages"),
      [
        'import { UserListSchema as UserListPbSchema } from "./repeated_messages_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        "import {",
        "  RepeatedMessageFeatureGrpcRegistry,",
        "  type UserList,",
        '} from "./repeated_messages_effect_grpc.js";',
        'const value: UserList = { users: [{ id: "1", addresses: [{ city: "Berlin" }] }] };',
        'const entry = RepeatedMessageFeatureGrpcRegistry.get("features.v1.RepeatedMessageFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(UserListPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("enums"),
      [
        'import { EnumUserSchema as EnumUserPbSchema } from "./enums_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import { EnumFeatureGrpcRegistry, type EnumUser, type UserState } from "./enums_effect_grpc.js";',
        "const state: UserState = 1;",
        'const value: EnumUser = { id: "1", state, history: [state, 2] };',
        'const entry = EnumFeatureGrpcRegistry.get("features.v1.EnumFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(EnumUserPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("imported_messages", {
        primary: "imported_messages.proto",
        protoFiles: ["imported_common.proto", "imported_messages.proto"],
      }),
      [
        'import { GetImportedUserResponseSchema as ResponsePbSchema } from "./imported_messages_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import type { ImportedUser } from "./imported_common_effect_grpc.js";',
        'import { ImportedMessageFeatureGrpcRegistry, type GetImportedUserResponse } from "./imported_messages_effect_grpc.js";',
        'const user: ImportedUser = { id: "1", name: "Ada" };',
        "const value: GetImportedUserResponse = { user, state: 1 };",
        'const entry = ImportedMessageFeatureGrpcRegistry.get("features.v1.ImportedMessageFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(ResponsePbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("int64_bigint"),
      [
        'import { Int64ScalarsSchema as Int64ScalarsPbSchema } from "./int64_bigint_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import { Int64BigintFeatureGrpcRegistry, type Int64Scalars } from "./int64_bigint_effect_grpc.js";',
        "const value: Int64Scalars = { id: 1n, count: 2n, delta: -3n, hash: 4n, value: -5n };",
        'const entry = Int64BigintFeatureGrpcRegistry.get("features.v1.Int64BigintFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(Int64ScalarsPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("well_known_types"),
      [
        'import { Duration } from "effect";',
        'import { WellKnownValuesSchema as WellKnownValuesPbSchema } from "./well_known_types_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import { WellKnownTypeFeatureGrpcRegistry, type GrpcGoogleProtobufBoolValue, type GrpcGoogleProtobufDuration, type GrpcGoogleProtobufTimestamp, type WellKnownValues } from "./well_known_types_effect_grpc.js";',
        "const value: WellKnownValues = { createdAt: new Date(0), timeout: Duration.seconds(1), enabled: true };",
        "const timestamp: GrpcGoogleProtobufTimestamp = new Date(0);",
        "const duration: GrpcGoogleProtobufDuration = Duration.seconds(1);",
        "const boolValue: GrpcGoogleProtobufBoolValue = true;",
        'const entry = WellKnownTypeFeatureGrpcRegistry.get("features.v1.WellKnownTypeFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(WellKnownValuesPbSchema, entry.toGrpcRequest(value));",
        'const timestampEntry = WellKnownTypeFeatureGrpcRegistry.get("features.v1.WellKnownTypeFeature/EchoTimestamp");',
        'if (!timestampEntry) throw new Error("missing timestamp registry entry");',
        "timestampEntry.toGrpcRequest(timestamp);",
        'const durationEntry = WellKnownTypeFeatureGrpcRegistry.get("features.v1.WellKnownTypeFeature/EchoDuration");',
        'if (!durationEntry) throw new Error("missing duration registry entry");',
        "durationEntry.toGrpcRequest(duration);",
        'const boolValueEntry = WellKnownTypeFeatureGrpcRegistry.get("features.v1.WellKnownTypeFeature/EchoBoolValue");',
        'if (!boolValueEntry) throw new Error("missing bool value registry entry");',
        "boolValueEntry.toGrpcRequest(boolValue);",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("field_shapes"),
      [
        'import { Duration } from "effect";',
        'import { FieldShapeValuesSchema as FieldShapeValuesPbSchema } from "./field_shapes_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        "import {",
        "  FieldShapeFeatureGrpcRegistry,",
        "  type FieldShapeValues,",
        '} from "./field_shapes_effect_grpc.js";',
        "const value: FieldShapeValues = {",
        "  ratio: 1.5,",
        "  score: 2.5,",
        "  count: 3,",
        "  total: 4n,",
        "  size: 5,",
        "  serial: 6n,",
        '  label: "sample",',
        "  blob: new Uint8Array([1, 2]),",
        '  payload: { typeUrl: "type.googleapis.com/example.Payload", value: "AwQ=" },',
        "  metadata: { enabled: true },",
        "  dynamicValue: { nested: false },",
        '  listValue: [1, "two"],',
        '  updateMask: "fooBar,baz",',
        '  choice: { case: "timeout", value: Duration.seconds(1) },',
        '  labelsByNumber: { 1: "one" },',
        "  statesByNumber: { 2: 1 },",
        "  statesByName: { ready: 1 },",
        '  childrenByNumber: { 3: { id: "child" } },',
        "};",
        'const entry = FieldShapeFeatureGrpcRegistry.get("features.v1.FieldShapeFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(FieldShapeValuesPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("maps"),
      [
        'import { MapValuesSchema as MapValuesPbSchema } from "./maps_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import { MapFeatureGrpcRegistry, type MapValues } from "./maps_effect_grpc.js";',
        'const value: MapValues = { labels: { env: "test" }, counts: { seen: 1 }, users: { one: { id: "1" } } };',
        'const entry = MapFeatureGrpcRegistry.get("features.v1.MapFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(MapValuesPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("oneofs"),
      [
        'import { SearchRequestSchema as SearchRequestPbSchema } from "./oneofs_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import { OneofFeatureGrpcRegistry, type SearchRequest } from "./oneofs_effect_grpc.js";',
        'const value: SearchRequest = { query: { case: "user", value: { id: "1" } } };',
        'const entry = OneofFeatureGrpcRegistry.get("features.v1.OneofFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(SearchRequestPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("nested_messages"),
      [
        'import { NestedOuterSchema as NestedOuterPbSchema } from "./nested_messages_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        "import {",
        "  NestedMessageFeatureGrpcRegistry,",
        "  type NestedOuter,",
        "  type NestedOuter_Inner,",
        '} from "./nested_messages_effect_grpc.js";',
        'const inner: NestedOuter_Inner = { id: "1", status: 1 };',
        "const value: NestedOuter = {",
        "  inner,",
        "  items: [inner],",
        "  byId: { one: inner },",
        '  choice: { case: "picked", value: inner },',
        "};",
        'const entry = NestedMessageFeatureGrpcRegistry.get("features.v1.NestedMessageFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(NestedOuterPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("cross_package", {
        primary: "cross_package.proto",
        protoFiles: ["cross_package_common.proto", "cross_package.proto"],
      }),
      [
        'import { CrossPackageRequestSchema as RequestPbSchema } from "./cross_package_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        'import type { CommonUser } from "./cross_package_common_effect_grpc.js";',
        "import {",
        "  CrossPackageFeatureGrpcRegistry,",
        "  type CrossPackageRequest,",
        '} from "./cross_package_effect_grpc.js";',
        'const user: CommonUser = { id: "1", state: 1 };',
        "const value: CrossPackageRequest = {",
        "  user,",
        "  users: [user],",
        "  byId: { one: user },",
        "  state: 1,",
        "  states: [1],",
        '  choice: { case: "picked", value: user },',
        "};",
        'const entry = CrossPackageFeatureGrpcRegistry.get("features.v1.CrossPackageFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(RequestPbSchema, entry.toGrpcRequest(value));",
        "",
      ].join("\n"),
    );
    typecheckProtoFeature(
      generateProtoFeature("optional_scalars"),
      [
        'import { OptionalScalarsSchema as OptionalScalarsPbSchema } from "./optional_scalars_pb.js";',
        'import { create } from "@bufbuild/protobuf";',
        "import {",
        "  OptionalScalarFeatureGrpcRegistry,",
        "  type OptionalScalars,",
        '} from "./optional_scalars_effect_grpc.js";',
        "const value: OptionalScalars = {",
        '  name: "a",',
        "  enabled: true,",
        "  blob: new Uint8Array([1, 2]),",
        "  score: 1,",
        "  total: 2n,",
        "  state: 1,",
        "};",
        "const empty: OptionalScalars = {};",
        'const entry = OptionalScalarFeatureGrpcRegistry.get("features.v1.OptionalScalarFeature/Echo");',
        'if (!entry) throw new Error("missing registry entry");',
        "create(OptionalScalarsPbSchema, entry.toGrpcRequest(value));",
        "create(OptionalScalarsPbSchema, entry.toGrpcRequest(empty));",
        "",
      ].join("\n"),
    );
  }, 120_000);

  it("converts repeated scalar fields through generated registry output", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("repeated_scalars"),
      "RepeatedScalarFeatureGrpcRegistry",
      "features.v1.RepeatedScalarFeature/Echo",
    );

    expect(entry.fromGrpcRequest({})).toEqual({
      tags: [],
      flags: [],
      blobs: [],
      scores: [],
      counts: [],
      ratios: [],
      weights: [],
    });
    expect(
      entry.toGrpcRequest({
        tags: undefined,
        flags: undefined,
        blobs: undefined,
        scores: undefined,
        counts: undefined,
        ratios: undefined,
        weights: undefined,
      }),
    ).toEqual({
      tags: [],
      flags: [],
      blobs: [],
      scores: [],
      counts: [],
      ratios: [],
      weights: [],
    });

    const grpcValue = {
      tags: ["alpha", "beta"],
      flags: [true, false],
      blobs: [new Uint8Array([1, 2])],
      scores: [1, -2],
      counts: [3, 4],
      ratios: [1.5],
      weights: [2.5],
    };
    const encodedValue = {
      ...grpcValue,
      blobs: ["AQI="],
    };

    expect(entry.fromGrpcRequest(grpcValue)).toEqual(encodedValue);
    expect(entry.toGrpcRequest(encodedValue)).toEqual(grpcValue);
  });

  it("converts repeated same-file message fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("repeated_messages"),
      "RepeatedMessageFeatureGrpcRegistry",
      "features.v1.RepeatedMessageFeature/Echo",
    );

    expect(entry.fromGrpcRequest({})).toEqual({ users: [] });
    expect(entry.fromGrpcRequest({ users: [] })).toEqual({ users: [] });
    expect(entry.toGrpcRequest({ users: undefined })).toEqual({ users: [] });
    expect(entry.toGrpcRequest({ users: [] })).toEqual({ users: [] });
    expect(
      entry.fromGrpcRequest({
        users: [{ id: "1", addresses: [{ city: "Berlin" }] }],
      }),
    ).toEqual({
      users: [{ id: "1", addresses: [{ city: "Berlin" }] }],
    });
    expect(
      entry.toGrpcRequest({
        users: [{ id: "1", addresses: [{ city: "Berlin" }] }],
      }),
    ).toEqual({
      users: [{ id: "1", addresses: [{ city: "Berlin" }] }],
    });
  });

  it("converts enum fields and accepts forward-compatible values", async () => {
    const { entry, generated } = await registryEntry(
      generateProtoFeature("enums"),
      "EnumFeatureGrpcRegistry",
      "features.v1.EnumFeature/Echo",
    );

    expect(entry.fromGrpcRequest({ id: "1", state: 1 })).toEqual({
      id: "1",
      state: 1,
      history: [],
    });
    expect(entry.fromGrpcRequest({ id: "1", state: 99 })).toEqual({
      id: "1",
      state: 99,
      history: [],
    });
    expect(
      entry.fromGrpcRequest({ id: "1", state: 1, history: [1, 2, 99] }),
    ).toEqual({ id: "1", state: 1, history: [1, 2, 99] });
    expect(
      entry.toGrpcRequest({ id: "1", state: 1, history: [1, 2, 99] }),
    ).toEqual({ id: "1", state: 1, history: [1, 2, 99] });
    expect(
      Schema.decodeUnknownSync(generated.EnumUserSchema)({
        id: "1",
        state: 99,
        history: [99],
      }),
    ).toEqual({ id: "1", state: 99, history: [99] });
  });

  it("converts imported same-package message fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("imported_messages", {
        primary: "imported_messages.proto",
        protoFiles: ["imported_common.proto", "imported_messages.proto"],
      }),
      "ImportedMessageFeatureGrpcRegistry",
      "features.v1.ImportedMessageFeature/Echo",
    );

    const value = { user: { id: "1", name: "Ada" }, state: 1 };
    expect(entry.fromGrpcRequest(value)).toEqual(value);
    expect(entry.toGrpcRequest(value)).toEqual(value);
  });

  it("converts 64-bit scalar fields as bigint by default", async () => {
    const { entry, generated } = await registryEntry(
      generateProtoFeature("int64_bigint"),
      "Int64BigintFeatureGrpcRegistry",
      "features.v1.Int64BigintFeature/Echo",
    );
    const grpcValue = {
      id: 1n,
      count: 2n,
      delta: -3n,
      hash: 4n,
      value: -5n,
    };
    const encodedValue = {
      id: "1",
      count: "2",
      delta: "-3",
      hash: "4",
      value: "-5",
    };

    expect(entry.fromGrpcRequest(grpcValue)).toEqual(encodedValue);
    expect(entry.toGrpcRequest(encodedValue)).toEqual(grpcValue);
    expect(() =>
      Schema.decodeUnknownSync(generated.Int64ScalarsSchema)({
        ...grpcValue,
        count: -1n,
      }),
    ).toThrow();
  });

  it("converts Timestamp and Duration fields", async () => {
    const feature = generateProtoFeature("well_known_types");
    const { entry } = await registryEntry(
      feature,
      "WellKnownTypeFeatureGrpcRegistry",
      "features.v1.WellKnownTypeFeature/Echo",
    );

    const decoded = entry.fromGrpcRequest({
      createdAt: { seconds: 1n, nanos: 500_000_000 },
      timeout: { seconds: 2n, nanos: 250_000_000 },
      enabled: { value: true },
    });

    expect(decoded).toEqual({
      createdAt: "1970-01-01T00:00:01.500Z",
      timeout: { _tag: "Millis", value: 2250 },
      enabled: true,
    });
    expect(
      (
        entry.fromGrpcRequest({
          createdAt: { seconds: 0n, nanos: 1_999_999 },
        }) as { readonly createdAt?: unknown }
      ).createdAt,
    ).toBe("1970-01-01T00:00:00.001Z");
    expect(entry.fromGrpcRequest({})).toEqual({
      createdAt: undefined,
      timeout: undefined,
      enabled: undefined,
    });
    expect(
      entry.toGrpcRequest({
        createdAt: "1970-01-01T00:00:00.000Z",
        timeout: { _tag: "Millis", value: 0 },
        enabled: true,
      }),
    ).toEqual({
      createdAt: { seconds: 0n, nanos: 0 },
      timeout: { seconds: 0n, nanos: 0 },
      enabled: true,
    });
    expect(
      entry.toGrpcRequest({
        createdAt: "1970-01-01T00:00:00.000Z",
        timeout: { _tag: "Nanos", value: "1" },
        enabled: false,
      }),
    ).toEqual({
      createdAt: { seconds: 0n, nanos: 0 },
      timeout: { seconds: 0n, nanos: 1 },
      enabled: false,
    });

    const { entry: timestampEntry } = await registryEntry(
      feature,
      "WellKnownTypeFeatureGrpcRegistry",
      "features.v1.WellKnownTypeFeature/EchoTimestamp",
    );
    expect(
      timestampEntry.fromGrpcRequest({ seconds: 1n, nanos: 500_000_000 }),
    ).toBe("1970-01-01T00:00:01.500Z");
    expect(timestampEntry.toGrpcRequest("1970-01-01T00:00:00.000Z")).toEqual({
      seconds: 0n,
      nanos: 0,
    });

    const { entry: durationEntry } = await registryEntry(
      feature,
      "WellKnownTypeFeatureGrpcRegistry",
      "features.v1.WellKnownTypeFeature/EchoDuration",
    );
    expect(
      durationEntry.fromGrpcRequest({ seconds: 2n, nanos: 250_000_000 }),
    ).toEqual({ _tag: "Millis", value: 2250 });
    expect(durationEntry.toGrpcRequest({ _tag: "Nanos", value: "1" })).toEqual({
      seconds: 0n,
      nanos: 1,
    });

    const { entry: boolValueEntry } = await registryEntry(
      feature,
      "WellKnownTypeFeatureGrpcRegistry",
      "features.v1.WellKnownTypeFeature/EchoBoolValue",
    );
    expect(boolValueEntry.fromGrpcRequest({ value: true })).toBe(true);
    expect(boolValueEntry.fromGrpcRequest({})).toBe(false);
    expect(boolValueEntry.toGrpcRequest(false)).toEqual({ value: false });
  });

  it("converts comprehensive field shapes", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("field_shapes"),
      "FieldShapeFeatureGrpcRegistry",
      "features.v1.FieldShapeFeature/Echo",
    );
    const value = {
      ratio: 1.5,
      score: 2.5,
      count: 3,
      total: "4",
      size: 5,
      serial: "6",
      label: "sample",
      blob: "AQI=",
      payload: {
        typeUrl: "type.googleapis.com/example.Payload",
        value: "AwQ=",
      },
      metadata: { enabled: true },
      dynamicValue: { nested: false },
      listValue: [1, "two"],
      updateMask: "fooBar,baz",
      choice: { case: "state", value: 1 },
      labelsByNumber: { 1: "one" },
      statesByNumber: { 2: 1 },
      statesByName: { ready: 1 },
      childrenByNumber: { 3: { id: "child" } },
    };

    const grpcValue = entry.toGrpcRequest(value);

    expect(grpcValue).toMatchObject({
      ratio: 1.5,
      score: 2.5,
      count: 3,
      total: 4n,
      size: 5,
      serial: 6n,
      label: "sample",
      blob: new Uint8Array([1, 2]),
      payload: {
        typeUrl: "type.googleapis.com/example.Payload",
        value: new Uint8Array([3, 4]),
      },
      choice: { case: "state", value: 1 },
      labelsByNumber: { 1: "one" },
      statesByNumber: { 2: 1 },
      statesByName: { ready: 1 },
      childrenByNumber: { 3: { id: "child" } },
    });
    expect(entry.fromGrpcRequest(grpcValue)).toEqual(value);
    expect(
      (
        entry.fromGrpcRequest({
          choice: { case: "timeout", value: { seconds: 1n, nanos: 0 } },
        }) as { readonly choice?: unknown }
      ).choice,
    ).toEqual({ case: "timeout", value: { _tag: "Millis", value: 1000 } });
    expect(
      (
        entry.fromGrpcRequest({
          choice: { case: "note", value: { value: "hello" } },
        }) as { readonly choice?: unknown }
      ).choice,
    ).toEqual({ case: "note", value: "hello" });
    expect(
      (
        entry.toGrpcRequest({
          choice: { case: "note", value: "hello" },
        }) as { readonly choice?: unknown }
      ).choice,
    ).toEqual({ case: "note", value: { value: "hello" } });
  });

  it("converts string-key map fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("maps"),
      "MapFeatureGrpcRegistry",
      "features.v1.MapFeature/Echo",
    );
    const value = {
      labels: { env: "test" },
      counts: { seen: 1 },
      users: { one: { id: "1" } },
    };

    expect(entry.fromGrpcRequest({})).toEqual({
      labels: {},
      counts: {},
      users: {},
    });
    expect(entry.fromGrpcRequest(value)).toEqual(value);
    expect(entry.toGrpcRequest(value)).toEqual(value);
  });

  it("converts oneof fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("oneofs"),
      "OneofFeatureGrpcRegistry",
      "features.v1.OneofFeature/Echo",
    );

    // The unset oneof case encodes as `null` (the JSON codec's `Schema.Undefined`
    // representation), so it decodes back to the `undefined` case.
    expect(entry.fromGrpcRequest({})).toEqual({ query: { case: null } });
    expect(
      entry.fromGrpcRequest({ query: { case: "id", value: "1" } }),
    ).toEqual({ query: { case: "id", value: "1" } });
    expect(
      entry.fromGrpcRequest({
        query: { case: "email", value: "a@example.com" },
      }),
    ).toEqual({ query: { case: "email", value: "a@example.com" } });
    expect(
      entry.fromGrpcRequest({ query: { case: "user", value: { id: "1" } } }),
    ).toEqual({ query: { case: "user", value: { id: "1" } } });
    expect(() =>
      entry.toGrpcRequest({ query: { case: "missing", value: "x" } }),
    ).toThrow("Unknown oneof case query");
  });

  it("converts nested message and nested enum fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("nested_messages"),
      "NestedMessageFeatureGrpcRegistry",
      "features.v1.NestedMessageFeature/Echo",
    );

    const inner = { id: "1", status: 1 };
    const value = {
      inner,
      items: [inner, { id: "2", status: 99 }],
      byId: { one: inner },
      choice: { case: "picked", value: inner },
    };
    expect(entry.fromGrpcRequest(value)).toEqual(value);
    expect(entry.toGrpcRequest(value)).toEqual(value);
    expect(entry.fromGrpcRequest({})).toEqual({
      items: [],
      byId: {},
      choice: { case: null },
    });
  });

  it("converts cross-package imported message and enum fields", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("cross_package", {
        primary: "cross_package.proto",
        protoFiles: ["cross_package_common.proto", "cross_package.proto"],
      }),
      "CrossPackageFeatureGrpcRegistry",
      "features.v1.CrossPackageFeature/Echo",
    );

    const user = { id: "1", state: 1 };
    const value = {
      user,
      users: [user],
      byId: { one: user },
      state: 1,
      states: [1, 99],
      choice: { case: "picked", value: user },
    };
    expect(entry.fromGrpcRequest(value)).toEqual(value);
    expect(entry.toGrpcRequest(value)).toEqual(value);
    expect(entry.fromGrpcRequest({})).toEqual({
      users: [],
      byId: {},
      states: [],
      choice: { case: null },
    });
  });

  it("converts cross-package imported method input and output", async () => {
    const { entry } = await registryEntry(
      generateProtoFeature("cross_package", {
        primary: "cross_package.proto",
        protoFiles: ["cross_package_common.proto", "cross_package.proto"],
      }),
      "CrossPackageFeatureGrpcRegistry",
      "features.v1.CrossPackageFeature/GetUser",
    );

    const user = { id: "1", state: 1 };
    expect(entry.fromGrpcRequest(user)).toEqual(user);
    expect(entry.toGrpcRequest(user)).toEqual(user);
  });

  it("converts optional scalar and enum fields", async () => {
    const { entry, generated } = await registryEntry(
      generateProtoFeature("optional_scalars"),
      "OptionalScalarFeatureGrpcRegistry",
      "features.v1.OptionalScalarFeature/Echo",
    );

    const grpcValue = {
      name: "a",
      enabled: true,
      blob: new Uint8Array([1, 2]),
      score: 1,
      total: 2n,
      state: 1,
    };
    const encodedValue = {
      ...grpcValue,
      blob: "AQI=",
      total: "2",
    };
    const absent = {
      name: undefined,
      enabled: undefined,
      blob: undefined,
      score: undefined,
      total: undefined,
      state: undefined,
    };

    expect(entry.fromGrpcRequest(grpcValue)).toEqual(encodedValue);
    expect(entry.toGrpcRequest(encodedValue)).toEqual(grpcValue);
    expect(entry.fromGrpcRequest({})).toEqual(absent);
    expect(entry.toGrpcRequest({})).toEqual(absent);
    expect(
      Schema.decodeUnknownSync(generated.OptionalScalarsSchema)({}),
    ).toEqual({});
  });
});
