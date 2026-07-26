import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { fileDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { base64Encode } from "@bufbuild/protobuf/wire";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";

import * as GrpcHealth from "../src/GrpcHealth.js";
import type * as GrpcMethodRegistry from "../src/GrpcMethodRegistry.js";
import * as GrpcReflection from "../src/GrpcReflection.js";

/** `grpc::StatusCode` wire values used by reflection error responses. */
const NOT_FOUND = 5;
const INVALID_ARGUMENT = 3;

const HEALTH_PROTO = "grpc/health/v1/health.proto";

const decodeFileName = (bytes: Uint8Array): string =>
  fromBinary(FileDescriptorProtoSchema, bytes).name;

describe("GrpcReflection index", () => {
  const index = GrpcReflection.makeIndex([
    GrpcHealth.HealthGrpcRegistry,
    GrpcReflection.ReflectionGrpcRegistry,
  ]);

  it("lists every registered service, sorted", () => {
    const response = GrpcReflection.respond(index, {
      host: "localhost",
      messageRequest: { case: "listServices", value: "*" },
    });

    expect(response.validHost).toBe("localhost");
    expect(response.messageResponse).toEqual({
      case: "listServicesResponse",
      value: {
        service: [
          { name: "grpc.health.v1.Health" },
          { name: "grpc.reflection.v1.ServerReflection" },
        ],
      },
    });
  });

  it.each([
    ["service", "grpc.health.v1.Health"],
    ["method", "grpc.health.v1.Health.Check"],
    ["message", "grpc.health.v1.HealthCheckRequest"],
    ["nested enum", "grpc.health.v1.HealthCheckResponse.ServingStatus"],
    ["leading dot", ".grpc.health.v1.Health"],
  ])("resolves file_containing_symbol for a %s", (_kind, symbol) => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: { case: "fileContainingSymbol", value: symbol },
    });

    expect(descriptorNames(response)).toEqual([HEALTH_PROTO]);
  });

  it("resolves file_by_filename", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: { case: "fileByFilename", value: HEALTH_PROTO },
    });

    expect(descriptorNames(response)).toEqual([HEALTH_PROTO]);
  });

  it("answers an unknown filename with an in-band NOT_FOUND", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: { case: "fileByFilename", value: "missing.proto" },
    });

    expect(response.messageResponse).toEqual({
      case: "errorResponse",
      value: {
        errorCode: NOT_FOUND,
        errorMessage: "file not found: missing.proto",
      },
    });
  });

  it("answers unknown names with an in-band NOT_FOUND echoing the request", () => {
    const request = {
      host: "localhost",
      messageRequest: {
        case: "fileContainingSymbol",
        value: "no.such.Symbol",
      },
    } as const;
    const response = GrpcReflection.respond(index, request);

    expect(response.messageResponse).toEqual({
      case: "errorResponse",
      value: {
        errorCode: NOT_FOUND,
        errorMessage: "symbol not found: no.such.Symbol",
      },
    });
    expect(response.originalRequest).toEqual(request);
  });

  it("answers a request without a message_request with INVALID_ARGUMENT", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: { case: undefined },
    });

    expect(response.messageResponse).toMatchObject({
      case: "errorResponse",
      value: { errorCode: INVALID_ARGUMENT },
    });
  });
});

describe("GrpcReflection index with imports and extensions", () => {
  // Two synthetic descriptors: test/a.proto imports test/b.proto and declares
  // an extension of test.b.BMsg, exercising dependency closures and the
  // extension lookups (the vendored health/reflection protos have neither).
  const protoB = create(FileDescriptorProtoSchema, {
    name: "test/b.proto",
    package: "test.b",
    syntax: "proto2",
    messageType: [{ name: "BMsg", extensionRange: [{ start: 100, end: 200 }] }],
  });
  const genFileB = fileDesc(
    base64Encode(toBinary(FileDescriptorProtoSchema, protoB)),
  );
  const protoA = create(FileDescriptorProtoSchema, {
    name: "test/a.proto",
    package: "test.a",
    syntax: "proto2",
    dependency: ["test/b.proto"],
    messageType: [{ name: "Req" }, { name: "Res" }],
    service: [
      {
        name: "AService",
        method: [
          { name: "Do", inputType: ".test.a.Req", outputType: ".test.a.Res" },
        ],
      },
    ],
    extension: [
      {
        name: "my_ext",
        number: 100,
        extendee: ".test.b.BMsg",
        // TYPE_STRING / LABEL_OPTIONAL wire values from descriptor.proto.
        type: 9,
        label: 1,
      },
    ],
  });
  const genFileA = fileDesc(
    base64Encode(toBinary(FileDescriptorProtoSchema, protoA)),
    [genFileB],
  );
  // makeIndex only reads `entry.service`; the rest of the registry entry is
  // irrelevant to indexing.
  const registry = new Map([
    ["test.a.AService/Do", { service: serviceDesc(genFileA, 0) }],
  ]) as unknown as GrpcMethodRegistry.GrpcMethodRegistry;
  const index = GrpcReflection.makeIndex([registry]);

  it("returns the transitive import closure, requested file first", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "fileContainingSymbol",
        value: "test.a.AService.Do",
      },
    });

    expect(descriptorNames(response)).toEqual(["test/a.proto", "test/b.proto"]);
  });

  it("imported files are themselves queryable", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: { case: "fileContainingSymbol", value: "test.b.BMsg" },
    });

    expect(descriptorNames(response)).toEqual(["test/b.proto"]);
  });

  it("resolves file_containing_extension", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "fileContainingExtension",
        value: { containingType: "test.b.BMsg", extensionNumber: 100 },
      },
    });

    expect(descriptorNames(response)).toEqual(["test/a.proto", "test/b.proto"]);

    const missing = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "fileContainingExtension",
        value: { containingType: "test.b.BMsg", extensionNumber: 101 },
      },
    });
    expect(missing.messageResponse).toMatchObject({
      case: "errorResponse",
      value: { errorCode: NOT_FOUND },
    });
  });

  it("resolves all_extension_numbers_of_type", () => {
    const known = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "allExtensionNumbersOfType",
        value: "test.b.BMsg",
      },
    });
    expect(known.messageResponse).toEqual({
      case: "allExtensionNumbersResponse",
      value: { baseTypeName: "test.b.BMsg", extensionNumber: [100] },
    });

    const noExtensions = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "allExtensionNumbersOfType",
        value: "test.a.Req",
      },
    });
    expect(noExtensions.messageResponse).toEqual({
      case: "allExtensionNumbersResponse",
      value: { baseTypeName: "test.a.Req", extensionNumber: [] },
    });

    const unknown = GrpcReflection.respond(index, {
      host: "",
      messageRequest: {
        case: "allExtensionNumbersOfType",
        value: "test.Missing",
      },
    });
    expect(unknown.messageResponse).toMatchObject({
      case: "errorResponse",
      value: { errorCode: NOT_FOUND },
    });
  });
});

const descriptorNames = (
  response: GrpcReflection.ServerReflectionResponse,
): ReadonlyArray<string> =>
  response.messageResponse.case === "fileDescriptorResponse"
    ? response.messageResponse.value.fileDescriptorProto.map(decodeFileName)
    : [];
