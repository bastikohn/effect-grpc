import { create, fromBinary, toBinary } from "@bufbuild/protobuf";
import { fileDesc, serviceDesc } from "@bufbuild/protobuf/codegenv2";
import { base64Decode, base64Encode } from "@bufbuild/protobuf/wire";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Context, Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcHealth from "../src/GrpcHealth.js";
import type * as GrpcMethodRegistry from "../src/GrpcMethodRegistry.js";
import * as GrpcReflection from "../src/GrpcReflection.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";

/** `grpc::StatusCode` wire values used by reflection error responses. */
const NOT_FOUND = 5;
const INVALID_ARGUMENT = 3;

const HEALTH_PROTO = "grpc/health/v1/health.proto";

const decodeFileName = (base64: string): string =>
  fromBinary(FileDescriptorProtoSchema, base64Decode(base64)).name;

describe("GrpcReflection index", () => {
  const index = GrpcReflection.makeIndex([
    GrpcHealth.HealthGrpcRegistry,
    GrpcReflection.ReflectionGrpcRegistry,
  ]);

  it("lists every registered service, sorted", () => {
    const response = GrpcReflection.respond(index, {
      host: "localhost",
      listServices: "*",
    });

    expect(response.validHost).toBe("localhost");
    expect(response.listServicesResponse?.service).toEqual([
      { name: "grpc.health.v1.Health" },
      { name: "grpc.reflection.v1.ServerReflection" },
      { name: "grpc.reflection.v1alpha.ServerReflection" },
    ]);
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
      fileContainingSymbol: symbol,
    });

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual([HEALTH_PROTO]);
  });

  it("resolves file_by_filename", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      fileByFilename: HEALTH_PROTO,
    });

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual([HEALTH_PROTO]);
  });

  it("answers unknown names with an in-band NOT_FOUND echoing the request", () => {
    const request = {
      host: "localhost",
      fileContainingSymbol: "no.such.Symbol",
    };
    const response = GrpcReflection.respond(index, request);

    expect(response.fileDescriptorResponse).toBeUndefined();
    expect(response.errorResponse).toEqual({
      errorCode: NOT_FOUND,
      errorMessage: "symbol not found: no.such.Symbol",
    });
    expect(response.originalRequest).toEqual(request);
  });

  it("answers a request without a message_request with INVALID_ARGUMENT", () => {
    const response = GrpcReflection.respond(index, { host: "" });

    expect(response.errorResponse).toMatchObject({
      errorCode: INVALID_ARGUMENT,
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
      fileContainingSymbol: "test.a.AService.Do",
    });

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual(["test/a.proto", "test/b.proto"]);
  });

  it("imported files are themselves queryable", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      fileContainingSymbol: "test.b.BMsg",
    });

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual(["test/b.proto"]);
  });

  it("resolves file_containing_extension", () => {
    const response = GrpcReflection.respond(index, {
      host: "",
      fileContainingExtension: {
        containingType: "test.b.BMsg",
        extensionNumber: 100,
      },
    });

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual(["test/a.proto", "test/b.proto"]);

    const missing = GrpcReflection.respond(index, {
      host: "",
      fileContainingExtension: {
        containingType: "test.b.BMsg",
        extensionNumber: 101,
      },
    });
    expect(missing.errorResponse).toMatchObject({ errorCode: NOT_FOUND });
  });

  it("resolves all_extension_numbers_of_type", () => {
    const known = GrpcReflection.respond(index, {
      host: "",
      allExtensionNumbersOfType: "test.b.BMsg",
    });
    expect(known.allExtensionNumbersResponse).toEqual({
      baseTypeName: "test.b.BMsg",
      extensionNumber: [100],
    });

    const noExtensions = GrpcReflection.respond(index, {
      host: "",
      allExtensionNumbersOfType: "test.a.Req",
    });
    expect(noExtensions.allExtensionNumbersResponse).toEqual({
      baseTypeName: "test.a.Req",
      extensionNumber: [],
    });

    const unknown = GrpcReflection.respond(index, {
      host: "",
      allExtensionNumbersOfType: "test.Missing",
    });
    expect(unknown.errorResponse).toMatchObject({ errorCode: NOT_FOUND });
  });
});

describe("grpc.reflection over the server protocol", () => {
  it("answers a mixed request stream on v1, in order", async () => {
    const responses = await withReflectionServer(
      "grpc.reflection.v1.ServerReflection",
      [
        {
          host: "localhost",
          messageRequest: { case: "listServices", value: "*" },
        },
        {
          host: "localhost",
          messageRequest: {
            case: "fileContainingSymbol",
            value: "grpc.health.v1.Health",
          },
        },
        {
          host: "localhost",
          messageRequest: { case: "fileByFilename", value: "missing.proto" },
        },
      ],
    );

    expect(responses).toHaveLength(3);

    const [list, file, missing] = responses;

    const listOneof = messageResponseOf(list);
    expect(listOneof.case).toBe("listServicesResponse");
    expect(
      (
        listOneof.value as { service: ReadonlyArray<{ name: string }> }
      ).service.map((entry) => entry.name),
    ).toContain("grpc.health.v1.Health");

    const fileOneof = messageResponseOf(file);
    expect(fileOneof.case).toBe("fileDescriptorResponse");
    const descriptors = (
      fileOneof.value as { fileDescriptorProto: ReadonlyArray<Uint8Array> }
    ).fileDescriptorProto;
    expect(
      descriptors.map(
        (bytes) => fromBinary(FileDescriptorProtoSchema, bytes).name,
      ),
    ).toEqual([HEALTH_PROTO]);

    const missingOneof = messageResponseOf(missing);
    expect(missingOneof.case).toBe("errorResponse");
    expect(missingOneof.value).toMatchObject({ errorCode: NOT_FOUND });
    // The failed request is echoed back per the spec.
    expect(missing).toMatchObject({
      originalRequest: {
        messageRequest: { case: "fileByFilename", value: "missing.proto" },
      },
    });
  });

  it("serves the identical protocol under the v1alpha alias", async () => {
    const responses = await withReflectionServer(
      "grpc.reflection.v1alpha.ServerReflection",
      [
        {
          host: "",
          messageRequest: {
            case: "fileContainingSymbol",
            value: "grpc.reflection.v1alpha.ServerReflection",
          },
        },
      ],
    );

    const oneof = messageResponseOf(responses[0]);
    expect(oneof.case).toBe("fileDescriptorResponse");
    const descriptors = (
      oneof.value as { fileDescriptorProto: ReadonlyArray<Uint8Array> }
    ).fileDescriptorProto;
    expect(
      descriptors.map(
        (bytes) => fromBinary(FileDescriptorProtoSchema, bytes).name,
      ),
    ).toEqual(["grpc/reflection/v1alpha/reflection.proto"]);
  });
});

const messageResponseOf = (
  response: unknown,
): { case?: string; value?: unknown } =>
  (response as Record<string, { case?: string; value?: unknown } | undefined>)[
    "messageResponse"
  ] ?? {};

/**
 * Runs the real reflection handlers behind the server protocol — streaming
 * handlers layer, registry converters, and connect route implementation,
 * without a TCP listener — and collects the responses to `requests`.
 */
const withReflectionServer = (
  serviceTypeName: string,
  requests: ReadonlyArray<unknown>,
): Promise<ReadonlyArray<unknown>> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        const reflection = GrpcReflection.service([GrpcHealth.service]);
        const context = yield* Layer.build(reflection.handlers);
        const { routes } = yield* GrpcServerProtocol.make({
          registry: new Map([
            ...GrpcHealth.HealthGrpcRegistry,
            ...reflection.registry,
          ]),
          handlers: Context.get(context, GrpcServerProtocol.GrpcHandlers),
        });

        const implementation = captureImplementations(routes)[serviceTypeName];
        if (!implementation) {
          throw new Error(`No route registered for ${serviceTypeName}`);
        }
        return yield* Effect.promise(async () => {
          const responses: unknown[] = [];
          const iterable = implementation["serverReflectionInfo"]!(
            (async function* () {
              yield* requests;
            })(),
            handlerContext(),
          ) as AsyncIterable<unknown>;
          for await (const response of iterable) {
            responses.push(response);
          }
          return responses;
        });
      }),
    ),
  );

const captureImplementations = (
  routes: (router: ConnectRouter) => ConnectRouter,
) => {
  const implementations: Record<
    string,
    Record<
      string,
      (
        request: unknown,
        context: HandlerContext,
      ) => Promise<unknown> | AsyncIterable<unknown>
    >
  > = {};
  const router = {
    service(service: { typeName: string }, implementation: unknown) {
      implementations[service.typeName] =
        implementation as (typeof implementations)[string];
      return router;
    },
  };

  routes(router as unknown as ConnectRouter);
  return implementations;
};

const handlerContext = (): HandlerContext =>
  ({
    requestHeader: new Headers(),
    signal: new AbortController().signal,
  }) as HandlerContext;
