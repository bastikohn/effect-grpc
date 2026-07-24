import { create, type DescFile } from "@bufbuild/protobuf";
import {
  CodeGeneratorRequestSchema,
  type DescriptorProto,
  DescriptorProtoSchema,
  type EnumDescriptorProto,
  EnumDescriptorProtoSchema,
  EnumValueDescriptorProtoSchema,
  FieldDescriptorProto_Label,
  FieldDescriptorProto_Type,
  type FileDescriptorProto,
  FieldDescriptorProtoSchema,
  FileDescriptorProtoSchema,
  MessageOptionsSchema,
  type MethodDescriptorProto,
  MethodDescriptorProtoSchema,
  type OneofDescriptorProto,
  OneofDescriptorProtoSchema,
  ServiceDescriptorProtoSchema,
} from "@bufbuild/protobuf/wkt";
import { describe, expect, it } from "vitest";

import { generateFile } from "../src/generate.js";
import { parseOptions } from "../src/options.js";
import { detectImportCycles, plugin } from "../src/pluginDefinition.js";
import type { GeneratorFile } from "../src/types.js";

const demoFile: GeneratorFile = {
  protoFileName: "demo/v1/user_service.proto",
  packageName: "demo.v1",
  importExtension: "js",
  imports: [],
  enums: [],
  messages: [
    {
      name: "GetUserRequest",
      fields: [{ kind: "scalar", name: "id", type: "string" }],
    },
    {
      name: "GetUserResponse",
      fields: [
        {
          kind: "message",
          name: "user",
          messageName: "User",
          source: "local",
          optional: true,
        },
      ],
    },
    {
      name: "WatchUsersRequest",
      fields: [
        { kind: "scalar", name: "tenantId", type: "string" },
        { kind: "scalar", name: "count", type: "number" },
      ],
    },
    {
      name: "User",
      fields: [
        { kind: "scalar", name: "id", type: "string" },
        { kind: "scalar", name: "name", type: "string" },
      ],
    },
    {
      name: "UserEvent",
      fields: [
        { kind: "scalar", name: "id", type: "string" },
        { kind: "scalar", name: "name", type: "string" },
        { kind: "scalar", name: "action", type: "string" },
        { kind: "scalar", name: "sequence", type: "number" },
      ],
    },
  ],
  services: [
    {
      name: "UserService",
      typeName: "demo.v1.UserService",
      methods: [
        {
          name: "GetUser",
          localName: "getUser",
          kind: "unary",
          inputType: { name: "GetUserRequest" },
          outputType: { name: "GetUserResponse" },
        },
        {
          name: "WatchUsers",
          localName: "watchUsers",
          kind: "server-streaming",
          inputType: { name: "WatchUsersRequest" },
          outputType: { name: "UserEvent" },
        },
        {
          name: "UploadUsers",
          localName: "uploadUsers",
          kind: "client-streaming",
          inputType: { name: "User" },
          outputType: { name: "GetUserResponse" },
        },
        {
          name: "ChatUsers",
          localName: "chatUsers",
          kind: "bidi-streaming",
          inputType: { name: "UserEvent" },
          outputType: { name: "UserEvent" },
        },
      ],
    },
  ],
};

describe("generateFile", () => {
  it("generates schemas, registry, client, and server glue", () => {
    const output = generateFile(demoFile);

    expect(output).toContain("export const UserSchema = Schema.Struct");
    expect(output).toContain(
      "user: Schema.optional(Schema.suspend((): typeof UserSchema => UserSchema))",
    );
    expect(output).toContain(
      'user: readField(message, "user") == null ? undefined : fromUser(readField(message, "user"))',
    );
    expect(output).toContain("export type UserServiceClientError");
    expect(output).toContain("UserServiceGrpcRegistry");
    expect(output).toContain("export interface UserServiceClientService");
    expect(output).toContain("export interface UserServiceImplementation");
    expect(output).toContain("UserServiceHandlersLayer");
    expect(output).toMatchSnapshot();
  });

  it("routes every method through the unified handlers map", () => {
    const output = generateFile(demoFile);

    // The server path is fully off Effect RPC: no `Rpc.make`/`RpcGroup`
    // consts and no `effect/unstable/rpc` import in generated code.
    expect(output).not.toContain("Rpc.make(");
    expect(output).not.toContain("RpcGroup");
    expect(output).not.toContain("effect/unstable/rpc");
    expect(output).toContain(
      'invoker.clientStream("demo.v1.UserService/UploadUsers", requests, options)',
    );
    expect(output).toContain(
      'invoker.bidiStream("demo.v1.UserService/ChatUsers", requests, options)',
    );
    expect(output).toContain("GrpcServerProtocol.handlersLayer<R>({");
    expect(output).toContain('kind: "unary"');
    expect(output).toContain('kind: "server-streaming"');
    expect(output).toContain('kind: "client-streaming"');
    expect(output).toContain('kind: "bidi-streaming"');
    expect(output).toContain(
      "requests: Stream.Stream<User, GrpcStatusError.GrpcStatusError>",
    );
    expect(output).toContain("successSchema: GetUserResponseSchema");
  });

  it("omits the Stream import for unary-only services", () => {
    const output = generateFile({
      protoFileName: "demo/v1/ping.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [],
      enums: [],
      messages: [
        {
          name: "PingRequest",
          fields: [{ kind: "scalar", name: "id", type: "string" }],
        },
        {
          name: "PingResponse",
          fields: [{ kind: "scalar", name: "id", type: "string" }],
        },
      ],
      services: [
        {
          name: "PingService",
          typeName: "demo.v1.PingService",
          methods: [
            {
              name: "Ping",
              localName: "ping",
              kind: "unary",
              inputType: { name: "PingRequest" },
              outputType: { name: "PingResponse" },
            },
          ],
        },
      ],
    });

    expect(output).toContain(
      'import { Context, Effect, Layer, Schema } from "effect";',
    );
    expect(output).not.toContain("Stream");
  });

  it("omits readField and compact when every message is empty", () => {
    const output = generateFile({
      protoFileName: "demo/v1/empty.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [],
      enums: [],
      messages: [
        { name: "VoidRequest", fields: [] },
        { name: "VoidResponse", fields: [] },
      ],
      services: [
        {
          name: "VoidService",
          typeName: "demo.v1.VoidService",
          methods: [
            {
              name: "Call",
              localName: "call",
              kind: "unary",
              inputType: { name: "VoidRequest" },
              outputType: { name: "VoidResponse" },
            },
          ],
        },
      ],
    });

    expect(output).not.toContain("readField");
    expect(output).not.toContain("compact");
    expect(output).toContain(
      "export const fromVoidRequest = (_message: unknown): unknown => ({});",
    );
    expect(output).toContain(
      "export const toVoidRequest = (_value: unknown): Record<string, unknown> => ({});",
    );
  });

  it("emits the annotated Schema.suspend for a recursive self-edge", () => {
    const output = generateFile({
      protoFileName: "demo/v1/node.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [],
      enums: [],
      messages: [
        {
          name: "Node",
          fields: [
            {
              kind: "message",
              name: "next",
              messageName: "Node",
              source: "local",
              optional: true,
            },
          ],
        },
      ],
      services: [
        {
          name: "NodeService",
          typeName: "demo.v1.NodeService",
          methods: [
            {
              name: "Echo",
              localName: "echo",
              kind: "unary",
              inputType: { name: "Node" },
              outputType: { name: "Node" },
            },
          ],
        },
      ],
    });

    // A cyclic edge must break the type recursion with an explicit annotation
    // rather than the self-referential `typeof NodeSchema` form.
    expect(output).toContain(
      "next: Schema.optional(Schema.suspend((): Schema.Codec<unknown, unknown, never, never> => NodeSchema))",
    );
    expect(output).not.toContain("Schema.suspend((): typeof NodeSchema");
  });

  it("omits the bare imported message type when only schema and converters are used", () => {
    const output = generateFile({
      protoFileName: "demo/v1/profile.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [
        {
          protoFileName: "demo/v1/common.proto",
          messages: ["CommonUser"],
          enums: [],
        },
      ],
      enums: [],
      messages: [
        {
          name: "Profile",
          fields: [
            {
              kind: "message",
              name: "user",
              messageName: "CommonUser",
              source: "imported",
              optional: true,
            },
          ],
        },
      ],
      services: [
        {
          name: "ProfileService",
          typeName: "demo.v1.ProfileService",
          methods: [
            {
              name: "GetProfile",
              localName: "getProfile",
              kind: "unary",
              inputType: { name: "Profile" },
              outputType: { name: "Profile" },
            },
          ],
        },
      ],
    });

    // A field-only imported message is referenced exclusively through its
    // schema and converters; the bare `type` alias would be an unused import
    // and fail consumers compiling with `noUnusedLocals`.
    expect(output).toContain("  CommonUserSchema,");
    expect(output).toContain("  fromCommonUser,");
    expect(output).toContain("  toCommonUser,");
    expect(output).not.toContain("type CommonUser");
  });

  it("keeps bare imported types used by method signatures and enum casts", () => {
    const output = generateFile({
      protoFileName: "demo/v1/profile.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [
        {
          protoFileName: "demo/v1/common.proto",
          messages: ["CommonUser"],
          enums: ["CommonState"],
        },
      ],
      enums: [],
      messages: [
        {
          name: "Profile",
          fields: [{ kind: "enum", name: "state", enumName: "CommonState" }],
        },
      ],
      services: [
        {
          name: "ProfileService",
          typeName: "demo.v1.ProfileService",
          methods: [
            {
              name: "GetUser",
              localName: "getUser",
              kind: "unary",
              inputType: { name: "CommonUser" },
              outputType: { name: "CommonUser" },
            },
          ],
        },
      ],
    });

    // A message used as a method input/output appears bare in client/server
    // signatures; an imported enum field appears bare in the from-converter
    // cast. Both aliases must stay imported.
    expect(output).toContain("  type CommonUser,");
    expect(output).toContain("  type CommonState,");
    expect(output).toContain("request: CommonUser");
    expect(output).toContain('readField(message, "state") as CommonState');
  });

  it("omits client-only rpc imports for a streaming-only service", () => {
    const output = generateFile({
      protoFileName: "demo/v1/upload.proto",
      packageName: "demo.v1",
      importExtension: "js",
      imports: [],
      enums: [],
      messages: [
        {
          name: "UploadRequest",
          fields: [{ kind: "scalar", name: "id", type: "string" }],
        },
        {
          name: "UploadResponse",
          fields: [{ kind: "scalar", name: "id", type: "string" }],
        },
      ],
      services: [
        {
          name: "UploadService",
          typeName: "demo.v1.UploadService",
          methods: [
            {
              name: "Upload",
              localName: "upload",
              kind: "client-streaming",
              inputType: { name: "UploadRequest" },
              outputType: { name: "UploadResponse" },
            },
            {
              name: "Chat",
              localName: "chat",
              kind: "bidi-streaming",
              inputType: { name: "UploadRequest" },
              outputType: { name: "UploadResponse" },
            },
          ],
        },
      ],
    });

    // Generated code no longer touches `effect/unstable/rpc` at all: the
    // client depends on the `GrpcInvoker` seam and the server publishes its
    // handlers through `GrpcServerProtocol.handlersLayer`.
    expect(output).not.toContain("effect/unstable/rpc");
    expect(output).toContain("const invoker = yield* GrpcInvoker.GrpcInvoker;");
    expect(output).not.toContain("RpcClient");
    expect(output).not.toContain("RpcClientError");
    expect(output).not.toContain("GrpcClientProtocol");
  });
});

describe("plugin fixture", () => {
  it("generates a real plugin response from descriptor input", () => {
    const response = plugin.run(fixtureRequest());

    expect(response.file).toHaveLength(1);
    expect(response.file[0]?.name).toBe("demo/v1/user_service_effect_grpc.ts");
    expect(response.file[0]?.content).toContain("UserServiceGrpcRegistry");
  });

  it("supports google.protobuf.Empty as a method input or output", () => {
    const response = plugin.run(
      fixtureRequest([], {
        dependency: ["google/protobuf/empty.proto"],
        extraFiles: [emptyFile()],
        methodInputType: ".google.protobuf.Empty",
        methodOutputType: ".google.protobuf.Empty",
      }),
    );

    expect(response.file[0]?.content).toContain(
      "export const Grpc$GoogleProtobufEmptySchema = Schema.Struct({});",
    );
    expect(response.file[0]?.content).toContain(
      "payloadSchema: Grpc$GoogleProtobufEmptySchema",
    );
    expect(response.file[0]?.content).toContain(
      "toGrpcRequest: toGrpc$GoogleProtobufEmpty",
    );
  });

  it("supports BoolValue as a method input or output", () => {
    const response = plugin.run(
      fixtureRequest([], {
        dependency: [
          "google/protobuf/duration.proto",
          "google/protobuf/wrappers.proto",
        ],
        extraFiles: [durationFile(), wrappersFile()],
        methodInputType: ".google.protobuf.BoolValue",
        methodOutputType: ".google.protobuf.Duration",
      }),
    );

    expect(response.file[0]?.content).toContain(
      "export const Grpc$GoogleProtobufDurationSchema = Schema.Duration;",
    );
    expect(response.file[0]?.content).toContain(
      "export const Grpc$GoogleProtobufBoolValueSchema = Schema.Boolean;",
    );
    expect(response.file[0]?.content).toContain(
      "payloadSchema: Grpc$GoogleProtobufBoolValueSchema",
    );
    expect(response.file[0]?.content).toContain(
      "successSchema: Grpc$GoogleProtobufDurationSchema",
    );
    expect(response.file[0]?.content).toContain(
      "toGrpcRequest: toGrpc$GoogleProtobufBoolValueMessage",
    );
    expect(response.file[0]?.content).toContain(
      "fromGrpcResponse: fromGrpc$GoogleProtobufDuration",
    );
  });

  it("supports non-string map keys", () => {
    const response = plugin.run(
      fixtureRequest(
        [
          field("labels", 1, FieldDescriptorProto_Type.MESSAGE, {
            label: FieldDescriptorProto_Label.REPEATED,
            typeName: ".demo.v1.GetUserRequest.LabelsEntry",
          }),
        ],
        {
          requestNestedTypes: [
            create(DescriptorProtoSchema, {
              name: "LabelsEntry",
              field: [
                field("key", 1, FieldDescriptorProto_Type.INT32),
                field("value", 2, FieldDescriptorProto_Type.STRING),
              ],
              options: create(MessageOptionsSchema, { mapEntry: true }),
            }),
          ],
        },
      ),
    );

    expect(response.file[0]?.content).toContain(
      "labels: Schema.Record(Schema.Number, Schema.String)",
    );
    expect(response.file[0]?.content).toContain(
      "map(([key, value]) => [Number(key), (value) as string])",
    );
  });

  it("supports enum map value fields", () => {
    const response = plugin.run(
      fixtureRequest(
        [
          field("states", 1, FieldDescriptorProto_Type.MESSAGE, {
            label: FieldDescriptorProto_Label.REPEATED,
            typeName: ".demo.v1.GetUserRequest.StatesEntry",
          }),
        ],
        {
          enumType: [
            create(EnumDescriptorProtoSchema, {
              name: "Kind",
              value: [
                create(EnumValueDescriptorProtoSchema, {
                  name: "KIND_UNSPECIFIED",
                  number: 0,
                }),
              ],
            }),
          ],
          requestNestedTypes: [
            create(DescriptorProtoSchema, {
              name: "StatesEntry",
              field: [
                field("key", 1, FieldDescriptorProto_Type.STRING),
                field("value", 2, FieldDescriptorProto_Type.ENUM, {
                  typeName: ".demo.v1.Kind",
                }),
              ],
              options: create(MessageOptionsSchema, { mapEntry: true }),
            }),
          ],
        },
      ),
    );

    expect(response.file[0]?.content).toContain(
      "states: Schema.Record(Schema.String, KindSchema)",
    );
    expect(response.file[0]?.content).toContain("value as Kind");
  });

  it("supports enum oneof fields", () => {
    const response = plugin.run(
      fixtureRequest(
        [
          field("kind", 1, FieldDescriptorProto_Type.ENUM, {
            oneofIndex: 0,
            typeName: ".demo.v1.Kind",
          }),
        ],
        {
          enumType: [
            create(EnumDescriptorProtoSchema, {
              name: "Kind",
              value: [
                create(EnumValueDescriptorProtoSchema, {
                  name: "KIND_UNSPECIFIED",
                  number: 0,
                }),
              ],
            }),
          ],
          requestOneofs: [
            create(OneofDescriptorProtoSchema, { name: "lookup" }),
          ],
        },
      ),
    );

    expect(response.file[0]?.content).toContain(
      'Schema.Struct({ case: Schema.Literal("kind"), value: KindSchema })',
    );
  });

  it("supports Any fields", () => {
    const response = plugin.run(
      fixtureRequest(
        [
          field("payload", 1, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".google.protobuf.Any",
          }),
        ],
        {
          dependency: ["google/protobuf/any.proto"],
          extraFiles: [anyFile()],
        },
      ),
    );

    expect(response.file[0]?.content).toContain(
      "payload: Schema.optional(Schema.Struct({ typeUrl: Schema.String, value: Schema.String }))",
    );
    expect(response.file[0]?.content).toContain(
      "const fromGrpc$GoogleProtobufAny",
    );
  });

  it("fails fast for unsupported import cycles", () => {
    expect(() =>
      detectImportCycles([
        reflectedFile("demo/v1/user_service.proto", ["demo/v1/profile.proto"]),
        reflectedFile("demo/v1/profile.proto", ["demo/v1/user_service.proto"]),
      ]),
    ).toThrow(
      "import cycle detected: demo/v1/user_service.proto -> demo/v1/profile.proto -> demo/v1/user_service.proto",
    );
  });

  it("fails fast for unsupported proto2 required and default fields", () => {
    expectUnsupportedField(
      [
        field("id", 1, FieldDescriptorProto_Type.STRING, {
          label: FieldDescriptorProto_Label.REQUIRED,
        }),
      ],
      "proto2 required field demo.v1.GetUserRequest.id",
      { syntax: "proto2" },
    );

    expectUnsupportedField(
      [
        field("id", 1, FieldDescriptorProto_Type.STRING, {
          defaultValue: "demo",
        }),
      ],
      "proto2 default field demo.v1.GetUserRequest.id",
      { syntax: "proto2" },
    );
  });
});

describe("parseOptions", () => {
  it("defaults int64 to bigint and accepts the explicit option", () => {
    expect(parseOptions([]).int64).toBe("bigint");
    expect(parseOptions([{ key: "int64", value: "bigint" }]).int64).toBe(
      "bigint",
    );
    expect(() => parseOptions([{ key: "int64", value: "number" }])).toThrow(
      "Unsupported int64 option",
    );
  });

  it("defaults methods to all four kinds and validates values", () => {
    expect([...parseOptions([]).methods]).toEqual([
      "unary",
      "server-streaming",
      "client-streaming",
      "bidi-streaming",
    ]);
    expect([
      ...parseOptions([{ key: "methods", value: "unary,client-streaming" }])
        .methods,
    ]).toEqual(["unary", "client-streaming"]);
    expect(() =>
      parseOptions([{ key: "methods", value: "full-duplex" }]),
    ).toThrow("Unsupported methods option: full-duplex.");
  });
});

describe("streaming methods", () => {
  it("generates the direct bridge for client-streaming and bidi methods", () => {
    const response = plugin.run(
      fixtureRequest([], {
        extraMethods: [
          create(MethodDescriptorProtoSchema, {
            name: "UploadUsers",
            inputType: ".demo.v1.User",
            outputType: ".demo.v1.GetUserResponse",
            clientStreaming: true,
          }),
          create(MethodDescriptorProtoSchema, {
            name: "ChatUsers",
            inputType: ".demo.v1.User",
            outputType: ".demo.v1.User",
            clientStreaming: true,
            serverStreaming: true,
          }),
        ],
      }),
    );

    const content = response.file[0]?.content;
    expect(content).toContain(
      'invoker.clientStream("demo.v1.UserService/UploadUsers"',
    );
    expect(content).toContain(
      'invoker.bidiStream("demo.v1.UserService/ChatUsers"',
    );
    expect(content).not.toContain("effect/unstable/rpc");
    expect(content).toContain("GrpcServerProtocol.handlersLayer");
  });

  it("skips streaming methods when the methods option excludes them", () => {
    const response = plugin.run(
      fixtureRequest([], {
        parameter:
          "target=ts,import_extension=js,errors=grpc-status,methods=unary,server-streaming",
        extraMethods: [
          create(MethodDescriptorProtoSchema, {
            name: "UploadUsers",
            inputType: ".demo.v1.User",
            outputType: ".demo.v1.GetUserResponse",
            clientStreaming: true,
          }),
        ],
      }),
    );

    expect(response.file[0]?.content).not.toContain("UploadUsers");
  });
});

const fixtureRequest = (
  requestFields = [field("id", 1, FieldDescriptorProto_Type.STRING)],
  options?: {
    readonly dependency?: ReadonlyArray<string>;
    readonly enumType?: ReadonlyArray<EnumDescriptorProto>;
    readonly extraFiles?: ReadonlyArray<FileDescriptorProto>;
    readonly extraMethods?: ReadonlyArray<MethodDescriptorProto>;
    readonly parameter?: string;
    readonly requestNestedEnums?: ReadonlyArray<EnumDescriptorProto>;
    readonly requestNestedTypes?: ReadonlyArray<DescriptorProto>;
    readonly requestOneofs?: ReadonlyArray<OneofDescriptorProto>;
    readonly syntax?: "proto2" | "proto3";
    readonly methodInputType?: string;
    readonly methodOutputType?: string;
  },
) =>
  create(CodeGeneratorRequestSchema, {
    fileToGenerate: ["demo/v1/user_service.proto"],
    parameter:
      options?.parameter ?? "target=ts,import_extension=js,errors=grpc-status",
    protoFile: [
      ...(options?.extraFiles ?? []),
      create(FileDescriptorProtoSchema, {
        name: "demo/v1/user_service.proto",
        package: "demo.v1",
        syntax: options?.syntax ?? "proto3",
        dependency: [...(options?.dependency ?? [])],
        enumType: [...(options?.enumType ?? [])],
        messageType: [
          create(DescriptorProtoSchema, {
            name: "GetUserRequest",
            field: requestFields,
            enumType: [...(options?.requestNestedEnums ?? [])],
            nestedType: [...(options?.requestNestedTypes ?? [])],
            oneofDecl: [...(options?.requestOneofs ?? [])],
          }),
          create(DescriptorProtoSchema, {
            name: "GetUserResponse",
            field: [
              field("user", 1, FieldDescriptorProto_Type.MESSAGE, {
                typeName: ".demo.v1.User",
              }),
            ],
          }),
          create(DescriptorProtoSchema, {
            name: "User",
            field: [
              field("id", 1, FieldDescriptorProto_Type.STRING),
              field("name", 2, FieldDescriptorProto_Type.STRING),
            ],
          }),
        ],
        service: [
          create(ServiceDescriptorProtoSchema, {
            name: "UserService",
            method: [
              create(MethodDescriptorProtoSchema, {
                name: "GetUser",
                inputType:
                  options?.methodInputType ?? ".demo.v1.GetUserRequest",
                outputType:
                  options?.methodOutputType ?? ".demo.v1.GetUserResponse",
              }),
              ...(options?.extraMethods ?? []),
            ],
          }),
        ],
      }),
    ],
  });

const field = (
  name: string,
  number: number,
  type: FieldDescriptorProto_Type,
  options?: {
    readonly defaultValue?: string;
    readonly label?: FieldDescriptorProto_Label;
    readonly oneofIndex?: number;
    readonly typeName?: string;
  },
) =>
  create(FieldDescriptorProtoSchema, {
    name,
    number,
    defaultValue: options?.defaultValue,
    label: options?.label ?? FieldDescriptorProto_Label.OPTIONAL,
    oneofIndex: options?.oneofIndex,
    type,
    typeName: options?.typeName ?? "",
  });

const expectUnsupportedField = (
  requestFields: Parameters<typeof fixtureRequest>[0],
  message: string,
  options?: Parameters<typeof fixtureRequest>[1],
) => {
  expect(() => plugin.run(fixtureRequest(requestFields, options))).toThrow(
    message,
  );
};

const anyFile = () =>
  create(FileDescriptorProtoSchema, {
    name: "google/protobuf/any.proto",
    package: "google.protobuf",
    syntax: "proto3",
    messageType: [
      create(DescriptorProtoSchema, {
        name: "Any",
        field: [
          field("type_url", 1, FieldDescriptorProto_Type.STRING),
          field("value", 2, FieldDescriptorProto_Type.BYTES),
        ],
      }),
    ],
  });

const emptyFile = () =>
  create(FileDescriptorProtoSchema, {
    name: "google/protobuf/empty.proto",
    package: "google.protobuf",
    syntax: "proto3",
    messageType: [
      create(DescriptorProtoSchema, {
        name: "Empty",
      }),
    ],
  });

const durationFile = () =>
  create(FileDescriptorProtoSchema, {
    name: "google/protobuf/duration.proto",
    package: "google.protobuf",
    syntax: "proto3",
    messageType: [
      create(DescriptorProtoSchema, {
        name: "Duration",
        field: [
          field("seconds", 1, FieldDescriptorProto_Type.INT64),
          field("nanos", 2, FieldDescriptorProto_Type.INT32),
        ],
      }),
    ],
  });

const wrappersFile = () =>
  create(FileDescriptorProtoSchema, {
    name: "google/protobuf/wrappers.proto",
    package: "google.protobuf",
    syntax: "proto3",
    messageType: [
      create(DescriptorProtoSchema, {
        name: "BoolValue",
        field: [field("value", 1, FieldDescriptorProto_Type.BOOL)],
      }),
    ],
  });

const reflectedFile = (
  name: string,
  dependency: ReadonlyArray<string>,
): DescFile =>
  ({
    name: name.slice(0, -".proto".length),
    proto: { name, dependency },
  }) as DescFile;
