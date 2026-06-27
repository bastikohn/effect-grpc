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
import { supportedMethodKind } from "../src/unsupported.js";

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
          inputType: "GetUserRequest",
          outputType: "GetUserResponse",
        },
        {
          name: "WatchUsers",
          localName: "watchUsers",
          kind: "server-streaming",
          inputType: "WatchUsersRequest",
          outputType: "UserEvent",
        },
      ],
    },
  ],
};

describe("generateFile", () => {
  it("generates schemas, RPCs, registry, client, and server glue", () => {
    const output = generateFile(demoFile);

    expect(output).toContain("export const UserSchema = Schema.Struct");
    expect(output).toContain(
      "user: Schema.optional(Schema.suspend((): typeof UserSchema => UserSchema))",
    );
    expect(output).toContain(
      'user: readField(message, "user") == null ? undefined : fromUser(readField(message, "user"))',
    );
    expect(output).toContain("export type UserServiceClientError");
    expect(output).toContain('Rpc.make("demo.v1.UserService/GetUser"');
    expect(output).toContain("stream: true");
    expect(output).toContain("UserServiceGrpcRegistry");
    expect(output).toContain("export interface UserServiceClientService");
    expect(output).toContain("export interface UserServiceImplementation");
    expect(output).toContain("UserServiceHandlersLayer");
    expect(output).toMatchSnapshot();
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
      "export const GrpcGoogleProtobufEmptySchema = Schema.Struct({});",
    );
    expect(response.file[0]?.content).toContain(
      "payload: GrpcGoogleProtobufEmptySchema",
    );
    expect(response.file[0]?.content).toContain(
      "toGrpcRequest: toGrpcGoogleProtobufEmpty",
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
      "export const GrpcGoogleProtobufDurationSchema = Schema.Duration;",
    );
    expect(response.file[0]?.content).toContain(
      "export const GrpcGoogleProtobufBoolValueSchema = Schema.Boolean;",
    );
    expect(response.file[0]?.content).toContain(
      "payload: GrpcGoogleProtobufBoolValueSchema",
    );
    expect(response.file[0]?.content).toContain(
      "success: GrpcGoogleProtobufDurationSchema",
    );
    expect(response.file[0]?.content).toContain(
      "toGrpcRequest: toGrpcGoogleProtobufBoolValue",
    );
    expect(response.file[0]?.content).toContain(
      "fromGrpcResponse: fromGrpcGoogleProtobufDuration",
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
      "const fromGrpcGoogleProtobufAny",
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
});

describe("supportedMethodKind", () => {
  it("fails on client-streaming by default", () => {
    expect(() =>
      supportedMethodKind({
        serviceTypeName: "demo.v1.UserService",
        methodName: "Upload",
        methodKind: "client_streaming",
        ignoreUnsupportedMethods: false,
      }),
    ).toThrow(
      "demo.v1.UserService/Upload is client-streaming.\nThe first prototype supports only unary and server-streaming.",
    );
  });

  it("fails on bidirectional-streaming by default", () => {
    expect(() =>
      supportedMethodKind({
        serviceTypeName: "demo.v1.UserService",
        methodName: "Chat",
        methodKind: "bidi_streaming",
        ignoreUnsupportedMethods: false,
      }),
    ).toThrow("demo.v1.UserService/Chat is bidirectional-streaming.");
  });
});

const fixtureRequest = (
  requestFields = [field("id", 1, FieldDescriptorProto_Type.STRING)],
  options?: {
    readonly dependency?: ReadonlyArray<string>;
    readonly enumType?: ReadonlyArray<EnumDescriptorProto>;
    readonly extraFiles?: ReadonlyArray<FileDescriptorProto>;
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
      "target=ts,import_extension=js,errors=grpc-status,methods=unary,server-streaming",
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
