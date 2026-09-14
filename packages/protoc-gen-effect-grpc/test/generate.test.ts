import { create } from "@bufbuild/protobuf";
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
  file_google_protobuf_any,
  file_google_protobuf_duration,
  file_google_protobuf_empty,
  file_google_protobuf_wrappers,
  FileDescriptorProtoSchema,
  MessageOptionsSchema,
  type MethodDescriptorProto,
  MethodDescriptorProtoSchema,
  type OneofDescriptorProto,
  OneofDescriptorProtoSchema,
  ServiceDescriptorProtoSchema,
} from "@bufbuild/protobuf/wkt";
import { assert, describe, expect, it } from "@effect/vitest";

import { plugin } from "../src/pluginDefinition.js";

describe("plugin fixture", () => {
  // The snapshot pins the whole emission for all four method kinds; the
  // assertions name the two decisions a reader cannot infer from it — the
  // annotated `Schema.suspend` recursive edge, and that the server path
  // routes through `handlersEffect` rather than Effect RPC.
  it("generates schemas, registry, client, and server glue", () => {
    const response = plugin.run(
      fixtureRequest(undefined, {
        extraMethods: [
          create(MethodDescriptorProtoSchema, {
            name: "WatchUsers",
            inputType: ".demo.v1.GetUserRequest",
            outputType: ".demo.v1.User",
            serverStreaming: true,
          }),
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
    assert.include(
      content,
      "user: Schema.optional(Schema.suspend((): typeof UserSchema => UserSchema))",
    );
    assert.include(content, "GrpcServerProtocol.handlersEffect<R>({");
    assert.notInclude(content, "Rpc.make(");
    assert.notInclude(content, "RpcGroup");
    assert.notInclude(content, "effect/unstable/rpc");
    expect(content).toMatchSnapshot();
  });

  it("omits readField and compact when every message is empty", () => {
    const response = plugin.run(fixtureRequest([], { emptyMessages: true }));

    const content = response.file[0]?.content;
    assert.notInclude(content, "readField");
    assert.notInclude(content, "compact");
    assert.include(
      content,
      "export const fromGetUserRequest = (_message: unknown): unknown => ({});",
    );
    assert.include(
      content,
      "export const toGetUserRequest = (_value: unknown): Record<string, unknown> => ({});",
    );
  });

  it("emits the annotated Schema.suspend for a recursive self-edge", () => {
    const response = plugin.run(
      fixtureRequest([
        field("next", 1, FieldDescriptorProto_Type.MESSAGE, {
          typeName: ".demo.v1.GetUserRequest",
        }),
      ]),
    );

    // A cyclic edge must break the type recursion with an explicit annotation
    // rather than the self-referential `typeof GetUserRequestSchema` form.
    const content = response.file[0]?.content;
    assert.include(
      content,
      "next: Schema.optional(Schema.suspend((): Schema.Codec<unknown, unknown, never, never> => GetUserRequestSchema))",
    );
    assert.notInclude(
      content,
      "Schema.suspend((): typeof GetUserRequestSchema",
    );
  });

  it("aliases colliding same-name imports from another package", () => {
    // demo.v1.User (declared locally) and other.v1.User (imported) both
    // generate the identifier `User`. Before imports went through
    // protoplugin, the spliced import statement redeclared the local names
    // and the generated file failed to compile (TS2300); protoplugin's
    // collision aliasing renames the foreign symbols instead.
    const response = plugin.run(
      fixtureRequest(
        [
          field("friend", 1, FieldDescriptorProto_Type.MESSAGE, {
            typeName: ".other.v1.User",
          }),
        ],
        {
          dependency: ["demo/v1/other.proto"],
          extraFiles: [
            create(FileDescriptorProtoSchema, {
              name: "demo/v1/other.proto",
              package: "other.v1",
              syntax: "proto3",
              messageType: [
                create(DescriptorProtoSchema, {
                  name: "User",
                  field: [field("id", 1, FieldDescriptorProto_Type.STRING)],
                }),
              ],
            }),
          ],
          methodOutputType: ".other.v1.User",
        },
      ),
    );

    const content = response.file[0]?.content;
    // Local declarations keep their names...
    assert.include(content, "export const UserSchema = Schema.Struct({");
    assert.include(content, "export const fromUser = ");
    // ...and every foreign same-name import is aliased.
    assert.include(content, "UserSchema as UserSchema$1");
    assert.include(content, "import type { User as User$1 }");
    assert.include(content, "friend: Schema.optional(UserSchema$1)");
    assert.include(content, "successSchema: UserSchema$1,");
    assert.include(content, "fromGrpcResponse: fromUser$1,");
    assert.include(content, "Effect.Effect<User$1, UserServiceClientError>");
  });

  it("generates a real plugin response from descriptor input", () => {
    const response = plugin.run(fixtureRequest());

    assert.lengthOf(response.file, 1);
    assert.strictEqual(
      response.file[0]?.name,
      "demo/v1/user_service_effect_grpc.ts",
    );
    assert.include(response.file[0]?.content, "UserServiceGrpcRegistry");
    // protoplugin's standard preamble, above the import block.
    assert.match(
      response.file[0]?.content,
      /^\/\/ @generated by protoc-gen-effect-grpc /,
    );
    assert.include(
      response.file[0]?.content,
      "// @generated from file demo/v1/user_service.proto (package demo.v1, syntax proto3)",
    );
  });

  it("emits no file for a proto without messages, enums, or services", () => {
    // No explicit guard in the plugin: nothing is printed for an empty model,
    // and protoplugin drops generated files whose content is empty (the
    // preamble alone does not count).
    const response = plugin.run(
      create(CodeGeneratorRequestSchema, {
        fileToGenerate: ["demo/v1/empty.proto"],
        parameter: "target=ts,import_extension=js",
        protoFile: [
          create(FileDescriptorProtoSchema, {
            name: "demo/v1/empty.proto",
            package: "demo.v1",
            syntax: "proto3",
          }),
        ],
      }),
    );

    assert.lengthOf(response.file, 0);
  });

  it("defaults to extensionless imports and honours import_extension", () => {
    // Without import_extension the plugin follows protoplugin's parsed
    // default ("none"): relative imports carry no extension. Node ESM
    // consumers should pass import_extension=js, as every template does.
    const extensionless = plugin.run(
      fixtureRequest(undefined, { parameter: "target=ts" }),
    );
    assert.include(extensionless.file[0]?.content, 'from "./user_service_pb";');
    assert.notInclude(extensionless.file[0]?.content, "_pb.js");

    const withJs = plugin.run(fixtureRequest());
    assert.include(withJs.file[0]?.content, 'from "./user_service_pb.js";');
  });

  it("supports google.protobuf.Empty as a method input or output", () => {
    const response = plugin.run(
      fixtureRequest([], {
        dependency: ["google/protobuf/empty.proto"],
        extraFiles: [file_google_protobuf_empty.proto],
        methodInputType: ".google.protobuf.Empty",
        methodOutputType: ".google.protobuf.Empty",
      }),
    );

    assert.include(
      response.file[0]?.content,
      "export const Grpc$GoogleProtobufEmptySchema = Schema.Struct({});",
    );
    assert.include(
      response.file[0]?.content,
      "payloadSchema: Grpc$GoogleProtobufEmptySchema",
    );
    assert.include(
      response.file[0]?.content,
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
        extraFiles: [
          file_google_protobuf_duration.proto,
          file_google_protobuf_wrappers.proto,
        ],
        methodInputType: ".google.protobuf.BoolValue",
        methodOutputType: ".google.protobuf.Duration",
      }),
    );

    assert.include(
      response.file[0]?.content,
      "export const Grpc$GoogleProtobufDurationSchema = Schema.Duration;",
    );
    assert.include(
      response.file[0]?.content,
      "export const Grpc$GoogleProtobufBoolValueSchema = Schema.Boolean;",
    );
    assert.include(
      response.file[0]?.content,
      "payloadSchema: Grpc$GoogleProtobufBoolValueSchema",
    );
    assert.include(
      response.file[0]?.content,
      "successSchema: Grpc$GoogleProtobufDurationSchema",
    );
    assert.include(
      response.file[0]?.content,
      "toGrpcRequest: toGrpc$GoogleProtobufBoolValueMessage",
    );
    assert.include(
      response.file[0]?.content,
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

    assert.include(
      response.file[0]?.content,
      "labels: Schema.Record(Schema.Number, Schema.String)",
    );
    assert.include(
      response.file[0]?.content,
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

    assert.include(
      response.file[0]?.content,
      "states: Schema.Record(Schema.String, KindSchema)",
    );
    assert.include(response.file[0]?.content, "value as Kind");
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

    assert.include(
      response.file[0]?.content,
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
          extraFiles: [file_google_protobuf_any.proto],
        },
      ),
    );

    assert.include(
      response.file[0]?.content,
      "payload: Schema.optional(Schema.Struct({ typeUrl: Schema.String, value: Schema.String }))",
    );
    assert.include(
      response.file[0]?.content,
      "const fromGrpc$GoogleProtobufAny",
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
    assert.include(
      content,
      'invoker.clientStream("demo.v1.UserService/UploadUsers"',
    );
    assert.include(
      content,
      'invoker.bidiStream("demo.v1.UserService/ChatUsers"',
    );
    assert.notInclude(content, "effect/unstable/rpc");
    assert.include(content, "GrpcServerProtocol.handlersEffect");
    // Generated code no longer touches Effect RPC on either side: the client
    // depends on the GrpcInvoker seam, the server on handlersEffect.
    assert.include(content, "const invoker = yield* GrpcInvoker.GrpcInvoker;");
    assert.notInclude(content, "RpcClient");
    assert.notInclude(content, "GrpcClientProtocol");
  });
});

const fixtureRequest = (
  requestFields = [field("id", 1, FieldDescriptorProto_Type.STRING)],
  options?: {
    readonly dependency?: ReadonlyArray<string>;
    readonly enumType?: ReadonlyArray<EnumDescriptorProto>;
    readonly extraFiles?: ReadonlyArray<FileDescriptorProto>;
    readonly extraMethods?: ReadonlyArray<MethodDescriptorProto>;
    readonly requestNestedEnums?: ReadonlyArray<EnumDescriptorProto>;
    readonly requestNestedTypes?: ReadonlyArray<DescriptorProto>;
    readonly requestOneofs?: ReadonlyArray<OneofDescriptorProto>;
    readonly syntax?: "proto2" | "proto3";
    readonly methodInputType?: string;
    readonly methodOutputType?: string;
    readonly parameter?: string;
    /** Strip every message's fields (`readsFields === false`). */
    readonly emptyMessages?: boolean;
  },
) =>
  create(CodeGeneratorRequestSchema, {
    fileToGenerate: ["demo/v1/user_service.proto"],
    parameter: options?.parameter ?? "target=ts,import_extension=js",
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
            field: options?.emptyMessages
              ? []
              : [
                  field("user", 1, FieldDescriptorProto_Type.MESSAGE, {
                    typeName: ".demo.v1.User",
                  }),
                ],
          }),
          create(DescriptorProtoSchema, {
            name: "User",
            field: options?.emptyMessages
              ? []
              : [
                  field("id", 1, FieldDescriptorProto_Type.STRING),
                  field("name", 2, FieldDescriptorProto_Type.STRING),
                  // A singular numeric scalar keeps `Schema.Number` and the
                  // `as number` converter cast pinned in the buf-free tier.
                  field("count", 3, FieldDescriptorProto_Type.INT32),
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
  assert.throws(
    () => plugin.run(fixtureRequest(requestFields, options)),
    message,
  );
};
