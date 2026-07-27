import {
  ScalarType,
  type DescEnum,
  type DescField,
  type DescFile,
  type DescMessage,
  type DescMethod,
  type DescOneof,
  type DescService,
} from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";
import { createEcmaScriptPlugin } from "@bufbuild/protoplugin";

import { generateFile } from "./generate.js";
import { effectFileName, grpcEmptyName, grpcWellKnownName } from "./naming.js";
import {
  isWellKnownType,
  methodKindModel,
  scalarKind,
  supportedField,
} from "./unsupported.js";
import type {
  EnumModel,
  FieldValueModel,
  FieldModel,
  GeneratorFile,
  MapKeyModel,
  MessageModel,
  MethodModel,
  MethodTypeModel,
  OneofCaseModel,
  ServiceModel,
} from "./types.js";
import { wellKnownKind } from "./wellKnown.js";

export const plugin = createEcmaScriptPlugin({
  name: "protoc-gen-effect-grpc",
  version: "0.1.0-alpha.0",
  generateTs(schema) {
    for (const file of schema.files) {
      const model = modelFromFile(file);
      const generated = schema.generateFile(
        effectFileName(`${file.name}.proto`),
      );
      generated.preamble(file);
      // A proto file with nothing to generate emits no output: protoplugin
      // drops files whose printed content is empty (a preamble alone does not
      // count), so no explicit guard is needed here.
      for (const entry of generateFile(model)) {
        generated.print(entry);
      }
    }
  },
});

// Generated declaration name matching protoc-gen-es: nested types use the
// parent chain joined with underscores (e.g. `Outer_Inner`).
const declName = (desc: DescMessage | DescEnum): string => {
  const packageName = desc.file.proto.package;
  const local =
    packageName && desc.typeName.startsWith(`${packageName}.`)
      ? desc.typeName.slice(packageName.length + 1)
      : desc.typeName;
  return local.replaceAll(".", "_");
};

const allMessages = (file: DescFile): ReadonlyArray<DescMessage> => {
  const messages: Array<DescMessage> = [];
  const visit = (message: DescMessage) => {
    messages.push(message);
    for (const nested of message.nestedMessages) visit(nested);
  };
  for (const message of file.messages) visit(message);
  return messages;
};

const allEnums = (file: DescFile): ReadonlyArray<DescEnum> => [
  ...file.enums,
  ...allMessages(file).flatMap((message) => message.nestedEnums),
];

const modelFromFile = (file: DescFile): GeneratorFile => ({
  protoFileName: `${file.name}.proto`,
  enums: allEnums(file).map(enumModel),
  messages: allMessages(file).map(messageModel),
  services: file.services
    .map(serviceModel)
    .filter((service) => service.methods.length > 0),
});

const enumModel = (desc: DescEnum): EnumModel => ({
  name: declName(desc),
});

const messageModel = (message: DescMessage): MessageModel => ({
  name: declName(message),
  fields: message.members.map((member) =>
    member.kind === "oneof" ? oneofModel(message, member) : fieldModel(member),
  ),
});

const hasExplicitPresence = (field: DescField) =>
  !field.oneof && field.presence !== FeatureSet_FieldPresence.IMPLICIT;

const fieldModel = (field: DescField): FieldModel => {
  supportedField(field);
  switch (field.fieldKind) {
    case "scalar":
    case "enum":
      return {
        ...valueModel(field, field.fieldKind),
        optional: hasExplicitPresence(field) ? true : undefined,
      };
    case "message":
      return { ...valueModel(field, "message"), optional: true };
    case "list":
      return {
        kind: "list",
        name: field.localName,
        item: valueModel(field, field.listKind),
      };
    case "map":
      return {
        kind: "map",
        name: field.localName,
        key: mapKeyModel(field),
        value: valueModel(field, field.mapKind),
      };
  }
};

/**
 * The value-carrying members of `DescField`. `.scalar`/`.enum`/`.message` sit
 * on the same runtime object whether the field is singular, a list item or a
 * map value, but each lives on a different arm of the `DescField` union — so
 * the dispatch takes the arm as a separate argument and reads through here.
 */
type ValueField = {
  readonly localName: string;
  readonly parent: DescField["parent"];
  readonly scalar: ScalarType;
  readonly enum: DescEnum;
  readonly message: DescMessage;
};

const valueModel = (
  field: DescField,
  kind: "scalar" | "enum" | "message",
): FieldValueModel => {
  const value = field as unknown as ValueField;
  const name = value.localName;
  switch (kind) {
    case "scalar":
      return {
        kind: "scalar",
        name,
        type: scalarKind(value.scalar),
        unsigned: isUnsignedScalar(value.scalar),
      };
    case "enum":
      return {
        kind: "enum",
        name,
        enumName: declName(value.enum),
        importedFrom: importedFromFile(value.enum, value.parent.file),
      };
    case "message": {
      const wellKnown = wellKnownKind(value.message.typeName);
      return wellKnown
        ? { kind: "well-known", name, type: wellKnown }
        : {
            kind: "message",
            name,
            messageName: declName(value.message),
            importedFrom: importedFromFile(value.message, value.parent.file),
          };
    }
  }
};

/** The declaring proto file's name, or `undefined` when declared locally. */
const importedFromFile = (
  desc: DescMessage | DescEnum,
  file: DescFile,
): string | undefined =>
  desc.file.name === file.name ? undefined : `${desc.file.name}.proto`;

const mapKeyModel = (
  field: Extract<DescField, { readonly fieldKind: "map" }>,
): MapKeyModel => {
  switch (field.mapKey) {
    case ScalarType.INT32:
    case ScalarType.UINT32:
    case ScalarType.SINT32:
    case ScalarType.FIXED32:
    case ScalarType.SFIXED32:
      return { kind: "map-key", type: "number" };
    default:
      return { kind: "map-key", type: "string" };
  }
};

const oneofModel = (message: DescMessage, oneof: DescOneof): FieldModel => ({
  kind: "oneof",
  name: oneof.localName,
  converterName: `${declName(message)}_${oneof.localName}`,
  cases: oneof.fields.map(oneofCaseModel),
});

const oneofCaseModel = (field: DescField): OneofCaseModel => {
  supportedField(field);
  if (field.fieldKind === "list" || field.fieldKind === "map") {
    throw new Error(
      `Unsupported protobuf oneof field kind: ${field.fieldKind}`,
    );
  }
  return { name: field.localName, value: valueModel(field, field.fieldKind) };
};

const isUnsignedScalar = (scalar: DescField["scalar"]) =>
  scalar === ScalarType.UINT64 ||
  scalar === ScalarType.FIXED64 ||
  scalar === ScalarType.UINT32;

// Well-known identity comes from the descriptor's `typeName`, never from the
// generated name: `GrpcGoogleProtobufTimestamp` is a name any `.proto` may
// declare, and only `google.protobuf.Timestamp` is the well-known.
const methodTypeModel = (
  service: DescService,
  method: DescMethod,
  message: DescMessage,
): MethodTypeModel => {
  switch (message.typeName) {
    case "google.protobuf.Empty":
      return { name: grpcEmptyName, wellKnown: "empty" };
  }
  const kind = wellKnownKind(message.typeName);
  if (kind) {
    return { name: grpcWellKnownName(kind), wellKnown: kind };
  }
  if (isWellKnownType(message)) {
    throw new Error(
      [
        "Unsupported gRPC method message:",
        `  ${service.typeName}/${method.name} uses well-known type ${message.typeName}.`,
        "Well-known protobuf types are not supported as method input or output.",
      ].join("\n"),
    );
  }
  return {
    name: declName(message),
    importedFrom: importedFromFile(message, service.file),
  };
};

const serviceModel = (service: DescService): ServiceModel => ({
  name: service.name,
  typeName: service.typeName,
  methods: service.methods.map(
    (method): MethodModel => ({
      name: method.name,
      localName: method.localName,
      kind: methodKindModel(method.methodKind),
      inputType: methodTypeModel(service, method, method.input),
      outputType: methodTypeModel(service, method, method.output),
    }),
  ),
});
