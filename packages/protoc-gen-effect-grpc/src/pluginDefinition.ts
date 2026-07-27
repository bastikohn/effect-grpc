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
  defaultOptions,
  parseOptions,
  type GeneratorOptions,
} from "./options.js";
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
  ImportModel,
  MapKeyModel,
  MessageModel,
  MethodModel,
  MethodTypeModel,
  OneofCaseModel,
  ServiceModel,
} from "./types.js";
import { wellKnownKind, wellKnownProtobufName } from "./wellKnown.js";

export const plugin = createEcmaScriptPlugin({
  name: "protoc-gen-effect-grpc",
  version: "0.1.0-alpha.0",
  parseOptions,
  generateTs(schema) {
    const options = { ...defaultOptions, ...schema.options };
    detectImportCycles(schema.allFiles);
    for (const file of schema.files) {
      const model = modelFromFile(file, options);
      if (
        model.enums.length === 0 &&
        model.messages.length === 0 &&
        model.services.length === 0
      ) {
        continue;
      }
      const generated = schema.generateFile(
        effectFileName(`${file.name}.proto`),
      );
      for (const line of generateFile(model).split("\n")) {
        generated.print(line);
      }
    }
  },
});

export const detectImportCycles = (files: ReadonlyArray<DescFile>) => {
  const byName = new Map(files.map((file) => [protoFileName(file), file]));
  const visiting: Array<string> = [];
  const visited = new Set<string>();

  const visit = (file: DescFile) => {
    const name = protoFileName(file);
    const active = visiting.indexOf(name);
    if (active >= 0) {
      throw new Error(
        `Unsupported protobuf imports: import cycle detected: ${[
          ...visiting.slice(active),
          name,
        ].join(" -> ")}`,
      );
    }
    if (visited.has(name)) return;

    visiting.push(name);
    for (const dependency of file.proto.dependency) {
      const dependencyFile = byName.get(dependency);
      if (dependencyFile) visit(dependencyFile);
    }
    visiting.pop();
    visited.add(name);
  };

  for (const file of files) {
    visit(file);
  }
};

const protoFileName = (file: DescFile) =>
  file.proto.name || `${file.name}.proto`;

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

const modelFromFile = (
  file: DescFile,
  options: GeneratorOptions,
): GeneratorFile => ({
  protoFileName: `${file.name}.proto`,
  packageName: file.proto.package,
  importExtension: options.importExtension,
  imports: importsFromFile(file),
  enums: allEnums(file).map(enumModel),
  messages: allMessages(file).map(messageModel),
  services: file.services
    .map((service) => serviceModel(service, options))
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
      return fieldScalarModel(field);
    case "message":
      return { ...messageValueModel(field), optional: true };
    case "enum":
      return fieldEnumModel(field);
    case "list":
      return {
        kind: "list",
        name: field.localName,
        item: listValueModel(field),
      };
    case "map":
      return {
        kind: "map",
        name: field.localName,
        key: mapKeyModel(field),
        value: mapValueModel(field),
      };
  }
};

const fieldScalarModel = (
  field: Extract<DescField, { readonly fieldKind: "scalar" }>,
): FieldModel => ({
  ...scalarValueModel(field),
  optional: hasExplicitPresence(field) ? true : undefined,
});

const fieldEnumModel = (
  field: Extract<DescField, { readonly fieldKind: "enum" }>,
): FieldModel => ({
  ...enumValueModel(field),
  optional: hasExplicitPresence(field) ? true : undefined,
});

const scalarValueModel = (
  field: Extract<DescField, { readonly fieldKind: "scalar" }>,
): FieldValueModel => ({
  kind: "scalar",
  name: field.localName,
  type: scalarKind(field.scalar),
  unsigned: isUnsignedScalar(field.scalar),
});

const enumValueModel = (
  field: Extract<DescField, { readonly fieldKind: "enum" }>,
): FieldValueModel => ({
  kind: "enum",
  name: field.localName,
  enumName: declName(field.enum),
});

const messageValueModel = (
  field: Extract<DescField, { readonly fieldKind: "message" }>,
): FieldValueModel => {
  const kind = wellKnownKind(field.message.typeName);
  if (kind) {
    return {
      kind: "well-known",
      name: field.localName,
      type: kind,
    };
  }
  return {
    kind: "message",
    name: field.localName,
    messageName: declName(field.message),
    source:
      field.message.file.name === field.parent.file.name ? "local" : "imported",
  };
};

const listValueModel = (
  field: Extract<DescField, { readonly fieldKind: "list" }>,
): FieldValueModel => {
  switch (field.listKind) {
    case "scalar":
      return {
        kind: "scalar",
        name: field.localName,
        type: scalarKind(field.scalar),
        unsigned: isUnsignedScalar(field.scalar),
      };
    case "enum":
      return {
        kind: "enum",
        name: field.localName,
        enumName: declName(field.enum),
      };
    case "message": {
      const kind = wellKnownKind(field.message.typeName);
      if (kind) {
        return {
          kind: "well-known",
          name: field.localName,
          type: kind,
        };
      }
      return {
        kind: "message",
        name: field.localName,
        messageName: declName(field.message),
        source:
          field.message.file.name === field.parent.file.name
            ? "local"
            : "imported",
      };
    }
  }
};

const mapValueModel = (
  field: Extract<DescField, { readonly fieldKind: "map" }>,
): FieldValueModel => {
  switch (field.mapKind) {
    case "scalar":
      return {
        kind: "scalar",
        name: field.localName,
        type: scalarKind(field.scalar),
        unsigned: isUnsignedScalar(field.scalar),
      };
    case "enum":
      return {
        kind: "enum",
        name: field.localName,
        enumName: declName(field.enum),
      };
    case "message": {
      const kind = wellKnownKind(field.message.typeName);
      if (kind) {
        return {
          kind: "well-known",
          name: field.localName,
          type: kind,
        };
      }
      return {
        kind: "message",
        name: field.localName,
        messageName: declName(field.message),
        source:
          field.message.file.name === field.parent.file.name
            ? "local"
            : "imported",
      };
    }
  }
};

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
  switch (field.fieldKind) {
    case "scalar":
      return {
        name: field.localName,
        value: scalarValueModel(field),
      };
    case "message":
      return {
        name: field.localName,
        value: messageValueModel(field),
      };
    case "enum":
      return {
        name: field.localName,
        value: enumValueModel(field),
      };
    case "list":
    case "map":
      throw new Error(
        `Unsupported protobuf oneof field kind: ${field.fieldKind}`,
      );
  }
};

const importsFromFile = (file: DescFile): ReadonlyArray<ImportModel> => {
  const imports = new Map<
    string,
    { readonly messages: Set<string>; readonly enums: Set<string> }
  >();
  const record = (desc: DescMessage | DescEnum) => {
    if (desc.file.name === file.name || isWellKnownType(desc)) return;
    const protoFile = `${desc.file.name}.proto`;
    const entry = imports.get(protoFile) ?? {
      messages: new Set<string>(),
      enums: new Set<string>(),
    };
    if (desc.kind === "message") entry.messages.add(declName(desc));
    else entry.enums.add(declName(desc));
    imports.set(protoFile, entry);
  };

  for (const message of allMessages(file)) {
    for (const field of message.fields) {
      supportedField(field);
      for (const desc of referencedDescs(field)) record(desc);
    }
  }
  for (const service of file.services) {
    for (const method of service.methods) {
      record(method.input);
      record(method.output);
    }
  }
  return [...imports.entries()].map(([protoFileName, entry]) => ({
    protoFileName,
    messages: [...entry.messages].sort(),
    enums: [...entry.enums].sort(),
  }));
};

const referencedDescs = (
  field: DescField,
): ReadonlyArray<DescMessage | DescEnum> => {
  switch (field.fieldKind) {
    case "message":
      return [field.message];
    case "enum":
      return [field.enum];
    case "list":
      return field.listKind === "message"
        ? [field.message]
        : field.listKind === "enum"
          ? [field.enum]
          : [];
    case "map":
      return field.mapKind === "message"
        ? [field.message]
        : field.mapKind === "enum"
          ? [field.enum]
          : [];
    case "scalar":
      return [];
  }
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
    return {
      name: grpcWellKnownName(wellKnownProtobufName(kind)),
      wellKnown: kind,
    };
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
  return { name: declName(message) };
};

const serviceModel = (
  service: DescService,
  options: GeneratorOptions,
): ServiceModel => ({
  name: service.name,
  typeName: service.typeName,
  methods: service.methods.flatMap((method): ReadonlyArray<MethodModel> => {
    const kind = methodKindModel(method.methodKind);
    if (!options.methods.has(kind)) return [];
    return [
      {
        name: method.name,
        localName: method.localName,
        kind,
        inputType: methodTypeModel(service, method, method.input),
        outputType: methodTypeModel(service, method, method.output),
      },
    ];
  }),
});
