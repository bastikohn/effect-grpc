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
import { effectFileName } from "./naming.js";
import {
  defaultOptions,
  parseOptions,
  type GeneratorOptions,
} from "./options.js";
import {
  isWellKnownType,
  scalarKind,
  supportedField,
  supportedMethodKind,
} from "./unsupported.js";
import type {
  EnumModel,
  FieldModel,
  GeneratorFile,
  ImportModel,
  MessageModel,
  MethodModel,
  OneofCaseModel,
  ServiceModel,
} from "./types.js";

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
    member.kind === "oneof" ? oneofModel(member) : fieldModel(member),
  ),
});

const hasExplicitPresence = (field: DescField) =>
  !field.oneof && field.presence !== FeatureSet_FieldPresence.IMPLICIT;

const fieldModel = (field: DescField): FieldModel => {
  supportedField(field);
  switch (field.fieldKind) {
    case "scalar":
      return {
        kind: "scalar",
        name: field.localName,
        type: scalarKind(field.scalar),
        unsigned: isUnsignedScalar(field.scalar),
        optional: hasExplicitPresence(field) ? true : undefined,
      };
    case "message":
      if (field.message.typeName === "google.protobuf.Timestamp") {
        return {
          kind: "well-known",
          name: field.localName,
          type: "timestamp",
          optional: true,
        };
      }
      if (field.message.typeName === "google.protobuf.Duration") {
        return {
          kind: "well-known",
          name: field.localName,
          type: "duration",
          optional: true,
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
        optional: true,
      };
    case "enum":
      return {
        kind: "enum",
        name: field.localName,
        enumName: declName(field.enum),
        optional: hasExplicitPresence(field) ? true : undefined,
      };
    case "list":
      if (field.listKind === "message") {
        return {
          kind: "list",
          name: field.localName,
          item: {
            kind: "message",
            name: field.localName,
            messageName: declName(field.message),
            source:
              field.message.file.name === field.parent.file.name
                ? "local"
                : "imported",
          },
        };
      }
      if (field.listKind === "enum") {
        return {
          kind: "list",
          name: field.localName,
          item: {
            kind: "enum",
            name: field.localName,
            enumName: declName(field.enum),
          },
        };
      }
      return {
        kind: "list",
        name: field.localName,
        item: {
          kind: "scalar",
          name: field.localName,
          type: scalarKind(field.scalar),
          unsigned: isUnsignedScalar(field.scalar),
        },
      };
    case "map":
      if (field.mapKind === "message") {
        return {
          kind: "map",
          name: field.localName,
          key: {
            kind: "scalar",
            name: "key",
            type: "string",
          },
          value: {
            kind: "message",
            name: field.localName,
            messageName: declName(field.message),
            source:
              field.message.file.name === field.parent.file.name
                ? "local"
                : "imported",
          },
        };
      }
      if (field.mapKind !== "scalar") {
        throw new Error(
          `Unsupported protobuf field kind: map:${field.mapKind}`,
        );
      }
      return {
        kind: "map",
        name: field.localName,
        key: {
          kind: "scalar",
          name: "key",
          type: "string",
        },
        value: {
          kind: "scalar",
          name: field.localName,
          type: scalarKind(field.scalar),
          unsigned: isUnsignedScalar(field.scalar),
        },
      };
  }
};

const oneofModel = (oneof: DescOneof): FieldModel => ({
  kind: "oneof",
  name: oneof.localName,
  cases: oneof.fields.map(oneofCaseModel),
});

const oneofCaseModel = (field: DescField): OneofCaseModel => {
  supportedField(field);
  switch (field.fieldKind) {
    case "scalar":
      return {
        name: field.localName,
        value: {
          kind: "scalar",
          name: field.localName,
          type: scalarKind(field.scalar),
          unsigned: isUnsignedScalar(field.scalar),
        },
      };
    case "message":
      if (
        field.message.typeName === "google.protobuf.Duration" ||
        field.message.typeName === "google.protobuf.Timestamp"
      ) {
        throw new Error("Unsupported protobuf oneof field kind: well-known");
      }
      return {
        name: field.localName,
        value: {
          kind: "message",
          name: field.localName,
          messageName: declName(field.message),
          source:
            field.message.file.name === field.parent.file.name
              ? "local"
              : "imported",
        },
      };
    case "enum":
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

const supportedMethodMessage = (
  service: DescService,
  method: DescMethod,
  message: DescMessage,
) => {
  if (isWellKnownType(message)) {
    throw new Error(
      [
        "Unsupported gRPC method message:",
        `  ${service.typeName}/${method.name} uses well-known type ${message.typeName}.`,
        "Well-known protobuf types are not supported as method input or output.",
      ].join("\n"),
    );
  }
};

const serviceModel = (
  service: DescService,
  options: GeneratorOptions,
): ServiceModel => ({
  name: service.name,
  typeName: service.typeName,
  methods: service.methods.flatMap((method): ReadonlyArray<MethodModel> => {
    const kind = supportedMethodKind({
      serviceTypeName: service.typeName,
      methodName: method.name,
      methodKind: method.methodKind,
      ignoreUnsupportedMethods: options.ignoreUnsupportedMethods,
    });
    if (!kind || !options.methods.has(kind)) return [];
    supportedMethodMessage(service, method, method.input);
    supportedMethodMessage(service, method, method.output);
    return [
      {
        name: method.name,
        localName: method.localName,
        kind,
        inputType: declName(method.input),
        outputType: declName(method.output),
      },
    ];
  }),
});
