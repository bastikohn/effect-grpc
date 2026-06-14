import type {
  EnumModel,
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  MessageModel,
  ScalarKind,
  WellKnownKind,
} from "./types.js";
import {
  grpcBoolValueName,
  grpcDurationName,
  grpcEmptyName,
  grpcTimestampName,
} from "./naming.js";

export const generateSchemas = (file: GeneratorFile) => {
  const ordered = orderMessages(file.messages);
  return [
    ...(usesGrpcEmpty(file)
      ? [
          `export const ${grpcEmptyName}Schema = Schema.Struct({});`,
          `export type ${grpcEmptyName} = Schema.Schema.Type<typeof ${grpcEmptyName}Schema>;`,
          "",
        ]
      : []),
    ...wellKnownMethodSchemas(file),
    ...file.enums.flatMap(enumSchema),
    ...ordered.flatMap((message) => [
      `export const ${message.name}Schema = Schema.Struct({`,
      ...message.fields.map(
        (field) => `  ${field.name}: ${fieldSchema(field)},`,
      ),
      "});",
      `export type ${message.name} = Schema.Schema.Type<typeof ${message.name}Schema>;`,
      "",
    ]),
  ];
};

const usesGrpcEmpty = (file: GeneratorFile) =>
  file.services.some((service) =>
    service.methods.some(
      (method) =>
        method.inputType === grpcEmptyName ||
        method.outputType === grpcEmptyName,
    ),
  );

const wellKnownMethodSchemas = (file: GeneratorFile) =>
  [
    ["timestamp", grpcTimestampName] as const,
    ["duration", grpcDurationName] as const,
    ["bool-value", grpcBoolValueName] as const,
  ].flatMap(([type, name]) =>
    usesMethodType(file, name)
      ? [
          `export const ${name}Schema = ${wellKnownSchema(type)};`,
          `export type ${name} = Schema.Schema.Type<typeof ${name}Schema>;`,
          "",
        ]
      : [],
  );

const usesMethodType = (file: GeneratorFile, typeName: string) =>
  file.services.some((service) =>
    service.methods.some(
      (method) =>
        method.inputType === typeName || method.outputType === typeName,
    ),
  );

const enumSchema = (field: EnumModel) => [
  `export const ${field.name}Schema = Schema.Number;`,
  `export type ${field.name} = number;`,
  "",
];

const orderMessages = (messages: ReadonlyArray<MessageModel>) => {
  const byName = new Map(messages.map((message) => [message.name, message]));
  const visited = new Set<string>();
  const ordered: Array<MessageModel> = [];

  const visit = (message: MessageModel) => {
    if (visited.has(message.name)) return;
    visited.add(message.name);
    for (const dependency of messageDependencies(message)) {
      const dependencyMessage = byName.get(dependency);
      if (dependencyMessage) visit(dependencyMessage);
    }
    ordered.push(message);
  };

  for (const message of messages) {
    visit(message);
  }
  return ordered;
};

const messageDependencies = (message: MessageModel) =>
  message.fields.flatMap((field) => {
    if (field.kind === "message" && field.source === "local") {
      return [field.messageName];
    }
    if (
      field.kind === "list" &&
      field.item.kind === "message" &&
      field.item.source === "local"
    ) {
      return [field.item.messageName];
    }
    if (
      field.kind === "map" &&
      field.value.kind === "message" &&
      field.value.source === "local"
    ) {
      return [field.value.messageName];
    }
    if (field.kind === "oneof") {
      return field.cases.flatMap((oneofCase) =>
        oneofCase.value.kind === "message" && oneofCase.value.source === "local"
          ? [oneofCase.value.messageName]
          : [],
      );
    }
    return [];
  });

const fieldSchema = (field: FieldModel): string => {
  switch (field.kind) {
    case "scalar": {
      const schema = scalarSchema(field.type, field.unsigned);
      return field.optional ? `Schema.optional(${schema})` : schema;
    }
    case "message":
      return field.optional
        ? `Schema.optional(${field.messageName}Schema)`
        : `${field.messageName}Schema`;
    case "enum":
      return field.optional
        ? `Schema.optional(${field.enumName}Schema)`
        : `${field.enumName}Schema`;
    case "well-known":
      return field.optional
        ? `Schema.optional(${wellKnownSchema(field.type)})`
        : wellKnownSchema(field.type);
    case "list":
      return `Schema.Array(${valueSchema(field.item)})`;
    case "map":
      return `Schema.Record(Schema.String, ${valueSchema(field.value)})`;
    case "oneof":
      return `Schema.Union([${[
        ...field.cases.map(
          (oneofCase) =>
            `Schema.Struct({ case: Schema.Literal("${oneofCase.name}"), value: ${valueSchema(oneofCase.value)} })`,
        ),
        "Schema.Struct({ case: Schema.Undefined, value: Schema.optional(Schema.Undefined) })",
      ].join(", ")}])`;
  }
};

const valueSchema = (field: FieldValueModel): string => {
  switch (field.kind) {
    case "scalar":
      return scalarSchema(field.type, field.unsigned);
    case "message":
      return `${field.messageName}Schema`;
    case "enum":
      return `${field.enumName}Schema`;
    case "well-known":
      return wellKnownSchema(field.type);
  }
};

const scalarSchema = (type: ScalarKind, unsigned?: boolean) => {
  switch (type) {
    case "string":
      return "Schema.String";
    case "number":
      return "Schema.Number";
    case "boolean":
      return "Schema.Boolean";
    case "bytes":
      return "Schema.Uint8Array";
    case "bigint":
      return unsigned
        ? "Schema.BigInt.check(Schema.isGreaterThanOrEqualToBigInt(0n))"
        : "Schema.BigInt";
    default:
      return "Schema.Unknown";
  }
};

const wellKnownSchema = (type: WellKnownKind) => {
  switch (type) {
    case "timestamp":
      return "Schema.Date";
    case "duration":
      return "Schema.Duration";
    case "bool-value":
      return "Schema.Boolean";
  }
};
