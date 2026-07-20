import type {
  EnumModel,
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  ScalarKind,
  WellKnownKind,
} from "./types.js";
import type { FileUsage } from "./fileUsage.js";
import { wellKnownKinds } from "./fileUsage.js";
import { grpcEmptyName, grpcWellKnownName } from "./naming.js";
import { wellKnownProtobufName } from "./wellKnown.js";

export const generateSchemas = (file: GeneratorFile, usage: FileUsage) => [
  ...(usage.usesGrpcEmpty
    ? [
        `export const ${grpcEmptyName}Schema = Schema.Struct({});`,
        `export type ${grpcEmptyName} = Schema.Schema.Type<typeof ${grpcEmptyName}Schema>;`,
        "",
      ]
    : []),
  ...wellKnownMethodSchemas(usage),
  ...file.enums.flatMap(enumSchema),
  ...usage.orderedMessages.flatMap((message) => [
    `export const ${message.name}Schema = Schema.Struct({`,
    ...message.fields.map(
      (field) =>
        `  ${field.name}: ${fieldSchema(field, message.name, usage.recursiveEdges)},`,
    ),
    "});",
    `export type ${message.name} = Schema.Schema.Type<typeof ${message.name}Schema>;`,
    "",
  ]),
];

const wellKnownMethodSchemas = (usage: FileUsage) =>
  wellKnownKinds
    .filter((type) => usage.wellKnownMethods.has(type))
    .flatMap((type) => {
      const name = grpcWellKnownName(wellKnownProtobufName(type));
      return [
        `export const ${name}Schema = ${wellKnownSchema(type)};`,
        `export type ${name} = Schema.Schema.Type<typeof ${name}Schema>;`,
        "",
      ];
    });

const enumSchema = (field: EnumModel) => [
  `export const ${field.name}Schema = Schema.Number;`,
  `export type ${field.name} = number;`,
  "",
];

const fieldSchema = (
  field: FieldModel,
  messageName: string,
  recursiveEdges: ReadonlySet<string>,
): string => {
  switch (field.kind) {
    case "scalar": {
      const schema = scalarSchema(field.type, field.unsigned);
      return field.optional ? `Schema.optional(${schema})` : schema;
    }
    case "message":
      return field.optional
        ? `Schema.optional(${messageSchema(field, messageName, recursiveEdges)})`
        : messageSchema(field, messageName, recursiveEdges);
    case "enum":
      return field.optional
        ? `Schema.optional(${field.enumName}Schema)`
        : `${field.enumName}Schema`;
    case "well-known":
      return field.optional
        ? `Schema.optional(${wellKnownSchema(field.type)})`
        : wellKnownSchema(field.type);
    case "list":
      return `Schema.Array(${valueSchema(field.item, messageName, recursiveEdges)})`;
    case "map":
      return `Schema.Record(${mapKeySchema(field.key.type)}, ${valueSchema(field.value, messageName, recursiveEdges)})`;
    case "oneof":
      return `Schema.Union([${[
        ...field.cases.map(
          (oneofCase) =>
            `Schema.Struct({ case: Schema.Literal("${oneofCase.name}"), value: ${valueSchema(oneofCase.value, messageName, recursiveEdges)} })`,
        ),
        "Schema.Struct({ case: Schema.Undefined, value: Schema.optional(Schema.Undefined) })",
      ].join(", ")}])`;
  }
};

const valueSchema = (
  field: FieldValueModel,
  messageName: string,
  recursiveEdges: ReadonlySet<string>,
): string => {
  switch (field.kind) {
    case "scalar":
      return scalarSchema(field.type, field.unsigned);
    case "message":
      return messageSchema(field, messageName, recursiveEdges);
    case "enum":
      return `${field.enumName}Schema`;
    case "well-known":
      return wellKnownSchema(field.type);
  }
};

const messageSchema = (
  field: Extract<FieldValueModel, { readonly kind: "message" }>,
  currentMessageName: string,
  recursiveEdges: ReadonlySet<string>,
) =>
  field.source === "local"
    ? recursiveEdges.has(`${currentMessageName}->${field.messageName}`)
      ? `Schema.suspend((): Schema.Codec<unknown, unknown, never, never> => ${field.messageName}Schema)`
      : `Schema.suspend((): typeof ${field.messageName}Schema => ${field.messageName}Schema)`
    : `${field.messageName}Schema`;

const mapKeySchema = (
  type: Extract<FieldModel, { readonly kind: "map" }>["key"]["type"],
) => {
  switch (type) {
    case "number":
      return "Schema.Number";
    case "string":
      return "Schema.String";
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
    case "double-value":
    case "float-value":
    case "int32-value":
      return scalarSchema("number");
    case "uint32-value":
      return scalarSchema("number", true);
    case "int64-value":
      return scalarSchema("bigint");
    case "uint64-value":
      return scalarSchema("bigint", true);
    case "bool-value":
      return "Schema.Boolean";
    case "string-value":
      return "Schema.String";
    case "bytes-value":
      return "Schema.Uint8Array";
    case "any":
      return "Schema.Struct({ typeUrl: Schema.String, value: Schema.String })";
    case "struct":
    case "value":
    case "list-value":
      return "Schema.Unknown";
    case "field-mask":
      return "Schema.String";
  }
};
