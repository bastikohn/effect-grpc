import type { Printable } from "@bufbuild/protoplugin";

import type {
  EnumFieldModel,
  EnumModel,
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  MessageFieldModel,
  ScalarKind,
} from "./types.js";
import type { FileUsage } from "./fileUsage.js";
import { grpcEmptyName, grpcWellKnownName } from "./naming.js";
import { exportDecl, joinPrintables } from "./printing.js";
import * as sym from "./symbols.js";
import { wellKnownKinds, type WellKnownKind } from "./wellKnown.js";

/** The effect `Schema` namespace import followed by raw text. */
const S = (rest: string): Printable => [sym.Schema, rest];

export const generateSchemas = (
  file: GeneratorFile,
  usage: FileUsage,
): ReadonlyArray<Printable> => [
  ...(usage.usesGrpcEmpty
    ? [
        [
          exportDecl("const", `${grpcEmptyName}Schema`),
          " = ",
          S(".Struct({});"),
        ],
        [
          exportDecl("type", grpcEmptyName),
          " = ",
          S(`.Schema.Type<typeof ${grpcEmptyName}Schema>;`),
        ],
        "",
      ]
    : []),
  ...wellKnownMethodSchemas(usage),
  ...file.enums.flatMap(enumSchema),
  ...file.messages.flatMap(
    (message): ReadonlyArray<Printable> => [
      [exportDecl("const", `${message.name}Schema`), " = ", S(".Struct({")],
      ...message.fields.map(
        (field): Printable => [
          `  ${field.name}: `,
          fieldSchema(field, message.name, usage.recursiveEdges),
          ",",
        ],
      ),
      "});",
      [
        exportDecl("type", message.name),
        " = ",
        S(`.Schema.Type<typeof ${message.name}Schema>;`),
      ],
      "",
    ],
  ),
];

const wellKnownMethodSchemas = (usage: FileUsage): ReadonlyArray<Printable> =>
  wellKnownKinds
    .filter((type) => usage.wellKnownMethods.has(type))
    .flatMap((type): ReadonlyArray<Printable> => {
      const name = grpcWellKnownName(type);
      return [
        [
          exportDecl("const", `${name}Schema`),
          " = ",
          wellKnownSchema(type),
          ";",
        ],
        [
          exportDecl("type", name),
          " = ",
          S(`.Schema.Type<typeof ${name}Schema>;`),
        ],
        "",
      ];
    });

const enumSchema = (field: EnumModel): ReadonlyArray<Printable> => [
  [exportDecl("const", `${field.name}Schema`), " = ", S(".Number;")],
  [exportDecl("type", field.name), " = number;"],
  "",
];

const fieldSchema = (
  field: FieldModel,
  messageName: string,
  recursiveEdges: ReadonlySet<string>,
): Printable => {
  switch (field.kind) {
    case "scalar": {
      const schema = scalarSchema(field.type, field.unsigned);
      return field.optional ? optional(schema) : schema;
    }
    case "message": {
      const schema = messageSchema(field, messageName, recursiveEdges);
      return field.optional ? optional(schema) : schema;
    }
    case "enum":
      return field.optional
        ? optional(enumSchemaRef(field))
        : enumSchemaRef(field);
    case "well-known":
      return field.optional
        ? optional(wellKnownSchema(field.type))
        : wellKnownSchema(field.type);
    case "list":
      return [
        S(".Array("),
        valueSchema(field.item, messageName, recursiveEdges),
        ")",
      ];
    case "map":
      return [
        S(".Record("),
        mapKeySchema(field.key.type),
        ", ",
        valueSchema(field.value, messageName, recursiveEdges),
        ")",
      ];
    case "oneof":
      return [
        S(".Union(["),
        joinPrintables(
          [
            ...field.cases.map(
              (oneofCase): Printable => [
                S(".Struct({ case: "),
                S(`.Literal("${oneofCase.name}"), value: `),
                valueSchema(oneofCase.value, messageName, recursiveEdges),
                " })",
              ],
            ),
            [
              S(".Struct({ case: "),
              S(".Undefined, value: "),
              S(".optional("),
              S(".Undefined) })"),
            ],
          ],
          ", ",
        ),
        "])",
      ];
  }
};

const optional = (schema: Printable): Printable => [
  S(".optional("),
  schema,
  ")",
];

const valueSchema = (
  field: FieldValueModel,
  messageName: string,
  recursiveEdges: ReadonlySet<string>,
): Printable => {
  switch (field.kind) {
    case "scalar":
      return scalarSchema(field.type, field.unsigned);
    case "message":
      return messageSchema(field, messageName, recursiveEdges);
    case "enum":
      return enumSchemaRef(field);
    case "well-known":
      return wellKnownSchema(field.type);
  }
};

const enumSchemaRef = (field: EnumFieldModel): Printable =>
  field.importedFrom === undefined
    ? `${field.enumName}Schema`
    : sym.effectValue(field.importedFrom, `${field.enumName}Schema`);

const messageSchema = (
  field: MessageFieldModel,
  currentMessageName: string,
  recursiveEdges: ReadonlySet<string>,
): Printable =>
  field.importedFrom === undefined
    ? recursiveEdges.has(`${currentMessageName}->${field.messageName}`)
      ? [
          S(".suspend((): "),
          S(
            `.Codec<unknown, unknown, never, never> => ${field.messageName}Schema)`,
          ),
        ]
      : S(
          `.suspend((): typeof ${field.messageName}Schema => ${field.messageName}Schema)`,
        )
    : sym.effectValue(field.importedFrom, `${field.messageName}Schema`);

const mapKeySchema = (
  type: Extract<FieldModel, { readonly kind: "map" }>["key"]["type"],
): Printable => {
  switch (type) {
    case "number":
      return S(".Number");
    case "string":
      return S(".String");
  }
};

const scalarSchema = (type: ScalarKind, unsigned?: boolean): Printable => {
  switch (type) {
    case "string":
      return S(".String");
    case "number":
      return S(".Number");
    case "boolean":
      return S(".Boolean");
    case "bytes":
      return S(".Uint8Array");
    case "bigint":
      return unsigned
        ? [S(".BigInt.check("), S(".isGreaterThanOrEqualToBigInt(0n))")]
        : S(".BigInt");
    default:
      return S(".Unknown");
  }
};

const wellKnownSchema = (type: WellKnownKind): Printable => {
  switch (type) {
    case "Timestamp":
      return S(".Date");
    case "Duration":
      return S(".Duration");
    case "DoubleValue":
    case "FloatValue":
    case "Int32Value":
      return scalarSchema("number");
    case "UInt32Value":
      return scalarSchema("number", true);
    case "Int64Value":
      return scalarSchema("bigint");
    case "UInt64Value":
      return scalarSchema("bigint", true);
    case "BoolValue":
      return S(".Boolean");
    case "StringValue":
      return S(".String");
    case "BytesValue":
      return S(".Uint8Array");
    case "Any":
      return [S(".Struct({ typeUrl: "), S(".String, value: "), S(".String })")];
    case "Struct":
    case "Value":
    case "ListValue":
      return S(".Unknown");
    case "FieldMask":
      return S(".String");
  }
};
