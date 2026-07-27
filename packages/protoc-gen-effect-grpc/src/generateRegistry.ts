import type { Printable } from "@bufbuild/protoplugin";

import type { FileUsage } from "./fileUsage.js";
import { isWrapperWellKnownKind } from "./fileUsage.js";
import {
  grpcEmptyName,
  grpcGeneratedName,
  grpcWellKnownName,
  serviceRegistryName,
} from "./naming.js";
import { exportDecl } from "./printing.js";
import * as sym from "./symbols.js";
import type {
  EnumFieldModel,
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  MessageFieldModel,
  MethodTypeModel,
  ScalarKind,
} from "./types.js";
import {
  wellKnownJsonSchemaName,
  wrapperWellKnownKinds,
  type WellKnownKind,
} from "./wellKnown.js";

export const generateRegistry = (
  file: GeneratorFile,
  usage: FileUsage,
): ReadonlyArray<Printable> => [
  ...(usage.usesGrpcEmpty
    ? [
        [
          exportDecl("const", `from${grpcEmptyName}`),
          ` = (_message: unknown): ${grpcEmptyName} => ({});`,
        ],
        "",
        [
          exportDecl("const", `to${grpcEmptyName}`),
          " = (_value: unknown) => ({});",
        ],
        "",
      ]
    : []),
  ...generateConverters(file, usage),
  ...file.services.flatMap(
    (service): ReadonlyArray<Printable> => [
      [
        exportDecl("const", serviceRegistryName(service.name)),
        " = new Map<string, ",
        sym.GrpcMethodRegistry,
        ".GrpcMethodEntry>([",
      ],
      ...service.methods.flatMap(
        (method): ReadonlyArray<Printable> => [
          "  [",
          `    "${service.typeName}/${method.name}",`,
          "    {",
          `      kind: "${method.kind}",`,
          `      tag: "${service.typeName}/${method.name}",`,
          [
            "      service: ",
            sym.pbValue(file.protoFileName, service.name),
            ",",
          ],
          `      localName: "${method.localName}",`,
          [
            "      payloadSchema: ",
            methodValueRef(method.inputType, `${method.inputType.name}Schema`),
            ",",
          ],
          [
            "      successSchema: ",
            methodValueRef(
              method.outputType,
              `${method.outputType.name}Schema`,
            ),
            ",",
          ],
          ["      toGrpcRequest: ", toRegistryConverter(method.inputType), ","],
          [
            "      fromGrpcRequest: ",
            methodValueRef(method.inputType, `from${method.inputType.name}`),
            ",",
          ],
          [
            "      toGrpcResponse: ",
            toRegistryConverter(method.outputType),
            ",",
          ],
          [
            "      fromGrpcResponse: ",
            methodValueRef(method.outputType, `from${method.outputType.name}`),
            ",",
          ],
          "    },",
          "  ],",
        ],
      ),
      "]);",
      "",
    ],
  ),
];

/** A name declared beside the method's type: local text or an import. */
const methodValueRef = (type: MethodTypeModel, name: string): Printable =>
  type.importedFrom === undefined
    ? name
    : sym.effectValue(type.importedFrom, name);

/** A message field's converter: local text or an import from its source. */
const messageConverter = (
  direction: "from" | "to",
  field: MessageFieldModel,
): Printable =>
  field.importedFrom === undefined
    ? `${direction}${field.messageName}`
    : sym.effectValue(field.importedFrom, `${direction}${field.messageName}`);

/** An enum's generated type in a cast position: local text or a type import. */
const enumTypeRef = (field: EnumFieldModel): Printable =>
  field.importedFrom === undefined
    ? field.enumName
    : sym.effectType(field.importedFrom, field.enumName);

/** `CodegenSupport.readField(message, "<field>")`. */
const readField = (fieldName: string): Printable => [
  sym.CodegenSupport,
  `.readField(message, "${fieldName}")`,
];

const generateConverters = (
  file: GeneratorFile,
  usage: FileUsage,
): ReadonlyArray<Printable> => {
  const messages = file.messages;
  return [
    ...(!usage.readsFields
      ? []
      : [
          // Effect Schema treats an absent optional field as a missing *key*, not
          // a present `undefined` value: decoding `{ field: undefined }` against an
          // `optional` field fails. Strip undefined-valued keys so converted
          // messages decode (and round-trip) cleanly.
          "const compact = <T extends Record<string, unknown>>(object: T): T => {",
          "  const result: Record<string, unknown> = {};",
          "  for (const key of Object.keys(object)) {",
          "    if (object[key] !== undefined) result[key] = object[key];",
          "  }",
          "  return result as T;",
          "};",
          "",
        ]),
    ...scalarConverters(usage),
    ...wellKnownConverters(usage),
    ...messages.flatMap(
      (message): ReadonlyArray<Printable> =>
        message.fields.length === 0
          ? [
              [
                exportDecl("const", `from${message.name}`),
                " = (_message: unknown): unknown => ({});",
              ],
              "",
              [
                exportDecl("const", `to${message.name}`),
                " = (_value: unknown): Record<string, unknown> => ({});",
              ],
              "",
            ]
          : [
              ...message.fields.flatMap((field) =>
                field.kind === "oneof" ? oneofConverters(field) : [],
              ),
              [
                exportDecl("const", `from${message.name}`),
                " = (message: unknown): unknown => compact({",
              ],
              ...message.fields.map(
                (field): Printable => [
                  `  ${field.name}: `,
                  fromField(field),
                  ",",
                ],
              ),
              "});",
              "",
              [
                exportDecl("const", `to${message.name}`),
                " = (value: unknown): Record<string, unknown> => {",
              ],
              "  const message = value as Record<string, unknown>;",
              "  return compact({",
              ...message.fields.map(
                (field): Printable => [
                  `    ${field.name}: `,
                  toField(field),
                  ",",
                ],
              ),
              "  });",
              "};",
              "",
            ],
    ),
  ];
};

const fromField = (field: FieldModel): Printable => {
  if (field.kind === "message") {
    const value = readField(field.name);
    return [
      value,
      " == null ? undefined : ",
      messageConverter("from", field),
      "(",
      value,
      ")",
    ];
  }
  if (field.kind === "enum") {
    return [
      readField(field.name),
      " as ",
      enumTypeRef(field),
      field.optional ? " | undefined" : "",
    ];
  }
  if (field.kind === "well-known") {
    const value = readField(field.name);
    return [
      value,
      ` == null ? undefined : from${wellKnownConverterName(field.type)}(`,
      value,
      ")",
    ];
  }
  if (field.kind === "scalar") {
    const value = readField(field.name);
    return field.optional
      ? [value, " == null ? undefined : ", fromValue(value, field)]
      : fromValue(value, field);
  }
  if (field.kind === "list") {
    return [
      "((",
      readField(field.name),
      " as ReadonlyArray<unknown> | undefined) ?? []).map((value) => ",
      fromValue("value", field.item),
      ")",
    ];
  }
  if (field.kind === "map") {
    return [
      "Object.fromEntries(Object.entries((",
      readField(field.name),
      " as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [",
      fromMapKey("key", field.key.type),
      ", ",
      fromValue("value", field.value),
      "]))",
    ];
  }
  return [`from${oneofConverterName(field)}(`, readField(field.name), ")"];
};

const toField = (field: FieldModel): Printable => {
  const value = readField(field.name);
  if (field.kind === "message") {
    return [
      value,
      " == null ? undefined : ",
      messageConverter("to", field),
      "(",
      value,
      ")",
    ];
  }
  if (field.kind === "scalar") {
    return field.optional
      ? [value, " == null ? undefined : ", toValue(value, field)]
      : toValue(value, field);
  }
  if (field.kind === "enum") {
    return [value, " as number", field.optional ? " | undefined" : ""];
  }
  if (field.kind === "well-known") {
    return [
      value,
      " == null ? undefined : ",
      toWellKnownValue(value, field, "bare"),
    ];
  }
  if (field.kind === "list") {
    return [
      "((",
      value,
      " as ReadonlyArray<unknown> | undefined) ?? []).map((value) => ",
      toValue("value", field.item, "boxed"),
      ")",
    ];
  }
  if (field.kind === "map") {
    return [
      "Object.fromEntries(Object.entries((",
      value,
      " as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [",
      toMapKey("key", field.key.type),
      ", ",
      toValue("value", field.value, "boxed"),
      "]))",
    ];
  }
  if (field.kind === "oneof") {
    return [`to${oneofConverterName(field)}(`, value, ")"];
  }
  return value;
};

const scalarTsType = (type: ScalarKind) => {
  switch (type) {
    case "string":
      return "string";
    case "number":
      return "number";
    case "boolean":
      return "boolean";
    case "bytes":
      return "Uint8Array";
    case "bigint":
      return "bigint";
  }
};

const fromMapKey = (value: string, type: "number" | "string") => {
  switch (type) {
    case "number":
      return `Number(${value})`;
    case "string":
      return value;
  }
};

const toMapKey = fromMapKey;

const fromValue = (value: Printable, field: FieldValueModel): Printable => {
  switch (field.kind) {
    case "scalar":
      return fromScalarValue(value, field);
    case "message":
      return [messageConverter("from", field), "(", value, ")"];
    case "enum":
      return [value, " as ", enumTypeRef(field)];
    case "well-known":
      return [`from${wellKnownConverterName(field.type)}(`, value, ")"];
  }
};

const toValue = (
  value: Printable,
  field: FieldValueModel,
  wrapperEncoding: "bare" | "boxed" = "bare",
): Printable => {
  switch (field.kind) {
    case "scalar":
      return toScalarValue(value, field);
    case "enum":
      return [value, " as number"];
    case "message":
      return [messageConverter("to", field), "(", value, ")"];
    case "well-known":
      return toWellKnownValue(value, field, wrapperEncoding);
  }
};

const toWellKnownValue = (
  value: Printable,
  field: Extract<FieldValueModel, { readonly kind: "well-known" }>,
  wrapperEncoding: "bare" | "boxed",
): Printable =>
  wrapperEncoding === "boxed" && wrapperWellKnownKinds.has(field.type)
    ? [`to${wellKnownConverterName(field.type)}Message(`, value, ")"]
    : [`to${wellKnownConverterName(field.type)}(`, value, ")"];

const fromScalarValue = (
  value: Printable,
  field: Extract<FieldValueModel, { readonly kind: "scalar" }>,
): Printable => {
  switch (field.type) {
    case "bytes":
      return [`from${bytesConverterName}((`, value, ") as Uint8Array)"];
    case "bigint":
      return ["String(", value, ")"];
    default:
      return ["(", value, `) as ${scalarTsType(field.type)}`];
  }
};

const toScalarValue = (
  value: Printable,
  field: Extract<FieldValueModel, { readonly kind: "scalar" }>,
): Printable => {
  switch (field.type) {
    case "bytes":
      return [`to${bytesConverterName}(`, value, ")"];
    case "bigint":
      return ["BigInt((", value, ") as string)"];
    default:
      return ["(", value, `) as ${scalarTsType(field.type)}`];
  }
};

const oneofConverters = (
  field: Extract<FieldModel, { readonly kind: "oneof" }>,
): ReadonlyArray<Printable> => [
  `const from${oneofConverterName(field)} = (value: unknown): unknown => {`,
  `  const oneof = (value ?? { case: undefined }) as { readonly case?: string; readonly value?: unknown };`,
  // The unset case arrives as `undefined` from protobuf-es but as `null` from
  // the JSON codec; coalesce so both select the `undefined` branch.
  "  switch (oneof.case ?? undefined) {",
  ...field.cases.flatMap(
    (oneofCase): ReadonlyArray<Printable> => [
      `    case "${oneofCase.name}":`,
      [
        `      return { case: "${oneofCase.name}", value: `,
        fromValue("oneof.value", oneofCase.value),
        " };",
      ],
    ],
  ),
  "    case undefined:",
  // The JSON codec represents the unset `Schema.Undefined` case as `null`, so
  // emit `null` here for the value to decode (protobuf-es uses `undefined`).
  "      return { case: null };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${oneof.case}\`);`,
  "  }",
  "};",
  "",
  `const to${oneofConverterName(field)} = (value: unknown): unknown => {`,
  `  const oneof = value ?? { case: undefined };`,
  `  const message = oneof as { readonly case?: string; readonly value?: unknown };`,
  // See `from*Oneof`: the JSON codec encodes the unset case as `null`.
  "  switch (message.case ?? undefined) {",
  ...field.cases.flatMap(
    (oneofCase): ReadonlyArray<Printable> => [
      `    case "${oneofCase.name}":`,
      [
        `      return { case: "${oneofCase.name}", value: `,
        toValue("message.value", oneofCase.value, "boxed"),
        " };",
      ],
    ],
  ),
  "    case undefined:",
  "      return { case: undefined };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${message.case}\`);`,
  "  }",
  "};",
  "",
];

// The base64 helpers (and the `node:buffer` import they need) are required
// whenever base64 bytes conversion is emitted: a bytes scalar field, or a
// BytesValue/Any well-known used as a field OR as a method input/output.
const scalarConverters = (usage: FileUsage): ReadonlyArray<Printable> =>
  usage.usesBase64Bytes
    ? [
        `const from${bytesConverterName} = (value: Uint8Array): string =>`,
        ["  ", sym.Buffer, `.from(value).toString("base64");`],
        "",
        `const to${bytesConverterName} = (value: unknown): Uint8Array =>`,
        [
          "  Uint8Array.from(",
          sym.Buffer,
          `.from(value as string, "base64"));`,
        ],
        "",
      ]
    : [];

const wellKnownConverters = (usage: FileUsage): ReadonlyArray<Printable> => {
  return [
    ...(usage.wellKnownUsed.has("Timestamp")
      ? [
          [
            wellKnownConverterDecl(
              usage,
              "Timestamp",
              `from${wellKnownConverterName("Timestamp")}`,
            ),
            " = (value: unknown): string => {",
          ],
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const seconds = Number(message.seconds ?? 0);",
          "  const nanos = message.nanos ?? 0;",
          "  return new Date(seconds * 1000 + Math.trunc(nanos / 1_000_000)).toISOString();",
          "};",
          "",
          [
            wellKnownConverterDecl(
              usage,
              "Timestamp",
              `to${wellKnownConverterName("Timestamp")}`,
            ),
            " = (value: unknown) => {",
          ],
          "  const millis = new Date(value as string).getTime();",
          "  const seconds = Math.floor(millis / 1000);",
          "  return {",
          "    seconds: BigInt(seconds),",
          "    nanos: Math.trunc((millis - seconds * 1000) * 1_000_000),",
          "  };",
          "};",
          "",
        ]
      : []),
    ...(usage.wellKnownUsed.has("Duration")
      ? [
          [
            wellKnownConverterDecl(
              usage,
              "Duration",
              `from${wellKnownConverterName("Duration")}`,
            ),
            " = (value: unknown) => {",
          ],
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const nanos = BigInt(message.seconds ?? 0) * 1_000_000_000n + BigInt(message.nanos ?? 0);",
          `  return nanos % 1_000_000n === 0n ? { _tag: "Millis", value: Number(nanos / 1_000_000n) } : { _tag: "Nanos", value: String(nanos) };`,
          "};",
          "",
          [
            wellKnownConverterDecl(
              usage,
              "Duration",
              `to${wellKnownConverterName("Duration")}`,
            ),
            " = (value: unknown) => {",
          ],
          "  const duration = value as { readonly _tag?: string; readonly value?: unknown };",
          '  const nanos = duration._tag === "Millis"',
          "    ? BigInt(duration.value as number) * 1_000_000n",
          '    : duration._tag === "Nanos"',
          "      ? BigInt(duration.value as string)",
          `      : (() => { throw new Error(\`Unsupported Duration encoding: \${duration._tag}\`); })();`,
          "  return {",
          "    seconds: nanos / 1_000_000_000n,",
          "    nanos: Number(nanos % 1_000_000_000n),",
          "  };",
          "};",
          "",
        ]
      : []),
    ...wrapperConverter(usage, "DoubleValue", "number", false, "0"),
    ...wrapperConverter(usage, "FloatValue", "number", false, "0"),
    ...wrapperConverter(usage, "Int32Value", "number", false, "0"),
    ...wrapperConverter(usage, "UInt32Value", "number", true, "0"),
    ...wrapperConverter(usage, "Int64Value", "bigint", false, "0n"),
    ...wrapperConverter(usage, "UInt64Value", "bigint", true, "0n"),
    ...wrapperConverter(usage, "BoolValue", "boolean", false, "false"),
    ...wrapperConverter(usage, "StringValue", "string", false, '""'),
    ...wrapperConverter(
      usage,
      "BytesValue",
      "bytes",
      false,
      "new Uint8Array()",
    ),
    ...(usage.wellKnownUsed.has("Any")
      ? [
          [
            wellKnownConverterDecl(
              usage,
              "Any",
              `from${wellKnownConverterName("Any")}`,
            ),
            " = (value: unknown) => {",
          ],
          "  const message = value as { readonly typeUrl?: string; readonly value?: Uint8Array };",
          "  return {",
          `    typeUrl: message.typeUrl ?? "",`,
          `    value: from${bytesConverterName}(message.value ?? new Uint8Array()),`,
          "  };",
          "};",
          "",
          [
            wellKnownConverterDecl(
              usage,
              "Any",
              `to${wellKnownConverterName("Any")}`,
            ),
            " = (value: unknown) => {",
          ],
          "  const message = value as { readonly typeUrl?: string; readonly value?: string };",
          "  return {",
          `    typeUrl: message.typeUrl ?? "",`,
          `    value: to${bytesConverterName}(message.value ?? ""),`,
          "  };",
          "};",
          "",
        ]
      : []),
    ...jsonWellKnownConverter(usage, "Struct"),
    ...jsonWellKnownConverter(usage, "Value"),
    ...jsonWellKnownConverter(usage, "ListValue"),
    ...jsonWellKnownConverter(usage, "FieldMask"),
  ];
};

// A wrapper used as a method type stays the `{ value }` message on the wire, so
// the registry needs the boxing converter rather than the bare one.
const toRegistryConverter = (type: MethodTypeModel): Printable =>
  isWrapperWellKnownKind(type.wellKnown)
    ? `to${type.name}Message`
    : methodValueRef(type, `to${type.name}`);

const wrapperConverter = (
  usage: FileUsage,
  type: WellKnownKind,
  scalar: ScalarKind,
  unsigned: boolean,
  defaultValue: string,
): ReadonlyArray<Printable> => {
  if (!usage.wellKnownUsed.has(type)) return [];
  // A wrapper value can reach the converter either already unwrapped (a bare
  // scalar, e.g. when nested through another converter) or as the `{ value }`
  // message; accept both. Nested wrapper fields use bare scalars because
  // protobuf-es unwraps wrapper fields.
  const scalarField = {
    kind: "scalar",
    name: "value",
    type: scalar,
    unsigned,
  } as const;
  const guard =
    scalar === "bytes"
      ? "value instanceof Uint8Array"
      : `typeof value === "${scalar}"`;
  const unwrapped = `\n    ${guard}\n      ? value\n      : ((value as { readonly value?: unknown }).value ?? ${defaultValue})\n  `;
  return [
    [
      wellKnownConverterDecl(
        usage,
        type,
        `from${wellKnownConverterName(type)}`,
      ),
      " = (value: unknown) =>",
    ],
    ["  ", fromScalarValue(unwrapped, scalarField), ";"],
    "",
    [
      wellKnownConverterDecl(usage, type, `to${wellKnownConverterName(type)}`),
      " = (value: unknown) => ",
      toScalarValue("value", scalarField),
      ";",
    ],
    "",
    ...(usage.boxedWrappers.has(type)
      ? [
          `const to${wellKnownConverterName(type)}Message = (value: unknown) => ({`,
          ["  value: ", toScalarValue("value", scalarField), ","],
          "});",
          "",
        ]
      : []),
  ];
};

const jsonWellKnownConverter = (
  usage: FileUsage,
  type: WellKnownKind,
): ReadonlyArray<Printable> => {
  const schema = wellKnownJsonSchemaName(type);
  return schema && usage.wellKnownUsed.has(type)
    ? [
        [
          wellKnownConverterDecl(
            usage,
            type,
            `from${wellKnownConverterName(type)}`,
          ),
          " = (value: unknown) =>",
        ],
        ["  ", sym.toJson, "(", sym.wktSchema(schema), ", value as never);"],
        "",
        [
          wellKnownConverterDecl(
            usage,
            type,
            `to${wellKnownConverterName(type)}`,
          ),
          " = (value: unknown) =>",
        ],
        ["  ", sym.fromJson, "(", sym.wktSchema(schema), ", value as never);"],
        "",
      ]
    : [];
};

const bytesConverterName = grpcGeneratedName("Bytes");

const oneofConverterName = (
  field: Extract<FieldModel, { readonly kind: "oneof" }>,
) => grpcGeneratedName(`${field.converterName}Oneof`);

// A well-known method type's converters share the name of its schema and type
// (`Grpc$GoogleProtobufTimestamp`), so the registry needs no lookup at all: it
// reads `from<name>`/`to<name>` off the method model like any other type.
const wellKnownConverterName = (type: WellKnownKind) => grpcWellKnownName(type);

// Only converters standing in for a well-known method type are exported (the
// registry reads them off the method name); field-only converters stay local
// and keep the plain-const text — there is no local-name registration in
// protoplugin, so the `Grpc$` namespace remains their collision guard.
const wellKnownConverterDecl = (
  usage: FileUsage,
  type: WellKnownKind,
  name: string,
): Printable =>
  usage.wellKnownMethods.has(type)
    ? exportDecl("const", name)
    : `const ${name}`;
