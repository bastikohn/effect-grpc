import {
  grpcEmptyName,
  grpcWellKnownName,
  serviceRegistryName,
} from "./naming.js";
import type {
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  ScalarKind,
  WellKnownKind,
} from "./types.js";
import { wellKnownProtobufName } from "./wellKnown.js";

export const generateRegistry = (file: GeneratorFile) => [
  ...(usesGrpcEmpty(file)
    ? [
        `export const from${grpcEmptyName} = (_message: unknown): ${grpcEmptyName} => ({});`,
        "",
        `export const to${grpcEmptyName} = (_value: unknown) => ({});`,
        "",
      ]
    : []),
  ...generateConverters(file),
  ...file.services.flatMap((service) => [
    `export const ${serviceRegistryName(service.name)} = new Map<string, GrpcMethodRegistry.GrpcMethodEntry>([`,
    ...service.methods.flatMap((method) => [
      "  [",
      `    "${service.typeName}/${method.name}",`,
      "    {",
      `      kind: "${method.kind}",`,
      `      tag: "${service.typeName}/${method.name}",`,
      `      service: ${service.name},`,
      `      localName: "${method.localName}",`,
      `      payloadSchema: ${method.inputType}Schema,`,
      `      successSchema: ${method.outputType}Schema,`,
      `      toGrpcRequest: ${toRegistryConverter(method.inputType)},`,
      `      fromGrpcRequest: from${method.inputType},`,
      `      toGrpcResponse: ${toRegistryConverter(method.outputType)},`,
      `      fromGrpcResponse: from${method.outputType},`,
      "    },",
      "  ],",
    ]),
    "]);",
    "",
  ]),
];

const usesGrpcEmpty = (file: GeneratorFile) =>
  file.services.some((service) =>
    service.methods.some(
      (method) =>
        method.inputType === grpcEmptyName ||
        method.outputType === grpcEmptyName,
    ),
  );

const generateConverters = (file: GeneratorFile) => {
  const messages = file.messages;
  return [
    ...(messages.length === 0
      ? []
      : [
          "const readField = (message: unknown, field: string): unknown =>",
          `  typeof message === "object" && message !== null ? (message as Record<string, unknown>)[field] : undefined;`,
          "",
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
    ...scalarConverters(file),
    ...wellKnownConverters(file),
    ...messages.flatMap((message) => [
      ...message.fields.flatMap((field) =>
        field.kind === "oneof" ? oneofConverters(field) : [],
      ),
      `export const from${message.name} = (message: unknown): unknown => compact({`,
      ...message.fields.map((field) => `  ${field.name}: ${fromField(field)},`),
      "});",
      "",
      `export const to${message.name} = (value: unknown): Record<string, unknown> => {`,
      "  const message = value as Record<string, unknown>;",
      "  return compact({",
      ...message.fields.map((field) => `    ${field.name}: ${toField(field)},`),
      "  });",
      "};",
      "",
    ]),
  ];
};

const fromField = (field: FieldModel): string => {
  if (field.kind === "message") {
    const value = `readField(message, "${field.name}")`;
    return `${value} == null ? undefined : from${field.messageName}(${value})`;
  }
  if (field.kind === "enum") {
    return `readField(message, "${field.name}") as ${field.enumName}${field.optional ? " | undefined" : ""}`;
  }
  if (field.kind === "well-known") {
    const value = `readField(message, "${field.name}")`;
    return `${value} == null ? undefined : from${wellKnownConverterName(field.type)}(${value})`;
  }
  if (field.kind === "scalar") {
    const value = `readField(message, "${field.name}")`;
    return field.optional
      ? `${value} == null ? undefined : ${fromValue(value, field)}`
      : fromValue(value, field);
  }
  if (field.kind === "list") {
    return `((readField(message, "${field.name}") as ReadonlyArray<unknown> | undefined) ?? []).map((value) => ${fromValue("value", field.item)})`;
  }
  if (field.kind === "map") {
    return `Object.fromEntries(Object.entries((readField(message, "${field.name}") as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [${fromMapKey("key", field.key.type)}, ${fromValue("value", field.value)}]))`;
  }
  return `from${field.converterName}Oneof(readField(message, "${field.name}"))`;
};

const toField = (field: FieldModel): string => {
  const value = `readField(message, "${field.name}")`;
  if (field.kind === "message") {
    return `${value} == null ? undefined : to${field.messageName}(${value})`;
  }
  if (field.kind === "scalar") {
    return field.optional
      ? `${value} == null ? undefined : ${toValue(value, field)}`
      : toValue(value, field);
  }
  if (field.kind === "enum") {
    return `${value} as number${field.optional ? " | undefined" : ""}`;
  }
  if (field.kind === "well-known") {
    return `${value} == null ? undefined : ${toWellKnownValue(value, field, "bare")}`;
  }
  if (field.kind === "list") {
    return `((${value} as ReadonlyArray<unknown> | undefined) ?? []).map((value) => ${toValue("value", field.item, "boxed")})`;
  }
  if (field.kind === "map") {
    return `Object.fromEntries(Object.entries((${value} as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [${toMapKey("key", field.key.type)}, ${toValue("value", field.value, "boxed")}]))`;
  }
  if (field.kind === "oneof") return `to${field.converterName}Oneof(${value})`;
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

const fromValue = (value: string, field: FieldValueModel): string => {
  switch (field.kind) {
    case "scalar":
      return fromScalarValue(value, field);
    case "message":
      return `from${field.messageName}(${value})`;
    case "enum":
      return `${value} as ${field.enumName}`;
    case "well-known":
      return `from${wellKnownConverterName(field.type)}(${value})`;
  }
};

const toValue = (
  value: string,
  field: FieldValueModel,
  wrapperEncoding: "bare" | "boxed" = "bare",
): string => {
  switch (field.kind) {
    case "scalar":
      return toScalarValue(value, field);
    case "enum":
      return `${value} as number`;
    case "message":
      return `to${field.messageName}(${value})`;
    case "well-known":
      return toWellKnownValue(value, field, wrapperEncoding);
  }
};

const toWellKnownValue = (
  value: string,
  field: Extract<FieldValueModel, { readonly kind: "well-known" }>,
  wrapperEncoding: "bare" | "boxed",
) =>
  wrapperEncoding === "boxed" && isWrapperWellKnownKind(field.type)
    ? `to${wellKnownConverterName(field.type)}Message(${value})`
    : `to${wellKnownConverterName(field.type)}(${value})`;

const fromScalarValue = (
  value: string,
  field: Extract<FieldValueModel, { readonly kind: "scalar" }>,
) => {
  switch (field.type) {
    case "bytes":
      return `fromBytes((${value}) as Uint8Array)`;
    case "bigint":
      return `String(${value})`;
    default:
      return `(${value}) as ${scalarTsType(field.type)}`;
  }
};

const toScalarValue = (
  value: string,
  field: Extract<FieldValueModel, { readonly kind: "scalar" }>,
) => {
  switch (field.type) {
    case "bytes":
      return `toBytes(${value})`;
    case "bigint":
      return `BigInt((${value}) as string)`;
    default:
      return `(${value}) as ${scalarTsType(field.type)}`;
  }
};

const oneofConverters = (
  field: Extract<FieldModel, { readonly kind: "oneof" }>,
) => [
  `const from${field.converterName}Oneof = (value: unknown): unknown => {`,
  `  const oneof = (value ?? { case: undefined }) as { readonly case?: string; readonly value?: unknown };`,
  // The unset case arrives as `undefined` from protobuf-es but may come back
  // as `null` after decoding round-trips; coalesce so both select the
  // `undefined` branch.
  "  switch (oneof.case ?? undefined) {",
  ...field.cases.flatMap((oneofCase) => [
    `    case "${oneofCase.name}":`,
    `      return { case: "${oneofCase.name}", value: ${fromValue("oneof.value", oneofCase.value)} };`,
  ]),
  "    case undefined:",
  // The unset case decodes against `Schema.Undefined`, so keep the key
  // present with an `undefined` value (protobuf-es also uses `undefined`).
  "      return { case: undefined };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${oneof.case}\`);`,
  "  }",
  "};",
  "",
  `const to${field.converterName}Oneof = (value: unknown): unknown => {`,
  `  const oneof = value ?? { case: undefined };`,
  `  const message = oneof as { readonly case?: string; readonly value?: unknown };`,
  // See `from*Oneof`: coalesce `null` and `undefined` for the unset case.
  "  switch (message.case ?? undefined) {",
  ...field.cases.flatMap((oneofCase) => [
    `    case "${oneofCase.name}":`,
    `      return { case: "${oneofCase.name}", value: ${toValue("message.value", oneofCase.value, "boxed")} };`,
  ]),
  "    case undefined:",
  "      return { case: undefined };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${message.case}\`);`,
  "  }",
  "};",
  "",
];

// `fromBytes`/`toBytes` (and the `node:buffer` import they need) are required
// whenever base64 bytes conversion is emitted: a bytes scalar field, or a
// BytesValue/Any well-known used as a field OR as a method input/output.
export const usesBase64Bytes = (file: GeneratorFile): boolean => {
  const fields = file.messages.flatMap((message) => message.fields);
  return (
    fields.some((field) => usesScalar(field, "bytes")) ||
    usesWellKnownInFile(file, "bytes-value") ||
    usesWellKnownInFile(file, "any")
  );
};

const scalarConverters = (file: GeneratorFile) =>
  usesBase64Bytes(file)
    ? [
        "const fromBytes = (value: Uint8Array): string =>",
        `  Buffer.from(value).toString("base64");`,
        "",
        "const toBytes = (value: unknown): Uint8Array =>",
        `  Uint8Array.from(Buffer.from(value as string, "base64"));`,
        "",
      ]
    : [];

const wellKnownConverters = (file: GeneratorFile) => {
  return [
    ...(usesWellKnownInFile(file, "timestamp")
      ? [
          `${wellKnownConverterDecl(file, "timestamp")} from${wellKnownConverterName("timestamp")} = (value: unknown): string => {`,
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const seconds = Number(message.seconds ?? 0);",
          "  const nanos = message.nanos ?? 0;",
          "  return new Date(seconds * 1000 + Math.trunc(nanos / 1_000_000)).toISOString();",
          "};",
          "",
          `${wellKnownConverterDecl(file, "timestamp")} to${wellKnownConverterName("timestamp")} = (value: unknown) => {`,
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
    ...(usesWellKnownInFile(file, "duration")
      ? [
          `${wellKnownConverterDecl(file, "duration")} from${wellKnownConverterName("duration")} = (value: unknown) => {`,
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const nanos = BigInt(message.seconds ?? 0) * 1_000_000_000n + BigInt(message.nanos ?? 0);",
          `  return nanos % 1_000_000n === 0n ? { _tag: "Millis", millis: Number(nanos / 1_000_000n) } : { _tag: "Nanos", nanos: String(nanos) };`,
          "};",
          "",
          `${wellKnownConverterDecl(file, "duration")} to${wellKnownConverterName("duration")} = (value: unknown) => {`,
          "  const duration = value as { readonly _tag?: string; readonly millis?: number; readonly nanos?: string };",
          '  const nanos = duration._tag === "Millis"',
          "    ? BigInt(duration.millis as number) * 1_000_000n",
          '    : duration._tag === "Nanos"',
          "      ? BigInt(duration.nanos as string)",
          `      : (() => { throw new Error(\`Unsupported Duration encoding: \${duration._tag}\`); })();`,
          "  return {",
          "    seconds: nanos / 1_000_000_000n,",
          "    nanos: Number(nanos % 1_000_000_000n),",
          "  };",
          "};",
          "",
        ]
      : []),
    ...wrapperConverter(file, "double-value", "number", false, "0"),
    ...wrapperConverter(file, "float-value", "number", false, "0"),
    ...wrapperConverter(file, "int32-value", "number", false, "0"),
    ...wrapperConverter(file, "uint32-value", "number", true, "0"),
    ...wrapperConverter(file, "int64-value", "bigint", false, "0n"),
    ...wrapperConverter(file, "uint64-value", "bigint", true, "0n"),
    ...wrapperConverter(file, "bool-value", "boolean", false, "false"),
    ...wrapperConverter(file, "string-value", "string", false, '""'),
    ...wrapperConverter(
      file,
      "bytes-value",
      "bytes",
      false,
      "new Uint8Array()",
    ),
    ...(usesWellKnownInFile(file, "any")
      ? [
          `${wellKnownConverterDecl(file, "any")} from${wellKnownConverterName("any")} = (value: unknown) => {`,
          "  const message = value as { readonly typeUrl?: string; readonly value?: Uint8Array };",
          "  return {",
          `    typeUrl: message.typeUrl ?? "",`,
          "    value: fromBytes(message.value ?? new Uint8Array()),",
          "  };",
          "};",
          "",
          `${wellKnownConverterDecl(file, "any")} to${wellKnownConverterName("any")} = (value: unknown) => {`,
          "  const message = value as { readonly typeUrl?: string; readonly value?: string };",
          "  return {",
          `    typeUrl: message.typeUrl ?? "",`,
          `    value: toBytes(message.value ?? ""),`,
          "  };",
          "};",
          "",
        ]
      : []),
    ...jsonWellKnownConverter(file, "struct"),
    ...jsonWellKnownConverter(file, "value"),
    ...jsonWellKnownConverter(file, "list-value"),
    ...jsonWellKnownConverter(file, "field-mask"),
  ];
};

const wrapperWellKnownKinds = [
  "double-value",
  "float-value",
  "int32-value",
  "uint32-value",
  "int64-value",
  "uint64-value",
  "bool-value",
  "string-value",
  "bytes-value",
] as const satisfies ReadonlyArray<WellKnownKind>;

const wrapperWellKnownKindByGrpcName = (
  name: string,
): WellKnownKind | undefined =>
  wrapperWellKnownKinds.find((type) => wellKnownConverterName(type) === name);

const isWrapperWellKnownKind = (type: WellKnownKind): boolean =>
  wrapperWellKnownKinds.includes(
    type as (typeof wrapperWellKnownKinds)[number],
  );

const toRegistryConverter = (name: string) => {
  const wrapper = wrapperWellKnownKindByGrpcName(name);
  return wrapper ? `to${wellKnownConverterName(wrapper)}Message` : `to${name}`;
};

const wrapperConverter = (
  file: GeneratorFile,
  type: WellKnownKind,
  scalar: ScalarKind,
  unsigned: boolean,
  defaultValue: string,
) => {
  if (!usesWellKnownInFile(file, type)) return [];
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
    `${wellKnownConverterDecl(file, type)} from${wellKnownConverterName(type)} = (value: unknown) =>`,
    `  ${fromScalarValue(unwrapped, {
      kind: "scalar",
      name: "value",
      type: scalar,
      unsigned,
    })};`,
    "",
    `${wellKnownConverterDecl(file, type)} to${wellKnownConverterName(type)} = (value: unknown) => ${toScalarValue(
      "value",
      scalarField,
    )};`,
    "",
    ...(usesBoxedWrapperInFile(file, type)
      ? [
          `const to${wellKnownConverterName(type)}Message = (value: unknown) => ({`,
          `  value: ${toScalarValue("value", scalarField)},`,
          "});",
          "",
        ]
      : []),
  ];
};

const usesBoxedWrapperInFile = (file: GeneratorFile, type: WellKnownKind) => {
  const fields = file.messages.flatMap((message) => message.fields);
  return (
    usesWellKnownMethod(file, type) ||
    fields.some((field) => usesBoxedWrapper(field, type))
  );
};

const jsonWellKnownConverter = (file: GeneratorFile, type: WellKnownKind) => {
  const schema = wellKnownJsonSchema(type);
  return schema && usesWellKnownInFile(file, type)
    ? [
        `${wellKnownConverterDecl(file, type)} from${wellKnownConverterName(type)} = (value: unknown) =>`,
        `  protobufToJson(${schema}, value as never);`,
        "",
        `${wellKnownConverterDecl(file, type)} to${wellKnownConverterName(type)} = (value: unknown) =>`,
        `  protobufFromJson(${schema}, value as never);`,
        "",
      ]
    : [];
};

const wellKnownJsonSchema = (type: WellKnownKind) => {
  switch (type) {
    case "struct":
      return "ProtobufStructSchema";
    case "value":
      return "ProtobufValueSchema";
    case "list-value":
      return "ProtobufListValueSchema";
    case "field-mask":
      return "ProtobufFieldMaskSchema";
    default:
      return undefined;
  }
};

// Well-known converters share a single namespaced identity (e.g.
// `GrpcGoogleProtobufValue`) so they never collide with the per-message
// converters `from<MessageName>`/`to<MessageName>`, and are exported under that
// same name when the type is used as a method input/output (so the registry can
// reference them) — no separate alias is needed.
const wellKnownConverterName = (type: WellKnownKind) =>
  grpcWellKnownName(wellKnownProtobufName(type));

const wellKnownConverterDecl = (file: GeneratorFile, type: WellKnownKind) =>
  usesWellKnownMethod(file, type) ? "export const" : "const";

const usesWellKnownMethod = (file: GeneratorFile, type: WellKnownKind) => {
  const name = grpcWellKnownName(wellKnownProtobufName(type));
  return file.services.some((service) =>
    service.methods.some(
      (method) => method.inputType === name || method.outputType === name,
    ),
  );
};

const usesWellKnownInFile = (file: GeneratorFile, type: WellKnownKind) => {
  const fields = file.messages.flatMap((message) => message.fields);
  return (
    fields.some((field) => usesWellKnown(field, type)) ||
    usesWellKnownMethod(file, type)
  );
};

const usesScalar = (field: FieldModel, type: ScalarKind): boolean => {
  switch (field.kind) {
    case "scalar":
      return field.type === type;
    case "list":
      return field.item.kind === "scalar" && field.item.type === type;
    case "map":
      return field.value.kind === "scalar" && field.value.type === type;
    case "oneof":
      return field.cases.some(
        (oneofCase) =>
          oneofCase.value.kind === "scalar" && oneofCase.value.type === type,
      );
    default:
      return false;
  }
};

const usesWellKnown = (field: FieldModel, type: WellKnownKind): boolean => {
  switch (field.kind) {
    case "well-known":
      return field.type === type;
    case "list":
      return field.item.kind === "well-known" && field.item.type === type;
    case "map":
      return field.value.kind === "well-known" && field.value.type === type;
    case "oneof":
      return field.cases.some(
        (oneofCase) =>
          oneofCase.value.kind === "well-known" &&
          oneofCase.value.type === type,
      );
    default:
      return false;
  }
};

const usesBoxedWrapper = (field: FieldModel, type: WellKnownKind): boolean => {
  switch (field.kind) {
    case "list":
      return field.item.kind === "well-known" && field.item.type === type;
    case "map":
      return field.value.kind === "well-known" && field.value.type === type;
    case "oneof":
      return field.cases.some(
        (oneofCase) =>
          oneofCase.value.kind === "well-known" &&
          oneofCase.value.type === type,
      );
    default:
      return false;
  }
};
