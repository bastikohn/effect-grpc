import {
  grpcBoolValueName,
  grpcDurationName,
  grpcEmptyName,
  grpcTimestampName,
  serviceRegistryName,
} from "./naming.js";
import type {
  FieldModel,
  FieldValueModel,
  GeneratorFile,
  MessageModel,
  WellKnownKind,
} from "./types.js";

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
      `      toGrpcRequest: to${method.inputType},`,
      `      fromGrpcRequest: from${method.inputType},`,
      `      toGrpcResponse: to${method.outputType},`,
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
        ]),
    ...scalarConverters(messages),
    ...wellKnownConverters(file),
    ...messages.flatMap((message) => [
      ...message.fields.flatMap((field) =>
        field.kind === "oneof" ? oneofConverters(field) : [],
      ),
      `export const from${message.name} = (message: unknown): unknown => ({`,
      ...message.fields.map((field) => `  ${field.name}: ${fromField(field)},`),
      "});",
      "",
      `export const to${message.name} = (value: unknown) => {`,
      "  const message = value as Record<string, unknown>;",
      "  return {",
      ...message.fields.map((field) => `    ${field.name}: ${toField(field)},`),
      "  };",
      "};",
      "",
    ]),
    ...wellKnownMethodConverters(file),
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
    return `${value} == null ? undefined : from${wellKnownName(field.type)}(${value})`;
  }
  if (field.kind === "list" && field.item.kind === "message") {
    return `((readField(message, "${field.name}") as ReadonlyArray<unknown> | undefined) ?? []).map(from${field.item.messageName})`;
  }
  if (field.kind === "list" && field.item.kind === "enum") {
    return `(readField(message, "${field.name}") as ReadonlyArray<${field.item.enumName}> | undefined) ?? []`;
  }
  if (field.kind === "scalar") {
    const value = `readField(message, "${field.name}")`;
    return field.optional
      ? `${value} == null ? undefined : ${fromValue(value, field)}`
      : fromValue(value, field);
  }
  if (field.kind === "list" && field.item.kind === "scalar") {
    return `((readField(message, "${field.name}") as ReadonlyArray<${scalarTsType(field.item.type)}> | undefined) ?? []).map((value) => ${fromValue("value", field.item)})`;
  }
  if (field.kind === "map" && field.value.kind === "message") {
    return `Object.fromEntries(Object.entries((readField(message, "${field.name}") as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [key, from${field.value.messageName}(value)]))`;
  }
  if (field.kind === "map" && field.value.kind === "scalar") {
    return `Object.fromEntries(Object.entries((readField(message, "${field.name}") as Record<string, ${scalarTsType(field.value.type)}> | undefined) ?? {}).map(([key, value]) => [key, ${fromValue("value", field.value)}]))`;
  }
  return `from${field.name}Oneof(readField(message, "${field.name}"))`;
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
    return `${value} == null ? undefined : to${wellKnownName(field.type)}(${value})`;
  }
  if (field.kind === "list" && field.item.kind === "message") {
    return `((${value} as ReadonlyArray<unknown> | undefined) ?? []).map(to${field.item.messageName})`;
  }
  if (field.kind === "list" && field.item.kind === "enum") {
    return `(${value} as ReadonlyArray<number> | undefined) ?? []`;
  }
  if (field.kind === "list") {
    return `((${value} as ReadonlyArray<unknown> | undefined) ?? []).map((value) => ${toValue("value", field.item)})`;
  }
  if (field.kind === "map" && field.value.kind === "message") {
    return `Object.fromEntries(Object.entries((${value} as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [key, to${field.value.messageName}(value)]))`;
  }
  if (field.kind === "map") {
    return `Object.fromEntries(Object.entries((${value} as Record<string, unknown> | undefined) ?? {}).map(([key, value]) => [key, ${toValue("value", field.value)}]))`;
  }
  if (field.kind === "oneof") return `to${field.name}Oneof(${value})`;
  return value;
};

const scalarTsType = (
  type: Extract<FieldModel, { readonly kind: "scalar" }>["type"],
) => {
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

const fromValue = (value: string, field: FieldValueModel): string => {
  switch (field.kind) {
    case "scalar":
      return fromScalarValue(value, field);
    case "message":
      return `from${field.messageName}(${value})`;
    case "enum":
      return `${value} as ${field.enumName}`;
    case "well-known":
      return `from${wellKnownName(field.type)}(${value})`;
  }
};

const toValue = (value: string, field: FieldValueModel): string => {
  switch (field.kind) {
    case "scalar":
      return toScalarValue(value, field);
    case "enum":
      return `${value} as number`;
    case "message":
      return `to${field.messageName}(${value})`;
    case "well-known":
      return `to${wellKnownName(field.type)}(${value})`;
  }
};

const fromScalarValue = (
  value: string,
  field: Extract<FieldValueModel, { readonly kind: "scalar" }>,
) => {
  switch (field.type) {
    case "bytes":
      return `fromBytes(${value} as Uint8Array)`;
    case "bigint":
      return `String(${value})`;
    default:
      return `${value} as ${scalarTsType(field.type)}`;
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
      return `BigInt(${value} as string)`;
    default:
      return `${value} as ${scalarTsType(field.type)}`;
  }
};

const oneofConverters = (
  field: Extract<FieldModel, { readonly kind: "oneof" }>,
) => [
  `const from${field.name}Oneof = (value: unknown) => {`,
  `  const oneof = (value ?? { case: undefined }) as { readonly case?: string; readonly value?: unknown };`,
  "  switch (oneof.case) {",
  ...field.cases.flatMap((oneofCase) => [
    `    case "${oneofCase.name}":`,
    `      return { case: "${oneofCase.name}", value: ${fromValue("oneof.value", oneofCase.value)} };`,
  ]),
  "    case undefined:",
  "      return { case: undefined };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${oneof.case}\`);`,
  "  }",
  "};",
  "",
  `const to${field.name}Oneof = (value: unknown) => {`,
  `  const oneof = value ?? { case: undefined };`,
  `  const message = oneof as { readonly case?: string; readonly value?: unknown };`,
  "  switch (message.case) {",
  ...field.cases.flatMap((oneofCase) => [
    `    case "${oneofCase.name}":`,
    `      return { case: "${oneofCase.name}", value: ${toValue("message.value", oneofCase.value)} };`,
  ]),
  "    case undefined:",
  "      return { case: undefined };",
  "    default:",
  `      throw new Error(\`Unknown oneof case ${field.name}: \${message.case}\`);`,
  "  }",
  "};",
  "",
];

const scalarConverters = (messages: ReadonlyArray<MessageModel>) => {
  const fields = messages.flatMap((message) => message.fields);
  return [
    ...(fields.some((field) => usesScalar(field, "bytes"))
      ? [
          "const fromBytes = (value: Uint8Array): string =>",
          `  Buffer.from(value).toString("base64");`,
          "",
          "const toBytes = (value: unknown): Uint8Array =>",
          `  Uint8Array.from(Buffer.from(value as string, "base64"));`,
          "",
        ]
      : []),
  ];
};

const wellKnownConverters = (file: GeneratorFile) => {
  const messages = file.messages;
  const fields = messages.flatMap((message) => message.fields);
  return [
    ...(fields.some((field) => usesWellKnown(field, "timestamp")) ||
    usesWellKnownMethod(file, "timestamp")
      ? [
          "const fromTimestamp = (value: unknown): string => {",
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const seconds = Number(message.seconds ?? 0);",
          "  const nanos = message.nanos ?? 0;",
          "  return new Date(seconds * 1000 + Math.trunc(nanos / 1_000_000)).toISOString();",
          "};",
          "",
          "const toTimestamp = (value: unknown) => {",
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
    ...(fields.some((field) => usesWellKnown(field, "duration")) ||
    usesWellKnownMethod(file, "duration")
      ? [
          "const fromDuration = (value: unknown) => {",
          "  const message = value as { readonly seconds?: bigint | number; readonly nanos?: number };",
          "  const nanos = BigInt(message.seconds ?? 0) * 1_000_000_000n + BigInt(message.nanos ?? 0);",
          `  return nanos % 1_000_000n === 0n ? { _tag: "Millis", value: Number(nanos / 1_000_000n) } : { _tag: "Nanos", value: String(nanos) };`,
          "};",
          "",
          "const toDuration = (value: unknown) => {",
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
    ...(fields.some((field) => usesWellKnown(field, "bool-value")) ||
    usesWellKnownMethod(file, "bool-value")
      ? [
          "const fromBoolValue = (value: unknown) =>",
          `  (value as { readonly value?: boolean }).value ?? false;`,
          "",
          "const toBoolValue = (value: unknown) => ({",
          "  value: value as boolean,",
          "});",
          "",
        ]
      : []),
  ];
};

const wellKnownMethodConverters = (file: GeneratorFile) =>
  [
    ["timestamp", grpcTimestampName] as const,
    ["duration", grpcDurationName] as const,
    ["bool-value", grpcBoolValueName] as const,
  ].flatMap(([type, name]) =>
    usesWellKnownMethod(file, type)
      ? [
          `export const from${name} = from${wellKnownName(type)};`,
          `export const to${name} = to${wellKnownName(type)};`,
          "",
        ]
      : [],
  );

const usesWellKnownMethod = (file: GeneratorFile, type: WellKnownKind) => {
  const name =
    type === "timestamp"
      ? grpcTimestampName
      : type === "duration"
        ? grpcDurationName
        : grpcBoolValueName;
  return file.services.some((service) =>
    service.methods.some(
      (method) => method.inputType === name || method.outputType === name,
    ),
  );
};

const usesScalar = (
  field: FieldModel,
  type: Extract<FieldValueModel, { readonly kind: "scalar" }>["type"],
): boolean => {
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
    default:
      return false;
  }
};

const wellKnownName = (type: WellKnownKind) => {
  switch (type) {
    case "timestamp":
      return "Timestamp";
    case "duration":
      return "Duration";
    case "bool-value":
      return "BoolValue";
  }
};
