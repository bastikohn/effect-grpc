/**
 * The supported `google.protobuf.*` types, in canonical emission order. The
 * protobuf type name *is* the kind, so no name derivation is needed anywhere.
 */
export const wellKnownKinds = [
  "Timestamp",
  "Duration",
  "DoubleValue",
  "FloatValue",
  "Int64Value",
  "UInt64Value",
  "Int32Value",
  "UInt32Value",
  "BoolValue",
  "StringValue",
  "BytesValue",
  "Any",
  "Struct",
  "Value",
  "ListValue",
  "FieldMask",
] as const;

export type WellKnownKind = (typeof wellKnownKinds)[number];

/** Wrapper kinds that need the boxed `{ value }` message encoding. */
export const wrapperWellKnownKinds: ReadonlySet<WellKnownKind> = new Set(
  wellKnownKinds.slice(
    wellKnownKinds.indexOf("DoubleValue"),
    wellKnownKinds.indexOf("BytesValue") + 1,
  ),
);

const packagePrefix = "google.protobuf.";

export const wellKnownKind = (typeName: string): WellKnownKind | undefined => {
  const name = typeName.startsWith(packagePrefix)
    ? typeName.slice(packagePrefix.length)
    : undefined;
  return name !== undefined &&
    (wellKnownKinds as ReadonlyArray<string>).includes(name)
    ? (name as WellKnownKind)
    : undefined;
};

/**
 * The `@bufbuild/protobuf/wkt` schema a JSON-encoded well-known type needs,
 * under its exported name. A proto message that happens to generate the same
 * name (e.g. a message named `Struct`) is safe: protoplugin aliases the
 * imported symbol on collision with any exported declaration.
 */
export const wellKnownJsonSchemaName = (kind: WellKnownKind) => {
  switch (kind) {
    case "Struct":
    case "Value":
    case "ListValue":
    case "FieldMask":
      return `${kind}Schema`;
    default:
      return undefined;
  }
};
