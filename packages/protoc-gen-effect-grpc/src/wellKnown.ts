import type { WellKnownKind } from "./types.js";

const wellKnownTypes = {
  "google.protobuf.Timestamp": {
    kind: "timestamp",
    protobufName: "Timestamp",
  },
  "google.protobuf.Duration": {
    kind: "duration",
    protobufName: "Duration",
  },
  "google.protobuf.DoubleValue": {
    kind: "double-value",
    protobufName: "DoubleValue",
  },
  "google.protobuf.FloatValue": {
    kind: "float-value",
    protobufName: "FloatValue",
  },
  "google.protobuf.Int64Value": {
    kind: "int64-value",
    protobufName: "Int64Value",
  },
  "google.protobuf.UInt64Value": {
    kind: "uint64-value",
    protobufName: "UInt64Value",
  },
  "google.protobuf.Int32Value": {
    kind: "int32-value",
    protobufName: "Int32Value",
  },
  "google.protobuf.UInt32Value": {
    kind: "uint32-value",
    protobufName: "UInt32Value",
  },
  "google.protobuf.BoolValue": {
    kind: "bool-value",
    protobufName: "BoolValue",
  },
  "google.protobuf.StringValue": {
    kind: "string-value",
    protobufName: "StringValue",
  },
  "google.protobuf.BytesValue": {
    kind: "bytes-value",
    protobufName: "BytesValue",
  },
  "google.protobuf.Any": {
    kind: "any",
    protobufName: "Any",
  },
  "google.protobuf.Struct": {
    kind: "struct",
    protobufName: "Struct",
  },
  "google.protobuf.Value": {
    kind: "value",
    protobufName: "Value",
  },
  "google.protobuf.ListValue": {
    kind: "list-value",
    protobufName: "ListValue",
  },
  "google.protobuf.FieldMask": {
    kind: "field-mask",
    protobufName: "FieldMask",
  },
} as const satisfies Record<
  string,
  { readonly kind: WellKnownKind; readonly protobufName: string }
>;

export const wellKnownKind = (typeName: string): WellKnownKind | undefined =>
  wellKnownTypes[typeName as keyof typeof wellKnownTypes]?.kind;

export const wellKnownProtobufName = (kind: WellKnownKind): string => {
  for (const entry of Object.values(wellKnownTypes)) {
    if (entry.kind === kind) return entry.protobufName;
  }
  return kind;
};

export const wellKnownJsonSchemaName = (kind: WellKnownKind) => {
  switch (kind) {
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
