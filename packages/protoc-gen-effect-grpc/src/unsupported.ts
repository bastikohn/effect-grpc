import {
  ScalarType,
  type DescEnum,
  type DescField,
  type DescMessage,
} from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";

import type { MethodModel, ScalarKind } from "./types.js";
import { wellKnownKind } from "./wellKnown.js";

export const methodKindModel = (
  methodKind:
    | "unary"
    | "server_streaming"
    | "client_streaming"
    | "bidi_streaming",
): MethodModel["kind"] => {
  switch (methodKind) {
    case "unary":
      return "unary";
    case "server_streaming":
      return "server-streaming";
    case "client_streaming":
      return "client-streaming";
    case "bidi_streaming":
      return "bidi-streaming";
  }
};

export const supportedField = (field: DescField): void => {
  if (field.fieldKind === "scalar") {
    if (field.presence === FeatureSet_FieldPresence.LEGACY_REQUIRED) {
      unsupportedField(
        field,
        `proto2 required field ${field.parent.typeName}.${field.name}`,
        "proto2 required fields",
      );
    }
    if (
      field.proto.defaultValue !== undefined &&
      field.proto.defaultValue !== ""
    ) {
      unsupportedField(
        field,
        `proto2 default field ${field.parent.typeName}.${field.name}`,
        "proto2 default values",
      );
    }
    return;
  }
  const message =
    field.fieldKind === "message" ||
    (field.fieldKind === "list" && field.listKind === "message") ||
    (field.fieldKind === "map" && field.mapKind === "message")
      ? field.message
      : undefined;
  if (
    message !== undefined &&
    isWellKnownType(message) &&
    wellKnownKind(message.typeName) === undefined
  ) {
    unsupportedField(
      field,
      `well-known type field ${field.parent.typeName}.${field.name} (${message.typeName})`,
      "well-known protobuf types",
    );
  }
};

export const scalarKind = (scalar: ScalarType): ScalarKind => {
  switch (scalar) {
    case ScalarType.STRING:
      return "string";
    case ScalarType.BOOL:
      return "boolean";
    case ScalarType.BYTES:
      return "bytes";
    case ScalarType.DOUBLE:
    case ScalarType.FLOAT:
    case ScalarType.INT32:
    case ScalarType.FIXED32:
    case ScalarType.UINT32:
    case ScalarType.SFIXED32:
    case ScalarType.SINT32:
      return "number";
    case ScalarType.INT64:
    case ScalarType.UINT64:
    case ScalarType.FIXED64:
    case ScalarType.SFIXED64:
    case ScalarType.SINT64:
      return "bigint";
  }
};

const unsupportedField = (
  field: DescField,
  subject: string,
  feature: string,
): never => {
  throw new Error(
    [
      "Unsupported protobuf field:",
      `  ${subject} is not supported.`,
      `The generator supports only explicitly covered protobuf constructs; ${feature} must be added deliberately with fixtures.`,
    ].join("\n"),
  );
};

export const isWellKnownType = (desc: DescMessage | DescEnum) =>
  desc.file.proto.package === "google.protobuf";
