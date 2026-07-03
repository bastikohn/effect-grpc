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
  switch (field.fieldKind) {
    case "scalar":
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
    case "message":
      if (isWellKnownType(field.message)) {
        supportedWellKnownField(field, field.message);
      }
      return;
    case "enum":
      return;
    case "list":
      switch (field.listKind) {
        case "scalar":
        case "enum":
          return;
        case "message":
          if (isWellKnownType(field.message)) {
            supportedWellKnownField(field, field.message);
          }
          return;
      }
      return;
    case "map":
      switch (field.mapKind) {
        case "scalar":
          return;
        case "message":
          if (isWellKnownType(field.message)) {
            supportedWellKnownField(field, field.message);
          }
          return;
        case "enum":
          return;
      }
      return;
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

export const isSupportedWellKnownType = (message: DescMessage) =>
  wellKnownKind(message.typeName) !== undefined;

const supportedWellKnownField = (
  field: DescField,
  message: DescMessage,
): void => {
  if (isSupportedWellKnownType(message)) return;
  unsupportedField(
    field,
    `well-known type field ${field.parent.typeName}.${field.name} (${message.typeName})`,
    "well-known protobuf types",
  );
};
