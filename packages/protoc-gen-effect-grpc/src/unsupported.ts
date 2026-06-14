import {
  ScalarType,
  type DescEnum,
  type DescField,
  type DescMessage,
} from "@bufbuild/protobuf";
import { FeatureSet_FieldPresence } from "@bufbuild/protobuf/wkt";

import type { MethodModel, ScalarKind } from "./types.js";

export const supportedMethodKind = (options: {
  readonly serviceTypeName: string;
  readonly methodName: string;
  readonly methodKind:
    | "unary"
    | "server_streaming"
    | "client_streaming"
    | "bidi_streaming";
  readonly ignoreUnsupportedMethods: boolean;
}): MethodModel["kind"] | undefined => {
  switch (options.methodKind) {
    case "unary":
      return "unary";
    case "server_streaming":
      return "server-streaming";
    case "client_streaming":
    case "bidi_streaming": {
      if (options.ignoreUnsupportedMethods) return undefined;
      const kind =
        options.methodKind === "client_streaming"
          ? "client-streaming"
          : "bidirectional-streaming";
      throw new Error(
        [
          "Unsupported gRPC method kind:",
          `  ${options.serviceTypeName}/${options.methodName} is ${kind}.`,
          "The first prototype supports only unary and server-streaming.",
        ].join("\n"),
      );
    }
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
      if (isSupportedWellKnownType(field.message)) return;
      if (isWellKnownType(field.message)) {
        unsupportedField(
          field,
          `well-known type field ${field.parent.typeName}.${field.name} (${field.message.typeName})`,
          "well-known protobuf types",
        );
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
            unsupportedField(
              field,
              `repeated well-known type field ${field.parent.typeName}.${field.name} (${field.message.typeName})`,
              "repeated well-known type fields",
            );
          }
          return;
      }
      return;
    case "map":
      if (field.mapKey !== ScalarType.STRING) {
        unsupportedField(
          field,
          `map field ${field.parent.typeName}.${field.name}`,
          "non-string map keys",
        );
      }
      switch (field.mapKind) {
        case "scalar":
          return;
        case "message":
          if (isWellKnownType(field.message)) {
            unsupportedField(
              field,
              `map field ${field.parent.typeName}.${field.name} (${field.message.typeName})`,
              "well-known type map values",
            );
          }
          return;
        case "enum":
          unsupportedField(
            field,
            `map field ${field.parent.typeName}.${field.name}`,
            "enum map values",
          );
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
  supportedWellKnownTypeNames.has(message.typeName);

const supportedWellKnownTypeNames = new Set([
  "google.protobuf.Duration",
  "google.protobuf.Timestamp",
]);
