import {
  type FieldModel,
  type FieldValueModel,
  type GeneratorFile,
  type MessageModel,
  type MethodWellKnownKind,
} from "./types.js";
import { wrapperWellKnownKinds, type WellKnownKind } from "./wellKnown.js";

/**
 * One analysis of the content a generated file needs — helper emission,
 * well-known types, recursive edges — computed once from the normalized
 * model. Renderers consume these facts instead of re-scanning the model, so
 * absence conditions (where nothing must be emitted) are decided in one
 * place. Imports are not analyzed anywhere: emitters print protoplugin
 * `ImportSymbol`s and protoplugin emits imports for what was printed.
 */
export interface FileUsage {
  /** Some message has a field, so converters need `readField`/`compact`. */
  readonly readsFields: boolean;
  readonly usesGrpcEmpty: boolean;
  /** A base64 bytes conversion helper is emitted somewhere. */
  readonly usesBase64Bytes: boolean;
  /** Well-known kinds used as method input/output types. */
  readonly wellKnownMethods: ReadonlySet<WellKnownKind>;
  /** Union of well-known kinds used as a field OR as a method type. */
  readonly wellKnownUsed: ReadonlySet<WellKnownKind>;
  /** Wrapper kinds that need the boxed `{ value }` message encoding. */
  readonly boxedWrappers: ReadonlySet<WellKnownKind>;
  /** `A->B` edges that participate in a cycle and need `Schema.suspend`. */
  readonly recursiveEdges: ReadonlySet<string>;
}

export const analyzeFileUsage = (file: GeneratorFile): FileUsage => {
  const wellKnownMethods = new Set<WellKnownKind>();
  let usesGrpcEmpty = false;
  for (const service of file.services) {
    for (const method of service.methods) {
      for (const type of [method.inputType, method.outputType]) {
        if (type.wellKnown === "empty") usesGrpcEmpty = true;
        else if (type.wellKnown) wellKnownMethods.add(type.wellKnown);
      }
    }
  }

  const wellKnownFields = new Set<WellKnownKind>();
  // A wrapper used as a method type or inside list/map/oneof needs the boxed
  // `{ value }` encoding; a direct wrapper field arrives unwrapped.
  const boxedWrappers = new Set<WellKnownKind>(
    [...wrapperWellKnownKinds].filter((kind) => wellKnownMethods.has(kind)),
  );
  let usesBytesScalar = false;
  for (const message of file.messages) {
    for (const field of message.fields) {
      for (const { value, boxed } of fieldValueOccurrences(field)) {
        if (value.kind === "well-known") {
          wellKnownFields.add(value.type);
          if (boxed && wrapperWellKnownKinds.has(value.type)) {
            boxedWrappers.add(value.type);
          }
        }
        if (value.kind === "scalar" && value.type === "bytes") {
          usesBytesScalar = true;
        }
      }
    }
  }

  const wellKnownUsed = new Set<WellKnownKind>([
    ...wellKnownFields,
    ...wellKnownMethods,
  ]);

  return {
    readsFields: file.messages.some((message) => message.fields.length > 0),
    usesGrpcEmpty,
    usesBase64Bytes:
      usesBytesScalar ||
      wellKnownUsed.has("BytesValue") ||
      wellKnownUsed.has("Any"),
    wellKnownMethods,
    wellKnownUsed,
    boxedWrappers,
    recursiveEdges: findRecursiveEdges(file.messages),
  };
};

export const isWrapperWellKnownKind = (
  type: MethodWellKnownKind | undefined,
): boolean => wrapperWellKnownKinds.has(type as WellKnownKind);

/** Every value position a field contributes, with its wrapper-boxing context. */
const fieldValueOccurrences = (
  field: FieldModel,
): ReadonlyArray<{
  readonly value: FieldValueModel;
  readonly boxed: boolean;
}> => {
  switch (field.kind) {
    case "scalar":
    case "message":
    case "enum":
    case "well-known":
      return [{ value: field, boxed: false }];
    case "list":
      return [{ value: field.item, boxed: true }];
    case "map":
      return [{ value: field.value, boxed: true }];
    case "oneof":
      return field.cases.map((oneofCase) => ({
        value: oneofCase.value,
        boxed: true,
      }));
  }
};

const messageDependencies = (message: MessageModel) =>
  message.fields.flatMap((field) =>
    fieldValueOccurrences(field).flatMap(({ value }) =>
      value.kind === "message" && value.importedFrom === undefined
        ? [value.messageName]
        : [],
    ),
  );

const findRecursiveEdges = (messages: ReadonlyArray<MessageModel>) => {
  const dependenciesByMessage = new Map(
    messages.map((message) => [message.name, messageDependencies(message)]),
  );
  const recursiveEdges = new Set<string>();

  const hasPath = (
    from: string,
    to: string,
    seen = new Set<string>(),
  ): boolean => {
    if (from === to) return true;
    if (seen.has(from)) return false;
    seen.add(from);
    return (dependenciesByMessage.get(from) ?? []).some((dependency) =>
      hasPath(dependency, to, seen),
    );
  };

  for (const message of messages) {
    for (const dependency of dependenciesByMessage.get(message.name) ?? []) {
      if (hasPath(dependency, message.name)) {
        recursiveEdges.add(`${message.name}->${dependency}`);
      }
    }
  }
  return recursiveEdges;
};
