import { grpcEmptyName, grpcWellKnownName } from "./naming.js";
import {
  isRequestStreaming,
  type FieldModel,
  type FieldValueModel,
  type GeneratorFile,
  type MessageModel,
  type WellKnownKind,
} from "./types.js";
import { wellKnownJsonSchemaName, wellKnownProtobufName } from "./wellKnown.js";

/**
 * One analysis of what a generated file actually uses — imports, helpers,
 * method partitions, well-known types, recursive edges — computed once from
 * the normalized model. Renderers consume these facts instead of re-scanning
 * the model, so absence conditions (where nothing must be emitted) are
 * decided in one place.
 */
export interface FileUsage {
  readonly hasServices: boolean;
  /** Some method goes through Effect RPC (unary or server-streaming). */
  readonly hasRpcMethods: boolean;
  /** Some method uses the direct streaming bridge (client/bidi streaming). */
  readonly hasStreamingMethods: boolean;
  /** `Stream` appears in generated signatures only for non-unary methods. */
  readonly usesStream: boolean;
  /** Some message has a field, so converters need `readField`/`compact`. */
  readonly readsFields: boolean;
  readonly usesGrpcEmpty: boolean;
  /** A bytes conversion is emitted somewhere (needs `node:buffer`). */
  readonly usesBase64Bytes: boolean;
  /** Well-known kinds used by message fields (incl. list/map/oneof values). */
  readonly wellKnownFields: ReadonlySet<WellKnownKind>;
  /** Well-known kinds used as method input/output types. */
  readonly wellKnownMethods: ReadonlySet<WellKnownKind>;
  /** Union of well-known kinds used as a field OR as a method type. */
  readonly wellKnownUsed: ReadonlySet<WellKnownKind>;
  /** Wrapper kinds that need the boxed `{ value }` message encoding. */
  readonly boxedWrappers: ReadonlySet<WellKnownKind>;
  /** `[importedName, alias]` pairs from `@bufbuild/protobuf/wkt`, sorted. */
  readonly jsonWellKnownImports: ReadonlyArray<readonly [string, string]>;
  /**
   * Imported names whose bare `type` alias generated code references: enums
   * used in a field position (`from*` converters cast with `as <Enum>`) and
   * messages used as a method input/output (client/server signatures name the
   * type directly). Every other position goes through the imported
   * `Schema`/`from`/`to` symbols, so emitting the alias would leave an unused
   * import behind.
   */
  readonly usedImportedTypes: ReadonlySet<string>;
  /** Messages in dependency order for schema emission. */
  readonly orderedMessages: ReadonlyArray<MessageModel>;
  /** `A->B` edges that participate in a cycle and need `Schema.suspend`. */
  readonly recursiveEdges: ReadonlySet<string>;
}

/** Canonical emission order for well-known kinds. */
export const wellKnownKinds = [
  "timestamp",
  "duration",
  "double-value",
  "float-value",
  "int64-value",
  "uint64-value",
  "int32-value",
  "uint32-value",
  "bool-value",
  "string-value",
  "bytes-value",
  "any",
  "struct",
  "value",
  "list-value",
  "field-mask",
] as const satisfies ReadonlyArray<WellKnownKind>;

export const wrapperWellKnownKinds = [
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

export const analyzeFileUsage = (file: GeneratorFile): FileUsage => {
  const wellKnownMethods = new Set<WellKnownKind>();
  const methodTypeNames = new Set<string>();
  let usesGrpcEmpty = false;
  for (const service of file.services) {
    for (const method of service.methods) {
      for (const typeName of [method.inputType, method.outputType]) {
        methodTypeNames.add(typeName);
        if (typeName === grpcEmptyName) usesGrpcEmpty = true;
        const kind = kindByMethodTypeName.get(typeName);
        if (kind) wellKnownMethods.add(kind);
      }
    }
  }

  const wellKnownFields = new Set<WellKnownKind>();
  // A wrapper used as a method type or inside list/map/oneof needs the boxed
  // `{ value }` encoding; a direct wrapper field arrives unwrapped.
  const boxedWrappers = new Set<WellKnownKind>(
    wrapperWellKnownKinds.filter((kind) => wellKnownMethods.has(kind)),
  );
  let usesBytesScalar = false;
  const enumFieldTypeNames = new Set<string>();
  for (const message of file.messages) {
    for (const field of message.fields) {
      for (const { value, boxed } of fieldValueOccurrences(field)) {
        if (value.kind === "enum") enumFieldTypeNames.add(value.enumName);
        if (value.kind === "well-known") {
          wellKnownFields.add(value.type);
          if (boxed && isWrapperWellKnownKind(value.type)) {
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
  const usesWellKnown = (kind: WellKnownKind) => wellKnownUsed.has(kind);

  const jsonWellKnownImports = wellKnownKinds
    .flatMap((kind) => {
      const alias = usesWellKnown(kind)
        ? wellKnownJsonSchemaName(kind)
        : undefined;
      return alias ? [[alias.replace(/^Protobuf/, ""), alias] as const] : [];
    })
    .sort(([left], [right]) => left.localeCompare(right));

  return {
    hasServices: file.services.length > 0,
    hasRpcMethods: file.services.some((service) =>
      service.methods.some((method) => !isRequestStreaming(method)),
    ),
    hasStreamingMethods: file.services.some((service) =>
      service.methods.some(isRequestStreaming),
    ),
    usesStream: file.services.some((service) =>
      service.methods.some((method) => method.kind !== "unary"),
    ),
    readsFields: file.messages.some((message) => message.fields.length > 0),
    usesGrpcEmpty,
    usesBase64Bytes:
      usesBytesScalar || usesWellKnown("bytes-value") || usesWellKnown("any"),
    wellKnownFields,
    wellKnownMethods,
    wellKnownUsed,
    boxedWrappers,
    jsonWellKnownImports,
    usedImportedTypes: new Set(
      file.imports.flatMap((imported) => [
        ...imported.enums.filter((name) => enumFieldTypeNames.has(name)),
        ...imported.messages.filter((name) => methodTypeNames.has(name)),
      ]),
    ),
    orderedMessages: orderMessages(file.messages),
    recursiveEdges: findRecursiveEdges(file.messages),
  };
};

export const isWrapperWellKnownKind = (type: WellKnownKind): boolean =>
  wrapperWellKnownKinds.includes(
    type as (typeof wrapperWellKnownKinds)[number],
  );

const kindByMethodTypeName = new Map<string, WellKnownKind>(
  wellKnownKinds.map((kind) => [
    grpcWellKnownName(wellKnownProtobufName(kind)),
    kind,
  ]),
);

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

const orderMessages = (messages: ReadonlyArray<MessageModel>) => {
  const byName = new Map(messages.map((message) => [message.name, message]));
  const visited = new Set<string>();
  const ordered: Array<MessageModel> = [];

  const visit = (message: MessageModel) => {
    if (visited.has(message.name)) return;
    visited.add(message.name);
    for (const dependency of messageDependencies(message)) {
      const dependencyMessage = byName.get(dependency);
      if (dependencyMessage) visit(dependencyMessage);
    }
    ordered.push(message);
  };

  for (const message of messages) {
    visit(message);
  }
  return ordered;
};

const messageDependencies = (message: MessageModel) =>
  message.fields.flatMap((field) =>
    fieldValueOccurrences(field).flatMap(({ value }) =>
      value.kind === "message" && value.source === "local"
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
