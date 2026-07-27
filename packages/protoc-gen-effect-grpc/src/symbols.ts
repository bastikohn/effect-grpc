import { createImportSymbol, type ImportSymbol } from "@bufbuild/protoplugin";

/**
 * Every external name the emitters print, as protoplugin `ImportSymbol`s.
 * An import statement is emitted only for symbols actually printed, so the
 * emitters never decide whether an import line is needed — printing the
 * reference is the decision. Symbols referenced in both type and value
 * positions stay value imports (protoplugin narrows to `import type` only
 * when every printed use is type-only).
 */

export const Schema = createImportSymbol("Schema", "effect");
export const Stream = createImportSymbol("Stream", "effect");
export const Effect = createImportSymbol("Effect", "effect");
export const Layer = createImportSymbol("Layer", "effect");
export const Context = createImportSymbol("Context", "effect");

const runtimePackage = "@effect-grpc/effect-grpc";
export const CodegenSupport = createImportSymbol(
  "CodegenSupport",
  runtimePackage,
);
export const GrpcInvoker = createImportSymbol("GrpcInvoker", runtimePackage);
export const GrpcMethodRegistry = createImportSymbol(
  "GrpcMethodRegistry",
  runtimePackage,
);
export const GrpcServerProtocol = createImportSymbol(
  "GrpcServerProtocol",
  runtimePackage,
);
export const GrpcStatusError = createImportSymbol(
  "GrpcStatusError",
  runtimePackage,
);

export const Buffer = createImportSymbol("Buffer", "node:buffer");

export const fromJson = createImportSymbol("fromJson", "@bufbuild/protobuf");
export const toJson = createImportSymbol("toJson", "@bufbuild/protobuf");

/** A protobuf-es well-known schema, e.g. `StructSchema`. */
export const wktSchema = (schemaName: string): ImportSymbol =>
  createImportSymbol(schemaName, "@bufbuild/protobuf/wkt");

// Sibling generated files are addressed project-root relative with a `.js`
// ending; protoplugin relativizes the path to the importing file and rewrites
// the ending to the consumer's `import_extension` option.
const protoBase = (protoFileName: string) =>
  protoFileName.endsWith(".proto")
    ? protoFileName.slice(0, -".proto".length)
    : protoFileName;

/** A protobuf-es descriptor from the sibling `*_pb` file. */
export const pbValue = (protoFileName: string, name: string): ImportSymbol =>
  createImportSymbol(name, `./${protoBase(protoFileName)}_pb.js`);

/** A value exported by another proto file's `*_effect_grpc` output. */
export const effectValue = (
  protoFileName: string,
  name: string,
): ImportSymbol =>
  createImportSymbol(name, `./${protoBase(protoFileName)}_effect_grpc.js`);

/**
 * A type exported by another proto file's `*_effect_grpc` output. Type-only:
 * the generated type aliases have no runtime binding, so a value import of
 * one would break at ESM load time when a transpile-only consumer keeps it.
 */
export const effectType = (protoFileName: string, name: string): ImportSymbol =>
  createImportSymbol(
    name,
    `./${protoBase(protoFileName)}_effect_grpc.js`,
    true,
  );
