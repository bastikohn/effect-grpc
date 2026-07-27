import type { Printable } from "@bufbuild/protoplugin";

import * as sym from "./symbols.js";
import type { MethodTypeModel } from "./types.js";

/**
 * The printable protoplugin's `GeneratedFile.export()` returns, built directly
 * so emitters stay pure functions from model to printables. Every exported
 * generated declaration must go through here: protoplugin's collision aliasing
 * keys off the names registered by export statements (`identifiersTaken`), so
 * a hand-written `export const` string would silently opt the name out of the
 * aliasing guarantee.
 */
export const exportDecl = (declaration: string, name: string): Printable => ({
  kind: "es_export_stmt",
  name,
  declaration,
});

/** Interleave `separator` between printables, like `Array.join` for text. */
export const joinPrintables = (
  items: ReadonlyArray<Printable>,
  separator: string,
): Printable =>
  items.flatMap((item, index) => (index === 0 ? [item] : [separator, item]));

/**
 * A method input/output type in a type position: local declarations print
 * their name, imported ones a type-only import symbol.
 */
export const methodTypeRef = (type: MethodTypeModel): Printable =>
  type.importedFrom === undefined
    ? type.name
    : sym.effectType(type.importedFrom, type.name);
