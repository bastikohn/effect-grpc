import type { Printable } from "@bufbuild/protoplugin";

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
