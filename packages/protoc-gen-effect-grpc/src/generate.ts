import type { Printable } from "@bufbuild/protoplugin";

import { analyzeFileUsage } from "./fileUsage.js";
import { generateClient } from "./generateClient.js";
import { generateRegistry } from "./generateRegistry.js";
import { generateSchemas } from "./generateSchemas.js";
import { generateServer } from "./generateServer.js";
import type { GeneratorFile } from "./types.js";

/**
 * One printable per generated line, printed into protoplugin's
 * `GeneratedFile` (which appends the newlines). Imports are a consequence of
 * printing: emitters reference external names as `ImportSymbol`s, and
 * protoplugin emits exactly the import statements for the symbols actually
 * printed — with paths relativized, extensions rewritten per the
 * `import_extension` option, type-only imports split out, and colliding
 * foreign names aliased.
 */
export const generateFile = (file: GeneratorFile): ReadonlyArray<Printable> => {
  const usage = analyzeFileUsage(file);
  return [
    ...generateSchemas(file, usage),
    ...generateRegistry(file, usage),
    ...generateClient(file),
    ...generateServer(file),
  ];
};
