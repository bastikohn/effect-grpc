/**
 * Options forwarded to `@effect-grpc/protoc-gen-effect-grpc`. These mirror the
 * plugin's own option keys (see that package's `options.ts`) so the CLI stays a
 * thin front-end over the existing generator.
 */
export interface PluginOptions {
  readonly importExtension: "js" | "ts";
  readonly errors: "grpc-status";
  readonly methods: ReadonlyArray<"unary" | "server-streaming">;
  readonly ignoreUnsupportedMethods: boolean;
  readonly int64: "bigint";
}

export const defaultPluginOptions: PluginOptions = {
  importExtension: "js",
  errors: "grpc-status",
  methods: ["unary", "server-streaming"],
  ignoreUnsupportedMethods: false,
  int64: "bigint",
};

/**
 * Serialize {@link PluginOptions} into a protoc `parameter` string.
 *
 * The string is comma-joined because `@bufbuild/protoplugin` splits the request
 * parameter on commas before parsing each `key=value` token. `methods` is
 * emitted as `methods=<first>` followed by bare `<rest>` keys, which is exactly
 * how Buf forwards a `methods=unary,server-streaming` opt line — the plugin
 * resets its method set on `methods=` and unions on each bare method key.
 */
export const toParameterString = (options: PluginOptions): string => {
  const tokens: Array<string> = [
    `import_extension=${options.importExtension}`,
    `errors=${options.errors}`,
    `int64=${options.int64}`,
  ];

  if (options.methods.length > 0) {
    const [first, ...rest] = options.methods;
    tokens.push(`methods=${first}`, ...rest);
  }

  if (options.ignoreUnsupportedMethods) {
    tokens.push("ignore_unsupported_methods=true");
  }

  return tokens.join(",");
};
