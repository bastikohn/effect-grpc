import type { MethodModel } from "./types.js";

export type MethodKindOption = MethodModel["kind"];

const methodKinds: ReadonlyArray<MethodKindOption> = [
  "unary",
  "server-streaming",
  "client-streaming",
  "bidi-streaming",
];

export interface GeneratorOptions {
  readonly importExtension: "js" | "ts";
  readonly errors: "grpc-status";
  readonly methods: ReadonlySet<MethodKindOption>;
  readonly int64: "bigint";
}

export const defaultOptions: GeneratorOptions = {
  importExtension: "js",
  errors: "grpc-status",
  methods: new Set(methodKinds),
  int64: "bigint",
};

export const parseOptions = (
  rawOptions: ReadonlyArray<{ readonly key: string; readonly value: string }>,
): GeneratorOptions => {
  let importExtension: "js" | "ts" = defaultOptions.importExtension;
  let errors: "grpc-status" = defaultOptions.errors;
  let methods = defaultOptions.methods;
  let int64: GeneratorOptions["int64"] = defaultOptions.int64;

  for (const option of rawOptions) {
    switch (option.key) {
      case "import_extension":
        if (option.value !== "js" && option.value !== "ts") {
          throw new Error(
            `Unsupported import_extension: ${option.value}. Expected js or ts.`,
          );
        }
        importExtension = option.value;
        break;
      case "errors":
        if (option.value !== "grpc-status") {
          throw new Error(
            `Unsupported errors option: ${option.value}. Expected grpc-status.`,
          );
        }
        errors = option.value;
        break;
      case "methods":
        methods = new Set(
          option.value
            .split(",")
            .map((item) => item.trim())
            .filter(Boolean)
            .map(parseMethodKind),
        );
        break;
      case "unary":
      case "server-streaming":
      case "client-streaming":
      case "bidi-streaming":
        methods = new Set([...methods, option.key]);
        break;
      case "int64":
        if (option.value !== "bigint") {
          throw new Error(
            `Unsupported int64 option: ${option.value}. Expected bigint.`,
          );
        }
        int64 = option.value;
        break;
      default:
        throw new Error(`Unknown protoc-gen-effect-grpc option: ${option.key}`);
    }
  }

  return {
    importExtension,
    errors,
    methods,
    int64,
  };
};

const parseMethodKind = (value: string): MethodKindOption => {
  const kind = methodKinds.find((item) => item === value);
  if (!kind) {
    throw new Error(`Unsupported methods option: ${value}.`);
  }
  return kind;
};
