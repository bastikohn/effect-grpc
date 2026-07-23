import { execFileSync } from "node:child_process";
import { mkdirSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { create, fromBinary } from "@bufbuild/protobuf";
import {
  CodeGeneratorRequestSchema,
  FileDescriptorSetSchema,
} from "@bufbuild/protobuf/wkt";

import { plugin } from "../src/pluginDefinition.js";
import { effectFileName } from "../src/naming.js";

const packageRoot = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const repoRoot = resolve(packageRoot, "../..");
const fixturesRoot = join(packageRoot, "test/fixtures/proto-features");
const generatedRoot = join(packageRoot, "test/.generated/proto-features");
const defaultPluginOptions =
  "target=ts,import_extension=js,errors=grpc-status,methods=unary,server-streaming";

export interface GeneratedProtoFeature {
  readonly content: string;
  readonly effectFile: string;
  readonly files: ReadonlyMap<string, string>;
  readonly name: string;
  readonly outputDir: string;
}

export interface GenerateProtoFeatureOptions {
  readonly options?: ReadonlyArray<string>;
  readonly primary?: string;
  readonly protoFiles?: ReadonlyArray<string>;
}

const generated = new Map<string, GeneratedProtoFeature>();

export const generateProtoFeature = (
  name: string,
  options: GenerateProtoFeatureOptions = {},
): GeneratedProtoFeature => {
  const cached = generated.get(name);
  if (cached) return cached;

  const protoFiles = options.protoFiles ?? [`${name}.proto`];
  const protoPaths = protoFiles.map((protoFile) =>
    join(fixturesRoot, protoFile),
  );
  const primary = options.primary ?? protoFiles[0]!;
  const outputDir = join(generatedRoot, name);
  const descriptorSet = fromBinary(
    FileDescriptorSetSchema,
    execFileSync(
      "pnpm",
      [
        "exec",
        "buf",
        "build",
        fixturesRoot,
        ...protoPaths.flatMap((protoPath) => ["--path", protoPath]),
        "--as-file-descriptor-set",
        "-o",
        "-",
      ],
      { cwd: repoRoot },
    ),
  );
  const protoFileNames = new Set(protoFiles);
  const fileToGenerate = descriptorSet.file
    .map((file) => file.name)
    .filter((file) => protoFileNames.has(file));
  const response = plugin.run(
    create(CodeGeneratorRequestSchema, {
      fileToGenerate,
      parameter: [defaultPluginOptions, ...(options.options ?? [])].join(","),
      protoFile: descriptorSet.file,
    }),
  );

  if (response.error) {
    throw new Error(response.error);
  }
  if (response.file.length !== protoFiles.length) {
    throw new Error(
      `Expected ${protoFiles.length} generated file(s) for ${name}.`,
    );
  }

  rmSync(outputDir, { force: true, recursive: true });
  mkdirSync(outputDir, { recursive: true });
  generateProtobufEs(protoPaths, outputDir);

  for (const file of response.file) {
    const path = join(outputDir, file.name);
    mkdirSync(dirname(path), { recursive: true });
    writeFileSync(path, file.content);
  }

  const effectFile = join(outputDir, effectFileName(primary));
  const files = new Map(
    response.file.map((file) => [
      file.name,
      readFileSync(join(outputDir, file.name), "utf8"),
    ]),
  );
  const result = {
    content: readFileSync(effectFile, "utf8"),
    effectFile,
    files,
    name,
    outputDir,
  };
  generated.set(name, result);
  return result;
};

export const typecheckProtoFeature = (
  feature: GeneratedProtoFeature,
  source: string,
) => {
  const tsconfig = writeTypecheckProject(feature, source);
  try {
    execFileSync("pnpm", ["exec", "tsc", "--project", tsconfig], {
      cwd: repoRoot,
      stdio: "pipe",
    });
  } catch (error) {
    const failure = error as {
      readonly stderr?: Buffer;
      readonly stdout?: Buffer;
    };
    throw new Error(
      [failure.stdout?.toString(), failure.stderr?.toString()]
        .filter(Boolean)
        .join("\n"),
    );
  }
};

const generateProtobufEs = (
  protoPaths: ReadonlyArray<string>,
  outputDir: string,
) => {
  const template = join(outputDir, "buf.gen.protoc-es.yaml");
  writeFileSync(
    template,
    [
      "version: v2",
      "plugins:",
      "  - local: protoc-gen-es",
      "    out: .",
      "    opt:",
      "      - target=ts",
      "      - import_extension=js",
      "",
    ].join("\n"),
  );
  execFileSync(
    "pnpm",
    [
      "exec",
      "buf",
      "generate",
      fixturesRoot,
      ...protoPaths.flatMap((protoPath) => ["--path", protoPath]),
      "--template",
      template,
      "-o",
      outputDir,
    ],
    { cwd: repoRoot, stdio: "pipe" },
  );
};

const writeTypecheckProject = (
  feature: GeneratedProtoFeature,
  source: string,
) => {
  writeFileSync(join(feature.outputDir, "typecheck.ts"), source);

  const tsconfig = join(generatedRoot, `tsconfig.${feature.name}.json`);
  writeFileSync(
    tsconfig,
    JSON.stringify(
      {
        extends: "../../../tsconfig.json",
        compilerOptions: {
          // Generated output must never emit unused locals (e.g. dead
          // imported `type` aliases) — consumers compile with this flag.
          noUnusedLocals: true,
          paths: {
            "@effect-grpc/effect-grpc": ["packages/effect-grpc/src/index.ts"],
            "@effect-grpc/effect-grpc/*": ["packages/effect-grpc/src/*"],
            effect: [
              "packages/effect-grpc/node_modules/effect/dist/index.d.ts",
            ],
            "effect/unstable/http/*": [
              "packages/effect-grpc/node_modules/effect/dist/unstable/http/*.d.ts",
            ],
          },
        },
        include: [`${feature.name}/**/*.ts`],
      },
      null,
      2,
    ),
  );
  return tsconfig;
};
