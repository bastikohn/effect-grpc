import { execFileSync } from "node:child_process";
import {
  mkdirSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const packageNames = [
  "@effect-grpc/effect-grpc",
  "@effect-grpc/protoc-gen-effect-grpc",
];
const effectVersion = readWorkspaceCatalogVersion("effect");
const workDir = mkdtempSync(join(tmpdir(), "effect-grpc-package-smoke-"));
const packDir = join(workDir, "pack");
const consumerDir = join(workDir, "consumer");

const run = (command, args, cwd) => {
  execFileSync(command, args, {
    cwd,
    stdio: "inherit",
    env: { ...process.env, CI: "1" },
  });
};

function readWorkspaceCatalogVersion(name) {
  const workspace = readFileSync(join(root, "pnpm-workspace.yaml"), "utf8");
  const catalog = workspace.match(/^catalog:\n(?<body>(?:  .+\n?)+)/m);
  const range = catalog?.groups?.body.match(
    new RegExp(`^  ${name}: (?<range>\\S+)`, "m"),
  )?.groups?.range;

  if (!range) {
    throw new Error(`Missing ${name} catalog version in pnpm-workspace.yaml`);
  }

  return range;
}

try {
  mkdirSync(packDir, { recursive: true });
  mkdirSync(consumerDir, { recursive: true });

  run(
    "pnpm",
    [
      "--filter",
      "@effect-grpc/effect-grpc",
      "--filter",
      "@effect-grpc/protoc-gen-effect-grpc",
      "build",
    ],
    root,
  );

  for (const packageName of packageNames) {
    run(
      "pnpm",
      ["--filter", packageName, "pack", "--pack-destination", packDir],
      root,
    );
  }

  const tarballPaths = readdirSync(packDir)
    .filter((file) => file.endsWith(".tgz"))
    .map((file) => join(packDir, file));

  if (tarballPaths.length !== packageNames.length) {
    throw new Error(
      `Expected ${packageNames.length} package tarballs in ${packDir}`,
    );
  }

  const runtimeTarballPath = tarballPaths.find((file) =>
    file.includes("effect-grpc-effect-grpc-"),
  );
  const codegenTarballPath = tarballPaths.find((file) =>
    file.includes("effect-grpc-protoc-gen-effect-grpc-"),
  );

  if (!runtimeTarballPath || !codegenTarballPath) {
    throw new Error(`Missing expected package tarballs in ${packDir}`);
  }

  writeFileSync(
    join(consumerDir, "package.json"),
    JSON.stringify(
      {
        private: true,
        type: "module",
        dependencies: {
          "@effect-grpc/effect-grpc": `file:${runtimeTarballPath}`,
          "@effect-grpc/protoc-gen-effect-grpc": `file:${codegenTarballPath}`,
          "@bufbuild/buf": "^1.60.0",
          "@bufbuild/protobuf": "^2.0.0",
          "@bufbuild/protoc-gen-es": "^2.0.0",
          "@connectrpc/connect": "^2.0.0",
          effect: effectVersion,
          typescript: "^5.0.0",
        },
        pnpm: {
          onlyBuiltDependencies: ["msgpackr-extract"],
          overrides: {
            "@effect-grpc/effect-grpc": `file:${runtimeTarballPath}`,
          },
        },
      },
      null,
      2,
    ),
  );
  mkdirSync(join(consumerDir, "proto/demo/v1"), { recursive: true });
  writeFileSync(
    join(consumerDir, "buf.yaml"),
    `version: v2
modules:
  - path: proto
`,
  );
  writeFileSync(
    join(consumerDir, "buf.gen.yaml"),
    `version: v2
clean: true
plugins:
  - local: protoc-gen-es
    out: src/generated
    opt:
      - target=ts
      - import_extension=js
  - local: protoc-gen-effect-grpc
    out: src/generated
    opt:
      - target=ts
      - import_extension=js
      - errors=grpc-status
      - methods=unary,server-streaming
`,
  );
  writeFileSync(
    join(consumerDir, "proto/demo/v1/user_service.proto"),
    `syntax = "proto3";

package demo.v1;

service UserService {
  rpc GetUser(GetUserRequest) returns (GetUserResponse);
}

message GetUserRequest {
  string id = 1;
}

message GetUserResponse {
  User user = 1;
}

message User {
  string id = 1;
  string name = 2;
}
`,
  );
  writeFileSync(
    join(consumerDir, "smoke.ts"),
    `import { Effect, Layer } from "effect";
import { GrpcClientProtocol, GrpcMethodRegistry, GrpcStatusError } from "@effect-grpc/effect-grpc";
import { plugin } from "@effect-grpc/protoc-gen-effect-grpc";
import {
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  type UserServiceImplementation
} from "./src/generated/demo/v1/user_service_effect_grpc.js";

const error = GrpcStatusError.notFound("missing");
const registry: GrpcMethodRegistry.GrpcMethodRegistry = UserServiceGrpcRegistry;
const implementation: UserServiceImplementation = {
  getUser: (request) => Effect.succeed({
    user: { id: request.id, name: "Demo User" }
  })
};
const handlers = UserServiceHandlersLayer(implementation);
const clientLayer = UserServiceClientLayer.pipe(
  Layer.provide(
    GrpcClientProtocol.layer({
      baseUrl: new URL("http://127.0.0.1:50051"),
      registry
    })
  )
);

console.log(error.code, Boolean(plugin), handlers, clientLayer);
`,
  );
  writeFileSync(
    join(consumerDir, "runtime-smoke.mjs"),
    `import runtimePackage from "@effect-grpc/effect-grpc/package.json" with { type: "json" };
import codegenPackage from "@effect-grpc/protoc-gen-effect-grpc/package.json" with { type: "json" };
import * as runtime from "@effect-grpc/effect-grpc";
import * as codegen from "@effect-grpc/protoc-gen-effect-grpc";

const assert = (condition, message) => {
  if (!condition) {
    throw new Error(message);
  }
};

const assertWorkspaceFree = (pkg) => {
  const invalidPrefixes = ["workspace:", "catalog:"];
  for (const section of [
    "dependencies",
    "peerDependencies",
    "optionalDependencies",
    "devDependencies",
  ]) {
    for (const [name, range] of Object.entries(pkg[section] ?? {})) {
      assert(
        !invalidPrefixes.some((prefix) => String(range).startsWith(prefix)),
        \`\${pkg.name} publishes \${section}.\${name} with invalid range \${range}\`,
      );
    }
  }
};

const assertImportFails = async (specifier) => {
  try {
    await import(specifier);
  } catch {
    return;
  }
  throw new Error(\`Expected import to fail: \${specifier}\`);
};

assert(runtime.GrpcClientProtocol, "missing GrpcClientProtocol root export");
assert(runtime.GrpcServerProtocol, "missing GrpcServerProtocol root export");
assert(runtime.GrpcStatusError, "missing GrpcStatusError root export");
assert(codegen.plugin, "missing plugin root export");
assert(runtimePackage.name === "@effect-grpc/effect-grpc", "runtime package.json import failed");
assert(codegenPackage.name === "@effect-grpc/protoc-gen-effect-grpc", "codegen package.json import failed");
assertWorkspaceFree(runtimePackage);
assertWorkspaceFree(codegenPackage);
await assertImportFails("@effect-grpc/effect-grpc/internal/metadata");
await assertImportFails("@effect-grpc/protoc-gen-effect-grpc/generate");
await assertImportFails("@effect-grpc/protoc-gen-effect-grpc/internal/plugin");
`,
  );
  writeFileSync(
    join(consumerDir, "tsconfig.json"),
    JSON.stringify(
      {
        compilerOptions: {
          target: "ESNext",
          module: "ESNext",
          moduleResolution: "Bundler",
          strict: true,
          skipLibCheck: true,
        },
        include: ["smoke.ts"],
      },
      null,
      2,
    ),
  );

  run("pnpm", ["install", "--ignore-scripts"], consumerDir);
  run("node", ["runtime-smoke.mjs"], consumerDir);
  run("pnpm", ["exec", "protoc-gen-effect-grpc", "--version"], consumerDir);
  run("pnpm", ["exec", "buf", "generate"], consumerDir);
  run("pnpm", ["exec", "tsc", "--noEmit"], consumerDir);
} finally {
  rmSync(workDir, { recursive: true, force: true });
}
