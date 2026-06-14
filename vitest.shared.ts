import { fileURLToPath } from "node:url";
import { defineConfig } from "vitest/config";

export const resolveTestPath = (baseUrl: string, path: string) =>
  fileURLToPath(new URL(path, baseUrl));

const resolveRepoPath = (path: string) =>
  resolveTestPath(import.meta.url, path);

export const createVitestConfig = (aliases: Record<string, string> = {}) =>
  defineConfig({
    resolve: {
      alias: {
        "@effect-grpc/effect-grpc": resolveRepoPath(
          "packages/effect-grpc/src/index.ts",
        ),
        "@effect-grpc/protoc-gen-effect-grpc": resolveRepoPath(
          "packages/protoc-gen-effect-grpc/src/index.ts",
        ),
        ...aliases,
      },
    },
    test: {
      environment: "node",
      globals: false,
      include: ["test/**/*.{test,spec}.ts", "src/**/*.{test,spec}.ts"],
      coverage: {
        provider: "v8",
        reportsDirectory: "coverage",
      },
    },
  });
