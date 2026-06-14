import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import path from "node:path";
import { fileURLToPath } from "node:url";

import { NodeServices } from "@effect/platform-node";
import { Effect } from "effect";
import { afterEach, describe, expect, it } from "vitest";

import { generate } from "../src/run.js";

const fixtureRoot = fileURLToPath(
  new URL(
    "../../protoc-gen-effect-grpc/test/fixtures/proto-features",
    import.meta.url,
  ),
);
const outputDirs: Array<string> = [];

const tempOutputDir = async () => {
  const dir = await mkdtemp(path.join(tmpdir(), "effect-grpc-codegen-"));
  outputDirs.push(dir);
  return dir;
};

afterEach(async () => {
  await Promise.all(
    outputDirs
      .splice(0)
      .map((dir) => rm(dir, { recursive: true, force: true })),
  );
});

describe("generate", () => {
  it("writes protobuf-es and effect-grpc files", async () => {
    const outDir = await tempOutputDir();

    const result = await Effect.runPromise(
      generate({
        inputs: [path.join(fixtureRoot, "well_known_types.proto")],
        outDir,
        importPaths: [fixtureRoot],
      }).pipe(Effect.provide(NodeServices.layer)),
    );

    expect(result.files.map((file) => path.basename(file)).sort()).toEqual([
      "well_known_types_effect_grpc.ts",
      "well_known_types_pb.ts",
    ]);
    await expect(
      readFile(path.join(outDir, "well_known_types_pb.ts"), "utf8"),
    ).resolves.toContain("export const WellKnownTypeFeature");
    await expect(
      readFile(path.join(outDir, "well_known_types_effect_grpc.ts"), "utf8"),
    ).resolves.toContain('from "./well_known_types_pb.js"');
  });
});
