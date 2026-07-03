import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

const args = process.argv.slice(2);
const dryRun = args.includes("--dry-run");
const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const publishArgs = [
  "publish",
  "--access",
  "public",
  // Prereleases must never land on `latest`. Any changeset pre-mode release
  // (e.g. the v4 `1.0.0-beta.x` line) publishes to the `next` dist-tag; only
  // stable releases (the v3 `0.1.x` line, pre mode exited) go to `latest`.
  // pre.json is the source of truth for which line we're on, so a mistake in
  // pre mode can't silently publish a prerelease as `latest`.
  ...(args.some((arg) => arg === "--tag" || arg.startsWith("--tag="))
    ? []
    : ["--tag", readDistTag()]),
  // pnpm's default `publish-branch` is main/master, so publishing the v3 line
  // from the `v3` branch triggers an interactive confirmation that CI answers
  // "no" — silently skipping the npm upload while the job still goes green.
  // CI already gates the branch via the workflow `if:`, so disable pnpm's git
  // checks (branch, clean tree, up-to-date) for every publish, not just dry runs.
  ...(!args.includes("--no-git-checks") ? ["--no-git-checks"] : []),
  ...args,
];
const packages = [
  {
    name: "@effect-grpc/effect-grpc",
    dir: "packages/effect-grpc",
  },
  {
    name: "@effect-grpc/protoc-gen-effect-grpc",
    dir: "packages/protoc-gen-effect-grpc",
  },
  {
    name: "@effect-grpc/codegen",
    dir: "packages/codegen",
  },
];

for (const packageInfo of packages) {
  const version = readPackageVersion(packageInfo.dir);
  if (dryRun) {
    publish(packageInfo.name);
    continue;
  }
  if (isPublished(packageInfo.name, version)) {
    console.log(
      `${packageInfo.name}@${version} is already published; skipping.`,
    );
  } else {
    publish(packageInfo.name);
  }
  tagAndRelease(packageInfo, version);
}

function publish(name) {
  execFileSync("pnpm", ["--filter", name, ...publishArgs], {
    stdio: "inherit",
  });
}

// Returns the dist-tag to publish under. Any changeset pre-mode release is a
// prerelease and publishes to `next` (regardless of the prerelease identifier
// in the version string, e.g. `beta`); stable releases go to `latest`.
function readDistTag() {
  try {
    const pre = JSON.parse(
      readFileSync(join(root, ".changeset", "pre.json"), "utf8"),
    );
    if (pre.mode === "pre") {
      return "next";
    }
  } catch {
    // No pre.json (or unreadable): not in pre mode.
  }
  return "latest";
}

function readPackageVersion(dir) {
  const manifest = JSON.parse(
    readFileSync(join(root, dir, "package.json"), "utf8"),
  );
  return manifest.version;
}

function isPublished(name, version) {
  try {
    execFileSync("npm", ["view", `${name}@${version}`, "version"], {
      encoding: "utf8",
      stdio: ["ignore", "ignore", "pipe"],
    });
    return true;
  } catch (cause) {
    const error = cause;
    if (
      error.status === 1 &&
      typeof error.stderr === "string" &&
      (error.stderr.includes("E404") || error.stderr.includes("404"))
    ) {
      return false;
    }
    throw cause;
  }
}

// Tags and releases are created for every published version, not only the ones
// published by this run, so a rerun repairs a publish that failed halfway.
function tagAndRelease(packageInfo, version) {
  const tag = `${packageInfo.name}@${version}`;
  if (!tagExists(tag)) {
    execFileSync("git", ["tag", tag], { cwd: root, stdio: "inherit" });
  }
  execFileSync("git", ["push", "origin", `refs/tags/${tag}`], {
    cwd: root,
    stdio: "inherit",
  });
  if (!process.env.GITHUB_TOKEN && !process.env.GH_TOKEN) {
    console.warn(`No GITHUB_TOKEN/GH_TOKEN; skipping GitHub release ${tag}.`);
    return;
  }
  if (releaseExists(tag)) {
    console.log(`GitHub release ${tag} already exists; skipping.`);
    return;
  }
  const notes =
    readChangelogEntry(packageInfo.dir, version) ?? `Release ${tag}.`;
  execFileSync(
    "gh",
    [
      "release",
      "create",
      tag,
      "--title",
      tag,
      "--notes",
      notes,
      ...(version.includes("-") ? ["--prerelease"] : []),
    ],
    { cwd: root, stdio: "inherit" },
  );
}

function tagExists(tag) {
  const output = execFileSync("git", ["tag", "--list", tag], {
    cwd: root,
    encoding: "utf8",
  });
  return output.trim() === tag;
}

function releaseExists(tag) {
  try {
    execFileSync("gh", ["release", "view", tag], {
      cwd: root,
      stdio: "ignore",
    });
    return true;
  } catch {
    return false;
  }
}

function readChangelogEntry(dir, version) {
  let changelog;
  try {
    changelog = readFileSync(join(root, dir, "CHANGELOG.md"), "utf8");
  } catch {
    return undefined;
  }
  const lines = changelog.split("\n");
  const start = lines.findIndex((line) => line.trim() === `## ${version}`);
  if (start === -1) {
    return undefined;
  }
  let end = lines.findIndex(
    (line, index) => index > start && line.startsWith("## "),
  );
  if (end === -1) {
    end = lines.length;
  }
  const entry = lines
    .slice(start + 1, end)
    .join("\n")
    .trim();
  return entry === "" ? undefined : entry;
}
