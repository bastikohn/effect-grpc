import { defineConfig } from "@playwright/test";

export default defineConfig({
  testDir: "./packages/effect-grpc/test-browser",
  fullyParallel: false,
  workers: 1,
  timeout: 20_000,
  use: { browserName: "chromium", headless: true },
});
