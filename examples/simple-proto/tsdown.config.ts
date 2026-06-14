import { createConfig } from "../../tsdown.shared.js";

export default createConfig({
  platform: "node",
  entry: ["src/**/*.ts"],
  outDir: "dist",
});
