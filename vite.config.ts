import { defineConfig } from "vite-plus";

const generatedPatterns = [
  "coverage",
  "dist",
  "node_modules",
  ".pnpm-store",
  "**/src/generated/**",
  "pnpm-lock.yaml",
  "*.tsbuildinfo",
];

export default defineConfig({
  fmt: {
    ignorePatterns: generatedPatterns,
    printWidth: 80,
    sortPackageJson: false,
  },
  lint: {
    ignorePatterns: generatedPatterns,
    plugins: ["eslint", "oxc", "typescript"],
    categories: {
      correctness: "warn",
      suspicious: "warn",
      perf: "warn",
    },
    rules: {
      "eslint/no-await-in-loop": "off",
      "eslint/no-shadow": "off",
      "eslint/no-underscore-dangle": "off",
      "eslint/preserve-caught-error": "off",
      "oxc/no-map-spread": "off",
      "typescript/no-extraneous-class": "off",
    },
    options: {
      typeAware: false,
      typeCheck: false,
    },
  },
});
