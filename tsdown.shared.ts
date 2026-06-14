import { defineConfig, type UserConfig } from "tsdown";

export const createConfig = (config: UserConfig) =>
  defineConfig({
    clean: true,
    dts: true,
    sourcemap: true,
    format: ["esm"],
    unbundle: true,
    ...config,
  });
