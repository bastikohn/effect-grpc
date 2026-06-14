import { createVitestConfig } from "../../vitest.shared.js";

export default createVitestConfig({
  "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc":
    new URL(
      "../features-proto/src/generated/features/v1/showcase_effect_grpc.ts",
      import.meta.url,
    ).pathname,
  "@effect-grpc/features-proto/generated/features/v1/common_effect_grpc":
    new URL(
      "../features-proto/src/generated/features/v1/common_effect_grpc.ts",
      import.meta.url,
    ).pathname,
  "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc":
    new URL(
      "../simple-proto/src/generated/demo/v1/user_service_effect_grpc.ts",
      import.meta.url,
    ).pathname,
});
