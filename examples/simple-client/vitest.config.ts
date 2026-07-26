import { createVitestConfig } from "../../vitest.shared.js";

const generated = (path: string) =>
  new URL(`../simple-proto/src/generated/${path}.ts`, import.meta.url).pathname;

export default createVitestConfig({
  "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc":
    generated("demo/v1/user_service_effect_grpc"),
  "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc":
    generated("features/v1/showcase_effect_grpc"),
  "@effect-grpc/simple-proto/generated/features/v1/showcase_pb": generated(
    "features/v1/showcase_pb",
  ),
});
