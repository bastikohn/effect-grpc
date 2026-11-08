import { fileURLToPath } from "node:url";

import { createVitestConfig } from "../../vitest.shared.js";

// `URL.pathname` percent-encodes (e.g. spaces in the checkout path), so
// decode it back to a filesystem path like vitest.shared.ts does.
const generated = (path: string) =>
  fileURLToPath(
    new URL(`../simple-proto/src/generated/${path}.ts`, import.meta.url),
  );

export default createVitestConfig({
  "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc":
    generated("demo/v1/user_service_effect_grpc"),
  "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc":
    generated("features/v1/showcase_effect_grpc"),
  "@effect-grpc/simple-proto/generated/features/v1/showcase_pb": generated(
    "features/v1/showcase_pb",
  ),
});
