import { createVitestConfig, resolveTestPath } from "../../vitest.shared.js";

export default createVitestConfig({
  "effect/unstable/http": resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist/unstable/http",
  ),
  effect: resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist",
  ),
});
