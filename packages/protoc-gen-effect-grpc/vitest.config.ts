import { createVitestConfig, resolveTestPath } from "../../vitest.shared.js";

export default createVitestConfig({
  effect: resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist",
  ),
});
