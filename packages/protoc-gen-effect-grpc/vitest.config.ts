import { createVitestConfig, resolveTestPath } from "../../vitest.shared.js";

export default createVitestConfig({
  "effect/unstable/rpc": resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist/unstable/rpc",
  ),
  "effect/unstable/http": resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist/unstable/http",
  ),
  effect: resolveTestPath(
    import.meta.url,
    "../effect-grpc/node_modules/effect/dist",
  ),
});
