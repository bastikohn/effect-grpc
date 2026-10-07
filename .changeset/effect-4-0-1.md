---
"@effect-grpc/effect-grpc": patch
---

Target stable `effect@^4.0.1` (from `4.0.0-rc.115`). The `effect` peer
dependency is now a caret range instead of an exact prerelease pin, so
consumers need to upgrade `effect` and `@effect/platform-node` to 4.0.1 or
later. Effect 4.0.1 dropped the `unstable/` path segment
(`effect/unstable/http` is now `effect/http`); consumer imports of those
modules need the same rename.
