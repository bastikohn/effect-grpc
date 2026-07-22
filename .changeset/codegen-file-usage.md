---
"@effect-grpc/protoc-gen-effect-grpc": patch
---

Derive generated-file usage once in a single analysis (`fileUsage.ts`) that renderers consume instead of re-scanning the model — imports, helpers, method partitions, well-known usage, boxed wrappers, recursive edges, and empty-message facts now have one implementation. Fixes two unused-emission defects that could fail consumers compiling with `noUnusedLocals`: unary-only service files no longer import `Stream`, and files whose messages are all empty no longer emit the unused `readField`/`compact` helpers (empty-message converters now return `{}` directly).
