---
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Generated files compile cleanly under `noUnusedLocals`/`noUnusedParameters` and
can no longer be shadowed by legal proto names: base64, oneof, well-known, and
`Empty` converters moved into a `Grpc$` namespace, unused bare `type` aliases
and helper emissions are dropped, and file usage is derived once in a single
analysis the renderers consume. Regenerate to pick up the renamed converters.
