---
"@effect-grpc/protoc-gen-effect-grpc": patch
---

Stop emitting unused bare `type` aliases in the cross-package import block.
The import block previously always emitted `type <Message>` and `type <Enum>`
for every imported name, but generated code only references the bare alias for
enums used in a field position (from-converter `as <Enum>` casts) and for
messages used as a method input/output (client/server signatures) — field-only
imported messages are reached exclusively through their `Schema`/`from`/`to`
symbols. The dead alias tripped `TS6133` in consumers compiling generated
output with `noUnusedLocals`. The `fileUsage` analysis now records which
imported bare types are actually referenced (`usedImportedTypes`) and the
import block gates each alias on it; the generated-output typecheck fixtures
and the proto example packages now compile with `noUnusedLocals` to lock the
unused-emission defect class out.
