---
"@effect-grpc/protoc-gen-effect-grpc": patch
---

Fix generated converters for messages with no fields. Empty messages now emit
`_message`/`_value` parameters and omit the dead `const message = value as …`
local, matching the well-known `Empty` handling. Previously the non-underscore
forms were always emitted, tripping `noUnusedParameters`/`noUnusedLocals` in
consumers with stricter tsconfigs.
