---
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: the generator emits through protoplugin's `GeneratedFile`, so import
statements are derived from what is actually printed instead of a
hand-maintained prediction. Regenerate your protos. Three visible changes:

- `import_extension` now follows protoplugin's standard parsed option and its
  default of `none` (extensionless relative imports), matching
  `protoc-gen-es`; the plugin no longer forces `.js`. Node ESM consumers
  should pass `import_extension=js` in `buf.gen.yaml` — every documented
  recipe already does.
- Generated file headers change format: protoplugin's standard preamble
  (plugin name and version, the sanitized parameter string, and the source
  proto file) replaces the previous one-line `DO NOT EDIT` header. The
  version in the header is read from the plugin's own `package.json`, so it
  always matches the installed release — expect the header line to change
  when you regenerate after upgrading.
- Import statements collapse to one line per source, type-only imports split
  into `import type` statements, and imported names that collide with a local
  declaration are aliased (`User$1`). The aliasing fixes a real bug: importing
  a same-named message from another proto package previously emitted a file
  that failed to compile with duplicate identifiers. Well-known JSON schema
  references now use their plain `@bufbuild/protobuf/wkt` export names
  (`StructSchema` instead of a local `ProtobufStructSchema` alias) and JSON
  conversions call `toJson`/`fromJson`.
