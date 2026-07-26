---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Breaking: the public surface shrinks to what is actually load-bearing.

- The generator drops the inert `errors`, `int64` and `methods` options; only
  `import_extension` remains (now honoured, including `ts`). Remove the other
  `opt:` entries from `buf.gen.yaml`.
- `tracestate` is no longer propagated and `GrpcTracing` is gone; `traceparent`
  propagation and span parenting are unchanged.
- `GrpcReflection` request/response schemas use the generated `{case, value}`
  oneof representation, `fileDescriptorProto` is `Uint8Array`, and the legacy
  `grpc.reflection.v1alpha` alias is no longer registered.
- `GrpcHealth.make`/`layer` are nullary values, and `GrpcHealthService.statuses`
  plus `GrpcHealthOptions.initialStatuses` are removed.
- The `@effect-grpc/codegen` package is discontinued and will not be published
  again — it was a second front-end to this generator. `buf generate` with the
  `protoc-gen-effect-grpc` plugin is the supported path; see the README for the
  `buf.yaml`/`buf.gen.yaml` recipe.
