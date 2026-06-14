# Changesets

Add a changeset for user-visible changes to published packages.

No changeset is needed for tests, docs, CI-only changes, examples-only changes,
or internal refactors that do not affect published behavior.

Only `@effect-grpc/effect-grpc` and `@effect-grpc/protoc-gen-effect-grpc` are
currently intended to be published. Keep examples and private workspace
packages ignored unless they intentionally become npm packages.
