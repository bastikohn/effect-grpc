---
"@effect-grpc/effect-grpc": minor
"@effect-grpc/protoc-gen-effect-grpc": minor
---

Add a portable `/client` entrypoint with transport-independent `GrpcClient` helpers. Generated modules use the portable entrypoint and protobuf base64 codecs instead of Node Buffer. Existing Node transport and server exports remain available. Update both packages and regenerate clients to adopt the browser-safe dependency graph.
