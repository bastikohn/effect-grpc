---
"@effect-grpc/effect-grpc": minor
---

Add `GrpcWebClient.layer` for generated browser clients using official Connect and gRPC-Web transports. Unary and server-streaming calls support metadata, authentication, deadlines, and cancellation. Request-streaming calls fail with `unimplemented` before acquiring their input. Native Node capabilities remain unchanged. Document browser CORS setup and verify both protocols in real Chromium.
