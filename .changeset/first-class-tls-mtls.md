---
"@effect-grpc/effect-grpc": minor
---

Add first-class TLS/mTLS support to the server and client.

`GrpcNodeServer.serve`/`serveAll` accept a `tls` option (`key`, `cert`, both
PEM) and terminate TLS via `http2.createSecureServer`. Setting `clientCa`
enables mutual TLS: the handshake requires a client certificate signed by that
CA and rejects connections without one.

`GrpcClientProtocol.layer`/`makeTransport` accept a `tls` option that merges
into connect-node's `nodeOptions`: `ca` sets the trust anchor for private CAs,
`cert`/`key` present a client certificate for mTLS, and
`rejectUnauthorized: false` disables server verification for development.
`tls` requires an `https://` `baseUrl` and `cert`/`key` must be passed
together — violations fail fast with a clear error. The raw `nodeOptions`
escape hatch keeps working; `tls` wins for the keys it sets.

TLS handshake failures surface to callers as `GrpcStatusError` with code
`internal`, following connect-node's error mapping.
