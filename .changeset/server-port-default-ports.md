---
"@effect-grpc/effect-grpc": patch
---

**Client telemetry no longer drops `server.port` for default ports.** WHATWG
`URL` normalizes a scheme's default port away, so `new URL("https://api.example.com:443").port`
is the empty string — client spans and `rpc.client.call.duration` omitted the
semconv `server.port` attribute for every `https://` endpoint on 443 and every
`http://` endpoint on 80, including ones that spelled the port out explicitly.
The port now falls back to the scheme's default (443 / 80); only a
`serverAddress` override on some other scheme, which has no default to derive,
still reports `server.address` alone.
