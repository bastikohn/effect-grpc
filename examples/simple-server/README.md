# simple-server

Demo native gRPC server.

```sh
pnpm build
pnpm demo:server
```

Serves `demo.v1.UserService`, `features.v1.FeatureShowcaseService` and server
reflection. Defaults to `127.0.0.1:50051`; override with `--host` and `--port`.
