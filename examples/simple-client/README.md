# simple-client

Demo native gRPC client.

```sh
pnpm demo:client -- get-user --id 123
pnpm demo:client -- get-user --id missing
pnpm demo:client -- watch-users --tenant-id demo --count 3
```

Defaults to `http://127.0.0.1:50051`. Override with `--base-url`.
