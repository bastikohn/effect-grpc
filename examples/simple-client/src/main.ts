import { NodeRuntime } from "@effect/platform-node";
import { Effect, Layer, Stream } from "effect";

import { GrpcClientProtocol } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const args = process.argv.slice(2);
if (args[0] === "--") {
  args.shift();
}

const getArg = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 && args[index + 1] ? args[index + 1]! : fallback;
};

const command = args[0] ?? "get-user";
const baseUrl = new URL(getArg("base-url", "http://127.0.0.1:50051"));

const ClientProtocolLive = GrpcClientProtocol.layer({
  baseUrl,
  registry: UserServiceGrpcRegistry,
});

const MainLive = UserServiceClientLayer.pipe(Layer.provide(ClientProtocolLive));

const program = Effect.gen(function* () {
  const client = yield* UserServiceClient;

  switch (command) {
    case "get-user": {
      const id = getArg("id", "123");
      const result = yield* client.getUser({ id }).pipe(
        Effect.match({
          onFailure: (error) => ({ ok: false as const, error }),
          onSuccess: (response) => ({ ok: true as const, response }),
        }),
      );

      if (result.ok) {
        if (!result.response.user) {
          console.log("error: internal missing user in get-user response");
          return;
        }
        console.log(
          `user: ${result.response.user.id} ${result.response.user.name}`,
        );
      } else if (result.error._tag === "GrpcStatusError") {
        console.log(`error: ${result.error.code} ${result.error.message}`);
      } else {
        console.log(`error: unknown ${result.error.message}`);
      }
      return;
    }

    case "watch-users": {
      const tenantId = getArg("tenant-id", "demo");
      const count = Number(getArg("count", "3"));

      yield* client.watchUsers({ tenantId, count }).pipe(
        Stream.runForEach((event) =>
          Effect.sync(() => {
            console.log(
              `${event.sequence}: ${event.id} ${event.name} ${event.action}`,
            );
          }),
        ),
        Effect.matchEffect({
          onFailure: (error) =>
            Effect.sync(() => {
              if (error._tag === "GrpcStatusError") {
                console.log(`error: ${error.code} ${error.message}`);
              } else {
                console.log(`error: unknown ${error.message}`);
              }
            }),
          onSuccess: () => Effect.void,
        }),
      );
      return;
    }

    default:
      console.error(`Unknown command: ${command}`);
      process.exitCode = 1;
  }
});

NodeRuntime.runMain(program.pipe(Effect.provide(MainLive)));
