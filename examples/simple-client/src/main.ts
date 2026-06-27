import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Effect, Layer, Stream } from "effect";
import { CliError, Command, Flag } from "effect/unstable/cli";

import { GrpcClientProtocol } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const clientLayer = (baseUrl: URL) =>
  UserServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        registry: UserServiceGrpcRegistry,
      }),
    ),
  );

const withClient = <A, E, R>(
  baseUrl: URL,
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E, Exclude<R, UserServiceClient>> =>
  effect.pipe(Effect.provide(clientLayer(baseUrl)));

const reportError = (error: {
  readonly _tag?: string;
  readonly message: string;
}) =>
  Console.log(
    error._tag === "GrpcStatusError"
      ? `error: ${"code" in error ? error.code : "unknown"} ${error.message}`
      : `error: unknown ${error.message}`,
  );

const getUser = (baseUrl: URL, id: string) =>
  withClient(
    baseUrl,
    Effect.gen(function* () {
      const client = yield* UserServiceClient;

      yield* client.getUser({ id }).pipe(
        Effect.matchEffect({
          onFailure: reportError,
          onSuccess: ({ user }) =>
            user === undefined
              ? Console.error(
                  "error: internal missing user in get-user response",
                )
              : Console.log(`user: ${user.id} ${user.name}`),
        }),
      );
    }),
  );

const watchUsers = (baseUrl: URL, tenantId: string, count: number) =>
  withClient(
    baseUrl,
    Effect.gen(function* () {
      const client = yield* UserServiceClient;

      yield* client.watchUsers({ tenantId, count }).pipe(
        Stream.runForEach((event) =>
          Console.log(
            `${event.sequence}: ${event.id} ${event.name} ${event.action}`,
          ),
        ),
        Effect.matchEffect({
          onFailure: reportError,
          onSuccess: () => Effect.void,
        }),
      );
    }),
  );

const baseUrl = Flag.string("base-url").pipe(
  Flag.mapTryCatch(
    (value) => new URL(value),
    (error) =>
      `Invalid URL: ${error instanceof Error ? error.message : String(error)}`,
  ),
  Flag.withDefault(new URL("http://127.0.0.1:50051")),
  Flag.withDescription("gRPC server URL"),
);

const simpleClient = Command.make("effect-grpc-simple-client").pipe(
  Command.withSharedFlags({ baseUrl }),
  Command.withHandler(({ baseUrl }) => getUser(baseUrl, "123")),
  Command.withDescription("Call the effect-grpc simple demo service"),
);

const getUserCommand = Command.make(
  "get-user",
  {
    id: Flag.string("id").pipe(
      Flag.withDefault("123"),
      Flag.withDescription("user id"),
    ),
  },
  ({ id }) =>
    Effect.gen(function* () {
      const { baseUrl } = yield* simpleClient;
      yield* getUser(baseUrl, id);
    }),
).pipe(Command.withDescription("Fetch one user"));

const watchUsersCommand = Command.make(
  "watch-users",
  {
    tenantId: Flag.string("tenant-id").pipe(
      Flag.withDefault("demo"),
      Flag.withDescription("tenant id"),
    ),
    count: Flag.integer("count").pipe(
      Flag.withDefault(3),
      Flag.withDescription("number of events to request"),
    ),
  },
  ({ tenantId, count }) =>
    Effect.gen(function* () {
      const { baseUrl } = yield* simpleClient;
      yield* watchUsers(baseUrl, tenantId, count);
    }),
).pipe(Command.withDescription("Stream user events"));

const setFailureExitCode = Effect.sync(() => {
  process.exitCode = 1;
});

simpleClient.pipe(
  Command.withSubcommands([getUserCommand, watchUsersCommand]),
  Command.run({ version: "0.0.0" }),
  Effect.catch((error) =>
    CliError.isCliError(error) ? setFailureExitCode : Effect.fail(error),
  ),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
