import { Command, HelpDoc, Options, ValidationError } from "@effect/cli";
import { NodeContext, NodeRuntime } from "@effect/platform-node";
import { Console, Effect, Layer, Stream } from "effect";

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

const baseUrl = Options.text("base-url").pipe(
  Options.mapTryCatch(
    (value) => new URL(value),
    (error) =>
      HelpDoc.p(
        `Invalid URL: ${error instanceof Error ? error.message : String(error)}`,
      ),
  ),
  Options.withDefault(new URL("http://127.0.0.1:50051")),
  Options.withDescription("gRPC server URL"),
);

const simpleClient = Command.make(
  "effect-grpc-simple-client",
  { baseUrl },
  ({ baseUrl }) => getUser(baseUrl, "123"),
).pipe(Command.withDescription("Call the effect-grpc simple demo service"));

const getUserCommand = Command.make(
  "get-user",
  {
    id: Options.text("id").pipe(
      Options.withDefault("123"),
      Options.withDescription("user id"),
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
    tenantId: Options.text("tenant-id").pipe(
      Options.withDefault("demo"),
      Options.withDescription("tenant id"),
    ),
    count: Options.integer("count").pipe(
      Options.withDefault(3),
      Options.withDescription("number of events to request"),
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

const cli = Command.run(
  simpleClient.pipe(
    Command.withSubcommands([getUserCommand, watchUsersCommand]),
  ),
  { name: "effect-grpc-simple-client", version: "0.0.0" },
);

cli(process.argv).pipe(
  Effect.catchIf(ValidationError.isValidationError, () => setFailureExitCode),
  Effect.provide(NodeContext.layer),
  NodeRuntime.runMain,
);
