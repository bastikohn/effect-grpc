import { NodeRuntime, NodeServices } from "@effect/platform-node";
import { Console, Duration, Effect, Layer, Stream } from "effect";
import { CliError, Command, Flag } from "effect/unstable/cli";

import {
  GrpcClientProtocol,
  GrpcMethodRegistry,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";

// One transport serves both demo services: the generated client layers share
// the `GrpcInvoker` built from the merged registry.
const clientLayer = (baseUrl: URL) =>
  Layer.mergeAll(
    UserServiceClientLayer,
    FeatureShowcaseServiceClientLayer,
  ).pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl: baseUrl.toString().replace(/\/$/, ""),
        registry: GrpcMethodRegistry.merge([
          UserServiceGrpcRegistry,
          FeatureShowcaseServiceGrpcRegistry,
        ]),
      }),
    ),
  );

const withClient = <A, E, R>(baseUrl: URL, effect: Effect.Effect<A, E, R>) =>
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

const getUser = Effect.fn("getUser")(function* (id: string) {
  const client = yield* UserServiceClient;

  yield* client.getUser({ id }).pipe(
    Effect.matchEffect({
      onFailure: reportError,
      onSuccess: ({ user }) =>
        user === undefined
          ? Console.error("error: internal missing user in get-user response")
          : Console.log(`user: ${user.id} ${user.name}`),
    }),
  );
});

const watchUsers = Effect.fn("watchUsers")(function* (
  tenantId: string,
  count: number,
) {
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
});

// The feature showcase service on the same server: every supported field shape
// in one request.
const describeFeatures = Effect.fn("describeFeatures")(function* () {
  const client = yield* FeatureShowcaseServiceClient;

  yield* client
    .describe({
      tags: ["alpha", "beta"],
      scores: [10, 20],
      notes: [{ text: "generated feature demo" }],
      state: 1,
      owner: { id: "user-1", name: "Ada" },
      labels: { env: "demo" },
      counts: { attempts: 1 },
      reviewers: { primary: { id: "reviewer-1", role: "owner" } },
      createdAt: new Date(0),
      ttl: Duration.seconds(30),
      payload: new Uint8Array([1, 2, 3]),
      sequence: 42n,
      contact: { case: "contactEmail", value: "ada@example.com" },
    })
    .pipe(
      Effect.matchEffect({
        onFailure: reportError,
        onSuccess: (response) => Console.log(response.summary),
      }),
    );
});

const baseUrl = Flag.String("base-url").pipe(
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
  Command.withHandler(({ baseUrl }) => withClient(baseUrl, getUser("123"))),
  Command.withDescription("Call the effect-grpc simple demo service"),
);

const getUserCommand = Command.make(
  "get-user",
  {
    id: Flag.String("id").pipe(
      Flag.withDefault("123"),
      Flag.withDescription("user id"),
    ),
  },
  ({ id }) =>
    Effect.flatMap(simpleClient, ({ baseUrl }) =>
      withClient(baseUrl, getUser(id)),
    ),
).pipe(Command.withDescription("Fetch one user"));

const watchUsersCommand = Command.make(
  "watch-users",
  {
    tenantId: Flag.String("tenant-id").pipe(
      Flag.withDefault("demo"),
      Flag.withDescription("tenant id"),
    ),
    count: Flag.Int("count").pipe(
      Flag.withDefault(3),
      Flag.withDescription("number of events to request"),
    ),
  },
  ({ tenantId, count }) =>
    Effect.flatMap(simpleClient, ({ baseUrl }) =>
      withClient(baseUrl, watchUsers(tenantId, count)),
    ),
).pipe(Command.withDescription("Stream user events"));

const describeFeaturesCommand = Command.make("describe-features", {}, () =>
  Effect.flatMap(simpleClient, ({ baseUrl }) =>
    withClient(baseUrl, describeFeatures()),
  ),
).pipe(Command.withDescription("Round-trip the feature showcase request"));

const setFailureExitCode = Effect.sync(() => {
  process.exitCode = 1;
});

simpleClient.pipe(
  Command.withSubcommands([
    getUserCommand,
    watchUsersCommand,
    describeFeaturesCommand,
  ]),
  Command.run({ version: "0.0.0" }),
  Effect.catch((error) =>
    CliError.isCliError(error) ? setFailureExitCode : Effect.fail(error),
  ),
  Effect.provide(NodeServices.layer),
  NodeRuntime.runMain,
);
