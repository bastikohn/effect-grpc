import { NodeRuntime } from "@effect/platform-node";
import { Effect, Stream } from "effect";

import { GrpcNodeServer, GrpcStatusError } from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const getArg = (name: string, fallback: string): string => {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1]
    ? process.argv[index + 1]!
    : fallback;
};

const host = getArg("host", "127.0.0.1");
const port = Number(getArg("port", "50051"));

const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.logInfo(`getUser ${request.id}`).pipe(
      Effect.andThen(
        request.id === "missing"
          ? Effect.fail(
              GrpcStatusError.notFound(`User not found: ${request.id}`),
            )
          : Effect.succeed({
              user: {
                id: request.id,
                name: `User ${request.id}`,
              },
            }),
      ),
    ),

  watchUsers: (request) => {
    const count = request.count <= 0 ? 5 : request.count;
    return Stream.range(1, count).pipe(
      Stream.map((sequence) => ({
        id: `${request.tenantId}-${sequence}`,
        name: `User ${sequence}`,
        action: sequence % 2 === 0 ? "updated" : "created",
        sequence,
      })),
      Stream.tap((event) => Effect.logInfo(`sending event ${event.sequence}`)),
    );
  },
};

const program = Effect.scoped(
  GrpcNodeServer.serveAll({
    host,
    port,
    services: [
      {
        group: UserServiceRpcGroup,
        registry: UserServiceGrpcRegistry,
        handlers: UserServiceHandlersLayer(implementation),
      },
    ],
  }),
);

NodeRuntime.runMain(program);
