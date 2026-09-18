import { NodeRuntime } from "@effect/platform-node";
import { Effect, Stream } from "effect";

import {
  GrpcNodeServer,
  GrpcReflection,
  GrpcStatusError,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

import { featureShowcaseService } from "./featureShowcase.ts";
import {
  attachRequestId,
  CurrentRequest,
  currentRequest,
} from "./requestContext.ts";

const getArg = (name: string, fallback: string): string => {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1]
    ? process.argv[index + 1]!
    : fallback;
};

const host = getArg("host", "127.0.0.1");
const port = Number(getArg("port", "50051"));

// Handlers read the call's typed request id off the gRPC context and hand it
// to domain code as the application's own `CurrentRequest` service — provided
// per call around the domain effect/stream, so nothing else in `R` changes.
const implementation: UserServiceImplementation = {
  getUser: (request, context) =>
    Effect.gen(function* () {
      const { requestId } = yield* CurrentRequest;
      yield* Effect.logInfo(`getUser ${request.id} (request ${requestId})`);
      return yield* request.id === "missing"
        ? Effect.fail(GrpcStatusError.notFound(`User not found: ${request.id}`))
        : Effect.succeed({
            user: {
              id: request.id,
              name: `User ${request.id}`,
            },
          });
    }).pipe(Effect.provideService(CurrentRequest, currentRequest(context))),

  watchUsers: (request, context) => {
    const count = request.count <= 0 ? 5 : request.count;
    return Stream.range(1, count).pipe(
      Stream.map((sequence) => ({
        id: `${request.tenantId}-${sequence}`,
        name: `User ${sequence}`,
        action: sequence % 2 === 0 ? "updated" : "created",
        sequence,
      })),
      Stream.tap((event) =>
        Effect.gen(function* () {
          const { requestId } = yield* CurrentRequest;
          yield* Effect.logInfo(
            `sending event ${event.sequence} (request ${requestId})`,
          );
        }),
      ),
      Stream.provideService(CurrentRequest, currentRequest(context)),
    );
  },
};

const services = [
  {
    registry: UserServiceGrpcRegistry,
    handlers: UserServiceHandlers(implementation),
  },
  featureShowcaseService,
] as const;

const program = Effect.scoped(
  GrpcNodeServer.serveAll({
    host,
    port,
    // Reflection lets `grpcurl -plaintext 127.0.0.1:50051 list` and
    // `describe` work without local .proto files.
    services: [...services, GrpcReflection.service(services)],
    // Native connect interceptors run once per call, before the handler.
    interceptors: [attachRequestId],
  }),
);

NodeRuntime.runMain(program);
