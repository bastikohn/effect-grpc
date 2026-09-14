import { fromBinary } from "@bufbuild/protobuf";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Layer, Stream } from "effect";

import {
  GrpcClientProtocol,
  GrpcHealth,
  GrpcInvoker,
  GrpcMethodRegistry,
  GrpcNodeServer,
  GrpcReflection,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

import { freePort } from "./support.ts";

const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: { id: request.id, name: `User ${request.id}` },
    }),
  watchUsers: () => Stream.empty,
};

const decodeFileName = (bytes: Uint8Array): string =>
  fromBinary(FileDescriptorProtoSchema, bytes).name;

describe("grpc.reflection.v1 e2e", () => {
  it.live("lists every served service over the wire", () =>
    Effect.gen(function* () {
      const response = yield* withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* firstResponse(
            client.serverReflectionInfo(
              Stream.make({
                host: "localhost",
                messageRequest: { case: "listServices", value: "*" },
              }),
            ),
          );
        }),
      );

      assert.strictEqual(response.validHost, "localhost");
      assert.deepStrictEqual(listedServices(response), [
        "demo.v1.UserService",
        "grpc.health.v1.Health",
        "grpc.reflection.v1.ServerReflection",
      ]);
    }),
  );

  it.live("serves the descriptors of a generated service", () =>
    Effect.gen(function* () {
      const response = yield* withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* firstResponse(
            client.serverReflectionInfo(
              Stream.make({
                host: "",
                messageRequest: {
                  case: "fileContainingSymbol",
                  value: "demo.v1.UserService",
                },
              }),
            ),
          );
        }),
      );

      assert.deepStrictEqual(descriptorNames(response), [
        "demo/v1/user_service.proto",
      ]);
    }),
  );

  it.live("answers unknown symbols in-band and keeps the stream alive", () =>
    Effect.gen(function* () {
      const responses = yield* withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* client
            .serverReflectionInfo(
              Stream.make(
                {
                  host: "",
                  messageRequest: {
                    case: "fileContainingSymbol",
                    value: "demo.v1.Missing",
                  },
                },
                {
                  host: "",
                  messageRequest: {
                    case: "fileByFilename",
                    value: "demo/v1/user_service.proto",
                  },
                },
              ),
            )
            .pipe(Stream.take(2), Stream.runCollect);
        }),
      );

      assert.lengthOf(responses, 2);
      const first = responses[0]?.messageResponse;
      assert.strictEqual(first?.case, "errorResponse");
      if (first?.case === "errorResponse") {
        assert.strictEqual(first.value.errorCode, 5);
      }
      assert.deepStrictEqual(responses[0]?.originalRequest?.messageRequest, {
        case: "fileContainingSymbol",
        value: "demo.v1.Missing",
      });
      assert.deepStrictEqual(descriptorNames(responses[1]!), [
        "demo/v1/user_service.proto",
      ]);
    }),
  );
});

const listedServices = (
  response: GrpcReflection.ServerReflectionResponse,
): ReadonlyArray<string> =>
  response.messageResponse.case === "listServicesResponse"
    ? response.messageResponse.value.service.map((entry) => entry.name)
    : [];

const descriptorNames = (
  response: GrpcReflection.ServerReflectionResponse,
): ReadonlyArray<string> =>
  response.messageResponse.case === "fileDescriptorResponse"
    ? response.messageResponse.value.fileDescriptorProto.map(decodeFileName)
    : [];

const firstResponse = (
  responses: Stream.Stream<
    GrpcReflection.ServerReflectionResponse,
    GrpcReflection.ReflectionClientError
  >,
) =>
  Stream.take(responses, 1).pipe(
    Stream.runCollect,
    Effect.map((collected) => {
      const response = collected[0];
      if (response === undefined) {
        throw new Error("Expected a reflection response");
      }
      return response;
    }),
  );

const clientRegistry: GrpcMethodRegistry.GrpcMethodRegistry = new Map([
  ...UserServiceGrpcRegistry,
  ...GrpcHealth.HealthGrpcRegistry,
  ...GrpcReflection.ReflectionGrpcRegistry,
]);

const withReflectionServer = <A, E>(
  use: Effect.Effect<
    A,
    E,
    GrpcReflection.ReflectionClient | GrpcInvoker.GrpcInvoker
  >,
): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      const services = [
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers(implementation),
        },
        GrpcHealth.service,
      ] as const;

      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [...services, GrpcReflection.service(services)],
      }).pipe(Effect.provide(GrpcHealth.layer), Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      const protocol = GrpcClientProtocol.layer({
        baseUrl: `http://127.0.0.1:${port}`,
        defaultTimeoutMs: 1_000,
        registry: clientRegistry,
      });
      return yield* use.pipe(
        Effect.provide(
          Layer.mergeAll(
            GrpcReflection.ReflectionClientLayer.pipe(Layer.provide(protocol)),
            protocol,
          ),
        ),
      );
    }),
  );
