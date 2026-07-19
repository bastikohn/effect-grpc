import * as net from "node:net";
import { fromBinary } from "@bufbuild/protobuf";
import { base64Decode } from "@bufbuild/protobuf/wire";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import {
  GrpcClientProtocol,
  GrpcHealth,
  GrpcMethodRegistry,
  GrpcNodeServer,
  GrpcReflection,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: { id: request.id, name: `User ${request.id}` },
    }),
  watchUsers: () => Stream.empty,
};

const decodeFileName = (base64: string): string =>
  fromBinary(FileDescriptorProtoSchema, base64Decode(base64)).name;

describe("grpc.reflection.v1 e2e", () => {
  it("lists every served service over the wire", async () => {
    const response = await Effect.runPromise(
      withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* firstResponse(
            client.serverReflectionInfo(
              Stream.make({ host: "localhost", listServices: "*" }),
            ),
          );
        }),
      ),
    );

    expect(response.validHost).toBe("localhost");
    expect(
      response.listServicesResponse?.service.map((entry) => entry.name),
    ).toEqual([
      "demo.v1.UserService",
      "grpc.health.v1.Health",
      "grpc.reflection.v1.ServerReflection",
      "grpc.reflection.v1alpha.ServerReflection",
    ]);
  });

  it("serves the descriptors of a generated service", async () => {
    const response = await Effect.runPromise(
      withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* firstResponse(
            client.serverReflectionInfo(
              Stream.make({
                host: "",
                fileContainingSymbol: "demo.v1.UserService",
              }),
            ),
          );
        }),
      ),
    );

    const files = response.fileDescriptorResponse?.fileDescriptorProto ?? [];
    expect(files.map(decodeFileName)).toEqual(["demo/v1/user_service.proto"]);
  });

  it("answers unknown symbols in-band and keeps the stream alive", async () => {
    const responses = await Effect.runPromise(
      withReflectionServer(
        Effect.gen(function* () {
          const client = yield* GrpcReflection.ReflectionClient;
          return yield* client
            .serverReflectionInfo(
              Stream.make(
                { host: "", fileContainingSymbol: "demo.v1.Missing" },
                { host: "", fileByFilename: "demo/v1/user_service.proto" },
              ),
            )
            .pipe(Stream.take(2), Stream.runCollect);
        }),
      ),
    );

    expect(responses).toHaveLength(2);
    expect(responses[0]?.errorResponse).toMatchObject({ errorCode: 5 });
    expect(responses[0]?.originalRequest).toMatchObject({
      fileContainingSymbol: "demo.v1.Missing",
    });
    expect(
      responses[1]?.fileDescriptorResponse?.fileDescriptorProto.map(
        decodeFileName,
      ),
    ).toEqual(["demo/v1/user_service.proto"]);
  });

  it("serves the identical protocol under the v1alpha alias", async () => {
    const response = await Effect.runPromise(
      withReflectionServer(
        Effect.gen(function* () {
          const streaming = yield* GrpcClientProtocol.GrpcStreamingClient;
          const responses = yield* streaming
            .bidiStreaming(
              GrpcReflection.ReflectionV1AlphaTag,
              Stream.make({ host: "", listServices: "" }),
            )
            .pipe(Stream.take(1), Stream.runCollect);
          return responses[0] as GrpcReflection.ServerReflectionResponse;
        }),
      ),
    );

    expect(
      response.listServicesResponse?.service.map((entry) => entry.name),
    ).toContain("grpc.reflection.v1alpha.ServerReflection");
  });
});

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
    GrpcReflection.ReflectionClient | GrpcClientProtocol.GrpcStreamingClient
  >,
): Effect.Effect<A, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      const services = [
        {
          group: UserServiceRpcGroup,
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlersLayer(implementation),
        },
        GrpcHealth.service,
      ] as const;

      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [...services, GrpcReflection.service(services)],
      }).pipe(Effect.provide(GrpcHealth.layer()), Effect.forkScoped);
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

const freePort = Effect.promise(
  () =>
    new Promise<number>((resolve, reject) => {
      const server = net.createServer();
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === "object") {
            resolve(address.port);
          } else {
            reject(new Error("Unable to allocate a local port"));
          }
        });
      });
    }),
);
