import type { ConnectRouter, Interceptor } from "@connectrpc/connect";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "tstyche";

import {
  CodegenSupport,
  GrpcAuth,
  GrpcClientProtocol,
  GrpcHealth,
  GrpcMetadata,
  GrpcMethodRegistry,
  GrpcNodeServer,
  GrpcReflection,
  GrpcServerProtocol,
  GrpcStatusError,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const registry = new Map() as GrpcMethodRegistry.GrpcMethodRegistry;
declare const context: CodegenSupport.GrpcServerContext;
declare const metadata: GrpcMetadata.GrpcMetadata;
interface AuthToken {
  readonly token: string;
}
declare const authMetadata: Effect.Effect<
  GrpcMetadata.GrpcMetadata,
  never,
  AuthToken
>;
const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: {
        id: request.id,
        name: "Demo User",
      },
    }),
  watchUsers: (request) =>
    Stream.make({
      id: request.tenantId,
      name: "Demo User",
      action: "created",
      sequence: 1,
    }),
};

describe("public API", () => {
  it("keeps runtime constructors callable", () => {
    expect(GrpcStatusError.notFound).type.toBeCallableWith("missing");
    expect(GrpcClientProtocol.layer).type.toBeCallableWith({
      baseUrl: "http://127.0.0.1:50051",
      registry,
    });
    expect(GrpcClientProtocol.metadataInterceptor).type.toBeCallableWith(
      Effect.succeed(metadata),
    );
    expect(GrpcClientProtocol.metadataInterceptor(authMetadata)).type.toBe<
      Effect.Effect<Interceptor, never, AuthToken>
    >();
    expect(GrpcServerProtocol.make).type.toBeCallableWith({ registry });
    expect(GrpcAuth.bearerMetadata).type.toBeCallableWith("token");
    expect(GrpcAuth.bearerInterceptor).type.toBe<
      Effect.Effect<Interceptor, never, GrpcAuth.BearerToken>
    >();
    expect(GrpcAuth.staticTokenLayer("token")).type.toBe<
      Layer.Layer<GrpcAuth.BearerToken>
    >();
    expect(GrpcAuth.refreshingTokenLayer).type.toBeCallableWith({
      acquire: Effect.succeed("token"),
      refresh: (current: string) => Effect.succeed(current),
      interval: "1 hour",
    });
    expect(GrpcClientProtocol.layer).type.toBeCallableWith({
      baseUrl: "https://127.0.0.1:50051",
      registry,
      tls: {
        ca: "PEM",
        cert: "PEM",
        key: "PEM",
        rejectUnauthorized: false,
      },
    });
    expect(GrpcNodeServer.serve).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      routes: (router: ConnectRouter) => router,
    });
    expect(GrpcNodeServer.serve).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      routes: (router: ConnectRouter) => router,
      tls: { key: "PEM", cert: "PEM", clientCa: "PEM" },
    });
    expect(GrpcNodeServer.serveAll).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      tls: { key: "PEM", cert: "PEM" },
      services: [
        {
          group: UserServiceRpcGroup,
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlersLayer(implementation),
        },
      ],
    });
    expect(GrpcNodeServer.serveAll).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      services: [
        {
          group: UserServiceRpcGroup,
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlersLayer(implementation),
        },
        GrpcHealth.service,
      ],
    });
    expect(GrpcHealth.layer()).type.toBe<Layer.Layer<GrpcHealth.GrpcHealth>>();
    expect(GrpcHealth.make).type.toBeCallableWith({
      initialStatuses: [["", "SERVING"]],
    });
  });

  it("types the health service and client", () => {
    Effect.gen(function* () {
      const health = yield* GrpcHealth.GrpcHealth;
      yield* health.set("demo.v1.UserService", "SERVING");
      const status = yield* health.check("demo.v1.UserService");

      expect(status).type.toBe<GrpcHealth.ServingStatus>();
      expect(health.watch("demo.v1.UserService")).type.toBe<
        Stream.Stream<GrpcHealth.ServingStatus>
      >();
    }).pipe(Effect.provide(GrpcHealth.layer()));

    Effect.gen(function* () {
      const client = yield* GrpcHealth.HealthClient;
      const response = yield* client.check({ service: "" });
      const watched = client.watch({ service: "" });

      expect(response).type.toHaveProperty("status");
      expect(watched).type.toBeAssignableTo<Stream.Stream<unknown, unknown>>();
    });
  });

  it("types the reflection service and client", () => {
    const services = [
      {
        group: UserServiceRpcGroup,
        registry: UserServiceGrpcRegistry,
        handlers: UserServiceHandlersLayer(implementation),
      },
      GrpcHealth.service,
    ] as const;
    expect(GrpcNodeServer.serveAll).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      services: [...services, GrpcReflection.service(services)],
    });

    const index = GrpcReflection.makeIndex([UserServiceGrpcRegistry]);
    const response = GrpcReflection.respond(index, {
      host: "localhost",
      listServices: "*",
    });
    expect(response).type.toBe<GrpcReflection.ServerReflectionResponse>();

    Effect.gen(function* () {
      const client = yield* GrpcReflection.ReflectionClient;
      const responses = client.serverReflectionInfo(
        Stream.make({ host: "", fileContainingSymbol: "demo.v1.UserService" }),
      );

      expect(responses).type.toBe<
        Stream.Stream<
          GrpcReflection.ServerReflectionResponse,
          GrpcReflection.ReflectionClientError
        >
      >();
    });
  });

  it("types generated clients and handlers", () => {
    const layer = UserServiceClientLayer.pipe(
      Layer.provide(
        GrpcClientProtocol.layer({
          baseUrl: "http://127.0.0.1:50051",
          registry,
        }),
      ),
    );

    Effect.gen(function* () {
      const client = yield* UserServiceClient;
      const user = yield* client.getUser({ id: "123" });
      const events = client.watchUsers({ tenantId: "demo", count: 1 });

      expect(user).type.toHaveProperty("user");
      expect(events).type.toBeAssignableTo<Stream.Stream<unknown, unknown>>();
    }).pipe(Effect.provide(layer));

    expect(implementation.getUser).type.toBeCallableWith(
      { id: "123" },
      context,
    );
  });
});
