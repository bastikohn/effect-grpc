import type { ConnectRouter } from "@connectrpc/connect";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "tstyche";

import {
  CodegenSupport,
  GrpcClientProtocol,
  GrpcMethodRegistry,
  GrpcNodeServer,
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
      baseUrl: new URL("http://127.0.0.1:50051"),
      registry,
    });
    expect(GrpcServerProtocol.make).type.toBeCallableWith({ registry });
    expect(GrpcNodeServer.serve).type.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      routes: (router: ConnectRouter) => router,
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
      ],
    });
  });

  it("types generated clients and handlers", () => {
    const layer = UserServiceClientLayer.pipe(
      Layer.provide(
        GrpcClientProtocol.layer({
          baseUrl: new URL("http://127.0.0.1:50051"),
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
