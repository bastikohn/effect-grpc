import type { DescService } from "@bufbuild/protobuf";
import type { ConnectRouter, Interceptor } from "@connectrpc/connect";
import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "tstyche";

import {
  CodegenSupport,
  GrpcAuth,
  GrpcClientProtocol,
  GrpcHealth,
  GrpcInvoker,
  GrpcMetadata,
  GrpcMethodRegistry,
  GrpcNodeServer,
  GrpcReflection,
  GrpcServerProtocol,
  GrpcStatusError,
  type GrpcStatusCode,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  type UserServiceClientError,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

const registry = new Map() as GrpcMethodRegistry.GrpcMethodRegistry;
declare const entry: GrpcMethodRegistry.GrpcMethodEntry;
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
declare const serverHandlers: GrpcServerProtocol.GrpcHandlers;
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
    // Regression pin: the client layer provides `GrpcInvoker` alone — the
    // `RpcClient.Protocol` client path is retired.
    expect(
      GrpcClientProtocol.layer({
        baseUrl: "http://127.0.0.1:50051",
        registry,
      }),
    ).type.toBe<Layer.Layer<GrpcInvoker.GrpcInvoker>>();
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

  it("pins the status error to failure codes and one metadata channel", () => {
    expect(GrpcStatusError.make).type.toBeCallableWith({
      code: "unavailable",
      message: "down",
      metadata: [["x-demo", "1"]],
    });
    // Regression pin: `"ok"` is not a failure. An error carrying it recorded
    // success telemetry while the peer still saw the call fail as UNKNOWN.
    expect(GrpcStatusError.make).type.not.toBeCallableWith({
      code: "ok",
      message: "not a failure",
    });
    // The `"ok"`-free union consumers name in their own signatures.
    expect<
      GrpcStatusError.GrpcStatusError["code"]
    >().type.toBe<GrpcStatusCode.GrpcErrorStatusCode>();
    // Regression pin: an error has exactly one metadata channel — connect
    // writes it to the response trailers — so `trailers` is gone.
    expect(GrpcStatusError.make).type.not.toBeCallableWith({
      code: "unavailable",
      message: "down",
      trailers: [["x-demo", "1"]],
    });
    expect<GrpcStatusError.GrpcStatusError>().type.not.toHaveProperty(
      "trailers",
    );
  });

  it("pins the serveAll handlers seam and the protocol constructor", () => {
    // Regression pin: `ServeAllService.handlers` is a `GrpcHandlers` layer,
    // not `Layer<any>` — a wrong layer (e.g. the health *state* layer instead
    // of `HealthHandlersLayer`) used to typecheck and then silently answer
    // every method `unimplemented`.
    expect(GrpcNodeServer.serveAll).type.not.toBeCallableWith({
      host: "127.0.0.1",
      port: 50051,
      services: [
        {
          registry: UserServiceGrpcRegistry,
          handlers: GrpcHealth.layer(),
        },
      ],
    });
    // The retired Effect RPC wiring's `group` field is gone from the service
    // entry. (An excess `group:` property in a `serveAll` call is not
    // flagged by tsc — `const`-generic inference adopts the literal's own
    // type, so freshness-based excess-property checking never fires.)
    expect<GrpcNodeServer.ServeAllService>().type.not.toHaveProperty("group");

    // `make` accepts the unified handlers map and requires no `Scope` (or
    // anything else) to build the routes.
    expect(GrpcServerProtocol.make).type.toBeCallableWith({
      registry,
      handlers: serverHandlers,
    });
    expect(
      GrpcServerProtocol.make({ registry, handlers: serverHandlers }),
    ).type.toBe<Effect.Effect<GrpcServerProtocol.GrpcServerProtocolResult>>();
  });

  it("types the method registry contract", () => {
    expect(GrpcMethodRegistry.lookup(registry, "tag", "unary")).type.toBe<
      GrpcMethodRegistry.GrpcUnaryMethodEntry | undefined
    >();
    expect(
      GrpcMethodRegistry.lookup(registry, "tag", "bidi-streaming"),
    ).type.toBe<GrpcMethodRegistry.GrpcBidiStreamingMethodEntry | undefined>();

    expect(
      GrpcMethodRegistry.merge([registry, registry]),
    ).type.toBe<GrpcMethodRegistry.GrpcMethodRegistry>();
    expect(GrpcMethodRegistry.merge).type.toBeCallableWith(
      new Set<GrpcMethodRegistry.GrpcMethodRegistry>(),
    );

    expect(GrpcMethodRegistry.groupByService(registry)).type.toBe<
      ReadonlyMap<
        DescService,
        ReadonlyArray<GrpcMethodRegistry.GrpcMethodEntry>
      >
    >();

    expect(GrpcMethodRegistry.encodeRequest(entry, {})).type.toBe<
      Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>
    >();
    expect(GrpcMethodRegistry.decodeRequest(entry, {})).type.toBe<
      Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>
    >();
    expect(GrpcMethodRegistry.encodeResponse(entry, {})).type.toBe<
      Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>
    >();
    expect(GrpcMethodRegistry.decodeResponse(entry, {})).type.toBe<
      Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>
    >();
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
    // Regression pin: the generated handlers layer publishes the unified
    // 4-kind handler map — the Effect RPC server path
    // (`Rpc.ToHandler`/`RpcGroup`) is retired.
    expect(UserServiceHandlersLayer(implementation)).type.toBe<
      Layer.Layer<GrpcServerProtocol.GrpcHandlers>
    >();

    // Regression pin: `GrpcServerContext` is narrowed to metadata — the
    // Effect RPC `client`/`requestId` fields are gone.
    expect(context.metadata).type.toBe<GrpcMetadata.GrpcMetadata>();
    expect(context).type.not.toHaveProperty("client");
    expect(context).type.not.toHaveProperty("requestId");

    // Regression pin: the generated client layer is satisfiable by
    // `GrpcInvoker` alone — no residual `RpcClient.Protocol` requirement.
    // Widening the requirement channel (e.g. reintroducing `RpcClient.Protocol`)
    // would fail this assertion.
    expect(UserServiceClientLayer).type.toBe<
      Layer.Layer<UserServiceClient, never, GrpcInvoker.GrpcInvoker>
    >();

    // Regression pin: the generated client error is narrowed to
    // `GrpcStatusError` alone. Reintroducing `RpcClientError` into the union
    // would fail this assertion.
    expect<UserServiceClientError>().type.toBe<GrpcStatusError.GrpcStatusError>();

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
