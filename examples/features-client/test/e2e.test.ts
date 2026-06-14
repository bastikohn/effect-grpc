import * as net from "node:net";

import { createClient } from "@connectrpc/connect";
import { createGrpcTransport } from "@connectrpc/connect-node";
import { Duration, Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { GrpcClientProtocol, GrpcNodeServer } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlersLayer,
  FeatureShowcaseServiceRpcGroup,
  type FeatureRequest,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc";
import { FeatureShowcaseService } from "@effect-grpc/features-proto/generated/features/v1/showcase_pb";

const featureRequest = (): FeatureRequest => ({
  tags: ["alpha", "beta"],
  scores: [10, 20],
  notes: [{ text: "generated feature demo" }],
  state: 1,
  owner: { id: "user-1", name: "Ada" },
  labels: { env: "demo" },
  counts: { attempts: 1 },
  reviewers: { primary: { id: "reviewer-1", role: "owner" } },
  createdAt: new Date(1_500),
  ttl: Duration.nanos(2_250_000_001n),
  payload: new Uint8Array([1, 2, 3]),
  sequence: 42n,
  contact: { case: "contactUser", value: { id: "user-1", role: "owner" } },
});

const implementation: FeatureShowcaseServiceImplementation = {
  describe: (request) =>
    Effect.succeed({
      request,
      summary: summary(request),
    }),
};

const userImplementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: {
        id: request.id,
        name: "Secondary User",
      },
    }),
  watchUsers: (request) =>
    Stream.make({
      id: request.tenantId,
      name: "Secondary User",
      action: "created",
      sequence: 1,
    }),
};

describe("features demo e2e", () => {
  it("round-trips the supported feature matrix through the Effect client", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* FeatureShowcaseServiceClient;
          return yield* client.describe(featureRequest());
        }).pipe(Effect.provide(clientLayer(baseUrl))),
      ),
    );

    expect(response.summary).toBe(
      "owner=Ada tags=2 notes=1 labels=1 payload=3 sequence=42 contact=contactUser",
    );
    expectRuntimeRequest(response.request);
  });

  it("serves native gRPC calls from a non-Effect connect client", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.promise(async () => {
          const client = createClient(
            FeatureShowcaseService,
            createGrpcTransport({
              baseUrl: baseUrl.toString().replace(/\/$/, ""),
            }),
          );

          return client.describe({
            tags: ["alpha"],
            scores: [7],
            notes: [{ text: "direct gRPC" }],
            state: 99 as never,
            owner: { id: "user-2", name: "Grace" },
            labels: { env: "interop" },
            counts: { attempts: 2 },
            reviewers: { secondary: { id: "reviewer-2", role: "reviewer" } },
            createdAt: { seconds: 1n, nanos: 500_000_000 },
            ttl: { seconds: 2n, nanos: 250_000_001 },
            payload: new Uint8Array([4, 5, 6]),
            sequence: 99n,
            contact: { case: "contactEmail", value: "grace@example.com" },
          });
        }),
      ),
    );

    expect(response.summary).toBe(
      "owner=Grace tags=1 notes=1 labels=1 payload=3 sequence=99 contact=contactEmail",
    );
    expect(response.request?.state).toBe(99);
    expect(response.request?.createdAt).toMatchObject({
      seconds: 1n,
      nanos: 500_000_000,
    });
    expect(response.request?.ttl).toMatchObject({
      seconds: 2n,
      nanos: 250_000_001,
    });
    expect(response.request?.payload).toEqual(new Uint8Array([4, 5, 6]));
    expect(response.request?.sequence).toBe(99n);
    expect(response.request?.contact).toEqual({
      case: "contactEmail",
      value: "grace@example.com",
    });
  });

  it("routes requests to generated services after the first service", async () => {
    const response = await Effect.runPromise(
      withServer((baseUrl) =>
        Effect.gen(function* () {
          const client = yield* UserServiceClient;
          return yield* client.getUser({ id: "secondary" });
        }).pipe(Effect.provide(userClientLayer(baseUrl))),
      ),
    );

    expect(response.user).toEqual({
      id: "secondary",
      name: "Secondary User",
    });
  });
});

const summary = (request: FeatureRequest) =>
  [
    `owner=${request.owner?.name ?? "unknown"}`,
    `tags=${request.tags.length}`,
    `notes=${request.notes.length}`,
    `labels=${Object.keys(request.labels).length}`,
    `payload=${request.payload.length}`,
    `sequence=${request.sequence}`,
    `contact=${request.contact.case ?? "none"}`,
  ].join(" ");

const expectRuntimeRequest = (request: FeatureRequest | undefined) => {
  expect(request).toBeDefined();
  expect(request).toMatchObject({
    tags: ["alpha", "beta"],
    scores: [10, 20],
    notes: [{ text: "generated feature demo" }],
    state: 1,
    owner: { id: "user-1", name: "Ada" },
    labels: { env: "demo" },
    counts: { attempts: 1 },
    reviewers: { primary: { id: "reviewer-1", role: "owner" } },
    sequence: 42n,
    contact: { case: "contactUser", value: { id: "user-1", role: "owner" } },
  });
  expect(request?.createdAt?.getTime()).toBe(1_500);
  expect(Duration.toNanosUnsafe(request!.ttl!)).toBe(2_250_000_001n);
  expect(request?.payload).toEqual(new Uint8Array([1, 2, 3]));
};

const withServer = <A, E, R>(
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        services: [
          {
            group: FeatureShowcaseServiceRpcGroup,
            registry: FeatureShowcaseServiceGrpcRegistry,
            handlers: FeatureShowcaseServiceHandlersLayer(implementation),
          },
          {
            group: UserServiceRpcGroup,
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlersLayer(userImplementation),
          },
        ],
      }).pipe(Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      return yield* use(new URL(`http://127.0.0.1:${port}`));
    }),
  );

const clientLayer = (baseUrl: URL) =>
  FeatureShowcaseServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl,
        defaultTimeoutMs: 1_000,
        registry: FeatureShowcaseServiceGrpcRegistry,
      }),
    ),
  );

const userClientLayer = (baseUrl: URL) =>
  UserServiceClientLayer.pipe(
    Layer.provide(
      GrpcClientProtocol.layer({
        baseUrl,
        defaultTimeoutMs: 1_000,
        registry: UserServiceGrpcRegistry,
      }),
    ),
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
