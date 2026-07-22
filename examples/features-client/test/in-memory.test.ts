import { Effect, Layer, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { GrpcInvoker } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
} from "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc";

// The payoff of the invoker migration: generated clients depend on the
// `GrpcInvoker` seam alone, so they can be exercised end-to-end against the
// in-memory adapter — no sockets, protobuf descriptors, or HTTP/2. Handlers
// receive and return domain values. `UserService` supplies the unary and
// server-streaming shapes; `FeatureShowcaseService` the client- and
// bidi-streaming shapes, covering all four cardinalities.
const handlers: GrpcInvoker.GrpcInMemoryHandlers = {
  "demo.v1.UserService/GetUser": {
    kind: "unary",
    handler: (request) =>
      Effect.succeed({
        user: {
          id: (request as { readonly id: string }).id,
          name: "In-Memory User",
        },
      }),
  },
  "demo.v1.UserService/WatchUsers": {
    kind: "server-streaming",
    handler: (request) => {
      const tenantId = (request as { readonly tenantId: string }).tenantId;
      return Stream.make(
        {
          id: tenantId,
          name: "In-Memory User",
          action: "created",
          sequence: 1,
        },
        {
          id: tenantId,
          name: "In-Memory User",
          action: "updated",
          sequence: 2,
        },
      );
    },
  },
  "features.v1.FeatureShowcaseService/UploadNotes": {
    kind: "client-streaming",
    handler: (requests) =>
      Stream.runCollect(requests).pipe(
        Effect.map((notes) => ({
          count: notes.length,
          joined: notes
            .map((note) => (note as { readonly text: string }).text)
            .join(","),
        })),
      ),
  },
  "features.v1.FeatureShowcaseService/Chat": {
    kind: "bidi-streaming",
    handler: (requests) =>
      Stream.map(requests, (message) => {
        const chat = message as {
          readonly text: string;
          readonly sequence: number;
        };
        return { text: `echo:${chat.text}`, sequence: chat.sequence + 1 };
      }),
  },
};

const clientLayer = Layer.mergeAll(
  UserServiceClientLayer,
  FeatureShowcaseServiceClientLayer,
).pipe(Layer.provide(GrpcInvoker.layerInMemory(handlers)));

describe("generated clients over GrpcInvoker.layerInMemory", () => {
  it("round-trips a unary call", async () => {
    const response = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* UserServiceClient;
        return yield* client.getUser({ id: "123" });
      }).pipe(Effect.provide(clientLayer)),
    );

    expect(response.user).toEqual({ id: "123", name: "In-Memory User" });
  });

  it("round-trips a server-streaming call", async () => {
    const events = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* UserServiceClient;
        return yield* Stream.runCollect(
          client.watchUsers({ tenantId: "demo", count: 2 }),
        );
      }).pipe(Effect.provide(clientLayer)),
    );

    expect(events).toEqual([
      { id: "demo", name: "In-Memory User", action: "created", sequence: 1 },
      { id: "demo", name: "In-Memory User", action: "updated", sequence: 2 },
    ]);
  });

  it("round-trips a client-streaming call", async () => {
    const uploaded = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* FeatureShowcaseServiceClient;
        return yield* client.uploadNotes(
          Stream.make({ text: "alpha" }, { text: "beta" }, { text: "gamma" }),
        );
      }).pipe(Effect.provide(clientLayer)),
    );

    expect(uploaded).toEqual({ count: 3, joined: "alpha,beta,gamma" });
  });

  it("round-trips a bidi-streaming call", async () => {
    const echoes = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* FeatureShowcaseServiceClient;
        return yield* Stream.runCollect(
          client.chat(
            Stream.make(
              { text: "hi", sequence: 1 },
              { text: "there", sequence: 2 },
            ),
          ),
        );
      }).pipe(Effect.provide(clientLayer)),
    );

    expect(echoes).toEqual([
      { text: "echo:hi", sequence: 2 },
      { text: "echo:there", sequence: 3 },
    ]);
  });
});
