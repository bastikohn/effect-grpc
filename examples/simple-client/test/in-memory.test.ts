import { Effect, Layer, Result, Stream } from "effect";
import { describe, expect, it } from "vitest";

import { GrpcInvoker, GrpcStatusError } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";

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
  it("round-trips all four call shapes", async () => {
    // One test, four independent shapes. `mode: "result"` runs every effect
    // regardless of the others' outcomes and collects each one as a `Result`
    // (plain `Effect.all` would short-circuit on the first failure). Unwrapping
    // with `Result.merge` puts a failure in the value slot, and `expect.soft`
    // keeps going after a mismatch — so every broken shape is reported, and no
    // shape can hide another.
    const result = await Effect.runPromise(
      Effect.all(
        {
          unary: Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* client.getUser({ id: "123" });
          }),
          server: Effect.gen(function* () {
            const client = yield* UserServiceClient;
            return yield* Stream.runCollect(
              client.watchUsers({ tenantId: "demo", count: 2 }),
            );
          }),
          client: Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            return yield* client.uploadNotes(
              Stream.make(
                { text: "alpha" },
                { text: "beta" },
                { text: "gamma" },
              ),
            );
          }),
          bidi: Effect.gen(function* () {
            const client = yield* FeatureShowcaseServiceClient;
            return yield* Stream.runCollect(
              client.chat(
                Stream.make(
                  { text: "hi", sequence: 1 },
                  { text: "there", sequence: 2 },
                ),
              ),
            );
          }),
        },
        { mode: "result" },
      ).pipe(Effect.provide(clientLayer)),
    );

    expect.soft(Result.merge(result.unary)).toEqual({
      user: { id: "123", name: "In-Memory User" },
    });
    expect.soft(Result.merge(result.server)).toEqual([
      { id: "demo", name: "In-Memory User", action: "created", sequence: 1 },
      { id: "demo", name: "In-Memory User", action: "updated", sequence: 2 },
    ]);
    expect.soft(Result.merge(result.client)).toEqual({
      count: 3,
      joined: "alpha,beta,gamma",
    });
    expect.soft(Result.merge(result.bidi)).toEqual([
      { text: "echo:hi", sequence: 2 },
      { text: "echo:there", sequence: 3 },
    ]);
  });

  it("surfaces a handler failure through the generated client's narrowed error channel", async () => {
    // A handler that fails with a `GrpcStatusError`; the failure must reach the
    // caller through the generated client's `<Service>ClientError` channel,
    // which is now narrowed to `GrpcStatusError` alone.
    const failingHandlers: GrpcInvoker.GrpcInMemoryHandlers = {
      "demo.v1.UserService/GetUser": {
        kind: "unary",
        handler: (request) =>
          Effect.fail(
            GrpcStatusError.notFound(
              `no such user: ${(request as { readonly id: string }).id}`,
            ),
          ),
      },
    };
    const failingLayer = UserServiceClientLayer.pipe(
      Layer.provide(GrpcInvoker.layerInMemory(failingHandlers)),
    );

    const error = await Effect.runPromise(
      Effect.gen(function* () {
        const client = yield* UserServiceClient;
        // The error channel here is `UserServiceClientError`
        // (= `GrpcStatusError`); `Effect.flip` moves it into the success slot.
        return yield* Effect.flip(client.getUser({ id: "404" }));
      }).pipe(Effect.provide(failingLayer)),
    );

    expect(error._tag).toBe("GrpcStatusError");
    expect(error.code).toBe("not_found");
    expect(error.message).toBe("no such user: 404");
  });
});
