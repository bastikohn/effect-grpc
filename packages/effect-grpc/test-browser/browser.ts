import { Effect, Layer, Stream } from "effect";

import { GrpcClient, GrpcInvoker, GrpcWebClient } from "../src/client.js";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
} from "../../../examples/simple-proto/src/generated/demo/v1/user_service_effect_grpc.js";

import { FeatureShowcaseServiceGrpcRegistry } from "../../../examples/simple-proto/src/generated/features/v1/showcase_effect_grpc.js";

export interface BrowserRequest {
  readonly baseUrl: string;
  readonly protocol: "connect" | "grpc-web";
  readonly scenario:
    | "unary"
    | "stream"
    | "deadline"
    | "unary-deadline"
    | "cancel"
    | "unsupported"
    | "unauthorized";
  readonly id: string;
}

const run = async ({ baseUrl, protocol, scenario, id }: BrowserRequest) => {
  const interceptor = await Effect.runPromise(
    GrpcClient.metadataInterceptor(
      Effect.succeed([["authorization", "Bearer browser-token"]]),
    ),
  );
  const invokerLayer = GrpcWebClient.layer({
    baseUrl,
    protocol,
    registry: new Map([
      ...UserServiceGrpcRegistry,
      ...FeatureShowcaseServiceGrpcRegistry,
    ]),
    interceptors: [interceptor],
  });
  const layer = UserServiceClientLayer.pipe(Layer.provideMerge(invokerLayer));
  let values = 0;
  let acquired = 0;
  const started = performance.now();
  const program = Effect.gen(function* () {
    const client = yield* UserServiceClient;
    if (scenario === "unsupported") {
      const invoker = yield* GrpcInvoker.GrpcInvoker;
      const requests = Stream.suspend(() => {
        acquired += 1;
        return Stream.die("unsupported browser stream was acquired");
      });
      const clientError = yield* invoker
        .clientStream(
          "features.v1.FeatureShowcaseService/UploadNotes",
          requests,
        )
        .pipe(
          Effect.match({
            onSuccess: () => "unexpected success",
            onFailure: (error) => error.code,
          }),
        );
      const bidiError = yield* Stream.runDrain(
        invoker.bidiStream("features.v1.FeatureShowcaseService/Chat", requests),
      ).pipe(
        Effect.match({
          onSuccess: () => "unexpected success",
          onFailure: (error) => error.code,
        }),
      );
      return { codes: [clientError, bidiError], acquired };
    }
    if (
      scenario === "unary" ||
      scenario === "unary-deadline" ||
      scenario === "unauthorized"
    ) {
      return yield* client.getUser(
        { id },
        {
          metadata: [
            ["trace-bin", new Uint8Array([0, 1, 250, 255])],
            ["x-request-id", id],
            ...(scenario === "unauthorized"
              ? [["authorization", "invalid"] as const]
              : []),
          ],
          ...(scenario === "unary-deadline" ? { timeoutMs: 80 } : {}),
        },
      );
    }
    const stream = client
      .watchUsers(
        { tenantId: id, count: scenario === "stream" ? 3 : 100 },
        {
          timeoutMs: scenario === "deadline" ? 180 : 5000,
        },
      )
      .pipe(
        Stream.tap(() =>
          Effect.sync(() => {
            values += 1;
          }),
        ),
      );
    return yield* Stream.runCollect(
      scenario === "cancel" ? Stream.take(stream, 1) : stream,
    );
  }).pipe(
    Effect.provide(layer),
    Effect.match({
      onSuccess: (value) => ({
        ok: true as const,
        value,
        values,
        acquired,
        elapsed: performance.now() - started,
      }),
      onFailure: (error) => ({
        ok: false as const,
        code: error.code,
        values,
        acquired,
        elapsed: performance.now() - started,
      }),
    }),
  );
  return Effect.runPromise(program);
};

declare global {
  var browserRpc: typeof run;
}
globalThis.browserRpc = run;
