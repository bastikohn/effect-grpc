import { NodeRuntime } from "@effect/platform-node";
import { Duration, Effect, Layer } from "effect";

import { GrpcClientProtocol } from "@effect-grpc/effect-grpc";
import {
  FeatureShowcaseServiceClient,
  FeatureShowcaseServiceClientLayer,
  FeatureShowcaseServiceGrpcRegistry,
} from "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc";

const args = process.argv.slice(2);
if (args[0] === "--") {
  args.shift();
}

const getArg = (name: string, fallback: string): string => {
  const index = args.indexOf(`--${name}`);
  return index >= 0 && args[index + 1] ? args[index + 1]! : fallback;
};

const baseUrl = new URL(getArg("base-url", "http://127.0.0.1:50052"));

const ClientProtocolLive = GrpcClientProtocol.layer({
  baseUrl: baseUrl.toString().replace(/\/$/, ""),
  registry: FeatureShowcaseServiceGrpcRegistry,
});

const MainLive = FeatureShowcaseServiceClientLayer.pipe(
  Layer.provide(ClientProtocolLive),
);

const program = Effect.gen(function* () {
  const client = yield* FeatureShowcaseServiceClient;
  const response = yield* client.describe({
    tags: ["alpha", "beta"],
    scores: [10, 20],
    notes: [{ text: "generated feature demo" }],
    state: 1,
    owner: { id: "user-1", name: "Ada" },
    labels: { env: "demo" },
    counts: { attempts: 1 },
    reviewers: { primary: { id: "reviewer-1", role: "owner" } },
    createdAt: new Date(0),
    ttl: Duration.seconds(30),
    payload: new Uint8Array([1, 2, 3]),
    sequence: 42n,
    contact: { case: "contactEmail", value: "ada@example.com" },
  });

  console.log(response.summary);
});

NodeRuntime.runMain(program.pipe(Effect.provide(MainLive)));
