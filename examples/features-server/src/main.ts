import { NodeRuntime } from "@effect/platform-node";
import { Effect } from "effect";

import { GrpcNodeServer } from "@effect-grpc/effect-grpc";
import {
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlersLayer,
  FeatureShowcaseServiceRpcGroup,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/features-proto/generated/features/v1/showcase_effect_grpc";

const getArg = (name: string, fallback: string): string => {
  const index = process.argv.indexOf(`--${name}`);
  return index >= 0 && process.argv[index + 1]
    ? process.argv[index + 1]!
    : fallback;
};

const host = getArg("host", "127.0.0.1");
const port = Number(getArg("port", "50052"));

const implementation: FeatureShowcaseServiceImplementation = {
  describe: (request) =>
    Effect.succeed({
      request,
      summary: [
        `owner=${request.owner?.name ?? "unknown"}`,
        `tags=${request.tags.length}`,
        `notes=${request.notes.length}`,
        `labels=${Object.keys(request.labels).length}`,
        `payload=${request.payload.length}`,
        `sequence=${request.sequence}`,
        `contact=${request.contact.case ?? "none"}`,
      ].join(" "),
    }),
};

const program = Effect.scoped(
  GrpcNodeServer.serveAll({
    host,
    port,
    services: [
      {
        group: FeatureShowcaseServiceRpcGroup,
        registry: FeatureShowcaseServiceGrpcRegistry,
        handlers: FeatureShowcaseServiceHandlersLayer(implementation),
      },
    ],
  }),
);

NodeRuntime.runMain(program);
