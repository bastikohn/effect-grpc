import { Effect, Stream } from "effect";

import {
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlers,
  type FeatureShowcaseServiceImplementation,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";

// A second service on the same server, showing the wider protobuf surface:
// client- and bidi-streaming methods, repeated/map/oneof fields, well-known
// types.
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
  uploadNotes: (requests) =>
    Stream.runCollect(requests).pipe(
      Effect.map((notes) => ({
        count: notes.length,
        joined: notes.map((note) => note.text).join(","),
      })),
    ),
  chat: (requests) =>
    Stream.map(requests, (message) => ({
      text: `echo: ${message.text}`,
      sequence: message.sequence + 1,
    })),
};

export const featureShowcaseService = {
  registry: FeatureShowcaseServiceGrpcRegistry,
  handlers: FeatureShowcaseServiceHandlers(implementation),
};
