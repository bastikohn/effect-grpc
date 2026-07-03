import type {
  FromServerEncoded,
  ResponseDefectEncoded,
  ResponseExitEncoded,
} from "@effect/rpc/RpcMessage";
import { Deferred, Effect, Mailbox } from "effect";

const serverStreamBufferSize = 16;

export type CallState = UnaryCallState | ServerStreamingCallState;

export interface UnaryCallState {
  readonly kind: "unary";
  readonly offer: (response: FromServerEncoded) => Effect.Effect<void>;
  readonly awaitExit: Effect.Effect<
    ResponseExitEncoded | ResponseDefectEncoded,
    never
  >;
  readonly end: Effect.Effect<void>;
}

export interface ServerStreamingCallState {
  readonly kind: "server-streaming";
  readonly offer: (response: FromServerEncoded) => Effect.Effect<void>;
  readonly take: Effect.Effect<FromServerEncoded | undefined, never>;
  readonly end: Effect.Effect<void>;
}

export const makeUnary = Effect.gen(function* () {
  const result = yield* Deferred.make<
    ResponseExitEncoded | ResponseDefectEncoded
  >();

  return {
    kind: "unary" as const,
    offer(response) {
      switch (response._tag) {
        case "Exit":
        case "Defect":
          return Deferred.succeed(result, response).pipe(Effect.asVoid);
        case "Chunk":
          return Deferred.succeed(result, {
            _tag: "Defect",
            defect: "Unary gRPC call received streaming chunk",
          }).pipe(Effect.asVoid);
        case "ClientProtocolError":
          return Deferred.succeed(result, {
            _tag: "Defect",
            defect: response.error,
          }).pipe(Effect.asVoid);
        case "Pong":
          return Effect.void;
      }
    },
    awaitExit: Deferred.await(result),
    end: Deferred.succeed(result, {
      _tag: "Defect",
      defect: "Unary gRPC call ended before a response was produced",
    }).pipe(Effect.asVoid),
  } satisfies UnaryCallState;
});

export const makeServerStreaming = Effect.gen(function* () {
  const mailbox = yield* Mailbox.make<FromServerEncoded>(
    serverStreamBufferSize,
  );

  return {
    kind: "server-streaming" as const,
    offer(response) {
      return mailbox.offer(response).pipe(Effect.asVoid);
    },
    take: mailbox.take.pipe(
      Effect.catchTag("NoSuchElementException", () =>
        Effect.succeed(undefined),
      ),
    ),
    end: mailbox.end.pipe(Effect.asVoid),
  } satisfies ServerStreamingCallState;
});
