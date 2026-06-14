import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Effect, Exit, Option, Queue, Schema, Scope } from "effect";
import type * as Tracer from "effect/Tracer";
import * as Headers from "effect/unstable/http/Headers";
import * as HttpTraceContext from "effect/unstable/http/HttpTraceContext";
import * as RpcServer from "effect/unstable/rpc/RpcServer";
import type { FromClientEncoded } from "effect/unstable/rpc/RpcMessage";

import type {
  GrpcMethodEntry,
  GrpcMethodRegistry,
} from "./GrpcMethodRegistry.js";
import type { GrpcStatusCode } from "./GrpcStatusCode.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import * as CallState from "./internal/callState.js";
import { eof, requestId } from "./internal/effectRpc.js";
import { errorFromExit } from "./internal/status.js";
import * as GrpcTracing from "./internal/tracing.js";

export interface GrpcServerProtocolOptions {
  readonly registry: GrpcMethodRegistry;
}

export interface GrpcServerProtocolResult {
  readonly protocol: RpcServer.Protocol["Service"];
  readonly routes: (router: ConnectRouter) => ConnectRouter;
}

export const make = (
  options: GrpcServerProtocolOptions,
): Effect.Effect<GrpcServerProtocolResult, never, Scope.Scope> =>
  Effect.gen(function* () {
    const context = yield* Effect.context<never>();
    const run = Effect.runPromiseWith(context);
    const calls = new Map<number, CallState.CallState>();
    const clientIds = new Set<number>();
    const disconnects = yield* Queue.unbounded<number>();
    let nextClientId = 0;
    let writeRequest: (
      clientId: number,
      data: FromClientEncoded,
    ) => Effect.Effect<void> = () => Effect.void;

    const protocol = yield* RpcServer.Protocol.make((writeRequest_) => {
      writeRequest = writeRequest_;
      return Effect.succeed({
        disconnects,
        send(clientId, response) {
          const call = calls.get(clientId);
          return call ? call.offer(response) : Effect.void;
        },
        end(clientId) {
          const call = calls.get(clientId);
          return call ? call.end : Effect.void;
        },
        clientIds: Effect.sync(() => clientIds),
        initialMessage: Effect.succeed(Option.none()),
        supportsAck: false,
        supportsTransferables: false,
        supportsSpanPropagation: true,
      });
    });

    const allocate = (state: CallState.CallState) => {
      const clientId = nextClientId++;
      clientIds.add(clientId);
      calls.set(clientId, state);
      return clientId;
    };

    const cleanup = (
      clientId: number,
      state: CallState.CallState,
      context: HandlerContext,
      onAbort: () => void,
    ) => {
      let released = false;
      let disconnected = false;
      const signalDisconnect = () => {
        if (disconnected) return Promise.resolve();
        disconnected = true;
        return run(Queue.offer(disconnects, clientId).pipe(Effect.asVoid));
      };
      const release = async () => {
        if (released) return;
        released = true;
        context.signal.removeEventListener("abort", onAbort);
        calls.delete(clientId);
        clientIds.delete(clientId);
        await run(state.end);
        await signalDisconnect();
      };
      return { release, signalDisconnect };
    };

    const interrupt = (
      clientId: number,
      signalDisconnect: () => Promise<void>,
    ) =>
      run(
        writeRequest(clientId, {
          _tag: "Interrupt",
          requestId,
        }).pipe(
          Effect.andThen(Effect.promise(signalDisconnect)),
          Effect.asVoid,
        ),
      );

    const sendNativeRequest = (
      clientId: number,
      entry: GrpcMethodEntry,
      request: unknown,
      headers: ReadonlyArray<[string, string]>,
      span: Tracer.Span,
    ) => {
      const payload = fromGrpcRequest(entry, request);
      validatePayload(entry, payload);
      return run(
        writeRequest(clientId, {
          _tag: "Request",
          id: requestId,
          tag: entry.tag,
          payload,
          headers,
          ...GrpcTracing.traceFields(span),
        }).pipe(Effect.withParentSpan(span)),
      );
    };

    const endNativeRequest = (clientId: number) =>
      run(writeRequest(clientId, eof));

    const handleUnary = async (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
    ): Promise<unknown> => {
      const headers = Array.from(context.requestHeader.entries());
      return run(
        Effect.gen(function* () {
          const span = yield* Effect.currentSpan.pipe(Effect.orDie);
          return yield* Effect.promise(() =>
            handleUnaryNative(entry, request, context, headers, span),
          );
        }).pipe(
          Effect.withSpan(
            GrpcTracing.spanName(entry),
            GrpcTracing.serverSpanOptions(entry, traceParent(headers)),
          ),
        ),
      );
    };

    const handleUnaryNative = async (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
      headers: ReadonlyArray<[string, string]>,
      span: Tracer.Span,
    ): Promise<unknown> => {
      const state = await run(CallState.makeUnary);
      const clientId = allocate(state);
      let completed = false;
      let signalDisconnect = () => Promise.resolve();
      const recordStatus = statusRecorder(span);
      const onAbort = () => {
        void interrupt(clientId, signalDisconnect);
      };
      const call = cleanup(clientId, state, context, onAbort);
      signalDisconnect = call.signalDisconnect;
      context.signal.addEventListener("abort", onAbort, { once: true });

      try {
        await sendNativeRequest(clientId, entry, request, headers, span);
        const response = await run(state.awaitExit);
        completed = true;
        if (response._tag === "Defect") {
          throw GrpcStatusError.toConnectError(
            GrpcStatusError.internal("RPC handler defect", response.defect),
          );
        }
        if (response.exit._tag === "Failure") {
          throw GrpcStatusError.toConnectError(errorFromExit(response.exit));
        }
        const grpcResponse = toGrpcResponse(entry, response.exit.value);
        recordStatus("ok");
        return grpcResponse;
      } catch (cause) {
        const error = GrpcStatusError.fromConnectError(cause);
        recordStatus(error.code);
        throw GrpcStatusError.toConnectError(error);
      } finally {
        try {
          if (!completed) {
            await interrupt(clientId, call.signalDisconnect);
          }
          await endNativeRequest(clientId);
        } finally {
          await call.release();
        }
      }
    };

    const handleServerStreaming = async function* (
      entry: GrpcMethodEntry,
      request: unknown,
      context: HandlerContext,
    ): AsyncIterable<unknown> {
      const headers = Array.from(context.requestHeader.entries());
      const spanScope = await run(Scope.make());
      const span = await run(
        Effect.makeSpanScoped(
          GrpcTracing.spanName(entry),
          GrpcTracing.serverSpanOptions(entry, traceParent(headers)),
        ).pipe(Scope.provide(spanScope)),
      );
      const state = await run(CallState.makeServerStreaming);
      const clientId = allocate(state);
      let completed = false;
      let signalDisconnect = () => Promise.resolve();
      let spanExit: Exit.Exit<void, GrpcStatusError.GrpcStatusError> =
        Exit.void;
      const recordStatus = statusRecorder(span);
      const recordFailure = (error: GrpcStatusError.GrpcStatusError) => {
        recordStatus(error.code);
        spanExit = Exit.fail(error);
      };
      const onAbort = () => {
        void interrupt(clientId, signalDisconnect);
      };
      const call = cleanup(clientId, state, context, onAbort);
      signalDisconnect = call.signalDisconnect;
      context.signal.addEventListener("abort", onAbort, { once: true });

      try {
        await sendNativeRequest(clientId, entry, request, headers, span);
        while (true) {
          const response = await run(state.take);
          if (!response) {
            completed = true;
            recordStatus("ok");
            return;
          }
          switch (response._tag) {
            case "Chunk":
              for (const value of response.values) {
                yield toGrpcResponse(entry, value);
              }
              break;
            case "Exit":
              completed = true;
              if (response.exit._tag === "Failure") {
                throw GrpcStatusError.toConnectError(
                  errorFromExit(response.exit),
                );
              }
              recordStatus("ok");
              return;
            case "Defect":
              completed = true;
              throw GrpcStatusError.toConnectError(
                GrpcStatusError.internal("RPC handler defect", response.defect),
              );
            case "ClientProtocolError":
              completed = true;
              throw GrpcStatusError.toConnectError(
                GrpcStatusError.internal("RPC protocol error", response.error),
              );
            case "Pong":
              break;
          }
        }
      } catch (cause) {
        const error = GrpcStatusError.fromConnectError(cause);
        recordFailure(error);
        throw GrpcStatusError.toConnectError(error);
      } finally {
        try {
          if (!completed) {
            if (spanExit._tag === "Success") {
              recordFailure(GrpcStatusError.cancelled("RPC cancelled"));
            }
            await interrupt(clientId, call.signalDisconnect);
          }
          await endNativeRequest(clientId);
        } finally {
          try {
            await call.release();
          } finally {
            await run(Scope.close(spanScope, spanExit));
          }
        }
      }
    };

    const routes = (router: ConnectRouter) => {
      for (const [service, entries] of groupByService(options.registry)) {
        const implementation: Record<string, unknown> = {};
        for (const entry of entries) {
          implementation[entry.localName] =
            entry.kind === "unary"
              ? (request: unknown, context: HandlerContext) =>
                  handleUnary(entry, request, context)
              : (request: unknown, context: HandlerContext) =>
                  handleServerStreaming(entry, request, context);
        }
        router.service(service as never, implementation as never);
      }
      return router;
    };

    return { protocol, routes };
  });

const groupByService = (registry: GrpcMethodRegistry) => {
  const groups = new Map<GrpcMethodEntry["service"], Array<GrpcMethodEntry>>();
  for (const entry of registry.values()) {
    const group = groups.get(entry.service);
    if (group) {
      group.push(entry);
    } else {
      groups.set(entry.service, [entry]);
    }
  }
  return groups;
};

const fromGrpcRequest = (entry: GrpcMethodEntry, request: unknown) => {
  try {
    return entry.fromGrpcRequest(request as never);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
    );
  }
};

const validatePayload = (entry: GrpcMethodEntry, payload: unknown) => {
  try {
    Schema.decodeUnknownSync(Schema.toCodecJson(entry.payloadSchema))(payload);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
    );
  }
};

const toGrpcResponse = (entry: GrpcMethodEntry, value: unknown) => {
  try {
    return entry.toGrpcResponse(value);
  } catch (cause) {
    throw GrpcStatusError.toConnectError(
      GrpcStatusError.internal("Invalid gRPC response payload", cause),
    );
  }
};

const traceParent = (headers: ReadonlyArray<readonly [string, string]>) =>
  HttpTraceContext.fromHeaders(Headers.fromInput(headers)).pipe(
    Option.match({
      onNone: () => undefined,
      onSome: (span) => span,
    }),
  );

const statusRecorder = (span: Tracer.Span) => {
  let recorded = false;
  return (code: GrpcStatusCode) => {
    if (recorded) return;
    recorded = true;
    GrpcTracing.annotateSpanStatus(span, code);
  };
};
