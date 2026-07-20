import type { Transport } from "@connectrpc/connect";
import { Context, Layer } from "effect";
import type { Effect, Stream } from "effect";

import type { GrpcCallOptions } from "./CodegenSupport.js";
import type * as GrpcMetadata from "./GrpcMetadata.js";
import type { GrpcMethodRegistry } from "./GrpcMethodRegistry.js";
import type * as GrpcStatusError from "./GrpcStatusError.js";
import { makeConnect } from "./internal/connectInvoker.js";
import { makeInMemory } from "./internal/inMemoryInvoker.js";

/**
 * The invocation seam between generated clients and a gRPC transport: four
 * call shapes, one per method cardinality. Methods are identified by their
 * registry tag; the invoker owns method lookup, kind validation, codecs, and
 * one semantic call outcome (status + tracing) per call, so callers never
 * see transport machinery.
 *
 * Two implementations make the seam real: {@link layerConnect} invokes over
 * a connect transport in production, {@link layerInMemory} dispatches to
 * in-process handlers for deterministic tests — no sockets, protobuf
 * descriptors, or HTTP/2.
 */
export interface GrpcInvokerService {
  /** One request, one response. */
  readonly unary: (
    tag: string,
    request: unknown,
    options?: GrpcCallOptions,
  ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>;
  /** One request, streamed responses. */
  readonly serverStream: (
    tag: string,
    request: unknown,
    options?: GrpcCallOptions,
  ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>;
  /** Streamed requests, one response. */
  readonly clientStream: <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError | E>;
  /** Streamed requests, streamed responses. */
  readonly bidiStream: <A, E>(
    tag: string,
    requests: Stream.Stream<A, E>,
    options?: GrpcCallOptions,
  ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError | E>;
}

export class GrpcInvoker extends Context.Service<
  GrpcInvoker,
  GrpcInvokerService
>()("@effect-grpc/effect-grpc/GrpcInvoker") {}

export interface GrpcConnectInvokerOptions {
  readonly registry: GrpcMethodRegistry;
  /** A transport from `GrpcClientProtocol.makeTransport`, or any connect `Transport`. */
  readonly transport: Transport;
  /** Address reported in client span attributes. Telemetry only. */
  readonly serverAddress?: URL | undefined;
}

/** Production adapter: invokes methods over a connect transport. */
export const layerConnect = (
  options: GrpcConnectInvokerOptions,
): Layer.Layer<GrpcInvoker> => Layer.effect(GrpcInvoker, makeConnect(options));

/**
 * Normalized call context an in-memory handler receives — what the transport
 * would deliver to a server. Capture it in tests to assert on metadata and
 * timeouts.
 */
export interface GrpcInMemoryCall {
  readonly tag: string;
  readonly metadata: GrpcMetadata.GrpcMetadata;
  readonly timeoutMs?: number | undefined;
}

/**
 * In-memory implementations of the four call shapes. Handlers receive domain
 * values (the connect adapter's codecs are a wire concern) and the same
 * request-stream semantics a real server observes: a failed caller stream
 * arrives as `cancelled` while the caller gets its original error replayed.
 */
export type GrpcInMemoryHandler =
  | {
      readonly kind: "unary";
      readonly handler: (
        request: unknown,
        call: GrpcInMemoryCall,
      ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>;
    }
  | {
      readonly kind: "server-streaming";
      readonly handler: (
        request: unknown,
        call: GrpcInMemoryCall,
      ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>;
    }
  | {
      readonly kind: "client-streaming";
      readonly handler: (
        requests: Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
        call: GrpcInMemoryCall,
      ) => Effect.Effect<unknown, GrpcStatusError.GrpcStatusError>;
    }
  | {
      readonly kind: "bidi-streaming";
      readonly handler: (
        requests: Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
        call: GrpcInMemoryCall,
      ) => Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>;
    };

export type GrpcInMemoryHandlers = Record<string, GrpcInMemoryHandler>;

/**
 * Test adapter: dispatches directly to the given handlers. Enforces the same
 * invocation semantics as the connect adapter — unknown or kind-mismatched
 * tags fail with `unimplemented`, a failed request stream replays the
 * caller's original error, and `timeoutMs` bounds unary and client-streaming
 * calls with `deadline_exceeded`. Stream-shaped calls expose `timeoutMs` on
 * the call context but leave mid-stream deadline enforcement to transports.
 */
export const layerInMemory = (
  handlers: GrpcInMemoryHandlers,
): Layer.Layer<GrpcInvoker> =>
  Layer.succeed(GrpcInvoker, makeInMemory(handlers));
