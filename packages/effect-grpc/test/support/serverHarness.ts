import type { ConnectRouter, HandlerContext } from "@connectrpc/connect";
import { Schema } from "effect";

import type { GrpcMethodEntry } from "../../src/GrpcMethodRegistry.js";

/** The connect method implementations of one service, by method local name. */
export type ServiceImplementation = Record<string, unknown>;

/**
 * Runs `GrpcServerProtocol.make`'s `routes` against a fake router and returns
 * the single service implementation it registered — the whole server surface
 * a test needs without booting an HTTP/2 listener.
 */
export const captureImplementation = (
  routes: (router: ConnectRouter) => ConnectRouter,
): ServiceImplementation => {
  let implementation: ServiceImplementation | undefined;
  const router = {
    service(_service: unknown, serviceImplementation: ServiceImplementation) {
      implementation = serviceImplementation;
      return router;
    },
  };

  routes(router as unknown as ConnectRouter);

  if (!implementation) {
    throw new Error("Expected routes to register a service implementation");
  }
  return implementation;
};

/** The connect handler context a captured implementation is called with. */
export const handlerContext = (options?: {
  readonly headers?: ConstructorParameters<typeof Headers>[0];
  readonly signal?: AbortSignal;
}): HandlerContext =>
  ({
    requestHeader: new Headers(options?.headers),
    signal: options?.signal ?? new AbortController().signal,
  }) as HandlerContext;

/**
 * A four-shape method fixture for one service: a shared descriptor plus a
 * pass-through registry entry per call kind, which is all the protocol,
 * telemetry and invoker tests need from a registry.
 */
export const methodEntries = (serviceTypeName: string) => {
  const service = {
    typeName: serviceTypeName,
    methods: [
      { methodKind: "unary", localName: "get" },
      { methodKind: "server_streaming", localName: "watch" },
      { methodKind: "client_streaming", localName: "upload" },
      { methodKind: "bidi_streaming", localName: "chat" },
    ],
  } as unknown as GrpcMethodEntry["service"];

  const unary: GrpcMethodEntry = {
    kind: "unary",
    tag: `${serviceTypeName}/Get`,
    service,
    localName: "get",
    payloadSchema: Schema.Unknown,
    successSchema: Schema.Unknown,
    toGrpcRequest: (value) => value as never,
    fromGrpcRequest: (message) => message,
    toGrpcResponse: (value) => value as never,
    fromGrpcResponse: (message) => message,
  };

  return {
    unary,
    serverStreaming: {
      ...unary,
      kind: "server-streaming",
      tag: `${serviceTypeName}/Watch`,
      localName: "watch",
    } satisfies GrpcMethodEntry as GrpcMethodEntry,
    clientStreaming: {
      ...unary,
      kind: "client-streaming",
      tag: `${serviceTypeName}/Upload`,
      localName: "upload",
    } satisfies GrpcMethodEntry as GrpcMethodEntry,
    bidiStreaming: {
      ...unary,
      kind: "bidi-streaming",
      tag: `${serviceTypeName}/Chat`,
      localName: "chat",
    } satisfies GrpcMethodEntry as GrpcMethodEntry,
  };
};
