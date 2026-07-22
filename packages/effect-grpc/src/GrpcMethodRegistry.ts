import type {
  DescMessage,
  DescService,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import { Effect, Schema } from "effect";

import * as GrpcStatusError from "./GrpcStatusError.js";

export type GrpcMethodKind =
  | "unary"
  | "server-streaming"
  | "client-streaming"
  | "bidi-streaming";

export type GrpcMethodEntry =
  | GrpcUnaryMethodEntry
  | GrpcServerStreamingMethodEntry
  | GrpcClientStreamingMethodEntry
  | GrpcBidiStreamingMethodEntry;

export interface GrpcMethodEntryBase<
  Request extends DescMessage = DescMessage,
  Response extends DescMessage = DescMessage,
> {
  readonly kind: GrpcMethodKind;
  readonly tag: string;
  readonly service: DescService;
  readonly localName: string;
  readonly payloadSchema: Schema.Codec<unknown>;
  readonly successSchema: Schema.Codec<unknown>;
  readonly toGrpcRequest: (
    encodedPayload: unknown,
  ) => MessageInitShape<Request>;
  readonly fromGrpcRequest: (message: MessageShape<Request>) => unknown;
  readonly toGrpcResponse: (
    encodedSuccess: unknown,
  ) => MessageInitShape<Response>;
  readonly fromGrpcResponse: (message: MessageShape<Response>) => unknown;
}

export interface GrpcUnaryMethodEntry<
  Request extends DescMessage = DescMessage,
  Response extends DescMessage = DescMessage,
> extends GrpcMethodEntryBase<Request, Response> {
  readonly kind: "unary";
}

export interface GrpcServerStreamingMethodEntry<
  Request extends DescMessage = DescMessage,
  Response extends DescMessage = DescMessage,
> extends GrpcMethodEntryBase<Request, Response> {
  readonly kind: "server-streaming";
}

export interface GrpcClientStreamingMethodEntry<
  Request extends DescMessage = DescMessage,
  Response extends DescMessage = DescMessage,
> extends GrpcMethodEntryBase<Request, Response> {
  readonly kind: "client-streaming";
}

export interface GrpcBidiStreamingMethodEntry<
  Request extends DescMessage = DescMessage,
  Response extends DescMessage = DescMessage,
> extends GrpcMethodEntryBase<Request, Response> {
  readonly kind: "bidi-streaming";
}

export type GrpcMethodRegistry = ReadonlyMap<string, GrpcMethodEntry>;

/**
 * Looks up a method by tag and validates its cardinality in one step, so
 * callers cannot dispatch a tag to the wrong call shape.
 */
export const lookup = <K extends GrpcMethodKind>(
  registry: GrpcMethodRegistry,
  tag: string,
  kind: K,
): Extract<GrpcMethodEntry, { readonly kind: K }> | undefined => {
  const entry = registry.get(tag);
  return entry && entry.kind === kind
    ? (entry as Extract<GrpcMethodEntry, { readonly kind: K }>)
    : undefined;
};

/**
 * Merges per-service registries into one, enforcing the construction
 * invariant that a tag resolves to exactly one method.
 *
 * @throws `Error` on a duplicate tag. Registry composition is construction
 * work, so a violated invariant is a defect (wiring bug), not a typed
 * failure a caller is expected to recover from.
 */
export const merge = (
  registries: Iterable<GrpcMethodRegistry>,
): GrpcMethodRegistry => {
  const merged = new Map<string, GrpcMethodEntry>();
  for (const registry of registries) {
    for (const [tag, entry] of registry) {
      if (merged.has(tag)) {
        throw new Error(`Duplicate gRPC RPC tag: ${tag}`);
      }
      merged.set(tag, entry);
    }
  }
  return merged;
};

/** Groups entries by their service descriptor, e.g. for route registration. */
export const groupByService = (
  registry: GrpcMethodRegistry,
): ReadonlyMap<DescService, ReadonlyArray<GrpcMethodEntry>> => {
  const groups = new Map<DescService, Array<GrpcMethodEntry>>();
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

/*
 * Conversions between domain values and wire messages, with one error
 * policy: request-payload problems are the caller's fault
 * (`invalid_argument`), response-payload problems are the producer's fault
 * (`internal`). The direct streaming paths bypass Effect RPC, so these
 * apply the same JSON codecs `RpcClient`/`RpcServer` would (domain value
 * <-> encoded payload) around the registry's per-message converters.
 */

/** Client side: domain request value -> wire request message. */
export const encodeRequest = (
  entry: GrpcMethodEntry,
  value: unknown,
): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError> =>
  Effect.try({
    try: () => entry.toGrpcRequest(payloadCodecs(entry).encode(value)),
    catch: (cause) =>
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
  });

/** Server side: wire request message -> domain request value. */
export const decodeRequest = (
  entry: GrpcMethodEntry,
  message: unknown,
): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError> =>
  Effect.try({
    try: () =>
      payloadCodecs(entry).decode(entry.fromGrpcRequest(message as never)),
    catch: (cause) =>
      GrpcStatusError.invalidArgument("Invalid gRPC request payload", cause),
  });

/** Server side: domain response value -> wire response message. */
export const encodeResponse = (
  entry: GrpcMethodEntry,
  value: unknown,
): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError> =>
  Effect.try({
    try: () => entry.toGrpcResponse(successCodecs(entry).encode(value)),
    catch: (cause) =>
      GrpcStatusError.internal("Invalid gRPC response payload", cause),
  });

/** Client side: wire response message -> domain response value. */
export const decodeResponse = (
  entry: GrpcMethodEntry,
  message: unknown,
): Effect.Effect<unknown, GrpcStatusError.GrpcStatusError> =>
  Effect.try({
    try: () =>
      successCodecs(entry).decode(entry.fromGrpcResponse(message as never)),
    catch: (cause) =>
      GrpcStatusError.internal("Invalid gRPC response payload", cause),
  });

interface DirectionCodecs {
  readonly encode: (value: unknown) => unknown;
  readonly decode: (value: unknown) => unknown;
}

/*
 * The payload and success codecs are cached separately so each direction is
 * only built when its side is exercised — a success-schema construction
 * failure must surface under the response policy (`internal`), never inside
 * a request conversion as `invalid_argument`.
 */
const payloadCodecCache = new WeakMap<GrpcMethodEntry, DirectionCodecs>();
const successCodecCache = new WeakMap<GrpcMethodEntry, DirectionCodecs>();

const directionCodecs = (
  cache: WeakMap<GrpcMethodEntry, DirectionCodecs>,
  entry: GrpcMethodEntry,
  schema: Schema.Codec<unknown>,
): DirectionCodecs => {
  let codecs = cache.get(entry);
  if (!codecs) {
    const json = Schema.toCodecJson(schema);
    codecs = {
      encode: Schema.encodeUnknownSync(json),
      decode: Schema.decodeUnknownSync(json),
    };
    cache.set(entry, codecs);
  }
  return codecs;
};

/** Prepared request-payload JSON codecs, built once per entry. */
const payloadCodecs = (entry: GrpcMethodEntry): DirectionCodecs =>
  directionCodecs(payloadCodecCache, entry, entry.payloadSchema);

/** Prepared response-success JSON codecs, built once per entry. */
const successCodecs = (entry: GrpcMethodEntry): DirectionCodecs =>
  directionCodecs(successCodecCache, entry, entry.successSchema);
