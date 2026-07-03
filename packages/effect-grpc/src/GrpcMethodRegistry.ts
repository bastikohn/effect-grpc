import type {
  DescMessage,
  DescService,
  MessageInitShape,
  MessageShape,
} from "@bufbuild/protobuf";
import type { Schema } from "effect";

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
  readonly payloadSchema: Schema.Schema.AnyNoContext;
  readonly successSchema: Schema.Schema.AnyNoContext;
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
