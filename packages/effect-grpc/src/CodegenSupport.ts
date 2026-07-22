import type { Headers as EffectHeaders } from "effect/unstable/http/Headers";
import type { ServerClient } from "effect/unstable/rpc/Rpc";
import type { RequestId } from "effect/unstable/rpc/RpcMessage";

import * as GrpcMetadata from "./GrpcMetadata.js";

export interface GrpcCallOptions {
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  readonly timeoutMs?: number;
}

export interface GrpcServerContext {
  readonly client: ServerClient;
  readonly requestId: RequestId;
  readonly metadata: GrpcMetadata.GrpcMetadata;
}

export const serverContext = (options: {
  readonly client: ServerClient;
  readonly requestId: RequestId;
  readonly headers: EffectHeaders;
}): GrpcServerContext => ({
  client: options.client,
  requestId: options.requestId,
  metadata: GrpcMetadata.fromHeaders(Object.entries(options.headers)),
});
