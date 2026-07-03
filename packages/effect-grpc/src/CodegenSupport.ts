import type { Headers as EffectHeaders } from "@effect/platform/Headers";

import * as GrpcMetadata from "./GrpcMetadata.js";
import { headersFromCallOptions } from "./internal/metadata.js";

export interface GrpcCallOptions {
  readonly metadata?: GrpcMetadata.GrpcMetadata;
  readonly timeoutMs?: number;
}

export interface GrpcServerContext {
  readonly clientId: number;
  readonly metadata: GrpcMetadata.GrpcMetadata;
}

export const headersFromOptions = headersFromCallOptions;

export const serverContext = (options: {
  readonly clientId: number;
  readonly headers: EffectHeaders;
}): GrpcServerContext => ({
  clientId: options.clientId,
  metadata: GrpcMetadata.fromHeaders(Object.entries(options.headers)),
});
