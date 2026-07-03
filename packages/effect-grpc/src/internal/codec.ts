import { Schema } from "effect";

import type { GrpcMethodEntry } from "../GrpcMethodRegistry.js";

export interface EntryCodecs {
  readonly encodePayload: (value: unknown) => unknown;
  readonly decodePayload: (value: unknown) => unknown;
  readonly encodeSuccess: (value: unknown) => unknown;
  readonly decodeSuccess: (value: unknown) => unknown;
}

const cache = new WeakMap<GrpcMethodEntry, EntryCodecs>();

// The direct streaming bridge bypasses Effect RPC, so it has to apply the
// same JSON codecs `RpcClient`/`RpcServer` would (domain value <-> encoded
// payload) around the registry's per-message converters.
export const entryCodecs = (entry: GrpcMethodEntry): EntryCodecs => {
  let codecs = cache.get(entry);
  if (!codecs) {
    const payload = Schema.toCodecJson(entry.payloadSchema);
    const success = Schema.toCodecJson(entry.successSchema);
    codecs = {
      encodePayload: Schema.encodeUnknownSync(payload),
      decodePayload: Schema.decodeUnknownSync(payload),
      encodeSuccess: Schema.encodeUnknownSync(success),
      decodeSuccess: Schema.decodeUnknownSync(success),
    };
    cache.set(entry, codecs);
  }
  return codecs;
};
