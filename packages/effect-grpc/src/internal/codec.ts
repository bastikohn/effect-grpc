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
// same codecs `RpcClient`/`RpcServer` would (domain value <-> encoded
// payload) around the registry's per-message converters.
export const entryCodecs = (entry: GrpcMethodEntry): EntryCodecs => {
  let codecs = cache.get(entry);
  if (!codecs) {
    codecs = {
      encodePayload: Schema.encodeUnknownSync(entry.payloadSchema),
      decodePayload: Schema.decodeUnknownSync(entry.payloadSchema),
      encodeSuccess: Schema.encodeUnknownSync(entry.successSchema),
      decodeSuccess: Schema.decodeUnknownSync(entry.successSchema),
    };
    cache.set(entry, codecs);
  }
  return codecs;
};
