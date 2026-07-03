import type { FromClientEncoded } from "@effect/rpc/RpcMessage";

export const requestId = "0";

export const eof = { _tag: "Eof" } satisfies FromClientEncoded;
