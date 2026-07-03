import { Schema } from "effect";

export type GrpcMetadataValue = string | Uint8Array;

export type GrpcMetadata = ReadonlyArray<
  readonly [key: string, value: GrpcMetadataValue]
>;

export const schema = Schema.Array(
  Schema.Tuple(
    Schema.String,
    Schema.Union(Schema.String, Schema.Uint8ArrayFromBase64),
  ),
);

export const empty: GrpcMetadata = [];

export const fromHeaders = (
  headers: Headers | ReadonlyArray<readonly [string, string]>,
): GrpcMetadata => {
  const entries =
    headers instanceof Headers ? Array.from(headers.entries()) : headers;
  return entries.map(([key, value]) => [key.toLowerCase(), value] as const);
};

export const toHeaders = (metadata: GrpcMetadata): Headers => {
  const headers = new Headers();
  for (const [key, value] of metadata) {
    headers.append(
      key,
      value instanceof Uint8Array ? bytesToBase64(value) : value,
    );
  }
  return headers;
};

const bytesToBase64 = (bytes: Uint8Array) =>
  Buffer.from(bytes).toString("base64");
