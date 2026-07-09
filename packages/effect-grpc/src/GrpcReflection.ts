import type { DescExtension, DescFile, DescMessage } from "@bufbuild/protobuf";
import { toBinary } from "@bufbuild/protobuf";
import { base64Decode, base64Encode } from "@bufbuild/protobuf/wire";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Context, Effect, Layer, Schema, Stream } from "effect";
import type * as RpcClientError from "effect/unstable/rpc/RpcClientError";
import * as RpcGroup from "effect/unstable/rpc/RpcGroup";

import type * as CodegenSupport from "./CodegenSupport.js";
import * as GrpcClientProtocol from "./GrpcClientProtocol.js";
import type * as GrpcMethodRegistry from "./GrpcMethodRegistry.js";
import type { ServeAllService } from "./GrpcNodeServer.js";
import * as GrpcServerProtocol from "./GrpcServerProtocol.js";
import * as GrpcStatusCode from "./GrpcStatusCode.js";
import type * as GrpcStatusError from "./GrpcStatusError.js";
import * as ReflectionV1AlphaPb from "./internal/reflectionV1AlphaPb.js";
import * as ReflectionV1Pb from "./internal/reflectionV1Pb.js";

/**
 * Standard gRPC Server Reflection Protocol
 * (`grpc.reflection.v1.ServerReflection`), see
 * https://github.com/grpc/grpc/blob/master/doc/server-reflection.md.
 *
 * {@link service} answers reflection queries from the descriptors the
 * generated registries already carry, so tools like `grpcurl`, `grpcui`, and
 * Postman work against the server without local `.proto` files. Pass it the
 * same services you pass to `GrpcNodeServer.serveAll`:
 *
 * ```ts
 * const services = [userService, GrpcHealth.service] as const;
 * GrpcNodeServer.serveAll({
 *   host, port,
 *   services: [...services, GrpcReflection.service(services)],
 * })
 * ```
 *
 * The handler is registered under both the `v1` and the legacy `v1alpha`
 * service names; the two protocols are wire-identical.
 */

/** Payload of a `file_containing_extension` request. */
export const ExtensionRequestSchema = Schema.Struct({
  containingType: Schema.String,
  extensionNumber: Schema.Number,
});
export type ExtensionRequest = Schema.Schema.Type<
  typeof ExtensionRequestSchema
>;

/**
 * `grpc.reflection.v1.ServerReflectionRequest`. The proto `message_request`
 * oneof is flattened into optional fields; at most one may be set.
 */
export const ServerReflectionRequestSchema = Schema.Struct({
  host: Schema.String,
  fileByFilename: Schema.optional(Schema.String),
  fileContainingSymbol: Schema.optional(Schema.String),
  fileContainingExtension: Schema.optional(ExtensionRequestSchema),
  allExtensionNumbersOfType: Schema.optional(Schema.String),
  listServices: Schema.optional(Schema.String),
});
export type ServerReflectionRequest = Schema.Schema.Type<
  typeof ServerReflectionRequestSchema
>;

/**
 * Answer to file queries. Each element is a base64-encoded serialized
 * `google.protobuf.FileDescriptorProto`; the requested file comes first,
 * followed by its transitive imports.
 */
export const FileDescriptorResponseSchema = Schema.Struct({
  fileDescriptorProto: Schema.Array(Schema.String),
});
export type FileDescriptorResponse = Schema.Schema.Type<
  typeof FileDescriptorResponseSchema
>;

export const ExtensionNumberResponseSchema = Schema.Struct({
  baseTypeName: Schema.String,
  extensionNumber: Schema.Array(Schema.Number),
});
export type ExtensionNumberResponse = Schema.Schema.Type<
  typeof ExtensionNumberResponseSchema
>;

export const ListServiceResponseSchema = Schema.Struct({
  service: Schema.Array(Schema.Struct({ name: Schema.String })),
});
export type ListServiceResponse = Schema.Schema.Type<
  typeof ListServiceResponseSchema
>;

/** In-band error, carrying `grpc::StatusCode` values (e.g. 5 = NOT_FOUND). */
export const ErrorResponseSchema = Schema.Struct({
  errorCode: Schema.Number,
  errorMessage: Schema.String,
});
export type ErrorResponse = Schema.Schema.Type<typeof ErrorResponseSchema>;

/**
 * `grpc.reflection.v1.ServerReflectionResponse`. The proto `message_response`
 * oneof is flattened into optional fields; exactly one is set.
 */
export const ServerReflectionResponseSchema = Schema.Struct({
  validHost: Schema.String,
  originalRequest: Schema.optional(ServerReflectionRequestSchema),
  fileDescriptorResponse: Schema.optional(FileDescriptorResponseSchema),
  allExtensionNumbersResponse: Schema.optional(ExtensionNumberResponseSchema),
  listServicesResponse: Schema.optional(ListServiceResponseSchema),
  errorResponse: Schema.optional(ErrorResponseSchema),
});
export type ServerReflectionResponse = Schema.Schema.Type<
  typeof ServerReflectionResponseSchema
>;

export const ReflectionV1Tag =
  "grpc.reflection.v1.ServerReflection/ServerReflectionInfo";
export const ReflectionV1AlphaTag =
  "grpc.reflection.v1alpha.ServerReflection/ServerReflectionInfo";

const readField = (message: unknown, field: string): unknown =>
  typeof message === "object" && message !== null
    ? (message as Record<string, unknown>)[field]
    : undefined;

const readString = (message: unknown, field: string): string =>
  (readField(message, field) ?? "") as string;

const readNumber = (message: unknown, field: string): number =>
  (readField(message, field) ?? 0) as number;

type OneofField = { readonly case?: string; readonly value?: unknown };

const fromReflectionRequest = (message: unknown): unknown => {
  const request: Record<string, unknown> = {
    host: readString(message, "host"),
  };
  const oneof = readField(message, "messageRequest") as OneofField | undefined;
  switch (oneof?.case) {
    case "fileByFilename":
      request.fileByFilename = oneof.value ?? "";
      break;
    case "fileContainingSymbol":
      request.fileContainingSymbol = oneof.value ?? "";
      break;
    case "fileContainingExtension":
      request.fileContainingExtension = {
        containingType: readString(oneof.value, "containingType"),
        extensionNumber: readNumber(oneof.value, "extensionNumber"),
      };
      break;
    case "allExtensionNumbersOfType":
      request.allExtensionNumbersOfType = oneof.value ?? "";
      break;
    case "listServices":
      request.listServices = oneof.value ?? "";
      break;
  }
  return request;
};

const toReflectionRequest = (value: unknown): Record<string, unknown> => {
  const extension = readField(value, "fileContainingExtension");
  const messageRequest: OneofField =
    readField(value, "fileByFilename") !== undefined
      ? { case: "fileByFilename", value: readField(value, "fileByFilename") }
      : readField(value, "fileContainingSymbol") !== undefined
        ? {
            case: "fileContainingSymbol",
            value: readField(value, "fileContainingSymbol"),
          }
        : extension !== undefined
          ? {
              case: "fileContainingExtension",
              value: {
                containingType: readString(extension, "containingType"),
                extensionNumber: readNumber(extension, "extensionNumber"),
              },
            }
          : readField(value, "allExtensionNumbersOfType") !== undefined
            ? {
                case: "allExtensionNumbersOfType",
                value: readField(value, "allExtensionNumbersOfType"),
              }
            : readField(value, "listServices") !== undefined
              ? {
                  case: "listServices",
                  value: readField(value, "listServices"),
                }
              : { case: undefined };
  return { host: readString(value, "host"), messageRequest };
};

const toReflectionResponse = (value: unknown): Record<string, unknown> => {
  const originalRequest = readField(value, "originalRequest");
  const files = readField(value, "fileDescriptorResponse");
  const extensionNumbers = readField(value, "allExtensionNumbersResponse");
  const listServices = readField(value, "listServicesResponse");
  const error = readField(value, "errorResponse");
  const messageResponse: OneofField =
    files !== undefined
      ? {
          case: "fileDescriptorResponse",
          value: {
            fileDescriptorProto: (
              (readField(files, "fileDescriptorProto") ??
                []) as ReadonlyArray<string>
            ).map(base64Decode),
          },
        }
      : extensionNumbers !== undefined
        ? {
            case: "allExtensionNumbersResponse",
            value: {
              baseTypeName: readString(extensionNumbers, "baseTypeName"),
              extensionNumber: [
                ...((readField(extensionNumbers, "extensionNumber") ??
                  []) as ReadonlyArray<number>),
              ],
            },
          }
        : listServices !== undefined
          ? {
              case: "listServicesResponse",
              value: {
                service: (
                  (readField(listServices, "service") ??
                    []) as ReadonlyArray<unknown>
                ).map((entry) => ({ name: readString(entry, "name") })),
              },
            }
          : error !== undefined
            ? {
                case: "errorResponse",
                value: {
                  errorCode: readNumber(error, "errorCode"),
                  errorMessage: readString(error, "errorMessage"),
                },
              }
            : { case: undefined };
  return {
    validHost: readString(value, "validHost"),
    ...(originalRequest !== undefined
      ? { originalRequest: toReflectionRequest(originalRequest) }
      : {}),
    messageResponse,
  };
};

const fromReflectionResponse = (message: unknown): unknown => {
  const response: Record<string, unknown> = {
    validHost: readString(message, "validHost"),
  };
  const original = readField(message, "originalRequest");
  if (original !== undefined) {
    response.originalRequest = fromReflectionRequest(original);
  }
  const oneof = readField(message, "messageResponse") as OneofField | undefined;
  switch (oneof?.case) {
    case "fileDescriptorResponse":
      response.fileDescriptorResponse = {
        fileDescriptorProto: (
          (readField(oneof.value, "fileDescriptorProto") ??
            []) as ReadonlyArray<Uint8Array>
        ).map((bytes) => base64Encode(bytes)),
      };
      break;
    case "allExtensionNumbersResponse":
      response.allExtensionNumbersResponse = {
        baseTypeName: readString(oneof.value, "baseTypeName"),
        extensionNumber: [
          ...((readField(oneof.value, "extensionNumber") ??
            []) as ReadonlyArray<number>),
        ],
      };
      break;
    case "listServicesResponse":
      response.listServicesResponse = {
        service: (
          (readField(oneof.value, "service") ?? []) as ReadonlyArray<unknown>
        ).map((entry) => ({ name: readString(entry, "name") })),
      };
      break;
    case "errorResponse":
      response.errorResponse = {
        errorCode: readNumber(oneof.value, "errorCode"),
        errorMessage: readString(oneof.value, "errorMessage"),
      };
      break;
  }
  return response;
};

const reflectionEntry = (
  tag: string,
  service: GrpcMethodRegistry.GrpcMethodEntry["service"],
): GrpcMethodRegistry.GrpcMethodEntry => ({
  kind: "bidi-streaming",
  tag,
  service,
  localName: "serverReflectionInfo",
  payloadSchema: ServerReflectionRequestSchema,
  successSchema: ServerReflectionResponseSchema,
  toGrpcRequest: toReflectionRequest,
  fromGrpcRequest: fromReflectionRequest,
  toGrpcResponse: toReflectionResponse,
  fromGrpcResponse: fromReflectionResponse,
});

export const ReflectionGrpcRegistry = new Map<
  string,
  GrpcMethodRegistry.GrpcMethodEntry
>([
  [
    ReflectionV1Tag,
    reflectionEntry(ReflectionV1Tag, ReflectionV1Pb.ServerReflection),
  ],
  [
    ReflectionV1AlphaTag,
    reflectionEntry(ReflectionV1AlphaTag, ReflectionV1AlphaPb.ServerReflection),
  ],
]);

interface IndexedFile {
  /**
   * Base64-encoded serialized `FileDescriptorProto`s: the file itself first,
   * followed by its transitive imports.
   */
  readonly closure: ReadonlyArray<string>;
}

/**
 * Prebuilt lookup tables answering every reflection query, derived from the
 * descriptors carried by the method registries. Build one with
 * {@link makeIndex}; {@link service} does so automatically.
 */
export interface ReflectionIndex {
  readonly serviceNames: ReadonlyArray<string>;
  readonly filesByName: ReadonlyMap<string, IndexedFile>;
  readonly filesBySymbol: ReadonlyMap<string, IndexedFile>;
  /** Keyed by `<extendee typeName>:<field number>`. */
  readonly filesByExtension: ReadonlyMap<string, IndexedFile>;
  readonly extensionNumbers: ReadonlyMap<string, ReadonlyArray<number>>;
  /** Message type names, for `all_extension_numbers_of_type` existence checks. */
  readonly messageTypes: ReadonlySet<string>;
}

const fileName = (file: DescFile): string => file.proto.name ?? "";

/**
 * Builds a {@link ReflectionIndex} from method registries. Walks every
 * registered service's file and its transitive imports, indexing files by
 * name, declared symbols (services, methods, messages, enums, extensions),
 * and extension declarations.
 */
export const makeIndex = (
  registries: Iterable<GrpcMethodRegistry.GrpcMethodRegistry>,
): ReflectionIndex => {
  const files = new Map<string, DescFile>();
  const serviceNames = new Set<string>();
  const visitFile = (file: DescFile) => {
    if (files.has(fileName(file))) return;
    files.set(fileName(file), file);
    for (const dependency of file.dependencies) visitFile(dependency);
  };
  for (const registry of registries) {
    for (const entry of registry.values()) {
      serviceNames.add(entry.service.typeName);
      visitFile(entry.service.file);
    }
  }

  const serialized = new Map<string, string>();
  for (const [name, file] of files) {
    serialized.set(
      name,
      base64Encode(toBinary(FileDescriptorProtoSchema, file.proto)),
    );
  }
  const closureOf = (file: DescFile): ReadonlyArray<string> => {
    const seen = new Set<string>();
    const closure: string[] = [];
    const visit = (current: DescFile) => {
      if (seen.has(fileName(current))) return;
      seen.add(fileName(current));
      closure.push(serialized.get(fileName(current)) as string);
      for (const dependency of current.dependencies) visit(dependency);
    };
    visit(file);
    return closure;
  };

  const filesByName = new Map<string, IndexedFile>();
  const filesBySymbol = new Map<string, IndexedFile>();
  const filesByExtension = new Map<string, IndexedFile>();
  const extensionNumbers = new Map<string, number[]>();
  const messageTypes = new Set<string>();

  for (const file of files.values()) {
    const indexed: IndexedFile = { closure: closureOf(file) };
    filesByName.set(fileName(file), indexed);
    const registerExtension = (extension: DescExtension) => {
      filesBySymbol.set(extension.typeName, indexed);
      filesByExtension.set(
        `${extension.extendee.typeName}:${extension.number}`,
        indexed,
      );
      const numbers = extensionNumbers.get(extension.extendee.typeName) ?? [];
      numbers.push(extension.number);
      extensionNumbers.set(extension.extendee.typeName, numbers);
    };
    const visitMessage = (message: DescMessage) => {
      filesBySymbol.set(message.typeName, indexed);
      messageTypes.add(message.typeName);
      for (const nested of message.nestedMessages) visitMessage(nested);
      for (const nested of message.nestedEnums) {
        filesBySymbol.set(nested.typeName, indexed);
      }
      for (const nested of message.nestedExtensions) registerExtension(nested);
    };
    for (const service of file.services) {
      filesBySymbol.set(service.typeName, indexed);
      for (const method of service.methods) {
        filesBySymbol.set(`${service.typeName}.${method.name}`, indexed);
      }
    }
    for (const message of file.messages) visitMessage(message);
    for (const enumType of file.enums) {
      filesBySymbol.set(enumType.typeName, indexed);
    }
    for (const extension of file.extensions) registerExtension(extension);
  }

  return {
    serviceNames: [...serviceNames].sort(),
    filesByName,
    filesBySymbol,
    filesByExtension,
    extensionNumbers,
    messageTypes,
  };
};

const NOT_FOUND = GrpcStatusCode.toConnectCode("not_found") as number;
const INVALID_ARGUMENT = GrpcStatusCode.toConnectCode(
  "invalid_argument",
) as number;

const stripLeadingDot = (symbol: string): string =>
  symbol.startsWith(".") ? symbol.slice(1) : symbol;

/**
 * Answers a single reflection request from a prebuilt index, per the server
 * reflection spec: file queries return the file plus its transitive imports,
 * unknown names produce an in-band `errorResponse` with `NOT_FOUND` (never a
 * stream failure), and the original request is echoed back on every response.
 */
export const respond = (
  index: ReflectionIndex,
  request: ServerReflectionRequest,
): ServerReflectionResponse => {
  const base = { validHost: request.host, originalRequest: request };
  const notFound = (message: string): ServerReflectionResponse => ({
    ...base,
    errorResponse: { errorCode: NOT_FOUND, errorMessage: message },
  });
  const found = (
    file: IndexedFile | undefined,
    message: string,
  ): ServerReflectionResponse =>
    file === undefined
      ? notFound(message)
      : {
          ...base,
          fileDescriptorResponse: { fileDescriptorProto: file.closure },
        };

  if (request.listServices !== undefined) {
    return {
      ...base,
      listServicesResponse: {
        service: index.serviceNames.map((name) => ({ name })),
      },
    };
  }
  if (request.fileByFilename !== undefined) {
    return found(
      index.filesByName.get(request.fileByFilename),
      `file not found: ${request.fileByFilename}`,
    );
  }
  if (request.fileContainingSymbol !== undefined) {
    const symbol = stripLeadingDot(request.fileContainingSymbol);
    return found(
      index.filesBySymbol.get(symbol),
      `symbol not found: ${symbol}`,
    );
  }
  if (request.fileContainingExtension !== undefined) {
    const { containingType, extensionNumber } = request.fileContainingExtension;
    const extendee = stripLeadingDot(containingType);
    return found(
      index.filesByExtension.get(`${extendee}:${extensionNumber}`),
      `extension not found: ${extendee} (${extensionNumber})`,
    );
  }
  if (request.allExtensionNumbersOfType !== undefined) {
    const baseTypeName = stripLeadingDot(request.allExtensionNumbersOfType);
    if (!index.messageTypes.has(baseTypeName)) {
      return notFound(`type not found: ${baseTypeName}`);
    }
    return {
      ...base,
      allExtensionNumbersResponse: {
        baseTypeName,
        extensionNumber: index.extensionNumbers.get(baseTypeName) ?? [],
      },
    };
  }
  return {
    ...base,
    errorResponse: {
      errorCode: INVALID_ARGUMENT,
      errorMessage: "no message_request set",
    },
  };
};

/**
 * Ready-made entry for `GrpcNodeServer.serveAll`: registers the
 * `grpc.reflection.v1.ServerReflection` service (and its `v1alpha` alias)
 * answering from the descriptors of `services` — pass the same array you pass
 * to `serveAll`. The reflection service describes itself, so it does not need
 * to appear in its own input.
 */
export const service = (
  services: ReadonlyArray<ServeAllService<any>>,
): ServeAllService => {
  const index = makeIndex([
    ...services.map((entry) => entry.registry),
    ReflectionGrpcRegistry,
  ]);
  const handler = (
    requests: Stream.Stream<unknown, GrpcStatusError.GrpcStatusError>,
  ) =>
    Stream.map(requests, (request) =>
      respond(index, request as ServerReflectionRequest),
    );
  return {
    // The only method is bidi-streaming, which bypasses Effect RPC, so the
    // group is empty; the cast bridges RpcGroup's invariance on `never`.
    group: RpcGroup.make() as unknown as RpcGroup.RpcGroup<any>,
    registry: ReflectionGrpcRegistry,
    handlers: GrpcServerProtocol.streamingHandlersLayer({
      [ReflectionV1Tag]: { kind: "bidi-streaming", handler },
      [ReflectionV1AlphaTag]: { kind: "bidi-streaming", handler },
    }),
  };
};

export type ReflectionClientError =
  | GrpcStatusError.GrpcStatusError
  | RpcClientError.RpcClientError;

/**
 * Client for the `grpc.reflection.v1.ServerReflection` service of a remote
 * server, shaped like the clients emitted by `protoc-gen-effect-grpc`.
 */
export interface ReflectionClientService {
  readonly serverReflectionInfo: <E>(
    requests: Stream.Stream<ServerReflectionRequest, E>,
    options?: CodegenSupport.GrpcCallOptions,
  ) => Stream.Stream<ServerReflectionResponse, ReflectionClientError | E>;
}

const makeReflectionClient = Effect.gen(function* () {
  const streaming = yield* GrpcClientProtocol.GrpcStreamingClient;
  return {
    serverReflectionInfo: ((requests, options) =>
      streaming.bidiStreaming(
        ReflectionV1Tag,
        requests,
        options,
      )) as ReflectionClientService["serverReflectionInfo"],
  } satisfies ReflectionClientService;
});

export class ReflectionClient extends Context.Service<
  ReflectionClient,
  ReflectionClientService
>()("grpc.reflection.v1.ServerReflection/ReflectionClient", {
  make: makeReflectionClient,
}) {}

/**
 * Provides {@link ReflectionClient}. Include {@link ReflectionGrpcRegistry}
 * in the registry passed to `GrpcClientProtocol.layer`.
 */
export const ReflectionClientLayer = Layer.effect(
  ReflectionClient,
  ReflectionClient.make,
);
