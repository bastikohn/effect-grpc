import type { DescExtension, DescFile, DescMessage } from "@bufbuild/protobuf";
import { toBinary } from "@bufbuild/protobuf";
import { base64Decode, base64Encode } from "@bufbuild/protobuf/wire";
import { FileDescriptorProtoSchema } from "@bufbuild/protobuf/wkt";
import { Context, Effect, Layer, Schema, Stream } from "effect";

import type * as CodegenSupport from "./CodegenSupport.js";
import * as GrpcInvoker from "./GrpcInvoker.js";
import type * as GrpcMethodRegistry from "./GrpcMethodRegistry.js";
import type { ServeAllService } from "./GrpcNodeServer.js";
import * as GrpcServerProtocol from "./GrpcServerProtocol.js";
import * as GrpcStatusCode from "./GrpcStatusCode.js";
import type * as GrpcStatusError from "./GrpcStatusError.js";
import * as ReflectionPb from "./internal/reflectionPb.js";

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
 */

/**
 * `grpc.reflection.v1.ServerReflectionRequest`. The `message_request` oneof
 * uses the `{case, value}` representation the generator emits.
 */
export const ServerReflectionRequestSchema = Schema.Struct({
  host: Schema.String,
  messageRequest: Schema.Union([
    Schema.Struct({
      case: Schema.Literal("fileByFilename"),
      value: Schema.String,
    }),
    Schema.Struct({
      case: Schema.Literal("fileContainingSymbol"),
      value: Schema.String,
    }),
    Schema.Struct({
      case: Schema.Literal("fileContainingExtension"),
      value: Schema.Struct({
        containingType: Schema.String,
        extensionNumber: Schema.Number,
      }),
    }),
    Schema.Struct({
      case: Schema.Literal("allExtensionNumbersOfType"),
      value: Schema.String,
    }),
    Schema.Struct({
      case: Schema.Literal("listServices"),
      value: Schema.String,
    }),
    Schema.Struct({
      case: Schema.Undefined,
      value: Schema.optional(Schema.Undefined),
    }),
  ]),
});
export type ServerReflectionRequest = Schema.Schema.Type<
  typeof ServerReflectionRequestSchema
>;

/**
 * `grpc.reflection.v1.ServerReflectionResponse`. Each `fileDescriptorProto`
 * is a serialized `google.protobuf.FileDescriptorProto`: the requested file
 * first, followed by its transitive imports.
 */
export const ServerReflectionResponseSchema = Schema.Struct({
  validHost: Schema.String,
  originalRequest: Schema.optional(ServerReflectionRequestSchema),
  messageResponse: Schema.Union([
    Schema.Struct({
      case: Schema.Literal("fileDescriptorResponse"),
      value: Schema.Struct({
        fileDescriptorProto: Schema.Array(Schema.Uint8Array),
      }),
    }),
    Schema.Struct({
      case: Schema.Literal("allExtensionNumbersResponse"),
      value: Schema.Struct({
        baseTypeName: Schema.String,
        extensionNumber: Schema.Array(Schema.Number),
      }),
    }),
    Schema.Struct({
      case: Schema.Literal("listServicesResponse"),
      value: Schema.Struct({
        service: Schema.Array(Schema.Struct({ name: Schema.String })),
      }),
    }),
    /** In-band error carrying `grpc::StatusCode` values (e.g. 5 = NOT_FOUND). */
    Schema.Struct({
      case: Schema.Literal("errorResponse"),
      value: Schema.Struct({
        errorCode: Schema.Number,
        errorMessage: Schema.String,
      }),
    }),
    Schema.Struct({
      case: Schema.Undefined,
      value: Schema.optional(Schema.Undefined),
    }),
  ]),
});
export type ServerReflectionResponse = Schema.Schema.Type<
  typeof ServerReflectionResponseSchema
>;

export const ReflectionV1Tag =
  "grpc.reflection.v1.ServerReflection/ServerReflectionInfo";

const readField = (message: unknown, field: string): unknown =>
  typeof message === "object" && message !== null
    ? (message as Record<string, unknown>)[field]
    : undefined;

type OneofField = { readonly case?: string | null; readonly value?: unknown };

/*
 * The domain shape is protobuf-es' own `{case, value}`, so the converters only
 * reconcile two spellings: protobuf-es leaves an unset oneof as
 * `{case: undefined}` while the JSON codec spells `Schema.Undefined` as
 * `null`, and `bytes` is a `Uint8Array` on the wire but base64 in JSON.
 */
const fromOneof = (oneof: unknown): unknown => {
  const { case: kind, value } = (oneof ?? {}) as OneofField;
  return kind == null ? { case: null } : { case: kind, value };
};

const toOneof = (oneof: unknown): unknown => {
  const { case: kind, value } = (oneof ?? {}) as OneofField;
  return kind == null ? { case: undefined } : { case: kind, value };
};

const descriptorsOf = <A>(value: unknown): ReadonlyArray<A> =>
  (readField(value, "fileDescriptorProto") ?? []) as ReadonlyArray<A>;

const fromReflectionRequest = (message: unknown): unknown => ({
  host: (readField(message, "host") ?? "") as string,
  messageRequest: fromOneof(readField(message, "messageRequest")),
});

const toReflectionRequest = (value: unknown): Record<string, unknown> => ({
  host: (readField(value, "host") ?? "") as string,
  messageRequest: toOneof(readField(value, "messageRequest")),
});

const fromReflectionResponse = (message: unknown): unknown => {
  const original = readField(message, "originalRequest");
  const oneof = readField(message, "messageResponse") as OneofField | undefined;
  return {
    validHost: (readField(message, "validHost") ?? "") as string,
    ...(original == null
      ? {}
      : { originalRequest: fromReflectionRequest(original) }),
    messageResponse:
      oneof?.case === "fileDescriptorResponse"
        ? {
            case: oneof.case,
            value: {
              fileDescriptorProto: descriptorsOf<Uint8Array>(oneof.value).map(
                (bytes) => base64Encode(bytes),
              ),
            },
          }
        : fromOneof(oneof),
  };
};

const toReflectionResponse = (value: unknown): Record<string, unknown> => {
  const original = readField(value, "originalRequest");
  const oneof = readField(value, "messageResponse") as OneofField | undefined;
  return {
    validHost: (readField(value, "validHost") ?? "") as string,
    ...(original == null
      ? {}
      : { originalRequest: toReflectionRequest(original) }),
    messageResponse:
      oneof?.case === "fileDescriptorResponse"
        ? {
            case: oneof.case,
            value: {
              fileDescriptorProto: descriptorsOf<string>(oneof.value).map(
                (value) => base64Decode(value),
              ),
            },
          }
        : toOneof(oneof),
  };
};

export const ReflectionGrpcRegistry = new Map<
  string,
  GrpcMethodRegistry.GrpcMethodEntry
>([
  [
    ReflectionV1Tag,
    {
      kind: "bidi-streaming",
      tag: ReflectionV1Tag,
      service: ReflectionPb.ServerReflectionV1,
      localName: "serverReflectionInfo",
      payloadSchema: ServerReflectionRequestSchema,
      successSchema: ServerReflectionResponseSchema,
      toGrpcRequest: toReflectionRequest,
      fromGrpcRequest: fromReflectionRequest,
      toGrpcResponse: toReflectionResponse,
      fromGrpcResponse: fromReflectionResponse,
    },
  ],
]);

interface IndexedFile {
  /**
   * Serialized `FileDescriptorProto`s: the file itself first, followed by its
   * transitive imports.
   */
  readonly closure: ReadonlyArray<Uint8Array>;
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

  const serialized = new Map<string, Uint8Array>();
  for (const [name, file] of files) {
    serialized.set(name, toBinary(FileDescriptorProtoSchema, file.proto));
  }
  const closureOf = (file: DescFile): ReadonlyArray<Uint8Array> => {
    const seen = new Set<string>();
    const closure: Uint8Array[] = [];
    const visit = (current: DescFile) => {
      if (seen.has(fileName(current))) return;
      seen.add(fileName(current));
      closure.push(serialized.get(fileName(current)) as Uint8Array);
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
    messageResponse: {
      case: "errorResponse",
      value: { errorCode: NOT_FOUND, errorMessage: message },
    },
  });
  const found = (
    file: IndexedFile | undefined,
    message: string,
  ): ServerReflectionResponse =>
    file === undefined
      ? notFound(message)
      : {
          ...base,
          messageResponse: {
            case: "fileDescriptorResponse",
            value: { fileDescriptorProto: file.closure },
          },
        };

  const query = request.messageRequest;
  switch (query.case) {
    case "listServices":
      return {
        ...base,
        messageResponse: {
          case: "listServicesResponse",
          value: { service: index.serviceNames.map((name) => ({ name })) },
        },
      };
    case "fileByFilename":
      return found(
        index.filesByName.get(query.value),
        `file not found: ${query.value}`,
      );
    case "fileContainingSymbol": {
      const symbol = stripLeadingDot(query.value);
      return found(
        index.filesBySymbol.get(symbol),
        `symbol not found: ${symbol}`,
      );
    }
    case "fileContainingExtension": {
      const extendee = stripLeadingDot(query.value.containingType);
      return found(
        index.filesByExtension.get(
          `${extendee}:${query.value.extensionNumber}`,
        ),
        `extension not found: ${extendee} (${query.value.extensionNumber})`,
      );
    }
    case "allExtensionNumbersOfType": {
      const baseTypeName = stripLeadingDot(query.value);
      if (!index.messageTypes.has(baseTypeName)) {
        return notFound(`type not found: ${baseTypeName}`);
      }
      return {
        ...base,
        messageResponse: {
          case: "allExtensionNumbersResponse",
          value: {
            baseTypeName,
            extensionNumber: index.extensionNumbers.get(baseTypeName) ?? [],
          },
        },
      };
    }
    default:
      return {
        ...base,
        messageResponse: {
          case: "errorResponse",
          value: {
            errorCode: INVALID_ARGUMENT,
            errorMessage: "no message_request set",
          },
        },
      };
  }
};

/**
 * Ready-made entry for `GrpcNodeServer.serveAll`: registers the
 * `grpc.reflection.v1.ServerReflection` service answering from the descriptors
 * of `services` — pass the same array you pass to `serveAll`. The reflection
 * service describes itself, so it does not need to appear in its own input.
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
    registry: ReflectionGrpcRegistry,
    handlers: GrpcServerProtocol.handlersLayer({
      [ReflectionV1Tag]: { kind: "bidi-streaming", handler },
    }),
  };
};

export type ReflectionClientError = GrpcStatusError.GrpcStatusError;

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
  const invoker = yield* GrpcInvoker.GrpcInvoker;
  return {
    serverReflectionInfo: ((requests, options) =>
      invoker.bidiStream(
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
