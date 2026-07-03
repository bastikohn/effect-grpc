import {
  serviceHandlersLayerName,
  serviceImplementationName,
  serviceRpcsName,
} from "./naming.js";
import {
  isRequestStreaming,
  type GeneratorFile,
  type MethodModel,
  type ServiceModel,
} from "./types.js";

export const generateServer = (file: GeneratorFile) =>
  file.services.flatMap((service) => {
    const rpcMethods = service.methods.filter(
      (method) => !isRequestStreaming(method),
    );
    const streamingMethods = service.methods.filter(isRequestStreaming);
    return [
      `export interface ${serviceImplementationName(service.name)}<R = never> {`,
      ...service.methods.map(
        (method) =>
          `  readonly ${method.localName}: ${implementationSignature(method)};`,
      ),
      "}",
      "",
      `export const ${serviceHandlersLayerName(service.name)} = <R>(`,
      `  implementation: ${serviceImplementationName(service.name)}<R>,`,
      `): Layer.Layer<${handlersLayerProvides(service, rpcMethods, streamingMethods)}, never, R> =>`,
      ...handlersLayerBody(service, rpcMethods, streamingMethods),
      "",
    ];
  });

const implementationSignature = (method: MethodModel): string => {
  switch (method.kind) {
    case "unary":
      return `(request: ${method.inputType}, context: CodegenSupport.GrpcServerContext) => Effect.Effect<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`;
    case "server-streaming":
      return `(request: ${method.inputType}, context: CodegenSupport.GrpcServerContext) => Stream.Stream<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`;
    case "client-streaming":
      return `(requests: Stream.Stream<${method.inputType}, GrpcStatusError.GrpcStatusError>, context: CodegenSupport.GrpcServerContext) => Effect.Effect<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`;
    case "bidi-streaming":
      return `(requests: Stream.Stream<${method.inputType}, GrpcStatusError.GrpcStatusError>, context: CodegenSupport.GrpcServerContext) => Stream.Stream<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`;
  }
};

const handlersLayerProvides = (
  service: ServiceModel,
  rpcMethods: ReadonlyArray<MethodModel>,
  streamingMethods: ReadonlyArray<MethodModel>,
): string =>
  [
    ...(rpcMethods.length > 0
      ? [`Rpc.ToHandler<${serviceRpcsName(service.name)}>`]
      : []),
    ...(streamingMethods.length > 0
      ? ["GrpcServerProtocol.GrpcStreamingHandlers"]
      : []),
  ].join(" | ");

const handlersLayerBody = (
  service: ServiceModel,
  rpcMethods: ReadonlyArray<MethodModel>,
  streamingMethods: ReadonlyArray<MethodModel>,
): ReadonlyArray<string> => {
  const rpcLayer = [
    `${service.name}RpcGroup.toLayer({`,
    ...rpcMethods.map(
      (method) =>
        `  "${service.typeName}/${method.name}": (request, options) => implementation.${method.localName}(request, CodegenSupport.serverContext(options)),`,
    ),
    `}) as Layer.Layer<Rpc.ToHandler<${serviceRpcsName(service.name)}>, never, R>`,
  ];
  const streamingLayer = [
    "GrpcServerProtocol.streamingHandlersLayer<R>({",
    ...streamingMethods.flatMap((method) => [
      `  "${service.typeName}/${method.name}": {`,
      `    kind: "${method.kind}",`,
      `    handler: (requests, context) => implementation.${method.localName}(requests as Stream.Stream<${method.inputType}, GrpcStatusError.GrpcStatusError>, context),`,
      "  },",
    ]),
    "})",
  ];
  if (streamingMethods.length === 0) {
    return indent(rpcLayer, "  ", ";");
  }
  if (rpcMethods.length === 0) {
    return indent(streamingLayer, "  ", ";");
  }
  return [
    "  Layer.mergeAll(",
    ...indent(rpcLayer, "    ", ","),
    ...indent(streamingLayer, "    ", ","),
    "  );",
  ];
};

const indent = (
  lines: ReadonlyArray<string>,
  prefix: string,
  suffix: string,
): ReadonlyArray<string> =>
  lines.map((line, index) =>
    index === lines.length - 1
      ? `${prefix}${line}${suffix}`
      : `${prefix}${line}`,
  );
