import {
  serviceHandlersLayerName,
  serviceImplementationName,
} from "./naming.js";
import type { GeneratorFile, MethodModel } from "./types.js";

export const generateServer = (file: GeneratorFile) =>
  file.services.flatMap((service) => [
    `export interface ${serviceImplementationName(service.name)}<R = never> {`,
    ...service.methods.map(
      (method) =>
        `  readonly ${method.localName}: ${implementationSignature(method)};`,
    ),
    "}",
    "",
    `export const ${serviceHandlersLayerName(service.name)} = <R>(`,
    `  implementation: ${serviceImplementationName(service.name)}<R>,`,
    `): Layer.Layer<GrpcServerProtocol.GrpcHandlers, never, R> =>`,
    "  GrpcServerProtocol.handlersLayer<R>({",
    ...service.methods.flatMap((method) => [
      `    "${service.typeName}/${method.name}": {`,
      `      kind: "${method.kind}",`,
      `      handler: ${handlerBinding(method)},`,
      "    },",
    ]),
    "  });",
    "",
  ]);

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

// The handlers map is untyped (`unknown` values); the `as` casts pin the
// domain types the implementation signature promises.
const handlerBinding = (method: MethodModel): string => {
  switch (method.kind) {
    case "unary":
    case "server-streaming":
      return `(request, context) => implementation.${method.localName}(request as ${method.inputType}, context)`;
    case "client-streaming":
    case "bidi-streaming":
      return `(requests, context) => implementation.${method.localName}(requests as Stream.Stream<${method.inputType}, GrpcStatusError.GrpcStatusError>, context)`;
  }
};
