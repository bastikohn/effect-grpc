import {
  serviceHandlersLayerName,
  serviceImplementationName,
  serviceRpcsName,
} from "./naming.js";
import type { GeneratorFile } from "./types.js";

export const generateServer = (file: GeneratorFile) =>
  file.services.flatMap((service) => [
    `export interface ${serviceImplementationName(service.name)}<R = never> {`,
    ...service.methods.map((method) => {
      const returnType =
        method.kind === "server-streaming"
          ? `Stream.Stream<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`
          : `Effect.Effect<${method.outputType}, GrpcStatusError.GrpcStatusError, R>`;
      return `  readonly ${method.localName}: (request: ${method.inputType}, context: CodegenSupport.GrpcServerContext) => ${returnType};`;
    }),
    "}",
    "",
    `export const ${serviceHandlersLayerName(service.name)} = <R>(`,
    `  implementation: ${serviceImplementationName(service.name)}<R>,`,
    `): Layer.Layer<Rpc.ToHandler<${serviceRpcsName(service.name)}>, never, R> =>`,
    `  ${service.name}RpcGroup.toLayer({`,
    ...service.methods.map(
      (method) =>
        `    "${service.typeName}/${method.name}": (request, options) => implementation.${method.localName}(request, CodegenSupport.serverContext(options)),`,
    ),
    "  }) as Layer.Layer<Rpc.ToHandler<",
    `    ${serviceRpcsName(service.name)}`,
    "  >, never, R>;",
    "",
  ]);
