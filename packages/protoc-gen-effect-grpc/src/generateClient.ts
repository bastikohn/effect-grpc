import {
  serviceClientLayerName,
  serviceClientName,
  serviceClientServiceName,
  serviceGroupName,
} from "./naming.js";
import type { GeneratorFile } from "./types.js";

export const generateClient = (file: GeneratorFile) =>
  file.services.flatMap((service) => [
    `export type ${service.name}ClientError = GrpcStatusError.GrpcStatusError | RpcClientError.RpcClientError;`,
    "",
    `export interface ${serviceClientServiceName(service.name)} {`,
    ...service.methods.flatMap((method) => {
      const returnType =
        method.kind === "server-streaming"
          ? `Stream.Stream<${method.outputType}, ${service.name}ClientError>`
          : `Effect.Effect<${method.outputType}, ${service.name}ClientError>`;
      return [
        `  readonly ${method.localName}: (request: ${method.inputType}, options?: CodegenSupport.GrpcCallOptions) => ${returnType};`,
      ];
    }),
    "}",
    "",
    `const make${serviceClientName(service.name)} = Effect.gen(function* () {`,
    `  const client = yield* RpcClient.make(${serviceGroupName(service.name)});`,
    "  return {",
    ...service.methods.map(
      (method) =>
        `    ${method.localName}: (request, options) => client["${service.typeName}/${method.name}"](request, { headers: CodegenSupport.headersFromOptions(options) }),`,
    ),
    `  } satisfies ${serviceClientServiceName(service.name)};`,
    "});",
    "",
    `export class ${serviceClientName(service.name)} extends Context.Service<${serviceClientName(service.name)}, ${serviceClientServiceName(service.name)}>()("${service.typeName}/${serviceClientName(service.name)}", {`,
    `  make: make${serviceClientName(service.name)},`,
    "}) {}",
    "",
    `export const ${serviceClientLayerName(service.name)} = Layer.effect(${serviceClientName(service.name)}, ${serviceClientName(service.name)}.make);`,
    "",
  ]);
