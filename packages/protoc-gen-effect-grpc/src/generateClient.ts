import {
  serviceClientLayerName,
  serviceClientName,
  serviceClientServiceName,
} from "./naming.js";
import type { GeneratorFile, MethodModel, ServiceModel } from "./types.js";

export const generateClient = (file: GeneratorFile) =>
  file.services.flatMap((service) => [
    `export type ${service.name}ClientError = GrpcStatusError.GrpcStatusError;`,
    "",
    `export interface ${serviceClientServiceName(service.name)} {`,
    ...service.methods.map(
      (method) =>
        `  readonly ${method.localName}: ${clientMethodSignature(service, method)};`,
    ),
    "}",
    "",
    `const make${serviceClientName(service.name)} = Effect.gen(function* () {`,
    "  const invoker = yield* GrpcInvoker.GrpcInvoker;",
    "  return {",
    ...service.methods.map((method) => clientMethodImpl(service, method)),
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

const clientMethodSignature = (
  service: ServiceModel,
  method: MethodModel,
): string => {
  const clientError = `${service.name}ClientError`;
  switch (method.kind) {
    case "unary":
      return `(request: ${method.inputType.name}, options?: CodegenSupport.GrpcCallOptions) => Effect.Effect<${method.outputType.name}, ${clientError}>`;
    case "server-streaming":
      return `(request: ${method.inputType.name}, options?: CodegenSupport.GrpcCallOptions) => Stream.Stream<${method.outputType.name}, ${clientError}>`;
    case "client-streaming":
      return `<E>(requests: Stream.Stream<${method.inputType.name}, E>, options?: CodegenSupport.GrpcCallOptions) => Effect.Effect<${method.outputType.name}, ${clientError} | E>`;
    case "bidi-streaming":
      return `<E>(requests: Stream.Stream<${method.inputType.name}, E>, options?: CodegenSupport.GrpcCallOptions) => Stream.Stream<${method.outputType.name}, ${clientError} | E>`;
  }
};

// Every method delegates to the {@link GrpcInvoker} seam, which returns
// `unknown` — the `as` cast pins the domain type the signature promises.
const clientMethodImpl = (
  service: ServiceModel,
  method: MethodModel,
): string => {
  const tag = `${service.typeName}/${method.name}`;
  const methodType = `${serviceClientServiceName(service.name)}["${method.localName}"]`;
  switch (method.kind) {
    case "unary":
      return `    ${method.localName}: ((request, options) => invoker.unary("${tag}", request, options)) as ${methodType},`;
    case "server-streaming":
      return `    ${method.localName}: ((request, options) => invoker.serverStream("${tag}", request, options)) as ${methodType},`;
    case "client-streaming":
      return `    ${method.localName}: ((requests, options) => invoker.clientStream("${tag}", requests, options)) as ${methodType},`;
    case "bidi-streaming":
      return `    ${method.localName}: ((requests, options) => invoker.bidiStream("${tag}", requests, options)) as ${methodType},`;
  }
};
