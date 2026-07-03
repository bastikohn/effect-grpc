import {
  serviceClientLayerName,
  serviceClientName,
  serviceClientServiceName,
  serviceGroupName,
} from "./naming.js";
import {
  isRequestStreaming,
  type GeneratorFile,
  type MethodModel,
  type ServiceModel,
} from "./types.js";

export const generateClient = (file: GeneratorFile) =>
  file.services.flatMap((service) => {
    const hasRpcMethods = service.methods.some(
      (method) => !isRequestStreaming(method),
    );
    const hasStreamingMethods = service.methods.some(isRequestStreaming);
    return [
      `export type ${service.name}ClientError = GrpcStatusError.GrpcStatusError | RpcClientError.RpcClientError;`,
      "",
      `export interface ${serviceClientServiceName(service.name)} {`,
      ...service.methods.map(
        (method) =>
          `  readonly ${method.localName}: ${clientMethodSignature(service, method)};`,
      ),
      "}",
      "",
      `const make${serviceClientName(service.name)} = Effect.gen(function* () {`,
      ...(hasRpcMethods
        ? [
            // `flatten: true` keeps the full "<package>.<Service>/<Method>"
            // tags callable; the default client shape would group them by the
            // package prefix.
            `  const client = yield* RpcClient.make(${serviceGroupName(service.name)}, { flatten: true });`,
          ]
        : []),
      ...(hasStreamingMethods
        ? ["  const streaming = yield* GrpcClientProtocol.GrpcStreamingClient;"]
        : []),
      "  return {",
      ...service.methods.map((method) => clientMethodImpl(service, method)),
      `  } satisfies ${serviceClientServiceName(service.name)};`,
      "});",
      "",
      `export class ${serviceClientName(service.name)} extends Context.Tag("${service.typeName}/${serviceClientName(service.name)}")<${serviceClientName(service.name)}, ${serviceClientServiceName(service.name)}>() {`,
      `  static readonly make = make${serviceClientName(service.name)};`,
      "}",
      "",
      `export const ${serviceClientLayerName(service.name)} = Layer.scoped(${serviceClientName(service.name)}, ${serviceClientName(service.name)}.make);`,
      "",
    ];
  });

const clientMethodSignature = (
  service: ServiceModel,
  method: MethodModel,
): string => {
  const clientError = `${service.name}ClientError`;
  switch (method.kind) {
    case "unary":
      return `(request: ${method.inputType}, options?: CodegenSupport.GrpcCallOptions) => Effect.Effect<${method.outputType}, ${clientError}>`;
    case "server-streaming":
      return `(request: ${method.inputType}, options?: CodegenSupport.GrpcCallOptions) => Stream.Stream<${method.outputType}, ${clientError}>`;
    case "client-streaming":
      return `<E>(requests: Stream.Stream<${method.inputType}, E>, options?: CodegenSupport.GrpcCallOptions) => Effect.Effect<${method.outputType}, ${clientError} | E>`;
    case "bidi-streaming":
      return `<E>(requests: Stream.Stream<${method.inputType}, E>, options?: CodegenSupport.GrpcCallOptions) => Stream.Stream<${method.outputType}, ${clientError} | E>`;
  }
};

const clientMethodImpl = (
  service: ServiceModel,
  method: MethodModel,
): string => {
  const tag = `${service.typeName}/${method.name}`;
  const methodType = `${serviceClientServiceName(service.name)}["${method.localName}"]`;
  switch (method.kind) {
    case "unary":
    case "server-streaming":
      return `    ${method.localName}: (request, options) => client("${tag}", request, { headers: CodegenSupport.headersFromOptions(options) }),`;
    case "client-streaming":
      return `    ${method.localName}: ((requests, options) => streaming.clientStreaming("${tag}", requests, options)) as ${methodType},`;
    case "bidi-streaming":
      return `    ${method.localName}: ((requests, options) => streaming.bidiStreaming("${tag}", requests, options)) as ${methodType},`;
  }
};
