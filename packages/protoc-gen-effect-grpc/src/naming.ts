export const effectFileName = (protoFileName: string) =>
  protoFileName.endsWith(".proto")
    ? `${protoFileName.slice(0, -".proto".length)}_effect_grpc.ts`
    : `${protoFileName}_effect_grpc.ts`;

export const serviceClientServiceName = (serviceName: string) =>
  `${serviceName}ClientService`;

export const serviceClientName = (serviceName: string) =>
  `${serviceName}Client`;

export const serviceClientLayerName = (serviceName: string) =>
  `${serviceName}ClientLayer`;

export const serviceImplementationName = (serviceName: string) =>
  `${serviceName}Implementation`;

export const serviceHandlersName = (serviceName: string) =>
  `${serviceName}Handlers`;

export const serviceRegistryName = (serviceName: string) =>
  `${serviceName}GrpcRegistry`;

/**
 * Every identifier the generator introduces itself — base64/oneof helpers, the
 * schemas, types and converters standing in for a well-known method type —
 * lives under a single `Grpc$` namespace. `$` is legal in TypeScript
 * identifiers but never in protobuf ones, so no `.proto` declaration can reach
 * these names, whatever it calls its messages.
 */
export const grpcGeneratedName = (name: string) => `Grpc$${name}`;

export const grpcWellKnownName = (protobufName: string) =>
  grpcGeneratedName(`GoogleProtobuf${protobufName}`);

export const grpcEmptyName = grpcWellKnownName("Empty");
