import { dirname, relative } from "node:path/posix";

export const lowerFirst = (value: string) =>
  value.length === 0 ? value : value[0]!.toLowerCase() + value.slice(1);

export const protoBaseName = (protoFileName: string) => {
  const slash = protoFileName.lastIndexOf("/");
  const fileName = slash >= 0 ? protoFileName.slice(slash + 1) : protoFileName;
  return fileName.endsWith(".proto")
    ? fileName.slice(0, -".proto".length)
    : fileName;
};

export const pbImportPath = (protoFileName: string, extension: "js" | "ts") =>
  `./${protoBaseName(protoFileName)}_pb.${extension}`;

export const effectFileName = (protoFileName: string) =>
  protoFileName.endsWith(".proto")
    ? `${protoFileName.slice(0, -".proto".length)}_effect_grpc.ts`
    : `${protoFileName}_effect_grpc.ts`;

export const effectImportPath = (
  fromProtoFileName: string,
  toProtoFileName: string,
  extension: "js" | "ts",
) => {
  const path = relative(
    dirname(fromProtoFileName),
    effectFileName(toProtoFileName),
  ).replace(/\.ts$/, `.${extension}`);
  return path.startsWith(".") ? path : `./${path}`;
};

export const rpcConstName = (serviceName: string, methodName: string) =>
  `${serviceName}_${methodName}Rpc`;

export const serviceGroupName = (serviceName: string) =>
  `${serviceName}RpcGroup`;

export const serviceRpcsName = (serviceName: string) => `${serviceName}Rpcs`;

export const serviceClientServiceName = (serviceName: string) =>
  `${serviceName}ClientService`;

export const serviceClientName = (serviceName: string) =>
  `${serviceName}Client`;

export const serviceClientLayerName = (serviceName: string) =>
  `${serviceName}ClientLayer`;

export const serviceImplementationName = (serviceName: string) =>
  `${serviceName}Implementation`;

export const serviceHandlersLayerName = (serviceName: string) =>
  `${serviceName}HandlersLayer`;

export const serviceRegistryName = (serviceName: string) =>
  `${serviceName}GrpcRegistry`;

export const grpcWellKnownName = (protobufName: string) =>
  `GrpcGoogleProtobuf${protobufName}`;

export const grpcEmptyName = grpcWellKnownName("Empty");
export const grpcTimestampName = grpcWellKnownName("Timestamp");
export const grpcDurationName = grpcWellKnownName("Duration");
export const grpcBoolValueName = grpcWellKnownName("BoolValue");
