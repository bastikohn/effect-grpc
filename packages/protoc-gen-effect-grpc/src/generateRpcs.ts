import { rpcConstName, serviceGroupName, serviceRpcsName } from "./naming.js";
import { isRequestStreaming, type GeneratorFile } from "./types.js";

export const generateRpcs = (file: GeneratorFile) =>
  file.services.flatMap((service) => {
    const rpcMethods = service.methods.filter(
      (method) => !isRequestStreaming(method),
    );
    return [
      ...rpcMethods.flatMap((method) => [
        `export const ${rpcConstName(service.name, method.name)} = Rpc.make("${service.typeName}/${method.name}", {`,
        `  payload: ${method.inputType}Schema,`,
        `  success: ${method.outputType}Schema,`,
        "  error: GrpcStatusError.GrpcStatusError,",
        ...(method.kind === "server-streaming" ? ["  stream: true,"] : []),
        "});",
        "",
      ]),
      `export const ${serviceGroupName(service.name)} = RpcGroup.make(${rpcMethods.map((method) => rpcConstName(service.name, method.name)).join(", ")});`,
      `export type ${serviceRpcsName(service.name)} = ${
        rpcMethods.length > 0
          ? rpcMethods
              .map(
                (method) => `typeof ${rpcConstName(service.name, method.name)}`,
              )
              .join(" | ")
          : "never"
      };`,
      "",
    ];
  });
