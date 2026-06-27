import { rpcConstName, serviceGroupName, serviceRpcsName } from "./naming.js";
import type { GeneratorFile } from "./types.js";

export const generateRpcs = (file: GeneratorFile) =>
  file.services.flatMap((service) => [
    ...service.methods.flatMap((method) => [
      `export const ${rpcConstName(service.name, method.name)} = Rpc.make("${service.typeName}/${method.name}", {`,
      `  payload: ${method.inputType}Schema,`,
      `  success: ${method.outputType}Schema,`,
      "  error: GrpcStatusError.GrpcStatusError,",
      ...(method.kind === "server-streaming" ? ["  stream: true,"] : []),
      "});",
      "",
    ]),
    `export const ${serviceGroupName(service.name)} = RpcGroup.make(${service.methods.map((method) => rpcConstName(service.name, method.name)).join(", ")});`,
    `export type ${serviceRpcsName(service.name)} = ${service.methods.map((method) => `typeof ${rpcConstName(service.name, method.name)}`).join(" | ")};`,
    "",
  ]);
