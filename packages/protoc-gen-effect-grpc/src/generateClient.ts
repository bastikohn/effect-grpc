import type { Printable } from "@bufbuild/protoplugin";

import {
  serviceClientLayerName,
  serviceClientName,
  serviceClientServiceName,
} from "./naming.js";
import { exportDecl, methodTypeRef } from "./printing.js";
import * as sym from "./symbols.js";
import type { GeneratorFile, MethodModel, ServiceModel } from "./types.js";

export const generateClient = (file: GeneratorFile): ReadonlyArray<Printable> =>
  file.services.flatMap((service): ReadonlyArray<Printable> => [
    [
      exportDecl("type", `${service.name}ClientError`),
      " = ",
      sym.GrpcStatusError,
      ".GrpcStatusError;",
    ],
    "",
    [exportDecl("interface", serviceClientServiceName(service.name)), " {"],
    ...service.methods.map((method): Printable => [
      `  readonly ${method.localName}: `,
      clientMethodSignature(service, method),
      ";",
    ]),
    "}",
    "",
    [
      `const make${serviceClientName(service.name)} = `,
      sym.Effect,
      ".gen(function* () {",
    ],
    ["  const invoker = yield* ", sym.GrpcInvoker, ".GrpcInvoker;"],
    "  return {",
    ...service.methods.map((method) => clientMethodImpl(service, method)),
    `  } satisfies ${serviceClientServiceName(service.name)};`,
    "});",
    "",
    [
      exportDecl("class", serviceClientName(service.name)),
      " extends ",
      sym.Context,
      `.Service<${serviceClientName(service.name)}, ${serviceClientServiceName(service.name)}>()("${service.typeName}/${serviceClientName(service.name)}", {`,
    ],
    `  make: make${serviceClientName(service.name)},`,
    "}) {}",
    "",
    [
      exportDecl("const", serviceClientLayerName(service.name)),
      " = ",
      sym.Layer,
      `.effect(${serviceClientName(service.name)}, ${serviceClientName(service.name)}.make);`,
    ],
    "",
  ]);

const clientMethodSignature = (
  service: ServiceModel,
  method: MethodModel,
): Printable => {
  const clientError = `${service.name}ClientError`;
  const input = methodTypeRef(method.inputType);
  const output = methodTypeRef(method.outputType);
  switch (method.kind) {
    case "unary":
      return [
        "(request: ",
        input,
        ", options?: ",
        sym.CodegenSupport,
        ".GrpcCallOptions) => ",
        sym.Effect,
        ".Effect<",
        output,
        `, ${clientError}>`,
      ];
    case "server-streaming":
      return [
        "(request: ",
        input,
        ", options?: ",
        sym.CodegenSupport,
        ".GrpcCallOptions) => ",
        sym.Stream,
        ".Stream<",
        output,
        `, ${clientError}>`,
      ];
    case "client-streaming":
      return [
        "<E>(requests: ",
        sym.Stream,
        ".Stream<",
        input,
        ", E>, options?: ",
        sym.CodegenSupport,
        ".GrpcCallOptions) => ",
        sym.Effect,
        ".Effect<",
        output,
        `, ${clientError} | E>`,
      ];
    case "bidi-streaming":
      return [
        "<E>(requests: ",
        sym.Stream,
        ".Stream<",
        input,
        ", E>, options?: ",
        sym.CodegenSupport,
        ".GrpcCallOptions) => ",
        sym.Stream,
        ".Stream<",
        output,
        `, ${clientError} | E>`,
      ];
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
