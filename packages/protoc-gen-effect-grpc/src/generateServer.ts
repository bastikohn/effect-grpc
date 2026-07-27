import type { Printable } from "@bufbuild/protoplugin";

import { serviceHandlersName, serviceImplementationName } from "./naming.js";
import { exportDecl, methodTypeRef } from "./printing.js";
import * as sym from "./symbols.js";
import type { GeneratorFile, MethodModel } from "./types.js";

export const generateServer = (file: GeneratorFile): ReadonlyArray<Printable> =>
  file.services.flatMap((service): ReadonlyArray<Printable> => [
    [
      exportDecl("interface", serviceImplementationName(service.name)),
      "<R = never> {",
    ],
    ...service.methods.map((method): Printable => [
      `  readonly ${method.localName}: `,
      implementationSignature(method),
      ";",
    ]),
    "}",
    "",
    [exportDecl("const", serviceHandlersName(service.name)), " = <R>("],
    `  implementation: ${serviceImplementationName(service.name)}<R>,`,
    [
      "): ",
      sym.Effect,
      ".Effect<",
      sym.GrpcServerProtocol,
      ".GrpcHandlers, never, R> =>",
    ],
    ["  ", sym.GrpcServerProtocol, ".handlersEffect<R>({"],
    ...service.methods.flatMap((method): ReadonlyArray<Printable> => [
      `    "${service.typeName}/${method.name}": {`,
      `      kind: "${method.kind}",`,
      ["      handler: ", handlerBinding(method), ","],
      "    },",
    ]),
    "  });",
    "",
  ]);

const implementationSignature = (method: MethodModel): Printable => {
  const input = methodTypeRef(method.inputType);
  const output = methodTypeRef(method.outputType);
  switch (method.kind) {
    case "unary":
      return [
        "(request: ",
        input,
        ", context: ",
        sym.CodegenSupport,
        ".GrpcServerContext) => ",
        sym.Effect,
        ".Effect<",
        output,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError, R>",
      ];
    case "server-streaming":
      return [
        "(request: ",
        input,
        ", context: ",
        sym.CodegenSupport,
        ".GrpcServerContext) => ",
        sym.Stream,
        ".Stream<",
        output,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError, R>",
      ];
    case "client-streaming":
      return [
        "(requests: ",
        sym.Stream,
        ".Stream<",
        input,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError>, context: ",
        sym.CodegenSupport,
        ".GrpcServerContext) => ",
        sym.Effect,
        ".Effect<",
        output,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError, R>",
      ];
    case "bidi-streaming":
      return [
        "(requests: ",
        sym.Stream,
        ".Stream<",
        input,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError>, context: ",
        sym.CodegenSupport,
        ".GrpcServerContext) => ",
        sym.Stream,
        ".Stream<",
        output,
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError, R>",
      ];
  }
};

// The handlers map is untyped (`unknown` values); the `as` casts pin the
// domain types the implementation signature promises.
const handlerBinding = (method: MethodModel): Printable => {
  switch (method.kind) {
    case "unary":
    case "server-streaming":
      return [
        `(request, context) => implementation.${method.localName}(request as `,
        methodTypeRef(method.inputType),
        ", context)",
      ];
    case "client-streaming":
    case "bidi-streaming":
      return [
        `(requests, context) => implementation.${method.localName}(requests as `,
        sym.Stream,
        ".Stream<",
        methodTypeRef(method.inputType),
        ", ",
        sym.GrpcStatusError,
        ".GrpcStatusError>, context)",
      ];
  }
};
