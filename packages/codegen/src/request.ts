import { create } from "@bufbuild/protobuf";
import {
  CodeGeneratorRequestSchema,
  type CodeGeneratorRequest,
  type FileDescriptorSet,
} from "@bufbuild/protobuf/wkt";

/**
 * Assemble the {@link CodeGeneratorRequest} the plugin expects: the full set of
 * descriptors in `protoFile`, the user's target files in `fileToGenerate`, and
 * the serialized options in `parameter`.
 */
export const buildCodeGeneratorRequest = (
  set: FileDescriptorSet,
  fileToGenerate: ReadonlyArray<string>,
  parameter: string,
): CodeGeneratorRequest =>
  create(CodeGeneratorRequestSchema, {
    fileToGenerate: [...fileToGenerate],
    parameter,
    protoFile: set.file,
  });
