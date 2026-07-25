// Copyright 2016 The gRPC Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Hand-committed descriptor of the standard gRPC Server Reflection Protocol
// (https://github.com/grpc/grpc/blob/master/doc/server-reflection.md), produced
// by `protoc-gen-es` from the canonical
// `grpc/reflection/v1/reflection.proto` (Copyright 2016 The gRPC Authors,
// Apache-2.0). The protocol is frozen, so the descriptor is vendored instead
// of generated at build time.
//
// Only the service descriptor is needed: the connect router and the wire
// codecs resolve against the `FileDescriptorProto` below, and this package
// models the reflection messages with Effect `Schema` in `GrpcReflection.ts`.

import type { DescService } from "@bufbuild/protobuf";
import { fileDesc } from "@bufbuild/protobuf/codegenv2";

/**
 * Describes the file grpc/reflection/v1/reflection.proto.
 */
const file_grpc_reflection_v1_reflection = fileDesc(
  "CiNncnBjL3JlZmxlY3Rpb24vdjEvcmVmbGVjdGlvbi5wcm90bxISZ3JwYy5yZWZsZWN0aW9uLnYxIoUCChdTZXJ2ZXJSZWZsZWN0aW9uUmVxdWVzdBIMCgRob3N0GAEgASgJEhoKEGZpbGVfYnlfZmlsZW5hbWUYAyABKAlIABIgChZmaWxlX2NvbnRhaW5pbmdfc3ltYm9sGAQgASgJSAASSQoZZmlsZV9jb250YWluaW5nX2V4dGVuc2lvbhgFIAEoCzIkLmdycGMucmVmbGVjdGlvbi52MS5FeHRlbnNpb25SZXF1ZXN0SAASJwodYWxsX2V4dGVuc2lvbl9udW1iZXJzX29mX3R5cGUYBiABKAlIABIXCg1saXN0X3NlcnZpY2VzGAcgASgJSABCEQoPbWVzc2FnZV9yZXF1ZXN0IkUKEEV4dGVuc2lvblJlcXVlc3QSFwoPY29udGFpbmluZ190eXBlGAEgASgJEhgKEGV4dGVuc2lvbl9udW1iZXIYAiABKAUiuAMKGFNlcnZlclJlZmxlY3Rpb25SZXNwb25zZRISCgp2YWxpZF9ob3N0GAEgASgJEkUKEG9yaWdpbmFsX3JlcXVlc3QYAiABKAsyKy5ncnBjLnJlZmxlY3Rpb24udjEuU2VydmVyUmVmbGVjdGlvblJlcXVlc3QSTgoYZmlsZV9kZXNjcmlwdG9yX3Jlc3BvbnNlGAQgASgLMiouZ3JwYy5yZWZsZWN0aW9uLnYxLkZpbGVEZXNjcmlwdG9yUmVzcG9uc2VIABJVCh5hbGxfZXh0ZW5zaW9uX251bWJlcnNfcmVzcG9uc2UYBSABKAsyKy5ncnBjLnJlZmxlY3Rpb24udjEuRXh0ZW5zaW9uTnVtYmVyUmVzcG9uc2VIABJJChZsaXN0X3NlcnZpY2VzX3Jlc3BvbnNlGAYgASgLMicuZ3JwYy5yZWZsZWN0aW9uLnYxLkxpc3RTZXJ2aWNlUmVzcG9uc2VIABI7Cg5lcnJvcl9yZXNwb25zZRgHIAEoCzIhLmdycGMucmVmbGVjdGlvbi52MS5FcnJvclJlc3BvbnNlSABCEgoQbWVzc2FnZV9yZXNwb25zZSI3ChZGaWxlRGVzY3JpcHRvclJlc3BvbnNlEh0KFWZpbGVfZGVzY3JpcHRvcl9wcm90bxgBIAMoDCJLChdFeHRlbnNpb25OdW1iZXJSZXNwb25zZRIWCg5iYXNlX3R5cGVfbmFtZRgBIAEoCRIYChBleHRlbnNpb25fbnVtYmVyGAIgAygFIksKE0xpc3RTZXJ2aWNlUmVzcG9uc2USNAoHc2VydmljZRgBIAMoCzIjLmdycGMucmVmbGVjdGlvbi52MS5TZXJ2aWNlUmVzcG9uc2UiHwoPU2VydmljZVJlc3BvbnNlEgwKBG5hbWUYASABKAkiOgoNRXJyb3JSZXNwb25zZRISCgplcnJvcl9jb2RlGAEgASgFEhUKDWVycm9yX21lc3NhZ2UYAiABKAkyiQEKEFNlcnZlclJlZmxlY3Rpb24SdQoUU2VydmVyUmVmbGVjdGlvbkluZm8SKy5ncnBjLnJlZmxlY3Rpb24udjEuU2VydmVyUmVmbGVjdGlvblJlcXVlc3QaLC5ncnBjLnJlZmxlY3Rpb24udjEuU2VydmVyUmVmbGVjdGlvblJlc3BvbnNlKAEwAUJmChVpby5ncnBjLnJlZmxlY3Rpb24udjFCFVNlcnZlclJlZmxlY3Rpb25Qcm90b1ABWjRnb29nbGUuZ29sYW5nLm9yZy9ncnBjL3JlZmxlY3Rpb24vZ3JwY19yZWZsZWN0aW9uX3YxYgZwcm90bzM",
);

/**
 * Describes the service grpc.reflection.v1.ServerReflection.
 */
export const ServerReflectionV1: DescService =
  file_grpc_reflection_v1_reflection.services.find(
    (service) => service.typeName === "grpc.reflection.v1.ServerReflection",
  )!;
