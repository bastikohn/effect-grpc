import type { Message } from "@bufbuild/protobuf";
import type {
  GenFile,
  GenMessage,
  GenService,
} from "@bufbuild/protobuf/codegenv2";
import {
  fileDesc,
  messageDesc,
  serviceDesc,
} from "@bufbuild/protobuf/codegenv2";

/**
 * Describes the file grpc/health/v1/health.proto.
 *
 * Hand-committed descriptor of the standard gRPC Health Checking Protocol
 * (https://github.com/grpc/grpc/blob/master/doc/health-checking.md), produced
 * by `protoc-gen-es` from the canonical `grpc/health/v1/health.proto`
 * (Copyright 2015 The gRPC Authors, Apache-2.0). The messages are frozen by
 * the spec, so the descriptor is vendored instead of generated at build time.
 */
export const file_grpc_health_v1_health: GenFile = fileDesc(
  "ChtncnBjL2hlYWx0aC92MS9oZWFsdGgucHJvdG8SDmdycGMuaGVhbHRoLnYxIiUKEkhlYWx0aENoZWNrUmVxdWVzdBIPCgdzZXJ2aWNlGAEgASgJIqkBChNIZWFsdGhDaGVja1Jlc3BvbnNlEkEKBnN0YXR1cxgBIAEoDjIxLmdycGMuaGVhbHRoLnYxLkhlYWx0aENoZWNrUmVzcG9uc2UuU2VydmluZ1N0YXR1cyJPCg1TZXJ2aW5nU3RhdHVzEgsKB1VOS05PV04QABILCgdTRVJWSU5HEAESDwoLTk9UX1NFUlZJTkcQAhITCg9TRVJWSUNFX1VOS05PV04QAzKuAQoGSGVhbHRoElAKBUNoZWNrEiIuZ3JwYy5oZWFsdGgudjEuSGVhbHRoQ2hlY2tSZXF1ZXN0GiMuZ3JwYy5oZWFsdGgudjEuSGVhbHRoQ2hlY2tSZXNwb25zZRJSCgVXYXRjaBIiLmdycGMuaGVhbHRoLnYxLkhlYWx0aENoZWNrUmVxdWVzdBojLmdycGMuaGVhbHRoLnYxLkhlYWx0aENoZWNrUmVzcG9uc2UwAWIGcHJvdG8z",
);

/**
 * Wire values of `grpc.health.v1.HealthCheckResponse.ServingStatus`.
 */
export type ServingStatusCode = 0 | 1 | 2 | 3;

/**
 * @generated from message grpc.health.v1.HealthCheckRequest
 */
export type HealthCheckRequestPb =
  Message<"grpc.health.v1.HealthCheckRequest"> & {
    service: string;
  };

/**
 * Describes the message grpc.health.v1.HealthCheckRequest.
 */
export const HealthCheckRequestPbSchema: GenMessage<HealthCheckRequestPb> =
  messageDesc(file_grpc_health_v1_health, 0);

/**
 * @generated from message grpc.health.v1.HealthCheckResponse
 */
export type HealthCheckResponsePb =
  Message<"grpc.health.v1.HealthCheckResponse"> & {
    status: number;
  };

/**
 * Describes the message grpc.health.v1.HealthCheckResponse.
 */
export const HealthCheckResponsePbSchema: GenMessage<HealthCheckResponsePb> =
  messageDesc(file_grpc_health_v1_health, 1);

/**
 * @generated from service grpc.health.v1.Health
 */
export const Health: GenService<{
  check: {
    methodKind: "unary";
    input: typeof HealthCheckRequestPbSchema;
    output: typeof HealthCheckResponsePbSchema;
  };
  watch: {
    methodKind: "server_streaming";
    input: typeof HealthCheckRequestPbSchema;
    output: typeof HealthCheckResponsePbSchema;
  };
}> = serviceDesc(file_grpc_health_v1_health, 0);
