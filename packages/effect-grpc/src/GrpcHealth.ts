import {
  Context,
  Effect,
  Layer,
  Schema,
  Stream,
  SubscriptionRef,
} from "effect";

import type * as CodegenSupport from "./CodegenSupport.js";
import * as GrpcInvoker from "./GrpcInvoker.js";
import type * as GrpcMethodRegistry from "./GrpcMethodRegistry.js";
import type { ServeAllService } from "./GrpcNodeServer.js";
import * as GrpcServerProtocol from "./GrpcServerProtocol.js";
import * as GrpcStatusError from "./GrpcStatusError.js";
import * as HealthPb from "./internal/healthPb.js";

/**
 * Standard gRPC Health Checking Protocol (`grpc.health.v1.Health`), see
 * https://github.com/grpc/grpc/blob/master/doc/health-checking.md.
 *
 * The {@link GrpcHealth} service holds a per-service status map; {@link layer}
 * provides it, and {@link service} plugs the `Health` RPCs into
 * `GrpcNodeServer.serveAll` next to the application services:
 *
 * ```ts
 * GrpcNodeServer.serveAll({
 *   host, port,
 *   services: [userService, GrpcHealth.service],
 * }).pipe(Effect.provide(GrpcHealth.layer()))
 * ```
 *
 * Applications flip statuses through the service:
 *
 * ```ts
 * const health = yield* GrpcHealth.GrpcHealth;
 * yield* health.set("demo.v1.UserService", "SERVING");
 * ```
 */

/**
 * Serving status of a single service, as defined by
 * `grpc.health.v1.HealthCheckResponse.ServingStatus`. `SERVICE_UNKNOWN` is
 * only reported by `Watch` for services the server does not know about.
 */
export const ServingStatusSchema = Schema.Literals([
  "UNKNOWN",
  "SERVING",
  "NOT_SERVING",
  "SERVICE_UNKNOWN",
]);
export type ServingStatus = Schema.Schema.Type<typeof ServingStatusSchema>;

export const HealthCheckRequestSchema = Schema.Struct({
  service: Schema.String,
});
export type HealthCheckRequest = Schema.Schema.Type<
  typeof HealthCheckRequestSchema
>;

export const HealthCheckResponseSchema = Schema.Struct({
  status: ServingStatusSchema,
});
export type HealthCheckResponse = Schema.Schema.Type<
  typeof HealthCheckResponseSchema
>;

const servingStatusCodes: Record<ServingStatus, HealthPb.ServingStatusCode> = {
  UNKNOWN: 0,
  SERVING: 1,
  NOT_SERVING: 2,
  SERVICE_UNKNOWN: 3,
};

const servingStatusFromCode = (code: unknown): ServingStatus => {
  switch (code) {
    case 1:
      return "SERVING";
    case 2:
      return "NOT_SERVING";
    case 3:
      return "SERVICE_UNKNOWN";
    default:
      return "UNKNOWN";
  }
};

const readField = (message: unknown, field: string): unknown =>
  typeof message === "object" && message !== null
    ? (message as Record<string, unknown>)[field]
    : undefined;

const fromHealthCheckRequest = (message: unknown): unknown => ({
  service: (readField(message, "service") ?? "") as string,
});

const toHealthCheckRequest = (value: unknown): Record<string, unknown> => ({
  service: (readField(value, "service") ?? "") as string,
});

const fromHealthCheckResponse = (message: unknown): unknown => ({
  status: servingStatusFromCode(readField(message, "status")),
});

const toHealthCheckResponse = (value: unknown): Record<string, unknown> => ({
  status:
    servingStatusCodes[readField(value, "status") as ServingStatus] ??
    servingStatusCodes.UNKNOWN,
});

export const HealthGrpcRegistry = new Map<
  string,
  GrpcMethodRegistry.GrpcMethodEntry
>([
  [
    "grpc.health.v1.Health/Check",
    {
      kind: "unary",
      tag: "grpc.health.v1.Health/Check",
      service: HealthPb.Health,
      localName: "check",
      payloadSchema: HealthCheckRequestSchema,
      successSchema: HealthCheckResponseSchema,
      toGrpcRequest: toHealthCheckRequest,
      fromGrpcRequest: fromHealthCheckRequest,
      toGrpcResponse: toHealthCheckResponse,
      fromGrpcResponse: fromHealthCheckResponse,
    },
  ],
  [
    "grpc.health.v1.Health/Watch",
    {
      kind: "server-streaming",
      tag: "grpc.health.v1.Health/Watch",
      service: HealthPb.Health,
      localName: "watch",
      payloadSchema: HealthCheckRequestSchema,
      successSchema: HealthCheckResponseSchema,
      toGrpcRequest: toHealthCheckRequest,
      fromGrpcRequest: fromHealthCheckRequest,
      toGrpcResponse: toHealthCheckResponse,
      fromGrpcResponse: fromHealthCheckResponse,
    },
  ],
]);

/**
 * Mutable per-service health state backing the `grpc.health.v1.Health`
 * handlers. The empty-string service name (`""`) is the overall server
 * status, per the health checking spec.
 *
 * `check` follows `Health/Check` semantics: unknown services fail with
 * `not_found`. `watch` follows `Health/Watch` semantics: it emits the current
 * status immediately — `SERVICE_UNKNOWN` for unknown services — and then a
 * new element on every effective change (consecutive duplicates are
 * suppressed).
 */
export interface GrpcHealthService {
  /** Snapshot of every registered service status. */
  readonly statuses: Effect.Effect<ReadonlyMap<string, ServingStatus>>;
  /**
   * Current status of `service` (defaults to `""`, the overall server).
   * Fails with `not_found` when the service is not registered.
   */
  readonly check: (
    service?: string,
  ) => Effect.Effect<ServingStatus, GrpcStatusError.GrpcStatusError>;
  /**
   * Current status of `service` followed by every status change.
   * Unregistered services yield `SERVICE_UNKNOWN` (and resume with real
   * statuses once registered).
   */
  readonly watch: (service?: string) => Stream.Stream<ServingStatus>;
  /** Register `service` or update its status. Use `""` for the server. */
  readonly set: (service: string, status: ServingStatus) => Effect.Effect<void>;
  /** Unregister `service`: `check` fails, watchers see `SERVICE_UNKNOWN`. */
  readonly clear: (service: string) => Effect.Effect<void>;
}

export class GrpcHealth extends Context.Service<
  GrpcHealth,
  GrpcHealthService
>()("@effect-grpc/effect-grpc/GrpcHealth") {}

export interface GrpcHealthOptions {
  /**
   * Statuses registered when the service is built. Defaults to marking the
   * overall server (`""`) as `SERVING`, mirroring the reference
   * implementations (e.g. grpc-go's `health.NewServer`).
   */
  readonly initialStatuses?: Iterable<readonly [string, ServingStatus]>;
}

export const make = (
  options?: GrpcHealthOptions,
): Effect.Effect<GrpcHealthService> =>
  Effect.gen(function* () {
    const statuses = yield* SubscriptionRef.make<
      ReadonlyMap<string, ServingStatus>
    >(new Map(options?.initialStatuses ?? [["", "SERVING"]]));

    const check = (service = "") =>
      SubscriptionRef.get(statuses).pipe(
        Effect.flatMap((map) => {
          const status = map.get(service);
          return status === undefined
            ? Effect.fail(
                GrpcStatusError.notFound(`unknown service: ${service}`),
              )
            : Effect.succeed(status);
        }),
      );

    const watch = (service = "") =>
      SubscriptionRef.changes(statuses).pipe(
        Stream.map(
          (map): ServingStatus => map.get(service) ?? "SERVICE_UNKNOWN",
        ),
        Stream.changes,
      );

    const set = (service: string, status: ServingStatus) =>
      SubscriptionRef.update(
        statuses,
        (map) =>
          new Map(map).set(service, status) as ReadonlyMap<
            string,
            ServingStatus
          >,
      );

    const clear = (service: string) =>
      SubscriptionRef.update(statuses, (map) => {
        if (!map.has(service)) return map;
        const next = new Map(map);
        next.delete(service);
        return next;
      });

    return {
      statuses: SubscriptionRef.get(statuses),
      check,
      watch,
      set,
      clear,
    } satisfies GrpcHealthService;
  });

/** Provides {@link GrpcHealth} backed by an in-memory status map. */
export const layer = (options?: GrpcHealthOptions): Layer.Layer<GrpcHealth> =>
  Layer.effect(GrpcHealth, make(options));

/**
 * Handlers for the `grpc.health.v1.Health` RPCs, reading statuses from
 * {@link GrpcHealth}.
 */
export const HealthHandlersLayer: Layer.Layer<
  GrpcServerProtocol.GrpcHandlers,
  never,
  GrpcHealth
> = GrpcServerProtocol.handlersLayer<GrpcHealth>({
  "grpc.health.v1.Health/Check": {
    kind: "unary",
    handler: (request) =>
      Effect.gen(function* () {
        const health = yield* GrpcHealth;
        const status = yield* health.check(
          (request as HealthCheckRequest).service,
        );
        return { status } satisfies HealthCheckResponse;
      }),
  },
  "grpc.health.v1.Health/Watch": {
    kind: "server-streaming",
    handler: (request) =>
      Stream.unwrap(
        Effect.gen(function* () {
          const health = yield* GrpcHealth;
          return Stream.map(
            health.watch((request as HealthCheckRequest).service),
            (status): HealthCheckResponse => ({ status }),
          );
        }),
      ),
  },
});

/**
 * Ready-made entry for `GrpcNodeServer.serveAll`: registers the
 * `grpc.health.v1.Health` service next to the application services. Requires
 * {@link GrpcHealth} (provide it with {@link layer}).
 */
export const service: ServeAllService<GrpcHealth> = {
  registry: HealthGrpcRegistry,
  handlers: HealthHandlersLayer,
};

export type HealthClientError = GrpcStatusError.GrpcStatusError;

/**
 * Client for the `grpc.health.v1.Health` service of a remote server, shaped
 * like the clients emitted by `protoc-gen-effect-grpc`.
 */
export interface HealthClientService {
  readonly check: (
    request: HealthCheckRequest,
    options?: CodegenSupport.GrpcCallOptions,
  ) => Effect.Effect<HealthCheckResponse, HealthClientError>;
  readonly watch: (
    request: HealthCheckRequest,
    options?: CodegenSupport.GrpcCallOptions,
  ) => Stream.Stream<HealthCheckResponse, HealthClientError>;
}

const makeHealthClient = Effect.gen(function* () {
  const invoker = yield* GrpcInvoker.GrpcInvoker;
  return {
    check: ((request, options) =>
      invoker.unary(
        "grpc.health.v1.Health/Check",
        request,
        options,
      )) as HealthClientService["check"],
    watch: ((request, options) =>
      invoker.serverStream(
        "grpc.health.v1.Health/Watch",
        request,
        options,
      )) as HealthClientService["watch"],
  } satisfies HealthClientService;
});

export class HealthClient extends Context.Service<
  HealthClient,
  HealthClientService
>()("grpc.health.v1.Health/HealthClient", { make: makeHealthClient }) {}

/**
 * Provides {@link HealthClient}. Include {@link HealthGrpcRegistry} in the
 * registry passed to `GrpcClientProtocol.layer`.
 */
export const HealthClientLayer = Layer.effect(HealthClient, HealthClient.make);
