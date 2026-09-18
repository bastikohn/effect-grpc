import { Deferred, Effect, Fiber, Stream } from "effect";
import { describe, expect, it } from "vitest";

import {
  type CodegenSupport,
  GrpcClientProtocol,
  GrpcInvoker,
  GrpcMethodRegistry,
  GrpcStatusError,
  type GrpcMetadata,
} from "@effect-grpc/effect-grpc";
import {
  UserServiceGrpcRegistry,
  UserServiceHandlers,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";
import {
  FeatureShowcaseServiceGrpcRegistry,
  FeatureShowcaseServiceHandlers,
} from "@effect-grpc/simple-proto/generated/features/v1/showcase_effect_grpc";
import { withServer } from "./support.ts";

type Metadata = GrpcMetadata.GrpcMetadata;
const headers: Metadata = [
  ["X-Response", "header"],
  ["response-bin", new Uint8Array([0, 255])],
];
const trailers: Metadata = [
  ["x-complete", "trailer"],
  ["complete-bin", new Uint8Array([1, 254])],
];
const shapes = ["unary", "server", "client", "bidi"] as const;
const tags = {
  unary: "demo.v1.UserService/GetUser",
  server: "demo.v1.UserService/WatchUsers",
  client: "features.v1.FeatureShowcaseService/UploadNotes",
  bidi: "features.v1.FeatureShowcaseService/Chat",
};
const message = { id: "1", name: "Ada", action: "created", sequence: 1 };
const streaming = (
  invoker: GrpcInvoker.GrpcInvokerService,
  shape: "server" | "bidi",
  options: CodegenSupport.GrpcCallOptions,
) =>
  shape === "server"
    ? invoker.serverStream(tags.server, { tenantId: "t", count: 1 }, options)
    : invoker.bidiStream(
        tags.bidi,
        Stream.make({ text: "hi", sequence: 1 }),
        options,
      );
const invoke = (
  invoker: GrpcInvoker.GrpcInvokerService,
  shape: (typeof shapes)[number],
  options: CodegenSupport.GrpcCallOptions,
) =>
  shape === "unary"
    ? invoker.unary(tags.unary, { id: "1" }, options)
    : shape === "client"
      ? invoker.clientStream(tags.client, Stream.make({ text: "hi" }), options)
      : Stream.runCollect(streaming(invoker, shape, options));

const serve = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E>,
  options: {
    readonly before?: (
      context: CodegenSupport.GrpcServerContext,
    ) => Effect.Effect<void, GrpcStatusError.GrpcStatusError>;
    readonly after?: (
      context: CodegenSupport.GrpcServerContext,
    ) => Effect.Effect<void, GrpcStatusError.GrpcStatusError>;
    readonly neverEnd?: boolean;
  } = {},
) => {
  const before = (context: CodegenSupport.GrpcServerContext) =>
    context
      .writeResponseHeaders(headers)
      .pipe(Effect.andThen(options.before?.(context) ?? Effect.void));
  const after = (context: CodegenSupport.GrpcServerContext) =>
    (options.after?.(context) ?? Effect.void).pipe(
      Effect.andThen(Effect.sleep(5)),
      Effect.andThen(context.writeResponseTrailers(trailers)),
    );
  const effect = <A>(context: CodegenSupport.GrpcServerContext, value: A) =>
    before(context).pipe(
      Effect.andThen(Effect.succeed(value)),
      Effect.ensuring(after(context).pipe(Effect.orDie)),
    );
  const stream = <A>(context: CodegenSupport.GrpcServerContext, value: A) =>
    Stream.unwrap(
      before(context).pipe(
        Effect.as(
          Stream.succeed(value).pipe(
            Stream.concat(options.neverEnd ? Stream.never : Stream.empty),
            Stream.ensuring(
              after(context).pipe(Effect.catch(() => Effect.void)),
            ),
          ),
        ),
      ),
    );
  return withServer(
    {
      services: [
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers({
            getUser: (_, context) =>
              effect(context, { user: { id: "1", name: "Ada" } }),
            watchUsers: (_, context) => stream(context, message),
          }),
        },
        {
          registry: FeatureShowcaseServiceGrpcRegistry,
          handlers: FeatureShowcaseServiceHandlers({
            describe: (request) => Effect.succeed({ request, summary: "" }),
            uploadNotes: (_, context) =>
              effect(context, { count: 1, joined: "hi" }),
            chat: (_, context) => stream(context, { text: "hi", sequence: 1 }),
          }),
        },
      ],
    },
    (baseUrl) =>
      GrpcInvoker.GrpcInvoker.pipe(
        Effect.flatMap(use),
        Effect.provide(
          GrpcClientProtocol.layer({
            baseUrl: baseUrl.toString(),
            registry: GrpcMethodRegistry.merge([
              UserServiceGrpcRegistry,
              FeatureShowcaseServiceGrpcRegistry,
            ]),
          }),
        ),
      ),
  );
};

describe("native response metadata", () => {
  it.each(shapes)(
    "observes binary headers and finalizer trailers once for %s",
    async (shape) => {
      const seen: Array<readonly [string, Metadata]> = [];
      await Effect.runPromise(
        serve((invoker) =>
          invoke(invoker, shape, {
            onResponseHeaders: (metadata) => {
              seen.push(["headers", metadata]);
            },
            onResponseTrailers: (metadata) => {
              seen.push(["trailers", metadata]);
            },
          }),
        ),
      );
      expect(seen.map(([kind]) => kind)).toEqual(["headers", "trailers"]);
      expect(seen[0]![1]).toContainEqual(["x-response", "header"]);
      expect(seen[0]![1]).toContainEqual([
        "response-bin",
        new Uint8Array([0, 255]),
      ]);
      expect(seen[1]![1]).toContainEqual(["x-complete", "trailer"]);
      expect(seen[1]![1]).toContainEqual([
        "complete-bin",
        new Uint8Array([1, 254]),
      ]);
    },
  );

  it.each(["server", "bidi"] as const)(
    "keeps trailers unavailable while %s is open and on early termination",
    async (shape) => {
      const events: string[] = [];
      await Effect.runPromise(
        serve(
          (invoker) =>
            streaming(invoker, shape, {
              onResponseHeaders: () => {
                events.push("headers");
              },
              onResponseTrailers: () => {
                events.push("trailers");
              },
            }).pipe(
              Stream.tap(() =>
                Effect.sync(() => expect(events).toEqual(["headers"])),
              ),
              Stream.take(1),
              Stream.runDrain,
            ),
          { neverEnd: true },
        ),
      );
      expect(events).toEqual(["headers"]);
    },
  );

  it.each(shapes)(
    "normalizes a throwing %s header observer and tears down",
    async (shape) => {
      const error = await Effect.runPromise(
        serve((invoker) =>
          invoke(invoker, shape, {
            onResponseHeaders: () => {
              throw new Error("observer failed");
            },
          }).pipe(Effect.flip),
        ),
      );
      expect(error).toMatchObject({
        _tag: "GrpcStatusError",
        code: "internal",
        message: "Response metadata observer failed",
      });
    },
  );

  it.each(shapes)(
    "normalizes a throwing %s trailer observer",
    async (shape) => {
      const error = await Effect.runPromise(
        serve((invoker) =>
          invoke(invoker, shape, {
            onResponseTrailers: () => {
              throw new Error("trailer observer failed");
            },
          }).pipe(Effect.flip),
        ),
      );
      expect(error).toMatchObject({
        code: "internal",
        message: "Response metadata observer failed",
      });
    },
  );

  it("retains failure metadata and never reports successful trailers", async () => {
    let called = false;
    const failure = GrpcStatusError.make({
      code: "permission_denied",
      message: "denied",
      metadata: [["error-bin", new Uint8Array([3])]],
    });
    const error = await Effect.runPromise(
      serve(
        (invoker) =>
          invoke(invoker, "unary", {
            onResponseTrailers: () => {
              called = true;
            },
          }).pipe(Effect.flip),
        { before: () => Effect.fail(failure) },
      ),
    );
    expect(error.code).toBe("permission_denied");
    expect(error.metadata).toContainEqual(["error-bin", new Uint8Array([3])]);
    expect(called).toBe(false);
  });

  it("does not report trailers after cancellation", async () => {
    let trailersCalled = false;
    await Effect.runPromise(
      Effect.gen(function* () {
        const received = yield* Deferred.make<void>();
        yield* serve(
          (invoker) =>
            Effect.gen(function* () {
              const fiber = yield* streaming(invoker, "server", {
                onResponseTrailers: () => {
                  trailersCalled = true;
                },
              }).pipe(
                Stream.tap(() => Deferred.succeed(received, undefined)),
                Stream.runDrain,
                Effect.forkChild,
              );
              yield* Deferred.await(received);
              yield* Fiber.interrupt(fiber);
            }),
          { neverEnd: true },
        );
      }),
    );
    expect(trailersCalled).toBe(false);
  });
});
