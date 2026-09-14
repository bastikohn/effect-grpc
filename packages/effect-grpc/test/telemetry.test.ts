import type { HandlerContext, Transport } from "@connectrpc/connect";
import { Code, ConnectError } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import {
  Cause,
  Context,
  Deferred,
  Effect,
  Exit,
  Fiber,
  Metric,
  Option,
  Stream,
} from "effect";
import * as Tracer from "effect/Tracer";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type { GrpcMethodEntry } from "../src/GrpcMethodRegistry.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import * as GrpcStatusError from "../src/GrpcStatusError.js";
import type { ServiceImplementation } from "./support/serverHarness.js";
import {
  captureImplementation,
  handlerContext,
  methodEntries,
} from "./support/serverHarness.js";

const TRACEPARENT_PATTERN = /^00-[0-9a-f]{32}-[0-9a-f]{16}-0[01]$/;

type HeadersImport = ConstructorParameters<typeof Headers>[0];

describe("client telemetry", () => {
  it.effect(
    "forwards trace headers and records duration for client-streaming calls",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport, headers } = fakeTransport({
          stream: async function* (input) {
            let count = 0;
            for await (const _ of input) {
              count++;
            }
            yield { received: count };
          },
        });
        const parent = Tracer.externalSpan({
          traceId: "0123456789abcdef0123456789abcdef",
          spanId: "0123456789abcdef",
          sampled: true,
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const invoker = yield* GrpcInvoker.GrpcInvoker;
            const response = yield* invoker.clientStream(
              clientStreamingEntry.tag,
              Stream.make({ id: "1" }, { id: "2" }),
            );
            const metrics = yield* Metric.snapshot;
            return { response, metrics };
          }).pipe(
            Effect.withParentSpan(parent),
            Effect.provide(clientLayer(transport)),
          ),
        );

        assert.deepStrictEqual(result.response, { received: 2 });
        assert.match(headers[0]!.get("traceparent")!, TRACEPARENT_PATTERN);

        const span = telemetry.expectSpan(clientStreamingEntry.tag);
        assert.strictEqual(span.kind, "client");
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "OK",
        );

        expectDuration(result.metrics, "rpc.client.call.duration", {
          "rpc.system.name": "grpc",
          "rpc.method": "demo.v1.TelemetryService/Upload",
          "rpc.response.status_code": "OK",
        });
      }),
  );

  it.effect(
    "records failed client-streaming calls with the failure status",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport } = fakeTransport({
          stream: () => {
            throw new ConnectError("nope", Code.PermissionDenied);
          },
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const invoker = yield* GrpcInvoker.GrpcInvoker;
            const error = yield* invoker
              .clientStream(clientStreamingEntry.tag, Stream.make({ id: "1" }))
              .pipe(Effect.flip);
            const metrics = yield* Metric.snapshot;
            return { error, metrics };
          }).pipe(Effect.provide(clientLayer(transport))),
        );

        assert.deepInclude(result.error, { code: "permission_denied" });
        const span = telemetry.expectSpan(clientStreamingEntry.tag);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "PERMISSION_DENIED",
        );
        assert.strictEqual(
          span.attributes.get("error.type"),
          "PERMISSION_DENIED",
        );

        expectDuration(result.metrics, "rpc.client.call.duration", {
          "rpc.method": "demo.v1.TelemetryService/Upload",
          "rpc.response.status_code": "PERMISSION_DENIED",
          "error.type": "PERMISSION_DENIED",
        });
      }),
  );

  it.effect("records OK when a bidi response stream completes naturally", () =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();
      const { transport } = fakeTransport({
        stream: async function* (input) {
          for await (const request of input) {
            yield request;
          }
        },
      });

      const result = yield* telemetry.provide(
        Effect.gen(function* () {
          const invoker = yield* GrpcInvoker.GrpcInvoker;
          const responses = yield* invoker
            .bidiStream(
              bidiStreamingEntry.tag,
              Stream.make({ id: "1" }, { id: "2" }),
            )
            .pipe(Stream.runCollect);
          const metrics = yield* Metric.snapshot;
          return { responses, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      );

      assert.lengthOf(result.responses, 2);
      const span = telemetry.expectSpan(bidiStreamingEntry.tag);
      assert.strictEqual(span.attributes.get("rpc.response.status_code"), "OK");

      expectDuration(result.metrics, "rpc.client.call.duration", {
        "rpc.method": "demo.v1.TelemetryService/Chat",
        "rpc.response.status_code": "OK",
      });
    }),
  );

  // Waits on a real `setTimeout` for the call to reach the transport, so it
  // needs the live clock.
  it.live(
    "ends the client span as an error when a unary call is interrupted",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport } = fakeTransport({
          unary: (_header, signal) =>
            new Promise((_resolve, reject) => {
              signal?.addEventListener(
                "abort",
                () => reject(new ConnectError("cancelled", Code.Canceled)),
                { once: true },
              );
            }),
        });

        yield* telemetry.provide(
          Effect.gen(function* () {
            const invoker = yield* GrpcInvoker.GrpcInvoker;
            const fiber = yield* invoker
              .unary(unaryEntry.tag, {})
              .pipe(Effect.forkChild);
            // Let the call reach the in-flight transport request.
            yield* Effect.promise<void>(
              (): Promise<void> =>
                new Promise((resolve) => setTimeout(resolve, 10)),
            );
            yield* Fiber.interrupt(fiber);
          }).pipe(Effect.provide(clientLayer(transport))),
        );

        const span = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "CANCELLED",
        );
        assert.strictEqual(span.attributes.get("error.type"), "CANCELLED");
        // Interruption must not end the span with an interrupt-only exit, which
        // exporters map to OK; per semconv a CANCELLED client span is an error.
        // An interrupted exit is also `Failure`, so assert the cause carries a
        // real failure — the distinction the OTLP exporter makes.
        const exit = spanEndExit(span);
        assert.strictEqual(exit._tag, "Failure");
        if (exit._tag === "Failure") {
          assert.isFalse(Cause.hasInterruptsOnly(exit.cause));
        }
      }),
  );

  it.effect(
    "records CANCELLED when the consumer stops a bidi stream early",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport } = fakeTransport({
          stream: async function* (input) {
            for await (const request of input) {
              yield request;
            }
          },
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const invoker = yield* GrpcInvoker.GrpcInvoker;
            const responses = yield* invoker
              .bidiStream(
                bidiStreamingEntry.tag,
                Stream.make({ id: "1" }, { id: "2" }, { id: "3" }),
              )
              .pipe(Stream.take(1), Stream.runCollect);
            const metrics = yield* Metric.snapshot;
            return { responses, metrics };
          }).pipe(Effect.provide(clientLayer(transport))),
        );

        assert.lengthOf(result.responses, 1);
        const span = telemetry.expectSpan(bidiStreamingEntry.tag);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "CANCELLED",
        );
        // The stream scope closes successfully on an early consumer close, but
        // per semconv the CANCELLED client span must still end as an error.
        assert.strictEqual(spanEndExit(span)._tag, "Failure");

        expectDuration(result.metrics, "rpc.client.call.duration", {
          "rpc.method": "demo.v1.TelemetryService/Chat",
          "rpc.response.status_code": "CANCELLED",
          "error.type": "CANCELLED",
        });
      }),
  );

  // Generated clients resolve every call shape through the `GrpcInvoker` seam
  // (its connect adapter's `withCallSpanEffect` / `withCallSpanStream`). These
  // cases assert the unary and server-streaming shapes carry the same spans,
  // status, metrics, and trace headers as the streaming shapes above.
  const callInvokerUnary = (
    tag: string,
    callOptions?: Parameters<GrpcInvoker.GrpcInvokerService["unary"]>[2],
  ) =>
    Effect.gen(function* () {
      const invoker = yield* GrpcInvoker.GrpcInvoker;
      return yield* invoker.unary(tag, {}, callOptions);
    });

  it.effect(
    "records semconv span attributes, injects trace headers, and observes duration on unary success",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport, headers } = fakeTransport({
          unary: () => ({ ok: true }),
        });
        const parent = Tracer.externalSpan({
          traceId: "0123456789abcdef0123456789abcdef",
          spanId: "0123456789abcdef",
          sampled: true,
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const response = yield* callInvokerUnary(unaryEntry.tag);
            const metrics = yield* Metric.snapshot;
            return { response, metrics };
          }).pipe(
            Effect.withParentSpan(parent),
            Effect.provide(clientLayer(transport)),
          ),
        );

        assert.deepStrictEqual(result.response, { ok: true });
        const span = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(span.kind, "client");
        assert.strictEqual(span.attributes.get("rpc.system.name"), "grpc");
        assert.strictEqual(
          span.attributes.get("rpc.method"),
          "demo.v1.TelemetryService/Get",
        );
        assert.strictEqual(
          span.attributes.get("server.address"),
          "api.example.com",
        );
        assert.strictEqual(span.attributes.get("server.port"), 8443);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "OK",
        );
        assert.isUndefined(span.attributes.get("error.type"));
        assert.strictEqual(spanEndExit(span)._tag, "Success");

        const traceparent = headers[0]!.get("traceparent")!;
        assert.match(traceparent, TRACEPARENT_PATTERN);
        assert.strictEqual(traceparent, `00-${span.traceId}-${span.spanId}-01`);
        assert.strictEqual(span.traceId, parent.traceId);

        expectDuration(
          result.metrics,
          "rpc.client.call.duration",
          {
            unit: "s",
            "rpc.system.name": "grpc",
            "rpc.method": "demo.v1.TelemetryService/Get",
            "server.address": "api.example.com",
            "server.port": "8443",
            "rpc.response.status_code": "OK",
          },
          true,
        );
      }),
  );

  // `URL` normalizes a scheme's default port away, so `baseUrl` alone cannot
  // be trusted for `server.port` — it must fall back to the scheme.
  it.effect.each([
    { address: "https://api.example.com:443", port: 443 },
    { address: "https://api.example.com", port: 443 },
    { address: "https://api.example.com:8443", port: 8443 },
    { address: "http://api.example.com", port: 80 },
  ])("reports server.port $port for $address", ({ address, port }) =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();
      const { transport } = fakeTransport({ unary: () => ({ ok: true }) });

      const metrics = yield* telemetry.provide(
        Effect.gen(function* () {
          yield* callInvokerUnary(unaryEntry.tag);
          return yield* Metric.snapshot;
        }).pipe(Effect.provide(clientLayer(transport, new URL(address)))),
      );

      const span = telemetry.expectSpan(unaryEntry.tag);
      assert.strictEqual(
        span.attributes.get("server.address"),
        "api.example.com",
      );
      assert.strictEqual(span.attributes.get("server.port"), port);

      expectDuration(metrics, "rpc.client.call.duration", {
        "server.address": "api.example.com",
        "server.port": String(port),
      });
    }),
  );

  // `serverAddress` is a telemetry-only override, unconstrained by scheme, and
  // `layerFromTransport` never sees a `baseUrl` at all — so the address can be
  // a non-special URL, whose `hostname` is `""`. Reporting that blank would
  // hand exporters a present-but-empty `server.address`.
  it.effect(
    "reports the whole URL as server.address for a scheme without a hostname",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport } = fakeTransport({ unary: () => ({ ok: true }) });

        const metrics = yield* telemetry.provide(
          Effect.gen(function* () {
            yield* callInvokerUnary(unaryEntry.tag);
            return yield* Metric.snapshot;
          }).pipe(
            Effect.provide(
              clientLayer(transport, new URL("unix:/var/run/grpc.sock")),
            ),
          ),
        );

        const span = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(
          span.attributes.get("server.address"),
          "unix:/var/run/grpc.sock",
        );
        // No default port for a scheme `URL` does not special-case.
        assert.isUndefined(span.attributes.get("server.port"));

        const duration = expectDuration(metrics, "rpc.client.call.duration", {
          "server.address": "unix:/var/run/grpc.sock",
        });
        assert.isUndefined(duration.attributes?.["server.port"]);
      }),
  );

  it.effect("respects a caller-provided traceparent", () =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();
      const { transport, headers } = fakeTransport({
        unary: () => ({ ok: true }),
      });
      const provided =
        "00-11111111111111111111111111111111-2222222222222222-01";

      yield* telemetry.provide(
        callInvokerUnary(unaryEntry.tag, {
          metadata: [["traceparent", provided]],
        }).pipe(Effect.provide(clientLayer(transport))),
      );

      assert.strictEqual(headers[0]?.get("traceparent"), provided);
    }),
  );

  it.effect("does not inject a traceparent for noop spans", () =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();
      const { transport, headers } = fakeTransport({
        unary: () => ({ ok: true }),
      });

      yield* telemetry.provide(
        callInvokerUnary(unaryEntry.tag).pipe(
          Effect.provide(clientLayer(transport)),
          Effect.withTracerEnabled(false),
        ),
      );

      assert.isNull(headers[0]?.get("traceparent"));
    }),
  );

  it.effect("records the status code and error.type on unary failure", () =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();
      const { transport } = fakeTransport({
        unary: () => {
          throw new ConnectError("missing", Code.NotFound);
        },
      });

      const result = yield* telemetry.provide(
        Effect.gen(function* () {
          const error = yield* callInvokerUnary(unaryEntry.tag).pipe(
            Effect.flip,
          );
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }).pipe(Effect.provide(clientLayer(transport))),
      );

      assert.deepInclude(result.error, { code: "not_found" });
      const span = telemetry.expectSpan(unaryEntry.tag);
      assert.strictEqual(
        span.attributes.get("rpc.response.status_code"),
        "NOT_FOUND",
      );
      assert.strictEqual(span.attributes.get("error.type"), "NOT_FOUND");
      assert.strictEqual(spanEndExit(span)._tag, "Failure");

      expectDuration(result.metrics, "rpc.client.call.duration", {
        "rpc.method": "demo.v1.TelemetryService/Get",
        "rpc.response.status_code": "NOT_FOUND",
        "error.type": "NOT_FOUND",
      });
    }),
  );

  it.effect(
    "forwards trace headers and records duration for server-streaming calls",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport, headers } = fakeTransport({
          stream: async function* () {
            yield { seq: 1 };
            yield { seq: 2 };
          },
        });
        const parent = Tracer.externalSpan({
          traceId: "0123456789abcdef0123456789abcdef",
          spanId: "0123456789abcdef",
          sampled: true,
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const invoker = yield* GrpcInvoker.GrpcInvoker;
            const responses = yield* invoker
              .serverStream(serverStreamingEntry.tag, {})
              .pipe(Stream.runCollect);
            const metrics = yield* Metric.snapshot;
            return { responses, metrics };
          }).pipe(
            Effect.withParentSpan(parent),
            Effect.provide(clientLayer(transport)),
          ),
        );

        assert.lengthOf(result.responses, 2);
        assert.match(headers[0]!.get("traceparent")!, TRACEPARENT_PATTERN);

        const span = telemetry.expectSpan(serverStreamingEntry.tag);
        assert.strictEqual(span.kind, "client");
        assert.strictEqual(
          span.attributes.get("rpc.method"),
          "demo.v1.TelemetryService/Watch",
        );
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "OK",
        );

        expectDuration(result.metrics, "rpc.client.call.duration", {
          "rpc.method": "demo.v1.TelemetryService/Watch",
          "rpc.response.status_code": "OK",
        });
      }),
  );
});

// The server protocol captures the build-time context and runs each connect
// call with `runPromiseWith`, so these tests need the live clock rather than
// the TestClock `it.effect` provides.
describe("server telemetry", () => {
  const traceId = "0af7651916cd43dd8448eb211c80319c";
  const parentSpanId = "b7ad6b7169203331";
  const incomingHeaders = {
    traceparent: `00-${traceId}-${parentSpanId}-01`,
  };

  it.live(
    "parents the unary span to the incoming traceparent and records duration",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const call = yield* serverCall(unaryEntry, {
              kind: "unary",
              handler: () => Effect.succeed({ ok: true }),
            });

            const response = yield* Effect.promise(() =>
              call({}, handlerContext({ headers: incomingHeaders })),
            );
            const metrics = yield* Metric.snapshot;
            return { response, metrics };
          }),
        );

        assert.deepStrictEqual(result.response, { ok: true });
        const span = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(span.kind, "server");
        assert.strictEqual(span.attributes.get("rpc.system.name"), "grpc");
        assert.strictEqual(
          span.attributes.get("rpc.method"),
          "demo.v1.TelemetryService/Get",
        );
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          "OK",
        );
        assert.strictEqual(span.traceId, traceId);

        const parent = Option.getOrThrow(span.parent);
        assert.strictEqual(parent._tag, "ExternalSpan");
        assert.strictEqual(parent.traceId, traceId);
        assert.strictEqual(parent.spanId, parentSpanId);

        expectDuration(
          result.metrics,
          "rpc.server.call.duration",
          {
            unit: "s",
            "rpc.system.name": "grpc",
            "rpc.method": "demo.v1.TelemetryService/Get",
            "rpc.response.status_code": "OK",
          },
          true,
        );
      }),
  );

  it.live(
    "parents downstream client calls made from a server-streaming handler",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();
        const { transport, headers } = fakeTransport({
          unary: () => ({ ok: true }),
        });

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const downstream = yield* Effect.provide(
              Effect.service(GrpcInvoker.GrpcInvoker),
              clientLayer(transport),
            );
            // The handler fiber is spawned by the response pump; this pins the
            // pump context rehydration: the scoped server span parents the
            // downstream span.
            const call = yield* serverCall(serverStreamingEntry, {
              kind: "server-streaming",
              handler: () =>
                Stream.fromEffect(
                  downstream.unary(unaryEntry.tag, {}).pipe(Effect.orDie),
                ),
            });
            return yield* Effect.promise(async () => {
              const responses: Array<unknown> = [];
              for await (const value of call(
                {},
                handlerContext({ headers: incomingHeaders }),
              )) {
                responses.push(value);
              }
              return responses;
            });
          }),
        );

        assert.deepStrictEqual(result, [{ ok: true }]);
        assert.match(headers[0]!.get("traceparent")!, TRACEPARENT_PATTERN);

        const serverSpan = telemetry.expectSpan(serverStreamingEntry.tag);
        assert.strictEqual(serverSpan.kind, "server");
        assert.strictEqual(serverSpan.traceId, traceId);
        // The downstream client span must be parented to the server span.
        const clientSpan = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(clientSpan.kind, "client");
        assert.strictEqual(clientSpan.traceId, traceId);
      }),
  );

  // Per semconv, server spans mark only server-fault codes as errors, so the
  // failure status alone decides whether `error.type` is recorded.
  it.live.each([
    {
      status: "NOT_FOUND",
      errorType: undefined,
      message: "missing",
      failure: () => GrpcStatusError.notFound("missing"),
    },
    {
      status: "INTERNAL",
      errorType: "INTERNAL",
      message: "boom",
      failure: () => GrpcStatusError.internal("boom"),
    },
  ])(
    "records $status with error.type $errorType on unary failure",
    ({ status, errorType, message, failure }) =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const call = yield* serverCall(unaryEntry, {
              kind: "unary",
              handler: () => Effect.fail(failure()),
            });

            const error = yield* Effect.promise(async () => {
              try {
                await call({}, handlerContext());
              } catch (cause) {
                return cause;
              }
              throw new Error("Expected unary handler to fail");
            });
            const metrics = yield* Metric.snapshot;
            return { error, metrics };
          }),
        );

        assert.deepInclude(result.error, { rawMessage: message });
        const span = telemetry.expectSpan(unaryEntry.tag);
        assert.strictEqual(
          span.attributes.get("rpc.response.status_code"),
          status,
        );
        assert.strictEqual(span.attributes.get("error.type"), errorType);

        const duration = expectDuration(
          result.metrics,
          "rpc.server.call.duration",
          {
            "rpc.method": "demo.v1.TelemetryService/Get",
            "rpc.response.status_code": status,
          },
        );
        assert.strictEqual(duration.attributes?.["error.type"], errorType);
      }),
  );

  // Regression pin for the client-abort span path: on abort, the connect
  // signal must interrupt only the handler body — the spanned effect has to
  // survive long enough to record `cancelled` while the span is open. The
  // OTLP exporter serializes a span inside `end()`, so a status attribute
  // written after an interrupt-torn span end never leaves the process.
  const abortedEffectCall = (
    entry: GrpcMethodEntry,
    handler: (
      interrupted: Deferred.Deferred<boolean>,
    ) => GrpcServerProtocol.GrpcHandler,
    request: () => unknown,
  ) =>
    Effect.gen(function* () {
      const interrupted = yield* Deferred.make<boolean>();
      const call = yield* serverCall(entry, handler(interrupted));
      const abort = new AbortController();

      const error = yield* Effect.promise(async () => {
        const pending = call(
          request(),
          handlerContext({ signal: abort.signal }),
        );
        await new Promise((resolve) => setTimeout(resolve, 10));
        abort.abort();
        try {
          await pending;
        } catch (cause) {
          return GrpcStatusError.fromConnectError(cause);
        }
        throw new Error("Expected the aborted call to fail");
      });
      const handlerInterrupted = yield* Deferred.await(interrupted);
      const metrics = yield* Metric.snapshot;
      return { error, handlerInterrupted, metrics };
    });

  const expectCancelledSpanEnd = (
    telemetry: ReturnType<typeof makeTestTelemetry>,
    tag: string,
  ) => {
    const span = telemetry.expectSpan(tag);
    const end = telemetry.endState(span);
    // The status must already be on the span when it ends...
    assert.strictEqual(
      end.attributesAtEnd.get("rpc.response.status_code"),
      "CANCELLED",
    );
    // ...and nothing may be written after the end — post-end attributes are
    // exactly what real exporters drop.
    assert.deepStrictEqual(end.attributesAfterEnd, []);
    // Per semconv, `cancelled` is not a server fault.
    assert.isUndefined(end.attributesAtEnd.get("error.type"));
    // The span must close cleanly with the recorded status, not with an
    // interrupt-only exit (which exporters map to an attributeless close).
    assert.strictEqual(spanEndExit(span)._tag, "Success");
  };

  it.live(
    "records CANCELLED while the span is open when the client aborts a unary call",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          abortedEffectCall(
            unaryEntry,
            (interrupted) => ({
              kind: "unary",
              handler: () =>
                Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Deferred.succeed(interrupted, true).pipe(Effect.asVoid),
                  ),
                ),
            }),
            () => ({}),
          ),
        );

        assert.deepInclude(result.error, { code: "cancelled" });
        assert.isTrue(result.handlerInterrupted);
        expectCancelledSpanEnd(telemetry, unaryEntry.tag);

        const duration = expectDuration(
          result.metrics,
          "rpc.server.call.duration",
          {
            "rpc.method": "demo.v1.TelemetryService/Get",
            "rpc.response.status_code": "CANCELLED",
          },
        );
        assert.isUndefined(duration.attributes?.["error.type"]);
      }),
  );

  it.live(
    "records CANCELLED while the span is open when the client aborts a client-streaming call",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          abortedEffectCall(
            clientStreamingEntry,
            (interrupted) => ({
              kind: "client-streaming",
              handler: () =>
                Effect.never.pipe(
                  Effect.onInterrupt(() =>
                    Deferred.succeed(interrupted, true).pipe(Effect.asVoid),
                  ),
                ),
            }),
            () =>
              (async function* () {
                yield { id: "1" };
              })(),
          ),
        );

        assert.deepInclude(result.error, { code: "cancelled" });
        assert.isTrue(result.handlerInterrupted);
        expectCancelledSpanEnd(telemetry, clientStreamingEntry.tag);

        expectDuration(result.metrics, "rpc.server.call.duration", {
          "rpc.method": "demo.v1.TelemetryService/Upload",
          "rpc.response.status_code": "CANCELLED",
        });
      }),
  );

  // Regression pin for the pump path: a handler stream failing with an
  // interrupt-only cause (the handler interrupting itself, not a client
  // abort) must map to CANCELLED like the effect-shaped calls do — before
  // the fix the pump squashed the cause into a generic error that mapped to
  // INTERNAL with an error span.
  it.live(
    "maps a handler-side interrupt on a server stream to CANCELLED, not INTERNAL",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const call = yield* serverCall(serverStreamingEntry, {
              kind: "server-streaming",
              handler: () =>
                Stream.make({ sequence: 1 }).pipe(
                  Stream.concat(Stream.fromEffect(Effect.interrupt)),
                ),
            });

            const outcome = yield* Effect.promise(async () => {
              const received: Array<unknown> = [];
              try {
                for await (const value of call({}, handlerContext())) {
                  received.push(value);
                }
              } catch (cause) {
                return {
                  received,
                  error: GrpcStatusError.fromConnectError(cause),
                };
              }
              throw new Error(
                "Expected the interrupted handler stream to fail",
              );
            });
            const metrics = yield* Metric.snapshot;
            return { outcome, metrics };
          }),
        );

        assert.deepStrictEqual(result.outcome.received, [{ sequence: 1 }]);
        assert.deepInclude(result.outcome.error, { code: "cancelled" });
        expectCancelledSpanEnd(telemetry, serverStreamingEntry.tag);

        const duration = expectDuration(
          result.metrics,
          "rpc.server.call.duration",
          {
            "rpc.method": "demo.v1.TelemetryService/Watch",
            "rpc.response.status_code": "CANCELLED",
          },
        );
        assert.isUndefined(duration.attributes?.["error.type"]);
      }),
  );

  it.live("records mid-stream bidi failures with the failure status", () =>
    Effect.gen(function* () {
      const telemetry = makeTestTelemetry();

      const result = yield* telemetry.provide(
        Effect.gen(function* () {
          const call = yield* serverCall(bidiStreamingEntry, {
            kind: "bidi-streaming",
            handler: (requests) =>
              Stream.mapEffect(requests, (request) =>
                (request as { readonly id: string }).id === "boom"
                  ? Effect.fail(GrpcStatusError.notFound("boom"))
                  : Effect.succeed(request),
              ),
          });

          const error = yield* Effect.promise(async () => {
            try {
              for await (const value of call(
                (async function* () {
                  yield { id: "1" };
                  yield { id: "boom" };
                })(),
                handlerContext({ headers: incomingHeaders }),
              )) {
                void value;
              }
            } catch (cause) {
              return GrpcStatusError.fromConnectError(cause);
            }
            throw new Error("Expected bidi handler failure");
          });
          const metrics = yield* Metric.snapshot;
          return { error, metrics };
        }),
      );

      assert.deepInclude(result.error, { code: "not_found" });
      const span = telemetry.expectSpan(bidiStreamingEntry.tag);
      assert.strictEqual(span.kind, "server");
      assert.strictEqual(
        span.attributes.get("rpc.response.status_code"),
        "NOT_FOUND",
      );
      // Per semconv, server spans mark only server-fault codes as errors.
      assert.isUndefined(span.attributes.get("error.type"));
      const parent = Option.getOrThrow(span.parent);
      assert.strictEqual(parent.traceId, traceId);

      const duration = expectDuration(
        result.metrics,
        "rpc.server.call.duration",
        {
          "rpc.method": "demo.v1.TelemetryService/Chat",
          "rpc.response.status_code": "NOT_FOUND",
        },
      );
      assert.isUndefined(duration.attributes?.["error.type"]);
    }),
  );

  // Regression pin for `handlersEffect`: it captures the whole build-time
  // context, and before the fix that was provided *over* the per-call
  // context — a handler built under a startup span then observed that
  // (already ended) span instead of the gRPC server span, breaking
  // child-span parenting and incoming trace propagation.
  it.live(
    "keeps request-local tracing over build-time context captured by handlersEffect",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            // Build the handlers the way `serveAll` does during startup:
            // under an ambient (bootstrap) span, with a build-time dependency.
            const handlers = yield* GrpcServerProtocol.handlersEffect({
              [unaryEntry.tag]: {
                kind: "unary",
                handler: () =>
                  Effect.gen(function* () {
                    const dep = yield* Effect.service(BuildDep);
                    yield* Effect.void.pipe(
                      Effect.withSpan("handler-child-unary"),
                    );
                    return { origin: dep.origin };
                  }),
              },
              [serverStreamingEntry.tag]: {
                kind: "server-streaming",
                handler: () =>
                  Stream.fromEffect(
                    Effect.void.pipe(
                      Effect.withSpan("handler-child-stream"),
                      Effect.as({ ok: true }),
                    ),
                  ),
              },
            }).pipe(
              Effect.withSpan("bootstrap"),
              Effect.provideService(BuildDep, { origin: "build" }),
            );

            const implementation = yield* serverImplementation(
              [unaryEntry, serverStreamingEntry],
              handlers,
            );

            const unaryResponse = yield* Effect.promise(() =>
              (implementation[unaryEntry.localName] as ServerCall)(
                {},
                handlerContext({ headers: incomingHeaders }),
              ),
            );
            yield* Effect.promise(async () => {
              for await (const value of (
                implementation[serverStreamingEntry.localName] as ServerCall
              )({}, handlerContext({ headers: incomingHeaders }))) {
                void value;
              }
            });
            return unaryResponse;
          }),
        );

        // The build-time dependency must still resolve for the handler...
        assert.deepStrictEqual(result, { origin: "build" });
        // ...while spans created by the handler parent to the per-call server
        // span on the incoming trace, not to the bootstrap span.
        const unaryServerSpan = telemetry.expectSpan(unaryEntry.tag);
        const unaryChild = telemetry.expectSpan("handler-child-unary");
        assert.strictEqual(unaryChild.traceId, traceId);
        assert.strictEqual(
          Option.getOrThrow(unaryChild.parent).spanId,
          unaryServerSpan.spanId,
        );

        const streamServerSpan = telemetry.expectSpan(serverStreamingEntry.tag);
        const streamChild = telemetry.expectSpan("handler-child-stream");
        assert.strictEqual(streamChild.traceId, traceId);
        assert.strictEqual(
          Option.getOrThrow(streamChild.parent).spanId,
          streamServerSpan.spanId,
        );
      }),
  );

  // Regression pin for the stream-call boundary: the bidi adapter invokes
  // user handler code eagerly, so a synchronous throw used to escape before
  // the try/finally that closes the span scope — surfacing as UNKNOWN to the
  // client and leaking the server span.
  it.live(
    "maps a synchronously throwing bidi handler to INTERNAL and still closes the span",
    () =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const call = yield* serverCall(bidiStreamingEntry, {
              kind: "bidi-streaming",
              handler: () => {
                throw new Error("sync defect");
              },
            });

            const error = yield* Effect.promise(async () => {
              try {
                for await (const value of call(
                  (async function* () {
                    yield { id: "1" };
                  })(),
                  handlerContext(),
                )) {
                  void value;
                }
              } catch (cause) {
                return GrpcStatusError.fromConnectError(cause);
              }
              throw new Error("Expected the throwing bidi handler to fail");
            });
            const metrics = yield* Metric.snapshot;
            return { error, metrics };
          }),
        );

        assert.deepInclude(result.error, { code: "internal" });
        const span = telemetry.expectSpan(bidiStreamingEntry.tag);
        // `endState` throws when the span never ended (the leaked-scope case).
        const end = telemetry.endState(span);
        assert.strictEqual(
          end.attributesAtEnd.get("rpc.response.status_code"),
          "INTERNAL",
        );
        assert.strictEqual(end.attributesAtEnd.get("error.type"), "INTERNAL");
        assert.deepStrictEqual(end.attributesAfterEnd, []);
        // A server-fault code ends the span in an error state.
        assert.strictEqual(spanEndExit(span)._tag, "Failure");

        expectDuration(result.metrics, "rpc.server.call.duration", {
          "rpc.method": "demo.v1.TelemetryService/Chat",
          "rpc.response.status_code": "INTERNAL",
          "error.type": "INTERNAL",
        });
      }),
  );

  // connect-node enforces the incoming `grpc-timeout` by aborting the handler
  // signal with a deadline_exceeded ConnectError as the abort reason, while a
  // plain client cancel carries no such reason. Regression pins: the server
  // must surface DEADLINE_EXCEEDED — a server-fault status per the repo's
  // semconv subset — instead of collapsing every abort into CANCELLED.
  const deadlineAbort = () => {
    const controller = new AbortController();
    const expire = () =>
      controller.abort(
        new ConnectError("the operation timed out", Code.DeadlineExceeded),
      );
    return { signal: controller.signal, expire };
  };

  it.live.each([
    {
      shape: "unary call",
      entry: unaryEntry,
      handler: {
        kind: "unary",
        handler: () => Effect.never,
      } satisfies GrpcServerProtocol.GrpcHandler,
      // The call rejects with the deadline status connect wrote.
      drive: async (
        call: ServerCall,
        context: HandlerContext,
      ): Promise<unknown> => {
        try {
          await call({}, context);
        } catch (cause) {
          return GrpcStatusError.fromConnectError(cause);
        }
        throw new Error("Expected the deadline expiry to fail the call");
      },
      expectOutcome: (outcome: unknown) =>
        assert.deepInclude(outcome, { code: "deadline_exceeded" }),
    },
    {
      shape: "server stream",
      entry: serverStreamingEntry,
      handler: {
        kind: "server-streaming",
        handler: () => Stream.never,
      } satisfies GrpcServerProtocol.GrpcHandler,
      // On expiry the pump closes the handler and the generator ends
      // cleanly — connect itself writes the deadline trailer.
      drive: async (
        call: ServerCall,
        context: HandlerContext,
      ): Promise<unknown> => {
        const values: Array<unknown> = [];
        for await (const value of call({}, context)) {
          values.push(value);
        }
        return values;
      },
      expectOutcome: (outcome: unknown) => assert.deepStrictEqual(outcome, []),
    },
  ])(
    "records DEADLINE_EXCEEDED when connect's deadline aborts a $shape",
    ({ entry, handler, drive, expectOutcome }) =>
      Effect.gen(function* () {
        const telemetry = makeTestTelemetry();

        const result = yield* telemetry.provide(
          Effect.gen(function* () {
            const call = yield* serverCall(entry, handler);
            const { signal, expire } = deadlineAbort();
            setTimeout(expire, 10);

            const outcome = yield* Effect.promise(() =>
              drive(call, handlerContext({ signal })),
            );
            const metrics = yield* Metric.snapshot;
            return { outcome, metrics };
          }),
        );

        expectOutcome(result.outcome);
        const span = telemetry.expectSpan(entry.tag);
        const end = telemetry.endState(span);
        assert.strictEqual(
          end.attributesAtEnd.get("rpc.response.status_code"),
          "DEADLINE_EXCEEDED",
        );
        assert.strictEqual(
          end.attributesAtEnd.get("error.type"),
          "DEADLINE_EXCEEDED",
        );
        assert.deepStrictEqual(end.attributesAfterEnd, []);
        // deadline_exceeded is a server fault: the span ends in an error state.
        assert.strictEqual(spanEndExit(span)._tag, "Failure");

        expectDuration(result.metrics, "rpc.server.call.duration", {
          "rpc.method": entry.tag,
          "rpc.response.status_code": "DEADLINE_EXCEEDED",
          "error.type": "DEADLINE_EXCEEDED",
        });
      }),
  );
});

// ---------------------------------------------------------------------------
// Test harness
// ---------------------------------------------------------------------------

/**
 * Attribute activity around a span's end. Real exporters (Effect's OTLP
 * tracer serializes the span inside `end()`) drop attributes written after
 * the span has ended, while Effect's in-memory span silently accepts them —
 * so status assertions must check `attributesAtEnd`, not `span.attributes`.
 */
interface SpanEndState {
  readonly attributesAtEnd: ReadonlyMap<string, unknown>;
  readonly attributesAfterEnd: ReadonlyArray<string>;
}

const makeTestTelemetry = () => {
  const spans: Array<Tracer.Span> = [];
  const endStates = new Map<
    Tracer.Span,
    {
      attributesAtEnd: ReadonlyMap<string, unknown> | undefined;
      readonly attributesAfterEnd: Array<string>;
    }
  >();
  const native = Context.get(Context.empty(), Tracer.Tracer);
  const tracer = Tracer.make({
    span(options) {
      const span = native.span(options);
      // Mirror what the OTLP exporter observes: snapshot the attributes at
      // the moment `end()` runs and record any attribute written afterwards.
      const state = {
        attributesAtEnd: undefined as ReadonlyMap<string, unknown> | undefined,
        attributesAfterEnd: [] as Array<string>,
      };
      endStates.set(span, state);
      const originalEnd = span.end.bind(span);
      const originalAttribute = span.attribute.bind(span);
      span.end = (endTime, exit) => {
        state.attributesAtEnd ??= new Map(span.attributes);
        originalEnd(endTime, exit);
      };
      span.attribute = (key, value) => {
        if (state.attributesAtEnd !== undefined) {
          state.attributesAfterEnd.push(key);
        }
        originalAttribute(key, value);
      };
      spans.push(span);
      return span;
    },
  });
  const registry = new Map<string, never>();
  const provide = <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ): Effect.Effect<A, E, R> =>
    effect.pipe(
      Effect.provideService(Tracer.Tracer, tracer),
      Effect.provideService(Metric.MetricRegistry, registry as never),
    );
  const expectSpan = (name: string): Tracer.Span => {
    const span = spans.find((candidate) => candidate.name === name);
    if (!span) {
      throw new Error(`Expected a span named ${name}`);
    }
    return span;
  };
  const endState = (span: Tracer.Span): SpanEndState => {
    const state = endStates.get(span);
    if (!state || state.attributesAtEnd === undefined) {
      throw new Error(`Expected span ${span.name} to be ended`);
    }
    return state as SpanEndState;
  };
  return { provide, expectSpan, endState };
};

/** The exit a span was ended with; throws when the span is still open. */
const spanEndExit = (span: Tracer.Span): Exit.Exit<unknown, unknown> => {
  const status = (
    span as unknown as {
      readonly status:
        | { readonly _tag: "Started" }
        | {
            readonly _tag: "Ended";
            readonly exit: Exit.Exit<unknown, unknown>;
          };
    }
  ).status;
  if (status._tag !== "Ended") {
    throw new Error(`Expected span ${span.name} to be ended`);
  }
  return status.exit;
};

type MetricSnapshot = ReadonlyArray<{
  readonly id: string;
  readonly attributes: Readonly<Record<string, string>> | undefined;
  readonly state: unknown;
}>;

/**
 * Asserts the single call-duration observation recorded under `id`: its
 * attributes contain `attributes` (or equal them when `exact`) and exactly one
 * call was counted. Returns the observation for the few call-shape-specific
 * assertions on top.
 */
const expectDuration = (
  metrics: MetricSnapshot,
  id: string,
  attributes: Readonly<Record<string, string>>,
  exact = false,
) => {
  const durations = metrics.filter((metric) => metric.id === id);
  assert.lengthOf(durations, 1);
  const duration = durations[0]!;
  if (exact) {
    assert.deepStrictEqual(duration.attributes, attributes);
  } else {
    assert.deepInclude(duration.attributes, attributes);
  }
  assert.strictEqual((duration.state as { readonly count: number }).count, 1);
  return duration;
};

const fakeTransport = (behavior: {
  readonly unary?: (header: Headers, signal?: AbortSignal) => unknown;
  readonly stream?: (
    input: AsyncIterable<unknown>,
    header: Headers,
  ) => AsyncIterable<unknown>;
}): { transport: Transport; headers: Array<Headers> } => {
  const headers: Array<Headers> = [];
  const transport = {
    async unary(
      _method: unknown,
      signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      header: HeadersImport | undefined,
      _input: unknown,
    ) {
      const captured = new Headers(header);
      headers.push(captured);
      const message = await behavior.unary!(captured, signal);
      return {
        stream: false,
        message,
        header: new Headers(),
        trailer: new Headers(),
      };
    },
    async stream(
      _method: unknown,
      _signal: AbortSignal | undefined,
      _timeoutMs: number | undefined,
      header: HeadersImport | undefined,
      input: AsyncIterable<unknown>,
    ) {
      const captured = new Headers(header);
      headers.push(captured);
      const message = behavior.stream!(input, captured);
      return {
        stream: true,
        message,
        header: new Headers(),
        trailer: new Headers(),
      };
    },
  } as unknown as Transport;
  return { transport, headers };
};

const clientLayer = (
  transport: Transport,
  serverAddress = new URL("http://api.example.com:8443"),
) =>
  GrpcClientProtocol.layerFromTransport({
    registry: new Map([
      [unaryEntry.tag, unaryEntry],
      [serverStreamingEntry.tag, serverStreamingEntry],
      [clientStreamingEntry.tag, clientStreamingEntry],
      [bidiStreamingEntry.tag, bidiStreamingEntry],
    ]),
    transport,
    serverAddress,
  });

/** Build-time handler dependency for the `handlersEffect` regression test. */
const BuildDep = Context.Service<{ readonly origin: string }>(
  "effect-grpc-test/BuildDep",
);

const {
  unary: unaryEntry,
  serverStreaming: serverStreamingEntry,
  clientStreaming: clientStreamingEntry,
  bidiStreaming: bidiStreamingEntry,
} = methodEntries("demo.v1.TelemetryService");

/**
 * A connect method implementation captured from the server protocol. The
 * intersection lets one type cover every call shape: effect-shaped calls are
 * awaited, stream-shaped calls are iterated.
 */
type ServerCall = (
  request: unknown,
  context: HandlerContext,
) => Promise<unknown> & AsyncIterable<unknown>;

/** Builds the connect implementations of `entries` behind `handlers`. */
const serverImplementation = (
  entries: ReadonlyArray<GrpcMethodEntry>,
  handlers: GrpcServerProtocol.GrpcHandlers,
): Effect.Effect<ServiceImplementation> =>
  GrpcServerProtocol.make({
    registry: new Map(entries.map((entry) => [entry.tag, entry])),
    handlers,
  }).pipe(Effect.map(({ routes }) => captureImplementation(routes)));

/** The connect implementation of a single method served by `handler`. */
const serverCall = (
  entry: GrpcMethodEntry,
  handler: GrpcServerProtocol.GrpcHandler,
): Effect.Effect<ServerCall> =>
  serverImplementation([entry], new Map([[entry.tag, handler]])).pipe(
    Effect.map(
      (implementation) => implementation[entry.localName] as ServerCall,
    ),
  );
