import type { Transport } from "@connectrpc/connect";
import { assert, describe, it } from "@effect/vitest";
import { Effect } from "effect";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcNodeServer from "../src/GrpcNodeServer.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";
import { freePort } from "./support/serverHarness.js";

/**
 * Adapter parity over a real listener. `grpc.health.v1.Health/Check` is the
 * only unary method the library ships a protobuf descriptor for, so it stands
 * in for a generated service: the registry is real, the handler is ours, and
 * the call travels HTTP/2 end to end.
 */
const CHECK = "grpc.health.v1.Health/Check";
const SERVING = { status: "SERVING" };

/** The entries under test, isolated from the transport's own headers. */
const underTest = (metadata: GrpcMetadata.GrpcMetadata) =>
  metadata.filter(([key]) => key.startsWith("x-parity"));

const callMetadata: GrpcMetadata.GrpcMetadata = [
  ["X-Parity-Ascii", "plain, with a comma"],
  ["x-parity-token-bin", new Uint8Array([0, 1, 250, 255])],
  ["x-parity-dup", "one"],
  ["x-parity-dup", "two"],
];

/** What `callMetadata` looks like once it has been through headers. */
const delivered: GrpcMetadata.GrpcMetadata = [
  ["x-parity-ascii", "plain, with a comma"],
  ["x-parity-dup", "one, two"],
  ["x-parity-token-bin", new Uint8Array([0, 1, 250, 255])],
];

describe("GrpcInvoker adapter parity", () => {
  // Boots a real HTTP/2 listener and waits for it in wall-clock time.
  it.live(
    "delivers identical metadata to a wire handler and an in-memory handler",
    () =>
      Effect.gen(function* () {
        const wire = yield* onTheWire((invoker) =>
          invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
        );
        const memory = yield* inMemory((invoker) =>
          invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
        );

        assert.deepStrictEqual(wire.response, SERVING);
        assert.deepStrictEqual(memory.response, SERVING);
        assert.deepStrictEqual(underTest(memory.seen!), underTest(wire.seen!));
        // Pinned explicitly, so a codec that drifts on *both* sides still fails:
        // keys lowercased and ordered, repeated ASCII keys joined, `-bin` decoded.
        assert.deepStrictEqual(underTest(wire.seen!), delivered);
      }),
  );
});

describe("GrpcInvoker (connect call options)", () => {
  // connect's own transports happen to clamp `timeoutMs <= 0` to "no
  // deadline" before `createDeadlineSignal` sees it, so the wire test above
  // cannot distinguish forwarding a zero from dropping it. A bare `Transport`
  // — which `layerConnect` accepts — can: `createDeadlineSignal` aborts a
  // `<= 0` timeout the instant the call starts.
  it.effect(
    "omits a non-positive timeoutMs instead of forwarding an expired deadline",
    () =>
      Effect.gen(function* () {
        const forwarded: Array<number | undefined> = [];
        const transport = {
          unary: (_method: unknown, _signal: unknown, timeoutMs?: number) => {
            forwarded.push(timeoutMs);
            return Promise.resolve({
              header: new Headers(),
              trailer: new Headers(),
              message: { status: 1 },
            });
          },
        } as unknown as Transport;

        yield* GrpcInvoker.GrpcInvoker.pipe(
          Effect.flatMap((invoker) =>
            Effect.all([
              invoker.unary(CHECK, { service: "" }, { timeoutMs: 0 }),
              invoker.unary(CHECK, { service: "" }, { timeoutMs: -1 }),
              invoker.unary(CHECK, { service: "" }, { timeoutMs: 5_000 }),
            ]),
          ),
          Effect.provide(
            GrpcInvoker.layerConnect({
              registry: GrpcHealth.HealthGrpcRegistry,
              transport,
            }),
          ),
        );

        assert.deepStrictEqual(forwarded, [undefined, undefined, 5_000]);
      }),
  );
});

interface Observed<A> {
  readonly response: A;
  readonly seen: GrpcMetadata.GrpcMetadata | undefined;
}

/**
 * Runs `use` against a real server over a loopback HTTP/2 listener. Scoped
 * internally so the listener is torn down as soon as the call has returned.
 */
const onTheWire = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E, never>,
): Effect.Effect<Observed<A>, E> =>
  Effect.scoped(
    Effect.gen(function* () {
      let seen: GrpcMetadata.GrpcMetadata | undefined;
      const { routes } = yield* GrpcServerProtocol.make({
        registry: GrpcHealth.HealthGrpcRegistry,
        handlers: new Map([
          [
            CHECK,
            {
              kind: "unary",
              handler: (_request, context) =>
                Effect.sync(() => {
                  seen = context.metadata;
                  return SERVING;
                }),
            } satisfies GrpcServerProtocol.GrpcHandler,
          ],
        ]),
      });
      const port = yield* freePort;
      yield* Effect.forkScoped(
        GrpcNodeServer.serve({ host: "127.0.0.1", port, routes }),
      );
      yield* Effect.sleep("50 millis");

      const response = yield* GrpcInvoker.GrpcInvoker.pipe(
        Effect.flatMap(use),
        Effect.provide(
          GrpcClientProtocol.layer({
            baseUrl: `http://127.0.0.1:${port}`,
            registry: GrpcHealth.HealthGrpcRegistry,
          }),
        ),
      );
      return { response, seen };
    }),
  );

/** The same call against `layerInMemory`, capturing the handler's view. */
const inMemory = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E, never>,
): Effect.Effect<Observed<A>, E> =>
  Effect.gen(function* () {
    let call: GrpcInvoker.GrpcInMemoryCall | undefined;
    const response = yield* GrpcInvoker.GrpcInvoker.pipe(
      Effect.flatMap(use),
      Effect.provide(
        GrpcInvoker.layerInMemory({
          [CHECK]: {
            kind: "unary",
            handler: (_request, observed) =>
              Effect.sync(() => {
                call = observed;
                return SERVING;
              }),
          },
        }),
      ),
    );
    return { response, seen: call?.metadata };
  });
