import * as net from "node:net";

import type { Transport } from "@connectrpc/connect";
import { Effect } from "effect";
import { describe, expect, it } from "vitest";

import * as GrpcClientProtocol from "../src/GrpcClientProtocol.js";
import * as GrpcHealth from "../src/GrpcHealth.js";
import * as GrpcInvoker from "../src/GrpcInvoker.js";
import type * as GrpcMetadata from "../src/GrpcMetadata.js";
import * as GrpcNodeServer from "../src/GrpcNodeServer.js";
import * as GrpcServerProtocol from "../src/GrpcServerProtocol.js";

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
  it("delivers identical metadata to a wire handler and an in-memory handler", async () => {
    const wire = await onTheWire((invoker) =>
      invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
    );
    const memory = await inMemory((invoker) =>
      invoker.unary(CHECK, { service: "" }, { metadata: callMetadata }),
    );

    expect(wire.response).toEqual(SERVING);
    expect(memory.response).toEqual(SERVING);
    expect(underTest(memory.seen!)).toEqual(underTest(wire.seen!));
    // Pinned explicitly, so a codec that drifts on *both* sides still fails:
    // keys lowercased and ordered, repeated ASCII keys joined, `-bin` decoded.
    expect(underTest(wire.seen!)).toEqual(delivered);
  });

  it.each([
    ["Uint8Array under an ASCII key", [["x-parity", new Uint8Array([1])]]],
    ["string under a -bin key", [["x-parity-bin", "not-bytes"]]],
    // Header syntax: `Headers.append` throws a `TypeError` on each of these,
    // which both adapters would surface as a defect rather than a status.
    ["key with a space", [["bad key", "v"]]],
    ["empty key", [["", "v"]]],
    ["non-ASCII key", [["ünicode", "v"]]],
    ["key with a colon", [["x:a", "v"]]],
    ["value with a newline", [["x-parity", "a\nb"]]],
  ] as ReadonlyArray<readonly [string, GrpcMetadata.GrpcMetadata]>)(
    "rejects a %s with invalid_argument on both adapters",
    async (_name, metadata) => {
      const wire = await onTheWire((invoker) =>
        Effect.flip(invoker.unary(CHECK, { service: "" }, { metadata })),
      );
      const memory = await inMemory((invoker) =>
        Effect.flip(invoker.unary(CHECK, { service: "" }, { metadata })),
      );

      expect(wire.response.code).toBe("invalid_argument");
      expect(memory.response.code).toBe("invalid_argument");
      expect(memory.response.message).toBe(wire.response.message);
      // The call never left, so no handler observed it.
      expect(wire.seen).toBeUndefined();
      expect(memory.seen).toBeUndefined();
    },
  );

  // End-to-end pin of the documented semantic. One case only: each run spins
  // up a listener, and the per-value normalization is covered without one by
  // the fake-`Transport` test below.
  it("treats timeoutMs=0 as no deadline on the wire", async () => {
    const wire = await onTheWire((invoker) =>
      invoker.unary(CHECK, { service: "" }, { timeoutMs: 0 }),
    );

    expect(wire.response).toEqual(SERVING);
    // The handler must not be told about a deadline that is not in force.
    expect(wire.seenTimeout).toBeUndefined();
  });

  it.each([0, -1])(
    "hides a non-positive timeoutMs=%s from the in-memory call context",
    async (timeoutMs) => {
      const memory = await inMemory((invoker) =>
        invoker.unary(CHECK, { service: "" }, { timeoutMs }),
      );

      expect(memory.response).toEqual(SERVING);
      expect(memory.call).toEqual({ tag: CHECK, metadata: [] });
    },
  );
});

describe("GrpcInvoker (connect call options)", () => {
  // connect's own transports happen to clamp `timeoutMs <= 0` to "no
  // deadline" before `createDeadlineSignal` sees it, so the wire test above
  // cannot distinguish forwarding a zero from dropping it. A bare `Transport`
  // — which `layerConnect` accepts — can: `createDeadlineSignal` aborts a
  // `<= 0` timeout the instant the call starts.
  it("omits a non-positive timeoutMs instead of forwarding an expired deadline", async () => {
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

    await Effect.runPromise(
      GrpcInvoker.GrpcInvoker.pipe(
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
      ),
    );

    expect(forwarded).toEqual([undefined, undefined, 5_000]);
  });
});

interface Observed<A> {
  readonly response: A;
  readonly seen: GrpcMetadata.GrpcMetadata | undefined;
  readonly seenTimeout: string | undefined;
  readonly call: GrpcInvoker.GrpcInMemoryCall | undefined;
}

/** Runs `use` against a real server over a loopback HTTP/2 listener. */
const onTheWire = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E, never>,
): Promise<Observed<A>> =>
  Effect.runPromise(
    Effect.scoped(
      Effect.gen(function* () {
        let seen: GrpcMetadata.GrpcMetadata | undefined;
        let seenTimeout: string | undefined;
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
                    seenTimeout = context.metadata.find(
                      ([key]) => key === "grpc-timeout",
                    )?.[1] as string | undefined;
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
        return { response, seen, seenTimeout, call: undefined };
      }),
    ),
  );

/** The same call against `layerInMemory`, capturing the handler's view. */
const inMemory = <A, E>(
  use: (invoker: GrpcInvoker.GrpcInvokerService) => Effect.Effect<A, E, never>,
): Promise<Observed<A>> =>
  Effect.runPromise(
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
      return {
        response,
        seen: call?.metadata,
        seenTimeout: undefined,
        call,
      };
    }),
  );

const freePort = Effect.promise(
  () =>
    new Promise<number>((resolve, reject) => {
      const server = net.createServer();
      server.once("error", reject);
      server.listen(0, "127.0.0.1", () => {
        const address = server.address();
        server.close(() => {
          if (address && typeof address === "object") resolve(address.port);
          else reject(new Error("Unable to allocate a local port"));
        });
      });
    }),
);
