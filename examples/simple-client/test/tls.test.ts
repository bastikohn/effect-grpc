import * as fs from "node:fs";
import * as path from "node:path";
import { assert, describe, it } from "@effect/vitest";
import { Effect, Exit, Layer } from "effect";

import { GrpcClientProtocol, GrpcNodeServer } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlers,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

import { withServer as serve } from "./support.ts";

// Long-lived self-signed chain committed for tests only; regenerate with
// fixtures/tls/generate.sh.
const fixture = (name: string): Buffer =>
  fs.readFileSync(path.join(import.meta.dirname, "fixtures", "tls", name));

const ca = fixture("ca.crt");
const serverTls = { key: fixture("server.key"), cert: fixture("server.crt") };
const clientCert = { cert: fixture("client.crt"), key: fixture("client.key") };

const implementation: UserServiceImplementation = {
  getUser: (request) =>
    Effect.succeed({
      user: { id: request.id, name: `User ${request.id}` },
    }),
  watchUsers: () => {
    throw new Error("not used in TLS tests");
  },
};

describe("TLS e2e", () => {
  it.live("round-trips a unary call over TLS", () =>
    Effect.gen(function* () {
      const response = yield* withServer({ tls: serverTls }, (baseUrl) =>
        getUser(baseUrl, { tls: { ca } }),
      );

      assert.deepStrictEqual(response, {
        user: { id: "123", name: "User 123" },
      });
    }),
  );

  it.live("rejects a server that is not trusted by the configured CA", () =>
    Effect.gen(function* () {
      const error = yield* withServer({ tls: serverTls }, (baseUrl) =>
        getUser(baseUrl, { tls: {} }).pipe(Effect.flip),
      );

      // connect-node surfaces TLS handshake failures as code `internal`.
      assert.deepInclude(error, {
        _tag: "GrpcStatusError",
        code: "internal",
      });
    }),
  );

  it.live(
    "connects to an untrusted server when rejectUnauthorized is false",
    () =>
      Effect.gen(function* () {
        const response = yield* withServer({ tls: serverTls }, (baseUrl) =>
          getUser(baseUrl, { tls: { rejectUnauthorized: false } }),
        );

        assert.deepStrictEqual(response, {
          user: { id: "123", name: "User 123" },
        });
      }),
  );

  it.live("round-trips a unary call over mTLS", () =>
    Effect.gen(function* () {
      const response = yield* withServer(
        { tls: { ...serverTls, clientCa: ca } },
        (baseUrl) => getUser(baseUrl, { tls: { ca, ...clientCert } }),
      );

      assert.deepStrictEqual(response, {
        user: { id: "123", name: "User 123" },
      });
    }),
  );

  it.live("rejects mTLS clients that present no certificate", () =>
    Effect.gen(function* () {
      const error = yield* withServer(
        { tls: { ...serverTls, clientCa: ca } },
        (baseUrl) => getUser(baseUrl, { tls: { ca } }).pipe(Effect.flip),
      );

      // connect-node surfaces TLS handshake failures as code `internal`.
      assert.deepInclude(error, {
        _tag: "GrpcStatusError",
        code: "internal",
      });
    }),
  );
});

describe("makeTransport TLS validation", () => {
  it.effect("requires an https baseUrl when tls is set", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        GrpcClientProtocol.makeTransport({
          baseUrl: "http://127.0.0.1:1",
          tls: { ca },
        }),
      );

      // A contradictory `tls` block is a wiring defect, not a typed failure.
      assert.isTrue(Exit.isFailure(exit));
      assert.match(String(exit), /requires an https:\/\/ baseUrl/);
    }),
  );

  it.effect("requires cert and key together", () =>
    Effect.gen(function* () {
      const exit = yield* Effect.exit(
        GrpcClientProtocol.makeTransport({
          baseUrl: "https://127.0.0.1:1",
          tls: { ca, cert: clientCert.cert },
        }),
      );

      assert.isTrue(Exit.isFailure(exit));
      assert.match(String(exit), /both 'cert' and 'key'/);
    }),
  );
});

const getUser = (
  baseUrl: URL,
  options: { readonly tls?: GrpcClientProtocol.GrpcClientTlsOptions },
) =>
  Effect.gen(function* () {
    const client = yield* UserServiceClient;
    return yield* client.getUser({ id: "123" });
  }).pipe(
    Effect.provide(
      UserServiceClientLayer.pipe(
        Layer.provide(
          GrpcClientProtocol.layer({
            baseUrl: baseUrl.toString().replace(/\/$/, ""),
            defaultTimeoutMs: 1_000,
            registry: UserServiceGrpcRegistry,
            tls: options.tls,
          }),
        ),
      ),
    ),
  );

const withServer = <A, E, R>(
  options: { readonly tls: GrpcNodeServer.GrpcServerTlsOptions },
  use: (baseUrl: URL) => Effect.Effect<A, E, R>,
): Effect.Effect<A, E, R> =>
  serve(
    {
      tls: options.tls,
      services: [
        {
          registry: UserServiceGrpcRegistry,
          handlers: UserServiceHandlers(implementation),
        },
      ],
    },
    use,
  );
