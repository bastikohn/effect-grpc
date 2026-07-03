import * as fs from "node:fs";
import * as net from "node:net";
import * as path from "node:path";
import { Effect, Layer } from "effect";
import { describe, expect, it } from "vitest";

import { GrpcClientProtocol, GrpcNodeServer } from "@effect-grpc/effect-grpc";
import {
  UserServiceClient,
  UserServiceClientLayer,
  UserServiceGrpcRegistry,
  UserServiceHandlersLayer,
  UserServiceRpcGroup,
  type UserServiceImplementation,
} from "@effect-grpc/simple-proto/generated/demo/v1/user_service_effect_grpc";

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
  it("round-trips a unary call over TLS", async () => {
    const response = await Effect.runPromise(
      withServer({ tls: serverTls }, (baseUrl) =>
        getUser(baseUrl, { tls: { ca } }),
      ),
    );

    expect(response).toEqual({ user: { id: "123", name: "User 123" } });
  });

  it("rejects a server that is not trusted by the configured CA", async () => {
    const error = await Effect.runPromise(
      withServer({ tls: serverTls }, (baseUrl) =>
        getUser(baseUrl, { tls: {} }).pipe(Effect.flip),
      ),
    );

    // connect-node surfaces TLS handshake failures as code `internal`.
    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "internal",
    });
  });

  it("connects to an untrusted server when rejectUnauthorized is false", async () => {
    const response = await Effect.runPromise(
      withServer({ tls: serverTls }, (baseUrl) =>
        getUser(baseUrl, { tls: { rejectUnauthorized: false } }),
      ),
    );

    expect(response).toEqual({ user: { id: "123", name: "User 123" } });
  });

  it("round-trips a unary call over mTLS", async () => {
    const response = await Effect.runPromise(
      withServer({ tls: { ...serverTls, clientCa: ca } }, (baseUrl) =>
        getUser(baseUrl, { tls: { ca, ...clientCert } }),
      ),
    );

    expect(response).toEqual({ user: { id: "123", name: "User 123" } });
  });

  it("rejects mTLS clients that present no certificate", async () => {
    const error = await Effect.runPromise(
      withServer({ tls: { ...serverTls, clientCa: ca } }, (baseUrl) =>
        getUser(baseUrl, { tls: { ca } }).pipe(Effect.flip),
      ),
    );

    // connect-node surfaces TLS handshake failures as code `internal`.
    expect(error).toMatchObject({
      _tag: "GrpcStatusError",
      code: "internal",
    });
  });
});

describe("makeTransport TLS validation", () => {
  it("requires an https baseUrl when tls is set", () => {
    expect(() =>
      GrpcClientProtocol.makeTransport({
        baseUrl: "http://127.0.0.1:1",
        tls: { ca },
      }),
    ).toThrowError(/requires an https:\/\/ baseUrl/);
  });

  it("requires cert and key together", () => {
    expect(() =>
      GrpcClientProtocol.makeTransport({
        baseUrl: "https://127.0.0.1:1",
        tls: { cert: clientCert.cert },
      }),
    ).toThrowError(/both 'cert' and 'key'/);
  });
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
  Effect.scoped(
    Effect.gen(function* () {
      const port = yield* freePort;
      yield* GrpcNodeServer.serveAll({
        host: "127.0.0.1",
        port,
        tls: options.tls,
        services: [
          {
            group: UserServiceRpcGroup,
            registry: UserServiceGrpcRegistry,
            handlers: UserServiceHandlersLayer(implementation),
          },
        ],
      }).pipe(Effect.forkScoped);
      yield* Effect.sleep("50 millis");

      return yield* use(new URL(`https://127.0.0.1:${port}`));
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
          if (address && typeof address === "object") {
            resolve(address.port);
          } else {
            reject(new Error("Unable to allocate a local port"));
          }
        });
      });
    }),
);
