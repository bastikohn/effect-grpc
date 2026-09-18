import { createServer, type Server } from "node:http";
import { setTimeout as delay } from "node:timers/promises";

import { test, expect, type Page } from "@playwright/test";
import { Code, ConnectError, cors } from "@connectrpc/connect";
import { connectNodeAdapter } from "@connectrpc/connect-node";
import { build } from "esbuild";

import { UserService } from "../../../examples/simple-proto/src/generated/demo/v1/user_service_pb.js";
import type { BrowserRequest } from "./browser.js";

const call = (page: Page, args: BrowserRequest) =>
  page.evaluate((request) => globalThis.browserRpc(request), args);

let appOrigin: string;
let rpcOrigin: string;
let deniedOrigin: string;
const servers: Server[] = [];
const completed = new Map<string, boolean>();
let allowedPreflights = 0;
let deniedPreflights = 0;
let deniedCalls = 0;

const listen = async (server: Server) => {
  servers.push(server);
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  const address = server.address();
  if (!address || typeof address === "string")
    throw new Error("missing server address");
  return `http://127.0.0.1:${address.port}`;
};

test.beforeAll(async () => {
  const bundle = await build({
    entryPoints: ["packages/effect-grpc/test-browser/browser.ts"],
    platform: "browser",
    bundle: true,
    format: "iife",
    write: false,
  });
  appOrigin = await listen(
    createServer((req, res) => {
      res.setHeader(
        "content-type",
        req.url === "/client.js" ? "text/javascript" : "text/html",
      );
      res.end(
        req.url === "/client.js"
          ? bundle.outputFiles[0].text
          : '<script src="/client.js"></script>',
      );
    }),
  );
  const adapter = connectNodeAdapter({
    routes(router) {
      router.service(UserService, {
        async getUser(request, context) {
          if (
            context.requestHeader.get("authorization") !==
            "Bearer browser-token"
          ) {
            throw new ConnectError(
              "missing browser token",
              Code.Unauthenticated,
            );
          }
          if (request.id.startsWith("slow")) {
            try {
              await delay(5000, undefined, { signal: context.signal });
            } finally {
              completed.set(request.id, context.signal.aborted);
            }
          }
          return {
            user: {
              id: request.id,
              name: JSON.stringify({
                auth: context.requestHeader.get("authorization"),
                binary: context.requestHeader.get("trace-bin"),
                requestId: context.requestHeader.get("x-request-id"),
              }),
            },
          };
        },
        async *watchUsers(request, context) {
          try {
            for (let sequence = 0; sequence < request.count; sequence += 1) {
              yield {
                id: request.tenantId,
                name: "Browser",
                action: "update",
                sequence,
              };
              await delay(15, undefined, { signal: context.signal });
            }
          } finally {
            completed.set(request.tenantId, context.signal.aborted);
          }
        },
      });
    },
  });
  rpcOrigin = await listen(
    createServer((req, res) => {
      if (req.headers.origin === appOrigin) {
        res.setHeader("access-control-allow-origin", appOrigin);
        res.setHeader("vary", "Origin");
        res.setHeader(
          "access-control-allow-methods",
          cors.allowedMethods.join(","),
        );
        res.setHeader(
          "access-control-allow-headers",
          [
            ...cors.allowedHeaders,
            "authorization",
            "trace-bin",
            "x-request-id",
            "traceparent",
          ].join(","),
        );
        res.setHeader(
          "access-control-expose-headers",
          cors.exposedHeaders.join(","),
        );
      }
      if (req.method === "OPTIONS") {
        allowedPreflights += 1;
        res.writeHead(204).end();
      } else {
        void adapter(req, res);
      }
    }),
  );
  deniedOrigin = await listen(
    createServer((req, res) => {
      if (req.method === "OPTIONS") deniedPreflights += 1;
      else deniedCalls += 1;
      res.writeHead(204).end();
    }),
  );
});

test.afterAll(async () => {
  await Promise.all(
    servers.map(
      (server) =>
        new Promise<void>((resolve, reject) => {
          server.close((error) => (error ? reject(error) : resolve()));
          server.closeAllConnections();
        }),
    ),
  );
});

for (const protocol of ["connect", "grpc-web"] as const) {
  test.describe(protocol, () => {
    test.beforeEach(async ({ page }) => {
      await page.goto(appOrigin);
      await page.waitForFunction(
        () => typeof globalThis.browserRpc === "function",
      );
    });

    test("unary metadata and authentication cross the browser CORS boundary", async ({
      page,
    }) => {
      const id = `${protocol}-metadata`;
      const request: BrowserRequest = {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "unary",
        id,
      };
      const result = await call(page, request);
      expect(result.ok).toBe(true);
      if (!result.ok || !("user" in result.value) || !result.value.user)
        throw new Error("missing unary response");
      expect(JSON.parse(result.value.user.name)).toEqual({
        auth: "Bearer browser-token",
        binary: "AAH6/w==",
        requestId: id,
      });
      expect(allowedPreflights).toBeGreaterThan(0);
    });

    test("authentication failures retain their RPC status across CORS", async ({
      page,
    }) => {
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "unauthorized",
        id: `${protocol}-unauthorized`,
      });
      expect(result).toMatchObject({ ok: false, code: "unauthenticated" });
    });

    test("streams responses to natural completion", async ({ page }) => {
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "stream",
        id: `${protocol}-stream`,
      });
      expect(result.ok).toBe(true);
      expect(result.values).toBe(3);
    });

    test("enforces one deadline despite frequent streaming messages", async ({
      page,
    }) => {
      const id = `${protocol}-deadline`;
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "deadline",
        id,
      });
      expect(result).toMatchObject({ ok: false, code: "deadline_exceeded" });
      expect(result.values).toBeGreaterThan(0);
      expect(result.values).toBeLessThan(100);
      expect(result.elapsed).toBeLessThan(2000);
      await expect.poll(() => completed.get(id)).toBe(true);
    });

    test("unary deadline aborts server work", async ({ page }) => {
      const id = `slow-${protocol}`;
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "unary-deadline",
        id,
      });
      expect(result).toMatchObject({ ok: false, code: "deadline_exceeded" });
      await expect.poll(() => completed.get(id)).toBe(true);
    });

    test("early stream termination cancels server work", async ({ page }) => {
      const id = `${protocol}-cancel`;
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "cancel",
        id,
      });
      expect(result.ok).toBe(true);
      expect(result.values).toBe(1);
      await expect.poll(() => completed.get(id)).toBe(true);
    });

    test("rejects request streaming without acquiring its source", async ({
      page,
    }) => {
      const result = await call(page, {
        baseUrl: rpcOrigin,
        protocol,
        scenario: "unsupported",
        id: "unused",
      });
      expect(result).toMatchObject({
        ok: true,
        value: { codes: ["unimplemented", "unimplemented"], acquired: 0 },
      });
    });

    test("CORS denial becomes a typed failure before the RPC is sent", async ({
      page,
    }) => {
      const before = deniedPreflights;
      const result = await call(page, {
        baseUrl: deniedOrigin,
        protocol,
        scenario: "unary",
        id: "denied",
      });
      expect(result).toMatchObject({ ok: false, code: "unknown" });
      expect(deniedPreflights).toBeGreaterThan(before);
      expect(deniedCalls).toBe(0);
    });
  });
}
