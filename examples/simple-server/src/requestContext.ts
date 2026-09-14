import {
  createContextKey,
  type ContextKey,
  type Interceptor,
} from "@connectrpc/connect";
import { Context } from "effect";

import type { CodegenSupport } from "@effect-grpc/effect-grpc";

/**
 * Request correlation through the native server interceptor chain.
 *
 * One module owns the typed key, so the interceptor that sets it and the
 * handlers that read it cannot disagree about its type. The value is minted
 * on the server: nothing here trusts a client-supplied header.
 */
export const requestIdKey: ContextKey<string | undefined> = createContextKey<
  string | undefined
>(undefined, { description: "server-generated request id" });

/** Tags every accepted call with a fresh request id before its handler runs. */
export const attachRequestId: Interceptor = (next) => async (request) => {
  request.contextValues.set(requestIdKey, crypto.randomUUID());
  return next(request);
};

/**
 * The application's own view of the current call — an ordinary Effect
 * service, provided per call by the handler that owns the call context (see
 * `main.ts`). Domain code depends on this, not on the gRPC context.
 */
export interface CurrentRequestService {
  /** `undefined` when {@link attachRequestId} is not installed. */
  readonly requestId: string | undefined;
  /** The registry tag of the method being served, e.g. `demo.v1.UserService/GetUser`. */
  readonly method: string;
}

export class CurrentRequest extends Context.Service<
  CurrentRequest,
  CurrentRequestService
>()("@effect-grpc/simple-server/CurrentRequest") {}

/** Builds the {@link CurrentRequest} value for one call from its context. */
export const currentRequest = (
  context: CodegenSupport.GrpcServerContext,
): CurrentRequestService => ({
  requestId: context.getContextValue(requestIdKey),
  method: context.method.tag,
});
