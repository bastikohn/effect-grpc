import { Effect } from "effect";

import type { GrpcCallOptions, GrpcServerContext } from "./CodegenSupport.js";
import * as GrpcStatusError from "./GrpcStatusError.js";

/**
 * Reads the incoming budget when this effect runs and bounds an outgoing
 * call by it. An exhausted budget fails without invoking downstream work;
 * a tighter positive caller timeout wins. Other options are preserved.
 *
 * Run immediately before invoking the downstream RPC, after asynchronous
 * setup. For streams, read inside `Stream.unwrap` so subscription gets the
 * current budget. Do not cache the resulting options across calls.
 */
export const callOptions = (
  context: Pick<GrpcServerContext, "remainingTimeoutMs">,
  options: GrpcCallOptions = {},
): Effect.Effect<GrpcCallOptions, GrpcStatusError.GrpcStatusError> =>
  Effect.suspend(() => {
    const remaining = context.remainingTimeoutMs();
    if (remaining === undefined) return Effect.succeed(options);
    if (remaining <= 0) {
      return Effect.fail(
        GrpcStatusError.deadlineExceeded("Upstream deadline exhausted"),
      );
    }
    return Effect.succeed({
      ...options,
      timeoutMs:
        options.timeoutMs !== undefined && options.timeoutMs > 0
          ? Math.min(remaining, options.timeoutMs)
          : remaining,
    });
  });
