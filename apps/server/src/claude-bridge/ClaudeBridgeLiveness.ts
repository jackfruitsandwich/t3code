import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { ProviderSessionLiveness } from "../orchestration-v2/ProviderSessionLiveness.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

/**
 * A Claude Code session outlives the server when a mod process holds it that
 * a person runs (a terminal, or a `claude --bg` job): an SDK child of the
 * server ends with it, whatever its last heartbeat says.
 */
export const layer = Layer.effect(
  ProviderSessionLiveness,
  Effect.gen(function* () {
    const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;
    return {
      ofNativeThread: (input) =>
        input.driver !== CLAUDE_PROVIDER
          ? Effect.succeed({ state: "gone" as const })
          : registry.findLiveProcess(input.nativeThreadId).pipe(
              Effect.map((entry) =>
                entry === null || !entry.session.isInteractive
                  ? { state: "gone" as const }
                  : entry.session.activeTurn === null
                    ? { state: "idle" as const }
                    : {
                        state: "running" as const,
                        turnStartedAt: entry.session.activeTurn.startedAt,
                      },
              ),
            ),
    };
  }),
);
