import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import {
  ProviderSessionLiveness,
  type ProviderSessionLivenessState,
} from "../orchestration-v2/ProviderSessionLiveness.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

const stateOf = (
  entry: ClaudeBridgeRegistry.ClaudeBridgeProcess | null,
): ProviderSessionLivenessState =>
  entry === null || !entry.session.isInteractive
    ? { state: "gone" }
    : entry.session.activeTurn === null
      ? { state: "idle" }
      : { state: "running", turnStartedAt: entry.session.activeTurn.startedAt };

/**
 * A Claude Code session outlives the server when a mod process holds it that
 * a person runs (a terminal, or a `claude --bg` job): an SDK child of the
 * server ends with it, whatever its last heartbeat says.
 *
 * A conversation sent to the background in its terminal carries on under a
 * new session id while the thread keeps the old one. The session it went on
 * in answers for it: the one left behind sits idle at a fresh prompt, or is
 * gone once its terminal closes, while the conversation may be mid-turn.
 */
export const layer = Layer.effect(
  ProviderSessionLiveness,
  Effect.gen(function* () {
    const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;

    /**
     * The live process the conversation of `sid` went on in, or null. Its
     * record names the session (every link followed); read in the home of
     * the process left behind, or, when that is gone, in the home of each
     * live session, since the new record sits beside the old one.
     */
    const movedTo = (sid: string, own: ClaudeBridgeRegistry.ClaudeBridgeProcess | null) =>
      Effect.gen(function* () {
        const homes =
          own === null
            ? [
                ...new Set(
                  (yield* registry.listLiveProcesses).map((entry) => entry.session.configDir),
                ),
              ]
            : [own.session.configDir];
        for (const home of homes) {
          const moved = yield* registry.continuationOf(sid, home);
          // Held by a thread of its own, the conversation is not this one's to show.
          if (moved === null || moved.heldElsewhere || moved.sid === sid) continue;
          const entry = yield* registry.findLiveProcess(moved.sid);
          if (entry !== null) return entry;
        }
        return null;
      });

    return {
      ofNativeThread: (input) =>
        input.driver !== CLAUDE_PROVIDER
          ? Effect.succeed({ state: "gone" as const })
          : Effect.gen(function* () {
              const own = yield* registry.findLiveProcess(input.nativeThreadId);
              const ownState = stateOf(own);
              if (ownState.state === "running") return ownState;
              const movedState = stateOf(yield* movedTo(input.nativeThreadId, own));
              return movedState.state === "gone" ? ownState : movedState;
            }),
    };
  }),
);
