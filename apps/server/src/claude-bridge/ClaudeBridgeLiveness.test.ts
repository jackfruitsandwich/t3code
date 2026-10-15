import { assert, it } from "@effect/vitest";
import { ProviderDriverKind } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { ProviderSessionLiveness } from "../orchestration-v2/ProviderSessionLiveness.ts";
import * as ClaudeBridgeLiveness from "./ClaudeBridgeLiveness.ts";
import { parseClaudeBridgeSessionFile } from "./ClaudeBridgeProtocol.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

const processOf = (sid: string, file: Record<string, unknown>) => {
  const session = parseClaudeBridgeSessionFile({
    sid,
    instanceId: `instance-${sid}`,
    heartbeatAt: 1_750_000_000_000,
    transcriptPath: `/records/${sid}.jsonl`,
    ...file,
  });
  assert.isNotNull(session);
  return { sid, instanceId: `instance-${sid}`, dir: `/bridge/${sid}`, session: session! };
};

const processes: Record<string, ClaudeBridgeRegistry.ClaudeBridgeProcess> = {
  terminal: processOf("terminal", {
    isInteractive: true,
    activeTurn: { turnId: "turn-1", startedAt: 1_749_999_000_000, transcriptOffset: 0 },
  }),
  "terminal-idle": processOf("terminal-idle", { isInteractive: true }),
  // The SDK child a T3 thread's first turn runs in ends with the server.
  "sdk-child": processOf("sdk-child", {
    isInteractive: false,
    activeTurn: { turnId: "turn-1", startedAt: 1_749_999_000_000, transcriptOffset: 0 },
  }),
};

it.effect("says which Claude sessions outlive the server, and whether they run a turn", () =>
  Effect.gen(function* () {
    const asked: Array<string> = [];
    const none = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry.pipe(
      Effect.provide(ClaudeBridgeRegistry.layerTest),
    );
    const liveness = yield* ProviderSessionLiveness.pipe(
      Effect.provide(
        ClaudeBridgeLiveness.layer.pipe(
          Layer.provide(
            Layer.succeed(
              ClaudeBridgeRegistry.ClaudeBridgeRegistry,
              ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
                ...none,
                findLiveProcess: (sid) =>
                  Effect.sync(() => {
                    asked.push(sid);
                    return processes[sid] ?? null;
                  }),
              }),
            ),
          ),
        ),
      ),
    );
    const of = (nativeThreadId: string, driver = CLAUDE_PROVIDER) =>
      liveness.ofNativeThread({ driver, nativeThreadId });
    assert.deepStrictEqual(yield* of("terminal"), {
      state: "running",
      turnStartedAt: 1_749_999_000_000,
    });
    assert.deepStrictEqual(yield* of("terminal-idle"), { state: "idle" });
    assert.deepStrictEqual(yield* of("sdk-child"), { state: "gone" });
    assert.deepStrictEqual(yield* of("ended"), { state: "gone" });
    assert.deepStrictEqual(yield* of("terminal", ProviderDriverKind.make("codex")), {
      state: "gone",
    });
    assert.deepStrictEqual(asked, ["terminal", "terminal-idle", "sdk-child", "ended"]);
  }),
);

// The left arrow sent these conversations to the background: each carries on
// under a new session id, and the thread still names the old one.
const moved: Record<string, ClaudeBridgeRegistry.ClaudeBridgeProcess> = {
  "left-idle": processOf("left-idle", { isInteractive: true, configDir: "/home/a" }),
  "moved-running": processOf("moved-running", {
    isInteractive: true,
    configDir: "/home/a",
    activeTurn: { turnId: "turn-2", startedAt: 1_749_999_500_000, transcriptOffset: 0 },
  }),
  "moved-idle": processOf("moved-idle", { isInteractive: true, configDir: "/home/b" }),
  "held-idle": processOf("held-idle", { isInteractive: true, configDir: "/home/a" }),
  "plain-idle": processOf("plain-idle", { isInteractive: true, configDir: "/home/a" }),
};
const continuedIn: Record<string, ClaudeBridgeRegistry.ClaudeBridgeContinuation> = {
  "left-idle": { sid: "moved-running", heldElsewhere: false },
  "left-gone": { sid: "moved-idle", heldElsewhere: false },
  // Another thread already holds where it went.
  "held-idle": { sid: "moved-running", heldElsewhere: true },
};

it.effect("answers for a conversation from the session it carried on in", () =>
  Effect.gen(function* () {
    const lookedIn: Array<string> = [];
    const none = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry.pipe(
      Effect.provide(ClaudeBridgeRegistry.layerTest),
    );
    const liveness = yield* ProviderSessionLiveness.pipe(
      Effect.provide(
        ClaudeBridgeLiveness.layer.pipe(
          Layer.provide(
            Layer.succeed(
              ClaudeBridgeRegistry.ClaudeBridgeRegistry,
              ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
                ...none,
                listLiveProcesses: Effect.succeed(Object.values(moved)),
                findLiveProcess: (sid) => Effect.succeed(moved[sid] ?? null),
                continuationOf: (sid, configDir) =>
                  Effect.sync(() => {
                    lookedIn.push(`${sid}@${configDir}`);
                    // The new record sits beside the old one, in the same home.
                    const to = continuedIn[sid];
                    return to !== undefined && moved[to.sid]?.session.configDir === configDir
                      ? to
                      : null;
                  }),
              }),
            ),
          ),
        ),
      ),
    );
    const of = (nativeThreadId: string) =>
      liveness.ofNativeThread({ driver: CLAUDE_PROVIDER, nativeThreadId });
    // Left behind at a fresh prompt while the conversation runs a turn elsewhere.
    assert.deepStrictEqual(yield* of("left-idle"), {
      state: "running",
      turnStartedAt: 1_749_999_500_000,
    });
    // Left behind and closed: the conversation lives on, between turns.
    assert.deepStrictEqual(yield* of("left-gone"), { state: "idle" });
    assert.deepStrictEqual(yield* of("held-idle"), { state: "idle" });
    assert.deepStrictEqual(yield* of("plain-idle"), { state: "idle" });
    assert.deepStrictEqual(yield* of("never-seen"), { state: "gone" });
    // A process left behind is asked in its own home; one that is gone, in
    // the home of each live session.
    assert.deepStrictEqual(lookedIn, [
      "left-idle@/home/a",
      "left-gone@/home/a",
      "left-gone@/home/b",
      "held-idle@/home/a",
      "plain-idle@/home/a",
      "never-seen@/home/a",
      "never-seen@/home/b",
    ]);
  }),
);
