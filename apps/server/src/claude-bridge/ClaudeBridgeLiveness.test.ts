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
