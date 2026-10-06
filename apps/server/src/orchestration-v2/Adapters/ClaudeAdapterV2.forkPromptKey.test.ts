import { assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { MessageId, RunAttemptId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";

type KeyInput = Parameters<typeof ClaudeAdapterV2.claudePromptKey>[0];

const input = (historyOrigin: string | undefined, messageId: string): KeyInput =>
  ({
    attemptId: RunAttemptId.make("attempt:run:thread:t1:ordinal:1:1"),
    appThread: { historyOrigin },
    message: { messageId: MessageId.make(messageId) },
  }) as unknown as KeyInput;

describe("claudePromptKey", () => {
  it.effect("gives a re-migrated v1 thread a new prompt uuid for the same attempt id", () =>
    Effect.gen(function* () {
      // Rehearsal copy and live database both number the thread's first v2 run 1.
      const rehearsal = yield* ClaudeAdapterV2.claudePromptUuid(
        ClaudeAdapterV2.claudePromptKey(input("v1_import", "a823b7bd-15a7-4051-bd3e-cf25b2a811f7")),
      );
      const live = yield* ClaudeAdapterV2.claudePromptUuid(
        ClaudeAdapterV2.claudePromptKey(input("v1_import", "0b5f3a7e-2c11-4d7a-9f43-5d0c6b1e8a22")),
      );
      assert.notStrictEqual(rehearsal, live);
    }).pipe(Effect.provide(NodeServices.layer)),
  );

  it("keeps upstream's attempt-only key for threads born in v2", () => {
    const key = ClaudeAdapterV2.claudePromptKey(input(undefined, "msg-1"));
    assert.strictEqual(key, "attempt:run:thread:t1:ordinal:1:1");
  });
});
