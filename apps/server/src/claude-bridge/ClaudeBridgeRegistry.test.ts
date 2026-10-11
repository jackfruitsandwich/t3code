import { assert, describe, it } from "@effect/vitest";

import {
  CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS,
  claudeBridgeStaleTurnEvents,
  claudeBridgeTurnAwaitingRunSince,
  claudeHomesToScan,
  claudeLaunchArgs,
  claudeLaunchEnvironment,
  isClaudeBridgeSessionRecord,
  pickSessionHome,
  type ClaudeBridgeLaunchInput,
  type ClaudeBridgeMissedTurn,
} from "./ClaudeBridgeRegistry.ts";

const input: ClaudeBridgeLaunchInput = {
  sid: "0d7a9a6e-0e4b-4a45-9e2b-1b8d0a0f6c11",
  cwd: "/work/project",
  configDir: "/home/me/.claude",
  binaryPath: "claude",
  environment: {},
  model: "claude-sonnet-4-6",
  permissionMode: "bypassPermissions",
  extraArgs: { settings: "/tmp/s.json" },
};

describe("claudeLaunchArgs", () => {
  it("passes the thread's model, permission mode and extra flags on a job's first launch", () => {
    assert.deepStrictEqual(claudeLaunchArgs(input, { hasSavedOptions: false }), [
      "--bg",
      "--resume",
      input.sid,
      "--model",
      "claude-sonnet-4-6",
      "--permission-mode",
      "bypassPermissions",
      "--settings",
      "/tmp/s.json",
    ]);
  });

  it("passes no flags when the job already has saved options, which flags would turn into a copy", () => {
    assert.deepStrictEqual(claudeLaunchArgs(input, { hasSavedOptions: true }), [
      "--bg",
      "--resume",
      input.sid,
    ]);
  });

  it("omits the default permission mode and an empty model", () => {
    assert.deepStrictEqual(
      claudeLaunchArgs(
        { ...input, model: "", permissionMode: "default", extraArgs: {} },
        { hasSavedOptions: false },
      ),
      ["--bg", "--resume", input.sid],
    );
  });
});

describe("claudeLaunchEnvironment", () => {
  it("drops the nested-session markers and names the bridge directory", () => {
    const environment = claudeLaunchEnvironment(
      { CLAUDECODE: "1", CLAUDE_CODE_ENTRYPOINT: "cli", CLAUDE_CODE_SSE_PORT: "1", PATH: "/bin" },
      "/state/bridge",
    );
    assert.deepStrictEqual(environment, { PATH: "/bin", T3CODE_BRIDGE_DIR: "/state/bridge" });
  });
});

describe("claudeHomesToScan", () => {
  it("checks the configured home first and then every sibling home that keeps sessions", () => {
    assert.deepStrictEqual(
      claudeHomesToScan("/home/me/.claude", ["/home/me/.claude", "/home/me/.claude-ev"]),
      ["/home/me/.claude", "/home/me/.claude-ev"],
    );
  });

  it("keeps the configured home even before it has a sessions folder", () => {
    assert.deepStrictEqual(claudeHomesToScan("/home/me/.claude", ["/home/me/.claude-ev"]), [
      "/home/me/.claude",
      "/home/me/.claude-ev",
    ]);
  });
});

describe("pickSessionHome", () => {
  it("picks the home whose prompt history saw the session most recently", () => {
    assert.strictEqual(
      pickSessionHome([
        { home: "/home/me/.claude", lastPromptAt: 100, hasJob: true },
        { home: "/home/me/.claude-ev", lastPromptAt: 200, hasJob: false },
      ]),
      "/home/me/.claude-ev",
    );
  });

  it("counts a home with only job state when no home recorded a prompt", () => {
    assert.strictEqual(
      pickSessionHome([
        { home: "/home/me/.claude", lastPromptAt: null, hasJob: false },
        { home: "/home/me/.claude-ev", lastPromptAt: null, hasJob: true },
      ]),
      "/home/me/.claude-ev",
    );
  });

  it("names no home when nothing recorded the session", () => {
    assert.strictEqual(
      pickSessionHome([{ home: "/home/me/.claude", lastPromptAt: null, hasJob: false }]),
      null,
    );
  });
});

describe("claudeBridgeTurnAwaitingRunSince", () => {
  const startedAt = 1_000_000;
  const endedAt = startedAt + 1_200;
  const start: ClaudeBridgeMissedTurn["start"] = {
    seq: 1,
    at: startedAt,
    type: "turn.start",
    turnId: "turn-1",
    text: "Reply with exactly: quick-1",
    transcriptOffset: 188,
  };
  const ended: ClaudeBridgeMissedTurn = {
    start,
    complete: {
      seq: 2,
      at: endedAt,
      type: "turn.complete",
      turnId: "turn-1",
      reason: "answer",
      answer: "quick-1",
      isAborted: false,
      durationMs: 1_200,
      transcriptOffset: 900,
    },
  };

  it("names a turn that ended with nothing attached while a run can still show it", () => {
    assert.strictEqual(claudeBridgeTurnAwaitingRunSince(ended, endedAt + 1_500), startedAt);
    assert.strictEqual(
      claudeBridgeTurnAwaitingRunSince(ended, endedAt + CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS - 1),
      startedAt,
    );
  });

  it("names none once no run would show it any more: the turn is history then", () => {
    assert.isUndefined(
      claudeBridgeTurnAwaitingRunSince(ended, endedAt + CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS),
    );
  });

  it("names none when an attachment took the turn, or while it still runs", () => {
    assert.isUndefined(claudeBridgeTurnAwaitingRunSince(null, endedAt));
    // A running turn is reported by the session itself, for as long as it runs.
    assert.isUndefined(claudeBridgeTurnAwaitingRunSince({ start, complete: null }, endedAt));
  });
});

describe("the record a mod process speaks for", () => {
  const sid = "aaaaaaaa-1111-4111-8111-111111111111";
  const branch = "bbbbbbbb-2222-4222-8222-222222222222";

  it("is the file named after the session's id", () => {
    assert.isTrue(isClaudeBridgeSessionRecord(sid, `/home/.claude/projects/-work/${sid}.jsonl`));
  });

  it("is not the record of the conversation the process is moving on to", () => {
    // After /branch the process names the new conversation's record while
    // its files still speak for the old session.
    assert.isFalse(
      isClaudeBridgeSessionRecord(sid, `/home/.claude/projects/-work/${branch}.jsonl`),
    );
    assert.isFalse(isClaudeBridgeSessionRecord(sid, ""));
  });
});

describe("turns that were over long before T3 read of them", () => {
  const at = 1_750_000_000_000;
  const start = (turnId: string, when: number, commandId?: string) =>
    ({
      seq: 1,
      at: when,
      type: "turn.start",
      turnId,
      text: "hi",
      transcriptOffset: 0,
      ...(commandId === undefined ? {} : { commandId }),
    }) as const;
  const complete = (turnId: string, when: number) =>
    ({
      seq: 2,
      at: when,
      type: "turn.complete",
      turnId,
      reason: "answer",
      answer: "",
      isAborted: false,
      durationMs: 900,
      transcriptOffset: 0,
    }) as const;
  const span = CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS;

  it("are history: neither their start nor their end is shown as a turn", () => {
    const events = [start("t1", at), complete("t1", at + 1_000)];
    assert.deepEqual([...claudeBridgeStaleTurnEvents(events, at + 1_000 + span)], [0, 1]);
  });

  it("are a run's to show while they ended a moment ago, or have not ended", () => {
    const events = [start("t1", at), complete("t1", at + 1_000)];
    assert.strictEqual(claudeBridgeStaleTurnEvents(events, at + span).size, 0);
    assert.strictEqual(claudeBridgeStaleTurnEvents([start("t1", at)], at + 10 * span).size, 0);
  });

  it("never include a turn sent from T3, or what T3 is waiting on", () => {
    const sent = [start("t1", at, "command-1"), complete("t1", at + 1_000)];
    assert.strictEqual(claudeBridgeStaleTurnEvents(sent, at + 10 * span).size, 0);
    const result = {
      seq: 3,
      at: at + 500,
      type: "command.result",
      commandId: "command-2",
      ok: true,
      startedTurn: false,
      waitingInTerminal: false,
      text: "done",
    } as const;
    const events = [start("t1", at), result, complete("t1", at + 1_000), start("t2", at + 60_000)];
    assert.deepEqual([...claudeBridgeStaleTurnEvents(events, at + 70_000)], [0, 2]);
  });
});
