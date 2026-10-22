import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as Clock from "effect/Clock";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import type { ClaudeBridgeEvent } from "./ClaudeBridgeProtocol.ts";
import {
  CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS,
  CLAUDE_BRIDGE_OFFERED_TURN_HOLD_MS,
  ClaudeBridgeRegistry,
  claudeBridgeMissedTurnsAfter,
  claudeBridgeStaleTurnEvents,
  claudeBridgeTurnAwaitingRunSince,
  claudeBridgeTurnsAwaitingRunSince,
  claudeHomesToScan,
  claudeLaunchArgs,
  claudeLaunchEnvironment,
  isClaudeBridgeSessionRecord,
  layer as registryLayer,
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
  it("drops the nested-session markers, names the bridge directory and says nobody is at its terminal", () => {
    const environment = claudeLaunchEnvironment(
      { CLAUDECODE: "1", CLAUDE_CODE_ENTRYPOINT: "cli", CLAUDE_CODE_SSE_PORT: "1", PATH: "/bin" },
      "/state/bridge",
    );
    assert.deepStrictEqual(environment, {
      PATH: "/bin",
      T3CODE_BRIDGE_DIR: "/state/bridge",
      // The process draws on a terminal no one watches: its mod reads this.
      T3CODE_BRIDGE_LAUNCH: "background",
    });
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

  // bothplaces b36bf247: the run offered for the turn attached 17.5 s after it ended.
  it("names a turn a run was offered for for longer: that run can attach late", () => {
    const offered = { ...ended, offered: true };
    const late = endedAt + CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS + 2_500;
    assert.isUndefined(claudeBridgeTurnAwaitingRunSince(ended, late));
    assert.strictEqual(claudeBridgeTurnAwaitingRunSince(offered, late), startedAt);
    assert.isUndefined(
      claudeBridgeTurnAwaitingRunSince(offered, endedAt + CLAUDE_BRIDGE_OFFERED_TURN_HOLD_MS),
    );
  });

  it("names the earliest of the turns begun with nothing attached that still await their run", () => {
    const next: ClaudeBridgeMissedTurn = {
      start: { ...start, turnId: "turn-2", at: endedAt + 500 },
      complete: null,
    };
    assert.strictEqual(
      claudeBridgeTurnsAwaitingRunSince([{ ...ended, offered: true }, next], endedAt + 1_000),
      startedAt,
    );
    assert.isUndefined(claudeBridgeTurnsAwaitingRunSince([next], endedAt + 1_000));
    assert.isUndefined(claudeBridgeTurnsAwaitingRunSince([], endedAt));
  });
});

describe("claudeBridgeMissedTurnsAfter", () => {
  const none: ReadonlyArray<ClaudeBridgeMissedTurn> = [];
  const startOf = (turnId: string, at: number): ClaudeBridgeMissedTurn["start"] => ({
    seq: at,
    at,
    type: "turn.start",
    turnId,
    text: turnId,
    transcriptOffset: at,
  });
  const endOf = (turnId: string, at: number): NonNullable<ClaudeBridgeMissedTurn["complete"]> => ({
    seq: at,
    at,
    type: "turn.complete",
    turnId,
    reason: "answer",
    answer: turnId,
    isAborted: false,
    durationMs: 1,
    transcriptOffset: at,
  });

  // bothplaces 099b48d6: the run offered for a short turn attached 6.9 s
  // after it, when the person had typed the next one.
  it("keeps the turn a run was offered for when the next begins before that run attaches", () => {
    const turns = [startOf("turn-a", 1), endOf("turn-a", 2), startOf("turn-b", 3)].reduce(
      claudeBridgeMissedTurnsAfter,
      none,
    );
    assert.deepStrictEqual(turns, [
      { start: startOf("turn-a", 1), complete: endOf("turn-a", 2) },
      { start: startOf("turn-b", 3), complete: null },
    ]);
  });

  it("gives an end only to its own turn, and keeps the last few", () => {
    const turns = [1, 2, 3, 4, 5]
      .map((n) => startOf(`turn-${n}`, n))
      .reduce(claudeBridgeMissedTurnsAfter, none);
    assert.deepStrictEqual(
      turns.map((turn) => turn.start.turnId),
      ["turn-2", "turn-3", "turn-4", "turn-5"],
    );
    const ended = claudeBridgeMissedTurnsAfter(turns, endOf("turn-3", 6));
    assert.deepStrictEqual(
      ended.map((turn) => turn.complete?.turnId ?? null),
      [null, "turn-3", null, null],
    );
    assert.deepStrictEqual(claudeBridgeMissedTurnsAfter(turns, endOf("turn-9", 7)), turns);
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
      interim: false,
      text: "done",
    } as const;
    const events = [start("t1", at), result, complete("t1", at + 1_000), start("t2", at + 60_000)];
    assert.deepEqual([...claudeBridgeStaleTurnEvents(events, at + 70_000)], [0, 2]);
  });
});

const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

describe("the sessions that count as live", () => {
  it.effect(
    "include one whose heartbeat was fresh when read, however long the server then stood still",
    () => {
      let stallAfterNextRead = false;
      // A server kept from carrying on right after it has read a session's file
      // (seen: a query at startup holding everything for seven seconds).
      const stallingFileSystem = Layer.effect(
        FileSystem.FileSystem,
        Effect.gen(function* () {
          const fs = yield* FileSystem.FileSystem;
          return {
            ...fs,
            readFileString: (file: string, encoding?: string) =>
              fs.readFileString(file, encoding).pipe(
                Effect.tap(() => {
                  if (!stallAfterNextRead || !file.endsWith("session.json")) return Effect.void;
                  stallAfterNextRead = false;
                  return TestClock.adjust("13 seconds");
                }),
              ),
          };
        }),
      ).pipe(Layer.provide(NodeServices.layer));
      const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
      return Effect.gen(function* () {
        const registry = yield* ClaudeBridgeRegistry;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const dir = path.join(registry.bridgeDir, "claude", "session-1", "instance-1");
        yield* fs.makeDirectory(dir, { recursive: true });
        yield* fs.writeFileString(
          path.join(dir, "session.json"),
          yield* encodeJson({
            sid: "session-1",
            instanceId: "instance-1",
            // A home of its own: the session's record is not looked for in the real one.
            configDir: path.join(registry.bridgeDir, "home"),
            heartbeatAt: yield* Clock.currentTimeMillis,
          }),
        );
        stallAfterNextRead = true;
        const live = yield* registry.listLiveProcesses;
        assert.isFalse(stallAfterNextRead);
        assert.deepStrictEqual(
          live.map((entry) => entry.sid),
          ["session-1"],
        );
      }).pipe(
        Effect.provide(
          registryLayer.pipe(
            Layer.provide(stallingFileSystem),
            Layer.provide(
              ServerConfig.layerTest(process.cwd(), { prefix: "t3-bridge-registry-test-" }),
            ),
            Layer.provideMerge(platform),
          ),
        ),
      );
    },
  );
});

describe("a turn a typed command begins with what it printed", () => {
  // A /goal typed in the terminal (Claude Code 2.1.299): Claude Code reports
  // the turn it starts with what the command printed.
  const printed = "<local-command-stdout>Goal set: ship it</local-command-stdout>";
  const commandRows = [
    {
      type: "user",
      uuid: "g1",
      parentUuid: null,
      promptId: "p1",
      message: {
        role: "user",
        content:
          "<command-name>/goal</command-name>\n<command-message>goal</command-message>\n<command-args>ship it</command-args>",
      },
    },
    {
      type: "user",
      uuid: "g2",
      parentUuid: "g1",
      promptId: "p1",
      turnOrigin: "human",
      message: { role: "user", content: printed },
    },
  ];
  const lines = (rows: ReadonlyArray<unknown>) =>
    rows.map((row) => JSON.stringify(row) + "\n").join("");

  /**
   * The text T3 is told the turn began with. `rowsLate`: the record holds the
   * command's rows only once the event pump has been over the session once
   * (the session waiting on a request makes the pump say when it has).
   */
  const reportedText = (rowsLate: boolean) => {
    const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
    return Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* ClaudeBridgeRegistry;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sid = "cccccccc-3333-4333-8333-333333333333";
        const home = path.join(registry.bridgeDir, "home");
        const record = path.join(home, "projects", "-work", `${sid}.jsonl`);
        yield* fs.makeDirectory(path.dirname(record), { recursive: true });
        const earlier = lines([
          { type: "user", uuid: "u0", parentUuid: null, message: { role: "user", content: "hi" } },
        ]);
        yield* fs.writeFileString(record, rowsLate ? earlier : earlier + lines(commandRows));
        const dir = path.join(registry.bridgeDir, "claude", sid, "instance-1");
        yield* fs.makeDirectory(path.join(dir, "events"), { recursive: true });
        const now = yield* Clock.currentTimeMillis;
        yield* fs.writeFileString(
          path.join(dir, "session.json"),
          yield* encodeJson({
            sid,
            instanceId: "instance-1",
            configDir: home,
            heartbeatAt: now,
            transcriptPath: record,
            isInteractive: true,
            ...(rowsLate ? { waiting: [{ toolUseId: "tool-1", kind: "approval" }] } : {}),
          }),
        );
        yield* fs.writeFileString(
          path.join(dir, "events", "000001.json"),
          yield* encodeJson({
            seq: 1,
            at: now,
            type: "turn.start",
            turnId: "turn-1",
            text: printed,
            transcriptOffset: earlier.length,
          }),
        );
        let written = !rowsLate;
        yield* registry.onUnattachedRequest(() =>
          written
            ? Effect.void
            : fs.writeFileString(record, earlier + lines(commandRows)).pipe(
                Effect.orDie,
                Effect.andThen(
                  Effect.sync(() => {
                    written = true;
                  }),
                ),
              ),
        );
        const started = yield* Deferred.make<ClaudeBridgeEvent>();
        yield* registry.onUnattachedEvent((event) =>
          event.type === "turn.start"
            ? Deferred.succeed(started, event).pipe(Effect.asVoid)
            : Effect.void,
        );
        // The event pump reads the session's events on its next pass.
        const start = yield* Deferred.await(started);
        return start.type === "turn.start" ? start.text : undefined;
      }),
    ).pipe(
      Effect.provide(
        registryLayer.pipe(
          Layer.provide(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-bridge-registry-test-" }),
          ),
          Layer.provideMerge(platform),
        ),
      ),
    );
  };

  it.live("reaches T3 named by the command's line, read from the record", () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* reportedText(false), "/goal ship it");
    }),
  );

  it.live("waits for the command's rows when Claude Code writes them after its report", () =>
    Effect.gen(function* () {
      assert.strictEqual(yield* reportedText(true), "/goal ship it");
    }),
  );
});

describe("a call T3 has answered", () => {
  // The session lists a call until its mod has taken T3's answer: a run that
  // attached for it meanwhile would ask the person again.
  it.live("offers no run for it while the session still lists it", () => {
    const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
    return Effect.scoped(
      Effect.gen(function* () {
        const registry = yield* ClaudeBridgeRegistry;
        const fs = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;
        const sid = "dddddddd-4444-4444-8444-444444444444";
        const dir = path.join(registry.bridgeDir, "claude", sid, "instance-1");
        yield* fs.makeDirectory(path.join(dir, "events"), { recursive: true });
        yield* fs.writeFileString(
          path.join(dir, "session.json"),
          yield* encodeJson({
            sid,
            instanceId: "instance-1",
            configDir: path.join(registry.bridgeDir, "home"),
            heartbeatAt: yield* Clock.currentTimeMillis,
            isInteractive: true,
            waiting: [
              { toolUseId: "tool-1", kind: "approval" },
              { toolUseId: "tool-2", kind: "question" },
            ],
          }),
        );
        const live = yield* registry.findLiveProcess(sid);
        assert.isNotNull(live);
        if (live === null) return;
        const allow = { type: "approval", toolUseId: "tool-1", decision: "allow" } as const;
        yield* registry.send(live, allow);
        const offered = yield* Deferred.make<ReadonlyArray<string>>();
        yield* registry.onUnattachedRequest((_process, waiting) =>
          Deferred.succeed(
            offered,
            waiting.map((request) => request.toolUseId),
          ).pipe(Effect.asVoid),
        );
        assert.deepStrictEqual(yield* Deferred.await(offered), ["tool-2"]);
        assert.deepStrictEqual(yield* registry.sentAnswer(live, "tool-1"), allow);
        assert.isNull(yield* registry.sentAnswer(live, "tool-2"));
      }),
    ).pipe(
      Effect.provide(
        registryLayer.pipe(
          Layer.provide(
            ServerConfig.layerTest(process.cwd(), { prefix: "t3-bridge-registry-test-" }),
          ),
          Layer.provideMerge(platform),
        ),
      ),
    );
  });
});
