import { assert, describe, it } from "@effect/vitest";

import {
  claudeApprovalMode,
  claudeBridgeAlsoInTerminal,
  claudeBridgeApproveWithMode,
  claudeSessionGrant,
  claudeBridgeAskingAgent,
  claudeBridgeAttachmentPaths,
  claudeBridgeMidTurnWords,
  CLAUDE_MID_TURN_CLOSING,
  CLAUDE_MID_TURN_OPENING,
  claudeBridgeModAtLeast,
  claudeBridgeRequestTurnText,
  claudeBridgeWakeDetail,
  claudeBridgeWakeRequest,
  claudeTranscriptCommandOutput,
  claudeTranscriptCommandText,
  claudeTranscriptOffsetAt,
  claudeTurnPromptLabel,
  isClaudeNarrationBlock,
  isClaudeTypedMidTurnRow,
  claudeContinuedInSession,
  claudeSessionContinuedNotice,
  claudeSessionNoticeText,
  claudeBridgeCompactSummary,
  claudeHookNoticeText,
  isClaudeBridgeNoticeFrame,
  makeClaudeBridgeFrameTranslator,
  makeClaudePlainCommandReader,
  parseClaudeBridgeEvent,
  claudeBridgeWakeOrigin,
  claudeBridgeWakeSender,
  isClaudePromptOfThePerson,
  claudeCompactingFrame,
  claudeSessionNoticeFrame,
  claudeSessionNotificationText,
  CLAUDE_TERMINAL_DIALOG_MARK,
  parseClaudeBridgeSessionFile,
  parseClaudeBridgeSlashCommands,
  parseClaudeSlashCommandText,
  readTranscriptRows,
  threadSettingsForClaudeSession,
} from "./ClaudeBridgeProtocol.ts";

const SID = "11111111-1111-4111-8111-111111111111";
const PROMPT_UUID = "22222222-2222-4222-8222-222222222222";

const translator = () =>
  makeClaudeBridgeFrameTranslator({
    sessionId: SID,
    cwd: "/workspace",
    model: "claude-opus-5-5",
    permissionMode: "default",
  });

/** A thinking block's signature as the API writes one, with `kind` where Claude Code reads it. */
const signatureOf = (kind: string): string => {
  const field = (number: number, body: Buffer) =>
    Buffer.concat([Buffer.from([(number << 3) | 2, body.length]), body]);
  const header = Buffer.concat([Buffer.from([0x08, 0x01]), field(8, Buffer.from(kind, "utf8"))]);
  return field(2, field(1, header)).toString("base64");
};

const assistantRow = (uuid: string, content: ReadonlyArray<Record<string, unknown>>) => ({
  type: "assistant",
  uuid,
  parentUuid: null,
  isSidechain: false,
  requestId: `req_${uuid}`,
  message: {
    id: "msg_1",
    type: "message",
    role: "assistant",
    model: "claude-opus-5-5",
    content,
    stop_reason: "end_turn",
    usage: {
      input_tokens: 3,
      output_tokens: 9,
      cache_read_input_tokens: 10,
      cache_creation_input_tokens: 2,
    },
  },
});

describe("ClaudeBridgeProtocol frame translator", () => {
  it("stamps a T3 prompt's uuid on the turn's first assistant frame and its result", () => {
    const frames = translator();
    const started = frames.onTurnStart({
      seq: 1,
      at: 1,
      type: "turn.start",
      turnId: "turn-1",
      text: "hello",
      transcriptOffset: 0,
      commandId: "cmd-1",
      promptUuid: PROMPT_UUID,
    });
    assert.deepEqual(
      started.map((frame) => frame.type),
      ["system"],
      "a T3 turn opens with init only",
    );
    const first = frames.onTranscriptRow(assistantRow("a1", [{ type: "text", text: "Hi" }]));
    const second = frames.onTranscriptRow(
      assistantRow("a2", [{ type: "tool_use", id: "tu1", name: "Bash", input: { command: "ls" } }]),
    );
    assert.equal(first[0]?.type, "assistant");
    assert.equal(Reflect.get(first[0]!, "user_message_uuid"), PROMPT_UUID);
    assert.isUndefined(Reflect.get(second[0]!, "user_message_uuid"));
    const toolResult = frames.onTranscriptRow({
      type: "user",
      uuid: "u1",
      isSidechain: false,
      message: {
        role: "user",
        content: [{ type: "tool_result", tool_use_id: "tu1", content: "file.txt" }],
      },
      toolUseResult: { stdout: "file.txt" },
    });
    assert.equal(toolResult[0]?.type, "user");
    assert.deepEqual(Reflect.get(toolResult[0]!, "tool_use_result"), { stdout: "file.txt" });
    frames.onTranscriptRow({ type: "cost-state", totalCostUSD: 0.5, modelUsage: {} });
    const result = frames.onTurnComplete({
      seq: 2,
      at: 2,
      type: "turn.complete",
      turnId: "turn-1",
      reason: "answer",
      answer: "Done.",
      isAborted: false,
      durationMs: 1200,
      transcriptOffset: 0,
      commandId: "cmd-1",
      promptUuid: PROMPT_UUID,
    });
    const frame = result[0];
    assert.equal(frame?.type, "result");
    if (frame?.type !== "result") return;
    assert.equal(frame.subtype, "success");
    assert.equal(frame.user_message_uuid, PROMPT_UUID);
    assert.equal(frame.total_cost_usd, 0.5);
    assert.equal(frame.num_turns, 2);
    assert.equal(frame.usage.output_tokens, 9);
    assert.isFalse(frames.hasOpenTurn());
  });

  it("opens a terminal-typed turn with a wake user frame carrying the text, and marks its result foreign", () => {
    const frames = translator();
    const started = frames.onTurnStart({
      seq: 1,
      at: 1,
      type: "turn.start",
      turnId: "turn-2",
      text: "fix the tests",
      transcriptOffset: 0,
    });
    assert.deepEqual(
      started.map((frame) => frame.type),
      ["user", "system"],
    );
    assert.equal(claudeBridgeWakeDetail(started[0]!), "fix the tests");
    assert.isTrue(frames.hasOpenTurn());
    const result = frames.onTurnComplete({
      seq: 2,
      at: 2,
      type: "turn.complete",
      turnId: "turn-2",
      reason: "aborted",
      answer: "",
      isAborted: true,
      durationMs: 10,
      transcriptOffset: 0,
    });
    const frame = result[0];
    if (frame?.type !== "result") throw new Error("expected result");
    assert.equal(frame.user_message_uuid, "bridge:turn-2");
    assert.equal(frame.terminal_reason, "aborted_streaming");
  });

  it("marks a turn opened to carry a request, with who asks, and leaves a typed turn unmarked", () => {
    const frames = translator();
    const text = claudeBridgeRequestTurnText({ agentType: "general-purpose" });
    assert.equal(text, "The general-purpose subagent is asking for permission");
    assert.equal(
      claudeBridgeRequestTurnText({ agentType: "subagent" }),
      "A subagent is asking for permission",
    );
    assert.equal(
      claudeBridgeRequestTurnText({ question: true }),
      "Claude Code is asking a question",
    );
    const opened = frames.onTurnStart(
      { seq: 0, at: 1, type: "turn.start", turnId: "request-1", text, transcriptOffset: 0 },
      { request: "subagent" },
    );
    assert.deepEqual(
      opened.map((frame) => frame.type),
      ["user", "system"],
    );
    assert.equal(claudeBridgeWakeDetail(opened[0]!), text);
    assert.equal(claudeBridgeWakeRequest(opened[0]!), "subagent");
    assert.isNull(claudeBridgeWakeRequest(opened[1]!));
    const typed = translator().onTurnStart({
      seq: 1,
      at: 1,
      type: "turn.start",
      turnId: "turn-3",
      text: "fix the tests",
      transcriptOffset: 0,
    });
    assert.isNull(claudeBridgeWakeRequest(typed[0]!));
  });

  it("passes on where the session said a turn's prompt came from, and from which agent", () => {
    const event = parseClaudeBridgeEvent({
      seq: 3,
      at: 5,
      type: "turn.start",
      turnId: "turn-5",
      text: '<agent-message from="a1">\nhello\n</agent-message>',
      transcriptOffset: 0,
      origin: "peer",
      sender: "general-purpose",
    });
    assert.isTrue(event?.type === "turn.start");
    if (event?.type !== "turn.start") return;
    const opened = translator().onTurnStart(event);
    assert.equal(claudeBridgeWakeOrigin(opened[0]!), "peer");
    assert.equal(claudeBridgeWakeSender(opened[0]!), "general-purpose");
    // A mod that does not say leaves both out.
    const silent = translator().onTurnStart({
      seq: 1,
      at: 1,
      type: "turn.start",
      turnId: "turn-6",
      text: "fix the tests",
      transcriptOffset: 0,
    });
    assert.isNull(claudeBridgeWakeOrigin(silent[0]!));
    assert.isNull(claudeBridgeWakeSender(silent[0]!));
  });

  it("takes the session's word for who submitted a prompt, and reads the text only without it", () => {
    const handBack = '<agent-message from="a1">\n[Subagent hand-back] ...\n</agent-message>';
    // The session's own word decides.
    assert.isTrue(isClaudePromptOfThePerson("fix the tests", "composer"));
    assert.isTrue(isClaudePromptOfThePerson("<system-reminder> what is this tag?", "composer"));
    assert.isTrue(isClaudePromptOfThePerson("from my phone", "bridge"));
    for (const origin of [
      "task-notification",
      "scheduled-trigger",
      "peer",
      "peer-send-message",
      "projects-relay",
      "channel",
      "coordinator",
      "observer",
      "observer-activity",
    ]) {
      assert.isFalse(isClaudePromptOfThePerson("looks typed", origin), origin);
    }
    // A mod that does not say, and origins that say too little: by how the prompt opens.
    for (const origin of [null, "plugin", "unclassified"]) {
      assert.isTrue(isClaudePromptOfThePerson("fix the tests", origin));
      assert.isFalse(isClaudePromptOfThePerson(handBack, origin));
      assert.isFalse(
        isClaudePromptOfThePerson("<task-notification>\n<task-id>b1</task-id>", origin),
      );
      assert.isFalse(isClaudePromptOfThePerson("Stop hook feedback: keep going", origin));
    }
  });

  it("shows a slash command typed in the terminal that starts a turn as the line, not its markup", () => {
    const row =
      "<command-message>d7-echo</command-message>\n<command-name>/d7-echo</command-name>\n<command-args>kiwi 91dbf</command-args>";
    assert.strictEqual(claudeTurnPromptLabel(row), "/d7-echo kiwi 91dbf");
    assert.strictEqual(
      claudeTurnPromptLabel(
        "<command-name>/review</command-name>\n<command-message>review</command-message>\n<command-args></command-args>",
      ),
      "/review",
    );
    // A prompt that only mentions such markup is the person's own words.
    const pasted = "why does the record say <command-name>/model</command-name> here?";
    assert.strictEqual(claudeTurnPromptLabel(pasted), pasted);
    const started = translator().onTurnStart({
      seq: 1,
      at: 1,
      type: "turn.start",
      turnId: "turn-4",
      text: row,
      transcriptOffset: 0,
    });
    assert.equal(claudeBridgeWakeDetail(started[0]!), "/d7-echo kiwi 91dbf");
  });

  it("ignores typed user rows, meta rows and sidechain rows", () => {
    const frames = translator();
    assert.lengthOf(
      frames.onTranscriptRow({
        type: "user",
        uuid: "u1",
        isSidechain: false,
        message: { role: "user", content: "typed in the terminal" },
      }),
      0,
    );
    assert.lengthOf(
      frames.onTranscriptRow({
        ...assistantRow("a-side", [{ type: "text", text: "subagent" }]),
        isSidechain: true,
      }),
      0,
    );
    assert.lengthOf(
      frames.onTranscriptRow({
        type: "user",
        uuid: "u2",
        isMeta: true,
        message: {
          role: "user",
          content: [{ type: "tool_result", tool_use_id: "x", content: "" }],
        },
      }),
      0,
    );
  });

  it("adopts a running turn without emitting frames and closes it on completion", () => {
    const frames = translator();
    frames.adoptTurn({ turnId: "turn-3", startedAt: 1, transcriptOffset: 100 });
    assert.isTrue(frames.hasOpenTurn());
    const ended = frames.onEnded("process gone");
    assert.equal(ended[0]?.type, "result");
    assert.isFalse(frames.hasOpenTurn());
    assert.lengthOf(frames.onEnded("again"), 0);
  });

  it("closes the open turn with nothing more said when its conversation carried on elsewhere", () => {
    const frames = translator();
    frames.adoptTurn({ turnId: "turn-4", startedAt: 1_000, transcriptOffset: 100 });
    const closed = frames.onContinuedElsewhere(4_000)[0];
    assert.equal(closed?.type, "result");
    if (closed?.type !== "result" || closed.subtype !== "success") return;
    assert.equal(closed.result, "");
    assert.isFalse(closed.is_error);
    assert.equal(closed.duration_ms, 3_000);
    assert.notProperty(closed, "terminal_reason");
    assert.isFalse(frames.hasOpenTurn());
    assert.lengthOf(frames.onContinuedElsewhere(5_000), 0);
  });

  it("leaves out rows older than the open turn: another session's conversation, copied in after it began", () => {
    const began = Date.parse("2026-10-11T10:29:19.794Z");
    const said = (uuid: string, text: string, timestamp: string) => ({
      ...assistantRow(uuid, [{ type: "text", text }]),
      timestamp,
    });
    const copied = said(
      "00000000-0000-4000-8000-0000000000c1",
      "before",
      "2026-10-11T10:28:24.000Z",
    );
    const own = said("00000000-0000-4000-8000-0000000000c2", "now", "2026-10-11T10:31:41.000Z");
    for (const open of ["adopted", "reported"] as const) {
      const frames = translator();
      // Between turns nothing is left out: there is no turn to be older than.
      assert.isFalse(frames.isFromBeforeTurn(copied));
      if (open === "adopted") {
        frames.adoptTurn({ turnId: "turn-5", startedAt: began, transcriptOffset: 220 });
      } else {
        frames.onTurnStart({
          seq: 1,
          at: began,
          type: "turn.start",
          turnId: "turn-5",
          text: "carry on",
          transcriptOffset: 220,
        });
      }
      assert.isTrue(frames.isFromBeforeTurn(copied));
      assert.lengthOf(frames.onTranscriptRow(copied), 0);
      assert.isFalse(frames.isFromBeforeTurn(own));
      assert.isAbove(frames.onTranscriptRow(own).length, 0);
      // The prompt's own row is stamped a moment before the turn is reported.
      assert.isFalse(
        frames.isFromBeforeTurn({ type: "user", timestamp: "2026-10-11T10:29:18.294Z" }),
      );
      // A row that carries no time (the session's settings) is never left out.
      assert.isFalse(frames.isFromBeforeTurn({ type: "permission-mode", permissionMode: "auto" }));
    }
  });

  it("passes on what the session says to the person in a row of its own, in its words", () => {
    const frames = translator();
    frames.adoptTurn({ turnId: "turn-6", startedAt: 0, transcriptOffset: 0 });
    const said = (fields: Record<string, unknown>) => ({
      type: "system",
      uuid: "00000000-0000-4000-8000-0000000000d1",
      isSidechain: false,
      isMeta: false,
      ...fields,
    });
    const carriedOn = said({
      subtype: "informational",
      level: "notice",
      content: "Continuing an interrupted response. Text before the interruption:\n\n1\n2\n3",
    });
    const [frame] = frames.onTranscriptRow(carriedOn);
    assert.isTrue(isClaudeBridgeNoticeFrame(frame));
    // The row's opening paragraph is the notice, under the row's own id.
    assert.equal(
      Reflect.get(frame ?? {}, "content"),
      "Continuing an interrupted response. Text before the interruption:",
    );
    assert.equal(Reflect.get(frame ?? {}, "uuid"), "00000000-0000-4000-8000-0000000000d1");
    assert.equal(
      claudeSessionNoticeText(
        said({
          subtype: "informational",
          level: "notice",
          content: "Usage limit reached · continuing automatically at 2:30am",
        }),
      ),
      "Usage limit reached · continuing automatically at 2:30am",
    );
    assert.equal(
      claudeSessionNoticeText(
        said({ subtype: "scheduled_task_fire", content: "Claude resuming /loop wakeup" }),
      ),
      "Claude resuming /loop wakeup",
    );
    // Rows shown another way, a subagent's, bookkeeping, and rows without words.
    for (const other of [
      said({
        subtype: "local_command",
        content: "<local-command-stdout>ok</local-command-stdout>",
      }),
      said({ subtype: "compact_boundary", content: "Conversation compacted" }),
      said({ subtype: "informational", content: "from a subagent", isSidechain: true }),
      said({ subtype: "informational", content: "bookkeeping", isMeta: true }),
      said({ subtype: "turn_duration", durationMs: 5 }),
      said({ subtype: "model_refusal_no_fallback", content: "" }),
      {
        type: "assistant",
        uuid: "00000000-0000-4000-8000-0000000000d2",
        content: "not a system row",
      },
    ]) {
      assert.isNull(claudeSessionNoticeText(other));
    }
  });

  it("passes on a hook's result where Claude Code's terminal shows one, in its words", () => {
    const frames = translator();
    frames.adoptTurn({ turnId: "turn-7", startedAt: 0, transcriptOffset: 0 });
    const attached = (
      attachment: Record<string, unknown>,
      fields: Record<string, unknown> = {},
    ) => ({
      type: "attachment",
      uuid: "00000000-0000-4000-8000-0000000000e1",
      isSidechain: false,
      attachment: { toolUseID: "tu-hook", ...attachment },
      ...fields,
    });
    const failed = attached({
      type: "hook_non_blocking_error",
      hookName: "UserPromptSubmit",
      hookEvent: "UserPromptSubmit",
      stderr:
        "\n  Failed with non-blocking status code: /bin/zsh: can't open input file: hl\nsecond line\n\nExpected schema: {}",
      stdout: "",
      exitCode: 127,
    });
    const [frame] = frames.onTranscriptRow(failed);
    assert.isTrue(isClaudeBridgeNoticeFrame(frame));
    assert.equal(Reflect.get(frame ?? {}, "uuid"), "00000000-0000-4000-8000-0000000000e1");
    // The hook's name, then its output's first line.
    assert.equal(
      Reflect.get(frame ?? {}, "content"),
      "UserPromptSubmit hook error\nFailed with non-blocking status code: /bin/zsh: can't open input file: hl",
    );
    assert.equal(
      claudeHookNoticeText(
        attached({
          type: "hook_non_blocking_error",
          hookName: "PreToolUse:Bash",
          hookEvent: "PreToolUse",
          stderr: "  ",
          stdout: `${"x".repeat(210)}\nmore`,
        }),
      ),
      `PreToolUse:Bash hook error\n${"x".repeat(200)}…`,
    );
    assert.equal(
      claudeHookNoticeText(
        attached({
          type: "hook_cancelled",
          hookName: "UserPromptSubmit",
          hookEvent: "UserPromptSubmit",
          command: "hl hook",
          timedOut: true,
          timeoutMs: 30000,
        }),
      ),
      `UserPromptSubmit hook [hl hook] timed out after 30s — output discarded. Raise the hook's "timeout" to allow more time.`,
    );
    assert.equal(
      claudeHookNoticeText(
        attached({
          type: "hook_system_message",
          hookName: "PostToolUse:Bash",
          hookEvent: "PostToolUse",
          content: "Tip: Run /ultrareview before you push.",
        }),
      ),
      "PostToolUse:Bash says: Tip: Run /ultrareview before you push.",
    );
    const stopped = (fields: Record<string, unknown>) => ({
      type: "system",
      subtype: "stop_hook_summary",
      uuid: "00000000-0000-4000-8000-0000000000e2",
      isSidechain: false,
      hookCount: 2,
      hookInfos: [{ command: "a" }, { command: "b" }],
      hookErrors: [],
      hookAdditionalContext: [],
      preventedContinuation: false,
      stopReason: "",
      hasOutput: true,
      level: "suggestion",
      ...fields,
    });
    assert.equal(
      claudeHookNoticeText(
        stopped({
          hookErrors: ["exit 2: lint failed"],
          hookAdditionalContext: ["tests are red"],
          preventedContinuation: true,
          stopReason: "Goal not met",
        }),
      ),
      "Ran 2 stop hooks\nGoal not met\nStop hook error: exit 2: lint failed\nStop hook feedback: tests are red",
    );
    // What the terminal leaves out: stop hooks that ran clean, a Stop hook's
    // own error, a cancel that was not a timeout, a subagent's hook.
    for (const hidden of [
      stopped({}),
      stopped({ level: "info" }),
      attached({
        type: "hook_non_blocking_error",
        hookName: "Stop",
        hookEvent: "Stop",
        stderr: "boom",
      }),
      attached({
        type: "hook_cancelled",
        hookName: "UserPromptSubmit",
        hookEvent: "UserPromptSubmit",
        timedOut: false,
        timeoutMs: 30000,
      }),
      attached({
        type: "hook_cancelled",
        hookName: "PreToolUse:Bash",
        hookEvent: "PreToolUse",
        timedOut: true,
      }),
      attached(
        { type: "hook_system_message", hookName: "PostToolUse:Bash", content: "from a subagent" },
        { isSidechain: true },
      ),
    ]) {
      assert.isNull(claudeHookNoticeText(hidden));
      assert.lengthOf(frames.onTranscriptRow(hidden), 0);
    }
  });

  it("puts the summary a compacted session goes on from on its compaction's frame", () => {
    const frames = translator();
    frames.adoptTurn({ turnId: "turn-8", startedAt: 0, transcriptOffset: 0 });
    const boundaryUuid = "00000000-0000-4000-8000-0000000000f1";
    const [boundary] = frames.onTranscriptRow({
      type: "system",
      subtype: "compact_boundary",
      uuid: boundaryUuid,
      parentUuid: null,
      isSidechain: false,
      content: "Conversation compacted",
      level: "info",
      compactMetadata: { trigger: "auto", preTokens: 235317, postTokens: 13677 },
    });
    assert.isUndefined(claudeBridgeCompactSummary(boundary));
    // The instructions a compaction reloads can come between it and its summary.
    for (const type of ["instructions", "session_context", "date"]) {
      frames.onTranscriptRow({
        type: "attachment",
        uuid: `attachment-${type}`,
        isSidechain: false,
        attachment: { type },
      });
    }
    const summary =
      "This session is being continued from a previous conversation that ran out of context.\n\nSummary:\n1. Primary Request and Intent: read the big file.";
    const summaryRow = {
      type: "user",
      uuid: "00000000-0000-4000-8000-0000000000fa",
      parentUuid: "00000000-0000-4000-8000-0000000000f4",
      isSidechain: false,
      isCompactSummary: true,
      isVisibleInTranscriptOnly: true,
      message: { role: "user", content: summary },
    };
    const [again] = frames.onTranscriptRow(summaryRow);
    // The compaction's own frame, under its own id, now with the summary.
    assert.equal(Reflect.get(again ?? {}, "subtype"), "compact_boundary");
    assert.equal(Reflect.get(again ?? {}, "uuid"), boundaryUuid);
    assert.deepEqual(
      Reflect.get(again ?? {}, "compact_metadata"),
      Reflect.get(boundary ?? {}, "compact_metadata"),
    );
    assert.equal(claudeBridgeCompactSummary(again), summary);
    // A summary row with no compaction before it goes nowhere.
    assert.lengthOf(frames.onTranscriptRow({ ...summaryRow, uuid: "summary-again" }), 0);
  });

  it("maps an error completion to an error result", () => {
    const frames = translator();
    const result = frames.onTurnComplete({
      seq: 1,
      at: 1,
      type: "turn.complete",
      turnId: "turn-4",
      reason: "error",
      answer: "API error",
      isAborted: false,
      durationMs: 5,
      transcriptOffset: 0,
      promptUuid: PROMPT_UUID,
    });
    const frame = result[0];
    if (frame?.type !== "result") throw new Error("expected result");
    assert.equal(frame.subtype, "error_during_execution");
    assert.isTrue(frame.is_error);
    assert.equal(frame.user_message_uuid, PROMPT_UUID);
  });
});

describe("ClaudeBridgeProtocol T3 tools called through the mod", () => {
  it("shows a call to a tool the mod declares under T3's own name for it", () => {
    const frames = translator().onTranscriptRow({
      type: "assistant",
      uuid: "33333333-3333-4333-8333-333333333333",
      message: {
        role: "assistant",
        content: [
          {
            type: "tool_use",
            id: "toolu_1",
            name: "mcp__t3bridge__html_render",
            input: { title: "T" },
          },
          { type: "tool_use", id: "toolu_2", name: "Bash", input: { command: "ls" } },
        ],
      },
    });
    const names = frames.flatMap((frame) =>
      frame.type === "assistant"
        ? frame.message.content.flatMap((block) => (block.type === "tool_use" ? [block.name] : []))
        : [],
    );
    assert.deepStrictEqual(names, ["mcp__t3-code__html_render", "Bash"]);
  });
});

describe("ClaudeBridgeProtocol thread settings", () => {
  it("shows a session's model and permission mode the way its thread names them", () => {
    assert.deepEqual(
      threadSettingsForClaudeSession({ model: "claude-haiku-5-5", permissionMode: "default" }),
      { model: "claude-haiku-5-5", runtimeMode: "approval-required", interactionMode: "default" },
    );
    assert.deepEqual(
      threadSettingsForClaudeSession({ model: "claude-opus-5-5[1m]", permissionMode: "plan" }),
      { model: "claude-opus-5-5", contextWindow: "1m", interactionMode: "plan" },
    );
  });

  it("leaves out what the session does not say", () => {
    assert.deepEqual(threadSettingsForClaudeSession({ model: null, permissionMode: null }), {});
    assert.deepEqual(
      threadSettingsForClaudeSession({ model: null, permissionMode: "someFutureMode" }),
      {},
    );
  });
});

describe("ClaudeBridgeProtocol record helpers", () => {
  const rowAt = (iso: string) => `${JSON.stringify({ type: "user", uuid: iso, timestamp: iso })}\n`;
  const first = rowAt("2026-07-01T10:00:00.000Z");
  const second = rowAt("2026-07-01T10:05:00.000Z");
  const third = rowAt("2026-07-01T10:06:00.000Z");
  const bytes = new TextEncoder().encode(first + second + third);

  it("finds where a turn starts in a record from the time it began", () => {
    assert.strictEqual(
      claudeTranscriptOffsetAt(bytes, 0, Date.parse("2026-07-01T10:04:59.000Z")),
      first.length,
    );
    assert.strictEqual(
      claudeTranscriptOffsetAt(bytes, 0, Date.parse("2026-07-01T09:00:00.000Z")),
      0,
    );
    assert.strictEqual(
      claudeTranscriptOffsetAt(bytes, 0, Date.parse("2026-07-01T11:00:00.000Z")),
      null,
    );
  });

  it("starts at the first whole row of a tail that begins mid-row", () => {
    const cut = 10;
    assert.strictEqual(
      claudeTranscriptOffsetAt(
        bytes.subarray(cut),
        1_000 + cut,
        Date.parse("2026-07-01T09:00:00.000Z"),
      ),
      1_000 + first.length,
    );
  });

  it("reads a ! command and what it printed the way the terminal shows them", () => {
    const row = (content: string) => ({ type: "user", message: { role: "user", content } });
    assert.strictEqual(
      claudeTranscriptCommandText(row("<bash-input>git status --short</bash-input>")),
      "! git status --short",
    );
    assert.strictEqual(
      claudeTranscriptCommandOutput(
        row("<bash-stdout> M a.ts\n</bash-stdout><bash-stderr>warning: x</bash-stderr>"),
      ),
      " M a.ts\n\nwarning: x",
    );
    assert.strictEqual(
      claudeTranscriptCommandOutput(row("<bash-stdout></bash-stdout><bash-stderr></bash-stderr>")),
      "",
    );
    assert.isNull(claudeTranscriptCommandOutput(row("plain text")));
  });

  it("drops the terminal's colour and dimming codes from what a command printed", () => {
    const escape = String.fromCharCode(27);
    assert.strictEqual(
      claudeTranscriptCommandOutput({
        type: "user",
        message: {
          role: "user",
          content: `<local-command-stdout>${escape}[2mCompacted (ctrl+o to see full summary)${escape}[22m</local-command-stdout>`,
        },
      }),
      "Compacted (ctrl+o to see full summary)",
    );
  });

  // Claude Code writes some commands without markup: the note for the model,
  // then the command as typed, then what it printed, each the child of the one
  // before. Only that chain is a command; a prompt that starts with "/" is not.
  it("reads a command written as plain rows, and what it printed", () => {
    const row = (uuid: string, parentUuid: string | null, content: string, isMeta = false) => ({
      type: "user",
      uuid,
      parentUuid,
      isSidechain: false,
      ...(isMeta ? { isMeta: true } : {}),
      message: { role: "user", content },
    });
    const caveat = "<local-command-caveat>Caveat: the messages below.</local-command-caveat>";
    const refusal = "This skill can only be invoked by Claude, not directly by users.";
    const read = makeClaudePlainCommandReader();
    assert.isNull(read(row("a", null, "/keybindings-help")));
    assert.isNull(read(row("n1", "a", caveat, true)));
    assert.deepStrictEqual(read(row("c1", "n1", "/keybindings-help")), {
      command: "/keybindings-help",
    });
    assert.deepStrictEqual(read(row("o1", "c1", refusal)), { output: refusal });
    // What follows the output is the person's again.
    assert.isNull(read(row("p1", "o1", "and now something else")));
    // A command that printed nothing as a plain row: the next prompt is not its output.
    assert.isNull(read(row("n2", "p1", caveat, true)));
    assert.deepStrictEqual(read(row("c2", "n2", "/code-review")), { command: "/code-review" });
    assert.isNull(read(row("p2", "x", "a prompt of the person's")));
    // A command with markup after the note is read by its markup, not here.
    assert.isNull(read(row("n3", "p2", caveat, true)));
    assert.isNull(read(row("c3", "n3", "<command-name>/model</command-name>")));
  });

  it("reads what a skill sent to the background printed, without the row's launch record", () => {
    assert.strictEqual(
      claudeTranscriptCommandOutput({
        type: "system",
        subtype: "local_command",
        content:
          '<local-command-stdout>Running in the background as @code-review</local-command-stdout>\n<forked-skill-launch>{"agentId":"a1","skillName":"code-review","description":"/code-review"}</forked-skill-launch>',
      }),
      "Running in the background as @code-review",
    );
  });

  it("keeps the note that a request is also being asked in the terminal", () => {
    const event = parseClaudeBridgeEvent({
      seq: 4,
      at: 1,
      type: "approval.request",
      toolUseId: "toolu_1",
      tool: "ExitPlanMode",
      input: { plan: "1. do it" },
      alsoInTerminal: true,
      suggestions: [{ type: "setMode", mode: "acceptEdits", destination: "session" }],
    });
    assert.isTrue(event?.type === "approval.request" && event.alsoInTerminal === true);
    assert.deepStrictEqual(event?.type === "approval.request" ? event.suggestions : null, [
      { type: "setMode", mode: "acceptEdits", destination: "session" },
    ]);
    assert.isTrue(claudeBridgeAlsoInTerminal({ signal: null, alsoInTerminal: true }));
    assert.isFalse(claudeBridgeAlsoInTerminal({ signal: null }));
  });

  it("keeps the permission mode a request offers with an approval", () => {
    const event = parseClaudeBridgeEvent({
      seq: 4,
      at: 1,
      type: "approval.request",
      toolUseId: "toolu_1",
      tool: "Bash",
      input: { command: "make" },
      alsoInTerminal: true,
      approveWithMode: "auto",
    });
    assert.strictEqual(event?.type === "approval.request" ? event.approveWithMode : null, "auto");
    assert.strictEqual(claudeBridgeApproveWithMode({ approveWithMode: "auto" }), "auto");
    assert.isUndefined(claudeBridgeApproveWithMode({ signal: null }));
    assert.strictEqual(claudeApprovalMode({ behavior: "allow", mode: "auto" }), "auto");
    assert.isUndefined(claudeApprovalMode({ behavior: "allow" }));
  });

  it("says what approving for the session grants, in the session's own rule words", () => {
    assert.strictEqual(
      claudeSessionGrant([
        {
          type: "addRules",
          rules: [
            { toolName: "Bash", ruleContent: "npm run:*" },
            { toolName: "mcp__elicit__ask_color" },
          ],
          behavior: "allow",
          destination: "localSettings",
        },
      ]),
      "Bash(npm run:*), mcp__elicit__ask_color",
    );
    assert.strictEqual(
      claudeSessionGrant([
        { type: "addDirectories", directories: ["/tmp/work"], destination: "session" },
      ]),
      "access to /tmp/work",
    );
    assert.strictEqual(
      claudeSessionGrant([{ type: "setMode", mode: "acceptEdits", destination: "session" }]),
      "accept edits mode",
    );
    // Nothing suggested, or nothing that allows: the grant is no wider than the request.
    assert.isUndefined(claudeSessionGrant(undefined));
    assert.isUndefined(claudeSessionGrant([]));
    assert.isUndefined(
      claudeSessionGrant([
        {
          type: "addRules",
          rules: [{ toolName: "Bash" }],
          behavior: "deny",
          destination: "session",
        },
      ]),
    );
  });

  it("keeps which kind of subagent a request comes from", () => {
    const event = parseClaudeBridgeEvent({
      seq: 5,
      at: 1,
      type: "approval.request",
      toolUseId: "toolu_2",
      tool: "Bash",
      input: { command: "echo hi" },
      alsoInTerminal: true,
      agentType: "general-purpose",
    });
    assert.strictEqual(
      event?.type === "approval.request" ? event.agentType : null,
      "general-purpose",
    );
    assert.strictEqual(
      claudeBridgeAskingAgent({ signal: null, askedByAgent: "general-purpose" }),
      "general-purpose",
    );
    assert.isNull(claudeBridgeAskingAgent({ signal: null }));
  });

  it("reads from a prompt the files T3 saved for it", () => {
    assert.deepStrictEqual(
      claudeBridgeAttachmentPaths(
        'What is this?\n\n[Attached image "shot.png" is saved at: /Users/me/.t3/userdata/attachments/t1/shot.png]\n\n[T3: the user attached an image to this message.]',
      ),
      ["/Users/me/.t3/userdata/attachments/t1/shot.png"],
    );
    assert.deepStrictEqual(claudeBridgeAttachmentPaths("no files here"), []);
    assert.deepStrictEqual(
      claudeBridgeAttachmentPaths('[Attached file "x" is saved at: /a/../etc/passwd]'),
      [],
    );
  });

  it("compares bridge mod versions by number", () => {
    assert.isTrue(claudeBridgeModAtLeast("0.3.4", "0.3.4"));
    assert.isTrue(claudeBridgeModAtLeast("0.10.0", "0.3.4"));
    assert.isFalse(claudeBridgeModAtLeast("0.3.3", "0.3.4"));
    assert.isFalse(claudeBridgeModAtLeast("", "0.3.4"));
  });

  it("tells the person's mid-turn message from the harness's notices", () => {
    const queued = (origin: unknown) => ({
      type: "attachment",
      attachment: { type: "queued_command", prompt: "x", origin },
    });
    assert.isTrue(isClaudeTypedMidTurnRow(queued({ kind: "human" })));
    assert.isFalse(isClaudeTypedMidTurnRow(queued({ kind: "task-notification" })));
    assert.isFalse(isClaudeTypedMidTurnRow({ type: "user", message: { content: "x" } }));
  });
});

describe("a record that says its conversation continued under another session", () => {
  const moved = '{"type":"continued-in","sessionId":"old","continuedInSessionId":"new-session"}';
  const said = '{"type":"user","uuid":"u1","message":{"role":"user","content":"hello"}}';
  const upkeep = '{"type":"cost-state","sessionId":"old","totalCostUSD":1}';

  it("reads where the conversation went from the record's last rows", () => {
    assert.strictEqual(claudeContinuedInSession(`${said}\n${upkeep}\n${moved}\n`), "new-session");
    // Bookkeeping rows written after the mark change nothing.
    assert.strictEqual(claudeContinuedInSession(`${said}\n${moved}\n${upkeep}\n`), "new-session");
  });

  it("says nothing for an ordinary record, or one taken up again after the mark", () => {
    assert.isNull(claudeContinuedInSession(`${said}\n${upkeep}\n`));
    assert.isNull(claudeContinuedInSession(`${moved}\n${said}\n`));
    assert.isNull(claudeContinuedInSession(""));
    assert.isNull(claudeContinuedInSession('{"type":"continued-in"}\nnot json\n'));
  });

  it("words the refusal with the session the conversation went to", () => {
    assert.include(claudeSessionContinuedNotice("new-session"), "Claude Code session new-session");
  });
});

describe("ClaudeBridgeProtocol parsers", () => {
  it("reads the session's own effort and its status line's figures, and leaves out what an older mod does not say", () => {
    const session = parseClaudeBridgeSessionFile({
      protocol: 2,
      sid: SID,
      instanceId: "abc",
      heartbeatAt: 1000,
      effort: "high",
      sessionEffort: "low",
      usage: {
        context: { tokens: 52_000, window: 200_000, percent: 26 },
        rateLimits: [
          { kind: "five_hour", percentUsed: 12.5, resetsAt: "2026-10-14T15:00:00Z" },
          { kind: "seven_day", percentUsed: 40 },
          { kind: "", percentUsed: 3 },
          { kind: "spend_limit", percentUsed: "a lot" },
        ],
        cost: { usd: 1.25 },
      },
    });
    assert.equal(session?.effort, "high");
    assert.equal(session?.sessionEffort, "low");
    assert.deepEqual(session?.usage, {
      context: { tokens: 52_000, window: 200_000, percent: 26 },
      rateLimits: [
        { kind: "five_hour", percentUsed: 12.5, resetsAt: "2026-10-14T15:00:00Z" },
        { kind: "seven_day", percentUsed: 40 },
      ],
      cost: { usd: 1.25 },
    });
    // A fresh window has no fill yet; a model without effort levels has no session effort.
    const fresh = parseClaudeBridgeSessionFile({
      sid: SID,
      instanceId: "abc",
      heartbeatAt: 1000,
      sessionEffort: null,
      usage: { context: { window: 200_000 }, rateLimits: [] },
    });
    assert.isNull(fresh?.sessionEffort);
    assert.deepEqual(fresh?.usage, { context: { window: 200_000 }, rateLimits: [] });
    const older = parseClaudeBridgeSessionFile({
      sid: SID,
      instanceId: "abc",
      heartbeatAt: 1000,
      usage: null,
    });
    assert.isFalse(older !== null && "sessionEffort" in older);
    assert.isFalse(older !== null && "usage" in older);
  });

  it("parses a session file and a turn.complete event with partial usage", () => {
    const session = parseClaudeBridgeSessionFile({
      protocol: 1,
      sid: SID,
      instanceId: "abc",
      heartbeatAt: 1000,
      activeTurn: { turnId: "t", startedAt: 900, transcriptOffset: 12 },
      ended: null,
    });
    assert.equal(session?.activeTurn?.transcriptOffset, 12);
    assert.isNull(session?.ended);
    const event = parseClaudeBridgeEvent({
      seq: 3,
      at: 5,
      type: "turn.complete",
      turnId: "t",
      reason: "answer",
      answer: "ok",
      isAborted: false,
      durationMs: 4,
      usage: { model: "claude-opus-5-5", outputTokens: 7, inputTokens: "bad" },
      transcriptOffset: 40,
    });
    assert.equal(event?.type, "turn.complete");
    if (event?.type !== "turn.complete") return;
    assert.deepEqual(event.usage, { model: "claude-opus-5-5", outputTokens: 7 });
    assert.isNull(parseClaudeBridgeEvent({ seq: 1, at: 1, type: "nope" }));
  });

  it("reads that a prompt sent to steer a turn was handed over inside it", () => {
    const event = parseClaudeBridgeEvent({
      seq: 4,
      at: 9,
      type: "prompt.delivered",
      commandId: "command-7",
      turnId: "turn-3",
      uuid: "row-1",
    });
    assert.deepEqual(event, {
      seq: 4,
      at: 9,
      type: "prompt.delivered",
      commandId: "command-7",
      turnId: "turn-3",
    });
    assert.isNull(parseClaudeBridgeEvent({ seq: 4, at: 9, type: "prompt.delivered" }));
    // The mod names the prompt too: a server that started since has only that to go by.
    assert.deepEqual(
      parseClaudeBridgeEvent({
        seq: 5,
        at: 9,
        type: "prompt.delivered",
        commandId: "command-7",
        promptUuid: "prompt-7",
        turnId: "turn-3",
      }),
      {
        seq: 5,
        at: 9,
        type: "prompt.delivered",
        commandId: "command-7",
        turnId: "turn-3",
        promptUuid: "prompt-7",
      },
    );
  });

  it("finds the person's words in the row the mod wrote to steer a running turn", () => {
    const row = (text: string, origin: Record<string, unknown>) => ({
      type: "user",
      isMeta: true,
      origin,
      message: { role: "user", content: [{ type: "text", text }] },
    });
    const ours = { kind: "plugin", name: "t3bridge" };
    const wrapped = `${CLAUDE_MID_TURN_OPENING}also check the logs${CLAUDE_MID_TURN_CLOSING}`;
    assert.equal(claudeBridgeMidTurnWords(row(wrapped, ours)), "also check the logs");
    // Another plugin's row in the same words, and the mod's rows in other words, are not it.
    assert.isNull(claudeBridgeMidTurnWords(row(wrapped, { kind: "plugin", name: "other" })));
    assert.isNull(claudeBridgeMidTurnWords(row("<bash-input>ls</bash-input>", ours)));
    assert.isNull(claudeBridgeMidTurnWords({ type: "assistant", origin: ours }));
  });

  it("reads what the session raised for the person's attention, in its words", () => {
    const raised = parseClaudeBridgeEvent({
      seq: 4,
      at: 6,
      type: "notification",
      message: " Claude Code needs your attention ",
      title: "Claude Code",
      kind: "elicitation_dialog",
    });
    assert.equal(raised?.type, "notification");
    if (raised?.type !== "notification") return;
    assert.equal(raised.message, "Claude Code needs your attention");
    assert.equal(raised.kind, "elicitation_dialog");
    assert.isFalse(raised.dialog);
    // With a dialog open in the terminal and no card here, T3 says where the door is.
    assert.equal(claudeSessionNotificationText(raised), "Claude Code needs your attention");
    assert.equal(
      claudeSessionNotificationText({ ...raised, dialog: true }),
      `Claude Code needs your attention\n\n${CLAUDE_TERMINAL_DIALOG_MARK}`,
    );
    // The thread shows it from the same frame as a notice row of the record.
    const frame = claudeSessionNoticeFrame({
      uuid: "00000000-0000-4000-8000-0000000000e1",
      sessionId: "s",
      subtype: "notification",
      content: raised.message,
    });
    assert.isTrue(isClaudeBridgeNoticeFrame(frame));
    assert.isNull(parseClaudeBridgeEvent({ seq: 5, at: 7, type: "notification", message: "  " }));
  });

  it("reads the session's word that it began to compact, and that it stopped", () => {
    const started = parseClaudeBridgeEvent({
      seq: 6,
      at: 8,
      type: "compact.started",
      trigger: "auto",
    });
    assert.equal(started?.type, "compact.started");
    if (started?.type !== "compact.started") return;
    assert.equal(started.trigger, "auto");
    assert.equal(
      parseClaudeBridgeEvent({ seq: 7, at: 9, type: "compact.ended" })?.type,
      "compact.ended",
    );
    // The thread hears it in the SDK's own words.
    const uuid = "00000000-0000-4000-8000-0000000000e2";
    assert.equal(
      claudeCompactingFrame({ uuid, sessionId: "s", compacting: true }).status,
      "compacting",
    );
    assert.isNull(claudeCompactingFrame({ uuid, sessionId: "s", compacting: false }).status);
  });

  it("reads only complete JSONL lines", () => {
    const { rows, consumedChars } = readTranscriptRows('{"a":1}\n{"b":2}\n{"c":');
    assert.lengthOf(rows, 2);
    assert.equal(consumedChars, 16);
  });

  it("takes any first word after the slash as a command name, and a path as no command", () => {
    assert.deepEqual(parseClaudeSlashCommandText("/model sonnet"), {
      name: "model",
      args: "sonnet",
    });
    assert.deepEqual(parseClaudeSlashCommandText(" /__remote-workflow "), {
      name: "__remote-workflow",
      args: "",
    });
    assert.deepEqual(parseClaudeSlashCommandText("/codex:review  fix it\nnow"), {
      name: "codex:review",
      args: "fix it\nnow",
    });
    assert.isNull(parseClaudeSlashCommandText("/Users/jack/notes.txt"));
    assert.isNull(parseClaudeSlashCommandText("/tmp/x is the folder"));
    assert.isNull(parseClaudeSlashCommandText("/"));
    assert.isNull(parseClaudeSlashCommandText("/model\nsonnet"));
    assert.isNull(parseClaudeSlashCommandText("see /model"));
  });

  it("reads which commands the session's own menu leaves out, and the hint after a name", () => {
    assert.deepEqual(
      parseClaudeBridgeSlashCommands([
        { name: "heapdump", description: "Dump the JS heap", source: "builtin", hidden: true },
        { name: "model", description: "Set the model", source: "builtin", hint: " [model] " },
        { name: "clear", description: "Start over", source: "builtin", hidden: false, hint: "" },
        { description: "no name" },
      ]),
      [
        { name: "heapdump", description: "Dump the JS heap", source: "builtin", hidden: true },
        { name: "model", description: "Set the model", source: "builtin", hint: "[model]" },
        { name: "clear", description: "Start over", source: "builtin" },
      ],
    );
  });
});

describe("narration, what the model says to the person between tool calls", () => {
  const said = {
    type: "thinking",
    thinking: "Tests pass; building now.",
    signature: signatureOf("narration"),
  };
  const thought = {
    type: "thinking",
    thinking: "Which file first?",
    signature: signatureOf("thinking"),
  };

  it("is told from a thought by the label in the block's signature, as Claude Code tells it", () => {
    assert.isTrue(isClaudeNarrationBlock(said));
    assert.isFalse(isClaudeNarrationBlock(thought));
    assert.isFalse(isClaudeNarrationBlock({ ...said, signature: "not a signature" }));
    assert.isFalse(isClaudeNarrationBlock({ ...said, signature: undefined }));
    assert.isFalse(isClaudeNarrationBlock({ ...said, thinking: "  " }), "nothing to draw");
    assert.isFalse(isClaudeNarrationBlock({ type: "text", text: "Done." }));
  });

  it("reaches the thread as the message the terminal draws; a thought stays a thought", () => {
    const frames = translator();
    const content = (row: ReturnType<typeof assistantRow>) =>
      Reflect.get(Reflect.get(frames.onTranscriptRow(row)[0]!, "message") as object, "content");
    assert.deepEqual(content(assistantRow("n1", [said])), [
      { type: "text", text: "Tests pass; building now." },
    ]);
    assert.deepEqual(content(assistantRow("t1", [thought])), [thought]);
  });
});
