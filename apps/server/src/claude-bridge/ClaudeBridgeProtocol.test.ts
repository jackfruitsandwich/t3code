import { assert, describe, it } from "@effect/vitest";

import {
  claudeBridgeAlsoInTerminal,
  claudeBridgeAskingAgent,
  claudeBridgeAttachmentPaths,
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
  makeClaudeBridgeFrameTranslator,
  parseClaudeBridgeEvent,
  parseClaudeBridgeSessionFile,
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

describe("ClaudeBridgeProtocol parsers", () => {
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

  it("reads only complete JSONL lines", () => {
    const { rows, consumedChars } = readTranscriptRows('{"a":1}\n{"b":2}\n{"c":');
    assert.lengthOf(rows, 2);
    assert.equal(consumedChars, 16);
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
