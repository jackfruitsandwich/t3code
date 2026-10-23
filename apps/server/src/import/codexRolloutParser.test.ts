import { assert, describe, it } from "@effect/vitest";

import { parseCodexRolloutJsonl, parseCodexSessionIndex } from "./codexRolloutParser.ts";
import { isEligibleCliSession } from "./sessionImportTypes.ts";

const SESSION_ID = "22222222-2222-4222-8222-222222222222";

function line(entry: Record<string, unknown>): string {
  return `${JSON.stringify(entry)}\n`;
}

const sessionMeta = (payload: Record<string, unknown> = {}) =>
  line({
    timestamp: "2026-07-02T08:00:00.000Z",
    type: "session_meta",
    payload: {
      id: SESSION_ID,
      cwd: "/repo/codex-project",
      timestamp: "2026-07-02T08:00:00.000Z",
      ...payload,
    },
  });

const userMessage = (text: string, timestamp = "2026-07-02T08:00:01.000Z") =>
  line({
    timestamp,
    type: "event_msg",
    payload: { type: "user_message", message: text },
  });

const agentMessage = (text: string, timestamp = "2026-07-02T08:00:09.000Z") =>
  line({
    timestamp,
    type: "event_msg",
    payload: { type: "agent_message", message: text },
  });

const completedMessage = (type: "UserMessage" | "AgentMessage", text: string, timestamp: string) =>
  line({
    timestamp,
    type: "event_msg",
    payload: {
      type: "item_completed",
      item: { type, content: [{ type: "text", text }] },
    },
  });

describe("parseCodexRolloutJsonl", () => {
  it("reads session meta and user/agent messages with line timestamps", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        line({ timestamp: "t", type: "turn_context", payload: {} }) +
        userMessage("Refactor the parser") +
        line({ timestamp: "t", type: "response_item", payload: { type: "reasoning" } }) +
        agentMessage("Done, parser refactored.") +
        line({ timestamp: "t", type: "token_count", payload: { total: 5 } }),
    );

    assert.equal(parsed.sessionId, SESSION_ID);
    assert.equal(parsed.cwd, "/repo/codex-project");
    assert.equal(parsed.startedAt, "2026-07-02T08:00:00.000Z");
    assert.deepEqual(
      parsed.messages.map((message) => ({
        role: message.role,
        text: message.text,
        timestamp: message.timestamp,
        lineIndex: message.lineIndex,
      })),
      [
        {
          role: "user",
          text: "Refactor the parser",
          timestamp: "2026-07-02T08:00:01.000Z",
          lineIndex: 2,
        },
        {
          role: "assistant",
          text: "Done, parser refactored.",
          timestamp: "2026-07-02T08:00:09.000Z",
          lineIndex: 4,
        },
      ],
    );
    assert.isTrue(isEligibleCliSession(parsed));
  });

  it("accepts session_id as the meta id field and truncates the title", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta({ id: undefined, session_id: SESSION_ID }) +
        userMessage("y".repeat(90)) +
        agentMessage("ok"),
    );

    assert.equal(parsed.sessionId, SESSION_ID);
    assert.equal(parsed.title, "y".repeat(60));
  });

  it("reads Codex Desktop completed message items", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta({ source: "vscode", originator: "Codex Desktop" }) +
        completedMessage("UserMessage", "Import this session", "2026-07-02T08:00:01.000Z") +
        completedMessage("AgentMessage", "Imported.", "2026-07-02T08:00:09.000Z"),
    );

    assert.deepEqual(
      parsed.messages.map(({ role, text }) => ({ role, text })),
      [
        { role: "user", text: "Import this session" },
        { role: "assistant", text: "Imported." },
      ],
    );
    assert.equal(parsed.originator, "Codex Desktop");
    assert.isTrue(isEligibleCliSession(parsed));
  });

  it("never throws on malformed lines and does not consume a partial tail", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        `garbage line\n` +
        userMessage("hello") +
        agentMessage("hi") +
        `{"timestamp":"2026-07-02T08:00:10.000Z","type":"event_msg","payl`,
    );

    assert.equal(parsed.malformedLineCount, 1);
    assert.equal(parsed.lineCount, 4);
    assert.lengthOf(parsed.messages, 2);
  });

  it("is ineligible without both a user and an agent message", () => {
    assert.isFalse(isEligibleCliSession(parseCodexRolloutJsonl(sessionMeta() + userMessage("x"))));
    assert.isFalse(isEligibleCliSession(parseCodexRolloutJsonl(sessionMeta() + agentMessage("x"))));
  });

  it("reads a turn's commands, tool calls, patches and searches as the live adapter's items", () => {
    const at = (second: number) => `2026-07-02T08:00:${String(second).padStart(2, "0")}.000Z`;
    const completed = (second: number, item: Record<string, unknown>) =>
      line({
        timestamp: at(second),
        type: "event_msg",
        payload: { type: "item_completed", turn_id: "turn-1", item },
      });
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        line({
          timestamp: at(1),
          type: "event_msg",
          payload: { type: "task_started", turn_id: "turn-1" },
        }) +
        line({
          timestamp: at(1),
          type: "turn_context",
          payload: { turn_id: "turn-1", model: "gpt-6-astra" },
        }) +
        completed(2, { type: "UserMessage", id: "u1", content: [{ type: "text", text: "go" }] }) +
        completed(3, {
          type: "CommandExecution",
          id: "call_1",
          command: ["/bin/zsh", "-lc", "ls -la"],
          aggregated_output: "total 0\n",
          exit_code: 0,
          status: "completed",
        }) +
        completed(4, {
          type: "CommandExecution",
          id: "call_2",
          command: ["git", "status"],
          aggregated_output: "",
          exit_code: 128,
          status: "failed",
        }) +
        completed(5, {
          type: "FileChange",
          id: "call_3",
          status: "completed",
          changes: {
            "/repo/b.ts": { type: "update", unified_diff: "@@ -1 +1 @@\n-a\n+b\n" },
            "/repo/a.ts": { type: "add", content: "new file\n" },
          },
        }) +
        completed(6, {
          type: "McpToolCall",
          id: "call_4",
          server: "linear",
          tool: "list_issues",
          arguments: { team: "T3" },
          status: "completed",
          result: { content: [{ type: "text", text: "3 issues" }], isError: false },
        }) +
        completed(7, {
          type: "WebSearch",
          id: "ws_1",
          query: "effect schema",
          action: { type: "search", query: "effect schema", queries: ["effect schema union"] },
        }) +
        completed(8, { type: "Extension", id: "ext_1", kind: "web.search", query: "vitest" }) +
        completed(9, { type: "Extension", id: "ext_2", kind: "clock.sleep" }) +
        completed(10, { type: "Reasoning", id: "rs_1" }) +
        completed(11, {
          type: "AgentMessage",
          id: "msg_1",
          content: [{ type: "text", text: "ok" }],
        }),
    );

    assert.equal(parsed.model, "gpt-6-astra");
    assert.deepEqual(
      parsed.messages.map(({ sourceId, nativeId, nativeTurnId }) => ({
        sourceId,
        nativeId,
        nativeTurnId,
      })),
      [
        { sourceId: "line-3", nativeId: "u1", nativeTurnId: "turn-1" },
        { sourceId: "line-12", nativeId: "msg_1", nativeTurnId: "turn-1" },
      ],
    );
    assert.deepEqual(
      (parsed.items ?? []).map(({ sourceId, nativeId, nativeTurnId, item }) => ({
        sourceId,
        nativeId,
        nativeTurnId,
        item,
      })),
      [
        {
          sourceId: "line-4",
          nativeId: "call_1",
          nativeTurnId: "turn-1",
          item: {
            type: "command_execution",
            status: "completed",
            input: "/bin/zsh -lc 'ls -la'",
            output: "total 0\n",
            exitCode: 0,
          },
        },
        {
          sourceId: "line-5",
          nativeId: "call_2",
          nativeTurnId: "turn-1",
          item: { type: "command_execution", status: "failed", input: "git status", exitCode: 128 },
        },
        // Every file of a patch shows, each on its own row, by path.
        {
          sourceId: "line-6",
          nativeId: "call_3",
          nativeTurnId: "turn-1",
          item: {
            type: "file_change",
            status: "completed",
            fileName: "/repo/a.ts",
            diffStr: "new file\n",
          },
        },
        {
          sourceId: "line-6-1",
          nativeId: "call_3",
          nativeTurnId: "turn-1",
          item: {
            type: "file_change",
            status: "completed",
            fileName: "/repo/b.ts",
            diffStr: "@@ -1 +1 @@\n-a\n+b\n",
          },
        },
        {
          sourceId: "line-7",
          nativeId: "call_4",
          nativeTurnId: "turn-1",
          item: {
            type: "tool_call",
            call: {
              type: "mcpToolCall",
              id: "call_4",
              server: "linear",
              tool: "list_issues",
              arguments: { team: "T3" },
              status: "completed",
              result: { content: [{ type: "text", text: "3 issues" }], isError: false },
              error: null,
              appContext: null,
            },
          },
        },
        {
          sourceId: "line-8",
          nativeId: "ws_1",
          nativeTurnId: "turn-1",
          item: {
            type: "web_search",
            status: "completed",
            patterns: ["effect schema union", "effect schema", "effect schema"],
          },
        },
        {
          sourceId: "line-9",
          nativeId: "ext_1",
          nativeTurnId: "turn-1",
          item: { type: "web_search", status: "completed", patterns: ["vitest"] },
        },
      ],
    );
  });

  it("reads interrupted turns and compactions as notices, a compaction once", () => {
    const event = (second: number, payload: Record<string, unknown>) =>
      line({
        timestamp: `2026-07-02T08:00:0${second}.000Z`,
        type: "event_msg",
        payload,
      });
    const turn = (id: string) => event(1, { type: "task_started", turn_id: id });
    const both = parseCodexRolloutJsonl(
      sessionMeta() +
        turn("turn-1") +
        event(2, { type: "turn_aborted", turn_id: "turn-1", reason: "interrupted" }) +
        turn("turn-2") +
        event(3, { type: "context_compacted" }) +
        event(4, {
          type: "item_completed",
          turn_id: "turn-2",
          item: { type: "ContextCompaction", id: "cc_1" },
        }),
    );
    assert.deepEqual(
      (both.items ?? []).map(({ sourceId, nativeTurnId, item }) => ({
        sourceId,
        nativeTurnId,
        item,
      })),
      [
        {
          sourceId: "line-2",
          nativeTurnId: "turn-1",
          item: { type: "system_notice", status: "completed", message: "Turn interrupted" },
        },
        {
          sourceId: "line-5",
          nativeTurnId: "turn-2",
          item: { type: "compaction", status: "completed" },
        },
      ],
    );
    // A record that writes only the event still shows the compaction.
    const eventOnly = parseCodexRolloutJsonl(
      sessionMeta() + turn("turn-1") + event(3, { type: "context_compacted" }),
    );
    assert.deepEqual(
      (eventOnly.items ?? []).map(({ sourceId, item }) => ({ sourceId, type: item.type })),
      [{ sourceId: "line-2", type: "compaction" }],
    );
  });

  it("takes the latest model and goal the record names", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        line({
          timestamp: "t",
          type: "turn_context",
          payload: { turn_id: "a", model: "gpt-5.5" },
        }) +
        line({
          timestamp: "t",
          type: "event_msg",
          payload: {
            type: "thread_goal_updated",
            goal: { objective: "ship it", status: "active" },
          },
        }) +
        line({
          timestamp: "t",
          type: "event_msg",
          payload: { type: "thread_settings_applied", thread_settings: { model: "gpt-6-astra" } },
        }) +
        line({
          timestamp: "t",
          type: "event_msg",
          payload: {
            type: "thread_goal_updated",
            goal: {
              objective: "ship it",
              status: "budgetLimited",
              tokensUsed: 1200,
              tokenBudget: 1000,
              timeUsedSeconds: 61,
            },
          },
        }),
    );
    assert.equal(parsed.model, "gpt-6-astra");
    assert.deepEqual(parsed.goal, {
      objective: "ship it",
      status: "budgetLimited",
      tokensUsed: 1200,
      tokenBudget: 1000,
      timeUsedSeconds: 61,
    });
    const cleared = parseCodexRolloutJsonl(
      sessionMeta() +
        line({
          timestamp: "t",
          type: "event_msg",
          payload: { type: "thread_goal_updated", goal: { objective: "x", status: "active" } },
        }) +
        line({ timestamp: "t", type: "event_msg", payload: { type: "thread_goal_cleared" } }),
    );
    assert.isNull(cleared.goal);
    assert.isUndefined(parseCodexRolloutJsonl(sessionMeta()).goal);
  });

  it("gives a later file of a thread its own row ids and no fork signature of its own", () => {
    const content =
      sessionMeta({
        history_base: { thread_id: SESSION_ID, end_ordinal_exclusive: 40, end_byte_offset: 9000 },
      }) +
      line({ timestamp: "t", type: "response_item", payload: { type: "message", id: "r1" } }) +
      userMessage("go on") +
      agentMessage("going on");
    const parsed = parseCodexRolloutJsonl(content, { segmentId: "seg-b" });
    assert.equal(parsed.sessionId, SESSION_ID);
    assert.isTrue(parsed.continuesEarlierFile);
    assert.deepEqual(parsed.forkSignature, []);
    assert.deepEqual(
      parsed.messages.map((message) => message.sourceId),
      ["segment-seg-b-line-2", "segment-seg-b-line-3"],
    );
    const whole = parseCodexRolloutJsonl(sessionMeta() + userMessage("hi") + agentMessage("yo"), {
      segmentId: "seg-c",
    });
    assert.isUndefined(whole.continuesEarlierFile);
  });
});

describe("parseCodexSessionIndex", () => {
  it("keeps the last name each thread was given, and drops a cleared one", () => {
    const names = parseCodexSessionIndex(
      line({ id: "a", thread_name: "first name", updated_at: "2026-10-01T00:00:00Z" }) +
        line({ id: "b", thread_name: "other", updated_at: "2026-10-01T00:00:01Z" }) +
        line({ id: "a", thread_name: " renamed in Codex ", updated_at: "2026-10-02T00:00:00Z" }) +
        line({ id: "b", thread_name: "", updated_at: "2026-10-02T00:00:01Z" }) +
        `not json\n` +
        `{"id":"c","thread_name":"partial`,
    );
    assert.deepEqual(
      [...names],
      [["a", { name: "renamed in Codex", updatedAt: "2026-10-02T00:00:00Z" }]],
    );
  });
});
