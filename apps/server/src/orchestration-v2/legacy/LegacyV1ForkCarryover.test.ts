import { assert, describe, it } from "@effect/vitest";
import {
  PlanId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";

import type { WuweiTranscript } from "../../provider/wuwei/wuweiEmbedApi.ts";
import { deriveProviderThread } from "../IdAllocator.ts";
import {
  activityTimeline,
  contextUsageFromActivities,
  importedShellStatus,
  legacyProviderThread,
  nativeThreadIdFromCursor,
  orderTimeline,
  wuweiTimeline,
  type ForkTimelineEntry,
  type ForkTimelineMessage,
  type LegacyActivityRow,
} from "./LegacyV1ForkCarryover.ts";

const THREAD_ID = ThreadId.make("thread:fork-carryover");
const CLAUDE_SESSION = "9fc38739-f7fb-4dd6-bd4b-ab64f5b62051";
/** A v1 Claude cursor as stored in provider_session_runtime. */
const CLAUDE_CURSOR = JSON.stringify({
  threadId: "2fc3bff2-195e-46c7-96ea-a5f806469dd5",
  resume: CLAUDE_SESSION,
  resumeSessionAt: "61c47921-5e86-4e40-adae-17d06bc6e472",
  turnCount: 3,
  turnStartMessageIds: ["d7a0ab3e-1d2b-4b4f-9a1c-3c1f5a6c7d8e"],
});
const WUWEI_SESSION = "2026-10-04-05-32-44-97tc";

const isoAt = (epochMs: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMs));
const at = (seconds: number) => isoAt(Date.UTC(2026, 8, 24, 18, 0, 0) + seconds * 1_000);

let sequence = 0;
const row = (
  activityId: string,
  kind: string,
  seconds: number,
  payload: unknown,
  summary = "",
  tone = "tool",
): LegacyActivityRow => ({
  activity_id: activityId,
  kind,
  tone,
  summary,
  payload_json: JSON.stringify(payload),
  created_at: at(seconds),
  sequence: ++sequence,
});

const build = (entries: ReadonlyArray<ForkTimelineEntry>) =>
  entries.flatMap((entry, index) => (entry.kind === "item" ? [entry.build(index + 1)] : []));

const itemFor = (items: ReadonlyArray<OrchestrationV2TurnItem>, activityId: string) => {
  const item = items.find((candidate) => candidate.id === `migration:v1:item:${activityId}`);
  assert.isDefined(item, `no item for ${activityId}`);
  return item!;
};

const appThread = (instanceId: string): OrchestrationV2AppThread => ({
  createdBy: "system",
  creationSource: "server",
  id: THREAD_ID,
  projectId: ProjectId.make("project:fork-carryover"),
  title: "Migrated",
  providerInstanceId: ProviderInstanceId.make(instanceId),
  modelSelection: { instanceId: ProviderInstanceId.make(instanceId), model: "model" },
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  historyOrigin: "v1_import",
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
  forkedFrom: null,
  createdAt: DateTime.makeUnsafe("2026-09-01T00:00:00.000Z"),
  updatedAt: DateTime.makeUnsafe("2026-09-24T18:30:00.000Z"),
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});

describe("nativeThreadIdFromCursor", () => {
  it("reads the native session out of each driver's v1 resume cursor", () => {
    assert.equal(nativeThreadIdFromCursor("claudeAgent", CLAUDE_CURSOR), CLAUDE_SESSION);
    assert.equal(
      nativeThreadIdFromCursor("claudeAgent", JSON.stringify({ sessionId: CLAUDE_SESSION })),
      CLAUDE_SESSION,
    );
    assert.equal(
      nativeThreadIdFromCursor(
        "codex",
        JSON.stringify({ threadId: "019fac53-994e-73c3-b4f6-3226e80366cb" }),
      ),
      "019fac53-994e-73c3-b4f6-3226e80366cb",
    );
    assert.equal(
      nativeThreadIdFromCursor(
        "wuwei",
        JSON.stringify({ schemaVersion: 1, sessionId: WUWEI_SESSION }),
      ),
      WUWEI_SESSION,
    );
  });

  it("refuses cursors that cannot resume", () => {
    // A Claude session that never ran a turn has nothing to resume.
    assert.isNull(
      nativeThreadIdFromCursor(
        "claudeAgent",
        JSON.stringify({ resume: CLAUDE_SESSION, turnCount: 0 }),
      ),
    );
    assert.isNull(
      nativeThreadIdFromCursor("claudeAgent", JSON.stringify({ resume: "not-a-session-uuid" })),
    );
    assert.isNull(nativeThreadIdFromCursor("codex", JSON.stringify({ threadId: "  " })));
    assert.isNull(nativeThreadIdFromCursor("wuwei", JSON.stringify({ schemaVersion: 1 })));
    assert.isNull(nativeThreadIdFromCursor("cursor", JSON.stringify({ sessionId: "abc" })));
    assert.isNull(nativeThreadIdFromCursor("codex", null));
    assert.isNull(nativeThreadIdFromCursor("codex", "{not json"));
    assert.isNull(nativeThreadIdFromCursor("codex", JSON.stringify(["threadId"])));
  });
});

describe("legacyProviderThread", () => {
  it("resumes the thread's own native session with the thread's timestamps", () => {
    const thread = appThread("claudeAgent");
    const contextUsage = { usedTokens: 316_845, maxTokens: 1_000_000 };
    const providerThread = legacyProviderThread({
      thread,
      runtime: {
        runtime_provider_name: "claudeAgent",
        runtime_provider_instance_id: "claudeAgent",
        runtime_resume_cursor_json: CLAUDE_CURSOR,
      },
      contextUsage,
    });
    const driver = ProviderDriverKind.make("claudeAgent");
    assert.deepStrictEqual(providerThread, {
      id: deriveProviderThread({ driver, nativeThreadId: CLAUDE_SESSION }),
      driver,
      providerInstanceId: ProviderInstanceId.make("claudeAgent"),
      providerSessionId: null,
      appThreadId: THREAD_ID,
      ownerNodeId: null,
      nativeThreadRef: { driver, nativeId: CLAUDE_SESSION, strength: "strong" },
      nativeConversationHeadRef: null,
      status: "idle",
      firstRunOrdinal: null,
      lastRunOrdinal: null,
      handoffIds: [],
      forkedFrom: null,
      pendingBackgroundTasks: [],
      contextUsage,
      createdAt: thread.createdAt,
      updatedAt: thread.updatedAt,
    });
  });

  it("treats a runtime row without an instance id as the driver's default instance", () => {
    const providerThread = legacyProviderThread({
      thread: appThread("codex"),
      runtime: {
        runtime_provider_name: "codex",
        runtime_provider_instance_id: null,
        runtime_resume_cursor_json: JSON.stringify({ threadId: "019fac53" }),
      },
      contextUsage: null,
    });
    assert.equal(providerThread?.nativeThreadRef?.nativeId, "019fac53");
    assert.equal(providerThread?.providerInstanceId, "codex");
    assert.isNull(providerThread?.contextUsage);
  });

  it("does not resume another instance's session or a thread without a runtime row", () => {
    assert.isNull(
      legacyProviderThread({
        thread: appThread("claude-work"),
        runtime: {
          runtime_provider_name: "claudeAgent",
          runtime_provider_instance_id: "claudeAgent",
          runtime_resume_cursor_json: CLAUDE_CURSOR,
        },
        contextUsage: null,
      }),
    );
    assert.isNull(
      legacyProviderThread({
        thread: appThread("codex"),
        runtime: {
          runtime_provider_name: null,
          runtime_provider_instance_id: null,
          runtime_resume_cursor_json: null,
        },
        contextUsage: null,
      }),
    );
  });
});

describe("contextUsageFromActivities", () => {
  it("takes the last readable context-window reading", () => {
    const rows = [
      row(
        "cw1",
        "context-window.updated",
        1,
        { usedTokens: 4_508, maxTokens: 372_000, inputTokens: 4_503, outputTokens: 5 },
        "Context window updated",
        "info",
      ),
      row(
        "cw2",
        "context-window.updated",
        2,
        {
          usedTokens: 21_046,
          maxTokens: 258_400,
          inputTokens: 21_041,
          cachedInputTokens: 0,
          outputTokens: 5,
          reasoningOutputTokens: 0,
          lastUsedTokens: 21_046,
          lastInputTokens: 21_041,
          lastCachedInputTokens: 0,
          lastOutputTokens: 5,
          lastReasoningOutputTokens: 0,
          compactsAutomatically: true,
        },
        "Context window updated",
        "info",
      ),
      row("t1", "tool.completed", 3, { itemType: "command_execution", detail: "pwd" }),
      // Unreadable readings never replace a good one.
      row("cw3", "context-window.updated", 4, { usedTokens: -1 }, "", "info"),
    ];
    assert.deepStrictEqual(contextUsageFromActivities(rows), {
      usedTokens: 21_046,
      maxTokens: 258_400,
      inputTokens: 21_041,
      cachedInputTokens: 0,
      outputTokens: 5,
      reasoningOutputTokens: 0,
      lastUsedTokens: 21_046,
      lastInputTokens: 21_041,
      lastCachedInputTokens: 0,
      lastOutputTokens: 5,
      lastReasoningOutputTokens: 0,
      compactsAutomatically: true,
    });
    assert.isNull(contextUsageFromActivities(rows.slice(2)));
  });
});

describe("activityTimeline", () => {
  it("collapses Claude tool rows into one item per tool call", () => {
    const bash = {
      toolName: "Bash",
      input: { command: "ls -la release", description: "Confirm rebuilt artifacts exist" },
    };
    const bashResult = {
      tool_use_id: "toolu_01UNbfUnGU4uXLL1vZAGtf9A",
      type: "tool_result",
      content: "236609938 release/T3-Code-0.0.30-arm64.dmg",
      is_error: false,
    };
    const items = build(
      activityTimeline(THREAD_ID, [
        row(
          "c1",
          "tool.started",
          1,
          { itemType: "command_execution", detail: "Bash: {}" },
          "Command run started",
        ),
        row(
          "c2",
          "tool.updated",
          2,
          {
            itemType: "command_execution",
            status: "inProgress",
            detail: "Bash: ls -la release",
            data: bash,
          },
          "Command run",
        ),
        row(
          "c3",
          "tool.updated",
          3,
          {
            itemType: "command_execution",
            status: "inProgress",
            detail: "Bash: ls -la release",
            data: { ...bash, result: bashResult },
          },
          "Command run",
        ),
        row(
          "c4",
          "tool.completed",
          4,
          {
            itemType: "command_execution",
            detail: "Bash: ls -la release",
            data: { ...bash, result: bashResult },
          },
          "Command run",
        ),
        row(
          "c5",
          "tool.completed",
          5,
          {
            itemType: "file_change",
            detail: 'Edit: {"file_path":"/repo/src/parser.ts"}',
            data: {
              toolName: "Edit",
              input: {
                replace_all: false,
                file_path: "/repo/src/parser.ts",
                old_string: "let cwd: string | undefined;",
                new_string: "let cwd: string | undefined;\nlet hasProject = false;",
              },
              result: {
                tool_use_id: "toolu_01PBhfXJaWGHMXYQZKFmkEqz",
                type: "tool_result",
                content: "The file /repo/src/parser.ts has been updated successfully.",
              },
            },
          },
          "File change",
        ),
        row(
          "c6",
          "tool.completed",
          6,
          {
            itemType: "dynamic_tool_call",
            detail: 'Read: {"file_path":"/repo/tasks/b1.output"}',
            data: {
              toolName: "Read",
              input: { file_path: "/repo/tasks/b1.output" },
              result: {
                tool_use_id: "toolu_012PGXdeqan1kW31uQu4EhuA",
                type: "tool_result",
                content: [{ type: "text", text: "1\tBUILD_DONE" }],
              },
            },
          },
          "Tool call",
        ),
      ]),
    );

    assert.deepStrictEqual(
      items.map((item) => item.id),
      ["migration:v1:item:c3", "migration:v1:item:c5", "migration:v1:item:c6"],
    );
    assert.deepInclude(itemFor(items, "c3"), {
      type: "command_execution",
      status: "completed",
      title: "Command run",
      input: "ls -la release",
      output: "236609938 release/T3-Code-0.0.30-arm64.dmg",
      threadId: THREAD_ID,
      runId: null,
      providerThreadId: null,
      nativeItemRef: null,
      startedAt: DateTime.makeUnsafe(at(3)),
      completedAt: DateTime.makeUnsafe(at(4)),
      updatedAt: DateTime.makeUnsafe(at(4)),
    });
    assert.deepInclude(itemFor(items, "c5"), {
      type: "file_change",
      status: "completed",
      fileName: "/repo/src/parser.ts",
      oldStr: "let cwd: string | undefined;",
      newStr: "let cwd: string | undefined;\nlet hasProject = false;",
    });
    assert.deepInclude(itemFor(items, "c6"), {
      type: "dynamic_tool",
      status: "completed",
      toolName: "Read",
      input: { file_path: "/repo/tasks/b1.output" },
      output: "1\tBUILD_DONE",
    });
  });

  // v1 marks a failed Claude tool on its tool.updated row and in result.is_error,
  // and a failed Codex MCP call only on its item.
  it("keeps a failed Claude tool call failed", () => {
    const failed = {
      toolName: "Bash",
      input: { command: "ssh -o ConnectTimeout=8 box uname -m" },
      result: {
        type: "tool_result",
        content: "Exit code 255\nssh: connect to host box port 22: Operation timed out",
        is_error: true,
        tool_use_id: "toolu_01Y7CbVRK2f7ddJeAVZCb3NS",
      },
    };
    const items = build(
      activityTimeline(THREAD_ID, [
        row(
          "f1",
          "tool.updated",
          1,
          {
            itemType: "command_execution",
            status: "failed",
            detail: "Bash: ssh -o ConnectTimeout=8 box uname -m",
            data: failed,
          },
          "Command run",
        ),
        row(
          "f2",
          "tool.completed",
          2,
          {
            itemType: "command_execution",
            detail: "Bash: ssh -o ConnectTimeout=8 box uname -m",
            data: failed,
          },
          "Command run",
        ),
      ]),
    );
    assert.equal(itemFor(items, "f1").status, "failed");
  });

  it("maps ACP and wuwei tool rows by toolCallId", () => {
    const content = (text: string) => [{ type: "content", content: { type: "text", text } }];
    const items = build(
      activityTimeline(THREAD_ID, [
        row(
          "w1",
          "tool.updated",
          1,
          {
            itemType: "command_execution",
            toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
            status: "inProgress",
            title: "Ran command",
            detail: "date && uptime",
            data: {
              command: "date && uptime",
              toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
              kind: "execute",
            },
          },
          "Ran command",
        ),
        row(
          "w2",
          "tool.completed",
          2,
          {
            itemType: "command_execution",
            toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
            status: "completed",
            title: "Ran command",
            detail: "date && uptime",
            data: {
              toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
              kind: "execute",
              command: "date && uptime",
              rawInput: { command: "date && uptime" },
              content: content("Thu Sep 24 18:12:30 PDT 2026\n"),
            },
          },
          "Ran command",
        ),
        row(
          "w3",
          "tool.completed",
          3,
          {
            itemType: "file_change",
            toolCallId: "toolu_01SXuE5zFRFjxawBgyZ3ARfC",
            status: "completed",
            title: "Changed files",
            detail: "/Users/me/.wuwei/t3/memory.md",
            data: {
              toolCallId: "toolu_01SXuE5zFRFjxawBgyZ3ARfC",
              kind: "edit",
              rawInput: {
                path: "/Users/me/.wuwei/t3/memory.md",
                oldText: "- Never infer my current deployment",
                newText: "- 4Real iOS repo notes\n- Never infer my current deployment",
              },
              locations: [{ path: "/Users/me/.wuwei/t3/memory.md" }],
              content: content("edited /Users/me/.wuwei/t3/memory.md"),
            },
          },
          "Changed files",
        ),
        row(
          "w4",
          "tool.completed",
          4,
          {
            itemType: "web_search",
            toolCallId: "toolu_01HZQb4xpm9DoPGwwS7ERPdE",
            status: "failed",
            title: "Searched files",
            detail: "T3 Chat iOS app voice to text",
            data: {
              toolCallId: "toolu_01HZQb4xpm9DoPGwwS7ERPdE",
              kind: "search",
              rawInput: { query: "T3 Chat iOS app voice to text", numResults: 8 },
              content: content("EXA_API_KEY not set (add it to .env)"),
            },
          },
          "Searched files",
        ),
        row(
          "w5",
          "tool.completed",
          5,
          {
            itemType: "dynamic_tool_call",
            toolCallId: "toolu_014GYgENMrkbhxtLosXYKmVe",
            status: "completed",
            title: "Read file",
            detail: "/Users/me/frames/sheet_02.jpg",
            data: {
              toolCallId: "toolu_014GYgENMrkbhxtLosXYKmVe",
              kind: "read",
              rawInput: { path: "/Users/me/frames/sheet_02.jpg" },
              locations: [{ path: "/Users/me/frames/sheet_02.jpg" }],
              content: [
                { type: "content", content: { type: "text", text: "sheet_02.jpg (2560x1080)" } },
                { type: "image", data: "…", mimeType: "image/jpeg" },
              ],
            },
          },
          "Read file",
        ),
        // Still running when v1 stopped: the call never completed.
        row(
          "w6",
          "tool.updated",
          6,
          {
            itemType: "command_execution",
            toolCallId: "call_BiDAruuh1VMbDnA7bgU0VIvL|fc_00de9b47",
            status: "inProgress",
            title: "Ran command",
            detail: "pwd; ls -la",
            data: {
              command: "pwd; ls -la",
              toolCallId: "call_BiDAruuh1VMbDnA7bgU0VIvL|fc_00de9b47",
              kind: "execute",
            },
          },
          "Ran command",
        ),
      ]),
    );

    assert.deepInclude(itemFor(items, "w1"), {
      type: "command_execution",
      status: "completed",
      title: "Ran command",
      input: "date && uptime",
      output: "Thu Sep 24 18:12:30 PDT 2026\n",
      completedAt: DateTime.makeUnsafe(at(2)),
    });
    assert.deepInclude(itemFor(items, "w3"), {
      type: "file_change",
      fileName: "/Users/me/.wuwei/t3/memory.md",
      oldStr: "- Never infer my current deployment",
      newStr: "- 4Real iOS repo notes\n- Never infer my current deployment",
    });
    assert.deepInclude(itemFor(items, "w4"), {
      type: "web_search",
      status: "failed",
      patterns: ["T3 Chat iOS app voice to text"],
    });
    assert.deepInclude(itemFor(items, "w5"), {
      type: "dynamic_tool",
      status: "completed",
      title: "Read file",
      input: { path: "/Users/me/frames/sheet_02.jpg" },
      output: "sheet_02.jpg (2560x1080)\n[image]",
    });
    assert.deepInclude(itemFor(items, "w6"), {
      type: "command_execution",
      status: "interrupted",
      input: "pwd; ls -la",
      completedAt: null,
    });
  });

  it("maps Codex tool rows from their item", () => {
    const items = build(
      activityTimeline(THREAD_ID, [
        row(
          "k1",
          "tool.started",
          1,
          { itemType: "command_execution", detail: "/bin/zsh -lc pwd" },
          "Command run started",
        ),
        row(
          "k2",
          "tool.completed",
          2,
          { itemType: "command_execution", detail: "/bin/zsh -lc pwd" },
          "Command run complete",
        ),
        row(
          "k3",
          "tool.completed",
          3,
          {
            itemType: "command_execution",
            detail: "/bin/zsh -lc 'git status --short'",
            data: {
              completedAtMs: 1_785_383_051_914,
              item: {
                command: "/bin/zsh -lc 'git status --short'",
                aggregatedOutput: " M apps/server/src/index.ts\n",
                exitCode: 1,
                status: "failed",
              },
            },
          },
          "Command run complete",
        ),
        row(
          "k4",
          "tool.completed",
          4,
          {
            itemType: "web_search",
            detail: "site:developers.openai.com/codex app server",
            data: {
              item: {
                action: { type: "search", query: "site:developers.openai.com/codex app server" },
                id: "exec-22d21aab",
                query: "site:developers.openai.com/codex app server",
              },
            },
          },
          "Web search",
        ),
        row(
          "k5",
          "tool.completed",
          5,
          {
            itemType: "mcp_tool_call",
            data: {
              completedAtMs: 1_787_536_296_305,
              item: {
                server: "node_repl",
                tool: "js",
                arguments: { title: "Verify Computer Use access", code: "await sky.list_apps()" },
                result: { content: [{ type: "text", text: '[{"displayName":"Ghostty"}]' }] },
                status: "completed",
              },
            },
          },
          "node_repl · js",
        ),
        row(
          "k6",
          "tool.completed",
          6,
          {
            itemType: "file_change",
            detail: "apps/server/src/index.ts",
            data: {
              item: { changes: [{ path: "apps/server/src/index.ts", kind: "update" }] },
            },
          },
          "File change",
        ),
      ]),
    );

    assert.deepStrictEqual(
      items.map((item) => item.id),
      [
        "migration:v1:item:k2",
        "migration:v1:item:k3",
        "migration:v1:item:k4",
        "migration:v1:item:k5",
        "migration:v1:item:k6",
      ],
    );
    assert.deepInclude(itemFor(items, "k2"), {
      type: "command_execution",
      input: "/bin/zsh -lc pwd",
    });
    assert.notProperty(itemFor(items, "k2"), "output");
    assert.deepInclude(itemFor(items, "k3"), {
      type: "command_execution",
      input: "/bin/zsh -lc 'git status --short'",
      output: " M apps/server/src/index.ts\n",
      exitCode: 1,
    });
    assert.deepInclude(itemFor(items, "k4"), {
      type: "web_search",
      patterns: ["site:developers.openai.com/codex app server"],
    });
    assert.deepInclude(itemFor(items, "k5"), {
      type: "dynamic_tool",
      toolName: "js",
      input: { title: "Verify Computer Use access", code: "await sky.list_apps()" },
      output: '[{"displayName":"Ghostty"}]',
      title: "node_repl · js",
    });
    assert.deepInclude(itemFor(items, "k6"), {
      type: "file_change",
      fileName: "apps/server/src/index.ts",
    });
  });

  it("clips very long tool output", () => {
    const items = build(
      activityTimeline(THREAD_ID, [
        row("big", "tool.completed", 1, {
          itemType: "command_execution",
          toolCallId: "call_big",
          status: "completed",
          data: { rawInput: { command: "cat huge.log" }, content: "x".repeat(150_000) },
        }),
      ]),
    );
    const item = itemFor(items, "big");
    assert.equal(
      item.type === "command_execution" ? item.output : undefined,
      `${"x".repeat(100_000)}…`,
    );
  });

  it("turns thinking, background tasks, plans, errors and notices into items", () => {
    const items = build(
      activityTimeline(THREAD_ID, [
        // wuwei's thinking, which v1 stored as "dream" task progress.
        row(
          "d1",
          "task.progress",
          1,
          {
            taskId: "wuwei-thinking:5c632ac0:1",
            title: "Thinking",
            detail: "A harmless request; a quick shell command will do.",
            summary: "A harmless request; a quick shell command will do.",
            agentKind: "background",
            taskType: "dream",
          },
          "Thinking",
          "info",
        ),
        // Codex reasoning summaries.
        row(
          "r1",
          "task.progress",
          2,
          {
            taskId: "019ccbdc-3726-7de2-8dfe-b800f4c732ba",
            detail: "**Inspecting code repository**\n\nI need to inspect the repository first.",
          },
          "Reasoning update",
          "info",
        ),
        // A Claude subagent.
        row(
          "s1",
          "task.started",
          3,
          {
            taskId: "a64ef6081fccf9359",
            taskType: "local_agent",
            detail: "Classify titles part 1-2",
            agentKind: "agent",
            title: "Classify titles part 1-2",
            toolUseId: "toolu_012yv8qJHU22QqogQZwxtD6e",
          },
          "local_agent task started",
          "info",
        ),
        row(
          "s2",
          "task.progress",
          4,
          {
            taskId: "a64ef6081fccf9359",
            title: "Classify titles part 1-2",
            detail: "Writing title_picks_A.txt",
            lastToolName: "Write",
            taskType: "local_agent",
          },
          "Writing title_picks_A.txt",
          "info",
        ),
        row(
          "s3",
          "task.completed",
          5,
          {
            taskId: "a64ef6081fccf9359",
            status: "completed",
            title: "Classify titles part 1-2",
            detail: "Wrote 412 picks to title_picks_A.txt",
          },
          "Task completed",
          "info",
        ),
        // A background shell task v1 never saw finish.
        row(
          "b1",
          "task.started",
          6,
          {
            taskId: "bxu52o2th",
            taskType: "local_bash",
            detail: "Typecheck and run importer integration test",
          },
          "local_bash task started",
          "info",
        ),
        row(
          "p1",
          "turn.plan.updated",
          7,
          {
            plan: [
              { step: "Study the x_search repo structure", status: "completed" },
              { step: " Research Claude Code skills distribution ", status: "inProgress" },
              { step: "Synthesize recommendations", status: "pending" },
            ],
          },
          "Plan updated",
          "info",
        ),
        row("p2", "turn.plan.updated", 8, { plan: [] }, "Plan updated", "info"),
        row(
          "e1",
          "runtime.error",
          9,
          { message: "ERROR codex_core::auth: Failed to refresh token: 401 Unauthorized" },
          "Runtime error",
          "error",
        ),
        row(
          "e2",
          "provider.turn.start.failed",
          10,
          { detail: `ProviderAdapterProcessError: ${"stack ".repeat(1_000)}` },
          "Provider turn start failed",
          "error",
        ),
        row(
          "n1",
          "runtime.warning",
          11,
          {
            message: "Unhandled Claude system message subtype 'task_updated'.",
            detail: { type: "system", subtype: "task_updated" },
          },
          "Runtime warning",
          "info",
        ),
        row(
          "x1",
          "context-window.updated",
          12,
          { usedTokens: 1, maxTokens: 2 },
          "Context window updated",
          "info",
        ),
        row(
          "x2",
          "checkpoint.captured",
          13,
          { turnCount: 1, status: "completed" },
          "Checkpoint captured",
          "info",
        ),
      ]),
    );

    assert.deepStrictEqual(
      items.map((item) => [item.id, item.type]),
      [
        ["migration:v1:item:d1", "reasoning"],
        ["migration:v1:item:r1", "reasoning"],
        ["migration:v1:item:p1", "todo_list"],
        ["migration:v1:item:e1", "error"],
        ["migration:v1:item:e2", "error"],
        ["migration:v1:item:n1", "system_notice"],
        ["migration:v1:item:s1", "dynamic_tool"],
        ["migration:v1:item:b1", "dynamic_tool"],
      ],
    );
    assert.deepInclude(itemFor(items, "d1"), {
      title: "Thinking",
      status: "completed",
      text: "A harmless request; a quick shell command will do.",
      streaming: false,
    });
    assert.deepInclude(itemFor(items, "s1"), {
      status: "completed",
      title: "Classify titles part 1-2",
      toolName: "Task",
      input: { title: "Classify titles part 1-2", taskType: "local_agent" },
      output: "Wrote 412 picks to title_picks_A.txt",
      completedAt: DateTime.makeUnsafe(at(5)),
    });
    assert.deepInclude(itemFor(items, "b1"), {
      status: "interrupted",
      title: "Typecheck and run importer integration test",
      completedAt: null,
    });
    assert.deepInclude(itemFor(items, "p1"), {
      type: "todo_list",
      title: "Plan",
      planId: PlanId.make("migration:v1:plan:p1"),
      steps: [
        { id: "1", text: "Study the x_search repo structure", status: "completed" },
        { id: "2", text: "Research Claude Code skills distribution", status: "running" },
        { id: "3", text: "Synthesize recommendations", status: "pending" },
      ],
    });
    assert.deepInclude(itemFor(items, "e1"), {
      status: "failed",
      title: "Runtime error",
      failure: {
        class: "provider_error",
        message: "ERROR codex_core::auth: Failed to refresh token: 401 Unauthorized",
        code: null,
        retryable: null,
      },
    });
    const startFailure = itemFor(items, "e2");
    const message = startFailure.type === "error" ? startFailure.failure.message : "";
    assert.equal(message.length, 4_097);
    assert.isTrue(message.startsWith("ProviderAdapterProcessError: stack"));
    assert.isTrue(message.endsWith("…"));
    assert.deepInclude(itemFor(items, "n1"), {
      status: "completed",
      message: "Unhandled Claude system message subtype 'task_updated'.",
    });
  });
});

const WUWEI_T0 = Date.UTC(2026, 9, 4, 5, 32, 44);
const transcript = (turns: WuweiTranscript["turns"]): WuweiTranscript => ({
  id: WUWEI_SESSION,
  label: "Date and memory",
  createdAt: WUWEI_T0,
  updatedAt: WUWEI_T0 + 60_000,
  workspaceRoot: "/Users/me",
  model: "gpt-6-sol",
  turns,
});
const iso = (offsetMs: number) => isoAt(WUWEI_T0 + offsetMs);

describe("wuweiTimeline", () => {
  it("rebuilds a wuwei chat as messages, thinking and tool items", () => {
    const entries = wuweiTimeline(
      THREAD_ID,
      transcript([
        {
          userItemId: "u1",
          userText: "What's the date?",
          userImageCount: 0,
          at: WUWEI_T0,
          entries: [
            { kind: "reasoning", id: "a2.thinking.0", text: "Run date.", at: WUWEI_T0 + 1_000 },
            { kind: "reasoning", id: "a2.thinking.1", text: "  ", at: WUWEI_T0 + 1_000 },
            {
              kind: "tool",
              id: "call_date",
              name: "bash",
              title: "Ran command",
              input: { command: "date" },
              output: "Sun Oct  4 05:32:46 PDT 2026",
              status: "completed",
              startedAt: WUWEI_T0 + 2_000,
              completedAt: WUWEI_T0 + 3_000,
            },
            { kind: "text", id: "a4.text.0", text: "", at: WUWEI_T0 + 4_000 },
            { kind: "text", id: "a4.text.1", text: "It is Sunday.", at: WUWEI_T0 + 4_000 },
          ],
        },
        {
          userItemId: "u5",
          userText: "Update memory with these",
          userImageCount: 2,
          at: WUWEI_T0 + 10_000,
          entries: [
            {
              kind: "tool",
              id: "call_edit",
              name: "edit",
              title: "Edited memory.md",
              input: { path: "/Users/me/.wuwei/t3/memory.md", oldText: "- a", newText: "- b" },
              output: "oldText not found",
              status: "failed",
              startedAt: WUWEI_T0 + 11_000,
              completedAt: WUWEI_T0 + 11_500,
            },
            { kind: "steer", id: "u7", text: "also note the date", at: WUWEI_T0 + 12_000 },
            {
              kind: "tool",
              id: "call_exa",
              name: "exa",
              title: "Searched the web",
              input: { query: "wuwei memory format" },
              output: null,
              status: "cancelled",
              startedAt: WUWEI_T0 + 13_000,
              completedAt: null,
            },
            {
              kind: "tool",
              id: "call_look",
              name: "look",
              title: "Looked at screen.png",
              input: { path: "/tmp/screen.png" },
              output: null,
              status: "missing",
              startedAt: WUWEI_T0 + 14_000,
              completedAt: null,
            },
          ],
        },
      ]),
    );

    const prefix = `wuwei:${WUWEI_SESSION}`;
    assert.deepStrictEqual(
      entries.map((entry) =>
        entry.kind === "message"
          ? [entry.role, entry.message_id, entry.text, entry.created_at]
          : ["item", entry.id, entry.at],
      ),
      [
        ["user", `${prefix}:u1`, "What's the date?", iso(0)],
        ["item", `migration:v1:item:${prefix}:a2.thinking.0:0`, iso(1_000)],
        ["item", `migration:v1:item:${prefix}:call_date:2`, iso(2_000)],
        ["assistant", `${prefix}:a4.text.1:4`, "It is Sunday.", iso(4_000)],
        ["user", `${prefix}:u5`, "Update memory with these\n\n[2 images]", iso(10_000)],
        ["item", `migration:v1:item:${prefix}:call_edit:0`, iso(11_000)],
        ["user", `${prefix}:u7:1`, "also note the date", iso(12_000)],
        ["item", `migration:v1:item:${prefix}:call_exa:2`, iso(13_000)],
        ["item", `migration:v1:item:${prefix}:call_look:3`, iso(14_000)],
      ],
    );
    for (const entry of entries) {
      if (entry.kind === "message") assert.equal(entry.thread_id, THREAD_ID);
    }

    const items = build(entries);
    assert.deepInclude(items[0], {
      type: "reasoning",
      title: "Thinking",
      status: "completed",
      text: "Run date.",
    });
    assert.deepInclude(items[1], {
      type: "command_execution",
      status: "completed",
      title: "Ran command",
      input: "date",
      output: "Sun Oct  4 05:32:46 PDT 2026",
      startedAt: DateTime.makeUnsafe(iso(2_000)),
      completedAt: DateTime.makeUnsafe(iso(3_000)),
    });
    assert.deepInclude(items[2], {
      type: "file_change",
      status: "failed",
      fileName: "/Users/me/.wuwei/t3/memory.md",
      oldStr: "- a",
      newStr: "- b",
    });
    assert.deepInclude(items[3], {
      type: "web_search",
      status: "cancelled",
      patterns: ["wuwei memory format"],
      completedAt: null,
    });
    assert.deepInclude(items[4], {
      type: "dynamic_tool",
      status: "interrupted",
      toolName: "look",
      input: { path: "/tmp/screen.png" },
    });
    assert.notProperty(items[4], "output");
  });

  // A wuwei chat that opens on an assistant message (a duet or a fork) has no user turn start.
  it("skips the empty user turn of a chat that opens on an assistant message", () => {
    const entries = wuweiTimeline(
      THREAD_ID,
      transcript([
        {
          userItemId: "",
          userText: "",
          userImageCount: 0,
          at: WUWEI_T0,
          entries: [{ kind: "text", id: "a1.text.0", text: "Forked here.", at: WUWEI_T0 }],
        },
      ]),
    );
    assert.deepStrictEqual(
      entries.map((entry) => (entry.kind === "message" ? entry.role : entry.kind)),
      ["assistant"],
    );
  });
});

describe("orderTimeline", () => {
  const message = (
    id: string,
    role: "user" | "assistant",
    seconds: number,
  ): ForkTimelineMessage => ({
    kind: "message",
    message_id: id,
    thread_id: THREAD_ID,
    role,
    text: id,
    attachments_json: null,
    context_json: null,
    is_streaming: 0,
    created_at: at(seconds),
    updated_at: at(seconds),
  });
  const item = (id: string, seconds: number): ForkTimelineEntry => ({
    kind: "item",
    id: `migration:v1:item:${id}` as never,
    at: at(seconds),
    build: () => {
      throw new Error("not built");
    },
  });
  const ids = (entries: ReadonlyArray<ForkTimelineEntry>) =>
    entries.map((entry) => (entry.kind === "message" ? entry.message_id : entry.id));

  it("orders by time and keeps tools inside the turn they ran in", () => {
    assert.deepStrictEqual(
      ids(
        orderTimeline([
          message("assistant-1", "assistant", 5),
          message("user-2", "user", 6),
          message("user-1", "user", 1),
          item("tool-b", 3),
          item("tool-a", 3),
          // Same second as the user's message and the reply: user, tool, reply.
          message("assistant-2", "assistant", 6),
          item("tool-c", 6),
        ]),
      ),
      [
        "user-1",
        "migration:v1:item:tool-b",
        "migration:v1:item:tool-a",
        "assistant-1",
        "user-2",
        "migration:v1:item:tool-c",
        "assistant-2",
      ],
    );
  });
});

describe("importedShellStatus", () => {
  it("presents an imported thread with no v2 run as completed", () => {
    assert.equal(importedShellStatus("idle", { historyOrigin: "v1_import" }), "completed");
  });

  it("leaves native threads and real run statuses alone", () => {
    assert.equal(importedShellStatus("idle", { historyOrigin: undefined }), "idle");
    assert.equal(importedShellStatus("running", { historyOrigin: "v1_import" }), "running");
  });
});
