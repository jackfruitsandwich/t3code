import { afterAll, assert, it } from "@effect/vitest";
import {
  MessageId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

import type { WuweiTranscript } from "../../provider/wuwei/wuweiEmbedApi.ts";
import { deriveProviderThread } from "../IdAllocator.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import {
  CLAUDE_MODEL,
  CODEX_MODEL,
  WUWEI_MODEL,
  claudeCursor,
  insertActivity,
  insertMessage,
  insertProject,
  insertRuntime,
  insertThread,
  isoAt,
  layerTest,
  legacyImportRow,
  threadEventIds,
  writeFakeWuweiBundle,
  wuweiCursor,
} from "./LegacyV1ForkImport.testkit.ts";

const WUWEI_SESSION = "2026-10-04-05-32-44-97tc";
const WUWEI_T0 = Date.UTC(2026, 9, 4, 5, 32, 44);
const wuweiIso = (offsetMs: number) => isoAt(WUWEI_T0 + offsetMs);

/** wuwei's own record of the chat, which also holds the thinking v1 never stored. */
const WUWEI_TRANSCRIPT: WuweiTranscript = {
  id: WUWEI_SESSION,
  label: "Date and memory",
  createdAt: WUWEI_T0,
  updatedAt: WUWEI_T0 + 60_000,
  workspaceRoot: "/tmp/fork-import",
  model: "gpt-6-sol",
  turns: [
    {
      userItemId: "u1",
      userText: "What's the date?",
      userImageCount: 0,
      at: WUWEI_T0,
      entries: [
        { kind: "reasoning", id: "a2.thinking.0", text: "Run date.", at: WUWEI_T0 + 1_000 },
        {
          kind: "tool",
          id: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
          name: "bash",
          title: "Ran command",
          input: { command: "date" },
          output: "Sun Oct  4 05:32:46 PDT 2026",
          status: "completed",
          startedAt: WUWEI_T0 + 2_000,
          completedAt: WUWEI_T0 + 3_000,
        },
        { kind: "text", id: "a4.text.0", text: "It is Sunday.", at: WUWEI_T0 + 4_000 },
      ],
    },
    {
      userItemId: "u5",
      userText: "Update memory with this",
      userImageCount: 1,
      at: WUWEI_T0 + 10_000,
      entries: [
        {
          kind: "tool",
          id: "toolu_01SXuE5zFRFjxawBgyZ3ARfC",
          name: "edit",
          title: "Edited memory.md",
          input: { path: "/Users/me/.wuwei/t3/memory.md", oldText: "- a", newText: "- b" },
          output: "oldText not found",
          status: "failed",
          startedAt: WUWEI_T0 + 11_000,
          completedAt: WUWEI_T0 + 11_500,
        },
        { kind: "steer", id: "u7", text: "also note the date", at: WUWEI_T0 + 12_000 },
        { kind: "text", id: "a8.text.0", text: "Noted.", at: WUWEI_T0 + 13_000 },
      ],
    },
  ],
};

// LegacyV1ThreadImporter loads wuwei through T3CODE_WUWEI_EMBED, and the loaded
// host is kept for the rest of this file, so every test here shares this bundle.
const bundle = writeFakeWuweiBundle({ [WUWEI_SESSION]: WUWEI_TRANSCRIPT });
const previousEmbed = process.env.T3CODE_WUWEI_EMBED;
process.env.T3CODE_WUWEI_EMBED = bundle.path;
afterAll(() => {
  if (previousEmbed === undefined) delete process.env.T3CODE_WUWEI_EMBED;
  else process.env.T3CODE_WUWEI_EMBED = previousEmbed;
  bundle.cleanup();
});

const claude = ProviderDriverKind.make("claudeAgent");
const codex = ProviderDriverKind.make("codex");
const wuwei = ProviderDriverKind.make("wuwei");

const byOrdinal = (items: ReadonlyArray<OrchestrationV2TurnItem>) =>
  [...items].sort((left, right) => left.ordinal - right.ordinal);

/** Each item as [type, ordinal], with the message id for message items. */
const timelineShape = (items: ReadonlyArray<OrchestrationV2TurnItem>) =>
  byOrdinal(items).map((item) =>
    item.type === "user_message" || item.type === "assistant_message"
      ? [item.type, item.ordinal, item.messageId]
      : [item.type, item.ordinal],
  );

it.layer(layerTest)("LegacyV1ThreadImporter personal-fork carryover", (it) => {
  it.effect("resumes each v1 native session once, without reordering the sidebar", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const claudeSession = "9fc38739-f7fb-4dd6-bd4b-ab64f5b62051";
      const codexThread = "019fac53-994e-73c3-b4f6-3226e80366cb";

      yield* insertProject;
      // Claude, resumable.
      yield* insertThread({
        threadId: "thread:resume:claude",
        modelSelectionJson: CLAUDE_MODEL,
        createdAt: "2026-09-01T00:00:00.000Z",
        updatedAt: "2026-09-20T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:claude",
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor(claudeSession),
      });
      yield* insertMessage({
        messageId: "message:resume:claude:1",
        threadId: "thread:resume:claude",
        role: "user",
        text: "Rebuild the DMG",
        createdAt: "2026-09-20T09:00:00.000Z",
      });
      yield* insertMessage({
        messageId: "message:resume:claude:2",
        threadId: "thread:resume:claude",
        role: "assistant",
        text: "Rebuilt.",
        createdAt: "2026-09-20T09:30:00.000Z",
      });
      // Codex, from before runtime rows carried an instance id.
      yield* insertThread({
        threadId: "thread:resume:codex",
        modelSelectionJson: CODEX_MODEL,
        createdAt: "2026-09-02T00:00:00.000Z",
        updatedAt: "2026-09-21T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:codex",
        providerName: "codex",
        instanceId: null,
        cursorJson: `{"threadId":"${codexThread}"}`,
      });
      // Its model now lives on another Claude instance: a provider switch, not a resume.
      yield* insertThread({
        threadId: "thread:resume:other-instance",
        modelSelectionJson: '{"instanceId":"claude-work","model":"claude-opus-4-6"}',
        createdAt: "2026-09-03T00:00:00.000Z",
        updatedAt: "2026-09-22T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:other-instance",
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor("0d3c4a63-2b0a-4a47-8f05-b8c5d0b4b0a1"),
      });
      // A Claude session that never ran a turn.
      yield* insertThread({
        threadId: "thread:resume:no-turns",
        modelSelectionJson: CLAUDE_MODEL,
        createdAt: "2026-09-04T00:00:00.000Z",
        updatedAt: "2026-09-23T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:no-turns",
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor("6a0e1c0e-58e5-4bb1-9d1b-1f6a0b1f2d3c", 0),
      });
      // A later thread pointing at the same Claude session.
      yield* insertThread({
        threadId: "thread:resume:duplicate",
        modelSelectionJson: CLAUDE_MODEL,
        createdAt: "2026-09-05T00:00:00.000Z",
        updatedAt: "2026-09-24T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:duplicate",
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor(claudeSession),
      });
      // A CLI session imported into a fork family, with no runtime row.
      yield* insertThread({
        threadId: "thread:resume:family",
        modelSelectionJson: CODEX_MODEL,
        createdAt: "2026-09-06T00:00:00.000Z",
        updatedAt: "2026-09-25T10:00:00.000Z",
        forkFamilyId: " 9fc38739-family ",
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 6,
        importedMessageCount: 2,
      });

      const claudeProjection = yield* projections.getThreadProjection(
        ThreadId.make("thread:resume:claude"),
      );
      const claudeProviderThreadId = deriveProviderThread({
        driver: claude,
        nativeThreadId: claudeSession,
      });
      assert.equal(claudeProjection.thread.activeProviderThreadId, claudeProviderThreadId);
      assert.equal(claudeProjection.thread.historyOrigin, "v1_import");
      assert.lengthOf(claudeProjection.providerThreads, 1);
      const claudeProviderThread = claudeProjection.providerThreads[0]!;
      assert.deepInclude(claudeProviderThread, {
        id: claudeProviderThreadId,
        driver: claude,
        providerInstanceId: ProviderInstanceId.make("claudeAgent"),
        providerSessionId: null,
        appThreadId: claudeProjection.thread.id,
        status: "idle",
        contextUsage: null,
      });
      assert.deepStrictEqual(claudeProviderThread.nativeThreadRef, {
        driver: claude,
        nativeId: claudeSession,
        strength: "strong",
      });
      // Migration does not move the thread in the sidebar.
      const v1UpdatedAt = DateTime.makeUnsafe("2026-09-20T10:00:00.000Z");
      assert.deepEqual(claudeProjection.thread.updatedAt, v1UpdatedAt);
      assert.deepEqual(claudeProviderThread.updatedAt, v1UpdatedAt);
      assert.deepEqual(
        claudeProviderThread.createdAt,
        DateTime.makeUnsafe("2026-09-01T00:00:00.000Z"),
      );
      const shell = (yield* projections.getShellSnapshot()).threads.find(
        (thread) => thread.id === "thread:resume:claude",
      );
      assert.equal(shell?.activeProviderThreadId, claudeProviderThreadId);
      assert.deepEqual(shell?.updatedAt, v1UpdatedAt);
      assert.include(
        yield* threadEventIds("thread:resume:claude"),
        "migration:v1:thread:thread:resume:claude:provider-thread",
      );
      // No tool history: messages stay on upstream's lazy transcript import.
      assert.isNull((yield* legacyImportRow("thread:resume:claude"))?.transcript_imported_at);

      const codexProjection = yield* projections.getThreadProjection(
        ThreadId.make("thread:resume:codex"),
      );
      assert.equal(
        codexProjection.thread.activeProviderThreadId,
        deriveProviderThread({ driver: codex, nativeThreadId: codexThread }),
      );
      assert.deepInclude(codexProjection.providerThreads[0], {
        providerInstanceId: ProviderInstanceId.make("codex"),
        nativeThreadRef: { driver: codex, nativeId: codexThread, strength: "strong" },
      });

      for (const threadId of [
        "thread:resume:other-instance",
        "thread:resume:no-turns",
        "thread:resume:duplicate",
        "thread:resume:family",
      ]) {
        const projection = yield* projections.getThreadProjection(ThreadId.make(threadId));
        assert.isNull(projection.thread.activeProviderThreadId, threadId);
        assert.deepStrictEqual(projection.providerThreads, [], threadId);
      }

      const family = yield* projections.getThreadProjection(ThreadId.make("thread:resume:family"));
      assert.equal(family.thread.forkFamilyId, "9fc38739-family");
      const familyShell = (yield* projections.getShellSnapshot()).threads.find(
        (thread) => thread.id === "thread:resume:family",
      );
      assert.equal(familyShell?.forkFamilyId, "9fc38739-family");
      assert.isUndefined(claudeProjection.thread.forkFamilyId);

      // A v1 thread found on a later startup cannot take over a session that
      // an earlier import already resumed.
      yield* insertThread({
        threadId: "thread:resume:late-duplicate",
        modelSelectionJson: CLAUDE_MODEL,
        createdAt: "2026-09-07T00:00:00.000Z",
        updatedAt: "2026-09-26T10:00:00.000Z",
      });
      yield* insertRuntime({
        threadId: "thread:resume:late-duplicate",
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor(claudeSession),
      });
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      const late = yield* projections.getThreadProjection(
        ThreadId.make("thread:resume:late-duplicate"),
      );
      assert.isNull(late.thread.activeProviderThreadId);
      const owners = yield* sql<{ readonly thread_id: string }>`
        SELECT thread_id FROM orchestration_v2_projection_provider_threads
        WHERE provider_thread_id = ${claudeProviderThreadId}
      `;
      assert.deepStrictEqual(owners, [{ thread_id: "thread:resume:claude" }]);

      assert.isTrue((yield* maintenance.verify).valid);
      assert.isTrue((yield* maintenance.rebuild).valid);
      const replayed = yield* projections.getThreadProjection(
        ThreadId.make("thread:resume:claude"),
      );
      assert.deepStrictEqual(replayed.thread, claudeProjection.thread);
      assert.deepStrictEqual(replayed.providerThreads, claudeProjection.providerThreads);
    }),
  );

  it.effect("imports a thread's v1 tool history with its messages as one timeline", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("thread:history:claude");
      const t = (seconds: number) => isoAt(Date.UTC(2026, 8, 10, 12, 0, 0) + seconds * 1_000);
      const bash = {
        toolName: "Bash",
        input: { command: "ls -la release", description: "Confirm rebuilt artifacts exist" },
        result: {
          tool_use_id: "toolu_01UNbfUnGU4uXLL1vZAGtf9A",
          type: "tool_result",
          content: "236609938 release/T3-Code-0.0.30-arm64.dmg",
          is_error: false,
        },
      };

      yield* insertProject;
      yield* insertThread({
        threadId,
        modelSelectionJson: CLAUDE_MODEL,
        createdAt: t(0),
        updatedAt: t(40),
      });
      yield* insertRuntime({
        threadId,
        providerName: "claudeAgent",
        instanceId: "claudeAgent",
        cursorJson: claudeCursor("4b6f1d2e-8c3a-4f5b-9e7d-0a1b2c3d4e5f"),
      });
      const messages = [
        ["message:history:1", "user", "Check the release artifacts", 0],
        ["message:history:2", "assistant", "Both artifacts exist.", 10],
        ["message:history:3", "user", "Now track the project flag in the parser", 20],
        ["message:history:4", "assistant", "Done.", 30],
      ] as const;
      for (const [messageId, role, text, seconds] of messages) {
        yield* insertMessage({ messageId, threadId, role, text, createdAt: t(seconds) });
      }
      const activities = [
        {
          activityId: "activity:history:bash:started",
          kind: "tool.started",
          summary: "Command run started",
          payload: { itemType: "command_execution", detail: "Bash: {}" },
          createdAt: t(1),
        },
        {
          activityId: "activity:history:bash:updated",
          kind: "tool.updated",
          summary: "Command run",
          payload: {
            itemType: "command_execution",
            status: "inProgress",
            detail: "Bash: ls -la release",
            data: bash,
          },
          createdAt: t(2),
        },
        {
          activityId: "activity:history:bash:completed",
          kind: "tool.completed",
          summary: "Command run",
          payload: { itemType: "command_execution", detail: "Bash: ls -la release", data: bash },
          createdAt: t(3),
        },
        {
          activityId: "activity:history:context:1",
          kind: "context-window.updated",
          summary: "Context window updated",
          tone: "info",
          payload: { usedTokens: 120_000, maxTokens: 1_000_000 },
          createdAt: t(4),
        },
        {
          activityId: "activity:history:edit",
          kind: "tool.completed",
          summary: "File change",
          payload: {
            itemType: "file_change",
            detail: 'Edit: {"file_path":"/repo/src/parser.ts"}',
            data: {
              toolName: "Edit",
              input: {
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
          createdAt: t(21),
        },
        {
          activityId: "activity:history:plan",
          kind: "turn.plan.updated",
          summary: "Plan updated",
          tone: "info",
          payload: {
            plan: [
              { step: "Read the parser", status: "completed" },
              { step: "Track the project flag", status: "inProgress" },
            ],
          },
          createdAt: t(22),
        },
        {
          activityId: "activity:history:error",
          kind: "runtime.error",
          summary: "Runtime error",
          tone: "error",
          payload: { message: "[ede_diagnostic] result_type=user stop_reason=null" },
          createdAt: t(23),
        },
        {
          activityId: "activity:history:warning",
          kind: "runtime.warning",
          summary: "Runtime warning",
          tone: "info",
          payload: { message: "Unhandled Claude system message subtype 'task_updated'." },
          createdAt: t(24),
        },
        {
          activityId: "activity:history:task:started",
          kind: "task.started",
          summary: "local_agent task started",
          tone: "info",
          payload: {
            taskId: "a64ef6081fccf9359",
            taskType: "local_agent",
            title: "Check other parsers",
            detail: "Check other parsers",
          },
          createdAt: t(25),
        },
        {
          activityId: "activity:history:task:completed",
          kind: "task.completed",
          summary: "Task completed",
          tone: "info",
          payload: {
            taskId: "a64ef6081fccf9359",
            status: "completed",
            title: "Check other parsers",
            detail: "No other parser reads the flag.",
          },
          createdAt: t(27),
        },
        {
          activityId: "activity:history:context:2",
          kind: "context-window.updated",
          summary: "Context window updated",
          tone: "info",
          payload: {
            usedTokens: 316_845,
            lastUsedTokens: 316_845,
            inputTokens: 313_358,
            outputTokens: 3_487,
            maxTokens: 1_000_000,
          },
          createdAt: t(28),
        },
        {
          activityId: "activity:history:checkpoint",
          kind: "checkpoint.captured",
          summary: "Checkpoint captured",
          tone: "info",
          payload: { turnCount: 1, status: "completed" },
          createdAt: t(29),
        },
      ];
      for (const activity of activities) yield* insertActivity({ threadId, ...activity });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 4,
      });

      const projection = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(timelineShape(projection.turnItems), [
        ["user_message", 1, "message:history:1"],
        ["command_execution", 2],
        ["assistant_message", 3, "message:history:2"],
        ["user_message", 4, "message:history:3"],
        ["file_change", 5],
        ["todo_list", 6],
        ["error", 7],
        ["system_notice", 8],
        ["dynamic_tool", 9],
        ["assistant_message", 10, "message:history:4"],
      ]);
      for (const item of projection.turnItems) {
        assert.isNull(item.runId);
        assert.isNull(item.providerThreadId);
      }
      assert.deepStrictEqual(
        projection.messages.map((message) => message.id),
        messages.map(([messageId]) => MessageId.make(messageId)),
      );
      const command = projection.turnItems.find((item) => item.type === "command_execution");
      assert.deepInclude(command, {
        id: TurnItemId.make("migration:v1:item:activity:history:bash:updated"),
        status: "completed",
        input: "ls -la release",
        output: "236609938 release/T3-Code-0.0.30-arm64.dmg",
      });

      // The thread's last v1 context reading feeds the context meter.
      assert.deepStrictEqual(projection.providerThreads[0]?.contextUsage, {
        usedTokens: 316_845,
        lastUsedTokens: 316_845,
        inputTokens: 313_358,
        outputTokens: 3_487,
        maxTokens: 1_000_000,
      });

      // The whole transcript arrived with the shell: nothing is left to hydrate.
      const imported = yield* legacyImportRow(threadId);
      assert.isNotNull(imported?.transcript_imported_at);
      assert.equal(imported?.imported_message_count, 4);
      const eventsBefore = yield* threadEventIds(threadId);
      assert.deepStrictEqual(yield* importer.ensureTranscript(threadId), {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 0,
        importedMessageCount: 0,
      });
      assert.deepStrictEqual(yield* threadEventIds(threadId), eventsBefore);

      assert.isTrue((yield* maintenance.verify).valid);
      assert.isTrue((yield* maintenance.rebuild).valid);
      const replayed = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(byOrdinal(replayed.turnItems), byOrdinal(projection.turnItems));
    }),
  );

  it.effect("takes a wuwei thread's history from wuwei's own session file", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const threadId = ThreadId.make("thread:wuwei:session-file");

      yield* insertProject;
      yield* insertThread({
        threadId,
        modelSelectionJson: WUWEI_MODEL,
        createdAt: wuweiIso(0),
        updatedAt: wuweiIso(60_000),
      });
      yield* insertRuntime({
        threadId,
        providerName: "wuwei",
        instanceId: "wuwei",
        cursorJson: wuweiCursor(WUWEI_SESSION),
      });
      // v1's copy of the same chat, without the thinking: superseded by the session file.
      yield* insertMessage({
        messageId: "message:wuwei:v1:1",
        threadId,
        role: "user",
        text: "What's the date?",
        createdAt: wuweiIso(0),
      });
      yield* insertActivity({
        activityId: "activity:wuwei:v1:tool",
        threadId,
        kind: "tool.completed",
        summary: "Ran command",
        payload: {
          itemType: "command_execution",
          toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
          status: "completed",
          title: "Ran command",
          detail: "date",
          data: {
            toolCallId: "toolu_01T8Bvra4XMgKC6FLVFZLXeY",
            kind: "execute",
            rawInput: { command: "date" },
          },
        },
        createdAt: wuweiIso(2_000),
      });
      yield* insertActivity({
        activityId: "activity:wuwei:v1:context",
        threadId,
        kind: "context-window.updated",
        summary: "Context window updated",
        tone: "info",
        payload: {
          usedTokens: 4_508,
          maxTokens: 372_000,
          inputTokens: 4_503,
          cachedInputTokens: 0,
          outputTokens: 5,
          lastUsedTokens: 4_508,
          lastInputTokens: 4_503,
          lastCachedInputTokens: 0,
          lastOutputTokens: 5,
        },
        createdAt: wuweiIso(14_000),
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 5,
      });

      const projection = yield* projections.getThreadProjection(threadId);
      const prefix = `wuwei:${WUWEI_SESSION}`;
      assert.deepStrictEqual(timelineShape(projection.turnItems), [
        ["user_message", 1, `${prefix}:u1`],
        ["reasoning", 2],
        ["command_execution", 3],
        ["assistant_message", 4, `${prefix}:a4.text.0:2`],
        ["user_message", 5, `${prefix}:u5`],
        ["file_change", 6],
        ["user_message", 7, `${prefix}:u7:1`],
        ["assistant_message", 8, `${prefix}:a8.text.0:2`],
      ]);
      assert.deepStrictEqual(
        projection.messages.map((message) => [message.role, message.text]),
        [
          ["user", "What's the date?"],
          ["assistant", "It is Sunday."],
          ["user", "Update memory with this\n\n[1 image]"],
          ["user", "also note the date"],
          ["assistant", "Noted."],
        ],
      );
      const items = byOrdinal(projection.turnItems);
      assert.deepInclude(items[1], { type: "reasoning", text: "Run date." });
      assert.deepInclude(items[2], {
        type: "command_execution",
        status: "completed",
        input: "date",
        output: "Sun Oct  4 05:32:46 PDT 2026",
      });
      assert.deepInclude(items[5], {
        type: "file_change",
        status: "failed",
        fileName: "/Users/me/.wuwei/t3/memory.md",
      });

      assert.deepStrictEqual(projection.providerThreads[0]?.nativeThreadRef, {
        driver: wuwei,
        nativeId: WUWEI_SESSION,
        strength: "strong",
      });
      assert.equal(projection.providerThreads[0]?.contextUsage?.usedTokens, 4_508);
      assert.isNotNull((yield* legacyImportRow(threadId))?.transcript_imported_at);
      assert.isTrue((yield* maintenance.verify).valid);
    }),
  );

  it.effect("falls back to v1 messages and activities when wuwei cannot read the chat", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const maintenance = yield* ProjectionMaintenance.ProjectionMaintenanceV2;
      const withHistory = ThreadId.make("thread:wuwei:gone");
      const withoutHistory = ThreadId.make("thread:wuwei:gone-quiet");
      const t = (seconds: number) => isoAt(Date.UTC(2026, 8, 24, 18, 12, 0) + seconds * 1_000);

      yield* insertProject;
      for (const [threadId, session] of [
        [withHistory, "2026-09-24-18-12-00-gone"],
        [withoutHistory, "2026-09-25-09-00-00-gone"],
      ] as const) {
        yield* insertThread({
          threadId,
          modelSelectionJson: WUWEI_MODEL,
          createdAt: t(0),
          updatedAt: t(60),
        });
        yield* insertRuntime({
          threadId,
          providerName: "wuwei",
          instanceId: "wuwei",
          cursorJson: wuweiCursor(session),
        });
        yield* insertMessage({
          messageId: `${threadId}:message:1`,
          threadId,
          role: "user",
          text: "Run a quick command",
          createdAt: t(0),
        });
        yield* insertMessage({
          messageId: `${threadId}:message:2`,
          threadId,
          role: "assistant",
          text: "Ran `date && uptime`.",
          createdAt: t(10),
        });
      }
      yield* insertActivity({
        activityId: "activity:gone:dream",
        threadId: withHistory,
        kind: "task.progress",
        summary: "Thinking",
        tone: "info",
        payload: {
          taskId: "wuwei-thinking:5c632ac0:1",
          title: "Thinking",
          detail: "A harmless request; a quick shell command will do.",
          agentKind: "background",
          taskType: "dream",
        },
        createdAt: t(1),
      });
      yield* insertActivity({
        activityId: "activity:gone:tool",
        threadId: withHistory,
        kind: "tool.completed",
        summary: "Ran command",
        payload: {
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
            content: [
              {
                type: "content",
                content: { type: "text", text: "Thu Sep 24 18:12:30 PDT 2026\n" },
              },
            ],
          },
        },
        createdAt: t(2),
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 2,
        importedMessageCount: 4,
      });

      const projection = yield* projections.getThreadProjection(withHistory);
      assert.deepStrictEqual(timelineShape(projection.turnItems), [
        ["user_message", 1, `${withHistory}:message:1`],
        ["reasoning", 2],
        ["command_execution", 3],
        ["assistant_message", 4, `${withHistory}:message:2`],
      ]);
      // The native ref still carries over; a missing chat falls back on the first turn.
      assert.equal(
        projection.thread.activeProviderThreadId,
        deriveProviderThread({ driver: wuwei, nativeThreadId: "2026-09-24-18-12-00-gone" }),
      );

      // Without v1 activities the thread keeps upstream's lazy message import.
      assert.isNull((yield* legacyImportRow(withoutHistory))?.transcript_imported_at);
      assert.deepStrictEqual(yield* importer.ensureTranscript(withoutHistory), {
        importedThreadCount: 1,
        importedMessageCount: 0,
      });
      assert.lengthOf((yield* projections.getThreadProjection(withoutHistory)).messages, 2);
      assert.isTrue((yield* maintenance.verify).valid);
    }),
  );
});
