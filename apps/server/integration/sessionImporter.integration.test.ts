import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  CommandId,
  defaultInstanceIdForDriver,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
} from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";

import { importedThreadId, makeSessionImporter } from "../src/import/Layers/SessionImporter.ts";
import { OrchestrationEngineService } from "../src/orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../src/orchestration/Services/ProjectionSnapshotQuery.ts";
import { ProviderSessionDirectory } from "../src/provider/Services/ProviderSessionDirectory.ts";
import { ServerSettingsService } from "../src/serverSettings.ts";
import {
  makeOrchestrationIntegrationHarness,
  type OrchestrationIntegrationHarness,
} from "./OrchestrationEngineHarness.integration.ts";

const CLAUDE_PROVIDER = ProviderDriverKind.make("claudeAgent");
const CODEX_PROVIDER = ProviderDriverKind.make("codex");

const CLAUDE_SESSION_ID = "aaaaaaaa-1111-4111-8111-111111111111";
const CLAUDE_INELIGIBLE_SESSION_ID = "bbbbbbbb-1111-4111-8111-111111111111";
const CLAUDE_FOREIGN_SESSION_ID = "dddddddd-1111-4111-8111-111111111111";
const CODEX_SESSION_ID = "cccccccc-2222-4222-8222-222222222222";

function jsonl(entries: ReadonlyArray<Record<string, unknown>>): string {
  return entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n";
}

function claudeUserEntry(input: {
  readonly uuid: string;
  readonly text: string;
  readonly timestamp: string;
  readonly cwd: string;
  readonly sessionId?: string;
}): Record<string, unknown> {
  return {
    type: "user",
    uuid: input.uuid,
    parentUuid: null,
    sessionId: input.sessionId ?? CLAUDE_SESSION_ID,
    cwd: input.cwd,
    timestamp: input.timestamp,
    isSidechain: false,
    message: { role: "user", content: input.text },
  };
}

function claudeAssistantEntry(input: {
  readonly uuid: string;
  readonly text: string;
  readonly timestamp: string;
  readonly cwd: string;
  readonly sessionId?: string;
}): Record<string, unknown> {
  return {
    type: "assistant",
    uuid: input.uuid,
    parentUuid: null,
    sessionId: input.sessionId ?? CLAUDE_SESSION_ID,
    cwd: input.cwd,
    timestamp: input.timestamp,
    isSidechain: false,
    message: {
      role: "assistant",
      content: [{ type: "text", text: input.text }],
    },
  };
}

function codexEventMsg(input: {
  readonly type: "user_message" | "agent_message";
  readonly text: string;
  readonly timestamp: string;
}): Record<string, unknown> {
  return {
    timestamp: input.timestamp,
    type: "event_msg",
    payload: { type: input.type, message: input.text },
  };
}

function withHarness<A, E>(
  use: (
    harness: OrchestrationIntegrationHarness,
  ) => Effect.Effect<A, E, FileSystem.FileSystem | Path.Path>,
) {
  return Effect.acquireUseRelease(
    makeOrchestrationIntegrationHarness({ provider: CODEX_PROVIDER }),
    use,
    (harness) => harness.dispose,
  ).pipe(Effect.provide(NodeServices.layer));
}

const makeImporter = (input: {
  readonly harness: OrchestrationIntegrationHarness;
  readonly claudeProjectsDir: string;
  readonly codexSessionsDir: string;
}) =>
  makeSessionImporter({
    claudeProjectsDir: input.claudeProjectsDir,
    codexSessionsDir: input.codexSessionsDir,
    // Fixtures are written immediately before sweeping.
    settleMs: 0,
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        Layer.succeed(OrchestrationEngineService, input.harness.engine),
        Layer.succeed(ProjectionSnapshotQuery, input.harness.snapshotQuery),
        Layer.succeed(ProviderSessionDirectory, input.harness.sessionDirectory),
        ServerSettingsService.layerTest(),
      ),
    ),
  );

const countPersistedEvents = (harness: OrchestrationIntegrationHarness) =>
  Stream.runCollect(harness.engine.readEvents(0, 100_000)).pipe(
    Effect.map((chunk) => Array.from(chunk).length),
  );

it.live(
  "imports claude and codex CLI sessions as threads with bindings, idempotently and incrementally",
  () =>
    withHarness((harness) =>
      Effect.gen(function* () {
        const fileSystem = yield* FileSystem.FileSystem;
        const path = yield* Path.Path;

        const claudeProjectsDir = path.join(harness.rootDir, "claude-home", "projects");
        const claudeProjectDir = path.join(claudeProjectsDir, "-repo-claude");
        const codexSessionsDir = path.join(harness.rootDir, "codex-home", "sessions");
        const codexDayDir = path.join(codexSessionsDir, "2026", "07", "29");
        const codexWorkspaceDir = path.join(harness.rootDir, "codex-workspace");
        yield* fileSystem.makeDirectory(claudeProjectDir, { recursive: true });
        yield* fileSystem.makeDirectory(codexDayDir, { recursive: true });
        yield* fileSystem.makeDirectory(codexWorkspaceDir, { recursive: true });

        const claudeCwd = harness.workspaceDir;
        const claudeSessionPath = path.join(claudeProjectDir, `${CLAUDE_SESSION_ID}.jsonl`);
        yield* fileSystem.writeFileString(
          claudeSessionPath,
          jsonl([
            { type: "mode", mode: "default" },
            claudeUserEntry({
              uuid: "user-1",
              text: "Fix the login bug please",
              timestamp: "2026-07-01T10:00:00.000Z",
              cwd: claudeCwd,
            }),
            claudeAssistantEntry({
              uuid: "assistant-1",
              text: "I fixed the login bug.",
              timestamp: "2026-07-01T10:00:05.000Z",
              cwd: claudeCwd,
            }),
            {
              ...claudeUserEntry({
                uuid: "sidechain-1",
                text: "sidechain noise",
                timestamp: "2026-07-01T10:00:06.000Z",
                cwd: claudeCwd,
              }),
              isSidechain: true,
            },
            { type: "ai-title", aiTitle: "Login bug fix" },
          ]),
        );

        // Only a user message: not eligible, must not become a thread.
        yield* fileSystem.writeFileString(
          path.join(claudeProjectDir, `${CLAUDE_INELIGIBLE_SESSION_ID}.jsonl`),
          jsonl([
            claudeUserEntry({
              uuid: "user-1",
              text: "no reply yet",
              timestamp: "2026-07-01T11:00:00.000Z",
              cwd: claudeCwd,
              sessionId: CLAUDE_INELIGIBLE_SESSION_ID,
            }),
          ]),
        );

        // A session already bound to a T3-native thread (no importCursor):
        // the importer must leave it alone entirely.
        const foreignThreadId = ThreadId.make("thread-native-1");
        yield* harness.sessionDirectory.upsert({
          threadId: foreignThreadId,
          provider: CLAUDE_PROVIDER,
          providerInstanceId: defaultInstanceIdForDriver(CLAUDE_PROVIDER),
          status: "stopped",
          runtimeMode: "full-access",
          resumeCursor: {
            threadId: foreignThreadId,
            resume: CLAUDE_FOREIGN_SESSION_ID,
            turnCount: 3,
          },
        });
        yield* fileSystem.writeFileString(
          path.join(claudeProjectDir, `${CLAUDE_FOREIGN_SESSION_ID}.jsonl`),
          jsonl([
            claudeUserEntry({
              uuid: "user-1",
              text: "t3-owned session",
              timestamp: "2026-07-01T12:00:00.000Z",
              cwd: claudeCwd,
              sessionId: CLAUDE_FOREIGN_SESSION_ID,
            }),
            claudeAssistantEntry({
              uuid: "assistant-1",
              text: "reply",
              timestamp: "2026-07-01T12:00:05.000Z",
              cwd: claudeCwd,
              sessionId: CLAUDE_FOREIGN_SESSION_ID,
            }),
          ]),
        );

        const codexSessionPath = path.join(codexDayDir, "rollout-2026-07-29T08-00-00-codex.jsonl");
        yield* fileSystem.writeFileString(
          codexSessionPath,
          jsonl([
            {
              timestamp: "2026-07-02T08:00:00.000Z",
              type: "session_meta",
              payload: {
                id: CODEX_SESSION_ID,
                cwd: codexWorkspaceDir,
                timestamp: "2026-07-02T08:00:00.000Z",
                git: { repository_url: "https://example.com/repo.git", branch: "main" },
              },
            },
            { timestamp: "2026-07-02T08:00:00.500Z", type: "turn_context", payload: {} },
            codexEventMsg({
              type: "user_message",
              text: "Refactor the rollout parser",
              timestamp: "2026-07-02T08:00:01.000Z",
            }),
            codexEventMsg({
              type: "agent_message",
              text: "Parser refactored.",
              timestamp: "2026-07-02T08:00:09.000Z",
            }),
          ]),
        );

        const importer = yield* makeImporter({ harness, claudeProjectsDir, codexSessionsDir });
        yield* importer.sweepNow();

        // Claude thread: title, history with original timestamps, model binding.
        const claudeThreadId = importedThreadId(CLAUDE_SESSION_ID);
        const claudeThread = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(claudeThreadId),
          () => new Error("expected imported claude thread"),
        );
        assert.equal(claudeThread.title, "Login bug fix");
        assert.equal(claudeThread.modelSelection.instanceId, "claudeAgent");
        assert.equal(claudeThread.createdAt, "2026-07-01T10:00:00.000Z");
        assert.deepEqual(
          claudeThread.messages.map((message) => ({
            role: message.role,
            text: message.text,
            streaming: message.streaming,
            createdAt: message.createdAt,
          })),
          [
            {
              role: "user",
              text: "Fix the login bug please",
              streaming: false,
              createdAt: "2026-07-01T10:00:00.000Z",
            },
            {
              role: "assistant",
              text: "I fixed the login bug.",
              streaming: false,
              createdAt: "2026-07-01T10:00:05.000Z",
            },
          ],
        );
        assert.equal(claudeThread.session?.status, "stopped");

        const claudeProject = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getActiveProjectByWorkspaceRoot(claudeCwd),
          () => new Error("expected imported claude project"),
        );
        assert.equal(claudeThread.projectId, claudeProject.id);

        const claudeBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(claudeThreadId),
          () => new Error("expected imported claude binding"),
        );
        assert.equal(claudeBinding.provider, CLAUDE_PROVIDER);
        assert.equal(claudeBinding.providerInstanceId, "claudeAgent");
        assert.equal(claudeBinding.status, "stopped");
        assert.deepEqual(claudeBinding.resumeCursor, {
          threadId: claudeThreadId,
          resume: CLAUDE_SESSION_ID,
          turnCount: 1,
        });
        assert.deepEqual(claudeBinding.runtimePayload, {
          cwd: claudeCwd,
          importCursor: {
            sourceSessionId: CLAUDE_SESSION_ID,
            sourcePath: claudeSessionPath,
            lineCount: 5,
            epoch: 2,
          },
        });

        // Codex thread and binding.
        const codexThreadId = importedThreadId(CODEX_SESSION_ID);
        const codexThread = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(codexThreadId),
          () => new Error("expected imported codex thread"),
        );
        assert.equal(codexThread.title, "Refactor the rollout parser");
        assert.deepEqual(
          codexThread.messages.map((message) => ({ role: message.role, text: message.text })),
          [
            { role: "user", text: "Refactor the rollout parser" },
            { role: "assistant", text: "Parser refactored." },
          ],
        );
        const codexProject = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getActiveProjectByWorkspaceRoot(codexWorkspaceDir),
          () => new Error("expected imported codex project"),
        );
        assert.equal(codexThread.projectId, codexProject.id);
        const codexBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(codexThreadId),
          () => new Error("expected imported codex binding"),
        );
        assert.equal(codexBinding.provider, CODEX_PROVIDER);
        assert.deepEqual(codexBinding.resumeCursor, { threadId: CODEX_SESSION_ID });

        // Ineligible and T3-owned sessions produce no threads; the foreign
        // binding is untouched.
        assert.isTrue(
          Option.isNone(
            yield* harness.snapshotQuery.getThreadDetailById(
              importedThreadId(CLAUDE_INELIGIBLE_SESSION_ID),
            ),
          ),
        );
        assert.isTrue(
          Option.isNone(
            yield* harness.snapshotQuery.getThreadDetailById(
              importedThreadId(CLAUDE_FOREIGN_SESSION_ID),
            ),
          ),
        );
        const foreignBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(foreignThreadId),
          () => new Error("expected untouched foreign binding"),
        );
        assert.deepEqual(foreignBinding.resumeCursor, {
          threadId: foreignThreadId,
          resume: CLAUDE_FOREIGN_SESSION_ID,
          turnCount: 3,
        });
        assert.isTrue(
          foreignBinding.runtimePayload == null ||
            (foreignBinding.runtimePayload as { importCursor?: unknown }).importCursor ===
              undefined,
        );

        // Idempotency: a fresh importer (empty change cache) re-reads every
        // file, and every command dedupes on its receipt — no new events.
        const eventCountAfterImport = yield* countPersistedEvents(harness);
        const secondImporter = yield* makeImporter({
          harness,
          claudeProjectsDir,
          codexSessionsDir,
        });
        yield* secondImporter.sweepNow();
        assert.equal(yield* countPersistedEvents(harness), eventCountAfterImport);

        // Incremental: the CLI appends to both sessions; the next sweep
        // imports only the tail.
        const claudeAppend = jsonl([
          claudeUserEntry({
            uuid: "user-2",
            text: "Now add a regression test",
            timestamp: "2026-07-01T10:05:00.000Z",
            cwd: claudeCwd,
          }),
          claudeAssistantEntry({
            uuid: "assistant-2",
            text: "Added a regression test.",
            timestamp: "2026-07-01T10:05:30.000Z",
            cwd: claudeCwd,
          }),
        ]);
        const existingClaudeContent = yield* fileSystem.readFileString(claudeSessionPath);
        yield* fileSystem.writeFileString(claudeSessionPath, existingClaudeContent + claudeAppend);
        const codexAppend = jsonl([
          codexEventMsg({
            type: "agent_message",
            text: "Anything else?",
            timestamp: "2026-07-02T08:01:00.000Z",
          }),
        ]);
        const existingCodexContent = yield* fileSystem.readFileString(codexSessionPath);
        yield* fileSystem.writeFileString(codexSessionPath, existingCodexContent + codexAppend);

        yield* importer.sweepNow();

        const claudeThreadAfter = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(claudeThreadId),
          () => new Error("expected claude thread after increment"),
        );
        assert.deepEqual(
          claudeThreadAfter.messages.map((message) => message.text),
          [
            "Fix the login bug please",
            "I fixed the login bug.",
            "Now add a regression test",
            "Added a regression test.",
          ],
        );
        const claudeBindingAfter = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(claudeThreadId),
          () => new Error("expected claude binding after increment"),
        );
        assert.deepEqual(claudeBindingAfter.resumeCursor, {
          threadId: claudeThreadId,
          resume: CLAUDE_SESSION_ID,
          turnCount: 2,
        });
        assert.deepEqual(claudeBindingAfter.runtimePayload, {
          cwd: claudeCwd,
          importCursor: {
            sourceSessionId: CLAUDE_SESSION_ID,
            sourcePath: claudeSessionPath,
            lineCount: 7,
            epoch: 2,
          },
        });

        const codexThreadAfter = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(codexThreadId),
          () => new Error("expected codex thread after increment"),
        );
        assert.deepEqual(
          codexThreadAfter.messages.map((message) => message.text),
          ["Refactor the rollout parser", "Parser refactored.", "Anything else?"],
        );

        // A no-change sweep after the increments stays quiet too.
        const eventCountAfterIncrement = yield* countPersistedEvents(harness);
        yield* importer.sweepNow();
        assert.equal(yield* countPersistedEvents(harness), eventCountAfterIncrement);

        // Adoption: once a native T3 session runs on an imported thread, the
        // importer hands the thread over — the cursor is stripped and later
        // source-file growth is never re-imported (native turns would
        // otherwise appear twice).
        yield* harness.sessionDirectory.upsert({
          threadId: claudeThreadId,
          provider: CLAUDE_PROVIDER,
          providerInstanceId: defaultInstanceIdForDriver(CLAUDE_PROVIDER),
          status: "running",
          runtimePayload: { model: "claude-fable-5", activeTurnId: "turn-adopted" },
        });
        const adoptionAppend = jsonl([
          claudeUserEntry({
            uuid: "user-3",
            text: "This turn came through native T3 chat",
            timestamp: "2026-07-01T10:10:00.000Z",
            cwd: claudeCwd,
          }),
          claudeAssistantEntry({
            uuid: "assistant-3",
            text: "Replying natively.",
            timestamp: "2026-07-01T10:10:30.000Z",
            cwd: claudeCwd,
          }),
        ]);
        const contentBeforeAdoption = yield* fileSystem.readFileString(claudeSessionPath);
        yield* fileSystem.writeFileString(
          claudeSessionPath,
          contentBeforeAdoption + adoptionAppend,
        );
        yield* importer.sweepNow();

        const adoptedThread = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(claudeThreadId),
          () => new Error("expected claude thread after adoption"),
        );
        assert.equal(adoptedThread.messages.length, 4);
        const adoptedBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(claudeThreadId),
          () => new Error("expected claude binding after adoption"),
        );
        const adoptedPayload = adoptedBinding.runtimePayload as Record<string, unknown>;
        assert.equal(adoptedPayload.importCursor, null);
        assert.equal(adoptedBinding.status, "running");

        // The handoff is permanent: further growth imports nothing.
        const contentAfterAdoption = yield* fileSystem.readFileString(claudeSessionPath);
        yield* fileSystem.writeFileString(
          claudeSessionPath,
          contentAfterAdoption +
            jsonl([
              claudeUserEntry({
                uuid: "user-4",
                text: "One more terminal message",
                timestamp: "2026-07-01T10:15:00.000Z",
                cwd: claudeCwd,
              }),
            ]),
        );
        const eventCountAfterAdoption = yield* countPersistedEvents(harness);
        yield* importer.sweepNow();
        assert.equal(yield* countPersistedEvents(harness), eventCountAfterAdoption);

        // Project-less codex chats (no git metadata) group into one shared
        // "Chats" project rooted at the codex home.
        const chatSessionId = "eeeeeeee-3333-4333-8333-333333333333";
        yield* fileSystem.writeFileString(
          path.join(codexDayDir, "rollout-2026-07-29T09-00-00-chat.jsonl"),
          jsonl([
            {
              timestamp: "2026-07-03T09:00:00.000Z",
              type: "session_meta",
              payload: {
                id: chatSessionId,
                cwd: path.join(harness.rootDir, "some-throwaway-cwd"),
                timestamp: "2026-07-03T09:00:00.000Z",
              },
            },
            codexEventMsg({
              type: "user_message",
              text: "Quick question",
              timestamp: "2026-07-03T09:00:01.000Z",
            }),
            codexEventMsg({
              type: "agent_message",
              text: "Quick answer.",
              timestamp: "2026-07-03T09:00:05.000Z",
            }),
          ]),
        );
        yield* importer.sweepNow();
        const chatsRoot = path.dirname(codexSessionsDir);
        const chatsProject = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getActiveProjectByWorkspaceRoot(chatsRoot),
          () => new Error("expected shared Chats project"),
        );
        const chatShell = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadShellById(importedThreadId(chatSessionId)),
          () => new Error("expected chat thread"),
        );
        assert.equal(chatShell.projectId, chatsProject.id);

        // Epoch migration: a chat imported under the old one-project-per-cwd
        // layout is moved into Chats and its per-chat project is deleted.
        const legacySessionId = "ffffffff-4444-4444-8444-444444444444";
        // Hand-rolled epoch-1 id: the legacy layout predates epoch scoping.
        const legacyThreadId = ThreadId.make(`cli-import-${legacySessionId}`);
        const legacyCwd = path.join(harness.rootDir, "legacy-chat-cwd");
        yield* fileSystem.makeDirectory(legacyCwd, { recursive: true });
        const legacyProjectId = ProjectId.make("cli-import-project-legacy-junk");
        yield* harness.engine.dispatch({
          type: "project.create",
          commandId: CommandId.make("test-legacy-project-create"),
          projectId: legacyProjectId,
          title: "legacy-chat-cwd",
          workspaceRoot: legacyCwd,
          createdAt: "2026-07-04T10:00:00.000Z",
        });
        yield* harness.engine.dispatch({
          type: "thread.create",
          commandId: CommandId.make("test-legacy-thread-create"),
          threadId: legacyThreadId,
          projectId: legacyProjectId,
          title: "Legacy chat",
          modelSelection: {
            instanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
            model: "gpt-5.6-sol",
          },
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: null,
          createdAt: "2026-07-04T10:00:00.000Z",
        });
        const legacyPath = path.join(codexDayDir, "rollout-2026-07-29T10-00-00-legacy.jsonl");
        const legacyContent = jsonl([
          {
            timestamp: "2026-07-04T10:00:00.000Z",
            type: "session_meta",
            payload: {
              id: legacySessionId,
              cwd: legacyCwd,
              timestamp: "2026-07-04T10:00:00.000Z",
            },
          },
          codexEventMsg({
            type: "user_message",
            text: "Legacy chat question",
            timestamp: "2026-07-04T10:00:01.000Z",
          }),
          codexEventMsg({
            type: "agent_message",
            text: "Legacy chat answer.",
            timestamp: "2026-07-04T10:00:05.000Z",
          }),
        ]);
        yield* fileSystem.writeFileString(legacyPath, legacyContent);
        yield* harness.sessionDirectory.upsert({
          threadId: legacyThreadId,
          provider: CODEX_PROVIDER,
          providerInstanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
          status: "stopped",
          runtimeMode: "full-access",
          resumeCursor: { threadId: legacySessionId },
          runtimePayload: {
            cwd: legacyCwd,
            // Epoch field deliberately absent: legacy layout marker.
            importCursor: {
              sourceSessionId: legacySessionId,
              sourcePath: legacyPath,
              lineCount: legacyContent.split("\n").length - 1,
            },
          },
        });
        yield* importer.sweepNow();
        // The migrated thread lives under a fresh epoch-scoped id; the old
        // thread is deleted and its binding tombstoned.
        const migratedThreadId = importedThreadId(legacySessionId);
        const migratedShell = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadShellById(migratedThreadId),
          () => new Error("expected migrated chat thread"),
        );
        assert.equal(migratedShell.projectId, chatsProject.id);
        assert.isTrue(
          Option.isNone(yield* harness.snapshotQuery.getThreadShellById(legacyThreadId)),
          "legacy thread should be deleted after migration",
        );
        assert.isTrue(
          Option.isNone(yield* harness.snapshotQuery.getActiveProjectByWorkspaceRoot(legacyCwd)),
          "per-chat legacy project should be deleted after migration",
        );
        const migratedThread = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(migratedThreadId),
          () => new Error("expected migrated thread detail"),
        );
        assert.deepEqual(
          migratedThread.messages.map((message) => message.text),
          ["Legacy chat question", "Legacy chat answer."],
        );
        const migratedBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(migratedThreadId),
          () => new Error("expected migrated binding"),
        );
        const migratedPayload = migratedBinding.runtimePayload as {
          importCursor?: { epoch?: number };
        };
        assert.equal(migratedPayload.importCursor?.epoch, 2);

        // Provider lifecycle sweeps merge `activeTurnId: null` into every
        // binding on app quit. That must NOT read as adoption: imports keep
        // flowing afterwards.
        yield* harness.sessionDirectory.upsert({
          threadId: codexThreadId,
          provider: CODEX_PROVIDER,
          providerInstanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
          status: "stopped",
          runtimePayload: { activeTurnId: null, lastRuntimeEvent: "provider.stopAll" },
        });
        const codexContentBefore = yield* fileSystem.readFileString(codexSessionPath);
        yield* fileSystem.writeFileString(
          codexSessionPath,
          codexContentBefore +
            jsonl([
              codexEventMsg({
                type: "user_message",
                text: "After a quit-cycle sweep",
                timestamp: "2026-07-05T08:00:00.000Z",
              }),
            ]),
        );
        yield* importer.sweepNow();
        const codexAfterLifecycle = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(codexThreadId),
          () => new Error("expected codex thread after lifecycle sweep"),
        );
        assert.include(
          codexAfterLifecycle.messages.map((message) => message.text),
          "After a quit-cycle sweep",
          "lifecycle-contaminated binding must not be treated as adopted",
        );

        // Self-heal: a cursor falsely stripped (importCursor: null) on a
        // binding with no real native activity gets reconstructed at the
        // current file position, and later growth imports again.
        yield* harness.sessionDirectory.upsert({
          threadId: codexThreadId,
          provider: CODEX_PROVIDER,
          providerInstanceId: defaultInstanceIdForDriver(CODEX_PROVIDER),
          status: "stopped",
          runtimePayload: { importCursor: null },
        });
        const codexContentStripped = yield* fileSystem.readFileString(codexSessionPath);
        yield* fileSystem.writeFileString(
          codexSessionPath,
          codexContentStripped +
            jsonl([
              codexEventMsg({
                type: "agent_message",
                text: "Lost while cursor was stripped",
                timestamp: "2026-07-05T09:00:00.000Z",
              }),
            ]),
        );
        yield* importer.sweepNow();
        const repairedBinding = Option.getOrThrowWith(
          yield* harness.sessionDirectory.getBinding(codexThreadId),
          () => new Error("expected repaired binding"),
        );
        const repairedPayload = repairedBinding.runtimePayload as {
          importCursor?: { epoch?: number; lineCount?: number };
        };
        assert.equal(repairedPayload.importCursor?.epoch, 2, "repaired cursor carries epoch");
        // Growth after the repair imports again.
        const codexContentRepaired = yield* fileSystem.readFileString(codexSessionPath);
        yield* fileSystem.writeFileString(
          codexSessionPath,
          codexContentRepaired +
            jsonl([
              codexEventMsg({
                type: "user_message",
                text: "Back in business",
                timestamp: "2026-07-05T10:00:00.000Z",
              }),
            ]),
        );
        yield* importer.sweepNow();
        const codexAfterRepair = Option.getOrThrowWith(
          yield* harness.snapshotQuery.getThreadDetailById(codexThreadId),
          () => new Error("expected codex thread after repair"),
        );
        assert.include(
          codexAfterRepair.messages.map((message) => message.text),
          "Back in business",
          "imports must resume after cursor repair",
        );
      }),
    ),
);
