/**
 * Acceptance test for the CLI-session importer on the v2 event log.
 *
 * Every scenario runs the real importer against real temp-dir Claude/Codex
 * transcript trees and a fresh in-memory v2 runtime (event sink, projections,
 * ProjectService, ThreadManagementService, provider_session_runtime), and
 * asserts on what the app would read back.
 */
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import {
  CommandId,
  defaultInstanceIdForDriver,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { isOrchestrationV2TurnItemVisible } from "@t3tools/shared/orchestrationV2Timeline";

import * as CheckpointStore from "../src/checkpointing/CheckpointStore.ts";
import * as ServerConfig from "../src/config.ts";
import * as GitWorkflow from "../src/git/GitWorkflowService.ts";
import {
  importedThreadId,
  makeSessionImporter,
  type SessionImporterLiveOptions,
} from "../src/import/Layers/SessionImporter.ts";
import * as McpSessionRegistryTestkit from "../src/mcp/McpSessionRegistry.testkit.ts";
import * as EventSink from "../src/orchestration-v2/EventSink.ts";
import { deriveProviderThread } from "../src/orchestration-v2/IdAllocator.ts";
import * as ProjectStore from "../src/orchestration-v2/ProjectStore.ts";
import * as RuntimeLayer from "../src/orchestration-v2/runtimeLayer.ts";
import * as ThreadManagementService from "../src/orchestration-v2/ThreadManagementService.ts";
import * as TurnItemPositionStore from "../src/orchestration-v2/TurnItemPositionStore.ts";
import * as ProviderSessionRuntime from "../src/persistence/ProviderSessionRuntime.ts";
import * as SqlitePersistence from "../src/persistence/Sqlite.ts";
import * as ProjectEnrichmentService from "../src/project/ProjectEnrichmentService.ts";
import * as ProjectService from "../src/project/ProjectService.ts";
import * as ProviderInstanceRegistry from "../src/provider/ProviderInstanceRegistry.ts";
import * as ServerSettings from "../src/serverSettings.ts";
import * as SourceControlProviderRegistry from "../src/sourceControl/SourceControlProviderRegistry.ts";
import * as WorkspacePaths from "../src/workspace/WorkspacePaths.ts";

const CLAUDE = ProviderDriverKind.make("claudeAgent");
const CODEX = ProviderDriverKind.make("codex");

// --- v2 runtime ---------------------------------------------------------------

const noEnrichment = {
  repositoryIdentity: null,
  faviconPath: null,
  repositoryIdentityResolved: true,
};

/**
 * The orchestrator, thread management, ProjectService and event sink over a
 * fresh in-memory database, with real workspace-path validation. Provider
 * plumbing the importer never reaches is stubbed.
 */
const layerRuntime = Layer.mergeAll(
  RuntimeLayer.layer.pipe(Layer.provide(RuntimeLayer.layerProjectService)),
  RuntimeLayer.layerProjectService,
  RuntimeLayer.layerEventSink,
  ProjectStore.layer,
  ProviderSessionRuntime.layer,
  TurnItemPositionStore.layer,
).pipe(
  Layer.provide(
    Layer.succeed(ProjectEnrichmentService.ProjectEnrichmentService, {
      peek: () => Effect.succeed(noEnrichment),
      request: () => Effect.void,
      getAvailable: () => Effect.succeed(noEnrichment),
      invalidate: () => Effect.void,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(WorkspacePaths.layer),
  Layer.provide(McpSessionRegistryTestkit.layer),
  Layer.provideMerge(SqlitePersistence.layerMemory),
  Layer.provide(Layer.mock(CheckpointStore.CheckpointStore)({})),
  Layer.provide(ServerConfig.layerTest(process.cwd(), { prefix: "t3-session-importer-config-" })),
  Layer.provideMerge(ServerSettings.layerTest()),
  Layer.provide(
    Layer.succeed(ProviderInstanceRegistry.ProviderInstanceRegistry, {
      getInstance: () => Effect.succeed(undefined),
      listInstances: Effect.succeed([]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.never,
    }),
  ),
  Layer.provide(Layer.mock(GitWorkflow.GitWorkflowService)({})),
  Layer.provide(
    Layer.mock(SourceControlProviderRegistry.SourceControlProviderRegistry)({
      resolveLink: () => Effect.die("unused"),
    }),
  ),
  Layer.provideMerge(NodeServices.layer),
);

// --- transcript fixtures (the shapes the fork's parsers read) -------------------

type Entry = Record<string, unknown>;
const jsonl = (entries: ReadonlyArray<Entry>) =>
  entries.map((entry) => JSON.stringify(entry)).join("\n") + "\n";

/** Claude Code `<projects>/<dir>/<session>.jsonl` entries for one session. */
const claudeSession = (input: {
  readonly sessionId: string;
  readonly cwd: string;
  /** Claude stamps `cli` for the interactive TUI and `sdk-*` for programmatic runs. */
  readonly entrypoint?: string;
}) => {
  const entry = (type: "user" | "assistant", uuid: string, text: string, at: string): Entry => ({
    type,
    uuid,
    parentUuid: null,
    sessionId: input.sessionId,
    cwd: input.cwd,
    timestamp: at,
    isSidechain: false,
    ...(input.entrypoint === undefined ? {} : { entrypoint: input.entrypoint }),
    message:
      type === "user"
        ? { role: "user", content: text }
        : { role: "assistant", content: [{ type: "text", text }] },
  });
  return {
    user: (uuid: string, text: string, at: string) => entry("user", uuid, text, at),
    assistant: (uuid: string, text: string, at: string) => entry("assistant", uuid, text, at),
  };
};

const GIT = { git: { repository_url: "https://example.com/repo.git", branch: "main" } };

/** Codex `sessions/YYYY/MM/DD/rollout-*.jsonl` lines. */
const codex = {
  meta: (id: string, cwd: string, at: string, extra: Entry = {}): Entry => ({
    timestamp: at,
    type: "session_meta",
    payload: { id, cwd, timestamp: at, ...extra },
  }),
  user: (text: string, at: string): Entry => ({
    timestamp: at,
    type: "event_msg",
    payload: { type: "user_message", message: text },
  }),
  assistant: (text: string, at: string): Entry => ({
    timestamp: at,
    type: "event_msg",
    payload: { type: "agent_message", message: text },
  }),
  /** Provider item ids are copied verbatim into forks: the fork signature. */
  item: (id: string, at: string): Entry => ({
    timestamp: at,
    type: "response_item",
    payload: { id, type: "reasoning" },
  }),
};

/**
 * The rows a paginated Codex record (the TUI and Desktop since October)
 * writes for a turn: its context, then each item it completed.
 */
const codexTurn = {
  /** Not T3's default model, so a thread that shows it took it from the record. */
  context: (turnId: string, at: string, model = "gpt-5.6-terra"): Entry => ({
    timestamp: at,
    type: "turn_context",
    payload: { turn_id: turnId, model },
  }),
  completed: (turnId: string, item: Entry, at: string): Entry => ({
    timestamp: at,
    type: "event_msg",
    payload: { type: "item_completed", turn_id: turnId, item },
  }),
  said: (turnId: string, role: "user" | "assistant", id: string, text: string, at: string): Entry =>
    codexTurn.completed(
      turnId,
      {
        type: role === "user" ? "UserMessage" : "AgentMessage",
        id,
        content: [{ type: "text", text }],
      },
      at,
    ),
  command: (turnId: string, id: string, command: ReadonlyArray<string>, at: string): Entry =>
    codexTurn.completed(
      turnId,
      {
        type: "CommandExecution",
        id,
        command,
        aggregated_output: "ok\n",
        exit_code: 0,
        status: "completed",
      },
      at,
    ),
  event: (payload: Entry, at: string): Entry => ({ timestamp: at, type: "event_msg", payload }),
};

/** A turn item as the thread shows it, in a word: a message's text, an item's kind and native id. */
const shownItem = (item: OrchestrationV2TurnItem) => {
  switch (item.type) {
    case "user_message":
    case "assistant_message":
      return item.text;
    case "system_notice":
      return `system_notice: ${item.message}`;
    default:
      return `${item.type}: ${item.nativeItemRef?.nativeId ?? ""}`;
  }
};

const shownTimeline = (h: Harness, threadId: ThreadId) =>
  h.threads
    .getThreadRecords(threadId, ["turnItems"])
    .pipe(
      Effect.map((records) =>
        records.turnItems.toSorted((left, right) => left.ordinal - right.ordinal),
      ),
    );

/**
 * A turn T3 drove on a Codex thread: its provider turn, the prompt under
 * T3's own id, and the reply and a command under the ids the live adapter
 * gives them.
 */
const seedCodexLiveTurn = (
  h: Harness,
  threadId: ThreadId,
  input: {
    readonly nativeTurnId: string;
    readonly prompt: string;
    readonly replyId: string;
    readonly reply: string;
    readonly commandId: string;
    readonly iso: string;
  },
) => {
  const at = DateTime.makeUnsafe(input.iso);
  const seed = seedRunPayload(threadId, CODEX, "completed", { ordinal: 1, iso: input.iso });
  const promptId = MessageId.make(`seed-run-message:${threadId}:1`);
  const replyId = MessageId.make(`message:provider:codex:native-item:${input.replyId}`);
  const message = (id: MessageId, role: "user" | "assistant", text: string) => ({
    id: EventId.make(`seed:${threadId}:codex-message:${id}`),
    type: "message.updated" as const,
    threadId,
    occurredAt: at,
    payload: {
      createdBy: role === "user" ? ("user" as const) : ("agent" as const),
      creationSource: role === "user" ? ("web" as const) : ("provider" as const),
      id,
      threadId,
      runId: seed.runId,
      nodeId: null,
      role,
      text,
      attachments: [],
      streaming: false,
      createdAt: at,
      updatedAt: at,
    },
  });
  return h.eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${threadId}:codex-turn:${input.nativeTurnId}`),
        type: "provider-turn.updated",
        threadId,
        occurredAt: at,
        payload: {
          id: ProviderTurnId.make(`provider-turn:provider:codex:native-turn:${input.nativeTurnId}`),
          providerThreadId: ProviderThreadId.make(`seed-provider-thread:${threadId}`),
          nodeId: NodeId.make(`seed-node:${threadId}`),
          runAttemptId: null,
          nativeTurnRef: { driver: CODEX, nativeId: input.nativeTurnId, strength: "strong" },
          ordinal: 1,
          status: "completed",
          startedAt: at,
          completedAt: at,
        },
      },
      message(promptId, "user", input.prompt),
      seedTimelineRow({
        threadId,
        key: "codex-prompt",
        messageId: promptId,
        role: "user",
        text: input.prompt,
        at,
        runId: seed.runId,
      }),
      message(replyId, "assistant", input.reply),
      seedTimelineRow({
        threadId,
        key: "codex-reply",
        messageId: replyId,
        role: "assistant",
        text: input.reply,
        at,
        runId: seed.runId,
      }),
      {
        id: EventId.make(`seed:${threadId}:codex-command:${input.commandId}`),
        type: "turn-item.updated",
        threadId,
        occurredAt: at,
        payload: {
          id: TurnItemId.make(`turn-item:provider:codex:native-item:${input.commandId}`),
          threadId,
          runId: seed.runId,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: { driver: CODEX, nativeId: input.commandId, strength: "strong" },
          parentItemId: null,
          ordinal: 0,
          status: "completed",
          title: null,
          startedAt: at,
          completedAt: at,
          updatedAt: at,
          type: "command_execution",
          input: "pnpm test",
          output: "ok\n",
          exitCode: 0,
        },
      },
    ],
  });
};

const iso = (value: DateTime.DateTime) => DateTime.formatIso(value);

// --- harness ------------------------------------------------------------------

const makeHarness = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const threads = yield* ThreadManagementService.ThreadManagementService;
  const projectStore = yield* ProjectStore.ProjectStoreV2;
  const projects = yield* ProjectService.ProjectService;
  const runtimes = yield* ProviderSessionRuntime.ProviderSessionRuntimeRepository;
  const eventSink = yield* EventSink.EventSinkV2;

  const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-session-importer-" });
  const claudeProjectsDir = path.join(root, "claude-home", "projects");
  const codexSessionsDir = path.join(root, "codex-home", "sessions");
  const claudeDir = path.join(claudeProjectsDir, "-repo");
  const codexDir = path.join(codexSessionsDir, "2026", "07", "29");
  const workspace = path.join(root, "workspace");
  for (const dir of [claudeDir, codexDir, workspace]) {
    yield* fs.makeDirectory(dir, { recursive: true });
  }

  const write = (file: string, entries: ReadonlyArray<Entry>) =>
    fs.writeFileString(file, jsonl(entries));
  const append = (file: string, entries: ReadonlyArray<Entry>) =>
    fs
      .readFileString(file)
      .pipe(Effect.flatMap((existing) => fs.writeFileString(file, existing + jsonl(entries))));

  const shell = (threadId: ThreadId) => threads.getThreadShell(threadId);
  const isLive = (threadId: ThreadId) =>
    shell(threadId).pipe(Effect.map((found) => found !== null && found.deletedAt === null));
  const liveShell = (threadId: ThreadId) =>
    shell(threadId).pipe(
      Effect.map((found) => {
        assert.isTrue(found !== null && found.deletedAt === null, `expected live ${threadId}`);
        return found!;
      }),
    );

  return {
    root,
    workspace,
    /** Shared project for project-less Codex chats. */
    chatsRoot: path.dirname(codexSessionsDir),
    /** Shared project for machine-launched sessions. */
    automationsRoot: path.dirname(claudeProjectsDir),
    claudeFile: (sessionId: string) => path.join(claudeDir, `${sessionId}.jsonl`),
    codexFile: (name: string) => path.join(codexDir, `rollout-${name}.jsonl`),
    makeDir: (...segments: ReadonlyArray<string>) => {
      const dir = path.join(root, ...segments);
      return fs.makeDirectory(dir, { recursive: true }).pipe(Effect.as(dir));
    },
    write,
    append,
    importer: (options: Partial<SessionImporterLiveOptions> = {}) =>
      makeSessionImporter({ claudeProjectsDir, codexSessionsDir, settleMs: 0, ...options }),
    shell,
    isLive,
    liveShell,
    texts: (threadId: ThreadId) =>
      threads
        .getThreadRecords(threadId, ["messages"])
        .pipe(Effect.map((records) => records.messages.map((message) => message.text))),
    binding: (threadId: ThreadId) =>
      runtimes
        .getByThreadId({ threadId })
        .pipe(Effect.map((row) => Option.getOrThrowWith(row, () => new Error(`no ${threadId}`)))),
    projectAt: (workspaceRoot: string) =>
      projectStore
        .findActiveByWorkspaceRoot(workspaceRoot)
        .pipe(Effect.map((row) => Option.getOrUndefined(row))),
    /** Head of the event log: unchanged means a sweep wrote nothing. */
    sequence: eventSink.latestSequence(),
    threads,
    projects,
    runtimes,
    eventSink,
  };
});

type Harness = Effect.Success<typeof makeHarness>;

type Scenario<E> = (h: Harness) => Effect.Effect<void, E, Layer.Success<typeof layerRuntime>>;
const inRuntime =
  <E>(body: Scenario<E>) =>
  () =>
    makeHarness.pipe(
      Effect.flatMap((h) => body(h)),
      Effect.provide(layerRuntime),
    );

/** One scenario: a fresh database and fresh transcript trees. */
const runtimeTest = <E>(name: string, body: Scenario<E>) => it.effect(name, inRuntime(body));
/** A scenario that documents a known importer bug; flips to failing once it is fixed. */

/** A thread T3 itself created, optionally bound to a native provider session. */
const seedT3Thread = (
  h: Harness,
  input: {
    readonly threadId: ThreadId;
    readonly projectId: ProjectId;
    readonly driver: ProviderDriverKind;
    readonly nativeSessionId?: string;
    readonly historyOrigin?: OrchestrationV2AppThread["historyOrigin"];
  },
) => {
  const at = DateTime.makeUnsafe("2026-07-20T00:00:00.000Z");
  const instanceId = defaultInstanceIdForDriver(input.driver);
  // Native-run provider threads keep a pending id, so it cannot be derived.
  const providerThreadId = ProviderThreadId.make(`native-thread:pending:${input.threadId}`);
  const events: Array<OrchestrationV2DomainEvent> = [
    {
      id: EventId.make(`seed:${input.threadId}:created`),
      type: "thread.created",
      threadId: input.threadId,
      occurredAt: at,
      payload: {
        id: input.threadId,
        projectId: input.projectId,
        title: "T3 thread",
        providerInstanceId: instanceId,
        modelSelection: { instanceId, model: "test-model" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        activeProviderThreadId: input.nativeSessionId === undefined ? null : providerThreadId,
        ...(input.historyOrigin === undefined ? {} : { historyOrigin: input.historyOrigin }),
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: input.threadId },
        forkedFrom: null,
        createdBy: "user",
        creationSource: "web",
        createdAt: at,
        updatedAt: at,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        lastVisitedAt: null,
        deletedAt: null,
      },
    },
  ];
  if (input.nativeSessionId !== undefined) {
    events.push({
      id: EventId.make(`seed:${input.threadId}:provider-thread`),
      type: "provider-thread.updated",
      threadId: input.threadId,
      driver: input.driver,
      providerInstanceId: instanceId,
      occurredAt: at,
      payload: {
        id: providerThreadId,
        driver: input.driver,
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: input.threadId,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: input.driver,
          nativeId: input.nativeSessionId,
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: 1,
        lastRunOrdinal: 1,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: at,
        updatedAt: at,
      },
    });
  }
  return h.eventSink.write({ events });
};

/** A T3 run on a thread: what chatting into an imported thread leaves behind. */
const seedRunPayload = (
  threadId: ThreadId,
  driver: ProviderDriverKind,
  status: "running" | "completed",
  when: { readonly ordinal: number; readonly iso: string } = {
    ordinal: 1,
    iso: "2026-07-20T00:00:00.000Z",
  },
) => {
  const at = DateTime.makeUnsafe(when.iso);
  const runId = RunId.make(
    when.ordinal === 1 ? `seed-run:${threadId}` : `seed-run:${threadId}:${when.ordinal}`,
  );
  const instanceId = defaultInstanceIdForDriver(driver);
  return {
    runId,
    driver,
    instanceId,
    at,
    payload: {
      id: runId,
      threadId,
      ordinal: when.ordinal,
      providerInstanceId: instanceId,
      modelSelection: { instanceId, model: "test-model" },
      providerThreadId: null,
      userMessageId: MessageId.make(`seed-run-message:${threadId}:${when.ordinal}`),
      rootNodeId: null,
      activeAttemptId: null,
      status,
      requestedAt: at,
      startedAt: at,
      completedAt: status === "completed" ? at : null,
      checkpointId: null,
      contextHandoffId: null,
    },
  };
};

/** A T3 run on the thread, as chatting into it from T3 leaves one. */
const seedT3Run = (
  h: Harness,
  threadId: ThreadId,
  driver: ProviderDriverKind,
  status: "running" | "completed" = "completed",
  when?: { readonly ordinal: number; readonly iso: string },
) => {
  const seed = seedRunPayload(threadId, driver, status, when);
  return h.eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${threadId}:run:${when?.ordinal ?? 1}`),
        type: "run.created",
        threadId,
        runId: seed.runId,
        driver,
        providerInstanceId: seed.instanceId,
        occurredAt: seed.at,
        payload: seed.payload,
      },
    ],
  });
};

const settleSeedT3Run = (h: Harness, threadId: ThreadId, driver: ProviderDriverKind) => {
  const seed = seedRunPayload(threadId, driver, "completed");
  return h.eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${threadId}:run:completed`),
        type: "run.updated",
        threadId,
        runId: seed.runId,
        driver,
        providerInstanceId: seed.instanceId,
        occurredAt: seed.at,
        payload: seed.payload,
      },
    ],
  });
};

/** A reply T3's own adapter wrote for a turn it drove. */
/** The timeline row that shows a seeded message: a thread shows what its timeline holds. */
const seedTimelineRow = (input: {
  readonly threadId: ThreadId;
  readonly key: string;
  readonly messageId: MessageId;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly at: DateTime.Utc;
  readonly runId: RunId | null;
}) => {
  const base = {
    id: TurnItemId.make(`seed-item:${input.threadId}:${input.key}`),
    threadId: input.threadId,
    runId: input.runId,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    ordinal: 0,
    status: "completed" as const,
    title: null,
    startedAt: input.at,
    completedAt: input.at,
    updatedAt: input.at,
  };
  return {
    id: EventId.make(`seed:${input.threadId}:item:${input.key}`),
    type: "turn-item.updated" as const,
    threadId: input.threadId,
    occurredAt: input.at,
    payload:
      input.role === "user"
        ? {
            ...base,
            createdBy: "user" as const,
            creationSource: "web" as const,
            type: "user_message" as const,
            messageId: input.messageId,
            inputIntent: "turn_start" as const,
            text: input.text,
            attachments: [],
          }
        : {
            ...base,
            type: "assistant_message" as const,
            messageId: input.messageId,
            text: input.text,
            streaming: false,
          },
  };
};

const seedNativeReply = (
  h: Harness,
  threadId: ThreadId,
  uuid: string,
  text: string,
  iso: string,
) => {
  const at = DateTime.makeUnsafe(iso);
  return h.eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${threadId}:native:${uuid}`),
        type: "message.updated",
        threadId,
        occurredAt: at,
        payload: {
          createdBy: "agent",
          creationSource: "provider",
          id: MessageId.make(`message:provider:claudeAgent:native-item:${uuid}`),
          threadId,
          runId: null,
          nodeId: null,
          role: "assistant",
          text,
          attachments: [],
          streaming: false,
          createdAt: at,
          updatedAt: at,
        },
      },
      seedTimelineRow({
        threadId,
        key: `native:${uuid}`,
        messageId: MessageId.make(`message:provider:claudeAgent:native-item:${uuid}`),
        role: "assistant",
        text,
        at,
        runId: null,
      }),
    ],
  });
};

/** A prompt sent from T3: its message carries T3's id, not the transcript row's. */
const seedT3Prompt = (
  h: Harness,
  threadId: ThreadId,
  runOrdinal: number,
  text: string,
  iso: string,
) => {
  const at = DateTime.makeUnsafe(iso);
  const seed = seedRunPayload(threadId, CLAUDE, "completed", { ordinal: runOrdinal, iso });
  return h.eventSink.write({
    events: [
      {
        id: EventId.make(`seed:${threadId}:prompt:${runOrdinal}`),
        type: "message.updated",
        threadId,
        occurredAt: at,
        payload: {
          createdBy: "user",
          creationSource: "web",
          id: MessageId.make(`seed-run-message:${threadId}:${runOrdinal}`),
          threadId,
          runId: seed.runId,
          nodeId: null,
          role: "user",
          text,
          attachments: [],
          streaming: false,
          createdAt: at,
          updatedAt: at,
        },
      },
      seedTimelineRow({
        threadId,
        key: `prompt:${runOrdinal}`,
        messageId: MessageId.make(`seed-run-message:${threadId}:${runOrdinal}`),
        role: "user",
        text,
        at,
        runId: seed.runId,
      }),
    ],
  });
};

const payloadOf = (row: ProviderSessionRuntime.ProviderSessionRuntime) =>
  row.runtimePayload as Record<string, unknown>;

// --- scenarios ----------------------------------------------------------------

runtimeTest("imports Claude and Codex sessions with their history and a native resume ref", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-111111111111";
    const ineligibleId = "bbbbbbbb-1111-4111-8111-111111111111";
    const codexId = "cccccccc-2222-4222-8222-222222222222";
    const codexCwd = yield* h.makeDir("codex-workspace");
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    yield* h.write(claudePath, [
      { type: "mode", mode: "default" },
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      { ...c.user("side-1", "sidechain noise", "2026-07-01T10:00:06.000Z"), isSidechain: true },
      { type: "ai-title", aiTitle: "Login bug fix" },
    ]);
    // A user message with no reply is not a conversation yet.
    yield* h.write(h.claudeFile(ineligibleId), [
      claudeSession({ sessionId: ineligibleId, cwd: h.workspace }).user(
        "lonely-1",
        "no reply yet",
        "2026-07-01T11:00:00.000Z",
      ),
    ]);
    yield* h.write(h.codexFile("codex"), [
      codex.meta(codexId, codexCwd, "2026-07-02T08:00:00.000Z", GIT),
      { timestamp: "2026-07-02T08:00:00.500Z", type: "turn_context", payload: {} },
      codex.user("Refactor the rollout parser", "2026-07-02T08:00:01.000Z"),
      codex.assistant("Parser refactored.", "2026-07-02T08:00:09.000Z"),
    ]);

    yield* (yield* h.importer()).sweepNow();

    // Claude: a v1_import thread in the cwd's project, history with original
    // timestamps, and a strong native ref to the source session.
    const claudeThreadId = importedThreadId(claudeId);
    const claudeRecords = yield* h.threads.getThreadRecords(claudeThreadId, [
      "messages",
      "providerThreads",
    ]);
    const claudeThread = claudeRecords.thread;
    const workspaceProject = yield* h.projectAt(h.workspace);
    assert.equal(claudeThread.projectId, workspaceProject?.projectId);
    assert.equal(workspaceProject?.title, "workspace");
    assert.equal(claudeThread.title, "Login bug fix");
    assert.equal(claudeThread.historyOrigin, "v1_import");
    assert.equal(claudeThread.providerInstanceId, "claudeAgent");
    assert.equal(claudeThread.modelSelection.instanceId, "claudeAgent");
    assert.equal(iso(claudeThread.createdAt), "2026-07-01T10:00:00.000Z");
    assert.equal(iso(claudeThread.updatedAt), "2026-07-01T10:00:05.000Z");
    assert.deepEqual(
      claudeRecords.messages.map((message) => ({
        role: message.role,
        text: message.text,
        streaming: message.streaming,
        createdAt: iso(message.createdAt),
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
    assert.equal(claudeRecords.providerThreads.length, 1);
    const claudeProviderThread = claudeRecords.providerThreads[0]!;
    assert.equal(claudeThread.activeProviderThreadId, claudeProviderThread.id);
    assert.equal(
      claudeProviderThread.id,
      deriveProviderThread({ driver: CLAUDE, nativeThreadId: claudeId }),
    );
    assert.equal(claudeProviderThread.appThreadId, claudeThreadId);
    assert.deepEqual(claudeProviderThread.nativeThreadRef, {
      driver: CLAUDE,
      nativeId: claudeId,
      strength: "strong",
    });

    // Its import cursor and fork signature live on the provider_session_runtime row.
    const claudeBinding = yield* h.binding(claudeThreadId);
    assert.equal(claudeBinding.providerName, "claudeAgent");
    assert.equal(claudeBinding.providerInstanceId, "claudeAgent");
    assert.equal(claudeBinding.status, "stopped");
    assert.deepEqual(claudeBinding.resumeCursor, {
      threadId: claudeThreadId,
      resume: claudeId,
      turnCount: 1,
    });
    assert.deepEqual(claudeBinding.runtimePayload, {
      cwd: h.workspace,
      importForkSig: ["user-1", "assistant-1"],
      importCursor: {
        sourceSessionId: claudeId,
        sourcePath: claudePath,
        lineCount: 5,
        epoch: 2,
      },
    });

    // Codex: the same shape, in its own cwd's project.
    const codexThreadId = importedThreadId(codexId);
    const codexRecords = yield* h.threads.getThreadRecords(codexThreadId, [
      "messages",
      "providerThreads",
    ]);
    assert.equal(codexRecords.thread.title, "Refactor the rollout parser");
    assert.equal(codexRecords.thread.projectId, (yield* h.projectAt(codexCwd))?.projectId);
    assert.deepEqual(
      codexRecords.messages.map((message) => [message.role, message.text]),
      [
        ["user", "Refactor the rollout parser"],
        ["assistant", "Parser refactored."],
      ],
    );
    assert.deepEqual(codexRecords.providerThreads[0]?.nativeThreadRef, {
      driver: CODEX,
      nativeId: codexId,
      strength: "strong",
    });
    assert.equal(codexRecords.thread.activeProviderThreadId, codexRecords.providerThreads[0]?.id);
    assert.deepEqual((yield* h.binding(codexThreadId)).resumeCursor, { threadId: codexId });

    assert.isNull(yield* h.shell(importedThreadId(ineligibleId)));

    // A restarted importer (empty change cache) re-reads every file and writes nothing.
    const head = yield* h.sequence;
    yield* (yield* h.importer()).sweepNow();
    assert.equal(yield* h.sequence, head);
  }),
);

runtimeTest("appends new transcript lines incrementally and bumps recency", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-111111111111";
    const codexId = "cccccccc-2222-4222-8222-222222222222";
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const codexPath = h.codexFile("codex");
    yield* h.write(claudePath, [
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ]);
    yield* h.write(codexPath, [
      codex.meta(codexId, h.workspace, "2026-07-02T08:00:00.000Z", GIT),
      codex.user("Refactor the rollout parser", "2026-07-02T08:00:01.000Z"),
      codex.assistant("Parser refactored.", "2026-07-02T08:00:09.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();

    yield* h.append(claudePath, [
      c.user("user-2", "Now add a regression test", "2026-07-01T10:05:00.000Z"),
      c.assistant("assistant-2", "Added a regression test.", "2026-07-01T10:05:30.000Z"),
    ]);
    yield* h.append(codexPath, [codex.assistant("Anything else?", "2026-07-02T08:01:00.000Z")]);
    yield* importer.sweepNow();

    const claudeThreadId = importedThreadId(claudeId);
    assert.deepEqual(yield* h.texts(claudeThreadId), [
      "Fix the login bug please",
      "I fixed the login bug.",
      "Now add a regression test",
      "Added a regression test.",
    ]);
    // The thread's recency follows the newest imported message.
    assert.equal(iso((yield* h.liveShell(claudeThreadId)).updatedAt), "2026-07-01T10:05:30.000Z");
    const claudeBinding = yield* h.binding(claudeThreadId);
    assert.deepEqual(claudeBinding.resumeCursor, {
      threadId: claudeThreadId,
      resume: claudeId,
      turnCount: 2,
    });
    assert.deepEqual(claudeBinding.runtimePayload, {
      cwd: h.workspace,
      importForkSig: ["user-1", "assistant-1", "user-2", "assistant-2"],
      importCursor: { sourceSessionId: claudeId, sourcePath: claudePath, lineCount: 4, epoch: 2 },
    });

    const codexThreadId = importedThreadId(codexId);
    assert.deepEqual(yield* h.texts(codexThreadId), [
      "Refactor the rollout parser",
      "Parser refactored.",
      "Anything else?",
    ]);
    assert.equal(iso((yield* h.liveShell(codexThreadId)).updatedAt), "2026-07-02T08:01:00.000Z");

    // Nothing changed: nothing written, by this importer or a restarted one.
    const head = yield* h.sequence;
    yield* importer.sweepNow();
    yield* (yield* h.importer()).sweepNow();
    assert.equal(yield* h.sequence, head);
  }),
);

const continuedConversation = (h: Harness, oldId: string, newId: string) => {
  const before = claudeSession({ sessionId: oldId, cwd: h.workspace });
  const after = claudeSession({ sessionId: newId, cwd: h.workspace });
  return {
    /** The session as it stood in its terminal. */
    old: [
      before.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      before.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ],
    /** Sent to the background: the last row says where the conversation went on. */
    mark: [
      {
        type: "continued-in",
        timestamp: "2026-07-01T10:06:00.000Z",
        sessionId: oldId,
        continuedInSessionId: newId,
      },
    ],
    /** The new session's record: a copy of the old rows, then its own. */
    carriedOn: [
      after.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      after.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      after.user("user-2", "Now add a regression test", "2026-07-01T10:07:00.000Z"),
      after.assistant("assistant-2", "Added a regression test.", "2026-07-01T10:07:30.000Z"),
    ],
    texts: [
      "Fix the login bug please",
      "I fixed the login bug.",
      "Now add a regression test",
      "Added a regression test.",
    ],
  };
};

runtimeTest("a session that carries on under a new id stays in the thread it had", (h) =>
  Effect.gen(function* () {
    const oldId = "aaaaaaaa-1111-4111-8111-111111111111";
    const newId = "bbbbbbbb-2222-4222-8222-222222222222";
    const rows = continuedConversation(h, oldId, newId);
    yield* h.write(h.claudeFile(oldId), rows.old);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    const threadId = importedThreadId(oldId);
    assert.deepEqual(yield* h.texts(threadId), rows.texts.slice(0, 2));

    yield* h.append(h.claudeFile(oldId), rows.mark);
    yield* h.write(h.claudeFile(newId), rows.carriedOn);
    yield* importer.sweepNow();

    assert.deepEqual(yield* h.texts(threadId), rows.texts);
    assert.isNull(yield* h.shell(importedThreadId(newId)), "no second thread");
    // The thread is still held under the id it was made for.
    assert.deepEqual((yield* h.binding(threadId)).resumeCursor, {
      threadId,
      resume: oldId,
      turnCount: 1,
    });

    // Nothing changed: nothing written, by this importer or a restarted one.
    const head = yield* h.sequence;
    yield* importer.sweepNow();
    yield* (yield* h.importer()).sweepNow();
    yield* (yield* h.importer()).sweepNow();
    assert.equal(yield* h.sequence, head);
    assert.isNull(yield* h.shell(importedThreadId(newId)), "nor after a restart");
  }),
);

runtimeTest("a carried-on session read before the one it came from still makes one thread", (h) =>
  Effect.gen(function* () {
    // Whichever of the two the sweep reads first.
    for (const [oldId, newId] of [
      ["11111111-1111-4111-8111-111111111111", "ffffffff-2222-4222-8222-222222222222"],
      ["eeeeeeee-3333-4333-8333-333333333333", "22222222-4444-4444-8444-444444444444"],
    ] as const) {
      const rows = continuedConversation(h, oldId, newId);
      yield* h.write(h.claudeFile(oldId), [...rows.old, ...rows.mark]);
      yield* h.write(h.claudeFile(newId), rows.carriedOn);
      const importer = yield* h.importer();
      yield* importer.sweepNow();
      yield* importer.sweepNow();

      assert.deepEqual(yield* h.texts(importedThreadId(oldId)), rows.texts);
      assert.isNull(yield* h.shell(importedThreadId(newId)), "no second thread");
    }
  }),
);

runtimeTest("waits for a transcript to settle and skips files over the size cap", (h) =>
  Effect.gen(function* () {
    const settlingId = "dddddddd-1111-4111-8111-111111111111";
    const c = claudeSession({ sessionId: settlingId, cwd: h.workspace });
    const settlingPath = h.claudeFile(settlingId);
    yield* h.write(settlingPath, [
      c.user("settle-user-1", "Still typing", "2026-07-01T10:00:00.000Z"),
      c.assistant("settle-assistant-1", "Still answering", "2026-07-01T10:00:05.000Z"),
    ]);
    const fs = yield* FileSystem.FileSystem;
    const mtimeMs = Option.getOrThrow((yield* fs.stat(settlingPath)).mtime).getTime();

    const settling = yield* h.importer({ settleMs: 20_000 });
    yield* TestClock.setTime(mtimeMs + 19_000);
    yield* settling.sweepNow();
    assert.isNull(yield* h.shell(importedThreadId(settlingId)), "a busy file waits");
    yield* TestClock.setTime(mtimeMs + 20_000);
    yield* settling.sweepNow();
    assert.isTrue(yield* h.isLive(importedThreadId(settlingId)), "a quiet file imports");

    const largeId = "eeeeeeee-1111-4111-8111-111111111111";
    const large = claudeSession({ sessionId: largeId, cwd: h.workspace });
    yield* h.write(h.claudeFile(largeId), [
      large.user("large-user-1", "x".repeat(4_000), "2026-07-01T11:00:00.000Z"),
      large.assistant("large-assistant-1", "ok", "2026-07-01T11:00:05.000Z"),
    ]);
    yield* (yield* h.importer({ maxFileBytes: 2_000 })).sweepNow();
    assert.isNull(yield* h.shell(importedThreadId(largeId)), "an oversized file is skipped");
    yield* (yield* h.importer()).sweepNow();
    assert.isTrue(yield* h.isLive(importedThreadId(largeId)), "only the cap kept it out");
  }),
);

runtimeTest("places sessions in a project per cwd and project-less Codex chats in Chats", (h) =>
  Effect.gen(function* () {
    const otherCwd = yield* h.makeDir("other-repo");
    const claudeA = "aaaaaaaa-0000-4000-8000-00000000000a";
    const claudeB = "aaaaaaaa-0000-4000-8000-00000000000b";
    const claudeOther = "aaaaaaaa-0000-4000-8000-00000000000c";
    const repoCodex = "cccccccc-0000-4000-8000-00000000000a";
    const chatA = "eeeeeeee-3333-4333-8333-33333333333a";
    const chatB = "eeeeeeee-3333-4333-8333-33333333333b";
    for (const [sessionId, cwd] of [
      [claudeA, h.workspace],
      [claudeB, h.workspace],
      [claudeOther, otherCwd],
    ] as const) {
      const c = claudeSession({ sessionId, cwd });
      yield* h.write(h.claudeFile(sessionId), [
        c.user(`${sessionId}-u`, `Question ${sessionId}`, "2026-07-01T10:00:00.000Z"),
        c.assistant(`${sessionId}-a`, "Answer", "2026-07-01T10:00:05.000Z"),
      ]);
    }
    yield* h.write(h.codexFile("repo"), [
      codex.meta(repoCodex, h.workspace, "2026-07-02T08:00:00.000Z", GIT),
      codex.user("Repo question", "2026-07-02T08:00:01.000Z"),
      codex.assistant("Repo answer", "2026-07-02T08:00:05.000Z"),
    ]);
    // No git metadata: a project-less chat, whatever throwaway cwd it ran in.
    for (const [sessionId, cwd] of [
      [chatA, joinPath(h.root, "throwaway-1")],
      [chatB, joinPath(h.root, "throwaway-2")],
    ] as const) {
      yield* h.write(h.codexFile(sessionId), [
        codex.meta(sessionId, cwd, "2026-07-03T09:00:00.000Z"),
        codex.user("Quick question", "2026-07-03T09:00:01.000Z"),
        codex.assistant("Quick answer.", "2026-07-03T09:00:05.000Z"),
      ]);
    }

    yield* (yield* h.importer()).sweepNow();

    const workspaceProject = (yield* h.projectAt(h.workspace))!;
    const otherProject = (yield* h.projectAt(otherCwd))!;
    assert.notEqual(workspaceProject.projectId, otherProject.projectId);
    assert.equal(
      (yield* h.liveShell(importedThreadId(claudeA))).projectId,
      workspaceProject.projectId,
    );
    assert.equal(
      (yield* h.liveShell(importedThreadId(claudeB))).projectId,
      workspaceProject.projectId,
    );
    assert.equal(
      (yield* h.liveShell(importedThreadId(repoCodex))).projectId,
      workspaceProject.projectId,
    );
    assert.equal(
      (yield* h.liveShell(importedThreadId(claudeOther))).projectId,
      otherProject.projectId,
    );

    const chats = (yield* h.projectAt(h.chatsRoot))!;
    assert.equal(chats.title, "Chats");
    assert.equal((yield* h.liveShell(importedThreadId(chatA))).projectId, chats.projectId);
    assert.equal((yield* h.liveShell(importedThreadId(chatB))).projectId, chats.projectId);
    assert.isUndefined(yield* h.projectAt(joinPath(h.root, "throwaway-1")));
  }),
);

runtimeTest("moves an epoch-1 per-cwd chat into Chats and drops its emptied project", (h) =>
  Effect.gen(function* () {
    const legacyId = "ffffffff-4444-4444-8444-444444444444";
    // Hand-rolled epoch-1 id and project: the layout before epoch scoping.
    const legacyThreadId = ThreadId.make(`cli-import-${legacyId}`);
    const legacyCwd = yield* h.makeDir("legacy-chat-cwd");
    const legacyProjectId = ProjectId.make("cli-import-project-legacy-chat");
    yield* h.projects.create({
      commandId: CommandId.make("test:legacy-project"),
      projectId: legacyProjectId,
      title: "legacy-chat-cwd",
      workspaceRoot: legacyCwd,
    });
    yield* seedT3Thread(h, {
      threadId: legacyThreadId,
      projectId: legacyProjectId,
      driver: CODEX,
      historyOrigin: "v1_import",
    });
    const legacyPath = h.codexFile("legacy");
    const legacyLines = [
      codex.meta(legacyId, legacyCwd, "2026-07-04T10:00:00.000Z"),
      codex.user("Legacy chat question", "2026-07-04T10:00:01.000Z"),
      codex.assistant("Legacy chat answer.", "2026-07-04T10:00:05.000Z"),
    ];
    yield* h.write(legacyPath, legacyLines);
    yield* h.runtimes.upsert({
      threadId: legacyThreadId,
      providerName: CODEX,
      providerInstanceId: defaultInstanceIdForDriver(CODEX),
      adapterKey: CODEX,
      runtimeMode: "full-access",
      status: "stopped",
      lastSeenAt: "2026-07-04T10:00:05.000Z",
      resumeCursor: { threadId: legacyId },
      runtimePayload: {
        cwd: legacyCwd,
        // No epoch: the legacy layout marker.
        importCursor: {
          sourceSessionId: legacyId,
          sourcePath: legacyPath,
          lineCount: legacyLines.length,
        },
      },
    });

    yield* (yield* h.importer()).sweepNow();

    const migratedThreadId = importedThreadId(legacyId);
    const chats = (yield* h.projectAt(h.chatsRoot))!;
    assert.equal((yield* h.liveShell(migratedThreadId)).projectId, chats.projectId);
    assert.deepEqual(yield* h.texts(migratedThreadId), [
      "Legacy chat question",
      "Legacy chat answer.",
    ]);
    assert.isFalse(yield* h.isLive(legacyThreadId), "the epoch-1 thread is deleted");
    assert.isUndefined(yield* h.projectAt(legacyCwd), "its per-chat project is deleted");
    const legacyBinding = yield* h.binding(legacyThreadId);
    assert.isNull(legacyBinding.resumeCursor, "the old binding is tombstoned");
    assert.isNull(payloadOf(legacyBinding).importCursor);
    const migratedCursor = payloadOf(yield* h.binding(migratedThreadId)).importCursor as {
      readonly epoch?: number;
    };
    assert.equal(migratedCursor.epoch, 2);
  }),
);

runtimeTest(
  "routes machine-launched sessions to Automations unless they fork a human session",
  (h) =>
    Effect.gen(function* () {
      const humanId = "aaaaaaaa-7777-4777-8777-777777777770";
      const sdkId = "33333333-7777-4777-8777-777777777771";
      const kinId = "33333333-7777-4777-8777-777777777772";
      const execId = "33333333-7777-4777-8777-777777777773";
      const execOriginatorId = "33333333-7777-4777-8777-777777777774";
      const human = claudeSession({ sessionId: humanId, cwd: h.workspace });
      yield* h.write(h.claudeFile(humanId), [
        human.user("h-user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        human.assistant("h-assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const sdk = claudeSession({ sessionId: sdkId, cwd: h.workspace, entrypoint: "sdk-cli" });
      yield* h.write(h.claudeFile(sdkId), [
        sdk.user("auto-user-1", "# nightly pipeline run", "2026-07-08T03:00:00.000Z"),
        sdk.assistant("auto-assistant-1", "pipeline output", "2026-07-08T03:00:30.000Z"),
      ]);
      // A programmatic /branch of the human conversation: its history copies the uuids.
      const kin = claudeSession({ sessionId: kinId, cwd: h.workspace, entrypoint: "sdk-ts" });
      yield* h.write(h.claudeFile(kinId), [
        kin.user("h-user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        kin.assistant("h-assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
        kin.user("kin-user-2", "Branched follow-up", "2026-07-08T04:00:00.000Z"),
        kin.assistant("kin-assistant-2", "Branched reply.", "2026-07-08T04:00:30.000Z"),
      ]);
      yield* h.write(h.codexFile("exec"), [
        codex.meta(execId, h.workspace, "2026-07-08T05:00:00.000Z", { ...GIT, source: "exec" }),
        codex.user("codex exec task", "2026-07-08T05:00:01.000Z"),
        codex.assistant("exec result", "2026-07-08T05:00:05.000Z"),
      ]);
      yield* h.write(h.codexFile("exec-originator"), [
        codex.meta(execOriginatorId, h.workspace, "2026-07-08T06:00:00.000Z", {
          ...GIT,
          originator: "codex_exec",
        }),
        codex.user("another exec task", "2026-07-08T06:00:01.000Z"),
        codex.assistant("exec result", "2026-07-08T06:00:05.000Z"),
      ]);

      const importer = yield* h.importer();
      yield* importer.sweepNow();

      const automations = (yield* h.projectAt(h.automationsRoot))!;
      assert.equal(automations.title, "Automations");
      const workspaceProject = (yield* h.projectAt(h.workspace))!;
      for (const sessionId of [sdkId, execId, execOriginatorId]) {
        assert.equal(
          (yield* h.liveShell(importedThreadId(sessionId))).projectId,
          automations.projectId,
          `${sessionId} is an automation`,
        );
        assert.equal(
          payloadOf(yield* h.binding(importedThreadId(sessionId))).importLaunch,
          "automation",
        );
      }
      const kinShell = yield* h.liveShell(importedThreadId(kinId));
      assert.equal(kinShell.projectId, workspaceProject.projectId, "kin stays beside its human");
      assert.equal(payloadOf(yield* h.binding(importedThreadId(kinId))).importLaunch, "kin");
      // Kin also joins the human session's fork family (smallest session id).
      assert.equal(kinShell.forkFamilyId, kinId);
      assert.equal((yield* h.liveShell(importedThreadId(humanId))).forkFamilyId, kinId);

      // An existing normal import that turns out to be machine-launched moves to
      // Automations under a variant id, and its emptied project goes away.
      const lateId = "44444444-8888-4888-8888-888888888881";
      const machineCwd = yield* h.makeDir("machine-cwd");
      const latePath = h.claudeFile(lateId);
      const lateBody = (entrypoint?: string) => {
        const late = claudeSession({
          sessionId: lateId,
          cwd: machineCwd,
          ...(entrypoint === undefined ? {} : { entrypoint }),
        });
        return [
          late.user("mig-user-1", "# batch job", "2026-07-09T03:00:00.000Z"),
          late.assistant("mig-assistant-1", "batch output", "2026-07-09T03:00:30.000Z"),
        ];
      };
      yield* h.write(latePath, lateBody());
      yield* importer.sweepNow();
      assert.isTrue(yield* h.isLive(importedThreadId(lateId)));
      assert.isDefined(yield* h.projectAt(machineCwd));
      yield* h.write(latePath, lateBody("sdk-cli"));
      yield* importer.sweepNow();
      assert.isFalse(yield* h.isLive(importedThreadId(lateId)), "it leaves its original id");
      assert.equal(
        (yield* h.liveShell(importedThreadId(lateId, "a"))).projectId,
        automations.projectId,
      );
      assert.isUndefined(yield* h.projectAt(machineCwd), "the emptied project is deleted");
    }),
);

runtimeTest("routes human-marked sessions under an automation root to Automations", (h) =>
  Effect.gen(function* () {
    const rootedId = "66666666-1111-4111-8111-999999999992";
    const farmCwd = yield* h.makeDir("agent-farm", "work-r9");
    const c = claudeSession({ sessionId: rootedId, cwd: farmCwd });
    yield* h.write(h.claudeFile(rootedId), [
      c.user("rooted-user-1", "walkthrough step one", "2026-07-11T03:00:00.000Z"),
      c.assistant("rooted-assistant-1", "done", "2026-07-11T03:00:30.000Z"),
    ]);
    yield* (yield* h.importer({ automationRoots: [joinPath(h.root, "agent-farm")] })).sweepNow();
    const automations = (yield* h.projectAt(h.automationsRoot))!;
    assert.equal((yield* h.liveShell(importedThreadId(rootedId))).projectId, automations.projectId);
    assert.isUndefined(yield* h.projectAt(farmCwd));
  }),
);

runtimeTest("skips Codex subagent sessions and removes ones imported earlier", (h) =>
  Effect.gen(function* () {
    const freshThreadSource = "22222222-6666-4666-8666-666666666661";
    const freshSourceField = "22222222-6666-4666-8666-666666666663";
    yield* h.write(h.codexFile("sub-thread-source"), [
      codex.meta(freshThreadSource, h.workspace, "2026-07-07T08:00:00.000Z", {
        ...GIT,
        thread_source: "subagent",
      }),
      codex.user("subagent task", "2026-07-07T08:00:01.000Z"),
      codex.assistant("subagent result", "2026-07-07T08:00:05.000Z"),
    ]);
    yield* h.write(h.codexFile("sub-source"), [
      codex.meta(freshSourceField, h.workspace, "2026-07-07T08:10:00.000Z", {
        ...GIT,
        source: { subagent: { parent_thread_id: "parent" } },
      }),
      codex.user("subagent task", "2026-07-07T08:10:01.000Z"),
      codex.assistant("subagent result", "2026-07-07T08:10:05.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    assert.isNull(yield* h.shell(importedThreadId(freshThreadSource)));
    assert.isNull(yield* h.shell(importedThreadId(freshSourceField)));

    // Imported before the marker was understood, alone in its project.
    const legacyId = "22222222-6666-4666-8666-666666666662";
    const subCwd = yield* h.makeDir("subagent-cwd");
    const legacyPath = h.codexFile("sub-legacy");
    const body = (marked: boolean) => [
      codex.meta(legacyId, subCwd, "2026-07-07T09:00:00.000Z", {
        ...GIT,
        ...(marked ? { thread_source: "subagent" } : {}),
      }),
      codex.user("was imported as a normal thread", "2026-07-07T09:00:01.000Z"),
      codex.assistant("done", "2026-07-07T09:00:05.000Z"),
    ];
    yield* h.write(legacyPath, body(false));
    yield* importer.sweepNow();
    assert.isTrue(yield* h.isLive(importedThreadId(legacyId)));
    assert.isDefined(yield* h.projectAt(subCwd));

    yield* h.write(legacyPath, body(true));
    yield* importer.sweepNow();
    assert.isFalse(yield* h.isLive(importedThreadId(legacyId)), "the subagent thread is removed");
    assert.isUndefined(yield* h.projectAt(subCwd), "its emptied project is removed");
    const tombstone = yield* h.binding(importedThreadId(legacyId));
    assert.isNull(tombstone.resumeCursor);
    assert.isNull(payloadOf(tombstone).importCursor);
  }),
);

// The importer deletes its own emptied projects; a later session in that
// workspace must get a live project again, not the deleted one.
runtimeTest("gives a workspace a live project again after its import project was deleted", (h) =>
  Effect.gen(function* () {
    const firstId = "77777777-0000-4000-8000-000000000001";
    const laterId = "77777777-0000-4000-8000-000000000002";
    const cwd = yield* h.makeDir("revisited-repo");
    const session = (sessionId: string, at: string) => [
      codex.meta(sessionId, cwd, at, GIT),
      codex.user(`Question in ${sessionId}`, at),
      codex.assistant("Answer", at),
    ];
    yield* h.write(h.codexFile("first"), session(firstId, "2026-07-13T10:00:00.000Z"));
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    const first = (yield* h.projectAt(cwd))!;
    // The user deletes the project (the importer does the same when it empties
    // one by removing a subagent or moving a thread to Automations).
    yield* h.projects.delete({
      commandId: CommandId.make("test:user-project-delete"),
      projectId: first.projectId,
      force: true,
    });
    assert.isUndefined(yield* h.projectAt(cwd));

    yield* h.write(h.codexFile("later"), session(laterId, "2026-07-14T10:00:00.000Z"));
    yield* importer.sweepNow();
    const later = yield* h.liveShell(importedThreadId(laterId));
    assert.notEqual(later.projectId, first.projectId, "not the deleted project");
    const project = yield* h.projectAt(cwd);
    assert.isDefined(project, "the workspace has a live project again");
    assert.equal(later.projectId, project?.projectId, "the new thread lives in it");
  }),
);

runtimeTest("repairs slash-command noise titles and leaves user renames alone", (h) =>
  Effect.gen(function* () {
    const titleId = "55555555-9999-4999-8999-999999999991";
    const c = claudeSession({ sessionId: titleId, cwd: h.workspace });
    const titlePath = h.claudeFile(titleId);
    const noise = "<command-name>/model</command-name> output";
    yield* h.write(titlePath, [
      c.user("title-user-1", noise, "2026-07-10T03:00:00.000Z"),
      c.user("title-user-2", "Rename the widget everywhere", "2026-07-10T03:01:00.000Z"),
      c.assistant("title-assistant-1", "Renamed.", "2026-07-10T03:01:30.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    const threadId = importedThreadId(titleId);
    assert.equal((yield* h.liveShell(threadId)).title, "Rename the widget everywhere");

    // A thread titled by the old first-message logic is repaired on the next change.
    yield* h.threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("test:noise-title"),
      threadId,
      title: noise,
    });
    yield* h.append(titlePath, [
      c.assistant("title-assistant-2", "Anything else?", "2026-07-10T03:02:00.000Z"),
    ]);
    yield* importer.sweepNow();
    assert.equal((yield* h.liveShell(threadId)).title, "Rename the widget everywhere");

    // Only noise is replaced: a user's own title survives later changes.
    yield* h.threads.dispatch({
      type: "thread.metadata.update",
      commandId: CommandId.make("test:user-title"),
      threadId,
      title: "My widget work",
    });
    yield* h.append(titlePath, [c.user("title-user-3", "One more", "2026-07-10T03:03:00.000Z")]);
    yield* importer.sweepNow();
    assert.equal((yield* h.liveShell(threadId)).title, "My widget work");
  }),
);

runtimeTest("stamps fork families on two-or-more member families without moving recency", (h) =>
  Effect.gen(function* () {
    const forkA = "11111111-5555-4555-8555-555555555551";
    const forkB = "11111111-5555-4555-8555-555555555552";
    const loner = "11111111-5555-4555-8555-555555555553";
    const forkFile = (sessionId: string, sharedItem: string, answer: string) => [
      codex.meta(sessionId, h.workspace, "2026-07-06T08:00:00.000Z", GIT),
      codex.item(sharedItem, "2026-07-06T08:00:01.000Z"),
      codex.user("Original fork prompt", "2026-07-06T08:00:02.000Z"),
      codex.assistant(answer, "2026-07-06T08:00:05.000Z"),
    ];
    yield* h.write(h.codexFile("fork-a"), forkFile(forkA, "rs_shared001", "In the original."));
    yield* h.write(h.codexFile("fork-b"), forkFile(forkB, "rs_shared001", "In the fork."));
    yield* h.write(h.codexFile("loner"), forkFile(loner, "rs_alone001", "Unrelated."));

    yield* (yield* h.importer()).sweepNow();

    const shellA = yield* h.liveShell(importedThreadId(forkA));
    const shellB = yield* h.liveShell(importedThreadId(forkB));
    // The family id is the smallest member session id: the original.
    assert.equal(shellA.forkFamilyId, forkA);
    assert.equal(shellB.forkFamilyId, forkA);
    assert.isUndefined((yield* h.liveShell(importedThreadId(loner))).forkFamilyId ?? undefined);
    // Stamping is metadata, not activity.
    assert.equal(iso(shellA.updatedAt), "2026-07-06T08:00:05.000Z");
    assert.equal(iso(shellB.updatedAt), "2026-07-06T08:00:05.000Z");

    // Stamped families are not re-stamped, even by a restarted importer.
    const head = yield* h.sequence;
    yield* (yield* h.importer()).sweepNow();
    assert.equal(yield* h.sequence, head);
  }),
);

runtimeTest(
  "keeps a Claude thread whole after T3 has run it: terminal rows land, T3's own turns stay out",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-111111111111";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();

      // Runtime noise on the binding (a provider stop-all merge) is not adoption.
      const before = yield* h.binding(threadId);
      yield* h.runtimes.upsert({
        ...before,
        status: "stopped",
        runtimePayload: { ...payloadOf(before), activeTurnId: null, lastRuntimeEvent: "stopAll" },
      });
      yield* h.append(claudePath, [
        c.user("user-2", "After a quit cycle", "2026-07-01T10:05:00.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.include(yield* h.texts(threadId), "After a quit cycle");

      // Chatting into the thread in T3 makes T3 the owner. The person may still
      // type in a terminal without the mod: those rows belong in the thread.
      yield* seedT3Run(h, threadId, CLAUDE);
      yield* h.append(claudePath, [
        c.user("user-3", "Typed in the terminal after T3 ran it", "2026-07-01T10:10:00.000Z"),
        c.assistant("assistant-3", "Replying in the terminal.", "2026-07-01T10:10:30.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.include(yield* h.texts(threadId), "Replying in the terminal.");
      const adopted = yield* h.binding(threadId);
      assert.isNotNull(payloadOf(adopted).importCursor);
      assert.deepEqual(adopted.resumeCursor, { threadId, resume: claudeId, turnCount: 3 });

      // A turn T3 drove: its adapter wrote the reply as it streamed, so T3's
      // copy can carry an earlier instant than the transcript row. The row's
      // uuid names it as T3's; what follows it in the terminal is imported.
      yield* seedNativeReply(
        h,
        threadId,
        "assistant-5",
        "Replying natively through T3.",
        "2026-07-01T10:19:00.000Z",
      );
      yield* h.append(claudePath, [
        c.user("user-5", "Sent from T3", "2026-07-01T10:19:00.000Z"),
        c.assistant("assistant-5", "Replying natively through T3.", "2026-07-01T10:19:30.000Z"),
        c.user("user-6", "Back in the terminal", "2026-07-01T10:25:00.000Z"),
      ]);
      yield* importer.sweepNow();
      const texts = yield* h.texts(threadId);
      assert.include(texts, "Back in the terminal");
      assert.notInclude(texts, "Sent from T3");
      assert.equal(texts.filter((text) => text === "Replying natively through T3.").length, 1);

      // A quiet transcript: a fresh sweep writes nothing.
      const head = yield* h.sequence;
      yield* (yield* h.importer()).sweepNow();
      assert.equal(yield* h.sequence, head);
    }),
);

// claude-ev (Oct 22): a session run under a second config folder reports its
// record under that folder, whose projects/ links to the one the sweep reads.
// The binding kept the reported path; compared with the sweep's as text, the
// thread was never caught up again, and nothing written between turns arrived.
runtimeTest(
  "catches up a thread whose binding names its record through a linked config folder",
  (h) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const path = yield* Path.Path;
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111e1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();
      yield* seedT3Run(h, threadId, CLAUDE);

      const linkedHome = yield* h.makeDir("claude-ev-home");
      yield* fs.symlink(path.dirname(path.dirname(claudePath)), path.join(linkedHome, "projects"));
      const reported = path.join(linkedHome, "projects", "-repo", `${claudeId}.jsonl`);
      const bound = yield* h.binding(threadId);
      const payload = payloadOf(bound);
      yield* h.runtimes.upsert({
        ...bound,
        runtimePayload: {
          ...payload,
          importCursor: { ...(payload.importCursor as object), sourcePath: reported },
        },
      });
      yield* h.append(claudePath, [
        c.user("user-2", "Typed in the claude-ev terminal", "2026-07-01T10:10:00.000Z"),
        c.assistant("assistant-2", "Replying under the other folder.", "2026-07-01T10:10:30.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.include(yield* h.texts(threadId), "Replying under the other folder.");
    }),
);

runtimeTest(
  "leaves an owned Claude thread alone while a T3 run or a bridged turn is writing it",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111b1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();

      // A T3 turn in flight writes the thread's rows itself.
      yield* seedT3Run(h, threadId, CLAUDE, "running");
      yield* h.append(claudePath, [
        c.assistant("assistant-2", "Streaming through T3.", "2026-07-01T10:05:00.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.notInclude(yield* h.texts(threadId), "Streaming through T3.");

      // The turn settles with T3's own copy of that reply. A live mod process
      // now holds the session and a turn is running in it: the attachment is
      // streaming that turn, so the importer stays out.
      yield* settleSeedT3Run(h, threadId, CLAUDE);
      yield* seedNativeReply(
        h,
        threadId,
        "assistant-2",
        "Streaming through T3.",
        "2026-07-01T10:05:00.000Z",
      );
      const fs = yield* FileSystem.FileSystem;
      const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
      yield* h.write(joinPath(instanceDir, "session.json"), [
        {
          sid: claudeId,
          instanceId: "inst-1",
          heartbeatAt: yield* Clock.currentTimeMillis,
          activeTurn: { turnId: "turn-3", startedAt: 0, transcriptOffset: 0 },
        },
      ]);
      yield* h.append(claudePath, [
        c.user("user-3", "Typed while bridged", "2026-07-01T10:06:00.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.notInclude(yield* h.texts(threadId), "Typed while bridged");

      // The process is gone: the thread catches up with what T3 never wrote,
      // and T3's own reply is not duplicated.
      yield* fs.remove(instanceDir, { recursive: true });
      yield* h.append(claudePath, [
        c.assistant("assistant-3", "Reply after the process ended.", "2026-07-01T10:07:00.000Z"),
      ]);
      yield* importer.sweepNow();
      const texts = yield* h.texts(threadId);
      assert.include(texts, "Typed while bridged");
      assert.include(texts, "Reply after the process ended.");
      assert.equal(texts.filter((text) => text === "Streaming through T3.").length, 1);
    }),
);

runtimeTest(
  "shows a running session's missed rows where they happened, and moves misplaced ones",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111c1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const timeline = h.threads.getThreadRecords(threadId, ["turnItems"]).pipe(
        Effect.map((records) =>
          records.turnItems
            .filter((item) => item.type === "user_message" || item.type === "assistant_message")
            .toSorted((left, right) => left.ordinal - right.ordinal)
            .map((item) => ({ text: item.text, run: Math.floor(item.ordinal / 1_000_000) })),
        ),
      );
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();

      // Rows from after T3's first run that an earlier build imported above
      // it: they are in the thread before the run is.
      yield* h.append(claudePath, [
        c.assistant("assistant-2", "Streaming through T3.", "2026-07-01T10:05:02.000Z"),
        c.user("user-3", "Typed with no process attached", "2026-07-01T10:06:00.000Z"),
        c.assistant("assistant-3", "Answered with no process.", "2026-07-01T10:06:05.000Z"),
      ]);
      yield* importer.sweepNow();
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:05:00.000Z",
      });
      assert.deepEqual(
        (yield* timeline).map((item) => item.run),
        [0, 0, 0, 0, 0],
      );

      // The session's process is gone when its next rows land: they go after
      // the run, and the misplaced ones move there too.
      yield* h.append(claudePath, [
        c.user("user-4", "Typed later, still no process", "2026-07-01T10:07:00.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.deepEqual(yield* timeline, [
        { text: "Fix the login bug please", run: 0 },
        { text: "I fixed the login bug.", run: 0 },
        { text: "Streaming through T3.", run: 1 },
        { text: "Typed with no process attached", run: 1 },
        { text: "Answered with no process.", run: 1 },
        { text: "Typed later, still no process", run: 1 },
      ]);

      // A process with the mod holds the session again and is idle. T3 ran a
      // second turn, then missed a terminal turn entirely.
      const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
      yield* h.write(joinPath(instanceDir, "session.json"), [
        { sid: claudeId, instanceId: "inst-1", heartbeatAt: yield* Clock.currentTimeMillis },
      ]);
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 2,
        iso: "2026-07-01T10:10:00.000Z",
      });
      yield* h.append(claudePath, [
        c.user("user-5", "Typed in the terminal", "2026-07-01T10:12:00.000Z"),
        c.assistant("assistant-5", "Terminal reply.", "2026-07-01T10:12:05.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.deepEqual((yield* timeline).slice(-2), [
        { text: "Typed in the terminal", run: 2 },
        { text: "Terminal reply.", run: 2 },
      ]);
      assert.equal((yield* timeline).length, 8);

      // The transcript changes without a new message: a later sweep writes nothing.
      const settled = yield* h.sequence;
      yield* h.append(claudePath, [{ type: "permission-mode", permissionMode: "default" }]);
      yield* importer.sweepNow();
      assert.equal(yield* h.sequence, settled);
    }),
);

runtimeTest(
  "a session that keeps working still gets what its thread lacks, and what was typed into its running turn",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111e1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();

      // T3 ran the thread once. Since then the session went on in its
      // terminal: one turn nothing was attached for, and one running now.
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      const turnStartedAt = "2026-07-01T10:10:00.000Z";
      yield* h.append(claudePath, [
        c.user("user-2", "A turn nothing was attached for", "2026-07-01T10:05:00.000Z"),
        c.assistant("assistant-2", "Its reply.", "2026-07-01T10:05:05.000Z"),
        c.user("user-3", "The turn running now", turnStartedAt),
        {
          type: "attachment",
          uuid: "queued-1",
          sessionId: claudeId,
          timestamp: "2026-07-01T10:10:30.000Z",
          isSidechain: false,
          attachment: {
            type: "queued_command",
            commandMode: "prompt",
            origin: { kind: "human" },
            prompt: "Typed into the running turn",
          },
        },
        c.assistant(
          "assistant-3",
          "Still arriving through the attachment.",
          "2026-07-01T10:10:40.000Z",
        ),
      ]);
      const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
      yield* h.write(joinPath(instanceDir, "session.json"), [
        {
          sid: claudeId,
          instanceId: "inst-1",
          heartbeatAt: yield* Clock.currentTimeMillis,
          activeTurn: {
            turnId: "turn-3",
            startedAt: DateTime.toEpochMillis(DateTime.makeUnsafe(turnStartedAt)),
            transcriptOffset: 0,
          },
        },
      ]);

      yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
      const texts = yield* h.texts(threadId);
      assert.include(texts, "A turn nothing was attached for");
      assert.include(texts, "Its reply.");
      assert.include(texts, "Typed into the running turn");
      // The turn in flight is the attachment's to show.
      assert.notInclude(texts, "The turn running now");
      assert.notInclude(texts, "Still arriving through the attachment.");

      // Asking again adds nothing, and neither does the sweep.
      yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
      yield* importer.sweepNow();
      assert.deepEqual(yield* h.texts(threadId), texts);

      // The turn ends with nothing having shown it. Its rows are added below
      // rows added on the earlier pass, and the thread still reads in the
      // order things were written.
      yield* h.write(joinPath(instanceDir, "session.json"), [
        {
          sid: claudeId,
          instanceId: "inst-1",
          heartbeatAt: yield* Clock.currentTimeMillis,
          activeTurn: null,
        },
      ]);
      yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
      const timeline = (yield* h.threads.getThreadRecords(threadId, ["turnItems"])).turnItems
        .filter((item) => item.type === "user_message" || item.type === "assistant_message")
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .map((item) => item.text);
      assert.deepEqual(timeline, [
        "Fix the login bug please",
        "I fixed the login bug.",
        "A turn nothing was attached for",
        "Its reply.",
        "The turn running now",
        "Typed into the running turn",
        "Still arriving through the attachment.",
      ]);
    }),
);

/** What the thread shows of the conversation, in the order it shows it. */
const conversation = (h: Harness, threadId: ThreadId) =>
  h.threads.getThreadRecords(threadId, ["turnItems"]).pipe(
    Effect.map((records) =>
      records.turnItems
        .filter((item) => item.type === "user_message" || item.type === "assistant_message")
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .map((item) => item.text),
    ),
  );

/**
 * What a run that showed a turn leaves in the thread: the prompt under T3's
 * own id, and each reply under the id of the record's row.
 */
const seedShownTurn = (
  h: Harness,
  threadId: ThreadId,
  input: {
    readonly runOrdinal: number;
    readonly prompt: string;
    readonly iso: string;
    readonly replies: ReadonlyArray<{
      readonly rowId: string;
      readonly text: string;
      readonly iso: string;
    }>;
  },
) =>
  Effect.gen(function* () {
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: input.runOrdinal,
      iso: input.iso,
    });
    yield* seedT3Prompt(h, threadId, input.runOrdinal, input.prompt, input.iso);
    const runId = seedRunPayload(threadId, CLAUDE, "completed", {
      ordinal: input.runOrdinal,
      iso: input.iso,
    }).runId;
    yield* h.eventSink.write({
      events: input.replies.map((reply) => {
        const seed = seedTimelineRow({
          threadId,
          key: `shown:${reply.rowId}`,
          messageId: MessageId.make(`message:provider:claudeAgent:native-item:${reply.rowId}`),
          role: "assistant",
          text: reply.text,
          at: DateTime.makeUnsafe(reply.iso),
          runId,
        });
        return {
          ...seed,
          payload: {
            ...seed.payload,
            id: TurnItemId.make(`turn-item:provider:claudeAgent:native-item:${reply.rowId}`),
          },
        };
      }),
    });
  });

const liveSessionFile = (
  h: Harness,
  sessionId: string,
  activeTurn: { readonly turnId: string; readonly startedAt: number } | null = null,
) =>
  Effect.gen(function* () {
    const instanceDir = yield* h.makeDir("bridge", "claude", sessionId, "inst-1");
    yield* h.write(joinPath(instanceDir, "session.json"), [
      {
        sid: sessionId,
        instanceId: "inst-1",
        heartbeatAt: yield* Clock.currentTimeMillis,
        activeTurn: activeTurn === null ? null : { ...activeTurn, transcriptOffset: 0 },
      },
    ]);
  });

runtimeTest(
  "a new session's first prompt is its run's to show, also when it was written before the turn was reported",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const prompt = "Reply with exactly: quick-1";
      const note = (uuid: string, type: string): Entry => ({
        type: "attachment",
        uuid,
        sessionId: claudeId,
        timestamp: "2026-07-01T10:00:00.000Z",
        isSidechain: false,
        attachment: { type },
      });
      // On a busy machine Claude Code had written the prompt, and the rows it
      // writes after one, before the turn was reported with the record's size.
      const written = [
        note("note-1", "environment"),
        c.user("user-1", prompt, "2026-07-01T10:00:00.000Z"),
        note("note-2", "skill_listing"),
      ];
      yield* h.write(claudePath, written);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.adoptLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        cwd: h.workspace,
        turnOffset: new TextEncoder().encode(jsonl(written)).length,
        promptText: prompt,
      });
      // The session has its thread, named by the prompt, with nothing in it:
      // the turn is the run's to show.
      assert.strictEqual((yield* h.liveShell(threadId)).title, prompt);
      assert.deepEqual(yield* conversation(h, threadId), []);

      // The run offered for the turn shows it; the record is compared after.
      yield* h.append(claudePath, [
        c.assistant("assistant-1", "quick-1", "2026-07-01T10:00:02.000Z"),
      ]);
      yield* seedShownTurn(h, threadId, {
        runOrdinal: 1,
        prompt,
        iso: "2026-07-01T10:00:01.000Z",
        replies: [{ rowId: "assistant-1", text: "quick-1", iso: "2026-07-01T10:00:02.000Z" }],
      });
      yield* liveSessionFile(h, claudeId);
      yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
      assert.deepEqual(yield* conversation(h, threadId), [prompt, "quick-1"]);
    }),
);

runtimeTest(
  "history before a new session's first turn still comes in, with an earlier prompt of the same words",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a2";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      // The session ran a turn before T3 knew it. The same words start the
      // turn in flight, whose own row is not written yet.
      const written = [
        c.user("user-1", "continue", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "Carried on.", "2026-07-01T10:00:05.000Z"),
      ];
      yield* h.write(claudePath, written);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.adoptLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        cwd: h.workspace,
        turnOffset: new TextEncoder().encode(jsonl(written)).length,
        promptText: "continue",
      });
      assert.deepEqual(yield* conversation(h, threadId), ["continue", "Carried on."]);
    }),
);

runtimeTest(
  "a turn that ended before its run attached is that run's to show, and history once no run comes",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a3";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const history = [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ];
      yield* h.write(claudePath, history);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      // Two short turns typed in the terminal, each over before the run T3
      // offered for it had attached.
      const firstStartedAt = "2026-07-01T10:10:00.000Z";
      yield* h.append(claudePath, [
        c.user("user-2", "Reply with exactly: quick-2", firstStartedAt),
        c.assistant("assistant-2", "quick-2", "2026-07-01T10:10:01.000Z"),
      ]);
      yield* liveSessionFile(h, claudeId);
      const heal = (turnAwaitingRunSince?: string) =>
        importer.healLiveClaudeSession({
          sessionId: claudeId,
          transcriptPath: claudePath,
          ...(turnAwaitingRunSince === undefined
            ? {}
            : {
                turnAwaitingRunSinceMs: DateTime.toEpochMillis(
                  DateTime.makeUnsafe(turnAwaitingRunSince),
                ),
              }),
        });
      const settled = ["Fix the login bug please", "I fixed the login bug."];

      // The record is compared while the first one's run is still to come.
      yield* heal(firstStartedAt);
      assert.deepEqual(yield* conversation(h, threadId), settled);
      // The run attaches and shows the turn from its start.
      yield* seedShownTurn(h, threadId, {
        runOrdinal: 2,
        prompt: "Reply with exactly: quick-2",
        iso: "2026-07-01T10:10:02.000Z",
        replies: [{ rowId: "assistant-2", text: "quick-2", iso: "2026-07-01T10:10:02.500Z" }],
      });
      yield* heal();
      const shownOnce = [...settled, "Reply with exactly: quick-2", "quick-2"];
      assert.deepEqual(yield* conversation(h, threadId), shownOnce);

      // No run ever comes for the second: left alone while one still could,
      // it is history after that.
      const secondStartedAt = "2026-07-01T10:20:00.000Z";
      yield* h.append(claudePath, [
        c.user("user-3", "Reply with exactly: quick-3", secondStartedAt),
        c.assistant("assistant-3", "quick-3", "2026-07-01T10:20:01.000Z"),
      ]);
      yield* heal(secondStartedAt);
      assert.deepEqual(yield* conversation(h, threadId), shownOnce);
      yield* heal();
      assert.deepEqual(yield* conversation(h, threadId), [
        ...shownOnce,
        "Reply with exactly: quick-3",
        "quick-3",
      ]);
    }),
);

runtimeTest(
  "a short turn over before T3 heard of it is left for its run on every path, and is history when none comes",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a5";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();
      const settled = ["Fix the login bug please", "I fixed the login bug."];
      // Nothing says a turn is awaiting its run: T3 has not read the turn's
      // events yet. The record and the mod's presence are all there is.
      const compared = (now: string, how: "record-change" | "sweep") =>
        Effect.gen(function* () {
          yield* TestClock.setTime(at(now));
          yield* liveSessionFile(h, claudeId);
          yield* how === "sweep"
            ? importer.sweepNow()
            : importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
          return yield* conversation(h, threadId);
        });

      // The first turn the session runs on a thread T3 only ever imported.
      yield* h.append(claudePath, [
        c.user("user-2", "Reply with exactly: quick-2", "2026-07-01T10:10:00.000Z"),
        c.assistant("assistant-2", "quick-2", "2026-07-01T10:10:01.000Z"),
      ]);
      assert.deepEqual(yield* compared("2026-07-01T10:10:05.000Z", "sweep"), settled);
      // Its run attaches late and shows the turn from its start.
      yield* seedShownTurn(h, threadId, {
        runOrdinal: 1,
        prompt: "Reply with exactly: quick-2",
        iso: "2026-07-01T10:10:07.000Z",
        replies: [{ rowId: "assistant-2", text: "quick-2", iso: "2026-07-01T10:10:07.500Z" }],
      });
      const shownOnce = [...settled, "Reply with exactly: quick-2", "quick-2"];
      assert.deepEqual(yield* compared("2026-07-01T10:10:08.000Z", "record-change"), shownOnce);
      assert.deepEqual(yield* compared("2026-07-01T10:11:00.000Z", "sweep"), shownOnce);

      // The next one, on a thread T3 has run: left alone on both paths while
      // a run could still show it, history once the record has been quiet.
      yield* h.append(claudePath, [
        c.user("user-3", "Reply with exactly: quick-3", "2026-07-01T10:20:00.000Z"),
        c.assistant("assistant-3", "quick-3", "2026-07-01T10:20:01.000Z"),
      ]);
      assert.deepEqual(yield* compared("2026-07-01T10:20:05.000Z", "record-change"), shownOnce);
      assert.deepEqual(yield* compared("2026-07-01T10:20:06.000Z", "sweep"), shownOnce);
      assert.deepEqual(yield* compared("2026-07-01T10:20:40.000Z", "sweep"), [
        ...shownOnce,
        "Reply with exactly: quick-3",
        "quick-3",
      ]);
    }),
);

runtimeTest(
  "a command still running in a live session is left for the turn that shows it, and is history if none does",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a4";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      const command = (uuid: string, name: string, iso: string): ReadonlyArray<Entry> => [
        // The line as typed, then the command's own row.
        c.user(`${uuid}-typed`, `/${name}`, iso),
        c.user(
          uuid,
          `<command-name>/${name}</command-name>\n            <command-message>${name}</command-message>\n            <command-args></command-args>`,
          iso,
        ),
      ];
      const printed = (uuid: string, text: string, iso: string): Entry => ({
        type: "system",
        subtype: "local_command",
        uuid,
        sessionId: claudeId,
        timestamp: iso,
        isSidechain: false,
        content: `<local-command-stdout>${text}</local-command-stdout>`,
      });
      const heal = (now: string) =>
        Effect.gen(function* () {
          yield* TestClock.setTime(at(now));
          yield* liveSessionFile(h, claudeId);
          yield* importer.healLiveClaudeSession({
            sessionId: claudeId,
            transcriptPath: claudePath,
          });
          return yield* conversation(h, threadId);
        });
      const lines = (shown: ReadonlyArray<string>, line: string) =>
        shown.filter((text) => text === line).length;

      // /compact typed in the terminal: two minutes pass before it prints.
      yield* h.append(claudePath, command("compact-1", "compact", "2026-07-01T10:10:00.000Z"));
      assert.strictEqual(lines(yield* heal("2026-07-01T10:10:40.000Z"), "/compact"), 0);
      assert.strictEqual(lines(yield* heal("2026-07-01T10:11:50.000Z"), "/compact"), 0);
      yield* h.append(claudePath, [
        printed(
          "compact-1-out",
          "Compacted (ctrl+o to see full summary)",
          "2026-07-01T10:12:00.000Z",
        ),
      ]);
      // Printed a moment ago: the live attachment is about to show it.
      assert.strictEqual(lines(yield* heal("2026-07-01T10:12:01.000Z"), "/compact"), 0);
      // It does, as a turn of its own with the command as its line.
      yield* seedShownTurn(h, threadId, {
        runOrdinal: 2,
        prompt: "/compact",
        iso: "2026-07-01T10:12:02.000Z",
        replies: [
          {
            rowId: "compact-1-out",
            text: "Compacted (ctrl+o to see full summary)",
            iso: "2026-07-01T10:12:02.000Z",
          },
        ],
      });
      const afterTurn = yield* heal("2026-07-01T10:13:00.000Z");
      assert.strictEqual(lines(afterTurn, "/compact"), 1);
      assert.strictEqual(lines(afterTurn, "Compacted (ctrl+o to see full summary)"), 1);

      // A command nothing ever shows live (it printed nothing, or nothing was
      // attached): held while it could still be running, history after that.
      yield* h.append(claudePath, command("rename-1", "rename", "2026-07-01T10:20:00.000Z"));
      assert.strictEqual(lines(yield* heal("2026-07-01T10:25:00.000Z"), "/rename"), 0);
      assert.strictEqual(lines(yield* heal("2026-07-01T10:31:00.000Z"), "/rename"), 1);
      // And one that printed with nothing attached to show it.
      yield* h.append(claudePath, [
        ...command("model-1", "model", "2026-07-01T10:40:00.000Z"),
        printed("model-1-out", "Set model to Sonnet", "2026-07-01T10:40:00.500Z"),
      ]);
      const later = yield* heal("2026-07-01T10:41:00.000Z");
      assert.strictEqual(lines(later, "/model"), 1);
      assert.strictEqual(lines(later, "Set model to Sonnet"), 1);

      // A compaction writes the command's own row only when it is over:
      // while it runs the record ends with the line as typed.
      yield* h.append(claudePath, [
        c.user("compact-2-typed", "/compact", "2026-07-01T10:50:00.000Z"),
      ]);
      assert.strictEqual(lines(yield* heal("2026-07-01T10:50:40.000Z"), "/compact"), 1);
      yield* h.append(claudePath, [
        command("compact-2", "compact", "2026-07-01T10:50:00.030Z")[1]!,
        printed(
          "compact-2-out",
          "Compacted (ctrl+o to see full summary)",
          "2026-07-01T10:52:00.000Z",
        ),
      ]);
      assert.strictEqual(lines(yield* heal("2026-07-01T10:52:01.000Z"), "/compact"), 1);
      yield* seedShownTurn(h, threadId, {
        runOrdinal: 3,
        prompt: "/compact",
        iso: "2026-07-01T10:52:02.000Z",
        replies: [
          {
            rowId: "compact-2-out",
            text: "Compacted (ctrl+o to see full summary)",
            iso: "2026-07-01T10:52:02.000Z",
          },
        ],
      });
      const twice = yield* heal("2026-07-01T10:53:00.000Z");
      assert.strictEqual(lines(twice, "/compact"), 2);
      assert.strictEqual(lines(twice, "Compacted (ctrl+o to see full summary)"), 2);
    }),
);

runtimeTest(
  "a typed command the live attachment left to the record goes in at once, before what T3 sent after it",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a7";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      // /plan typed in the terminal (Claude Code 2.1.300's two rows), then,
      // while the server could not read them, a message sent from T3 whose
      // run is under way.
      yield* h.append(claudePath, [
        c.user(
          "plan-1",
          "<command-name>/plan</command-name>\n            <command-message>plan</command-message>\n            <command-args></command-args>",
          "2026-07-01T10:10:00.000Z",
        ),
        c.user(
          "plan-1-out",
          "<local-command-stdout>Enabled plan mode</local-command-stdout>",
          "2026-07-01T10:10:00.000Z",
        ),
      ]);
      yield* seedT3Run(h, threadId, CLAUDE, "running", {
        ordinal: 2,
        iso: "2026-07-01T10:10:08.000Z",
      });
      yield* seedT3Prompt(h, threadId, 2, "Reply with exactly: after", "2026-07-01T10:10:08.000Z");
      yield* TestClock.setTime(at("2026-07-01T10:10:08.500Z"));
      yield* liveSessionFile(h, claudeId);

      // A command printed a moment ago is the attachment's to show.
      yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
      assert.notInclude(yield* conversation(h, threadId), "/plan");
      // Left to the record, it goes in now, once, where it was written.
      yield* importer.healLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        leftToRecord: ["plan-1-out"],
      });
      yield* importer.healLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        leftToRecord: ["plan-1-out"],
      });
      assert.deepStrictEqual(yield* conversation(h, threadId), [
        "Fix the login bug please",
        "I fixed the login bug.",
        "/plan",
        "Enabled plan mode",
        "Reply with exactly: after",
      ]);
    }),
);

runtimeTest(
  "a reply the run of a turn over before adoption missed stays below its prompt, healed again and again",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111a8";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));
      const prompt = "Reply with exactly: tr-one";
      // typedrace.py e501e525: the first turn was typed and answered, and was
      // over, before T3 took the session in; the run offered for it attached
      // too late to show it from its start, and shows only its prompt.
      yield* TestClock.setTime(at("2026-07-01T10:00:00.000Z"));
      yield* h.write(claudePath, [
        c.user("user-1", prompt, "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "tr-one", "2026-07-01T10:00:01.500Z"),
      ]);
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* TestClock.setTime(at("2026-07-01T10:00:17.000Z"));
      yield* importer.adoptLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        cwd: h.workspace,
        turnOffset: 0,
        promptText: prompt,
      });
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:00:17.000Z",
      });
      yield* seedT3Prompt(h, threadId, 1, prompt, "2026-07-01T10:00:17.000Z");
      // Then /plan typed in the terminal, which the attachment leaves to the record.
      yield* h.append(claudePath, [
        c.user(
          "plan-1",
          "<command-name>/plan</command-name>\n            <command-message>plan</command-message>\n            <command-args></command-args>",
          "2026-07-01T10:00:24.000Z",
        ),
        c.user(
          "plan-1-out",
          "<local-command-stdout>Enabled plan mode</local-command-stdout>",
          "2026-07-01T10:00:24.000Z",
        ),
      ]);
      const shown = [prompt, "tr-one", "/plan", "Enabled plan mode"];
      yield* TestClock.setTime(at("2026-07-01T10:00:24.250Z"));
      yield* liveSessionFile(h, claudeId);
      yield* importer.healLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: claudePath,
        leftToRecord: ["plan-1-out"],
      });
      assert.deepStrictEqual(yield* conversation(h, threadId), shown);
      // The comparisons after it keep the reply where it went in.
      for (const later of ["2026-07-01T10:00:31.000Z", "2026-07-01T10:01:30.000Z"]) {
        yield* TestClock.setTime(at(later));
        yield* liveSessionFile(h, claudeId);
        yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
        assert.deepStrictEqual(yield* conversation(h, threadId), shown);
      }
    }),
);

runtimeTest(
  "a prompt the session took back leaves the thread as a run T3 rewinds does, from the thread's floor on",
  (h) =>
    Effect.gen(function* () {
      const at = (iso: string) => DateTime.toEpochMillis(DateTime.makeUnsafe(iso));
      // Both sessions began before this server did (T3 adopting sessions
      // already under way): the floor is where the record ends when the
      // thread is first compared with it. (One that began while the server
      // ran is followed from its first line.)
      yield* TestClock.setTime(at("2026-07-01T10:00:40.000Z"));
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      const session = (claudeId: string) => {
        const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
        const threadId = importedThreadId(claudeId);
        const claudePath = h.claudeFile(claudeId);
        const after = (parentUuid: string, row: Entry): Entry => ({ ...row, parentUuid });
        return {
          threadId,
          claudePath,
          history: [
            c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
            after("user-1", c.assistant("assistant-1", "Fixed.", "2026-07-01T10:00:05.000Z")),
          ],
          // Sent, taken back with Esc (the record keeps the row, and the
          // harness's own rows after it), then sent again in more words from
          // where it had been sent.
          takenBack: [
            after(
              "assistant-1",
              c.user("taken-back", "Put the rig under git", "2026-07-01T10:05:00.000Z"),
            ),
            {
              type: "attachment",
              uuid: "hook-cancelled",
              parentUuid: "taken-back",
              sessionId: claudeId,
              timestamp: "2026-07-01T10:05:01.000Z",
              isSidechain: false,
              attachment: { type: "hook_cancelled" },
            },
            after(
              "assistant-1",
              c.user(
                "resent",
                "Put the rig under git, then the fresh cases",
                "2026-07-01T10:05:40.000Z",
              ),
            ),
            after("resent", c.assistant("resent-reply", "Done.", "2026-07-01T10:05:50.000Z")),
          ],
          heal: (now: string) =>
            Effect.gen(function* () {
              yield* TestClock.setTime(at(now));
              yield* liveSessionFile(h, claudeId);
              yield* importer.healLiveClaudeSession({
                sessionId: claudeId,
                transcriptPath: claudePath,
              });
            }),
          // What the bridge left: a run stopped with nothing but the prompt,
          // then the resend's run with its reply.
          seedRuns: Effect.gen(function* () {
            const stopped = seedRunPayload(threadId, CLAUDE, "completed", {
              ordinal: 2,
              iso: "2026-07-01T10:05:00.300Z",
            });
            yield* h.eventSink.write({
              events: [
                {
                  id: EventId.make(`seed:${threadId}:run:2`),
                  type: "run.created",
                  threadId,
                  runId: stopped.runId,
                  driver: CLAUDE,
                  providerInstanceId: stopped.instanceId,
                  occurredAt: stopped.at,
                  payload: { ...stopped.payload, status: "interrupted" },
                },
              ],
            });
            yield* seedT3Prompt(
              h,
              threadId,
              2,
              "Put the rig under git",
              "2026-07-01T10:05:00.300Z",
            );
            yield* seedShownTurn(h, threadId, {
              runOrdinal: 3,
              prompt: "Put the rig under git, then the fresh cases",
              iso: "2026-07-01T10:05:40.300Z",
              replies: [{ rowId: "resent-reply", text: "Done.", iso: "2026-07-01T10:05:50.000Z" }],
            });
          }),
          shown: h.threads.getThreadRecords(threadId, ["runs", "attempts", "turnItems"]).pipe(
            Effect.map((records) => ({
              runs: records.runs
                .toSorted((left, right) => left.ordinal - right.ordinal)
                .map((run) => `${run.ordinal}: ${run.status}`),
              said: records.turnItems
                .filter(
                  (item) =>
                    item.type === "user_message" &&
                    isOrchestrationV2TurnItemVisible({
                      item,
                      runs: records.runs,
                      attempts: records.attempts,
                      items: records.turnItems,
                    }),
                )
                .toSorted((left, right) => left.ordinal - right.ordinal)
                .map((item) => (item.type === "user_message" ? item.text : "")),
            })),
          ),
        };
      };

      // Taken back after the thread was first compared with the record.
      const live = session("aaaaaaaa-1111-4111-8111-1111111111e5");
      yield* h.write(live.claudePath, live.history);
      yield* importer.sweepNow();
      // A turn T3 ran, then the thread is first compared with the record: its floor.
      yield* seedT3Run(h, live.threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      yield* live.heal("2026-07-01T10:01:00.000Z");
      yield* h.append(live.claudePath, live.takenBack);
      yield* live.seedRuns;
      yield* live.heal("2026-07-01T10:07:00.000Z");
      assert.deepEqual(yield* live.shown, {
        runs: ["1: completed", "2: rolled_back", "3: completed"],
        said: ["Fix the login bug please", "Put the rig under git, then the fresh cases"],
      });
      const settled = yield* h.sequence;
      yield* live.heal("2026-07-01T10:08:00.000Z");
      assert.strictEqual(yield* h.sequence, settled);

      // Taken back before: the thread's history stays as it was.
      const before = session("aaaaaaaa-1111-4111-8111-1111111111e6");
      yield* h.write(before.claudePath, [...before.history, ...before.takenBack]);
      yield* importer.sweepNow();
      yield* seedT3Run(h, before.threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:01:00.000Z",
      });
      yield* before.seedRuns;
      yield* before.heal("2026-07-01T10:07:00.000Z");
      yield* before.heal("2026-07-01T10:08:00.000Z");
      assert.deepEqual((yield* before.shown).runs, [
        "1: completed",
        "2: interrupted",
        "3: completed",
      ]);
    }),
);

runtimeTest("a stock message T3 once showed as the person's own becomes a notice", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f1";
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const threadId = importedThreadId(claudeId);
    yield* h.write(claudePath, [
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ]);
    const bridgeDir = yield* h.makeDir("bridge");
    const importer = yield* h.importer({ bridgeDir });
    yield* importer.sweepNow();
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: 1,
      iso: "2026-07-01T10:01:00.000Z",
    });
    // What an earlier build wrote for a turn it picked up after a restart.
    const at = DateTime.makeUnsafe("2026-07-01T10:01:00.000Z");
    const stock = seedTimelineRow({
      threadId,
      key: "stock",
      messageId: MessageId.make(`seed-stock:${threadId}`),
      role: "user",
      text: "Background task completed.",
      at,
      runId: null,
    });
    yield* h.eventSink.write({
      events: [
        stock.payload.type === "user_message"
          ? { ...stock, payload: { ...stock.payload, creationSource: "provider" as const } }
          : stock,
      ],
    });
    const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
    yield* h.write(joinPath(instanceDir, "session.json"), [
      {
        sid: claudeId,
        instanceId: "inst-1",
        heartbeatAt: yield* Clock.currentTimeMillis,
        activeTurn: null,
      },
    ]);
    const rows = h.threads.getThreadRecords(threadId, ["turnItems"]).pipe(
      Effect.map((records) =>
        records.turnItems
          .filter((item) => item.type === "user_message" || item.type === "notification")
          .map((item) =>
            item.type === "user_message" ? `said: ${item.text}` : `notice: ${item.summary}`,
          )
          .toSorted(),
      ),
    );
    assert.include(yield* rows, "said: Background task completed.");

    yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
    assert.deepEqual(yield* rows, [
      "notice: Background activity updated",
      "said: Fix the login bug please",
    ]);
  }),
);

// Claude Code stamps each prompt row with where it came from (2.1.302): a
// coordinating session's hand-off and a plan to implement are no line of the
// person's, whatever their words.
runtimeTest(
  "a prompt its stamp says someone else sent, and a plan to implement, import as notices",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f4";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const threadId = importedThreadId(claudeId);
      yield* h.write(h.claudeFile(claudeId), [
        {
          ...c.user(
            "plan-1",
            "Implement the following plan:\n\n1. Do it.\n\n\nIf you need specific details from before exiting plan mode, read the full transcript at: /x.jsonl",
            "2026-07-01T10:00:00.000Z",
          ),
          origin: { kind: "auto-continuation" },
          planContent: "1. Do it.\n",
        },
        c.assistant("assistant-1", "Done.", "2026-07-01T10:00:05.000Z"),
        {
          ...c.user("hand-off-1", "Build the login page.", "2026-07-01T10:01:00.000Z"),
          origin: { kind: "coordinator" },
        },
        c.assistant("assistant-2", "Built.", "2026-07-01T10:01:05.000Z"),
      ]);
      yield* (yield* h.importer()).sweepNow();
      const records = yield* h.threads.getThreadRecords(threadId, ["turnItems"]);
      assert.deepEqual(
        records.turnItems
          .toSorted((left, right) => left.ordinal - right.ordinal)
          .map((item) =>
            item.type === "notification"
              ? `notice: ${item.summary} | ${item.detail ?? ""}`
              : item.type === "user_message"
                ? `said: ${item.text}`
                : item.type === "assistant_message"
                  ? `reply: ${item.text}`
                  : item.type,
          ),
        [
          "notice: Plan to implement | 1. Do it.",
          "reply: Done.",
          "notice: coordinator | Build the login page.",
          "reply: Built.",
        ],
      );
    }),
);

runtimeTest(
  "a background task reporting back is a notice in imported history, and one filed as the person's own becomes one",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f3";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      const report = (summary: string) =>
        `<task-notification>\n<task-id>b1</task-id>\n<status>completed</status>\n<summary>${summary}</summary>\n</task-notification>`;
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "Started the build.", "2026-07-01T10:00:05.000Z"),
        {
          ...c.user(
            "task-1",
            report('Background command "build" completed (exit code 0)'),
            "2026-07-01T10:02:00.000Z",
          ),
          origin: { kind: "task-notification" },
          promptSource: "system",
        },
        c.assistant("assistant-2", "The build passed.", "2026-07-01T10:02:05.000Z"),
        // Words the person typed stay theirs, whatever they open with: the
        // record says who wrote a row.
        {
          ...c.user(
            "user-2",
            "<task-notification> rows: what are they?",
            "2026-07-01T10:02:30.000Z",
          ),
          origin: { kind: "human" },
          promptSource: "typed",
        },
        c.assistant("assistant-3", "The harness writes them.", "2026-07-01T10:02:35.000Z"),
      ]);
      const shown = h.threads
        .getThreadRecords(threadId, ["turnItems"])
        .pipe(
          Effect.map((records) =>
            records.turnItems
              .toSorted((left, right) => left.ordinal - right.ordinal)
              .flatMap((item) =>
                item.type === "user_message"
                  ? [`said: ${item.text}`]
                  : item.type === "assistant_message"
                    ? [`reply: ${item.text}`]
                    : item.type === "notification"
                      ? [`notice (${item.outcome}): ${item.summary}`]
                      : [],
              ),
          ),
        );
      yield* (yield* h.importer()).sweepNow();
      const imported = [
        "said: Fix the login bug please",
        "reply: Started the build.",
        'notice (completed): Background command "build" completed (exit code 0)',
        "reply: The build passed.",
        "said: <task-notification> rows: what are they?",
        "reply: The harness writes them.",
      ];
      assert.deepEqual(yield* shown, imported);

      // What an earlier build wrote for such a row: the person's message, raw.
      const at = DateTime.makeUnsafe("2026-07-01T10:03:00.000Z");
      const filed = seedTimelineRow({
        threadId,
        key: "old-report",
        messageId: MessageId.make(`seed-old-report:${threadId}`),
        role: "user",
        text: report('Agent "Stage 2" finished'),
        at,
        runId: null,
      });
      yield* h.eventSink.write({
        events: [
          filed.payload.type === "user_message"
            ? { ...filed, payload: { ...filed.payload, creationSource: "server" as const } }
            : filed,
        ],
      });
      assert.include(yield* shown, `said: ${report('Agent "Stage 2" finished')}`);

      // The next start puts it right, once.
      const restarted = yield* h.importer();
      yield* restarted.sweepNow();
      const repaired = yield* shown;
      assert.include(repaired, 'notice (completed): Agent "Stage 2" finished');
      assert.deepEqual(
        repaired.filter((row) => row.startsWith("said: ")),
        ["said: Fix the login bug please", "said: <task-notification> rows: what are they?"],
      );
      const settled = yield* h.sequence;
      yield* restarted.sweepNow();
      yield* (yield* h.importer()).sweepNow();
      assert.strictEqual(yield* h.sequence, settled);
      assert.deepEqual(yield* shown, repaired);
    }),
);

runtimeTest(
  "a task's report filed as the person's own becomes a notice also where the session's record is gone",
  (h) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f6";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      yield* (yield* h.importer()).sweepNow();
      const filed = seedTimelineRow({
        threadId,
        key: "old-report",
        messageId: MessageId.make(`seed-old-report:${threadId}`),
        role: "user",
        text: '<task-notification>\n<task-id>m1</task-id>\n<summary>Monitor event: "deploy log"</summary>\n<event>done</event>\n</task-notification>',
        at: DateTime.makeUnsafe("2026-07-01T10:03:00.000Z"),
        runId: null,
      });
      yield* h.eventSink.write({
        events: [
          filed.payload.type === "user_message"
            ? { ...filed, payload: { ...filed.payload, creationSource: "server" as const } }
            : filed,
        ],
      });
      // Claude Code clears old records away; the thread stays.
      yield* fs.remove(claudePath);
      const rows = h.threads.getThreadRecords(threadId, ["turnItems"]).pipe(
        Effect.map((records) =>
          records.turnItems
            .filter((item) => item.type === "user_message" || item.type === "notification")
            .map((item) =>
              item.type === "user_message" ? `said: ${item.text}` : `notice: ${item.summary}`,
            )
            .toSorted(),
        ),
      );
      const restarted = yield* h.importer();
      yield* restarted.sweepNow();
      assert.deepEqual(yield* rows, [
        'notice: Monitor event: "deploy log"',
        "said: Fix the login bug please",
      ]);
      const settled = yield* h.sequence;
      yield* restarted.sweepNow();
      yield* (yield* h.importer()).sweepNow();
      assert.strictEqual(yield* h.sequence, settled);
    }),
);

runtimeTest(
  "a session a mod process holds gives its thread its own model and modes, at import and when first seen",
  (h) =>
    Effect.gen(function* () {
      const held = "aaaaaaaa-1111-4111-8111-1111111111f4";
      const later = "aaaaaaaa-1111-4111-8111-1111111111f5";
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      const record = (sessionId: string, mode: string) => {
        const c = claudeSession({ sessionId, cwd: h.workspace });
        return [
          { type: "permission-mode", permissionMode: mode, sessionId },
          c.user(`${sessionId}-u1`, "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
          c.assistant(`${sessionId}-a1`, "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
        ];
      };
      const holdSession = (sessionId: string, model: string, permissionMode: string | null) =>
        Effect.gen(function* () {
          const instanceDir = yield* h.makeDir("bridge", "claude", sessionId, "inst-1");
          yield* h.write(joinPath(instanceDir, "session.json"), [
            {
              sid: sessionId,
              instanceId: "inst-1",
              heartbeatAt: yield* Clock.currentTimeMillis,
              activeTurn: null,
              model,
              ...(permissionMode === null ? {} : { permissionMode }),
            },
          ]);
        });
      const settings = (sessionId: string) =>
        h.threads.getThreadRecords(importedThreadId(sessionId), []).pipe(
          Effect.map(({ thread }) => ({
            model: thread.modelSelection.model,
            options: thread.modelSelection.options ?? [],
            runtimeMode: thread.runtimeMode,
            interactionMode: thread.interactionMode,
            updatedAt: DateTime.formatIso(thread.updatedAt),
          })),
        );

      // A copy made with /fork: a mod process holds it, no turn has run, and
      // nothing is attached. The record's note of the mode outranks the mod's.
      yield* h.write(h.claudeFile(held), record(held, "acceptEdits"));
      yield* holdSession(held, "claude-haiku-5-5[1m]", "default");
      // A session nothing holds is imported as before.
      yield* h.write(h.claudeFile(later), record(later, "default"));
      yield* importer.sweepNow();
      const heldSettings = yield* settings(held);
      assert.deepEqual(
        { ...heldSettings, updatedAt: null },
        {
          model: "claude-haiku-5-5",
          options: [{ id: "contextWindow", value: "1m" }],
          runtimeMode: "auto-accept-edits",
          interactionMode: "default",
          updatedAt: null,
        },
      );
      const before = yield* settings(later);
      assert.strictEqual(before.runtimeMode, "full-access");

      // Its terminal is opened again and the mode changed there, before any
      // turn runs: the thread T3 never ran shows what the session says.
      yield* holdSession(later, "claude-sonnet-5-5", null);
      yield* h.append(h.claudeFile(later), [
        { type: "permission-mode", permissionMode: "plan", sessionId: later },
      ]);
      yield* importer.sweepNow();
      const after = yield* settings(later);
      assert.deepEqual(after, {
        model: "claude-sonnet-5-5",
        options: [],
        runtimeMode: "full-access",
        interactionMode: "plan",
        // Seeing a session is not activity in its thread.
        updatedAt: before.updatedAt,
      });
      // Seen again with nothing changed, both stay as they are.
      yield* h.append(h.claudeFile(later), [
        { type: "permission-mode", permissionMode: "plan", sessionId: later },
      ]);
      yield* importer.sweepNow();
      assert.deepEqual(yield* settings(later), after);
      assert.deepEqual(yield* settings(held), heldSettings);
    }),
);

runtimeTest("a reply shown from the terminal and again from the record is left showing once", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f2";
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const threadId = importedThreadId(claudeId);
    const history = [
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ];
    yield* h.write(claudePath, history);
    const bridgeDir = yield* h.makeDir("bridge");
    const importer = yield* h.importer({ bridgeDir });
    yield* importer.sweepNow();
    // The session goes on from here with T3 attached: these rows reach the
    // thread through the attachment, not through the import.
    yield* h.write(claudePath, [
      ...history,
      c.user("user-2", "And the logout bug", "2026-07-01T10:01:00.000Z"),
      c.assistant("assistant-2", "Looking at logout now.", "2026-07-01T10:01:05.000Z"),
      c.assistant("assistant-3", "Done.", "2026-07-01T10:01:30.000Z"),
      c.assistant("assistant-4", "Done.", "2026-07-01T10:01:40.000Z"),
      c.assistant("assistant-5", "Checking the tests.", "2026-07-01T10:02:00.000Z"),
      c.assistant("assistant-6", "All green.", "2026-07-01T10:30:00.000Z"),
    ]);
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: 1,
      iso: "2026-07-01T10:01:00.000Z",
    });
    yield* seedT3Run(h, threadId, CLAUDE, "running", {
      ordinal: 2,
      iso: "2026-07-01T10:03:00.000Z",
    });
    const firstRun = RunId.make(`seed-run:${threadId}`);
    const laterRun = RunId.make(`seed-run:${threadId}:2`);
    // What the live attachment left: `drawn` rows carry the terminal's own
    // message ids, the others the record's row ids.
    const live = (input: {
      readonly key: string;
      readonly nativeId: string;
      readonly text: string;
      readonly ordinal: number;
      readonly iso: string;
      readonly runId: RunId;
    }) => {
      const seed = seedTimelineRow({
        threadId,
        key: input.key,
        // As the live attachment names a reply: by the id of what it came from.
        messageId: MessageId.make(`message:provider:claudeAgent:native-item:${input.nativeId}`),
        role: "assistant",
        text: input.text,
        at: DateTime.makeUnsafe(input.iso),
        runId: input.runId,
      });
      return {
        ...seed,
        payload: {
          ...seed.payload,
          id: TurnItemId.make(`turn-item:provider:claudeAgent:native-item:${input.nativeId}`),
          ordinal: input.ordinal,
        },
      };
    };
    const rowA = {
      key: "row-a",
      nativeId: "assistant-2",
      text: "Looking at  logout now.",
      ordinal: 0,
      iso: "2026-07-01T10:01:07.000Z",
    };
    yield* h.eventSink.write({
      events: [
        // Drawn, then its row right after it. A restart since has read the
        // row again and tagged it with the later run: its place is the same,
        // its run no longer is.
        live({
          key: "drawn-a",
          nativeId: "drawn-message-a",
          text: "Looking at logout now.",
          ordinal: 0,
          iso: "2026-07-01T10:01:07.000Z",
          runId: firstRun,
        }),
        live({ ...rowA, runId: firstRun }),
        {
          ...live({ ...rowA, runId: laterRun }),
          id: EventId.make(`seed:${threadId}:item:row-a:read-again`),
        },
        // Two replies that say the same thing, both the record's own.
        live({
          key: "done-1",
          nativeId: "assistant-3",
          text: "Done.",
          ordinal: 0,
          iso: "2026-07-01T10:01:30.000Z",
          runId: firstRun,
        }),
        live({
          key: "done-2",
          nativeId: "assistant-4",
          text: "Done.",
          ordinal: 0,
          iso: "2026-07-01T10:01:40.000Z",
          runId: firstRun,
        }),
        // Drawn in the first run; its row was first read after a restart and
        // landed at the top of the later run.
        live({
          key: "drawn-b",
          nativeId: "drawn-message-b",
          text: "Checking the tests.",
          ordinal: 0,
          iso: "2026-07-01T10:02:02.000Z",
          runId: firstRun,
        }),
        // The same words drawn half an hour before a row of the record: two
        // replies, not one.
        live({
          key: "drawn-c",
          nativeId: "drawn-message-c",
          text: "All green.",
          ordinal: 0,
          iso: "2026-07-01T10:02:10.000Z",
          runId: firstRun,
        }),
        live({
          key: "row-b",
          nativeId: "assistant-5",
          text: "Checking the tests.",
          ordinal: 0,
          iso: "2026-07-01T10:03:01.000Z",
          runId: laterRun,
        }),
        live({
          key: "row-c",
          nativeId: "assistant-6",
          text: "All green.",
          ordinal: 0,
          iso: "2026-07-01T10:30:00.000Z",
          runId: laterRun,
        }),
      ],
    });
    const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
    yield* h.write(joinPath(instanceDir, "session.json"), [
      {
        sid: claudeId,
        instanceId: "inst-1",
        heartbeatAt: yield* Clock.currentTimeMillis,
        activeTurn: null,
      },
    ]);
    const shown = h.threads.getThreadRecords(threadId, ["turnItems"]).pipe(
      Effect.map((records) =>
        records.turnItems
          .flatMap((item) =>
            item.type === "assistant_message" && item.id.includes(":native-item:")
              ? [
                  {
                    id: item.id.slice(item.id.lastIndexOf(":") + 1),
                    at: item.ordinal,
                    text: item.text,
                  },
                ]
              : [],
          )
          .toSorted((left, right) => left.at - right.at || left.id.localeCompare(right.id))
          .filter((item) => item.text.length > 0)
          .map((item) => `${item.at} ${item.id}: ${item.text}`),
      ),
    );

    yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
    // Places are the thread's own: five rows in the first run's band in the
    // order they arrived (the drawn copy of the first reply holds the first),
    // then the later run's.
    assert.deepEqual(yield* shown, [
      "1000002 assistant-2: Looking at  logout now.",
      "1000003 assistant-3: Done.",
      "1000004 assistant-4: Done.",
      "1000005 assistant-5: Checking the tests.",
      "1000006 drawn-message-c: All green.",
      "2000002 assistant-6: All green.",
    ]);
    // Each reply the record holds is in the thread once, whatever its row is called.
    const replies = (yield* h.threads.getThreadRecords(threadId, ["turnItems"])).turnItems.flatMap(
      (item) => (item.type === "assistant_message" && item.text.length > 0 ? [item.text] : []),
    );
    assert.strictEqual(replies.filter((text) => text.includes("logout now")).length, 1);
    assert.strictEqual(replies.filter((text) => text === "Checking the tests.").length, 1);
    assert.strictEqual(replies.filter((text) => text === "Done.").length, 2);
    // A second pass changes nothing.
    yield* importer.healLiveClaudeSession({ sessionId: claudeId, transcriptPath: claudePath });
    assert.strictEqual((yield* shown).length, 6);
  }),
);

/**
 * A turn imported as history, then shown again from its start by a run that
 * picked the session up: both replies are there twice, what was typed sits
 * above the run instead of in it, and a line a later run read sits below.
 */
const turnShownTwice = (h: Harness, claudeId: string) =>
  Effect.gen(function* () {
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const threadId = importedThreadId(claudeId);
    const thinking = (uuid: string, messageId: string, at: string): Entry => ({
      ...c.assistant(uuid, "", at),
      message: { id: messageId, role: "assistant", content: [{ type: "thinking", thinking: "" }] },
    });
    const halfway = c.assistant("assistant-p", "Halfway there.", "2026-07-01T10:00:20.000Z");
    const record = [
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "Looking at login.", "2026-07-01T10:00:05.000Z"),
      thinking("thinking-1", "msg_one", "2026-07-01T10:00:10.000Z"),
      halfway,
      c.user("user-2", "And the logout bug", "2026-07-01T10:00:30.000Z"),
      thinking("thinking-2", "msg_two", "2026-07-01T10:00:40.000Z"),
      c.assistant("assistant-2", "Both fixed.", "2026-07-01T10:00:50.000Z"),
    ];
    // The import showed no progress line.
    yield* h.write(
      claudePath,
      record.filter((entry) => entry !== halfway),
    );
    const bridgeDir = yield* h.makeDir("bridge");
    const sweep = yield* h.importer({ bridgeDir });
    yield* sweep.sweepNow();
    yield* h.write(claudePath, record);
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: 1,
      iso: "2026-07-01T10:02:00.000Z",
    });
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: 2,
      iso: "2026-07-01T10:04:00.000Z",
    });
    const firstRun = RunId.make(`seed-run:${threadId}`);
    const laterRun = RunId.make(`seed-run:${threadId}:2`);
    const delivered = (nativeId: string, text: string, iso: string, runId: RunId) => {
      const seed = seedTimelineRow({
        threadId,
        key: `${nativeId}:${runId}`,
        messageId: MessageId.make(`message:provider:claudeAgent:native-item:${nativeId}`),
        role: "assistant",
        text,
        at: DateTime.makeUnsafe(iso),
        runId,
      });
      return {
        ...seed,
        payload: {
          ...seed.payload,
          id: TurnItemId.make(`turn-item:provider:claudeAgent:native-item:${nativeId}`),
        },
      };
    };
    const reasoned = (messageId: string, iso: string) => {
      const at = DateTime.makeUnsafe(iso);
      return {
        id: EventId.make(`seed:${threadId}:item:${messageId}`),
        type: "turn-item.updated" as const,
        threadId,
        occurredAt: at,
        payload: {
          id: TurnItemId.make(
            `turn-item:provider:claudeAgent:native-item:${messageId}%3Athinking%3A0`,
          ),
          threadId,
          runId: firstRun,
          nodeId: null,
          providerThreadId: null,
          providerTurnId: null,
          nativeItemRef: null,
          parentItemId: null,
          ordinal: 0,
          status: "completed" as const,
          title: null,
          startedAt: at,
          completedAt: at,
          updatedAt: at,
          type: "reasoning" as const,
          text: messageId,
          streaming: false,
        },
      };
    };
    yield* h.eventSink.write({
      events: [
        delivered("assistant-1", "Looking at login.", "2026-07-01T10:02:01.000Z", firstRun),
        reasoned("msg_one", "2026-07-01T10:02:02.000Z"),
        reasoned("msg_two", "2026-07-01T10:02:03.000Z"),
        delivered("assistant-2", "Both fixed.", "2026-07-01T10:02:04.000Z", firstRun),
        delivered("assistant-p", "Halfway there.", "2026-07-01T10:04:01.000Z", laterRun),
      ],
    });
    const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
    yield* h.write(joinPath(instanceDir, "session.json"), [
      {
        sid: claudeId,
        instanceId: "inst-1",
        heartbeatAt: yield* Clock.currentTimeMillis,
        activeTurn: null,
      },
    ]);
    /** What the thread reads as, top to bottom; a row with nothing in it draws nothing. */
    const reads = h.threads
      .getThreadRecords(threadId, ["turnItems"])
      .pipe(
        Effect.map((records) =>
          records.turnItems
            .toSorted((left, right) => left.ordinal - right.ordinal)
            .flatMap((item) =>
              (item.type === "user_message" ||
                item.type === "assistant_message" ||
                item.type === "reasoning") &&
              item.text.length > 0
                ? [`${Math.floor(item.ordinal / 1_000_000)} ${item.text}`]
                : [],
            ),
        ),
      );
    const places = h.threads
      .getThreadRecords(threadId, ["turnItems"])
      .pipe(
        Effect.map((records) =>
          records.turnItems.map((item) => `${item.id} ${item.ordinal}`).toSorted(),
        ),
      );
    return { threadId, claudePath, bridgeDir, reads, places };
  });

const DAMAGED_TURN = [
  "0 Fix the login bug please",
  "0 Looking at login.",
  "0 And the logout bug",
  "0 Both fixed.",
  "1 Looking at login.",
  "1 msg_one",
  "1 msg_two",
  "1 Both fixed.",
  "2 Halfway there.",
];

runtimeTest(
  "a turn imported and then shown again by a run reads once, in the order the record wrote it",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f3";
      const turn = yield* turnShownTwice(h, claudeId);
      assert.deepEqual(yield* turn.reads, DAMAGED_TURN);
      const importer = yield* h.importer({
        bridgeDir: turn.bridgeDir,
        recordOrderThreads: new Set([turn.threadId]),
      });
      const heal = importer.healLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: turn.claudePath,
      });
      const rowsBefore = (yield* turn.places).length;

      yield* heal;
      // The first prompt was written before anything the run shows and stays
      // above it; the rest sit in the run, between the rows they were written
      // between, and each reply is there once.
      assert.deepEqual(yield* turn.reads, [
        "0 Fix the login bug please",
        "1 Looking at login.",
        "1 msg_one",
        "1 Halfway there.",
        "1 And the logout bug",
        "1 msg_two",
        "1 Both fixed.",
      ]);
      // Nothing is taken out of the thread, and no two rows share a place.
      const places = yield* turn.places;
      assert.strictEqual(places.length, rowsBefore);
      assert.strictEqual(new Set(places.map((place) => place.split(" ")[1])).size, places.length);

      // A second pass writes nothing.
      const written = yield* h.sequence;
      yield* heal;
      assert.strictEqual(yield* h.sequence, written);
      assert.deepEqual(yield* turn.places, places);
    }),
);

runtimeTest(
  "a thread not kept in the record's order still shows a twice-shown reply once, and moves nothing",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f4";
      const turn = yield* turnShownTwice(h, claudeId);
      const importer = yield* h.importer({ bridgeDir: turn.bridgeDir });
      const places = yield* turn.places;
      yield* importer.healLiveClaudeSession({
        sessionId: claudeId,
        transcriptPath: turn.claudePath,
      });
      assert.deepEqual(yield* turn.reads, [
        "0 Fix the login bug please",
        "0 And the logout bug",
        "1 Looking at login.",
        "1 msg_one",
        "1 msg_two",
        "1 Both fixed.",
        "2 Halfway there.",
      ]);
      assert.deepEqual(yield* turn.places, places);
    }),
);

runtimeTest("a message sent into a turn that took it after its reply moves below that reply", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-1111111111f5";
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const threadId = importedThreadId(claudeId);
    yield* h.write(claudePath, [
      c.user("user-0", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-0", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ]);
    const bridgeDir = yield* h.makeDir("bridge");
    const importer = yield* h.importer({ bridgeDir });
    yield* importer.sweepNow();
    // T3 ran this turn. The steer was sent while the reply was being
    // written, so it was shown above it; with no tool call left to meet,
    // the session took it as its next prompt, after the reply.
    yield* seedT3Run(h, threadId, CLAUDE, "completed", {
      ordinal: 1,
      iso: "2026-07-01T10:05:00.000Z",
    });
    yield* seedT3Prompt(h, threadId, 1, "Count to three", "2026-07-01T10:05:00.000Z");
    const runId = RunId.make(`seed-run:${threadId}`);
    const steer = seedTimelineRow({
      threadId,
      key: "steer",
      messageId: MessageId.make(`seed-steer-message:${threadId}`),
      role: "user",
      text: "Now say done",
      at: DateTime.makeUnsafe("2026-07-01T10:05:10.000Z"),
      runId,
    });
    const reply = (uuid: string, text: string, iso: string) => {
      const seed = seedTimelineRow({
        threadId,
        key: uuid,
        messageId: MessageId.make(`message:provider:claudeAgent:native-item:${uuid}`),
        role: "assistant",
        text,
        at: DateTime.makeUnsafe(iso),
        runId,
      });
      return {
        ...seed,
        payload: {
          ...seed.payload,
          id: TurnItemId.make(`turn-item:provider:claudeAgent:native-item:${uuid}`),
        },
      };
    };
    const steered = { ...steer, payload: { ...steer.payload, inputIntent: "steer" as const } };
    yield* h.eventSink.write({
      events: [
        steered,
        reply("assistant-1", "One two three.", "2026-07-01T10:05:20.000Z"),
        reply("assistant-2", "done", "2026-07-01T10:05:30.000Z"),
      ],
    });
    yield* h.append(claudePath, [
      c.user("user-1", "Count to three", "2026-07-01T10:05:01.000Z"),
      c.assistant("assistant-1", "One two three.", "2026-07-01T10:05:20.000Z"),
      c.user("user-2", "Now say done", "2026-07-01T10:05:21.000Z"),
      c.assistant("assistant-2", "done", "2026-07-01T10:05:30.000Z"),
    ]);
    const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
    yield* h.write(joinPath(instanceDir, "session.json"), [
      {
        sid: claudeId,
        instanceId: "inst-1",
        heartbeatAt: yield* Clock.currentTimeMillis,
        activeTurn: null,
      },
    ]);
    const reads = h.threads
      .getThreadRecords(threadId, ["turnItems"])
      .pipe(
        Effect.map((records) =>
          records.turnItems
            .toSorted((left, right) => left.ordinal - right.ordinal)
            .flatMap((item) =>
              item.type === "user_message" || item.type === "assistant_message" ? [item.text] : [],
            ),
        ),
      );
    const turn = ["Count to three", "Now say done", "One two three.", "done"];
    assert.deepEqual((yield* reads).slice(-4), turn);
    const heal = importer.healLiveClaudeSession({
      sessionId: claudeId,
      transcriptPath: claudePath,
    });

    yield* heal;
    assert.deepEqual(yield* reads, [
      "Fix the login bug please",
      "I fixed the login bug.",
      "Count to three",
      "One two three.",
      "Now say done",
      "done",
    ]);
    // A second pass writes nothing.
    const written = yield* h.sequence;
    yield* heal;
    assert.strictEqual(yield* h.sequence, written);
  }),
);

runtimeTest(
  "does not repeat prompts and command output a running session's thread already shows",
  (h) =>
    Effect.gen(function* () {
      const claudeId = "aaaaaaaa-1111-4111-8111-1111111111d1";
      const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
      const claudePath = h.claudeFile(claudeId);
      const threadId = importedThreadId(claudeId);
      yield* h.write(claudePath, [
        c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
        c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
      ]);
      // The session began before this server did: what the record held when
      // the thread was first compared with it stays as the thread shows it.
      yield* TestClock.setTime(
        DateTime.toEpochMillis(DateTime.makeUnsafe("2026-07-01T10:30:00.000Z")),
      );
      const bridgeDir = yield* h.makeDir("bridge");
      const importer = yield* h.importer({ bridgeDir });
      yield* importer.sweepNow();
      const instanceDir = yield* h.makeDir("bridge", "claude", claudeId, "inst-1");
      yield* h.write(joinPath(instanceDir, "session.json"), [
        { sid: claudeId, instanceId: "inst-1", heartbeatAt: yield* Clock.currentTimeMillis },
      ]);

      // T3 showed this turn live: the prompt under its own id, the reply under
      // the transcript row's. The harness's own rows are not conversation.
      yield* seedT3Run(h, threadId, CLAUDE, "completed", {
        ordinal: 1,
        iso: "2026-07-01T10:05:00.000Z",
      });
      yield* seedT3Prompt(h, threadId, 1, "Now add a test", "2026-07-01T10:05:00.000Z");
      yield* seedNativeReply(h, threadId, "assistant-2", "Test added.", "2026-07-01T10:05:04.000Z");
      yield* h.append(claudePath, [
        c.user("user-2", "Now add a test", "2026-07-01T10:05:01.000Z"),
        c.assistant("assistant-2", "Test added.", "2026-07-01T10:05:04.000Z"),
        c.user("user-3", "<task-notification>done</task-notification>", "2026-07-01T10:06:00.000Z"),
        c.user("user-4", "[Request interrupted by user]", "2026-07-01T10:06:30.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.deepEqual(
        (yield* h.texts(threadId)).toSorted(),
        [
          "Fix the login bug please",
          "I fixed the login bug.",
          "Now add a test",
          "Test added.",
        ].toSorted(),
      );
    }),
);

runtimeTest("rebuilds a lost import cursor on a thread T3 never ran", (h) =>
  Effect.gen(function* () {
    const codexId = "cccccccc-2222-4222-8222-222222222222";
    const codexPath = h.codexFile("codex");
    const threadId = importedThreadId(codexId);
    yield* h.write(codexPath, [
      codex.meta(codexId, h.workspace, "2026-07-02T08:00:00.000Z", GIT),
      codex.user("Refactor the rollout parser", "2026-07-02T08:00:01.000Z"),
      codex.assistant("Parser refactored.", "2026-07-02T08:00:09.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();

    const before = yield* h.binding(threadId);
    yield* h.runtimes.upsert({
      ...before,
      runtimePayload: { ...payloadOf(before), importCursor: null },
    });
    // Lines written while the cursor was missing are skipped; the cursor is
    // rebuilt at the current end of the file.
    yield* h.append(codexPath, [
      codex.assistant("Lost while stripped", "2026-07-05T09:00:00.000Z"),
    ]);
    yield* importer.sweepNow();
    assert.deepEqual(payloadOf(yield* h.binding(threadId)).importCursor, {
      sourceSessionId: codexId,
      sourcePath: codexPath,
      lineCount: 4,
      epoch: 2,
    });

    yield* h.append(codexPath, [codex.user("Back in business", "2026-07-05T10:00:00.000Z")]);
    yield* importer.sweepNow();
    assert.include(yield* h.texts(threadId), "Back in business");
  }),
);

runtimeTest(
  "catches up a Claude thread T3 adopted before v2 from where its record ends, once",
  (h) =>
    Effect.gen(function* () {
      const importer = yield* h.importer();
      const sessions = {
        // v1 adoption left T3's own turns in the thread and cleared the cursor.
        withMessages: "aaaaaaaa-1111-4111-8111-1111111111a1",
        // v1 adoption left its markers on the binding.
        withMarkers: "aaaaaaaa-1111-4111-8111-1111111111a2",
      };
      for (const sessionId of Object.values(sessions)) {
        const c = claudeSession({ sessionId, cwd: h.workspace });
        yield* h.write(h.claudeFile(sessionId), [
          c.user(`${sessionId}-u1`, "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
          c.assistant(`${sessionId}-a1`, "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
        ]);
      }
      yield* importer.sweepNow();

      const at = DateTime.makeUnsafe("2026-07-01T10:10:00.000Z");
      const withMessagesThread = importedThreadId(sessions.withMessages);
      yield* h.eventSink.write({
        events: [
          {
            id: EventId.make("seed:migrated-native-message"),
            type: "message.updated",
            threadId: withMessagesThread,
            occurredAt: at,
            payload: {
              createdBy: "user",
              creationSource: "web",
              id: MessageId.make("v1-native-message"),
              threadId: withMessagesThread,
              runId: null,
              nodeId: null,
              role: "user",
              text: "Typed into T3 before the v2 migration",
              attachments: [],
              streaming: false,
              createdAt: at,
              updatedAt: at,
            },
          },
        ],
      });
      const stripCursor = (threadId: ThreadId, extra: Record<string, unknown>) =>
        Effect.gen(function* () {
          const row = yield* h.binding(threadId);
          yield* h.runtimes.upsert({
            ...row,
            runtimePayload: { ...payloadOf(row), importCursor: null, ...extra },
          });
        });
      yield* stripCursor(withMessagesThread, {});
      yield* stripCursor(importedThreadId(sessions.withMarkers), { model: "claude-fable-5" });

      // The terminal keeps writing to both transcripts without the mod. Two
      // rounds: each round's rows land once, and the rebuilt cursor carries on.
      for (const round of [1, 2]) {
        for (const sessionId of Object.values(sessions)) {
          const c = claudeSession({ sessionId, cwd: h.workspace });
          yield* h.append(h.claudeFile(sessionId), [
            c.user(`${sessionId}-u${round}x`, "Native turn", `2026-07-01T10:1${round}:00.000Z`),
            c.assistant(
              `${sessionId}-a${round}x`,
              "Native reply",
              `2026-07-01T10:1${round}:05.000Z`,
            ),
          ]);
        }
        yield* importer.sweepNow();
      }
      for (const sessionId of Object.values(sessions)) {
        const threadId = importedThreadId(sessionId);
        const texts = yield* h.texts(threadId);
        assert.equal(texts.filter((text) => text === "Native reply").length, 2, sessionId);
        assert.equal(texts.filter((text) => text === "Native turn").length, 2, sessionId);
        assert.isNotNull(payloadOf(yield* h.binding(threadId)).importCursor, sessionId);
      }
      const head = yield* h.sequence;
      yield* (yield* h.importer()).sweepNow();
      assert.equal(yield* h.sequence, head, "nothing new: neither thread is written again");
    }),
);

runtimeTest("keeps an epoch-2 Chats thread in place when its cursor is epoch-less or lost", (h) =>
  Effect.gen(function* () {
    const chatId = "eeeeeeee-3333-4333-8333-3333333333c1";
    const chatPath = h.codexFile("chat");
    const threadId = importedThreadId(chatId);
    yield* h.write(chatPath, [
      codex.meta(chatId, joinPath(h.root, "throwaway"), "2026-07-03T09:00:00.000Z"),
      codex.user("Quick question", "2026-07-03T09:00:01.000Z"),
      codex.assistant("Quick answer.", "2026-07-03T09:00:05.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    const chats = (yield* h.projectAt(h.chatsRoot))!;
    const setCursor = (cursor: unknown) =>
      Effect.gen(function* () {
        const row = yield* h.binding(threadId);
        yield* h.runtimes.upsert({
          ...row,
          runtimePayload: { ...payloadOf(row), importCursor: cursor },
        });
      });

    // A cursor without an epoch on an e2 thread is not an epoch-1 chat to migrate.
    yield* setCursor({ sourceSessionId: chatId, sourcePath: chatPath, lineCount: 3 });
    yield* h.append(chatPath, [codex.user("Follow-up one", "2026-07-03T09:01:00.000Z")]);
    yield* importer.sweepNow();
    assert.equal((yield* h.liveShell(threadId)).projectId, chats.projectId);
    assert.include(yield* h.texts(threadId), "Follow-up one");

    // A lost cursor is rebuilt as epoch 2 and imports carry on in place.
    yield* setCursor(null);
    yield* h.append(chatPath, [codex.assistant("Skipped while lost", "2026-07-03T09:02:00.000Z")]);
    yield* importer.sweepNow();
    assert.deepEqual(payloadOf(yield* h.binding(threadId)).importCursor, {
      sourceSessionId: chatId,
      sourcePath: chatPath,
      lineCount: 5,
      epoch: 2,
    });
    yield* h.append(chatPath, [codex.user("Follow-up two", "2026-07-03T09:03:00.000Z")]);
    yield* importer.sweepNow();
    assert.equal((yield* h.liveShell(threadId)).projectId, chats.projectId);
    assert.deepEqual(yield* h.texts(threadId), [
      "Quick question",
      "Quick answer.",
      "Follow-up one",
      "Follow-up two",
    ]);
  }),
);

runtimeTest("never imports a session T3 itself drives", (h) =>
  Effect.gen(function* () {
    const project = yield* h.projects.create({
      commandId: CommandId.make("test:native-project"),
      projectId: ProjectId.make("native-project"),
      title: "Native",
      workspaceRoot: h.workspace,
    });
    // v2: T3's provider thread carries the native session id. T3's Claude
    // adapter writes `sdk-*` transcripts, so without this guard the session
    // would come back as a machine import.
    const claudeNativeId = "99999999-0000-4000-8000-000000000001";
    const codexNativeId = "99999999-0000-4000-8000-000000000002";
    yield* seedT3Thread(h, {
      threadId: ThreadId.make("t3-native-claude"),
      projectId: project.id,
      driver: CLAUDE,
      nativeSessionId: claudeNativeId,
    });
    yield* seedT3Thread(h, {
      threadId: ThreadId.make("t3-native-codex"),
      projectId: project.id,
      driver: CODEX,
      nativeSessionId: codexNativeId,
    });
    const sdk = claudeSession({
      sessionId: claudeNativeId,
      cwd: h.workspace,
      entrypoint: "sdk-ts",
    });
    yield* h.write(h.claudeFile(claudeNativeId), [
      sdk.user("native-user-1", "Prompt sent from T3", "2026-07-12T10:00:00.000Z"),
      sdk.assistant("native-assistant-1", "Reply inside T3", "2026-07-12T10:00:05.000Z"),
    ]);
    yield* h.write(h.codexFile("native"), [
      codex.meta(codexNativeId, h.workspace, "2026-07-12T11:00:00.000Z", GIT),
      codex.user("Prompt sent from T3", "2026-07-12T11:00:01.000Z"),
      codex.assistant("Reply inside T3", "2026-07-12T11:00:05.000Z"),
    ]);

    // v1: a T3 binding without an import cursor owns its session too.
    const boundId = "99999999-0000-4000-8000-000000000003";
    const boundThreadId = ThreadId.make("t3-v1-bound");
    const boundRow = {
      threadId: boundThreadId,
      providerName: CLAUDE,
      providerInstanceId: defaultInstanceIdForDriver(CLAUDE),
      adapterKey: CLAUDE,
      runtimeMode: "full-access",
      status: "stopped",
      lastSeenAt: "2026-07-01T12:00:00.000Z",
      resumeCursor: { threadId: boundThreadId, resume: boundId, turnCount: 3 },
      runtimePayload: null,
    } satisfies ProviderSessionRuntime.ProviderSessionRuntime;
    yield* h.runtimes.upsert(boundRow);
    const bound = claudeSession({ sessionId: boundId, cwd: h.workspace });
    yield* h.write(h.claudeFile(boundId), [
      bound.user("bound-user-1", "t3-owned session", "2026-07-01T12:00:00.000Z"),
      bound.assistant("bound-assistant-1", "reply", "2026-07-01T12:00:05.000Z"),
    ]);

    yield* (yield* h.importer()).sweepNow();

    for (const sessionId of [claudeNativeId, codexNativeId, boundId]) {
      assert.isNull(yield* h.shell(importedThreadId(sessionId)), sessionId);
      assert.isNull(yield* h.shell(importedThreadId(sessionId, "a")), sessionId);
    }
    assert.isUndefined(yield* h.projectAt(h.automationsRoot));
    // The binding still resumes T3's session and never gains an import cursor
    // (its fork signature is backfilled so the thread can join a family).
    const boundAfter = yield* h.binding(boundThreadId);
    assert.deepEqual(boundAfter.resumeCursor, boundRow.resumeCursor);
    assert.notProperty(payloadOf(boundAfter), "importCursor");
  }),
);

runtimeTest("never resurrects an imported thread the user deleted", (h) =>
  Effect.gen(function* () {
    const claudeId = "aaaaaaaa-1111-4111-8111-111111111111";
    const c = claudeSession({ sessionId: claudeId, cwd: h.workspace });
    const claudePath = h.claudeFile(claudeId);
    const threadId = importedThreadId(claudeId);
    yield* h.write(claudePath, [
      c.user("user-1", "Fix the login bug please", "2026-07-01T10:00:00.000Z"),
      c.assistant("assistant-1", "I fixed the login bug.", "2026-07-01T10:00:05.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    yield* h.threads.dispatch({
      type: "thread.delete",
      commandId: CommandId.make("test:user-delete"),
      threadId,
    });
    assert.isFalse(yield* h.isLive(threadId));

    const head = yield* h.sequence;
    yield* h.append(claudePath, [
      c.user("user-2", "Still talking in the terminal", "2026-07-01T10:05:00.000Z"),
      c.assistant("assistant-2", "Still answering", "2026-07-01T10:05:05.000Z"),
    ]);
    yield* importer.sweepNow();
    yield* (yield* h.importer()).sweepNow();
    // Even with its binding gone, so the file looks brand new, the id stays dead.
    yield* h.runtimes.deleteByThreadId({ threadId });
    yield* (yield* h.importer()).sweepNow();
    assert.isFalse(yield* h.isLive(threadId));
    assert.isNull(yield* h.shell(importedThreadId(claudeId, "a")));
    assert.equal(yield* h.sequence, head, "nothing is written for a deleted thread");
  }),
);

runtimeTest("ignores other provider accounts' bindings for the same session ids", (h) =>
  Effect.gen(function* () {
    const codexId = "cccccccc-2222-4222-8222-222222222222";
    yield* h.write(h.codexFile("codex"), [
      codex.meta(codexId, h.workspace, "2026-07-02T08:00:00.000Z", GIT),
      codex.item("rs_shared_with_other_account", "2026-07-02T08:00:00.500Z"),
      codex.user("Refactor the rollout parser", "2026-07-02T08:00:01.000Z"),
      codex.assistant("Parser refactored.", "2026-07-02T08:00:09.000Z"),
    ]);
    // Another configured Codex account has its own transcript namespace: an
    // equal session id (and fork signature) there means nothing here.
    const otherThreadId = ThreadId.make("other-account-same-session");
    const otherRow = {
      threadId: otherThreadId,
      providerName: CODEX,
      providerInstanceId: ProviderInstanceId.make("codex-other-account"),
      adapterKey: CODEX,
      runtimeMode: "full-access",
      status: "stopped",
      lastSeenAt: "2026-07-02T08:00:00.000Z",
      resumeCursor: { threadId: codexId },
      runtimePayload: {
        importForkSig: ["rs_shared_with_other_account"],
        importCursor: {
          sourceSessionId: codexId,
          sourcePath: h.codexFile("codex"),
          lineCount: 1,
          epoch: 2,
        },
      },
    } satisfies ProviderSessionRuntime.ProviderSessionRuntime;
    yield* h.runtimes.upsert(otherRow);

    yield* (yield* h.importer()).sweepNow();

    const local = yield* h.liveShell(importedThreadId(codexId));
    assert.deepEqual(yield* h.texts(importedThreadId(codexId)), [
      "Refactor the rollout parser",
      "Parser refactored.",
    ]);
    assert.isUndefined(local.forkFamilyId ?? undefined, "no family across accounts");
    assert.deepEqual(yield* h.binding(otherThreadId), otherRow);
  }),
);

runtimeTest("marks Codex Desktop sessions as held by Desktop and keeps the mark on append", (h) =>
  Effect.gen(function* () {
    const desktopId = "dddddddd-5555-4555-8555-555555555551";
    const terminalId = "dddddddd-5555-4555-8555-555555555552";
    const cwd = yield* h.makeDir("desktop-workspace");
    const desktopPath = h.codexFile("desktop");
    yield* h.write(desktopPath, [
      codex.meta(desktopId, cwd, "2026-07-09T08:00:00.000Z", {
        ...GIT,
        source: "vscode",
        originator: "Codex Desktop",
      }),
      codex.user("Plan the migration", "2026-07-09T08:00:01.000Z"),
      codex.assistant("Here is the plan.", "2026-07-09T08:00:09.000Z"),
    ]);
    yield* h.write(h.codexFile("terminal"), [
      codex.meta(terminalId, cwd, "2026-07-09T09:00:00.000Z", {
        ...GIT,
        source: "vscode",
        originator: "codex-tui",
      }),
      codex.user("Run the tests", "2026-07-09T09:00:01.000Z"),
      codex.assistant("All green.", "2026-07-09T09:00:05.000Z"),
    ]);

    const importer = yield* h.importer();
    yield* importer.sweepNow();

    const desktopThreadId = importedThreadId(desktopId);
    // The stamp lands on the stored thread and reaches clients through the shell.
    assert.equal(
      (yield* h.threads.getThreadRecords(desktopThreadId, [])).thread.heldBy,
      "codex-desktop",
    );
    assert.equal((yield* h.liveShell(desktopThreadId)).heldBy, "codex-desktop");
    // A terminal session is joinable through the shared daemon: no stamp.
    assert.isUndefined((yield* h.liveShell(importedThreadId(terminalId))).heldBy);

    // Desktop keeps writing; the appended message bumps recency and keeps the stamp.
    yield* h.append(desktopPath, [
      codex.user("Now do step one", "2026-07-09T10:00:00.000Z"),
      codex.assistant("Step one done.", "2026-07-09T10:00:04.000Z"),
    ]);
    yield* importer.sweepNow();
    const shell = yield* h.liveShell(desktopThreadId);
    assert.equal(shell.heldBy, "codex-desktop");
    assert.equal(DateTime.formatIso(shell.updatedAt), "2026-07-09T10:00:04.000Z");
    assert.deepEqual(yield* h.texts(desktopThreadId), [
      "Plan the migration",
      "Here is the plan.",
      "Now do step one",
      "Step one done.",
    ]);
  }),
);

runtimeTest(
  "shows a Codex record's commands, tool calls, patches and notices as the live adapter's items",
  (h) =>
    Effect.gen(function* () {
      const codexId = "cccccccc-3333-4333-8333-333333333301";
      const threadId = importedThreadId(codexId);
      const file = h.codexFile(`2026-07-29T10-00-00-${codexId}`);
      const at = (second: number) => `2026-07-29T10:00:${String(second).padStart(2, "0")}.000Z`;
      yield* h.write(file, [
        codex.meta(codexId, h.workspace, at(0), GIT),
        codexTurn.context("turn-1", at(1)),
        codexTurn.said("turn-1", "user", "u1", "List the repo and fix the readme", at(2)),
        codexTurn.command("turn-1", "call_ls", ["/bin/zsh", "-lc", "ls -la"], at(3)),
        codexTurn.completed(
          "turn-1",
          {
            type: "FileChange",
            id: "call_patch",
            status: "completed",
            changes: {
              "/repo/README.md": { type: "update", unified_diff: "@@ -1 +1 @@\n-a\n+b\n" },
            },
          },
          at(4),
        ),
        codexTurn.completed(
          "turn-1",
          {
            type: "McpToolCall",
            id: "call_mcp",
            server: "linear",
            tool: "list_issues",
            arguments: { team: "T3" },
            status: "completed",
            result: { content: [{ type: "text", text: "no issues" }], isError: false },
          },
          at(5),
        ),
        codexTurn.completed(
          "turn-1",
          { type: "WebSearch", id: "ws_1", query: "readme style", action: { type: "search" } },
          at(6),
        ),
        codexTurn.completed("turn-1", { type: "ContextCompaction", id: "cc_1" }, at(7)),
        codexTurn.said("turn-1", "assistant", "msg_1", "Listed and fixed.", at(8)),
        codexTurn.context("turn-2", at(9)),
        codexTurn.said("turn-2", "user", "u2", "Now stop", at(10)),
        codexTurn.event({ type: "turn_aborted", turn_id: "turn-2", reason: "interrupted" }, at(11)),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();

      const timeline = yield* shownTimeline(h, threadId);
      assert.deepEqual(timeline.map(shownItem), [
        "List the repo and fix the readme",
        "command_execution: call_ls",
        "file_change: call_patch",
        "dynamic_tool: call_mcp",
        "web_search: ws_1",
        "compaction: cc_1",
        "Listed and fixed.",
        "Now stop",
        "system_notice: Turn interrupted",
      ]);
      const command = timeline.find((item) => item.type === "command_execution");
      assert.deepInclude(command, {
        id: TurnItemId.make(`cli-import:v2:${threadId}:line-3`),
        status: "completed",
        input: "/bin/zsh -lc 'ls -la'",
        output: "ok\n",
        exitCode: 0,
      });
      assert.deepInclude(
        timeline.find((item) => item.type === "file_change"),
        { fileName: "/repo/README.md", diffStr: "@@ -1 +1 @@\n-a\n+b\n" },
      );
      // Every item is an end: none is left for a restart to cancel.
      assert.deepEqual(timeline.filter((item) => item.status !== "completed").map(shownItem), []);
      // The thread shows the model the record's turns ran on.
      assert.equal(
        (yield* h.threads.getThreadRecords(threadId, [])).thread.modelSelection.model,
        "gpt-5.6-terra",
      );

      // A later turn's command lands with it.
      yield* h.append(file, [
        codexTurn.context("turn-3", "2026-07-29T10:01:00.000Z"),
        codexTurn.said("turn-3", "user", "u3", "Show the diff", "2026-07-29T10:01:01.000Z"),
        codexTurn.command("turn-3", "call_diff", ["git", "diff"], "2026-07-29T10:01:02.000Z"),
        codexTurn.said("turn-3", "assistant", "msg_3", "Here it is.", "2026-07-29T10:01:03.000Z"),
      ]);
      yield* importer.sweepNow();
      assert.deepEqual((yield* shownTimeline(h, threadId)).map(shownItem).slice(-4), [
        "system_notice: Turn interrupted",
        "Show the diff",
        "command_execution: call_diff",
        "Here it is.",
      ]);

      // A quiet record: a fresh sweep writes nothing.
      const head = yield* h.sequence;
      yield* (yield* h.importer()).sweepNow();
      assert.equal(yield* h.sequence, head);
    }),
);

runtimeTest(
  "keeps reading a Codex thread after T3 has run it: terminal turns land, T3's own turns stay out",
  (h) =>
    Effect.gen(function* () {
      const codexId = "cccccccc-3333-4333-8333-333333333302";
      const threadId = importedThreadId(codexId);
      const file = h.codexFile(`2026-07-29T11-00-00-${codexId}`);
      yield* h.write(file, [
        codex.meta(codexId, h.workspace, "2026-07-29T11:00:00.000Z", GIT),
        codexTurn.context("turn-1", "2026-07-29T11:00:01.000Z"),
        codexTurn.said("turn-1", "user", "u1", "First question", "2026-07-29T11:00:02.000Z"),
        codexTurn.said("turn-1", "assistant", "msg_1", "First answer", "2026-07-29T11:00:03.000Z"),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();

      // T3 runs a turn on the thread; the record writes it too.
      yield* seedT3Run(h, threadId, CODEX, "completed", {
        ordinal: 1,
        iso: "2026-07-29T11:05:00.000Z",
      });
      yield* seedCodexLiveTurn(h, threadId, {
        nativeTurnId: "turn-t3",
        prompt: "Sent from T3",
        replyId: "msg_t3",
        reply: "Answered in T3",
        commandId: "call_t3",
        iso: "2026-07-29T11:05:00.000Z",
      });
      yield* h.append(file, [
        codexTurn.context("turn-t3", "2026-07-29T11:05:00.000Z"),
        codexTurn.said("turn-t3", "user", "u_t3", "Sent from T3", "2026-07-29T11:05:01.000Z"),
        codexTurn.command("turn-t3", "call_t3", ["pnpm", "test"], "2026-07-29T11:05:02.000Z"),
        codexTurn.said(
          "turn-t3",
          "assistant",
          "msg_t3",
          "Answered in T3",
          "2026-07-29T11:05:03.000Z",
        ),
        // Then the person goes on in the Codex terminal.
        codexTurn.context("turn-term", "2026-07-29T11:10:00.000Z"),
        codexTurn.said(
          "turn-term",
          "user",
          "u_term",
          "Typed in the Codex terminal",
          "2026-07-29T11:10:01.000Z",
        ),
        codexTurn.command("turn-term", "call_term", ["git", "log"], "2026-07-29T11:10:02.000Z"),
        codexTurn.said(
          "turn-term",
          "assistant",
          "msg_term",
          "Answered in the terminal",
          "2026-07-29T11:10:03.000Z",
        ),
      ]);
      yield* importer.sweepNow();

      const shown = (yield* shownTimeline(h, threadId)).map(shownItem);
      const count = (text: string) => shown.filter((entry) => entry === text).length;
      assert.equal(count("Typed in the Codex terminal"), 1, shown.join(" | "));
      assert.equal(count("Answered in the terminal"), 1);
      assert.equal(count("command_execution: call_term"), 1);
      // T3's turn shows once: as T3 showed it.
      assert.equal(count("Sent from T3"), 1);
      assert.equal(count("Answered in T3"), 1);
      assert.equal(count("command_execution: call_t3"), 1);
      assert.isTrue(
        shown.indexOf("Typed in the Codex terminal") < shown.indexOf("Answered in the terminal"),
      );
      // The thread keeps its cursor, so the terminal's next turn lands too.
      assert.isNotNull(payloadOf(yield* h.binding(threadId)).importCursor);
      yield* h.append(file, [
        codexTurn.context("turn-term-2", "2026-07-29T11:20:00.000Z"),
        codexTurn.said("turn-term-2", "user", "u_term_2", "One more", "2026-07-29T11:20:01.000Z"),
        codexTurn.said(
          "turn-term-2",
          "assistant",
          "msg_term_2",
          "Done.",
          "2026-07-29T11:20:02.000Z",
        ),
      ]);
      yield* importer.sweepNow();
      assert.deepEqual((yield* shownTimeline(h, threadId)).map(shownItem).slice(-2), [
        "One more",
        "Done.",
      ]);

      const head = yield* h.sequence;
      yield* (yield* h.importer()).sweepNow();
      assert.equal(yield* h.sequence, head);
    }),
);

runtimeTest(
  "titles a Codex thread with Codex's own name and follows it, but never over a name set in T3",
  (h) =>
    Effect.gen(function* () {
      const path = yield* Path.Path;
      const codexId = "cccccccc-3333-4333-8333-333333333303";
      const threadId = importedThreadId(codexId);
      const indexPath = path.join(h.chatsRoot, "session_index.jsonl");
      const named = (name: string, at: string): Entry => ({
        id: codexId,
        thread_name: name,
        updated_at: at,
      });
      yield* h.write(indexPath, [named("Fix the flaky CI test", "2026-07-29T12:00:10Z")]);
      yield* h.write(h.codexFile(`2026-07-29T12-00-00-${codexId}`), [
        codex.meta(codexId, h.workspace, "2026-07-29T12:00:00.000Z", GIT),
        codexTurn.context("turn-1", "2026-07-29T12:00:01.000Z"),
        codexTurn.said("turn-1", "user", "u1", "ci is red again, look", "2026-07-29T12:00:02.000Z"),
        codexTurn.said("turn-1", "assistant", "msg_1", "Looking.", "2026-07-29T12:00:03.000Z"),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();
      assert.equal((yield* h.liveShell(threadId)).title, "Fix the flaky CI test");

      // Renamed in Codex: the thread follows, and keeps its place in the list.
      const before = yield* h.liveShell(threadId);
      yield* h.append(indexPath, [named("Flaky CI test fixed", "2026-07-29T13:00:00Z")]);
      yield* importer.sweepNow();
      const renamed = yield* h.liveShell(threadId);
      assert.equal(renamed.title, "Flaky CI test fixed");
      assert.equal(iso(renamed.updatedAt), iso(before.updatedAt));

      // Renamed in T3: that name is the person's, and stays.
      yield* h.threads.dispatch({
        type: "thread.metadata.update",
        commandId: CommandId.make("test:rename-in-t3"),
        threadId,
        title: "My own name for it",
      });
      yield* h.append(indexPath, [named("Renamed again in Codex", "2026-07-29T14:00:00Z")]);
      yield* importer.sweepNow();
      yield* (yield* h.importer()).sweepNow();
      assert.equal((yield* h.liveShell(threadId)).title, "My own name for it");
    }),
);

runtimeTest("shows the model and goal a Codex record last names", (h) =>
  Effect.gen(function* () {
    const codexId = "cccccccc-3333-4333-8333-333333333304";
    const threadId = importedThreadId(codexId);
    const file = h.codexFile(`2026-07-29T15-00-00-${codexId}`);
    const goal = (status: string, tokensUsed: number) =>
      codexTurn.event(
        {
          type: "thread_goal_updated",
          goal: {
            objective: "Ship the importer",
            status,
            tokensUsed,
            tokenBudget: null,
            timeUsedSeconds: 30,
          },
        },
        "2026-07-29T15:00:04.000Z",
      );
    yield* h.write(file, [
      codex.meta(codexId, h.workspace, "2026-07-29T15:00:00.000Z", GIT),
      codexTurn.context("turn-1", "2026-07-29T15:00:01.000Z", "gpt-5.6-terra"),
      codexTurn.said("turn-1", "user", "u1", "Work toward the goal", "2026-07-29T15:00:02.000Z"),
      codexTurn.said("turn-1", "assistant", "msg_1", "On it.", "2026-07-29T15:00:03.000Z"),
      goal("active", 100),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();
    const records = yield* h.threads.getThreadRecords(threadId, ["providerThreads"]);
    assert.equal(records.thread.modelSelection.model, "gpt-5.6-terra");
    assert.deepEqual(records.providerThreads[0]?.goal, {
      objective: "Ship the importer",
      status: "active",
      tokensUsed: 100,
      tokenBudget: null,
      timeUsedSeconds: 30,
    });

    // The person switches model in the terminal and the goal completes.
    yield* h.append(file, [
      codexTurn.event(
        { type: "thread_settings_applied", thread_settings: { model: "gpt-6-mini" } },
        "2026-07-29T15:10:00.000Z",
      ),
      codexTurn.context("turn-2", "2026-07-29T15:10:01.000Z", "gpt-6-mini"),
      codexTurn.said("turn-2", "user", "u2", "Finish it", "2026-07-29T15:10:02.000Z"),
      codexTurn.said("turn-2", "assistant", "msg_2", "Finished.", "2026-07-29T15:10:03.000Z"),
      goal("complete", 250),
    ]);
    yield* importer.sweepNow();
    const after = yield* h.threads.getThreadRecords(threadId, ["providerThreads"]);
    assert.equal(after.thread.modelSelection.model, "gpt-6-mini");
    assert.equal(after.providerThreads[0]?.goal?.status, "complete");
    assert.equal(after.providerThreads[0]?.goal?.tokensUsed, 250);
  }),
);

runtimeTest("follows a Codex thread into the later file Codex went on writing it in", (h) =>
  Effect.gen(function* () {
    const codexId = "cccccccc-3333-4333-8333-333333333305";
    const segmentId = "cccccccc-3333-4333-8333-3333333333b5";
    const threadId = importedThreadId(codexId);
    const first = h.codexFile(`2026-07-29T16-00-00-${codexId}`);
    const later = h.codexFile(`2026-07-29T16-00-00-${codexId}_${segmentId}`);
    yield* h.write(first, [
      codex.meta(codexId, h.workspace, "2026-07-29T16:00:00.000Z", GIT),
      codexTurn.context("turn-1", "2026-07-29T16:00:01.000Z"),
      codexTurn.said("turn-1", "user", "u1", "Start here", "2026-07-29T16:00:02.000Z"),
      codexTurn.said("turn-1", "assistant", "msg_1", "Started.", "2026-07-29T16:00:03.000Z"),
    ]);
    const importer = yield* h.importer();
    yield* importer.sweepNow();

    // Codex goes on in a later file, from the end of the first one.
    yield* h.write(later, [
      codex.meta(codexId, h.workspace, "2026-07-29T16:05:00.000Z", {
        history_base: { thread_id: codexId, end_ordinal_exclusive: 4, end_byte_offset: 600 },
      }),
      codexTurn.context("turn-2", "2026-07-29T16:05:01.000Z"),
      codexTurn.said("turn-2", "user", "u2", "Went on in the new file", "2026-07-29T16:05:02.000Z"),
      codexTurn.command("turn-2", "call_seg", ["ls"], "2026-07-29T16:05:03.000Z"),
      codexTurn.said("turn-2", "assistant", "msg_2", "Going on.", "2026-07-29T16:05:04.000Z"),
    ]);
    yield* importer.sweepNow();
    assert.deepEqual((yield* shownTimeline(h, threadId)).map(shownItem), [
      "Start here",
      "Started.",
      "Went on in the new file",
      "command_execution: call_seg",
      "Going on.",
    ]);
    const cursor = payloadOf(yield* h.binding(threadId)).importCursor as Record<string, unknown>;
    assert.equal(cursor.sourcePath, later);

    yield* h.append(later, [
      codexTurn.said("turn-2", "user", "u3", "And on", "2026-07-29T16:06:00.000Z"),
    ]);
    yield* importer.sweepNow();
    assert.equal((yield* h.texts(threadId)).at(-1), "And on");
  }),
);

runtimeTest(
  "a later Codex file read in the same sweep as its first one lands on the sweep after",
  (h) =>
    Effect.gen(function* () {
      const codexId = "cccccccc-3333-4333-8333-333333333306";
      const segmentId = "cccccccc-3333-4333-8333-3333333333b6";
      const threadId = importedThreadId(codexId);
      yield* h.write(h.codexFile(`2026-07-29T17-00-00-${codexId}`), [
        codex.meta(codexId, h.workspace, "2026-07-29T17:00:00.000Z", GIT),
        codexTurn.context("turn-1", "2026-07-29T17:00:01.000Z"),
        codexTurn.said("turn-1", "user", "u1", "Start here", "2026-07-29T17:00:02.000Z"),
        codexTurn.said("turn-1", "assistant", "msg_1", "Started.", "2026-07-29T17:00:03.000Z"),
      ]);
      yield* h.write(h.codexFile(`2026-07-29T17-00-00-${codexId}_${segmentId}`), [
        codex.meta(codexId, h.workspace, "2026-07-29T17:05:00.000Z", {
          history_base: { thread_id: codexId, end_ordinal_exclusive: 4, end_byte_offset: 600 },
        }),
        codexTurn.context("turn-2", "2026-07-29T17:05:01.000Z"),
        codexTurn.said("turn-2", "user", "u2", "Went on", "2026-07-29T17:05:02.000Z"),
        codexTurn.said("turn-2", "assistant", "msg_2", "Going on.", "2026-07-29T17:05:03.000Z"),
      ]);
      const importer = yield* h.importer();
      yield* importer.sweepNow();
      // The later file never makes a thread of its own: it holds only the end.
      assert.deepEqual(yield* h.texts(threadId), ["Start here", "Started."]);
      yield* importer.sweepNow();
      assert.deepEqual(yield* h.texts(threadId), [
        "Start here",
        "Started.",
        "Went on",
        "Going on.",
      ]);
    }),
);

function joinPath(...segments: ReadonlyArray<string>) {
  return segments.join("/");
}
