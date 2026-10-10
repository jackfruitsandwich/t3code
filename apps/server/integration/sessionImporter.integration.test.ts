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
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2DomainEvent,
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

function joinPath(...segments: ReadonlyArray<string>) {
  return segments.join("/");
}
