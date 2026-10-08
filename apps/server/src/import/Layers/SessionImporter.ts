import { forkParked } from "../../serverActivation.ts";
/**
 * SessionImporter - Background import of provider-native CLI sessions.
 *
 * Sweeps the Claude Code (`<config dir>/projects`) and Codex
 * (`<codex home>/sessions`) transcript directories and materializes each
 * eligible session as a T3 project/thread with its message history and a
 * native resume ref, so chatting into the thread continues the real provider
 * session. Source files are read-only. Threads are written straight to the
 * v2 event log the way upstream's `AgentSessionImporter` writes them
 * (`historyOrigin: "v1_import"`, a provider thread whose `nativeThreadRef` is
 * the source session), with deterministic event ids. The importer NEVER
 * starts a turn — importing history must not wake a live agent.
 *
 * Ownership: bindings in `provider_session_runtime` written by the importer
 * carry `runtimePayload.importCursor` ({ sourceSessionId, sourcePath,
 * lineCount }). A binding without that marker belongs to a T3-native session
 * and is never touched; a discovered file is only imported incrementally when
 * both the session id and path match the marker. Once a T3 run has happened
 * on an imported thread, T3 owns it: the cursor is dropped and the file is no
 * longer tailed. Sessions T3 itself drives (v2 provider threads) are never
 * imported.
 *
 * @module import/Layers/SessionImporter
 */
// @effect-diagnostics nodeBuiltinImport:off
import * as NodeCrypto from "node:crypto";
import * as NodeOS from "node:os";

import {
  CommandId,
  DEFAULT_MODEL,
  DEFAULT_MODEL_BY_PROVIDER,
  DEFAULT_PROVIDER_INTERACTION_MODE,
  defaultInstanceIdForDriver,
  EventId,
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ThreadHeldBy,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type ServerSettings,
} from "@t3tools/contracts";
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import {
  CLAUDE_BRIDGE_DIR_ENV,
  isClaudeBridgeSessionLive,
  parseClaudeBridgeSessionFile,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { ServerConfig } from "../../config.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { deriveProviderThread } from "../../orchestration-v2/IdAllocator.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import {
  ProviderSessionRuntimeRepository,
  type ProviderSessionRuntime,
} from "../../persistence/ProviderSessionRuntime.ts";
import { ProjectService } from "../../project/ProjectService.ts";
import { resolveCodexHomeLayout } from "../../provider/Drivers/CodexHomeLayout.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { parseClaudeSessionJsonl } from "../claudeSessionParser.ts";
import { parseCodexRolloutJsonl } from "../codexRolloutParser.ts";
import {
  isCommandNoiseText,
  isEligibleCliSession,
  isRecord,
  parseJsonlLine,
  type ParsedCliSession,
  type ParsedSessionMessage,
} from "../sessionImportTypes.ts";
import { SessionImporter, type SessionImporterShape } from "../Services/SessionImporter.ts";

const DEFAULT_SWEEP_INTERVAL_MS = 30 * 1000;

/**
 * Import layout epoch. Bumped when the project-placement rules change so
 * that migrated sessions can be re-imported under fresh ids. Epoch 2: codex
 * sessions without git metadata group into one shared "Chats" project
 * instead of one project per cwd.
 */
const IMPORT_EPOCH = 2;

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
/** Run statuses under which T3 is writing the thread's rows itself. */
const ACTIVE_RUN_STATUSES: ReadonlySet<string> = new Set([
  "preparing",
  "starting",
  "running",
  "waiting",
]);
const CODEX_DRIVER = ProviderDriverKind.make("codex");

/** A `provider_session_runtime` row, as the importer reads and merges it. */
interface ImportBinding {
  readonly threadId: ThreadId;
  readonly provider: ProviderDriverKind;
  readonly providerInstanceId: ProviderInstanceId | undefined;
  readonly status: ProviderSessionRuntime["status"];
  readonly resumeCursor: unknown;
  readonly runtimePayload: unknown;
}

// This sweeper reads only the default Claude/Codex homes. Other configured
// accounts may legitimately contain the same native session or message IDs.
const belongsToScannedProviderHome = (binding: ImportBinding) =>
  (binding.provider === CLAUDE_DRIVER || binding.provider === CODEX_DRIVER) &&
  binding.providerInstanceId === defaultInstanceIdForDriver(binding.provider);

const CHATS_PROJECT_TITLE = "Chats";
const AUTOMATIONS_PROJECT_TITLE = "Automations";

class SessionImportError extends Schema.TaggedError<SessionImportError>()("SessionImportError", {
  detail: Schema.String,
}) {}

export interface SessionImporterLiveOptions {
  /** Directory containing Claude Code `<project>/<session uuid>.jsonl` files. */
  readonly claudeProjectsDir?: string;
  /** Directory scanned recursively for Codex `rollout-*.jsonl` files. */
  readonly codexSessionsDir?: string;
  readonly sweepIntervalMs?: number;
  /** Whether `start()` forks the background loop; `sweepNow` always works. */
  readonly enabled?: boolean;
  /**
   * Files larger than this are skipped (whole-file reads would blow memory
   * on small hosts). Oversized sessions simply don't import.
   */
  readonly maxFileBytes?: number;
  /**
   * A file is only processed once it has been quiet for this long. Active
   * agent sessions rewrite their transcript continuously; re-parsing a
   * multi-MB file on every sweep pegs small hosts. Imports land at turn
   * boundaries, which is when they're readable anyway. 0 disables.
   */
  readonly settleMs?: number;
  /**
   * Bridge root (`<state dir>/bridge`). A Claude session with a live mod
   * registration under it belongs to the bridge, which adopts its terminal
   * turns itself; the importer leaves such a session alone.
   */
  readonly bridgeDir?: string;
  /**
   * Path prefixes whose sessions route to the Automations project regardless
   * of launch markers. Agent harnesses that drive real interactive CLIs
   * (tmux) produce files indistinguishable from a human's; the directories
   * they run in are the one clean signal. Kin detection still applies.
   */
  readonly automationRoots?: ReadonlyArray<string>;
}

// Whole-file reads: generous on a desktop, lowered on small hosts with
// T3CODE_IMPORT_MAX_FILE_BYTES (the box keeps 32 MiB).
const DEFAULT_MAX_FILE_BYTES = 256 * 1024 * 1024;
const DEFAULT_SETTLE_MS = 20 * 1000;

interface ImportCursor {
  readonly sourceSessionId: string;
  readonly sourcePath: string;
  readonly lineCount: number;
  /** Layout epoch the thread was imported under; absent = epoch 1. */
  readonly epoch?: number;
}

interface SweepStats {
  importedCount: number;
  updatedCount: number;
}

/**
 * A machine-launched session whose placement decision is postponed to the end
 * of the sweep, when every human session's fork signature is registered: a
 * machine session sharing early history with a human one is a fork of that
 * conversation (e.g. a `/branch` of a T3-driven thread) and belongs beside
 * it, not in Automations.
 */
interface DeferredMachineSession {
  readonly kind: "fresh" | "existing";
  readonly driver: ProviderDriverKind;
  readonly sessionId: string;
  readonly filePath: string;
  readonly parsed: ParsedCliSession;
  readonly binding?: ImportBinding;
  readonly cursor?: ImportCursor;
}

interface SweepContext {
  readonly bindingsBySource: ReadonlyMap<string, ImportBinding>;
  /** Native sessions T3 itself drives; their transcripts are never imported. */
  readonly t3NativeSessions: ReadonlySet<string>;
  readonly seenSessionIds: Set<string>;
  readonly stats: SweepStats;
  /** Workspace root of the shared project holding project-less codex chats. */
  readonly chatsWorkspaceRoot: string;
  /** Workspace root of the shared project holding machine-launched sessions. */
  readonly automationsWorkspaceRoot: string;
  /** Fork-signature id -> owning human-side source key, for kinship checks. */
  readonly sigOwners: Map<string, string>;
  readonly deferredMachine: DeferredMachineSession[];
}

const sourceKey = (driver: ProviderDriverKind, sessionId: string) => `${driver}:${sessionId}`;

const hashHex = (value: string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex").slice(0, 24);

/**
 * Deterministic T3 ids derived from the source session/workspace. Thread ids
 * are epoch-scoped because deleted thread ids can never be re-created, so a
 * layout migration re-imports under a fresh id and tombstones the old binding.
 */
export const importedThreadId = (sessionId: string, idVariant = "") =>
  ThreadId.make(`cli-import-e${IMPORT_EPOCH}${idVariant}-${sessionId}`);
/** Imported under the current layout epoch (any id variant). */
const isCurrentEpochThread = (threadId: string) =>
  threadId.startsWith(`cli-import-e${IMPORT_EPOCH}`);
export const importedProjectId = (workspaceRoot: string) =>
  ProjectId.make(`cli-import-project-${hashHex(workspaceRoot)}`);
const importCommandId = (
  driver: ProviderDriverKind,
  sessionId: string,
  facet: string,
  idVariant = "",
) => CommandId.make(`cli-import:e${IMPORT_EPOCH}${idVariant}:${driver}:${sessionId}:${facet}`);
export const importedMessageId = (sessionId: string, sourceId: string) =>
  MessageId.make(`cli-import-${sessionId}-${sourceId}`);
/**
 * Codex Desktop drives its sessions through a private app-server T3 cannot
 * attach to; the thread stays Desktop's and T3 only follows the file.
 */
const heldByForOriginator = (
  originator: string | undefined,
): OrchestrationV2ThreadHeldBy | undefined =>
  originator === "Codex Desktop" ? "codex-desktop" : undefined;

const importEventId = (threadId: ThreadId, facet: string) =>
  EventId.make(`cli-import:v2:${threadId}:${facet}`);

function readImportCursor(runtimePayload: unknown): ImportCursor | undefined {
  if (!isRecord(runtimePayload) || !isRecord(runtimePayload.importCursor)) {
    return undefined;
  }
  const cursor = runtimePayload.importCursor;
  if (
    typeof cursor.sourceSessionId !== "string" ||
    typeof cursor.sourcePath !== "string" ||
    typeof cursor.lineCount !== "number" ||
    !Number.isInteger(cursor.lineCount) ||
    cursor.lineCount < 0
  ) {
    return undefined;
  }
  return {
    sourceSessionId: cursor.sourceSessionId,
    sourcePath: cursor.sourcePath,
    lineCount: cursor.lineCount,
    ...(typeof cursor.epoch === "number" ? { epoch: cursor.epoch } : {}),
  };
}

function readForkSignature(runtimePayload: unknown): ReadonlyArray<string> | undefined {
  if (!isRecord(runtimePayload) || !Array.isArray(runtimePayload.importForkSig)) {
    return undefined;
  }
  const ids = runtimePayload.importForkSig.filter(
    (value): value is string => typeof value === "string" && value.length > 0,
  );
  return ids.length > 0 ? ids : undefined;
}

/** Sweep-end placement decision recorded on the binding: a machine-launched
 * session is either kin of a human conversation or an automation. */
function readLaunchClass(runtimePayload: unknown): string | undefined {
  if (!isRecord(runtimePayload) || typeof runtimePayload.importLaunch !== "string") {
    return undefined;
  }
  return runtimePayload.importLaunch;
}

function readResumeSessionId(binding: ImportBinding): string | undefined {
  if (!isRecord(binding.resumeCursor)) {
    return undefined;
  }
  if (binding.provider === CLAUDE_DRIVER) {
    return typeof binding.resumeCursor.resume === "string"
      ? binding.resumeCursor.resume
      : undefined;
  }
  if (binding.provider === CODEX_DRIVER) {
    return typeof binding.resumeCursor.threadId === "string"
      ? binding.resumeCursor.threadId
      : undefined;
  }
  return undefined;
}

const toBinding = (row: ProviderSessionRuntime): ImportBinding => ({
  threadId: row.threadId,
  provider: ProviderDriverKind.make(row.providerName),
  providerInstanceId: row.providerInstanceId ?? undefined,
  status: row.status,
  resumeCursor: row.resumeCursor,
  runtimePayload: row.runtimePayload,
});

const dateTime = (iso: string) => DateTime.makeUnsafe(iso);

/** Message + timeline events for imported text, as upstream's AgentSessionImporter writes them. */
function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly sessionId: string;
  readonly message: ParsedSessionMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { threadId, message } = input;
  const messageId = importedMessageId(input.sessionId, message.sourceId);
  const at = dateTime(message.timestamp);
  const turnItemId = TurnItemId.make(`cli-import:v2:${threadId}:${message.sourceId}`);
  const base = {
    id: turnItemId,
    threadId,
    runId: null,
    nodeId: null,
    providerThreadId: null,
    providerTurnId: null,
    nativeItemRef: null,
    parentItemId: null,
    // EventSink assigns the real position (next in the thread's runless band).
    ordinal: 0,
    status: "completed" as const,
    title: null,
    startedAt: at,
    completedAt: at,
    updatedAt: at,
  };
  return [
    {
      id: importEventId(threadId, `message:${message.sourceId}`),
      type: "message.updated",
      threadId,
      occurredAt: at,
      payload: {
        createdBy: message.role === "user" ? "user" : "agent",
        creationSource: "server",
        id: messageId,
        threadId,
        runId: null,
        nodeId: null,
        role: message.role,
        text: message.text,
        attachments: [],
        streaming: false,
        createdAt: at,
        updatedAt: at,
      },
    },
    {
      id: importEventId(threadId, `turn-item:${message.sourceId}`),
      type: "turn-item.updated",
      threadId,
      occurredAt: at,
      payload:
        message.role === "user"
          ? {
              ...base,
              createdBy: "user",
              creationSource: "server",
              type: "user_message",
              messageId,
              inputIntent: "turn_start",
              text: message.text,
              attachments: [],
            }
          : {
              ...base,
              type: "assistant_message",
              messageId,
              text: message.text,
              streaming: false,
            },
    },
  ];
}

const makeSessionImporter = (options?: SessionImporterLiveOptions) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const projects = yield* ProjectService;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const runtimes = yield* ProviderSessionRuntimeRepository;
    const serverSettings = yield* ServerSettingsService;
    const sql = yield* SqlClient.SqlClient;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const enabled = options?.enabled ?? true;
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const bridgeDir = options?.bridgeDir;
    const maxFileBytes = Math.max(1, options?.maxFileBytes ?? DEFAULT_MAX_FILE_BYTES);
    const settleMs = Math.max(0, options?.settleMs ?? DEFAULT_SETTLE_MS);
    const automationRoots = (options?.automationRoots ?? []).map((root) =>
      path.resolve(expandHomePath(root)),
    );

    // --- Store: the importer's whole view of T3 state -------------------------

    const listBindings = runtimes.list().pipe(Effect.map((rows) => rows.map(toBinding)));

    /**
     * Shallow-merges `runtimePayload` into the thread's binding (the
     * repository replaces the payload, so read-merge-write), keeping the
     * resume cursor unless one is given.
     */
    const mergeBinding = Effect.fn("SessionImporter.mergeBinding")(function* (input: {
      readonly threadId: ThreadId;
      readonly provider: ProviderDriverKind;
      readonly resumeCursor?: unknown;
      readonly runtimePayload: Record<string, unknown>;
    }) {
      const current = yield* runtimes.getByThreadId({ threadId: input.threadId });
      const previous = Option.getOrUndefined(current);
      const payload = isRecord(previous?.runtimePayload) ? previous.runtimePayload : {};
      const lastSeenAt = DateTime.formatIso(yield* DateTime.now);
      yield* runtimes.upsert({
        threadId: input.threadId,
        providerName: input.provider,
        providerInstanceId:
          previous?.providerInstanceId ?? defaultInstanceIdForDriver(input.provider),
        adapterKey: previous?.adapterKey ?? input.provider,
        runtimeMode: previous?.runtimeMode ?? "full-access",
        status: previous?.status ?? "stopped",
        lastSeenAt,
        resumeCursor:
          input.resumeCursor === undefined ? (previous?.resumeCursor ?? null) : input.resumeCursor,
        runtimePayload: { ...payload, ...input.runtimePayload },
      });
    });

    /** The live (not deleted) thread, or none. */
    const getThread = (threadId: ThreadId) =>
      threads
        .getThreadShell(threadId)
        .pipe(
          Effect.map((shell) =>
            shell === null || shell.deletedAt !== null ? Option.none() : Option.some(shell),
          ),
        );

    /** Any thread with this id ever existed: deleted ids are never re-created. */
    const threadEverExisted = (threadId: ThreadId) =>
      sql<{ readonly found: number }>`
        SELECT 1 AS found FROM orchestration_events
        WHERE application_event_version = 2
          AND aggregate_kind = 'thread'
          AND stream_id = ${threadId}
          AND event_type = 'thread.created'
        LIMIT 1
      `.pipe(Effect.map((rows) => rows.length > 0));

    /**
     * Whether the thread holds messages the importer didn't write: T3 itself
     * ran turns on it (before or after the v2 migration), so T3 owns it.
     */
    const hasT3Messages = (threadId: ThreadId) =>
      sql<{ readonly found: number }>`
        SELECT 1 AS found FROM orchestration_v2_projection_messages
        WHERE thread_id = ${threadId} AND message_id NOT LIKE 'cli-import-%'
        LIMIT 1
      `.pipe(Effect.map((rows) => rows.length > 0));

    /** A T3 run on an imported thread means T3 owns its native session now. */
    const hasT3Runs = (threadId: ThreadId) =>
      threads
        .getThreadRecords(threadId, ["runs"])
        .pipe(Effect.map((records) => records.runs.length > 0));

    /** Whether T3 is driving a turn on the thread right now. */
    const hasActiveRun = (threadId: ThreadId) =>
      threads
        .getThreadRecords(threadId, ["runs"])
        .pipe(
          Effect.map((records) => records.runs.some((run) => ACTIVE_RUN_STATUSES.has(run.status))),
        );

    /** Whether a Claude Code process with the T3 mod holds the session now. */
    const hasLiveBridgeProcess = (sessionId: string) =>
      Effect.gen(function* () {
        if (bridgeDir === undefined) return false;
        const dir = path.join(bridgeDir, "claude", sessionId);
        const instances = yield* fs
          .readDirectory(dir)
          .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
        const now = yield* Clock.currentTimeMillis;
        for (const instanceId of instances) {
          const raw = yield* fs
            .readFileString(path.join(dir, instanceId, "session.json"))
            .pipe(Effect.orElseSucceed(() => ""));
          const session = parseClaudeBridgeSessionFile(parseJsonlLine(raw) ?? null);
          if (session !== null && isClaudeBridgeSessionLive(session, now)) return true;
        }
        return false;
      });

    /**
     * The newest moment T3's own record of a thread reaches: every reply it
     * holds (the provider's, or imported) and the prompts of runs that
     * completed, by their last update, which for a streamed reply is its end.
     * A prompt whose run failed never reached the native session, so it does
     * not count.
     */
    const ownedWatermarkMs = (threadId: ThreadId) =>
      sql<{ readonly at: string | null }>`
        SELECT max(message.updated_at) AS at
        FROM orchestration_v2_projection_messages AS message
        LEFT JOIN orchestration_v2_projection_runs AS run ON run.run_id = message.run_id
        WHERE message.thread_id = ${threadId}
          AND (message.role = 'assistant' OR (message.role = 'user' AND run.status = 'completed'))
      `.pipe(
        Effect.map((rows) => {
          const at = rows[0]?.at;
          return at === undefined || at === null ? undefined : DateTime.toEpochMillis(dateTime(at));
        }),
      );

    /** The live thread whose provider thread names this native session, if any. */
    const lookupOwnedThread = (driver: ProviderDriverKind, sessionId: string) =>
      sql<{ readonly thread_id: string }>`
        SELECT provider_thread.thread_id
        FROM orchestration_v2_projection_provider_threads AS provider_thread
        JOIN orchestration_v2_projection_threads AS thread
          ON thread.thread_id = provider_thread.thread_id
        WHERE json_extract(provider_thread.payload_json, '$.nativeThreadRef.nativeId') = ${sessionId}
          AND json_extract(provider_thread.payload_json, '$.driver') = ${driver}
          AND thread.deleted_at IS NULL
          AND thread.archived_at IS NULL
        ORDER BY provider_thread.updated_at DESC
        LIMIT 1
      `.pipe(
        Effect.map((rows) =>
          rows[0] === undefined ? undefined : ThreadId.make(rows[0].thread_id),
        ),
        Effect.orElseSucceed((): ThreadId | undefined => undefined),
      );

    const deleteThread = (threadId: ThreadId, commandId: CommandId) =>
      threads.dispatch({ type: "thread.delete", commandId, threadId }).pipe(Effect.asVoid);

    /** Deletes a project the importer emptied; a project with threads stays. */
    const deleteProjectIfEmpty = (projectId: ProjectId, commandId: CommandId) =>
      Effect.gen(function* () {
        const remaining = yield* threads.listProjectThreads({ projectId, includeSubagents: true });
        if (remaining.some((thread) => thread.deletedAt === null)) return;
        yield* projects.delete({ commandId, projectId }).pipe(Effect.asVoid);
      });

    /** Sessions T3 itself drives: every v2 provider thread's native id. */
    const listT3NativeSessions = sql<{
      readonly driver: string | null;
      readonly native_id: string | null;
    }>`
      SELECT
        json_extract(provider_thread.payload_json, '$.driver') AS driver,
        json_extract(provider_thread.payload_json, '$.nativeThreadRef.nativeId') AS native_id
      FROM orchestration_v2_projection_provider_threads AS provider_thread
    `.pipe(
      Effect.map(
        (rows) =>
          new Set(
            rows.flatMap((row) =>
              row.driver === null || row.native_id === null
                ? []
                : [sourceKey(ProviderDriverKind.make(row.driver), row.native_id)],
            ),
          ),
      ),
    );

    // --------------------------------------------------------------------------

    /** Machine-marked by the provider, or living under a configured
     * automation root (tmux-driven harness sessions carry human markers). */
    const isAutomationSession = (parsed: ParsedCliSession) => {
      if (parsed.launch === "machine") {
        return true;
      }
      if (parsed.cwd === undefined || automationRoots.length === 0) {
        return false;
      }
      const cwd = path.resolve(expandHomePath(parsed.cwd));
      return automationRoots.some((root) => cwd.startsWith(root));
    };

    // Cheap change detection between sweeps; per spec a stale entry only
    // delays a file until it changes again, so process-local state is fine.
    const fileFingerprints = new Map<string, { size: number; mtimeMs: number }>();

    const resolveClaudeProjectsDir = (settings: ServerSettings) => {
      // Mirrors ClaudeSkills' config-dir precedence: the instance homePath
      // (exported as CLAUDE_CONFIG_DIR for the spawned CLI), then an ambient
      // CLAUDE_CONFIG_DIR, then `~/.claude`.
      const homePath = settings.providers.claudeAgent.homePath.trim();
      const environmentConfigDir = process.env.CLAUDE_CONFIG_DIR?.trim() ?? "";
      const configDir =
        homePath.length > 0
          ? path.resolve(expandHomePath(homePath))
          : environmentConfigDir.length > 0
            ? path.resolve(environmentConfigDir)
            : path.join(NodeOS.homedir(), ".claude");
      return path.join(configDir, "projects");
    };

    const listJsonlFiles = (dir: string) =>
      fs.readDirectory(dir, { recursive: true }).pipe(
        Effect.map((entries) =>
          entries
            .filter((entry) => entry.endsWith(".jsonl"))
            .sort()
            .map((entry) => path.join(dir, entry)),
        ),
        // A missing source directory just means nothing to import.
        Effect.orElseSucceed((): ReadonlyArray<string> => []),
      );

    const ensureProject = Effect.fn("SessionImporter.ensureProject")(function* (input: {
      readonly workspaceRoot: string;
      readonly sessionId: string;
      readonly title?: string;
    }) {
      const existing = yield* projectStore.findActiveByWorkspaceRoot(input.workspaceRoot);
      if (Option.isSome(existing)) {
        return existing.value.projectId;
      }

      const title = input.title ?? (path.basename(input.workspaceRoot) || "project");
      const create = (projectId: ProjectId, commandId: CommandId) =>
        projects.create({ commandId, projectId, title, workspaceRoot: input.workspaceRoot }).pipe(
          // A replayed receipt hands back the project the user (or a placement
          // move) later deleted; treat that as "not created" so the salted
          // retry below makes a fresh one.
          Effect.map((project) =>
            project.deletedAt === null ? Option.some(project.id) : Option.none<ProjectId>(),
          ),
          // A concurrent create for the same root loses the race; take the winner.
          Effect.catchTag("ProjectConflictError", (error) =>
            Effect.succeed(Option.some(error.conflictingProjectId)),
          ),
          Effect.catch((error) =>
            Effect.logDebug("session.importer.project-create-skipped", {
              workspaceRoot: input.workspaceRoot,
              error,
            }).pipe(Effect.as(Option.none<ProjectId>())),
          ),
        );

      const created = yield* create(
        importedProjectId(input.workspaceRoot),
        CommandId.make(`cli-import:project:${hashHex(input.workspaceRoot)}`),
      );
      if (Option.isSome(created)) {
        return created.value;
      }

      // The deterministic create was receipt-shadowed: an earlier import
      // created this project and the user has since deleted it. Salt the ids
      // with the session so the workspace gets a fresh project.
      const salted = yield* create(
        ProjectId.make(`cli-import-project-${hashHex(input.workspaceRoot)}-${input.sessionId}`),
        CommandId.make(`cli-import:project:${hashHex(input.workspaceRoot)}:${input.sessionId}`),
      );
      if (Option.isSome(salted)) {
        return salted.value;
      }
      return yield* new SessionImportError({
        detail: `Unable to create or find a project for workspace root '${input.workspaceRoot}'.`,
      });
    });

    /**
     * Appends source messages past `fromLineIndex` to an existing thread and
     * moves the thread's recency to its newest message. Messages the thread
     * already holds (an interrupted earlier append) are skipped.
     */
    const appendMessages = Effect.fn("SessionImporter.appendMessages")(function* (input: {
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly fromLineIndex: number;
      /** Only rows after this instant; the rest are T3's own turns. */
      readonly afterMs?: number;
    }) {
      const candidates = input.parsed.messages.filter(
        (message) =>
          message.lineIndex >= input.fromLineIndex &&
          (input.afterMs === undefined ||
            DateTime.toEpochMillis(dateTime(message.timestamp)) > input.afterMs),
      );
      if (candidates.length === 0) return 0;
      // Migrated threads hydrate their v1 transcript lazily; finish that first
      // so appended messages take positions after it.
      yield* threads.ensureLegacyTranscript(input.threadId);
      const existing = yield* sql<{ readonly message_id: string }>`
        SELECT message_id FROM orchestration_v2_projection_messages
        WHERE thread_id = ${input.threadId}
      `;
      const known = new Set(existing.map((row) => row.message_id));
      // T3's own copies of native rows carry the row's uuid after the last
      // colon (`assistant:<uuid>`, `message:provider:…:native-item:<uuid>`).
      const knownNative = new Set(
        existing.flatMap((row) => {
          const colon = row.message_id.lastIndexOf(":");
          return colon === -1 ? [] : [row.message_id.slice(colon + 1)];
        }),
      );
      const fresh = candidates.filter(
        (message) =>
          !known.has(importedMessageId(input.sessionId, message.sourceId)) &&
          !knownNative.has(message.sourceId),
      );
      if (fresh.length === 0) return 0;
      const records = yield* threads.getThreadRecords(input.threadId, []);
      const thread = records.thread;
      const newest = dateTime(fresh[fresh.length - 1]!.timestamp);
      const events: Array<OrchestrationV2DomainEvent> = fresh.flatMap((message) =>
        messageEvents({ threadId: input.threadId, sessionId: input.sessionId, message }),
      );
      const isNewer = DateTime.toEpochMillis(newest) > DateTime.toEpochMillis(thread.updatedAt);
      // Threads imported before the holder stamp existed pick it up on their
      // next appended message, riding the recency bump instead of a sweep.
      const heldBy = heldByForOriginator(input.parsed.originator);
      const stampHeldBy = heldBy !== undefined && thread.heldBy === undefined;
      if (isNewer || stampHeldBy) {
        events.push({
          id: isNewer
            ? importEventId(input.threadId, `recency:${fresh[fresh.length - 1]!.sourceId}`)
            : importEventId(input.threadId, `held-by:${heldBy}`),
          type: "thread.metadata-updated",
          threadId: input.threadId,
          providerInstanceId: thread.providerInstanceId,
          occurredAt: isNewer ? newest : thread.updatedAt,
          payload: {
            ...thread,
            ...(isNewer ? { updatedAt: newest } : {}),
            ...(stampHeldBy ? { heldBy } : {}),
          },
        });
      }
      yield* eventSink.write({ events });
      return fresh.length;
    });

    const upsertImportBinding = (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly filePath: string;
      /** Omitted on incremental updates so the merge keeps `cwd`. */
      readonly cwd?: string;
      /** Sweep-end machine placement decision; omitted keeps the stored one. */
      readonly launchClass?: "human" | "kin" | "automation";
    }) => {
      const userTurnCount = input.parsed.messages.filter(
        (message) => message.role === "user",
      ).length;
      const resumeCursor =
        input.driver === CLAUDE_DRIVER
          ? {
              threadId: input.threadId,
              resume: input.sessionId,
              turnCount: userTurnCount,
            }
          : { threadId: input.sessionId };
      return mergeBinding({
        threadId: input.threadId,
        provider: input.driver,
        resumeCursor,
        runtimePayload: {
          ...(input.cwd !== undefined ? { cwd: input.cwd } : {}),
          ...(input.parsed.forkSignature !== undefined && input.parsed.forkSignature.length > 0
            ? { importForkSig: input.parsed.forkSignature }
            : {}),
          ...(input.launchClass !== undefined ? { importLaunch: input.launchClass } : {}),
          importCursor: {
            sourceSessionId: input.sessionId,
            sourcePath: input.filePath,
            lineCount: input.parsed.lineCount,
            epoch: IMPORT_EPOCH,
          },
        },
      });
    };

    /**
     * A thread T3 owns whose native session kept going without T3 (a terminal
     * process without the mod, or from before the mod existed) gets the
     * transcript rows T3 never saw, as imported history, so the thread reads
     * whole from every client. Rows up to T3's own watermark are T3's turns
     * and stay out. While a T3 run is writing the thread, or a mod process
     * holds the session (the bridge adopts its turns), nothing is imported.
     */
    const catchUpOwnedThread = Effect.fn("SessionImporter.catchUpOwnedThread")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly filePath: string;
      readonly parsed: ParsedCliSession;
      readonly threadId: ThreadId;
      readonly fromLineIndex: number;
      readonly context: SweepContext;
    }) {
      if (input.parsed.lineCount <= input.fromLineIndex) return;
      if (yield* hasActiveRun(input.threadId)) return;
      if (yield* hasLiveBridgeProcess(input.sessionId)) return;
      const afterMs = yield* ownedWatermarkMs(input.threadId);
      const appendedCount = yield* appendMessages({
        sessionId: input.sessionId,
        threadId: input.threadId,
        parsed: input.parsed,
        fromLineIndex: input.fromLineIndex,
        ...(afterMs === undefined ? {} : { afterMs }),
      });
      yield* upsertImportBinding({
        driver: input.driver,
        sessionId: input.sessionId,
        threadId: input.threadId,
        parsed: input.parsed,
        filePath: input.filePath,
      });
      if (appendedCount > 0) {
        input.context.stats.updatedCount += 1;
        yield* Effect.logInfo("session.importer.owned-thread-caught-up", {
          provider: input.driver,
          sessionId: input.sessionId,
          threadId: input.threadId,
          appendedCount,
        });
      }
    });

    const importFreshSession = Effect.fn("SessionImporter.importFreshSession")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly filePath: string;
      readonly parsed: ParsedCliSession;
      readonly context: SweepContext;
      /** Overrides normal project placement (machine sessions -> Automations). */
      readonly placement?: {
        readonly workspaceRoot: string;
        readonly projectTitle: string;
        /** Salts thread ids when re-importing over a deleted thread. */
        readonly idVariant?: string;
      };
      readonly launchClass?: "human" | "kin" | "automation";
    }) {
      const { driver, sessionId, parsed } = input;
      const idVariant = input.placement?.idVariant ?? "";
      const firstMessage = parsed.messages[0];
      const lastMessage = parsed.messages[parsed.messages.length - 1];
      // Codex sessions without git metadata are project-less "chats": they
      // group into one shared Chats project instead of minting a project
      // per throwaway cwd.
      const isChat =
        input.placement === undefined && driver === CODEX_DRIVER && parsed.hasProject !== true;
      if (firstMessage === undefined || lastMessage === undefined) {
        return;
      }
      if (parsed.cwd === undefined && !isChat && input.placement === undefined) {
        return;
      }
      const workspaceRoot =
        input.placement?.workspaceRoot ??
        (isChat
          ? input.context.chatsWorkspaceRoot
          : path.resolve(expandHomePath(parsed.cwd ?? input.context.chatsWorkspaceRoot)));
      const projectTitle =
        input.placement?.projectTitle ?? (isChat ? CHATS_PROJECT_TITLE : undefined);
      // T3 projects need an existing folder. A session whose folder is gone
      // still imports, into the shared Chats project.
      const folderExists = yield* fs.exists(workspaceRoot).pipe(Effect.orElseSucceed(() => false));
      const projectId = folderExists
        ? yield* ensureProject({
            workspaceRoot,
            sessionId,
            ...(projectTitle !== undefined ? { title: projectTitle } : {}),
          })
        : yield* ensureProject({
            workspaceRoot: input.context.chatsWorkspaceRoot,
            sessionId,
            title: CHATS_PROJECT_TITLE,
          });
      const threadId = importedThreadId(sessionId, idVariant);
      if (yield* threadEverExisted(threadId)) {
        // Already imported (or deleted by the user): never re-create it.
        return;
      }
      const instanceId = defaultInstanceIdForDriver(driver);
      const createdAt = dateTime(parsed.startedAt ?? firstMessage.timestamp);
      const updatedAt = dateTime(lastMessage.timestamp);
      const providerThreadId = deriveProviderThread({ driver, nativeThreadId: sessionId });
      const heldBy = heldByForOriginator(parsed.originator);
      const thread: OrchestrationV2AppThread = {
        createdBy: "system",
        creationSource: "server",
        id: threadId,
        projectId,
        title: parsed.title ?? "Imported session",
        providerInstanceId: instanceId,
        modelSelection: {
          instanceId,
          model: DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL,
        },
        runtimeMode: "full-access",
        interactionMode: DEFAULT_PROVIDER_INTERACTION_MODE,
        branch: null,
        // Deliberately null: the imported session ran in the workspace root
        // itself, and provider cwd resolution falls back to it.
        worktreePath: null,
        linkedPullRequest: null,
        branchPullRequest: null,
        activeProviderThreadId: providerThreadId,
        historyOrigin: "v1_import",
        ...(heldBy === undefined ? {} : { heldBy }),
        lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: threadId },
        forkedFrom: null,
        createdAt,
        updatedAt,
        archivedAt: null,
        // Left to the settlement sweep: age-based placement under the user's settings.
        settledOverride: null,
        settledAt: null,
        unsettledAt: null,
        snoozedUntil: null,
        snoozedAt: null,
        pinnedAt: null,
        pinOrderKey: null,
        activeOrderKey: null,
        lastVisitedAt: null,
        deletedAt: null,
      };
      const providerThread: OrchestrationV2ProviderThread = {
        id: providerThreadId,
        driver,
        providerInstanceId: instanceId,
        providerSessionId: null,
        appThreadId: threadId,
        ownerNodeId: null,
        nativeThreadRef: { driver, nativeId: sessionId, strength: "strong" },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt,
        updatedAt,
      };
      yield* eventSink.write({
        events: [
          {
            id: importEventId(threadId, "created"),
            type: "thread.created",
            threadId,
            providerInstanceId: instanceId,
            occurredAt: createdAt,
            payload: thread,
          },
          ...parsed.messages.flatMap((message) => messageEvents({ threadId, sessionId, message })),
          {
            id: importEventId(threadId, "provider-thread"),
            type: "provider-thread.updated",
            threadId,
            driver,
            providerInstanceId: instanceId,
            occurredAt: updatedAt,
            payload: providerThread,
          },
        ],
      });
      yield* upsertImportBinding({
        driver,
        sessionId,
        threadId,
        parsed,
        filePath: input.filePath,
        cwd: parsed.cwd !== undefined ? path.resolve(expandHomePath(parsed.cwd)) : workspaceRoot,
        ...(input.launchClass !== undefined ? { launchClass: input.launchClass } : {}),
      });

      input.context.stats.importedCount += 1;
      yield* Effect.logInfo("session.importer.imported", {
        provider: driver,
        sessionId,
        threadId,
        projectId,
        messageCount: parsed.messages.length,
      });
    });

    const importSessionIncrement = Effect.fn("SessionImporter.importSessionIncrement")(
      function* (input: {
        readonly driver: ProviderDriverKind;
        readonly sessionId: string;
        readonly filePath: string;
        readonly parsed: ParsedCliSession;
        readonly binding: ImportBinding;
        readonly cursor: ImportCursor;
        readonly context: SweepContext;
        /** Set when re-entered from the sweep-end machine placement pass. */
        readonly skipMachineDecision?: boolean;
      }) {
        const { driver, sessionId, parsed, cursor } = input;
        // A T3 run has happened on this thread since import: T3 drives its
        // native session now. A Claude session can still grow without T3 (a
        // terminal without the mod), so its thread keeps catching up from
        // where T3's own record ends. A Codex thread is handed over for good:
        // drop the import cursor so this source file is never tailed again and
        // T3's own turns aren't re-imported as duplicates.
        if (yield* hasT3Runs(input.binding.threadId)) {
          if (driver === CLAUDE_DRIVER) {
            yield* catchUpOwnedThread({
              driver,
              sessionId,
              filePath: input.filePath,
              parsed,
              threadId: input.binding.threadId,
              fromLineIndex: cursor.lineCount,
              context: input.context,
            });
            return;
          }
          yield* mergeBinding({
            threadId: input.binding.threadId,
            provider: driver,
            runtimePayload: { importCursor: null },
          });
          yield* Effect.logInfo("session.importer.thread-adopted", {
            provider: driver,
            sessionId,
            threadId: input.binding.threadId,
          });
          return;
        }
        // Agent-spawned subagent sessions are provider plumbing — Codex's own
        // UI never shows them. Drop the imported thread and tombstone the
        // binding; the fresh-import path skips these files from now on.
        if (parsed.launch === "subagent") {
          const subagentShell = yield* getThread(input.binding.threadId);
          if (Option.isNone(subagentShell)) {
            return;
          }
          yield* deleteThread(
            input.binding.threadId,
            importCommandId(driver, sessionId, "subagent-thread-delete"),
          );
          yield* mergeBinding({
            threadId: input.binding.threadId,
            provider: driver,
            resumeCursor: null,
            runtimePayload: { importCursor: null },
          });
          yield* deleteProjectIfEmpty(
            subagentShell.value.projectId,
            importCommandId(driver, sessionId, "subagent-project-delete"),
          );
          yield* Effect.logInfo("session.importer.subagent-removed", {
            provider: driver,
            sessionId,
            threadId: input.binding.threadId,
          });
          return;
        }
        // Machine-launched sessions wait for the sweep-end placement pass,
        // which knows every human session's fork signature: kin of a human
        // conversation stays put, everything else moves to Automations. The
        // decision is recorded on the binding so it runs once.
        if (
          isAutomationSession(parsed) &&
          input.skipMachineDecision !== true &&
          readLaunchClass(input.binding.runtimePayload) === undefined
        ) {
          input.context.deferredMachine.push({
            kind: "existing",
            driver,
            sessionId,
            filePath: input.filePath,
            parsed,
            binding: input.binding,
            cursor,
          });
          return;
        }
        // Epoch migration: a project-less codex chat imported under the old
        // one-project-per-cwd layout moves into the shared Chats project.
        // Delete the old thread, re-import it fresh under an epoch-scoped id,
        // and drop the per-chat project it leaves empty. A thread the user
        // already deleted stays dead.
        if (
          driver === CODEX_DRIVER &&
          cursor.epoch === undefined &&
          parsed.hasProject !== true &&
          !isCurrentEpochThread(input.binding.threadId)
        ) {
          const migrateShell = yield* getThread(input.binding.threadId);
          if (Option.isNone(migrateShell)) {
            return;
          }
          yield* deleteThread(
            input.binding.threadId,
            importCommandId(driver, sessionId, "migrate-thread-delete"),
          );
          // Tombstone the old binding so it stops claiming this source
          // session; the re-import below owns it under an epoch-scoped
          // thread id (deleted thread ids cannot be re-created).
          yield* mergeBinding({
            threadId: input.binding.threadId,
            provider: driver,
            resumeCursor: null,
            runtimePayload: { importCursor: null },
          });
          yield* importFreshSession({
            driver,
            sessionId,
            filePath: input.filePath,
            parsed,
            context: input.context,
          });
          yield* deleteProjectIfEmpty(
            migrateShell.value.projectId,
            importCommandId(driver, sessionId, "migrate-project-delete"),
          );
          yield* Effect.logInfo("session.importer.chat-migrated", {
            provider: driver,
            sessionId,
            threadId: input.binding.threadId,
            oldProjectId: migrateShell.value.projectId,
          });
          return;
        }
        const threadId = input.binding.threadId;
        const shell = yield* getThread(threadId);
        if (Option.isNone(shell)) {
          // The imported thread was deleted in T3; leave the source alone
          // instead of resurrecting it.
          return;
        }
        if (parsed.lineCount <= cursor.lineCount) {
          return;
        }

        const appendedCount = yield* appendMessages({
          sessionId,
          threadId,
          parsed,
          fromLineIndex: cursor.lineCount,
        });
        yield* upsertImportBinding({
          driver,
          sessionId,
          threadId,
          parsed,
          filePath: input.filePath,
        });
        if (appendedCount > 0) {
          input.context.stats.updatedCount += 1;
          yield* Effect.logDebug("session.importer.updated", {
            provider: driver,
            sessionId,
            threadId,
            appendedCount,
          });
        }
      },
    );

    const processFile = Effect.fn("SessionImporter.processFile")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly filePath: string;
      readonly context: SweepContext;
    }) {
      const { driver, filePath, context } = input;
      const info = yield* fs.stat(filePath);
      const size = Number(info.size);
      const mtimeMs = Option.match(info.mtime, {
        onNone: () => 0,
        onSome: (mtime) => mtime.getTime(),
      });
      const fingerprint = fileFingerprints.get(filePath);
      if (
        fingerprint !== undefined &&
        fingerprint.size === size &&
        fingerprint.mtimeMs === mtimeMs
      ) {
        return;
      }
      // Still being written to: skip without recording the fingerprint so
      // the file is picked up on the first sweep after it settles.
      if (settleMs > 0) {
        const nowMs = yield* Clock.currentTimeMillis;
        if (nowMs - mtimeMs < settleMs) {
          return;
        }
      }
      // Recorded before importing: a failed import is retried when the file
      // changes again rather than re-attempted (and re-logged) every sweep.
      fileFingerprints.set(filePath, { size, mtimeMs });

      if (size > maxFileBytes) {
        yield* Effect.logInfo("session.importer.file-too-large", {
          filePath,
          size,
          maxFileBytes,
        });
        return;
      }

      const content = yield* fs.readFileString(filePath);
      const parsed =
        driver === CLAUDE_DRIVER
          ? parseClaudeSessionJsonl(content)
          : parseCodexRolloutJsonl(content);
      if (parsed.malformedLineCount > 0) {
        yield* Effect.logDebug("session.importer.malformed-lines", {
          filePath,
          malformedLineCount: parsed.malformedLineCount,
        });
      }

      const fileStem = path.basename(filePath, ".jsonl");
      const sessionId =
        parsed.sessionId ??
        (driver === CLAUDE_DRIVER && fileStem.length > 0 ? fileStem : undefined);
      if (sessionId === undefined) {
        return;
      }
      const key = sourceKey(driver, sessionId);
      if (context.seenSessionIds.has(key)) {
        return;
      }
      context.seenSessionIds.add(key);

      // Human-side sessions register their fork signature so the sweep-end
      // machine placement pass can recognize machine-launched forks of them.
      if (
        !isAutomationSession(parsed) &&
        parsed.launch !== "subagent" &&
        parsed.forkSignature !== undefined
      ) {
        for (const sigId of parsed.forkSignature) {
          if (!context.sigOwners.has(sigId)) {
            context.sigOwners.set(sigId, key);
          }
        }
      }

      const binding = context.bindingsBySource.get(key);
      if (binding !== undefined) {
        // Repair XML-noise titles left by first-message titling on sessions
        // that open with a slash command. Runs for adopted threads too — the
        // noise-prefix guard is what keeps user renames safe, not ownership.
        if (
          parsed.title !== undefined &&
          !isCommandNoiseText(parsed.title) &&
          binding.threadId.startsWith("cli-import")
        ) {
          const titleShell = yield* getThread(binding.threadId);
          if (
            Option.isSome(titleShell) &&
            isCommandNoiseText(titleShell.value.title) &&
            titleShell.value.title !== parsed.title
          ) {
            yield* threads.dispatch({
              type: "thread.metadata.update",
              commandId: importCommandId(driver, sessionId, `retitle:${hashHex(parsed.title)}`),
              threadId: binding.threadId,
              title: parsed.title,
            });
            yield* Effect.logInfo("session.importer.retitled", {
              provider: driver,
              sessionId,
              threadId: binding.threadId,
            });
          }
        }
        // Backfill fork signatures onto bindings imported before signatures
        // existed (including adopted/native ones), so their threads can join
        // fork families.
        if (
          readForkSignature(binding.runtimePayload) === undefined &&
          parsed.forkSignature !== undefined &&
          parsed.forkSignature.length > 0
        ) {
          yield* mergeBinding({
            threadId: binding.threadId,
            provider: driver,
            runtimePayload: { importForkSig: parsed.forkSignature },
          });
        }
        let cursor = readImportCursor(binding.runtimePayload);
        if (cursor === undefined && driver === CLAUDE_DRIVER) {
          // A Claude thread T3 took over, in any era, with no cursor: catch it
          // up from the whole transcript. The watermark keeps T3's own turns
          // out, and the binding gets a cursor for the sweeps after.
          if (Option.isSome(yield* getThread(binding.threadId))) {
            yield* catchUpOwnedThread({
              driver,
              sessionId,
              filePath,
              parsed,
              threadId: binding.threadId,
              fromLineIndex: 0,
              context,
            });
          }
          return;
        }
        // Self-heal: an importer-created thread (cli-import- prefix) that
        // lost its cursor although T3 never ran it gets the cursor rebuilt
        // at the current file position. A thread T3 ran — native messages,
        // a v2 run, or v1's adoption markers on the binding — keeps its
        // handoff and is never tailed again.
        const payload = isRecord(binding.runtimePayload) ? binding.runtimePayload : undefined;
        const adoptedBeforeV2 =
          binding.status !== "stopped" || Boolean(payload?.activeTurnId) || Boolean(payload?.model);
        if (
          cursor === undefined &&
          binding.threadId.startsWith("cli-import-") &&
          !adoptedBeforeV2 &&
          Option.isSome(yield* getThread(binding.threadId)) &&
          !(yield* hasT3Runs(binding.threadId)) &&
          !(yield* hasT3Messages(binding.threadId))
        ) {
          cursor = {
            sourceSessionId: sessionId,
            sourcePath: filePath,
            lineCount: parsed.lineCount,
            // Epoch-1 chats keep the legacy epoch so the Chats migration
            // below still runs for them; everything else is stamped current.
            ...(driver === CODEX_DRIVER &&
            parsed.hasProject !== true &&
            !isCurrentEpochThread(binding.threadId)
              ? {}
              : { epoch: IMPORT_EPOCH }),
          };
          yield* mergeBinding({
            threadId: binding.threadId,
            provider: driver,
            runtimePayload: { importCursor: cursor },
          });
          yield* Effect.logInfo("session.importer.cursor-repaired", {
            provider: driver,
            sessionId,
            threadId: binding.threadId,
          });
        }
        if (
          cursor === undefined ||
          cursor.sourceSessionId !== sessionId ||
          cursor.sourcePath !== filePath
        ) {
          // T3-native session, or a session adopted by live T3 chat (or a
          // secondary rollout file for it): not ours to import.
          return;
        }
        yield* importSessionIncrement({
          driver,
          sessionId,
          filePath,
          parsed,
          binding,
          cursor,
          context,
        });
        return;
      }

      // A session T3 itself started or resumed: it already has its thread. A
      // Claude one still catches up with rows T3 never wrote (a terminal
      // without the mod); the binding this creates carries the cursor on.
      if (context.t3NativeSessions.has(key)) {
        if (driver === CLAUDE_DRIVER) {
          const owned = yield* lookupOwnedThread(driver, sessionId);
          if (owned !== undefined) {
            yield* catchUpOwnedThread({
              driver,
              sessionId,
              filePath,
              parsed,
              threadId: owned,
              fromLineIndex: 0,
              context,
            });
          }
        }
        return;
      }
      if (!isEligibleCliSession(parsed)) {
        return;
      }
      // Agent-spawned subagent sessions never import: Codex's own UI hides
      // them, and they are conversation plumbing, not conversations.
      if (parsed.launch === "subagent") {
        return;
      }
      if (isAutomationSession(parsed)) {
        context.deferredMachine.push({ kind: "fresh", driver, sessionId, filePath, parsed });
        return;
      }
      yield* importFreshSession({ driver, sessionId, filePath, parsed, context });
    });

    /** Whether a machine-launched session shares early history with any
     * human-side session — i.e. it is a fork of a real conversation. */
    const hasHumanKin = (context: SweepContext, parsed: ParsedCliSession, selfKey: string) =>
      parsed.forkSignature?.some((sigId) => {
        const owner = context.sigOwners.get(sigId);
        return owner !== undefined && owner !== selfKey;
      }) ?? false;

    const processDeferredMachine = Effect.fn("SessionImporter.processDeferredMachine")(function* (
      context: SweepContext,
    ) {
      for (const deferred of context.deferredMachine) {
        const { driver, sessionId, filePath, parsed } = deferred;
        const key = sourceKey(driver, sessionId);
        const kin = hasHumanKin(context, parsed, key);
        if (deferred.kind === "fresh") {
          yield* importFreshSession({
            driver,
            sessionId,
            filePath,
            parsed,
            context,
            ...(kin
              ? { launchClass: "kin" as const }
              : {
                  placement: {
                    workspaceRoot: context.automationsWorkspaceRoot,
                    projectTitle: AUTOMATIONS_PROJECT_TITLE,
                  },
                  launchClass: "automation" as const,
                }),
          });
          continue;
        }
        const binding = deferred.binding;
        const cursor = deferred.cursor;
        if (binding === undefined || cursor === undefined) {
          continue;
        }
        if (kin) {
          // A fork of a human conversation: record the decision and resume
          // normal incremental handling in place.
          yield* mergeBinding({
            threadId: binding.threadId,
            provider: driver,
            runtimePayload: { importLaunch: "kin" },
          });
          yield* importSessionIncrement({
            driver,
            sessionId,
            filePath,
            parsed,
            binding,
            cursor,
            context,
            skipMachineDecision: true,
          });
          continue;
        }
        const shell = yield* getThread(binding.threadId);
        if (Option.isNone(shell)) {
          continue;
        }
        const automationsProject = yield* projectStore.findActiveByWorkspaceRoot(
          context.automationsWorkspaceRoot,
        );
        if (
          Option.isSome(automationsProject) &&
          shell.value.projectId === automationsProject.value.projectId
        ) {
          // Already placed; just record the decision.
          yield* mergeBinding({
            threadId: binding.threadId,
            provider: driver,
            runtimePayload: { importLaunch: "automation" },
          });
          continue;
        }
        // Move to Automations: delete the old thread, tombstone its
        // binding, and re-import under a variant-salted id (deleted thread
        // ids can never be re-created).
        const oldProjectId = shell.value.projectId;
        yield* deleteThread(
          binding.threadId,
          importCommandId(driver, sessionId, "automation-thread-delete"),
        );
        yield* mergeBinding({
          threadId: binding.threadId,
          provider: driver,
          resumeCursor: null,
          runtimePayload: { importCursor: null },
        });
        yield* importFreshSession({
          driver,
          sessionId,
          filePath,
          parsed,
          context,
          placement: {
            workspaceRoot: context.automationsWorkspaceRoot,
            projectTitle: AUTOMATIONS_PROJECT_TITLE,
            idVariant: "a",
          },
          launchClass: "automation",
        });
        yield* deleteProjectIfEmpty(
          oldProjectId,
          importCommandId(driver, sessionId, "automation-project-delete"),
        );
        yield* Effect.logInfo("session.importer.automation-migrated", {
          provider: driver,
          sessionId,
          oldThreadId: binding.threadId,
        });
      }
    });

    /**
     * Group imported threads into fork families: sessions sharing any early
     * history id are branches of one conversation. The family id is the
     * lexicographically smallest member session id (Codex uuidv7 ids sort by
     * creation time, so that is the original session). Only families with two
     * or more members are stamped, and only threads whose `forkFamilyId`
     * differs, without touching their recency.
     */
    const stampForkFamilies = Effect.fn("SessionImporter.stampForkFamilies")(function* () {
      const bindings = yield* listBindings;
      interface FamilyNode {
        readonly binding: ImportBinding;
        readonly sessionId: string;
        readonly sig: ReadonlyArray<string>;
      }
      const nodes: FamilyNode[] = [];
      for (const binding of bindings) {
        if (!belongsToScannedProviderHome(binding)) continue;
        const sig = readForkSignature(binding.runtimePayload);
        if (sig === undefined) {
          continue;
        }
        const sessionId =
          readImportCursor(binding.runtimePayload)?.sourceSessionId ?? readResumeSessionId(binding);
        if (sessionId === undefined) {
          continue;
        }
        nodes.push({ binding, sessionId, sig });
      }

      const parent = new Map<number, number>();
      const find = (index: number): number => {
        let root = index;
        while ((parent.get(root) ?? root) !== root) {
          root = parent.get(root) ?? root;
        }
        parent.set(index, root);
        return root;
      };
      const sigOwner = new Map<string, number>();
      for (let index = 0; index < nodes.length; index += 1) {
        parent.set(index, index);
        for (const sigId of nodes[index]?.sig ?? []) {
          const owner = sigOwner.get(sigId);
          if (owner === undefined) {
            sigOwner.set(sigId, index);
          } else {
            parent.set(find(index), find(owner));
          }
        }
      }
      const components = new Map<number, FamilyNode[]>();
      for (let index = 0; index < nodes.length; index += 1) {
        const node = nodes[index];
        if (node === undefined) {
          continue;
        }
        const root = find(index);
        components.set(root, [...(components.get(root) ?? []), node]);
      }

      let stampedCount = 0;
      for (const members of components.values()) {
        if (members.length < 2) {
          continue;
        }
        const familyId = members
          .map((member) => member.sessionId)
          .sort((left, right) => (left < right ? -1 : 1))[0];
        if (familyId === undefined) {
          continue;
        }
        for (const member of members) {
          const live = yield* getThread(member.binding.threadId);
          if (Option.isNone(live) || live.value.forkFamilyId === familyId) {
            continue;
          }
          const records = yield* threads.getThreadRecords(member.binding.threadId, []);
          const thread = records.thread;
          yield* eventSink.write({
            events: [
              {
                id: importEventId(thread.id, `family:${hashHex(familyId)}`),
                type: "thread.metadata-updated",
                threadId: thread.id,
                providerInstanceId: thread.providerInstanceId,
                occurredAt: thread.updatedAt,
                payload: { ...thread, forkFamilyId: familyId },
              },
            ],
          });
          stampedCount += 1;
        }
      }
      if (stampedCount > 0) {
        yield* Effect.logInfo("session.importer.families-stamped", { stampedCount });
      }
    });

    const sweep = Effect.gen(function* () {
      const settings = yield* serverSettings.getSettings;
      const claudeProjectsDir = options?.claudeProjectsDir ?? resolveClaudeProjectsDir(settings);
      const codexSessionsDir =
        options?.codexSessionsDir ??
        path.join(
          (yield* resolveCodexHomeLayout(settings.providers.codex).pipe(
            Effect.provideService(Path.Path, path),
          )).sharedHomePath,
          "sessions",
        );

      const bindings = yield* listBindings;
      const bindingsBySource = new Map<string, ImportBinding>();
      const sigOwners = new Map<string, string>();
      for (const binding of bindings) {
        if (!belongsToScannedProviderHome(binding)) continue;
        const cursor = readImportCursor(binding.runtimePayload);
        if (cursor !== undefined) {
          bindingsBySource.set(sourceKey(binding.provider, cursor.sourceSessionId), binding);
        }
        const resumeSessionId = readResumeSessionId(binding);
        if (resumeSessionId !== undefined) {
          const key = sourceKey(binding.provider, resumeSessionId);
          if (!bindingsBySource.has(key)) {
            bindingsBySource.set(key, binding);
          }
        }
        // Signatures from prior sweeps seed the kinship registry; automation
        // bindings are excluded so automations never claim kinship.
        const sig = readForkSignature(binding.runtimePayload);
        const sessionId = cursor?.sourceSessionId ?? resumeSessionId;
        if (
          sig !== undefined &&
          sessionId !== undefined &&
          readLaunchClass(binding.runtimePayload) !== "automation"
        ) {
          const key = sourceKey(binding.provider, sessionId);
          for (const sigId of sig) {
            if (!sigOwners.has(sigId)) {
              sigOwners.set(sigId, key);
            }
          }
        }
      }

      const context: SweepContext = {
        bindingsBySource,
        t3NativeSessions: yield* listT3NativeSessions,
        seenSessionIds: new Set<string>(),
        stats: { importedCount: 0, updatedCount: 0 },
        chatsWorkspaceRoot: path.dirname(codexSessionsDir),
        automationsWorkspaceRoot: path.dirname(claudeProjectsDir),
        sigOwners,
        deferredMachine: [],
      };

      const sources = [
        { driver: CLAUDE_DRIVER, dir: claudeProjectsDir },
        { driver: CODEX_DRIVER, dir: codexSessionsDir },
      ] as const;
      for (const source of sources) {
        const files = yield* listJsonlFiles(source.dir);
        for (const filePath of files) {
          yield* processFile({ driver: source.driver, filePath, context }).pipe(
            Effect.catch((error) =>
              Effect.logWarning("session.importer.file-failed", {
                provider: source.driver,
                filePath,
                error,
              }),
            ),
            Effect.catchDefect((defect) =>
              Effect.logWarning("session.importer.file-defect", {
                provider: source.driver,
                filePath,
                defect,
              }),
            ),
          );
        }
      }

      yield* processDeferredMachine(context).pipe(
        Effect.catch((error) =>
          Effect.logWarning("session.importer.machine-pass-failed", { error }),
        ),
        Effect.catchDefect((defect) =>
          Effect.logWarning("session.importer.machine-pass-defect", { defect }),
        ),
      );
      yield* stampForkFamilies().pipe(
        Effect.catch((error) =>
          Effect.logWarning("session.importer.family-pass-failed", { error }),
        ),
        Effect.catchDefect((defect) =>
          Effect.logWarning("session.importer.family-pass-defect", { defect }),
        ),
      );

      if (context.stats.importedCount > 0 || context.stats.updatedCount > 0) {
        yield* Effect.logInfo("session.importer.sweep-complete", {
          importedCount: context.stats.importedCount,
          updatedCount: context.stats.updatedCount,
        });
      }
    });

    const guardedSweep = sweep.pipe(
      Effect.catch((error: unknown) =>
        Effect.logWarning("session.importer.sweep-failed", {
          error,
        }),
      ),
      Effect.catchDefect((defect: unknown) =>
        Effect.logWarning("session.importer.sweep-defect", {
          defect,
        }),
      ),
    );

    const start: SessionImporterShape["start"] = () =>
      Effect.gen(function* () {
        if (!enabled) {
          yield* Effect.logInfo("session.importer.disabled", {
            reason: "importCliSessions is off",
          });
          return;
        }

        yield* forkParked(
          guardedSweep.pipe(Effect.repeat(Schedule.spaced(Duration.millis(sweepIntervalMs)))),
        );

        yield* Effect.logInfo("session.importer.started", {
          sweepIntervalMs,
        });
      });

    return {
      start,
      sweepNow: () => guardedSweep,
    } satisfies SessionImporterShape;
  });

export { makeSessionImporter };

export const makeSessionImporterLive = (options?: SessionImporterLiveOptions) =>
  Layer.effect(
    SessionImporter,
    Effect.gen(function* () {
      const serverConfig = yield* ServerConfig;
      const hostEnvironment = yield* HostProcessEnvironment;
      const path = yield* Path.Path;
      return yield* makeSessionImporter({
        enabled: serverConfig.importCliSessions ?? false,
        bridgeDir:
          hostEnvironment[CLAUDE_BRIDGE_DIR_ENV]?.trim() ||
          path.join(serverConfig.stateDir, "bridge"),
        ...(serverConfig.importSweepIntervalMs !== undefined
          ? { sweepIntervalMs: serverConfig.importSweepIntervalMs }
          : {}),
        ...(serverConfig.importSettleMs !== undefined
          ? { settleMs: serverConfig.importSettleMs }
          : {}),
        ...(serverConfig.importMaxFileBytes !== undefined
          ? { maxFileBytes: serverConfig.importMaxFileBytes }
          : {}),
        ...(serverConfig.importAutomationRoots !== undefined &&
        serverConfig.importAutomationRoots.length > 0
          ? { automationRoots: serverConfig.importAutomationRoots }
          : {}),
        ...options,
      });
    }),
  );

export const SessionImporterLive = makeSessionImporterLive();
