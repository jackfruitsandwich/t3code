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
  RunId,
  TurnItemId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ThreadHeldBy,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ProviderThread,
  type ServerSettings,
} from "@t3tools/contracts";
import { claudePromptAsDrawn } from "@t3tools/shared/claudePastedContent";
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

import { makeClaudeContinuationReader } from "../../claude-bridge/claudeContinuation.ts";
import {
  CLAUDE_BRIDGE_DIR_ENV,
  CLAUDE_TURN_IN_FLIGHT_MARGIN_MS,
  isClaudeBridgeSessionLive,
  parseClaudeBridgeSessionFile,
  threadSettingsForClaudeSession,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { ServerConfig } from "../../config.ts";
import * as EventSink from "../../orchestration-v2/EventSink.ts";
import { deriveProviderThread } from "../../orchestration-v2/IdAllocator.ts";
import * as ProjectStore from "../../orchestration-v2/ProjectStore.ts";
import { CONTINUATION_MESSAGE_TEXT } from "../../orchestration-v2/ProviderContinuationRequests.ts";
import * as ThreadManagementService from "../../orchestration-v2/ThreadManagementService.ts";
import * as TurnItemPositionStore from "../../orchestration-v2/TurnItemPositionStore.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import {
  ProviderSessionRuntimeRepository,
  type ProviderSessionRuntime,
} from "../../persistence/ProviderSessionRuntime.ts";
import { ProjectService } from "../../project/ProjectService.ts";
import { resolveCodexHomeLayout } from "../../provider/Drivers/CodexHomeLayout.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import {
  claudeTaskNotice,
  isClaudeTaskNoticeText,
  parseClaudeSessionJsonl,
} from "../claudeSessionParser.ts";
import { parseCodexRolloutJsonl } from "../codexRolloutParser.ts";
import {
  isCommandNoiseText,
  isEligibleCliSession,
  isRecord,
  parseJsonlLine,
  type ParsedCliSession,
  type ParsedSessionMessage,
  type ParsedSessionNotice,
  type ParsedSessionSystemNotice,
  truncateSessionTitle,
} from "../sessionImportTypes.ts";
import { SessionImporter, type SessionImporterShape } from "../Services/SessionImporter.ts";
import {
  bandForTime,
  claudeRecordLines,
  type HealingPlacement,
  type HealingKnownMessage,
  type HealingRun,
  planImportedRowOrdering,
  planImportedRowReseating,
  planNoticeHealing,
  planTaskNoticeHealing,
  planWithdrawnRuns,
  planRecordSeating,
  planTranscriptHealing,
  recordKeyOfTurnItem,
  RUN_BAND,
  sentPromptLines,
} from "../transcriptHealing.ts";

const DEFAULT_SWEEP_INTERVAL_MS = 30 * 1000;

const sameWords = (left: string, right: string) =>
  left.replace(/\s+/gu, " ").trim() === right.replace(/\s+/gu, " ").trim();

/** How many rows at the record's end are what a command printed. */
const trailingCommandOutput = (messages: ReadonlyArray<ParsedSessionMessage>): number => {
  let count = 0;
  while (count < messages.length) {
    const message = messages[messages.length - 1 - count]!;
    if (message.role !== "assistant" || message.command !== true) break;
    count += 1;
  }
  return count;
};

/**
 * The part of a running session's record that is history for the turn in
 * flight. The turn is reported with the record's size at its start, but
 * Claude Code can have written the turn's own prompt by then (seen on a busy
 * machine: the prompt's row and the rows after it were in the file before
 * the size was read). The prompt is the turn's. Where the record's last
 * prompt reads as the turn's, with nothing after it but what a command
 * printed, history ends before that row.
 */
export const claudeHistoryBeforeTurn = (history: string, promptText: string): string => {
  if (promptText.trim().length === 0) return history;
  const { messages } = parseClaudeSessionJsonl(history);
  const prompt = messages[messages.length - 1 - trailingCommandOutput(messages)];
  if (prompt === undefined || prompt.role !== "user" || prompt.midTurn === true) return history;
  if (!sameWords(prompt.text, promptText)) return history;
  return history
    .split("\n")
    .slice(0, prompt.lineIndex)
    .map((line) => `${line}\n`)
    .join("");
};

/**
 * How many rows at the end of a live session's record belong to a command
 * the live attachment is still to show as a turn of its own: the command's
 * line while it runs (nothing printed yet), and the line with what it
 * printed for a moment after. Older than that, nothing will show them, and
 * they are history like any other row.
 */
const TYPED_COMMAND_LINE = /^\/[A-Za-z][\w:.-]*(?:\s|$)/;
export const claudeCommandRowsInFlight = (
  messages: ReadonlyArray<ParsedSessionMessage>,
  nowMs: number,
): number => {
  const printed = trailingCommandOutput(messages);
  const line = messages[messages.length - 1 - printed];
  if (line === undefined || line.role !== "user") return 0;
  // A compaction writes the command's own row when it is over: while it
  // runs, the record ends with the line as the person typed it.
  const typedOnly = printed === 0 && TYPED_COMMAND_LINE.test(line.text.trim());
  if (line.command !== true && !typedOnly) return 0;
  const newest = Date.parse(messages[messages.length - 1]!.timestamp);
  const age = Math.abs(nowMs - newest);
  return age < (printed === 0 ? COMMAND_RUNNING_MS : COMMAND_SETTLE_MS) ? printed + 1 : 0;
};

/**
 * When the last turn of a record began, while the record is fresh: its
 * newest row younger than the span in which a run still shows a turn from
 * its start. Undefined once it has been quiet that long. In a session a mod
 * process holds, that turn is its run's to show, and the run can be seconds
 * behind the record (seen on a busy machine: a short turn was over, and
 * compared, before T3 had been told it began).
 */
export const claudeFreshTurnStartMs = (
  messages: ReadonlyArray<ParsedSessionMessage>,
  nowMs: number,
): number | undefined => {
  const newest = messages[messages.length - 1];
  if (newest === undefined) return undefined;
  if (Math.abs(nowMs - Date.parse(newest.timestamp)) >= FRESH_TURN_MS) return undefined;
  for (let index = messages.length - 1; index >= 0; index -= 1) {
    const message = messages[index]!;
    if (message.role === "user" && message.midTurn !== true) return Date.parse(message.timestamp);
  }
  return undefined;
};

/**
 * Import layout epoch. Bumped when the project-placement rules change so
 * that migrated sessions can be re-imported under fresh ids. Epoch 2: codex
 * sessions without git metadata group into one shared "Chats" project
 * instead of one project per cwd.
 */
const IMPORT_EPOCH = 2;

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
/** Run statuses under which T3 is writing the thread's rows itself. */
/** The same measure the attachment leaves older rows out by: a row is history or a run's, never both. */
const IN_FLIGHT_MARGIN_MS = CLAUDE_TURN_IN_FLIGHT_MARGIN_MS;
/**
 * How many rows from a drawn reply its record copy normally sits: the
 * thinking row the record puts before the text, and little else. A copy
 * farther away was read in again later and is put back beside it.
 */
const DOUBLED_REPLY_REACH = 4;
/**
 * How far apart a reply's drawing in the terminal and its row's writing in
 * the record can be and still be one reply: the record runs seconds behind,
 * never minutes.
 */
const DOUBLED_REPLY_WINDOW_MS = 5 * 60_000;
/** Rows written again per write when a thread's rows are seated by the record. */
const SEATING_WRITE_SIZE = 250;
/** In the state directory: the ids of the threads kept in the record's line order. */
const RECORD_ORDER_THREADS_FILE = "record-order-threads.json";
/**
 * How long a command typed into a session a mod holds may run before its
 * line is taken as history without what it printed. While it runs (a
 * compaction takes minutes) the record ends with the command's line, and the
 * live attachment shows the command as one turn once it has printed.
 */
const COMMAND_RUNNING_MS = 10 * 60_000;
/** How soon after a command has printed the live attachment has shown it as a turn. */
const COMMAND_SETTLE_MS = 10_000;
/**
 * How long after a mod-held session's record last moved its last turn is
 * left for a run to show: the span in which a run still shows a turn from
 * its start (CLAUDE_BRIDGE_MISSED_TURN_REPLAY_MS in the registry, counted
 * from the turn's end), and room for what a turn does between its last row
 * and its end. Past it, a turn no run has shown is history.
 */
const FRESH_TURN_MS = 25_000;
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
   * Threads whose messages are kept in the order of the record's lines, each
   * between the tool calls it was written among (see `planRecordSeating`).
   * Other threads keep every imported row after the run that was the latest
   * at its time.
   */
  readonly recordOrderThreads?: ReadonlySet<string>;
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

const importedTurnItemId = (threadId: ThreadId, sourceId: string) =>
  TurnItemId.make(`cli-import:v2:${threadId}:${sourceId}`);

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

/**
 * For each session the thread shows, the record line its notices between
 * turns are shown from: where the record ended when T3 first compared it.
 * The record before that is history, which the thread never takes in.
 */
function readNoticeFloors(runtimePayload: unknown): Readonly<Record<string, number>> {
  if (!isRecord(runtimePayload) || !isRecord(runtimePayload.importNoticesFrom)) return {};
  return Object.fromEntries(
    Object.entries(runtimePayload.importNoticesFrom).filter((entry): entry is [string, number] =>
      Number.isInteger(entry[1]),
    ),
  );
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

/** The notice a background task's report shows as: what a live wake shows, in the report's words. */
const taskNotification = (notice: ParsedSessionNotice) =>
  ({
    source: { kind: "background_task" },
    outcome: notice.outcome,
    summary: notice.summary,
  }) as const;

/** Message + timeline events for imported text, as upstream's AgentSessionImporter writes them. */
function messageEvents(input: {
  readonly threadId: ThreadId;
  readonly sessionId: string;
  readonly message: ParsedSessionMessage;
}): ReadonlyArray<OrchestrationV2DomainEvent> {
  const { threadId, message } = input;
  const messageId = importedMessageId(input.sessionId, message.sourceId);
  const at = dateTime(message.timestamp);
  const turnItemId = importedTurnItemId(threadId, message.sourceId);
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
  // A background task reporting back is the harness's row: it shows as the
  // notice a live wake shows, with the report's own words, and nobody said it.
  const notification =
    message.role === "user" && message.notice !== undefined
      ? taskNotification(message.notice)
      : undefined;
  return [
    {
      id: importEventId(threadId, `message:${message.sourceId}`),
      type: "message.updated",
      threadId,
      occurredAt: at,
      payload: {
        createdBy: message.role === "user" && notification === undefined ? "user" : "agent",
        creationSource: "server",
        id: messageId,
        threadId,
        runId: null,
        nodeId: null,
        role: message.role,
        text: message.text,
        ...(notification === undefined ? {} : { notification }),
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
        notification !== undefined
          ? { ...base, type: "notification", ...notification }
          : message.role === "user"
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

/** The timeline row for a notice the harness wrote between turns, as a live run shows one. */
function noticeEvent(
  threadId: ThreadId,
  notice: ParsedSessionSystemNotice,
): OrchestrationV2DomainEvent {
  const at = dateTime(notice.timestamp);
  return {
    id: importEventId(threadId, `notice:${notice.sourceId}`),
    type: "turn-item.updated",
    threadId,
    occurredAt: at,
    payload: {
      id: importedTurnItemId(threadId, notice.sourceId),
      threadId,
      runId: null,
      nodeId: null,
      providerThreadId: null,
      providerTurnId: null,
      nativeItemRef: null,
      parentItemId: null,
      // EventSink assigns the real position: the place allocated for it.
      ordinal: 0,
      status: "completed",
      title: notice.text,
      startedAt: at,
      completedAt: at,
      updatedAt: at,
      type: "system_notice",
      message: notice.text,
    },
  };
}

const makeSessionImporter = (options?: SessionImporterLiveOptions) =>
  Effect.gen(function* () {
    const eventSink = yield* EventSink.EventSinkV2;
    const projectStore = yield* ProjectStore.ProjectStoreV2;
    const projects = yield* ProjectService;
    const threads = yield* ThreadManagementService.ThreadManagementService;
    const turnItemPositions = yield* TurnItemPositionStore.TurnItemPositionStoreV2;
    const runtimes = yield* ProviderSessionRuntimeRepository;
    const serverSettings = yield* ServerSettingsService;
    const sql = yield* SqlClient.SqlClient;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const enabled = options?.enabled ?? true;
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const bridgeDir = options?.bridgeDir;
    const recordOrderThreads = options?.recordOrderThreads ?? new Set<string>();
    /** What a record-ordered thread looked like when it last needed nothing moved. */
    const seatedThreads = new Map<string, string>();
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

    /**
     * The model and modes of a session a mod process holds, as its thread
     * shows them: what an attachment writes onto the thread row when a turn
     * runs. Null when no mod process holds the session. The record's own
     * note of the permission mode outranks the mod's, as it does there.
     */
    const liveSessionThreadSettings = (sessionId: string, parsed: ParsedCliSession) =>
      liveBridgeSession(sessionId).pipe(
        Effect.map((live) =>
          live === null
            ? null
            : threadSettingsForClaudeSession({
                model: live.model,
                permissionMode: parsed.permissionMode ?? live.permissionMode,
              }),
        ),
      );

    /**
     * A thread T3 has never run, on a session a mod process holds: no
     * attachment has said what the session runs on, and the thread would
     * show T3's defaults. The row is given the session's own model and
     * modes; once T3 runs the thread, its attachment keeps them.
     */
    const showLiveSessionSettings = Effect.fn("SessionImporter.showLiveSessionSettings")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly sessionId: string;
        readonly parsed: ParsedCliSession;
      }) {
        const observed = yield* liveSessionThreadSettings(input.sessionId, input.parsed);
        if (observed === null) return;
        let thread = (yield* threads.getThreadRecords(input.threadId, [])).thread;
        const at = yield* Clock.currentTimeMillis;
        const events: Array<OrchestrationV2DomainEvent> = [];
        const write = (
          type:
            | "thread.model-selection-updated"
            | "thread.runtime-mode-updated"
            | "thread.interaction-mode-updated",
        ) =>
          events.push({
            // The thread follows what the session says now: not a fact
            // about a row of the record, so not an id a replay would repeat.
            id: importEventId(thread.id, `session-settings:${at}:${type}`),
            type,
            threadId: thread.id,
            providerInstanceId: thread.providerInstanceId,
            // Seeing a session is not activity in its thread.
            occurredAt: thread.updatedAt,
            payload: thread,
          });
        if (observed.model !== undefined && thread.modelSelection.model !== observed.model) {
          thread = {
            ...thread,
            // The previous model's options belong to that model.
            modelSelection: {
              instanceId: thread.modelSelection.instanceId,
              model: observed.model,
              ...(observed.contextWindow === undefined
                ? {}
                : { options: [{ id: "contextWindow", value: observed.contextWindow }] }),
            },
          };
          write("thread.model-selection-updated");
        }
        if (observed.runtimeMode !== undefined && thread.runtimeMode !== observed.runtimeMode) {
          thread = { ...thread, runtimeMode: observed.runtimeMode };
          write("thread.runtime-mode-updated");
        }
        if (
          observed.interactionMode !== undefined &&
          thread.interactionMode !== observed.interactionMode
        ) {
          thread = { ...thread, interactionMode: observed.interactionMode };
          write("thread.interaction-mode-updated");
        }
        if (events.length === 0) return;
        yield* eventSink.write({ events });
        yield* Effect.logInfo("session.importer.session-settings-shown", {
          sessionId: input.sessionId,
          threadId: input.threadId,
        });
      },
    );

    /** When each run T3 is driving on the thread right now was asked for. */
    const activeRunStartsMs = (threadId: ThreadId) =>
      sql<{ readonly status: string; readonly requested_at: string }>`
        SELECT status, requested_at
        FROM orchestration_v2_projection_runs
        WHERE thread_id = ${threadId}
      `.pipe(
        Effect.map((rows) =>
          rows.flatMap((run) =>
            ACTIVE_RUN_STATUSES.has(run.status)
              ? [DateTime.toEpochMillis(dateTime(run.requested_at))]
              : [],
          ),
        ),
      );

    /** What the Claude Code process holding the session now reports, if one with the T3 mod does. */
    const liveBridgeSession = (sessionId: string) =>
      Effect.gen(function* () {
        if (bridgeDir === undefined) return null;
        const dir = path.join(bridgeDir, "claude", sessionId);
        const instances = yield* fs
          .readDirectory(dir)
          .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
        const now = yield* Clock.currentTimeMillis;
        for (const instanceId of instances) {
          const read = fs
            .readFileString(path.join(dir, instanceId, "session.json"))
            .pipe(Effect.orElseSucceed(() => ""));
          let session = parseClaudeBridgeSessionFile(parseJsonlLine(yield* read) ?? null);
          // The mod rewrites the file in place: a read can land mid-write.
          if (session === null) {
            session = parseClaudeBridgeSessionFile(parseJsonlLine(yield* read) ?? null);
          }
          if (session !== null && isClaudeBridgeSessionLive(session, now)) return session;
        }
        return null;
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

    const continuations = makeClaudeContinuationReader(fs, path);
    const waitedForEarlierSession = new Set<string>();
    /**
     * The thread that holds a Claude session's conversation, and the session
     * it holds it under: the session's own thread, or the thread of an
     * earlier session whose record says the conversation continued here.
     */
    const claudeConversationThread = (sessionId: string, filePath: string) =>
      Effect.gen(function* () {
        const own = yield* lookupOwnedThread(CLAUDE_DRIVER, sessionId);
        if (own !== undefined) return { threadId: own, heldAs: sessionId };
        for (const earlier of yield* continuations.continuedThrough(filePath)) {
          const threadId = yield* lookupOwnedThread(CLAUDE_DRIVER, earlier);
          if (threadId !== undefined) return { threadId, heldAs: earlier };
        }
        return undefined;
      });

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
    const threadRuns = (threadId: ThreadId) =>
      sql<{ readonly run_id: string; readonly ordinal: number; readonly requested_at: string }>`
        SELECT run_id, ordinal, requested_at
        FROM orchestration_v2_projection_runs
        WHERE thread_id = ${threadId}
      `.pipe(
        Effect.map((rows) =>
          rows.map((run): HealingRun => ({
            id: run.run_id,
            ordinal: run.ordinal,
            startedAtMs: DateTime.toEpochMillis(dateTime(run.requested_at)),
          })),
        ),
      );

    /** Gives an imported row its place in the thread before its events are written. */
    const placeImportedRow = (threadId: ThreadId, placement: HealingPlacement) =>
      turnItemPositions.allocate({
        threadId,
        turnItemId: importedTurnItemId(threadId, placement.message.sourceId),
        runId: null,
        runOrdinal: placement.band,
      });

    /**
     * Imported rows used to be put above the thread's first run whenever they
     * were written. Moves each such row after the run that was the latest at
     * its time, so the thread reads in the order things happened.
     */
    const reseatImportedRows = Effect.fn("SessionImporter.reseatImportedRows")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly runs: ReadonlyArray<HealingRun>;
      readonly pass: "bands" | "order";
    }) {
      const { threadId, sessionId } = input;
      if (input.runs.length === 0) return;
      const itemPrefix = importedTurnItemId(threadId, "");
      const imported = (yield* sql<{ readonly turn_item_id: string; readonly ordinal: number }>`
        SELECT turn_item_id, ordinal
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId} AND run_id IS NULL
      `).flatMap((item) =>
        item.turn_item_id.startsWith(itemPrefix)
          ? [{ sourceId: item.turn_item_id.slice(itemPrefix.length), ordinal: item.ordinal }]
          : [],
      );
      // Rows in the wrong band move to theirs; within a band, rows that sit
      // above rows written before them go back in the order they were written.
      const reseats =
        input.pass === "bands"
          ? planImportedRowReseating({ rows: input.parsed.messages, imported, runs: input.runs })
          : planImportedRowOrdering({ rows: input.parsed.messages, imported });
      if (reseats.length === 0) return;
      const events: Array<OrchestrationV2DomainEvent> = [];
      for (const reseat of reseats) {
        yield* sql`
          DELETE FROM orchestration_v2_turn_item_positions
          WHERE thread_id = ${threadId}
            AND turn_item_id = ${importedTurnItemId(threadId, reseat.message.sourceId)}
        `;
        const position = yield* placeImportedRow(threadId, reseat);
        const item = messageEvents({ threadId, sessionId, message: reseat.message })[1]!;
        events.push({
          ...item,
          // A row can be moved more than once: each place it is given is its own event.
          id: importEventId(threadId, `turn-item-reseat:${position}:${reseat.message.sourceId}`),
        });
      }
      yield* eventSink.write({ events });
      yield* Effect.logInfo("session.importer.thread-reordered", {
        provider: input.driver,
        sessionId,
        threadId,
        movedCount: reseats.length,
      });
    });

    const appendMessages = Effect.fn("SessionImporter.appendMessages")(function* (input: {
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly fromLineIndex: number;
      /** Only rows after this instant; the rest are T3's own turns. */
      readonly afterMs?: number;
      /** The thread's runs: each row goes after the one that was the latest when it was written. */
      readonly runs?: ReadonlyArray<HealingRun>;
    }) {
      // A command still running, or just over, is the live attachment's to
      // show as a turn (see claudeCommandRowsInFlight): its rows wait.
      const held = claudeCommandRowsInFlight(input.parsed.messages, yield* Clock.currentTimeMillis);
      const settled =
        held === 0
          ? input.parsed.messages
          : input.parsed.messages.slice(0, input.parsed.messages.length - held);
      const candidates = settled.filter(
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
      if (input.runs !== undefined && input.runs.length > 0) {
        for (const message of fresh) {
          yield* placeImportedRow(input.threadId, {
            message,
            band: bandForTime(input.runs, DateTime.toEpochMillis(dateTime(message.timestamp))),
          });
        }
      }
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
     * A turn T3 picked up with nothing known of how it began used to open
     * with the stock words of a background wake, shown as the person's own
     * message. Nobody typed them. Where the session's record has no such
     * prompt, the row becomes the notice a background wake shows.
     */
    /**
     * Takes back the runs T3 holds for prompts the session took back before
     * answering them, as a rewind of T3's own does: the run and its root
     * node are marked rolled back, which every client leaves out of the
     * thread; nothing is deleted.
     */
    const takeBackWithdrawnRuns = Effect.fn("SessionImporter.takeBackWithdrawnRuns")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly rows: ReadonlyArray<ParsedSessionMessage>;
        readonly known: ReadonlyArray<HealingKnownMessage>;
        readonly fromLine: number;
      }) {
        const { threadId } = input;
        const records = yield* threads.getThreadRecords(threadId, ["runs", "nodes"]);
        const takenBack = new Set(
          planWithdrawnRuns({
            rows: input.rows,
            known: input.known,
            runs: records.runs.map((run) => ({ id: run.id, status: run.status })),
            fromLine: input.fromLine,
          }),
        );
        if (takenBack.size === 0) return;
        const now = yield* DateTime.now;
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const run of records.runs) {
          if (!takenBack.has(run.id)) continue;
          const root = records.nodes.find((node) => node.id === run.rootNodeId);
          events.push({
            id: importEventId(threadId, `withdrawn:${run.id}`),
            type: "run.updated",
            threadId,
            runId: run.id,
            ...(root === undefined ? {} : { nodeId: root.id }),
            providerInstanceId: run.providerInstanceId,
            occurredAt: now,
            payload: { ...run, status: "rolled_back" },
          });
          if (root !== undefined) {
            events.push({
              id: importEventId(threadId, `withdrawn-node:${run.id}`),
              type: "node.updated",
              threadId,
              runId: run.id,
              nodeId: root.id,
              providerInstanceId: run.providerInstanceId,
              occurredAt: now,
              payload: { ...root, status: "rolled_back" },
            });
          }
        }
        yield* eventSink.write({ events });
        yield* Effect.logInfo("session.importer.withdrawn-prompts-taken-back", {
          threadId,
          runIds: [...takenBack],
        });
      },
    );

    const retireInventedPrompts = Effect.fn("SessionImporter.retireInventedPrompts")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly rows: ReadonlyArray<ParsedSessionMessage>;
      }) {
        const { threadId } = input;
        const suspects = yield* sql<{ readonly turn_item_id: string }>`
        SELECT turn_item_id
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
          AND type = 'user_message'
          AND json_extract(payload_json, '$.createdBy') = 'user'
          AND json_extract(payload_json, '$.creationSource') = 'provider'
          AND json_extract(payload_json, '$.text') = ${CONTINUATION_MESSAGE_TEXT}
      `;
        if (suspects.length === 0) return;
        // The person can type those very words: then the record has them.
        if (
          input.rows.some((row) => row.role === "user" && row.text === CONTINUATION_MESSAGE_TEXT)
        ) {
          return;
        }
        const ids = new Set(suspects.map((row) => row.turn_item_id));
        const records = yield* threads.getThreadRecords(threadId, ["turnItems"]);
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const item of records.turnItems) {
          if (item.type !== "user_message" || !ids.has(item.id)) continue;
          events.push({
            id: importEventId(threadId, `invented-prompt-retired:${item.id}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: item.updatedAt,
            payload: {
              id: item.id,
              threadId: item.threadId,
              runId: item.runId,
              nodeId: item.nodeId,
              providerThreadId: item.providerThreadId,
              providerTurnId: item.providerTurnId,
              nativeItemRef: item.nativeItemRef,
              parentItemId: item.parentItemId,
              ordinal: item.ordinal,
              status: item.status,
              title: item.title,
              startedAt: item.startedAt,
              completedAt: item.completedAt,
              updatedAt: item.updatedAt,
              type: "notification",
              source: { kind: "background_task" },
              outcome: "updated",
              summary: "Background activity updated",
            },
          });
        }
        if (events.length === 0) return;
        yield* eventSink.write({ events });
        yield* Effect.logInfo("session.importer.invented-prompts-retired", {
          threadId,
          count: events.length,
        });
      },
    );

    /**
     * A background task reporting back used to be filed as the person's own
     * message, markup and all. Nobody typed it: each such row of a thread
     * becomes the notice it is, worded from the row itself. Run where the
     * session's record holds such reports; a thread with none left is one
     * short search of its own rows.
     */
    const retireTaskNoticePrompts = Effect.fn("SessionImporter.retireTaskNoticePrompts")(
      function* (input: {
        readonly threadId: ThreadId;
        readonly rows: ReadonlyArray<ParsedSessionMessage>;
      }) {
        const { threadId } = input;
        // The text sits at the head of the row's stored form: told without
        // reading every message of the thread in full.
        const suspects = yield* sql<{ readonly turn_item_id: string }>`
          SELECT turn_item_id
          FROM orchestration_v2_projection_turn_items
          WHERE thread_id = ${threadId}
            AND type = 'user_message'
            AND substr(payload_json, instr(payload_json, '"text":"') + 8, 19) = '<task-notification>'
        `;
        if (suspects.length === 0) return;
        // The record says who wrote a row: words the person did type stay theirs.
        const typed = new Set(
          input.rows.flatMap((row) =>
            row.role === "user" && row.notice === undefined && isClaudeTaskNoticeText(row.text)
              ? [row.text.trim()]
              : [],
          ),
        );
        const ids = new Set(suspects.map((row) => row.turn_item_id));
        const records = yield* threads.getThreadRecords(threadId, ["turnItems"]);
        const events: Array<OrchestrationV2DomainEvent> = [];
        for (const item of records.turnItems) {
          if (item.type !== "user_message" || !ids.has(item.id)) continue;
          if (item.createdBy !== "user" || !isClaudeTaskNoticeText(item.text)) continue;
          if (typed.has(item.text.trim())) continue;
          events.push({
            id: importEventId(threadId, `task-notice-retired:${item.id}`),
            type: "turn-item.updated",
            threadId,
            occurredAt: item.updatedAt,
            payload: {
              id: item.id,
              threadId: item.threadId,
              runId: item.runId,
              nodeId: item.nodeId,
              providerThreadId: item.providerThreadId,
              providerTurnId: item.providerTurnId,
              nativeItemRef: item.nativeItemRef,
              parentItemId: item.parentItemId,
              ordinal: item.ordinal,
              status: item.status,
              title: item.title,
              startedAt: item.startedAt,
              completedAt: item.completedAt,
              updatedAt: item.updatedAt,
              type: "notification",
              ...taskNotification(claudeTaskNotice(item.text)),
            },
          });
        }
        if (events.length === 0) return;
        yield* eventSink.write({ events });
        yield* Effect.logInfo("session.importer.task-notices-retired", {
          threadId,
          count: events.length,
        });
      },
    );

    /**
     * A reply can reach a thread twice: once from what the terminal drew,
     * under that message's own id, and once as the record's row for it (the
     * record can be seconds behind, and a restarted T3 reads a running turn
     * again from its start). The record's row is the reply. T3 cannot take a
     * row out of a thread, so the drawn copy is emptied (a reply with nothing
     * in it draws nothing), and where the record's row landed far from it
     * (re-read into a later run) that row takes the drawn copy's place.
     *
     * The record's row can itself be there twice: imported as history, then
     * delivered by a run that showed the turn from its start. The run's copy
     * sits among the turn's tool calls and is the one an import would have
     * left alone, so the imported copy is emptied.
     */
    const blankDoubledReplies = Effect.fn("SessionImporter.blankDoubledReplies")(function* (input: {
      readonly threadId: ThreadId;
      readonly rows: ReadonlyArray<ParsedSessionMessage>;
    }) {
      const { threadId } = input;
      const replies = yield* sql<{
        readonly turn_item_id: string;
        readonly run_id: string | null;
        readonly ordinal: number;
        readonly text: string | null;
        readonly started_at: string;
      }>`
        SELECT
          turn_item_id,
          run_id,
          ordinal,
          json_extract(payload_json, '$.text') AS text,
          coalesce(json_extract(payload_json, '$.startedAt'), updated_at) AS started_at
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
          AND type = 'assistant_message'
        ORDER BY ordinal
      `;
      if (replies.length < 2) return;
      const normalize = (text: string | null) => (text ?? "").replace(/\s+/gu, " ").trim();
      const writtenAtById = new Map(
        input.rows.flatMap((row) =>
          row.role === "assistant"
            ? [[row.sourceId, DateTime.toEpochMillis(dateTime(row.timestamp))] as const]
            : [],
        ),
      );
      // T3's own copy of a record row carries the row's id after the last colon.
      const recordRowOf = (id: string) => {
        let plain = id;
        for (let pass = 0; pass < 3 && plain.includes("%"); pass += 1) {
          try {
            plain = decodeURIComponent(plain);
          } catch {
            break;
          }
        }
        const rowId = plain.slice(plain.lastIndexOf(":") + 1);
        return writtenAtById.has(rowId) ? rowId : null;
      };
      // Each drawn copy pairs with the record's row of the same text written
      // closest to when it was drawn; each row stands for one copy.
      const pairs: Array<{ drawn: number; recorded: number; apartMs: number }> = [];
      replies.forEach((reply, drawn) => {
        const text = normalize(reply.text);
        if (text.length === 0 || recordRowOf(reply.turn_item_id) !== null) return;
        const drawnAtMs = DateTime.toEpochMillis(dateTime(reply.started_at));
        replies.forEach((other, recorded) => {
          const rowId = recordRowOf(other.turn_item_id);
          if (rowId === null || normalize(other.text) !== text) return;
          const apartMs = Math.abs((writtenAtById.get(rowId) ?? 0) - drawnAtMs);
          if (apartMs <= DOUBLED_REPLY_WINDOW_MS) pairs.push({ drawn, recorded, apartMs });
        });
      });
      const emptied = new Set<string>();
      const twins = new Set<string>();
      const importedPrefix = importedTurnItemId(threadId, "");
      const deliveredTexts = new Map<string, Set<string>>();
      for (const reply of replies) {
        const rowId = recordRowOf(reply.turn_item_id);
        if (rowId === null || reply.turn_item_id.startsWith(importedPrefix)) continue;
        deliveredTexts.set(
          rowId,
          (deliveredTexts.get(rowId) ?? new Set()).add(normalize(reply.text)),
        );
      }
      for (const reply of replies) {
        const text = normalize(reply.text);
        if (text.length === 0 || !reply.turn_item_id.startsWith(importedPrefix)) continue;
        const rowId = recordRowOf(reply.turn_item_id);
        if (rowId !== null && deliveredTexts.get(rowId)?.has(text) === true) {
          emptied.add(reply.turn_item_id);
          twins.add(reply.turn_item_id);
        }
      }
      if (pairs.length === 0 && emptied.size === 0) return;
      const placeOf = new Map<string, number>();
      const taken = new Set<number>();
      let movedCount = 0;
      for (const pair of pairs.toSorted((left, right) => left.apartMs - right.apartMs)) {
        const drawn = replies[pair.drawn]!;
        const recorded = replies[pair.recorded]!;
        if (emptied.has(drawn.turn_item_id) || taken.has(pair.recorded)) continue;
        taken.add(pair.recorded);
        emptied.add(drawn.turn_item_id);
        if (Math.abs(recorded.ordinal - drawn.ordinal) <= DOUBLED_REPLY_REACH) continue;
        // A row's place is kept apart from the row, one row to a place. The
        // emptied copy gives its place up for a new one at the end of the run
        // the record's row was read into, and that row takes the place it
        // left. In that order: the events below are what a rebuild of the
        // thread replays, and two rows never claim one place in it.
        yield* sql`
          DELETE FROM orchestration_v2_turn_item_positions
          WHERE thread_id = ${threadId} AND turn_item_id = ${drawn.turn_item_id}
        `;
        const parked = yield* turnItemPositions.allocate({
          threadId,
          turnItemId: TurnItemId.make(drawn.turn_item_id),
          runId: recorded.run_id === null ? null : RunId.make(recorded.run_id),
        });
        yield* sql`
          UPDATE orchestration_v2_turn_item_positions
          SET ordinal = ${drawn.ordinal}
          WHERE thread_id = ${threadId} AND turn_item_id = ${recorded.turn_item_id}
        `;
        placeOf.set(drawn.turn_item_id, parked);
        placeOf.set(recorded.turn_item_id, drawn.ordinal);
        movedCount += 1;
      }
      const records = yield* threads.getThreadRecords(threadId, ["turnItems"]);
      const vacated: Array<OrchestrationV2DomainEvent> = [];
      const placed: Array<OrchestrationV2DomainEvent> = [];
      for (const item of records.turnItems) {
        if (item.type !== "assistant_message") continue;
        const place = placeOf.get(item.id);
        if (!emptied.has(item.id) && place === undefined) continue;
        (emptied.has(item.id) ? vacated : placed).push({
          // A row can be given a place more than once: each is its own event.
          id: importEventId(
            threadId,
            `doubled-reply:${emptied.has(item.id) ? "emptied" : "kept"}:${place ?? "in-place"}:${item.id}`,
          ),
          type: "turn-item.updated",
          threadId,
          // An event's time becomes the thread's: emptying old history is
          // not the thread being used, so it keeps the time it has.
          occurredAt: twins.has(item.id) ? records.thread.updatedAt : item.updatedAt,
          payload: {
            ...item,
            ...(emptied.has(item.id) ? { text: "" } : {}),
            ...(place === undefined ? {} : { ordinal: place }),
          },
        });
      }
      // The emptied copies first: each frees the place a record row takes.
      const events = [...vacated, ...placed];
      if (events.length === 0) return;
      yield* eventSink.write({ events });
      yield* Effect.logInfo("session.importer.doubled-replies-blanked", {
        threadId,
        emptied: emptied.size,
        moved: movedCount,
      });
    });

    /**
     * Puts a thread's messages in the order of the record's lines (see
     * `planRecordSeating`): all of them for a thread kept in that order,
     * and for any other thread only a message T3 sent into a running turn
     * that the turn took later than it was sent.
     *
     * A row's place is kept apart from the row, one row to a place: the
     * places are rewritten together, then every row whose shown place is not
     * its place is written again, which is also what finishes a pass that
     * was cut short. Rows of a run still going are left for the pass after
     * it ends.
     */
    const seatRowsByRecord = Effect.fn("SessionImporter.seatRowsByRecord")(function* (input: {
      readonly threadId: ThreadId;
      readonly filePath: string;
      readonly parsed: ParsedCliSession;
      readonly runs: ReadonlyArray<HealingRun>;
      readonly only?: "sent";
    }) {
      const { threadId } = input;
      const sentRows = yield* sql<{
        readonly turn_item_id: string;
        readonly text: string | null;
        readonly sent_at: string;
      }>`
        SELECT
          turn_item_id,
          json_extract(payload_json, '$.text') AS text,
          coalesce(json_extract(payload_json, '$.startedAt'), updated_at) AS sent_at
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
          AND type = 'user_message'
          AND json_extract(payload_json, '$.inputIntent') IN ('steer', 'promoted_queued_to_steer')
      `;
      if (input.only === "sent" && sentRows.length === 0) return;
      const active = (yield* sql<{ readonly ordinal: number; readonly status: string }>`
        SELECT ordinal, status
        FROM orchestration_v2_projection_runs
        WHERE thread_id = ${threadId}
      `).flatMap((run) => (ACTIVE_RUN_STATUSES.has(run.status) ? [run.ordinal] : []));
      const frozenBand = active.length === 0 ? undefined : Math.min(...active);
      const below = frozenBand === undefined ? Number.MAX_SAFE_INTEGER : frozenBand * RUN_BAND;
      const positioned = yield* sql<{
        readonly turn_item_id: string;
        readonly ordinal: number;
        readonly shown: number | null;
        readonly type: string | null;
      }>`
        SELECT place.turn_item_id, place.ordinal, item.ordinal AS shown, item.type
        FROM orchestration_v2_turn_item_positions AS place
        LEFT JOIN orchestration_v2_projection_turn_items AS item
          ON item.thread_id = place.thread_id AND item.turn_item_id = place.turn_item_id
        WHERE place.thread_id = ${threadId} AND place.ordinal < ${below}
        ORDER BY place.ordinal
      `;
      const written = new Map<string, { readonly line: number; readonly atMs: number }>();
      for (const message of input.parsed.messages) {
        if (written.has(message.sourceId)) continue;
        written.set(message.sourceId, {
          line: message.lineIndex,
          atMs: DateTime.toEpochMillis(dateTime(message.timestamp)),
        });
      }
      const keys = new Map(
        positioned.map((row) => [row.turn_item_id, recordKeyOfTurnItem(row.turn_item_id)]),
      );
      const sent = sentPromptLines({
        // A message T3 sent carries T3's id, not a row of the record.
        sent: sentRows.flatMap((row) =>
          keys.get(row.turn_item_id) === null
            ? [
                {
                  turnItemId: row.turn_item_id,
                  text: row.text ?? "",
                  sentAtMs: DateTime.toEpochMillis(dateTime(row.sent_at)),
                },
              ]
            : [],
        ),
        rows: input.parsed.messages,
        shownIds: new Set([...keys.values()].flatMap((key) => (key === null ? [] : [key]))),
      });
      // Nothing to look at again until a row is added, a run ends, or the
      // record writes a sent message.
      const settled = positioned.every((row) => row.shown === null || row.shown === row.ordinal);
      const state = `${positioned.length}:${frozenBand ?? "none"}:${sent.size}`;
      if (settled && seatedThreads.get(threadId) === state) return;
      // Tool calls and reasoning place a message exactly; a sent message
      // only has to clear the replies written before it.
      const lines =
        input.only === "sent"
          ? new Map<string, number>()
          : claudeRecordLines(yield* fs.readFileString(input.filePath));
      const importedPrefix = importedTurnItemId(threadId, "");
      const rows = positioned.map((row) => {
        const key = keys.get(row.turn_item_id) ?? null;
        const message = key === null ? sent.get(row.turn_item_id) : written.get(key);
        return {
          turnItemId: row.turn_item_id,
          ordinal: row.ordinal,
          line: message?.line ?? (key === null ? null : (lines.get(key) ?? null)),
          kind: row.turn_item_id.startsWith(importedPrefix)
            ? ("imported" as const)
            : sent.has(row.turn_item_id)
              ? ("sent" as const)
              : row.type === "user_message" || row.type === "assistant_message"
                ? ("message" as const)
                : ("other" as const),
          atMs: message?.atMs ?? 0,
        };
      });
      const moves = planRecordSeating({
        rows,
        runs: input.runs,
        ...(frozenBand === undefined ? {} : { frozenBand }),
        ...(input.only === undefined ? {} : { only: input.only }),
      });
      if (moves.length > 0) {
        // Every moved row steps aside before any takes its place.
        yield* sql.withTransaction(
          Effect.gen(function* () {
            for (const [index, move] of moves.entries()) {
              yield* sql`
                UPDATE orchestration_v2_turn_item_positions
                SET ordinal = ${-(index + 1)}
                WHERE thread_id = ${threadId} AND turn_item_id = ${move.turnItemId}
              `;
            }
            for (const move of moves) {
              yield* sql`
                UPDATE orchestration_v2_turn_item_positions
                SET ordinal = ${move.ordinal}
                WHERE thread_id = ${threadId} AND turn_item_id = ${move.turnItemId}
              `;
            }
          }),
        );
      }
      // Outside a thread kept in the record's order, only the runs a sent
      // message sits in are this pass's to write.
      const bands =
        input.only === "sent"
          ? new Set(
              rows.flatMap((row) =>
                row.kind === "sent" ? [Math.floor(row.ordinal / RUN_BAND)] : [],
              ),
            )
          : undefined;
      const placeOf = new Map(
        (yield* sql<{ readonly turn_item_id: string; readonly ordinal: number }>`
          SELECT item.turn_item_id, place.ordinal
          FROM orchestration_v2_projection_turn_items AS item
          JOIN orchestration_v2_turn_item_positions AS place
            ON place.thread_id = item.thread_id AND place.turn_item_id = item.turn_item_id
          WHERE item.thread_id = ${threadId}
            AND item.ordinal <> place.ordinal
            AND place.ordinal < ${below}
        `).flatMap((row) =>
          bands === undefined || bands.has(Math.floor(row.ordinal / RUN_BAND))
            ? [[row.turn_item_id, row.ordinal] as const]
            : [],
        ),
      );
      if (placeOf.size > 0) {
        const records = yield* threads.getThreadRecords(threadId, ["turnItems"]);
        // A row can come back to a place it held before, and an event's id is
        // written once: the thread's own count of events tells the passes apart.
        const pass = yield* eventSink.latestSequence({ threadId });
        const events = records.turnItems.flatMap((item): Array<OrchestrationV2DomainEvent> => {
          const place = placeOf.get(item.id);
          return place === undefined
            ? []
            : [
                {
                  id: importEventId(threadId, `record-seat:${pass}:${place}:${item.id}`),
                  type: "turn-item.updated",
                  threadId,
                  // An event's time becomes the thread's: a row changing
                  // place is not the thread being used.
                  occurredAt: records.thread.updatedAt,
                  payload: { ...item, ordinal: place },
                },
              ];
        });
        for (let start = 0; start < events.length; start += SEATING_WRITE_SIZE) {
          yield* eventSink.write({ events: events.slice(start, start + SEATING_WRITE_SIZE) });
        }
        yield* Effect.logInfo("session.importer.rows-seated-by-record", {
          threadId,
          placesChanged: moves.length,
          rowsWritten: events.length,
        });
      }
      if (moves.length === 0) seatedThreads.set(threadId, state);
    });

    /**
     * A thread that shows a session through a live attachment holds what the
     * session's transcript holds: rows the attachment never delivered are
     * added, each after the run that was the latest when it was written. See
     * `transcriptHealing`.
     *
     * The comparison is by row id, so it needs a thread whose replies carry
     * the transcript's ids. A session that is running is always compared; one
     * that has ended only when the thread holds such a reply (`false` is
     * returned otherwise, and the caller catches up by time as before).
     */
    const healBridgedThread = Effect.fn("SessionImporter.healBridgedThread")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly filePath: string;
      readonly parsed: ParsedCliSession;
      readonly threadId: ThreadId;
      readonly runs: ReadonlyArray<HealingRun>;
      readonly context: Pick<SweepContext, "stats">;
      readonly live: boolean;
      /**
       * Set while a turn is running: its rows are still arriving through the
       * attachment, so only rows written before this moment are compared,
       * and what the person typed into the turn, which the attachment never
       * carries.
       */
      readonly settledBeforeMs?: number;
      /** The thread holds this conversation under an earlier session id. */
      readonly carriedOn?: boolean;
    }) {
      const { threadId, sessionId, parsed } = input;
      yield* threads.ensureLegacyTranscript(threadId);
      // What the thread shows: its timeline rows. A message the timeline has
      // no row for is not shown, whatever else T3 holds about it.
      const known = yield* sql<{
        readonly message_id: string | null;
        readonly role: string;
        readonly created_at: string;
        readonly run_id: string | null;
        readonly text: string | null;
      }>`
        SELECT
          json_extract(payload_json, '$.messageId') AS message_id,
          CASE type WHEN 'user_message' THEN 'user' ELSE 'assistant' END AS role,
          coalesce(json_extract(payload_json, '$.startedAt'), updated_at) AS created_at,
          run_id,
          json_extract(payload_json, '$.text') AS text
        FROM orchestration_v2_projection_turn_items
        WHERE thread_id = ${threadId}
          AND type IN ('user_message', 'assistant_message')
      `.pipe(
        Effect.map((rows) =>
          rows.flatMap((row) =>
            row.message_id === null ? [] : [{ ...row, message_id: row.message_id }],
          ),
        ),
      );
      const settledBeforeMs = input.settledBeforeMs;
      // A command a mod-held session is still running, or has just run, is
      // the live attachment's to show: taken in now, its line would be there
      // again when the attachment shows the command as a turn.
      // (Whether a mod holds the session is read from a file the mod
      // rewrites; a read that lands mid-write says "not live". The record
      // itself says a command is running, so the hold does not hang on that.)
      const commandInFlight = claudeCommandRowsInFlight(
        parsed.messages,
        yield* Clock.currentTimeMillis,
      );
      const settled = parsed.messages.slice(0, parsed.messages.length - commandInFlight);
      const rows =
        settledBeforeMs === undefined
          ? settled
          : settled.filter(
              (message) =>
                message.midTurn === true ||
                DateTime.toEpochMillis(dateTime(message.timestamp)) < settledBeforeMs,
            );
      if (!input.live) {
        const rowIds = new Set(parsed.messages.map((message) => message.sourceId));
        const holdsTranscriptReply = known.some((message) => {
          const colon = message.message_id.lastIndexOf(":");
          return (
            message.role === "assistant" &&
            colon !== -1 &&
            rowIds.has(message.message_id.slice(colon + 1))
          );
        });
        if (!holdsTranscriptReply) return false;
      }
      const knownMessages: ReadonlyArray<HealingKnownMessage> = known.flatMap((message) =>
        message.role === "user" || message.role === "assistant"
          ? [
              {
                messageId: message.message_id,
                role: message.role,
                text: message.text ?? "",
                atMs: DateTime.toEpochMillis(dateTime(message.created_at)),
                runId: message.run_id,
              },
            ]
          : [],
      );
      const missing = planTranscriptHealing({
        rows,
        known: knownMessages,
        runs: input.runs,
        importedMessageId: (sourceId) => importedMessageId(sessionId, sourceId),
      });
      // The harness's notices between turns are shown from where the record
      // ended when this thread was first compared with it, never before.
      const noticeFloors = readNoticeFloors(
        Option.getOrUndefined(yield* runtimes.getByThreadId({ threadId }))?.runtimePayload,
      );
      const noticesFrom = noticeFloors[sessionId];
      const noticesFromLine = noticesFrom ?? Number.POSITIVE_INFINITY;
      const systemNotices = parsed.systemNotices ?? [];
      // One a run showed carries the row's uuid; one added here, the imported id.
      const shownNotices = !systemNotices.some((notice) => notice.lineIndex >= noticesFromLine)
        ? []
        : yield* sql<{ readonly turn_item_id: string; readonly native_id: string | null }>`
            SELECT
              turn_item_id,
              json_extract(payload_json, '$.nativeItemRef.nativeId') AS native_id
            FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${threadId} AND type = 'system_notice'
          `;
      const notices = planNoticeHealing({
        notices: systemNotices,
        fromLine: noticesFromLine,
        shownIds: new Set(
          shownNotices.flatMap((row) =>
            [row.native_id, recordKeyOfTurnItem(row.turn_item_id)].filter(
              (id): id is string => id !== null,
            ),
          ),
        ),
        runs: input.runs,
        ...(settledBeforeMs === undefined ? {} : { settledBeforeMs }),
      });
      // A background task's notice written while no turn T3 followed was
      // open: the thread shows it as a live run would, from the same floor.
      const shownTaskNotices = !rows.some(
        (row) => row.notice !== undefined && row.lineIndex >= noticesFromLine,
      )
        ? []
        : yield* sql<{
            readonly turn_item_id: string;
            readonly summary: string | null;
            readonly started_at: string;
          }>`
            SELECT
              turn_item_id,
              json_extract(payload_json, '$.summary') AS summary,
              coalesce(json_extract(payload_json, '$.startedAt'), updated_at) AS started_at
            FROM orchestration_v2_projection_turn_items
            WHERE thread_id = ${threadId} AND type = 'notification'
          `;
      const importedPrefix = importedTurnItemId(threadId, "");
      const taskNotices = planTaskNoticeHealing({
        rows,
        known: knownMessages,
        shown: shownTaskNotices.map((item) => ({
          sourceId: item.turn_item_id.startsWith(importedPrefix)
            ? item.turn_item_id.slice(importedPrefix.length)
            : null,
          summary: item.summary ?? "",
          atMs: DateTime.toEpochMillis(dateTime(item.started_at)),
        })),
        runs: input.runs,
        fromLine: noticesFromLine,
      });
      if (missing.length > 0 || notices.length > 0 || taskNotices.length > 0) {
        // Rows and notices take their places in the order the record wrote them.
        const places = [
          ...[...missing, ...taskNotices].map((placement) => ({
            line: placement.message.lineIndex,
            place: placeImportedRow(threadId, placement),
          })),
          ...notices.map((placement) => ({
            line: placement.notice.lineIndex,
            place: turnItemPositions.allocate({
              threadId,
              turnItemId: importedTurnItemId(threadId, placement.notice.sourceId),
              runId: null,
              runOrdinal: placement.band,
            }),
          })),
        ].toSorted((left, right) => left.line - right.line);
        for (const { place } of places) yield* place;
        const records = yield* threads.getThreadRecords(threadId, []);
        const thread = records.thread;
        const events: Array<OrchestrationV2DomainEvent> = [
          ...missing.flatMap((placement) =>
            messageEvents({ threadId, sessionId, message: placement.message }),
          ),
          ...notices.map((placement) => noticeEvent(threadId, placement.notice)),
          ...taskNotices.flatMap((placement) =>
            messageEvents({ threadId, sessionId, message: placement.message }),
          ),
        ];
        const newest =
          missing.length === 0 ? null : dateTime(missing[missing.length - 1]!.message.timestamp);
        if (
          newest !== null &&
          DateTime.toEpochMillis(newest) > DateTime.toEpochMillis(thread.updatedAt)
        ) {
          events.push({
            id: importEventId(threadId, `recency:${missing[missing.length - 1]!.message.sourceId}`),
            type: "thread.metadata-updated",
            threadId,
            providerInstanceId: thread.providerInstanceId,
            occurredAt: newest,
            payload: { ...thread, updatedAt: newest },
          });
        }
        yield* eventSink.write({ events });
        input.context.stats.updatedCount += 1;
        yield* Effect.logInfo("session.importer.bridged-thread-healed", {
          provider: input.driver,
          sessionId,
          threadId,
          addedCount: missing.length,
          noticeCount: notices.length + taskNotices.length,
          live: input.live,
        });
      }
      // A prompt the session took back (Esc put it back in the input box) is
      // not in its conversation: the run T3 started for it is taken back as
      // T3's own rewind takes a run back, its rows kept. From the same floor.
      if (
        parsed.messages.some(
          (message) => message.withdrawn === true && message.lineIndex >= noticesFromLine,
        )
      ) {
        yield* takeBackWithdrawnRuns({
          threadId,
          rows: parsed.messages,
          known: knownMessages,
          fromLine: noticesFromLine,
        });
      }
      yield* retireInventedPrompts({ threadId, rows: parsed.messages });
      if (parsed.messages.some((message) => message.notice !== undefined)) {
        yield* retireTaskNoticePrompts({ threadId, rows: parsed.messages });
      }
      yield* blankDoubledReplies({ threadId, rows: parsed.messages });
      // The cursor says how far the file has been taken in; with a turn or a
      // command in flight its rows have not been, and the pass after it
      // still needs them.
      if (settledBeforeMs === undefined && commandInFlight === 0 && input.carriedOn !== true) {
        yield* upsertImportBinding({
          driver: input.driver,
          sessionId,
          threadId,
          parsed,
          filePath: input.filePath,
        });
      }
      // Where the record ends at this thread's first settled comparison is
      // where its notices start: none the record held before are added.
      if (noticesFrom === undefined && settledBeforeMs === undefined && commandInFlight === 0) {
        yield* mergeBinding({
          threadId,
          provider: input.driver,
          runtimePayload: { importNoticesFrom: { ...noticeFloors, [sessionId]: parsed.lineCount } },
        });
      }
      // Held rows are taken in by a later pass; a command that prints
      // nothing more leaves the file as it is, so the sweep looks again.
      if (commandInFlight > 0) fileFingerprints.delete(input.filePath);
      return true;
    });

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
      readonly context: Pick<SweepContext, "stats">;
      /**
       * When a turn began that has ended with nothing attached, and that a
       * run offered for it is still to show from its start. Its rows are that
       * run's, as a running turn's are the attachment's.
       */
      readonly turnAwaitingRunSinceMs?: number;
      /**
       * The thread holds this conversation under an earlier session id (the
       * session carried on from it): its binding keeps naming that one.
       */
      readonly carriedOn?: boolean;
    }) {
      const live = yield* liveBridgeSession(input.sessionId);
      const activeRuns = yield* activeRunStartsMs(input.threadId);
      // No mod process holds the session: a run of T3's own is writing the
      // thread, and what it writes is T3's to show.
      if (live === null && activeRuns.length > 0) return;
      // A session that is always working is never idle: everything from
      // before the turn in flight is compared now, not when it stops.
      // A turn just over in a session a mod process holds is its run's to
      // show, however far behind the record that run is.
      const freshTurnSinceMs =
        live === null
          ? undefined
          : claudeFreshTurnStartMs(input.parsed.messages, yield* Clock.currentTimeMillis);
      const turnInFlight = [
        ...activeRuns,
        ...(live !== null && live.activeTurn !== null ? [live.activeTurn.startedAt] : []),
        ...(input.turnAwaitingRunSinceMs === undefined ? [] : [input.turnAwaitingRunSinceMs]),
        ...(freshTurnSinceMs === undefined ? [] : [freshTurnSinceMs]),
      ];
      // Nothing may write the record again: the sweep looks once more.
      if (freshTurnSinceMs !== undefined) fileFingerprints.delete(input.filePath);
      const settledBeforeMs =
        turnInFlight.length === 0 ? undefined : Math.min(...turnInFlight) - IN_FLIGHT_MARGIN_MS;
      const runs = yield* threadRuns(input.threadId);
      const reseat = {
        driver: input.driver,
        sessionId: input.sessionId,
        threadId: input.threadId,
        parsed: input.parsed,
        runs,
      };
      // A thread kept in the record's line order has its rows seated after the
      // comparison; placing them by time first would only move them away.
      const byRecord = recordOrderThreads.has(input.threadId);
      if (!byRecord) yield* reseatImportedRows({ ...reseat, pass: "bands" });
      const compared = yield* healBridgedThread({
        ...input,
        runs,
        live: live !== null,
        ...(settledBeforeMs === undefined ? {} : { settledBeforeMs }),
      });
      // What was just added went to the end of its band.
      if (!byRecord) yield* reseatImportedRows({ ...reseat, pass: "order" });
      yield* seatRowsByRecord({
        threadId: input.threadId,
        filePath: input.filePath,
        parsed: input.parsed,
        runs,
        ...(byRecord ? {} : { only: "sent" as const }),
      });
      if (compared) return;
      // Catching up by time takes whatever T3's record does not reach yet,
      // which a turn in flight is still adding to.
      if (settledBeforeMs !== undefined) return;
      if (input.parsed.lineCount <= input.fromLineIndex) return;
      const afterMs = yield* ownedWatermarkMs(input.threadId);
      const appendedCount = yield* appendMessages({
        sessionId: input.sessionId,
        threadId: input.threadId,
        parsed: input.parsed,
        fromLineIndex: input.fromLineIndex,
        runs,
        ...(afterMs === undefined ? {} : { afterMs }),
      });
      if (claudeCommandRowsInFlight(input.parsed.messages, yield* Clock.currentTimeMillis) > 0) {
        fileFingerprints.delete(input.filePath);
      } else if (input.carriedOn !== true) {
        yield* upsertImportBinding({
          driver: input.driver,
          sessionId: input.sessionId,
          threadId: input.threadId,
          parsed: input.parsed,
          filePath: input.filePath,
        });
      }
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
      /**
       * Set for a session that is running now: it gets its thread even with no
       * history yet, dated from this instant.
       */
      readonly liveAt?: string;
    }) {
      const { driver, sessionId, parsed } = input;
      const idVariant = input.placement?.idVariant ?? "";
      const liveMessage: ParsedSessionMessage | undefined =
        input.liveAt === undefined
          ? undefined
          : { role: "user", sourceId: "live", text: "", timestamp: input.liveAt, lineIndex: 0 };
      const firstMessage = parsed.messages[0] ?? liveMessage;
      const lastMessage = parsed.messages[parsed.messages.length - 1] ?? liveMessage;
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
      // A session a mod process holds says what it runs on: its thread shows
      // that from the start, not T3's defaults.
      const observed =
        driver === CLAUDE_DRIVER ? yield* liveSessionThreadSettings(sessionId, parsed) : null;
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
          model: observed?.model ?? DEFAULT_MODEL_BY_PROVIDER[driver] ?? DEFAULT_MODEL,
          ...(observed?.contextWindow === undefined
            ? {}
            : { options: [{ id: "contextWindow", value: observed.contextWindow }] }),
        },
        runtimeMode: observed?.runtimeMode ?? "full-access",
        interactionMode: observed?.interactionMode ?? DEFAULT_PROVIDER_INTERACTION_MODE,
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
        if (driver === CLAUDE_DRIVER) {
          yield* showLiveSessionSettings({ threadId, sessionId, parsed });
        }
        if (parsed.lineCount <= cursor.lineCount) {
          return;
        }
        // The first turn a mod-held session runs on a thread T3 only ever
        // imported is its run's to show: the rows wait for the next sweep.
        if (
          driver === CLAUDE_DRIVER &&
          claudeFreshTurnStartMs(parsed.messages, yield* Clock.currentTimeMillis) !== undefined &&
          (yield* liveBridgeSession(sessionId)) !== null
        ) {
          fileFingerprints.delete(input.filePath);
          return;
        }

        const appendedCount = yield* appendMessages({
          sessionId,
          threadId,
          parsed,
          fromLineIndex: cursor.lineCount,
        });
        // The cursor says how far the file has been taken in: not past a
        // command whose rows are being held for the live attachment.
        if (claudeCommandRowsInFlight(parsed.messages, yield* Clock.currentTimeMillis) === 0) {
          yield* upsertImportBinding({
            driver,
            sessionId,
            threadId,
            parsed,
            filePath: input.filePath,
          });
        } else {
          fileFingerprints.delete(input.filePath);
        }
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

    /** A file of a sweep that changed since the last one: read whole and imported. */
    const importChangedFile = Effect.fn("SessionImporter.processFile")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly filePath: string;
      readonly context: SweepContext;
      readonly size: number;
    }) {
      const { driver, filePath, context, size } = input;
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
        // A background task's report an earlier build filed as the person's
        // own message becomes the notice it is.
        if (
          driver === CLAUDE_DRIVER &&
          parsed.messages.some((message) => message.notice !== undefined)
        ) {
          yield* retireTaskNoticePrompts({ threadId: binding.threadId, rows: parsed.messages });
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
      // A conversation that carried on under this id from a session with a
      // thread stays that thread's: its rows are compared there, and it never
      // becomes a second thread.
      if (driver === CLAUDE_DRIVER) {
        const held = yield* claudeConversationThread(sessionId, filePath);
        if (held !== undefined && held.heldAs !== sessionId) {
          yield* catchUpOwnedThread({
            driver,
            sessionId,
            filePath,
            parsed,
            threadId: held.threadId,
            fromLineIndex: 0,
            context,
            carriedOn: true,
          });
          return;
        }
        // The session it carried on from has no thread yet: that record gets
        // its turn first, in whatever order the sweep reads them. One sweep
        // is waited for it; a session that never becomes a thread is not
        // waited for again.
        if (
          held === undefined &&
          !waitedForEarlierSession.has(filePath) &&
          (yield* continuations.continuedFrom(filePath)) !== null
        ) {
          waitedForEarlierSession.add(filePath);
          fileFingerprints.delete(filePath);
          return;
        }
      }
      if (isAutomationSession(parsed)) {
        context.deferredMachine.push({ kind: "fresh", driver, sessionId, filePath, parsed });
        return;
      }
      yield* importFreshSession({ driver, sessionId, filePath, parsed, context });
    });

    /**
     * One file of a sweep. Nearly every file is as the last sweep left it, so
     * that check runs outside a span: a span for each of the thousands of
     * session files, every sweep, was most of what the trace file held.
     */
    const processFile = Effect.fnUntraced(function* (input: {
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
      yield* importChangedFile({ driver, filePath, context, size });
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

    /**
     * Threads whose session's record is no longer on disk are never reached
     * through it. Once after a start, when the first sweep has seen every
     * record there is, the reports filed as the person's own in those
     * threads are put right too, told by how they open.
     */
    let orphanTaskNoticesDue = true;
    const retireOrphanTaskNotices = Effect.fn("SessionImporter.retireOrphanTaskNotices")(function* (
      context: SweepContext,
    ) {
      if (!orphanTaskNoticesDue) return;
      orphanTaskNoticesDue = false;
      const done = new Set<string>();
      for (const [key, binding] of context.bindingsBySource) {
        if (binding.provider !== CLAUDE_DRIVER || context.seenSessionIds.has(key)) continue;
        if (done.has(binding.threadId)) continue;
        done.add(binding.threadId);
        yield* retireTaskNoticePrompts({ threadId: binding.threadId, rows: [] }).pipe(
          Effect.catch((error) =>
            Effect.logDebug("session.importer.task-notice-thread-skipped", {
              threadId: binding.threadId,
              error,
            }),
          ),
        );
      }
    });

    const buildSweepContext = Effect.gen(function* () {
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
      return { context, claudeProjectsDir, codexSessionsDir };
    });

    const sweep = Effect.gen(function* () {
      const { context, claudeProjectsDir, codexSessionsDir } = yield* buildSweepContext;

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
      yield* retireOrphanTaskNotices(context).pipe(
        Effect.catchDefect((defect) =>
          Effect.logWarning("session.importer.task-notice-pass-defect", { defect }),
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

    const adoptLiveClaudeSession: SessionImporterShape["adoptLiveClaudeSession"] = (input) =>
      Effect.gen(function* () {
        if (
          (yield* claudeConversationThread(input.sessionId, input.transcriptPath)) !== undefined
        ) {
          return;
        }
        const { context } = yield* buildSweepContext;
        const key = sourceKey(CLAUDE_DRIVER, input.sessionId);
        if (context.bindingsBySource.has(key) || context.t3NativeSessions.has(key)) return;
        // Only what the session wrote before the turn in flight is history;
        // the turn itself reaches the thread through the live attachment.
        const bytes = yield* fs
          .readFile(input.transcriptPath)
          .pipe(Effect.orElseSucceed(() => new Uint8Array(0)));
        const written = new TextDecoder().decode(
          bytes.subarray(0, Math.max(0, Math.min(bytes.length, input.turnOffset))),
        );
        const history = claudeHistoryBeforeTurn(written, input.promptText);
        const parsed = parseClaudeSessionJsonl(history);
        // Agent plumbing and machine-launched runs keep the sweep's placement rules.
        const whole = history === written ? parsed : parseClaudeSessionJsonl(written);
        if (whole.launch === "subagent" || isAutomationSession(whole)) return;
        const now = DateTime.formatIso(yield* DateTime.now);
        yield* importFreshSession({
          driver: CLAUDE_DRIVER,
          sessionId: input.sessionId,
          filePath: input.transcriptPath,
          parsed: {
            ...parsed,
            sessionId: parsed.sessionId ?? input.sessionId,
            cwd: parsed.cwd ?? input.cwd,
            title:
              parsed.title ??
              (input.promptText.trim().length > 0
                ? truncateSessionTitle(claudePromptAsDrawn(input.promptText))
                : undefined),
          },
          context,
          liveAt: now,
        });
      }).pipe(
        Effect.catch((error: unknown) =>
          Effect.logWarning("session.importer.adopt-live-failed", {
            sessionId: input.sessionId,
            error,
          }),
        ),
        Effect.catchDefect((defect: unknown) =>
          Effect.logWarning("session.importer.adopt-live-defect", {
            sessionId: input.sessionId,
            defect,
          }),
        ),
      );

    const healLiveClaudeSession: SessionImporterShape["healLiveClaudeSession"] = (input) =>
      Effect.gen(function* () {
        const held = yield* claudeConversationThread(input.sessionId, input.transcriptPath);
        // A thread no run has touched is plain history: the sweep tails it.
        if (held === undefined || !(yield* hasT3Runs(held.threadId))) return;
        const { threadId } = held;
        const bytes = yield* fs
          .readFile(input.transcriptPath)
          .pipe(Effect.orElseSucceed(() => new Uint8Array(0)));
        if (bytes.length === 0) return;
        const parsed = parseClaudeSessionJsonl(new TextDecoder().decode(bytes));
        yield* catchUpOwnedThread({
          driver: CLAUDE_DRIVER,
          sessionId: input.sessionId,
          filePath: input.transcriptPath,
          parsed,
          threadId,
          // Only the comparison by row applies to a session a mod holds.
          fromLineIndex: parsed.lineCount,
          context: { stats: { importedCount: 0, updatedCount: 0 } },
          carriedOn: held.heldAs !== input.sessionId,
          ...(input.turnAwaitingRunSinceMs === undefined
            ? {}
            : { turnAwaitingRunSinceMs: input.turnAwaitingRunSinceMs }),
        });
      }).pipe(
        Effect.catch((error: unknown) =>
          Effect.logWarning("session.importer.heal-live-failed", {
            sessionId: input.sessionId,
            error,
          }),
        ),
        Effect.catchDefect((defect: unknown) =>
          Effect.logWarning("session.importer.heal-live-defect", {
            sessionId: input.sessionId,
            defect,
          }),
        ),
      );

    const claudeConversationHolder: SessionImporterShape["claudeConversationHolder"] = (input) =>
      claudeConversationThread(input.sessionId, input.transcriptPath).pipe(
        Effect.map((held) => held?.heldAs ?? null),
      );

    const claudeSessionHeldElsewhere: SessionImporterShape["claudeSessionHeldElsewhere"] = (
      input,
    ) =>
      Effect.gen(function* () {
        const theirs = yield* lookupOwnedThread(CLAUDE_DRIVER, input.to);
        if (theirs === undefined) return false;
        return theirs !== (yield* lookupOwnedThread(CLAUDE_DRIVER, input.from));
      });

    return {
      start,
      sweepNow: () => guardedSweep,
      adoptLiveClaudeSession,
      healLiveClaudeSession,
      claudeConversationHolder,
      claudeSessionHeldElsewhere,
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
      const fs = yield* FileSystem.FileSystem;
      // The threads kept in the record's line order: a JSON list of thread
      // ids. Without the file no thread is.
      const recordOrderThreads = yield* fs
        .readFileString(path.join(serverConfig.stateDir, RECORD_ORDER_THREADS_FILE))
        .pipe(
          Effect.orElseSucceed(() => ""),
          Effect.map((text) => {
            const ids = parseJsonlLine(text);
            return new Set(
              Array.isArray(ids) ? ids.filter((id): id is string => typeof id === "string") : [],
            );
          }),
        );
      return yield* makeSessionImporter({
        enabled: serverConfig.importCliSessions ?? false,
        recordOrderThreads,
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
