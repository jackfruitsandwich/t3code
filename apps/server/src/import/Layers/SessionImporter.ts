import { forkParked } from "../../serverActivation.ts";
/**
 * SessionImporter - Background import of provider-native CLI sessions.
 *
 * Sweeps the Claude Code (`<config dir>/projects`) and Codex
 * (`<codex home>/sessions`) transcript directories and materializes each
 * eligible session as a T3 project/thread with its message history and a
 * resume binding, so chatting into the thread continues the real provider
 * session. Source files are read-only; every write goes through
 * `OrchestrationEngineService.dispatch` with commandIds derived
 * deterministically from the source session, so re-runs dedupe via command
 * receipts. The importer NEVER dispatches `thread.turn.start` — importing
 * history must not wake a live agent.
 *
 * Ownership: bindings written by the importer carry
 * `runtimePayload.importCursor` ({ sourceSessionId, sourcePath, lineCount }).
 * A binding without that marker belongs to a T3-native session and is never
 * touched; a discovered file is only imported incrementally when both the
 * session id and path match the marker, so a session adopted (forked) by
 * live T3 chat stops being appended to.
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
  MessageId,
  ProjectId,
  ProviderDriverKind,
  ThreadId,
  type ServerSettings,
} from "@t3tools/contracts";
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

import { ServerConfig } from "../../config.ts";
import { OrchestrationEngineService } from "../../orchestration/Services/OrchestrationEngine.ts";
import { ProjectionSnapshotQuery } from "../../orchestration/Services/ProjectionSnapshotQuery.ts";
import { expandHomePath } from "../../pathExpansion.ts";
import { resolveCodexHomeLayout } from "../../provider/Drivers/CodexHomeLayout.ts";
import {
  ProviderSessionDirectory,
  type ProviderRuntimeBindingWithMetadata,
} from "../../provider/Services/ProviderSessionDirectory.ts";
import { ServerSettingsService } from "../../serverSettings.ts";
import { parseClaudeSessionJsonl } from "../claudeSessionParser.ts";
import { parseCodexRolloutJsonl } from "../codexRolloutParser.ts";
import {
  isCommandNoiseText,
  isEligibleCliSession,
  isRecord,
  type ParsedCliSession,
} from "../sessionImportTypes.ts";
import { SessionImporter, type SessionImporterShape } from "../Services/SessionImporter.ts";

const DEFAULT_SWEEP_INTERVAL_MS = 30 * 1000;

/**
 * Import layout epoch. Bumped when the project-placement rules change so
 * that migrated sessions can be re-dispatched under fresh command ids
 * (epoch-1 receipts would otherwise shadow the re-import). Epoch 2: codex
 * sessions without git metadata group into one shared "Chats" project
 * instead of one project per cwd.
 */
const IMPORT_EPOCH = 2;

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const CODEX_DRIVER = ProviderDriverKind.make("codex");

// This sweeper reads only the default Claude/Codex homes. Other configured
// accounts may legitimately contain the same native session or message IDs.
const belongsToScannedProviderHome = (binding: ProviderRuntimeBindingWithMetadata) =>
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
   * Path prefixes whose sessions route to the Automations project regardless
   * of launch markers. Agent harnesses that drive real interactive CLIs
   * (tmux) produce files indistinguishable from a human's; the directories
   * they run in are the one clean signal. Kin detection still applies.
   */
  readonly automationRoots?: ReadonlyArray<string>;
}

const DEFAULT_MAX_FILE_BYTES = 32 * 1024 * 1024;
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
  readonly binding?: ProviderRuntimeBindingWithMetadata;
  readonly cursor?: ImportCursor;
}

interface SweepContext {
  readonly bindingsBySource: ReadonlyMap<string, ProviderRuntimeBindingWithMetadata>;
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
 * are epoch-scoped because deleted thread ids can never be re-created (the
 * read model retains them), so a layout migration re-imports under a fresh
 * id and tombstones the old binding.
 */
export const importedThreadId = (sessionId: string, idVariant = "") =>
  ThreadId.make(`cli-import-e${IMPORT_EPOCH}${idVariant}-${sessionId}`);
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

function readStampedForkFamily(runtimePayload: unknown): string | undefined {
  if (!isRecord(runtimePayload) || typeof runtimePayload.forkFamilyStamped !== "string") {
    return undefined;
  }
  return runtimePayload.forkFamilyStamped;
}

function readResumeSessionId(binding: ProviderRuntimeBindingWithMetadata): string | undefined {
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

const makeSessionImporter = (options?: SessionImporterLiveOptions) =>
  Effect.gen(function* () {
    const engine = yield* OrchestrationEngineService;
    const snapshotQuery = yield* ProjectionSnapshotQuery;
    const directory = yield* ProviderSessionDirectory;
    const serverSettings = yield* ServerSettingsService;
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;

    const enabled = options?.enabled ?? true;
    const sweepIntervalMs = Math.max(1, options?.sweepIntervalMs ?? DEFAULT_SWEEP_INTERVAL_MS);
    const maxFileBytes = Math.max(1, options?.maxFileBytes ?? DEFAULT_MAX_FILE_BYTES);
    const settleMs = Math.max(0, options?.settleMs ?? DEFAULT_SETTLE_MS);
    const automationRoots = (options?.automationRoots ?? []).map((root) =>
      path.resolve(expandHomePath(root)),
    );

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
      readonly createdAt: string;
      readonly title?: string;
    }) {
      const existing = yield* snapshotQuery.getActiveProjectByWorkspaceRoot(input.workspaceRoot);
      if (Option.isSome(existing)) {
        return existing.value.id;
      }

      const title = input.title ?? (path.basename(input.workspaceRoot) || "project");
      const create = (projectId: ProjectId, commandId: CommandId) =>
        engine
          .dispatch({
            type: "project.create",
            commandId,
            projectId,
            title,
            workspaceRoot: input.workspaceRoot,
            createdAt: input.createdAt,
          })
          .pipe(
            // A concurrent create for the same root loses the invariant
            // race; the requery below picks up whichever project won.
            Effect.catch((error) =>
              Effect.logDebug("session.importer.project-create-skipped", {
                workspaceRoot: input.workspaceRoot,
                error,
              }),
            ),
          );

      yield* create(
        importedProjectId(input.workspaceRoot),
        CommandId.make(`cli-import:project:${hashHex(input.workspaceRoot)}`),
      );
      const requeried = yield* snapshotQuery.getActiveProjectByWorkspaceRoot(input.workspaceRoot);
      if (Option.isSome(requeried)) {
        return requeried.value.id;
      }

      // The deterministic create was receipt-shadowed: an earlier import
      // created this project and the user has since deleted it. Salt the ids
      // with the session so the workspace gets a fresh project.
      yield* create(
        ProjectId.make(`cli-import-project-${hashHex(input.workspaceRoot)}-${input.sessionId}`),
        CommandId.make(`cli-import:project:${hashHex(input.workspaceRoot)}:${input.sessionId}`),
      );
      const salted = yield* snapshotQuery.getActiveProjectByWorkspaceRoot(input.workspaceRoot);
      if (Option.isSome(salted)) {
        return salted.value.id;
      }
      return yield* new SessionImportError({
        detail: `Unable to create or find a project for workspace root '${input.workspaceRoot}'.`,
      });
    });

    const dispatchMessages = Effect.fn("SessionImporter.dispatchMessages")(function* (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly fromLineIndex: number;
      readonly idVariant?: string;
    }) {
      const idVariant = input.idVariant ?? "";
      let dispatchedCount = 0;
      for (const message of input.parsed.messages) {
        if (message.lineIndex < input.fromLineIndex) {
          continue;
        }
        const messageId = importedMessageId(input.sessionId, message.sourceId);
        if (message.role === "user") {
          yield* engine.dispatch({
            type: "thread.message.user.record",
            commandId: importCommandId(
              input.driver,
              input.sessionId,
              `msg:${message.sourceId}`,
              idVariant,
            ),
            threadId: input.threadId,
            messageId,
            text: message.text,
            createdAt: message.timestamp,
          });
        } else {
          // Mirrors ProviderRuntimeIngestion.finalizeAssistantMessage: the
          // full text as one delta, then the finalizing complete.
          yield* engine.dispatch({
            type: "thread.message.assistant.delta",
            commandId: importCommandId(
              input.driver,
              input.sessionId,
              `msg:${message.sourceId}:delta`,
              idVariant,
            ),
            threadId: input.threadId,
            messageId,
            delta: message.text,
            createdAt: message.timestamp,
          });
          yield* engine.dispatch({
            type: "thread.message.assistant.complete",
            commandId: importCommandId(
              input.driver,
              input.sessionId,
              `msg:${message.sourceId}:complete`,
              idVariant,
            ),
            threadId: input.threadId,
            messageId,
            createdAt: message.timestamp,
          });
        }
        dispatchedCount += 1;
      }
      return dispatchedCount;
    });

    const upsertImportBinding = (input: {
      readonly driver: ProviderDriverKind;
      readonly sessionId: string;
      readonly threadId: ThreadId;
      readonly parsed: ParsedCliSession;
      readonly filePath: string;
      /** Omitted on incremental updates so the shallow merge keeps `cwd`. */
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
      return directory.upsert({
        threadId: input.threadId,
        provider: input.driver,
        providerInstanceId: defaultInstanceIdForDriver(input.driver),
        status: "stopped",
        runtimeMode: "full-access",
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
        /** Salts thread/command ids when re-importing over a deleted thread. */
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
      const createdAt = parsed.startedAt ?? firstMessage.timestamp;
      const projectTitle =
        input.placement?.projectTitle ?? (isChat ? CHATS_PROJECT_TITLE : undefined);
      const projectId = yield* ensureProject({
        workspaceRoot,
        sessionId,
        createdAt,
        ...(projectTitle !== undefined ? { title: projectTitle } : {}),
      });
      const threadId = importedThreadId(sessionId, idVariant);
      const instanceId = defaultInstanceIdForDriver(driver);

      yield* engine.dispatch({
        type: "thread.create",
        commandId: importCommandId(driver, sessionId, "thread.create", idVariant),
        threadId,
        projectId,
        title: parsed.title ?? "Imported session",
        historyImport: true,
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
        createdAt,
      });

      yield* dispatchMessages({
        driver,
        sessionId,
        threadId,
        parsed,
        fromLineIndex: 0,
        idVariant,
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
      // Keep a provider badge without waking a session. The server's
      // settlement reactor handles age-based placement under the user's settings.
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: importCommandId(driver, sessionId, "session", idVariant),
        threadId,
        session: {
          threadId,
          status: "stopped",
          providerName: driver,
          providerInstanceId: instanceId,
          runtimeMode: "full-access",
          activeTurnId: null,
          lastError: null,
          updatedAt: lastMessage.timestamp,
        },
        createdAt: lastMessage.timestamp,
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
        readonly binding: ProviderRuntimeBindingWithMetadata;
        readonly cursor: ImportCursor;
        readonly context: SweepContext;
        /** Set when re-entered from the sweep-end machine placement pass. */
        readonly skipMachineDecision?: boolean;
      }) {
        const { driver, sessionId, parsed, cursor } = input;
        // A native T3 session has run on this thread since import: the
        // adapter owns the binding now (it flips status away from "stopped"
        // and merges a real model/activeTurnId into runtimePayload). Hand
        // the thread over permanently — strip the import cursor so this
        // source file is never tailed again and native turns aren't
        // re-imported as duplicates. A terminal continuation of the source
        // session forks to a new session id and imports as a fresh thread.
        //
        // The signals must be NON-NULL values, not key presence: provider
        // lifecycle sweeps (e.g. stopAll on app quit) merge
        // `activeTurnId: null` into every binding's runtimePayload, which a
        // key-presence check misread as adoption and mass-stripped all
        // import cursors.
        const runtimePayload = input.binding.runtimePayload;
        const payloadRecord = isRecord(runtimePayload) ? runtimePayload : undefined;
        const nativelyOwned =
          (input.binding.status !== undefined && input.binding.status !== "stopped") ||
          (payloadRecord !== undefined &&
            (Boolean(payloadRecord.activeTurnId) || Boolean(payloadRecord.model)));
        if (nativelyOwned) {
          yield* directory.upsert({
            threadId: input.binding.threadId,
            provider: driver,
            providerInstanceId:
              input.binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: input.binding.status ?? "stopped",
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
          const subagentShell = yield* snapshotQuery.getThreadShellById(input.binding.threadId);
          if (Option.isNone(subagentShell)) {
            return;
          }
          const subagentProjectId = subagentShell.value.projectId;
          yield* engine.dispatch({
            type: "thread.delete",
            commandId: importCommandId(driver, sessionId, "subagent-thread-delete"),
            threadId: input.binding.threadId,
          });
          yield* directory.upsert({
            threadId: input.binding.threadId,
            provider: driver,
            providerInstanceId:
              input.binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: input.binding.status ?? "stopped",
            resumeCursor: null,
            runtimePayload: { importCursor: null },
          });
          const subagentRemaining =
            yield* snapshotQuery.getFirstActiveThreadIdByProjectId(subagentProjectId);
          if (Option.isNone(subagentRemaining)) {
            yield* engine.dispatch({
              type: "project.delete",
              commandId: importCommandId(driver, sessionId, "subagent-project-delete"),
              projectId: subagentProjectId,
            });
          }
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
        // Delete the old thread, re-import it fresh (epoch-salted command
        // ids bypass the old receipts), and drop the per-chat project it
        // leaves empty. A thread the user already deleted stays dead.
        if (driver === CODEX_DRIVER && cursor.epoch === undefined && parsed.hasProject !== true) {
          const migrateShell = yield* snapshotQuery.getThreadShellById(input.binding.threadId);
          if (Option.isNone(migrateShell)) {
            return;
          }
          const oldProjectId = migrateShell.value.projectId;
          yield* engine.dispatch({
            type: "thread.delete",
            commandId: importCommandId(driver, sessionId, "migrate-thread-delete"),
            threadId: input.binding.threadId,
          });
          // Tombstone the old binding so it stops claiming this source
          // session; the re-import below owns it under an epoch-scoped
          // thread id (deleted thread ids cannot be re-created).
          yield* directory.upsert({
            threadId: input.binding.threadId,
            provider: driver,
            providerInstanceId:
              input.binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: input.binding.status ?? "stopped",
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
          const remaining = yield* snapshotQuery.getFirstActiveThreadIdByProjectId(oldProjectId);
          if (Option.isNone(remaining)) {
            yield* engine.dispatch({
              type: "project.delete",
              commandId: importCommandId(driver, sessionId, "migrate-project-delete"),
              projectId: oldProjectId,
            });
          }
          yield* Effect.logInfo("session.importer.chat-migrated", {
            provider: driver,
            sessionId,
            threadId: input.binding.threadId,
            oldProjectId,
          });
          return;
        }
        const threadId = input.binding.threadId;
        const shell = yield* snapshotQuery.getThreadShellById(threadId);
        if (Option.isNone(shell)) {
          // The imported thread was deleted in T3; leave the source alone
          // instead of resurrecting it.
          return;
        }
        if (parsed.lineCount <= cursor.lineCount) {
          return;
        }

        const dispatchedCount = yield* dispatchMessages({
          driver,
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
        if (dispatchedCount > 0) {
          input.context.stats.updatedCount += 1;
          yield* Effect.logDebug("session.importer.updated", {
            provider: driver,
            sessionId,
            threadId,
            dispatchedCount,
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
          const titleShell = yield* snapshotQuery.getThreadShellById(binding.threadId);
          if (
            Option.isSome(titleShell) &&
            isCommandNoiseText(titleShell.value.title) &&
            titleShell.value.title !== parsed.title
          ) {
            yield* engine.dispatch({
              type: "thread.meta.update",
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
          yield* directory.upsert({
            threadId: binding.threadId,
            provider: driver,
            providerInstanceId: binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: binding.status ?? "stopped",
            runtimePayload: { importForkSig: parsed.forkSignature },
          });
        }
        let cursor = readImportCursor(binding.runtimePayload);
        // Self-heal: repair cursors falsely stripped by the key-presence
        // adoption bug. An importer-created thread (cli-import- prefix)
        // whose binding shows no real native activity (no truthy
        // model/activeTurnId, not running) but lost its cursor gets it
        // reconstructed at the current file position; a genuinely adopted
        // thread keeps its handoff.
        if (cursor === undefined && binding.threadId.startsWith("cli-import-")) {
          const payload = isRecord(binding.runtimePayload) ? binding.runtimePayload : undefined;
          const genuinelyAdopted =
            (binding.status !== undefined && binding.status !== "stopped") ||
            (payload !== undefined && (Boolean(payload.activeTurnId) || Boolean(payload.model)));
          if (!genuinelyAdopted) {
            cursor = {
              sourceSessionId: sessionId,
              sourcePath: filePath,
              lineCount: parsed.lineCount,
              // Chats keep the legacy epoch so the migration below still
              // runs for them; everything else is stamped current.
              ...(driver === CODEX_DRIVER && parsed.hasProject !== true
                ? {}
                : { epoch: IMPORT_EPOCH }),
            };
            yield* directory.upsert({
              threadId: binding.threadId,
              provider: driver,
              providerInstanceId: binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
              status: binding.status ?? "stopped",
              runtimePayload: { importCursor: cursor },
            });
            yield* Effect.logInfo("session.importer.cursor-repaired", {
              provider: driver,
              sessionId,
              threadId: binding.threadId,
            });
          }
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
          yield* directory.upsert({
            threadId: binding.threadId,
            provider: driver,
            providerInstanceId: binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: binding.status ?? "stopped",
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
        const shell = yield* snapshotQuery.getThreadShellById(binding.threadId);
        if (Option.isNone(shell)) {
          continue;
        }
        const automationsProject = yield* snapshotQuery.getActiveProjectByWorkspaceRoot(
          context.automationsWorkspaceRoot,
        );
        if (
          Option.isSome(automationsProject) &&
          shell.value.projectId === automationsProject.value.id
        ) {
          // Already placed; just record the decision.
          yield* directory.upsert({
            threadId: binding.threadId,
            provider: driver,
            providerInstanceId: binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
            status: binding.status ?? "stopped",
            runtimePayload: { importLaunch: "automation" },
          });
          continue;
        }
        // Move to Automations: delete the old thread, tombstone its
        // binding, and re-import under a variant-salted id (deleted thread
        // ids can never be re-created).
        const oldProjectId = shell.value.projectId;
        yield* engine.dispatch({
          type: "thread.delete",
          commandId: importCommandId(driver, sessionId, "automation-thread-delete"),
          threadId: binding.threadId,
        });
        yield* directory.upsert({
          threadId: binding.threadId,
          provider: driver,
          providerInstanceId: binding.providerInstanceId ?? defaultInstanceIdForDriver(driver),
          status: binding.status ?? "stopped",
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
        const remaining = yield* snapshotQuery.getFirstActiveThreadIdByProjectId(oldProjectId);
        if (Option.isNone(remaining)) {
          yield* engine.dispatch({
            type: "project.delete",
            commandId: importCommandId(driver, sessionId, "automation-project-delete"),
            projectId: oldProjectId,
          });
        }
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
     * or more members are stamped; the marker on the binding keeps re-stamps
     * from dispatching every sweep.
     */
    const stampForkFamilies = Effect.fn("SessionImporter.stampForkFamilies")(function* () {
      const bindings = yield* directory.listBindings();
      interface FamilyNode {
        readonly binding: ProviderRuntimeBindingWithMetadata;
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

      const nowIso = yield* Effect.map(DateTime.now, DateTime.formatIso);
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
          if (readStampedForkFamily(member.binding.runtimePayload) === familyId) {
            continue;
          }
          const shell = yield* snapshotQuery.getThreadShellById(member.binding.threadId);
          if (Option.isNone(shell)) {
            continue;
          }
          yield* engine.dispatch({
            type: "thread.fork-family.set",
            commandId: CommandId.make(`cli-import:family:${member.sessionId}:${hashHex(familyId)}`),
            threadId: member.binding.threadId,
            forkFamilyId: familyId,
            createdAt: nowIso,
          });
          yield* directory.upsert({
            threadId: member.binding.threadId,
            provider: member.binding.provider,
            providerInstanceId:
              member.binding.providerInstanceId ??
              defaultInstanceIdForDriver(member.binding.provider),
            status: member.binding.status ?? "stopped",
            runtimePayload: { forkFamilyStamped: familyId },
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

      const bindings = yield* directory.listBindings();
      const bindingsBySource = new Map<string, ProviderRuntimeBindingWithMetadata>();
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
      return yield* makeSessionImporter({
        enabled: serverConfig.importCliSessions,
        ...(serverConfig.importSweepIntervalMs !== undefined
          ? { sweepIntervalMs: serverConfig.importSweepIntervalMs }
          : {}),
        ...(serverConfig.importSettleMs !== undefined
          ? { settleMs: serverConfig.importSettleMs }
          : {}),
        ...(serverConfig.importAutomationRoots.length > 0
          ? { automationRoots: serverConfig.importAutomationRoots }
          : {}),
        ...options,
      });
    }),
  );

export const SessionImporterLive = makeSessionImporterLive();
