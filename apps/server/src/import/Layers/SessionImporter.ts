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
import { isEligibleCliSession, isRecord, type ParsedCliSession } from "../sessionImportTypes.ts";
import { SessionImporter, type SessionImporterShape } from "../Services/SessionImporter.ts";

const DEFAULT_SWEEP_INTERVAL_MS = 30 * 1000;

const CLAUDE_DRIVER = ProviderDriverKind.make("claudeAgent");
const CODEX_DRIVER = ProviderDriverKind.make("codex");

class SessionImportError extends Schema.TaggedErrorClass<SessionImportError>()(
  "SessionImportError",
  {
    detail: Schema.String,
  },
) {}

export interface SessionImporterLiveOptions {
  /** Directory containing Claude Code `<project>/<session uuid>.jsonl` files. */
  readonly claudeProjectsDir?: string;
  /** Directory scanned recursively for Codex `rollout-*.jsonl` files. */
  readonly codexSessionsDir?: string;
  readonly sweepIntervalMs?: number;
  /** Whether `start()` forks the background loop; `sweepNow` always works. */
  readonly enabled?: boolean;
}

interface ImportCursor {
  readonly sourceSessionId: string;
  readonly sourcePath: string;
  readonly lineCount: number;
}

interface SweepStats {
  importedCount: number;
  updatedCount: number;
}

interface SweepContext {
  readonly bindingsBySource: ReadonlyMap<string, ProviderRuntimeBindingWithMetadata>;
  readonly seenSessionIds: Set<string>;
  readonly stats: SweepStats;
}

const sourceKey = (driver: ProviderDriverKind, sessionId: string) => `${driver}:${sessionId}`;

const hashHex = (value: string) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex").slice(0, 24);

/** Deterministic T3 ids derived from the source session/workspace. */
export const importedThreadId = (sessionId: string) => ThreadId.make(`cli-import-${sessionId}`);
export const importedProjectId = (workspaceRoot: string) =>
  ProjectId.make(`cli-import-project-${hashHex(workspaceRoot)}`);
const importCommandId = (driver: ProviderDriverKind, sessionId: string, facet: string) =>
  CommandId.make(`cli-import:${driver}:${sessionId}:${facet}`);
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
  };
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
    }) {
      const existing = yield* snapshotQuery.getActiveProjectByWorkspaceRoot(input.workspaceRoot);
      if (Option.isSome(existing)) {
        return existing.value.id;
      }

      const title = path.basename(input.workspaceRoot) || "project";
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
    }) {
      let dispatchedCount = 0;
      for (const message of input.parsed.messages) {
        if (message.lineIndex < input.fromLineIndex) {
          continue;
        }
        const messageId = importedMessageId(input.sessionId, message.sourceId);
        if (message.role === "user") {
          yield* engine.dispatch({
            type: "thread.message.user.record",
            commandId: importCommandId(input.driver, input.sessionId, `msg:${message.sourceId}`),
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
          importCursor: {
            sourceSessionId: input.sessionId,
            sourcePath: input.filePath,
            lineCount: input.parsed.lineCount,
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
    }) {
      const { driver, sessionId, parsed } = input;
      const firstMessage = parsed.messages[0];
      const lastMessage = parsed.messages[parsed.messages.length - 1];
      if (parsed.cwd === undefined || firstMessage === undefined || lastMessage === undefined) {
        return;
      }
      const workspaceRoot = path.resolve(expandHomePath(parsed.cwd));
      const createdAt = parsed.startedAt ?? firstMessage.timestamp;
      const projectId = yield* ensureProject({ workspaceRoot, sessionId, createdAt });
      const threadId = importedThreadId(sessionId);
      const instanceId = defaultInstanceIdForDriver(driver);

      yield* engine.dispatch({
        type: "thread.create",
        commandId: importCommandId(driver, sessionId, "thread.create"),
        threadId,
        projectId,
        title: parsed.title ?? "Imported session",
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
      });
      yield* upsertImportBinding({
        driver,
        sessionId,
        threadId,
        parsed,
        filePath: input.filePath,
        cwd: workspaceRoot,
      });
      // "stopped" keeps the thread settled while still giving the sidebar a
      // provider badge; "starting"/"running" would un-settle it.
      yield* engine.dispatch({
        type: "thread.session.set",
        commandId: importCommandId(driver, sessionId, "session"),
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
      }) {
        const { driver, sessionId, parsed, cursor } = input;
        // A native T3 session has run on this thread since import: the
        // adapter owns the binding now (it flips status away from "stopped"
        // and merges fields like model/activeTurnId into runtimePayload).
        // Hand the thread over permanently — strip the import cursor so this
        // source file is never tailed again and native turns aren't
        // re-imported as duplicates. A terminal continuation of the source
        // session forks to a new session id and imports as a fresh thread.
        const runtimePayload = input.binding.runtimePayload;
        const nativelyOwned =
          (input.binding.status !== undefined && input.binding.status !== "stopped") ||
          (isRecord(runtimePayload) &&
            ("activeTurnId" in runtimePayload || "model" in runtimePayload));
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
        if (parsed.lineCount <= cursor.lineCount) {
          return;
        }
        const threadId = input.binding.threadId;
        const shell = yield* snapshotQuery.getThreadShellById(threadId);
        if (Option.isNone(shell)) {
          // The imported thread was deleted in T3; leave the source alone
          // instead of resurrecting it.
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
      // Recorded before importing: a failed import is retried when the file
      // changes again rather than re-attempted (and re-logged) every sweep.
      fileFingerprints.set(filePath, { size, mtimeMs });

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

      const binding = context.bindingsBySource.get(key);
      if (binding !== undefined) {
        const cursor = readImportCursor(binding.runtimePayload);
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
      yield* importFreshSession({ driver, sessionId, filePath, parsed, context });
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
      for (const binding of bindings) {
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
      }

      const context: SweepContext = {
        bindingsBySource,
        seenSessionIds: new Set<string>(),
        stats: { importedCount: 0, updatedCount: 0 },
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

        yield* Effect.forkScoped(
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
        ...options,
      });
    }),
  );

export const SessionImporterLive = makeSessionImporterLive();
