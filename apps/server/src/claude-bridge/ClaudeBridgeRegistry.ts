/**
 * ClaudeBridgeRegistry - the server's view of every Claude Code process that
 * runs the t3bridge mod (ClaudeBridgePlugin.ts), through the files in the
 * bridge directory (ClaudeBridgeProtocol.ts).
 *
 * It materializes the plugin on boot, pumps mod events to whoever is attached
 * to that session (a ClaudeBridgeQueryRunner session) or to the unattached
 * handlers (ClaudeBridgeAttachService), writes inbox commands, and launches a
 * background `claude --bg --resume <id>` when a session has no live process.
 */
import { HostProcessEnvironment } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Crypto from "effect/Crypto";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import type * as Scope from "effect/Scope";
import type * as ChildProcessSpawner from "effect/process/ChildProcessSpawner";

import { writeFileStringAtomically } from "../atomicWrite.ts";
import * as ServerConfig from "../config.ts";
import { isRecord, parseJsonlLine } from "../import/sessionImportTypes.ts";
import * as ProcessRunner from "../processRunner.ts";
import {
  materializeClaudeBridgePlugin,
  type ClaudeBridgePluginFiles,
} from "./ClaudeBridgePlugin.ts";
import {
  CLAUDE_BRIDGE_DIR_ENV,
  CLAUDE_BRIDGE_PLUGIN_ID,
  claudeTranscriptOffsetAt,
  isClaudeBridgeSessionLive,
  parseClaudeBridgeEvent,
  parseClaudeBridgeSessionFile,
  parseClaudeBridgeSlashCommands,
  CLAUDE_BRIDGE_MOD_VERSION,
  type ClaudeBridgeCommand,
  type ClaudeBridgeCommandBody,
  type ClaudeBridgeEvent,
  type ClaudeBridgeInbox,
  type ClaudeBridgeSessionFile,
  type ClaudeBridgeSlashCommand,
} from "./ClaudeBridgeProtocol.ts";

const EVENT_PUMP_INTERVAL_MS = 400;
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
/** A process dir whose heartbeat is this old is removed. */
const DEAD_PROCESS_SWEEP_MS = 120_000;
const INBOX_HISTORY = 64;
/** How often a session whose record was not found is looked for again. */
const TRANSCRIPT_LOOKUP_RETRY_MS = 5_000;
/** How far back from the end of a record a turn's first row is searched for. */
const TURN_START_SCAN_BYTES = 8 * 1024 * 1024;
const LAUNCH_ATTACH_TIMEOUT_MS = 45_000;

export class ClaudeBridgeLaunchError extends Schema.TaggedError<ClaudeBridgeLaunchError>()(
  "ClaudeBridgeLaunchError",
  {
    sid: Schema.String,
    detail: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return this.detail;
  }
}

export interface ClaudeBridgeMissedTurn {
  readonly start: Extract<ClaudeBridgeEvent, { readonly type: "turn.start" }>;
  readonly complete: Extract<ClaudeBridgeEvent, { readonly type: "turn.complete" }> | null;
}

export interface ClaudeBridgeProcess {
  readonly sid: string;
  readonly instanceId: string;
  /** The process directory holding the protocol files. */
  readonly dir: string;
  readonly session: ClaudeBridgeSessionFile;
}

export type ClaudeBridgeEventHandler = (
  event: ClaudeBridgeEvent,
  process: ClaudeBridgeProcess,
) => Effect.Effect<void>;

export interface ClaudeBridgeClientSelection {
  readonly model: string;
  readonly permissionMode: string;
}

export interface ClaudeBridgeToolEndpoint {
  readonly endpoint: string;
  readonly authorization: string;
}

export type ClaudeBridgeRecordChange = "turn.start" | "turn.complete" | "typed-mid-turn";

export type ClaudeBridgeRecordHandler = (
  process: ClaudeBridgeProcess,
  change: ClaudeBridgeRecordChange,
) => Effect.Effect<void>;

export interface ClaudeBridgeLaunchInput {
  readonly sid: string;
  readonly cwd: string;
  /** The Claude home the process reads (`CLAUDE_CONFIG_DIR` or `~/.claude`). */
  readonly configDir: string;
  readonly binaryPath: string;
  readonly environment: NodeJS.ProcessEnv;
  readonly model?: string;
  readonly permissionMode?: string;
  readonly extraArgs?: Record<string, string | null>;
}

export class ClaudeBridgeRegistry extends Context.Service<
  ClaudeBridgeRegistry,
  {
    readonly bridgeDir: string;
    readonly plugin: ClaudeBridgePluginFiles;
    /** Whether the mod is installed in a Claude home, so its sessions can be bridged. */
    readonly isPluginInstalled: (configDir: string) => Effect.Effect<boolean>;
    readonly listLiveProcesses: Effect.Effect<ReadonlyArray<ClaudeBridgeProcess>>;
    /** The live process of a session, the freshest heartbeat first. */
    readonly findLiveProcess: (sid: string) => Effect.Effect<ClaudeBridgeProcess | null>;
    readonly readSession: (
      process: ClaudeBridgeProcess,
    ) => Effect.Effect<ClaudeBridgeSessionFile | null>;
    /** The slash commands the process's session has, or null while the mod has not listed them. */
    readonly readCommands: (
      process: ClaudeBridgeProcess,
    ) => Effect.Effect<ReadonlyArray<ClaudeBridgeSlashCommand> | null>;
    /** Appends a command to the process inbox and returns its id. */
    readonly send: (
      process: ClaudeBridgeProcess,
      body: ClaudeBridgeCommandBody,
    ) => Effect.Effect<string>;
    /** Renews the attached marker the mod reads before holding an approval for T3. */
    /**
     * Says T3 is attached to the process. `tools` names where T3 answers the
     * session's calls to T3's own tools, with the credential of the thread
     * that shows it; the mod reads both from the same file.
     */
    readonly heartbeat: (
      process: ClaudeBridgeProcess,
      tools?: ClaudeBridgeToolEndpoint,
    ) => Effect.Effect<void>;
    readonly clearHeartbeat: (process: ClaudeBridgeProcess) => Effect.Effect<void>;
    /**
     * The model and mode a client last sent with a message to this session,
     * kept beside the session's bridge files so it outlives this server
     * process. A client keeps its own copy of a thread's settings and sends
     * it with every message: the same choice arriving again is that copy,
     * not the person choosing again.
     */
    readonly readClientSelection: (
      sid: string,
    ) => Effect.Effect<ClaudeBridgeClientSelection | null>;
    readonly writeClientSelection: (
      sid: string,
      selection: ClaudeBridgeClientSelection,
    ) => Effect.Effect<void>;
    /** Routes the session's events to `handler` until the scope closes. One subscriber per session. */
    readonly subscribe: (
      sid: string,
      handler: ClaudeBridgeEventHandler,
    ) => Effect.Effect<void, never, Scope.Scope>;
    /** Receives events of sessions nobody is subscribed to. */
    readonly onUnattachedEvent: (
      handler: ClaudeBridgeEventHandler,
    ) => Effect.Effect<void, never, Scope.Scope>;
    /**
     * Told when a session's record may hold something its attachment does not
     * carry: a turn began or ended, or the person typed into a running turn.
     */
    readonly onRecordChange: (
      handler: ClaudeBridgeRecordHandler,
    ) => Effect.Effect<void, never, Scope.Scope>;
    readonly noteRecordChange: (
      process: ClaudeBridgeProcess,
      change: ClaudeBridgeRecordChange,
    ) => Effect.Effect<void>;
    /**
     * The last turn the session began while nobody was subscribed, with its
     * end if that came too: a short turn can be over before the run offered
     * for it has attached, and whoever attaches next shows it. `peek` leaves
     * it in place, since its end may still arrive before the caller has
     * subscribed; `take` hands it over once, after subscribing.
     */
    readonly peekMissedTurn: (sid: string) => Effect.Effect<ClaudeBridgeMissedTurn | null>;
    readonly takeMissedTurn: (sid: string) => Effect.Effect<ClaudeBridgeMissedTurn | null>;
    /** Starts a background Claude Code process for the session and waits for its mod to report. */
    readonly launch: (
      input: ClaudeBridgeLaunchInput,
    ) => Effect.Effect<ClaudeBridgeProcess, ClaudeBridgeLaunchError>;
    /**
     * The Claude home under which `claude agents` lists a live terminal process of
     * the session (one without the mod), or null when no home does.
     */
    readonly findTerminalHome: (
      sid: string,
      input: Pick<ClaudeBridgeLaunchInput, "binaryPath" | "environment" | "cwd" | "configDir">,
    ) => Effect.Effect<string | null>;
    /**
     * The Claude home that last ran the session, judged by Claude's own records
     * (`history.jsonl` prompts, `--bg` job state), or null when none shows it.
     */
    readonly findSessionHome: (sid: string, configDir: string) => Effect.Effect<string | null>;
  }
>()("t3/claude-bridge/ClaudeBridgeRegistry") {}

const isProcessAlive = (pid: number): boolean => {
  try {
    process.kill(pid, 0);
    return true;
  } catch {
    return false;
  }
};

/** The spawned CLI must not believe it runs inside another Claude Code session. */
export function claudeLaunchEnvironment(
  environment: NodeJS.ProcessEnv,
  bridgeDir: string,
): NodeJS.ProcessEnv {
  const {
    CLAUDECODE: _claudecode,
    CLAUDE_CODE_ENTRYPOINT: _entrypoint,
    CLAUDE_CODE_SSE_PORT: _ssePort,
    ...rest
  } = environment;
  return { ...rest, [CLAUDE_BRIDGE_DIR_ENV]: bridgeDir };
}

/**
 * The environment a Claude process of `home` runs under. Claude keeps its
 * global state (trusted folders, onboarding, account) in `~/.claude.json` only
 * while `CLAUDE_CONFIG_DIR` is unset; naming the default home explicitly makes
 * it read `~/.claude/.claude.json` instead, which holds no trusted folder, so
 * every background launch there is refused as untrusted. The default home is
 * therefore selected by leaving the variable out.
 */
export function claudeHomeEnvironment(
  environment: NodeJS.ProcessEnv,
  home: string,
  defaultHome: string,
): NodeJS.ProcessEnv {
  if (home === defaultHome) {
    const { CLAUDE_CONFIG_DIR: _configDir, ...rest } = environment;
    return rest;
  }
  return { ...environment, CLAUDE_CONFIG_DIR: home };
}

/**
 * Marks `folder` trusted in the parsed content of a Claude global state file,
 * keeping everything else. Returns null when it already is, or when the
 * content is not the object Claude writes (never invent that file).
 */
export function withTrustedClaudeFolder(state: unknown, folder: string): unknown | null {
  if (!isRecord(state)) return null;
  const projects = isRecord(state.projects) ? state.projects : {};
  const existing = isRecord(projects[folder]) ? projects[folder] : {};
  if (existing.hasTrustDialogAccepted === true) return null;
  return {
    ...state,
    projects: { ...projects, [folder]: { ...existing, hasTrustDialogAccepted: true } },
  };
}

/**
 * A background job keeps the flags of its first launch as saved options and
 * `claude --bg --resume` with any other flags starts a COPY of the session
 * under a new id. So the flags go only with the first launch; a relaunch of a
 * job that already has saved options passes none and the saved ones apply.
 */
export function claudeLaunchArgs(
  input: ClaudeBridgeLaunchInput,
  options: { readonly hasSavedOptions: boolean },
): ReadonlyArray<string> {
  const args = ["--bg", "--resume", input.sid];
  if (options.hasSavedOptions) return args;
  if (input.model !== undefined && input.model.length > 0) {
    args.push("--model", input.model);
  }
  if (input.permissionMode !== undefined && input.permissionMode !== "default") {
    args.push("--permission-mode", input.permissionMode);
  }
  for (const [flag, value] of Object.entries(input.extraArgs ?? {})) {
    args.push(`--${flag}`);
    if (value !== null) args.push(value);
  }
  return args;
}

/**
 * Claude homes whose session records can name a terminal process holding a
 * session. `claude agents --json` reads one home at a time, and the person runs
 * Claude from more than one (`~/.claude`, `~/.claude-ev`), so the configured
 * home is checked first and then every sibling `.claude*` directory that keeps
 * a `sessions` folder. Without this a session open under another home looks
 * absent and T3 would start a second process on the same id.
 */
export function claudeHomesToScan(
  configDir: string,
  siblingHomesWithSessions: ReadonlyArray<string>,
): ReadonlyArray<string> {
  const homes = [configDir];
  for (const home of siblingHomesWithSessions) {
    if (!homes.includes(home)) homes.push(home);
  }
  return homes;
}

export interface ClaudeHomeEvidence {
  readonly home: string;
  /** Newest `history.jsonl` prompt the home recorded for the session. */
  readonly lastPromptAt: number | null;
  /** Whether the home keeps `--bg` job state for the session. */
  readonly hasJob: boolean;
}

/**
 * The home that ran a session most recently: the newest recorded prompt wins,
 * and a home with only job state counts behind any home with prompts. A
 * session resumed under the wrong home would run on the wrong account.
 */
export function pickSessionHome(evidence: ReadonlyArray<ClaudeHomeEvidence>): string | null {
  let best: { readonly home: string; readonly at: number } | null = null;
  for (const entry of evidence) {
    const at = entry.lastPromptAt ?? (entry.hasJob ? 0 : null);
    if (at === null) continue;
    if (best === null || at > best.at) best = { home: entry.home, at };
  }
  return best === null ? null : best.home;
}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const serverConfig = yield* ServerConfig.ServerConfig;
  const processRunner = yield* ProcessRunner.make();
  const hostEnvironment = yield* HostProcessEnvironment;
  const crypto = yield* Crypto.Crypto;

  const bridgeDir =
    hostEnvironment[CLAUDE_BRIDGE_DIR_ENV]?.trim() || path.join(serverConfig.stateDir, "bridge");
  const claudeDir = path.join(bridgeDir, "claude");
  // The state dir is this server's own; failing to write under it is fatal
  // elsewhere too.
  yield* fs.makeDirectory(claudeDir, { recursive: true }).pipe(Effect.orDie);
  const plugin = yield* materializeClaudeBridgePlugin(
    path.join(serverConfig.stateDir, "claude-bridge"),
  ).pipe(Effect.orDie);

  /** `~/.claude` as the launched processes see it. */
  const defaultHomeFor = (environment: NodeJS.ProcessEnv) =>
    path.join(environment.HOME ?? hostEnvironment.HOME ?? "", ".claude");
  const homeEnvironment = (environment: NodeJS.ProcessEnv, home: string) =>
    claudeHomeEnvironment(environment, home, defaultHomeFor(environment));

  // What a running mod compares itself with: a process on an older mod loads
  // this one by itself the next time it is idle.
  yield* fs
    .writeFileString(
      path.join(claudeDir, "mod-version.json"),
      encodeJson({ version: CLAUDE_BRIDGE_MOD_VERSION }),
    )
    .pipe(Effect.ignore);

  const subscribers = new Map<string, ClaudeBridgeEventHandler>();
  const missedTurns = new Map<string, ClaudeBridgeMissedTurn>();
  const peekMissedTurn = (sid: string) => Effect.sync(() => missedTurns.get(sid) ?? null);
  const takeMissedTurn = (sid: string) =>
    Effect.sync(() => {
      const missed = missedTurns.get(sid) ?? null;
      missedTurns.delete(sid);
      return missed;
    });
  const unattachedHandlers = new Set<ClaudeBridgeEventHandler>();
  const recordHandlers = new Set<ClaudeBridgeRecordHandler>();
  const noteRecordChange = (entry: ClaudeBridgeProcess, change: ClaudeBridgeRecordChange) =>
    Effect.forEach(recordHandlers, (handler) => handler(entry, change), { discard: true }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("claude-bridge.record-handler-failed", { sid: entry.sid, cause }),
      ),
    );

  const readJson = (file: string) =>
    fs.readFileString(file).pipe(
      Effect.map((content): unknown => parseJsonlLine(content) ?? null),
      Effect.orElseSucceed((): unknown => null),
    );

  const listDirectories = (dir: string) =>
    fs.readDirectory(dir).pipe(
      Effect.orElseSucceed((): ReadonlyArray<string> => []),
      Effect.flatMap((entries) =>
        Effect.forEach(
          entries,
          (entry) =>
            fs.stat(path.join(dir, entry)).pipe(
              Effect.map((info) => (info.type === "Directory" ? entry : null)),
              Effect.orElseSucceed(() => null),
            ),
          { concurrency: 8 },
        ),
      ),
      Effect.map((entries) => entries.filter((entry): entry is string => entry !== null)),
    );

  // A mod loaded into a session that was already running (/reload-plugins) is
  // not told where the session's record is, and counts its offsets from zero
  // until a hook names the file. The record is still where Claude Code keeps
  // it: the session's own file under one of that home's projects.
  const knownTranscripts = new Map<string, { readonly file: string; readonly lookedAt: number }>();
  const recordUnknownToMod = new Set<string>();
  const turnOffsets = new Map<string, number>();

  const findTranscript = (session: ClaudeBridgeSessionFile) =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const known = knownTranscripts.get(session.sid);
      if (
        known !== undefined &&
        (known.file.length > 0 || now - known.lookedAt < TRANSCRIPT_LOOKUP_RETRY_MS)
      ) {
        return known.file;
      }
      const projects = path.join(
        session.configDir.length > 0 ? session.configDir : defaultHomeFor(hostEnvironment),
        "projects",
      );
      const folders = yield* fs
        .readDirectory(projects)
        .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
      let file = "";
      for (const folder of folders) {
        const candidate = path.join(projects, folder, `${session.sid}.jsonl`);
        if (yield* fs.exists(candidate).pipe(Effect.orElseSucceed(() => false))) {
          file = candidate;
          break;
        }
      }
      knownTranscripts.set(session.sid, { file, lookedAt: now });
      return file;
    });

  /** Where in the record a turn that began at `startedAt` starts. */
  const turnOffset = (sid: string, turnId: string, file: string, startedAt: number) =>
    Effect.gen(function* () {
      const key = `${sid}:${turnId}`;
      const cached = turnOffsets.get(key);
      if (cached !== undefined) return cached;
      const size = yield* fs.stat(file).pipe(
        Effect.map((info) => Number(info.size)),
        Effect.orElseSucceed(() => 0),
      );
      const from = Math.max(0, size - TURN_START_SCAN_BYTES);
      const bytes = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* fs.open(file, { flag: "r" });
          yield* handle.seek(BigInt(from), "start");
          return Option.getOrElse(yield* handle.readAlloc(size - from), () => new Uint8Array(0));
        }),
      ).pipe(Effect.orElseSucceed(() => new Uint8Array(0)));
      // The prompt's row is written a moment either side of the mod's report.
      const offset = claudeTranscriptOffsetAt(bytes, from, startedAt - 1_500) ?? size;
      if (turnOffsets.size > 256) turnOffsets.delete(turnOffsets.keys().next().value!);
      turnOffsets.set(key, offset);
      return offset;
    });

  /** The session as its mod reports it, with the record found when the mod could not name it. */
  const locateRecord = (session: ClaudeBridgeSessionFile) =>
    Effect.gen(function* () {
      if (session.transcriptPath.length > 0) {
        recordUnknownToMod.delete(session.sid);
        return session;
      }
      const file = yield* findTranscript(session);
      if (file.length === 0) return session;
      recordUnknownToMod.add(session.sid);
      const active = session.activeTurn;
      return {
        ...session,
        transcriptPath: file,
        activeTurn:
          active === null
            ? null
            : {
                ...active,
                transcriptOffset: yield* turnOffset(
                  session.sid,
                  active.turnId,
                  file,
                  active.startedAt,
                ),
              },
      } satisfies ClaudeBridgeSessionFile;
    });

  const readProcess = (sid: string, instanceId: string) =>
    Effect.gen(function* () {
      const dir = path.join(claudeDir, sid, instanceId);
      const reported = parseClaudeBridgeSessionFile(
        yield* readJson(path.join(dir, "session.json")),
      );
      if (reported === null) return null;
      const session = yield* locateRecord(reported);
      return { sid, instanceId, dir, session } satisfies ClaudeBridgeProcess;
    });

  const listProcesses = Effect.gen(function* () {
    const sids = yield* listDirectories(claudeDir);
    const processes: Array<ClaudeBridgeProcess> = [];
    for (const sid of sids) {
      for (const instanceId of yield* listDirectories(path.join(claudeDir, sid))) {
        const entry = yield* readProcess(sid, instanceId);
        if (entry !== null) processes.push(entry);
      }
    }
    return processes;
  });

  const listLiveProcesses = Effect.gen(function* () {
    const processes = yield* listProcesses;
    const now = yield* Clock.currentTimeMillis;
    return processes
      .filter((entry) => isClaudeBridgeSessionLive(entry.session, now))
      .sort((a, b) => b.session.heartbeatAt - a.session.heartbeatAt);
  });

  const findLiveProcess = (sid: string) =>
    Effect.gen(function* () {
      const instances = yield* listDirectories(path.join(claudeDir, sid));
      const now = yield* Clock.currentTimeMillis;
      let best: ClaudeBridgeProcess | null = null;
      for (const instanceId of instances) {
        const entry = yield* readProcess(sid, instanceId);
        if (entry === null || !isClaudeBridgeSessionLive(entry.session, now)) continue;
        if (best === null || entry.session.heartbeatAt > best.session.heartbeatAt) best = entry;
      }
      return best;
    });

  const readSession = (entry: ClaudeBridgeProcess) =>
    readJson(path.join(entry.dir, "session.json")).pipe(
      Effect.map(parseClaudeBridgeSessionFile),
      Effect.flatMap((session) =>
        session === null ? Effect.succeed(null) : locateRecord(session),
      ),
    );

  const readCommands = (entry: ClaudeBridgeProcess) =>
    readJson(path.join(entry.dir, "commands.json")).pipe(
      Effect.map(parseClaudeBridgeSlashCommands),
    );

  // One writer per inbox (this server), so read-modify-write is safe; the
  // atomic rename keeps the mod from reading a half-written file.
  const send = (entry: ClaudeBridgeProcess, body: ClaudeBridgeCommandBody) =>
    Effect.gen(function* () {
      const inboxPath = path.join(entry.dir, "inbox.json");
      const current = yield* readJson(inboxPath);
      const existing: ClaudeBridgeInbox =
        isRecord(current) && typeof current.seq === "number" && Array.isArray(current.commands)
          ? { seq: current.seq, commands: current.commands as ReadonlyArray<ClaudeBridgeCommand> }
          : { seq: 0, commands: [] };
      const id = yield* crypto.randomUUIDv4.pipe(Effect.orDie);
      const seq = existing.seq + 1;
      const command: ClaudeBridgeCommand = { ...body, seq, id };
      const next: ClaudeBridgeInbox = {
        seq,
        commands: [...existing.commands.slice(-(INBOX_HISTORY - 1)), command],
      };
      yield* writeFileStringAtomically({ filePath: inboxPath, contents: encodeJson(next) }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
        Effect.orDie,
      );
      return id;
    });

  const heartbeat = (entry: ClaudeBridgeProcess, tools?: ClaudeBridgeToolEndpoint) =>
    Clock.currentTimeMillis.pipe(
      Effect.flatMap((now) =>
        fs.writeFileString(
          path.join(entry.dir, "t3.json"),
          encodeJson({
            heartbeatAt: now,
            pid: process.pid,
            ...(tools === undefined ? {} : { mcp: tools }),
          }),
          // It names a credential: the person's eyes only.
          { mode: 0o600 },
        ),
      ),
      Effect.ignore,
    );

  const clientSelectionFile = (sid: string) => path.join(claudeDir, sid, "client-selection.json");
  const readClientSelection = (sid: string) =>
    readJson(clientSelectionFile(sid)).pipe(
      Effect.map((value) =>
        isRecord(value) &&
        typeof value.model === "string" &&
        typeof value.permissionMode === "string"
          ? { model: value.model, permissionMode: value.permissionMode }
          : null,
      ),
    );
  const writeClientSelection = (sid: string, selection: ClaudeBridgeClientSelection) =>
    fs.writeFileString(clientSelectionFile(sid), encodeJson(selection)).pipe(Effect.ignore);

  const clearHeartbeat = (entry: ClaudeBridgeProcess) =>
    fs.remove(path.join(entry.dir, "t3.json")).pipe(Effect.ignore);

  const dispatch = (event: ClaudeBridgeEvent, entry: ClaudeBridgeProcess) =>
    Effect.gen(function* () {
      if (event.type === "turn.start" || event.type === "turn.complete") {
        yield* noteRecordChange(entry, event.type);
      }
      const subscriber = subscribers.get(entry.sid);
      if (subscriber !== undefined) {
        yield* subscriber(event, entry);
        return;
      }
      if (event.type === "turn.start") {
        missedTurns.set(entry.sid, { start: event, complete: null });
      } else if (event.type === "turn.complete") {
        const missed = missedTurns.get(entry.sid);
        if (missed !== undefined && missed.start.turnId === event.turnId) {
          missedTurns.set(entry.sid, { start: missed.start, complete: event });
        }
      }
      for (const handler of unattachedHandlers) {
        yield* handler(event, entry);
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("claude-bridge.event-handler-failed", {
          sid: entry.sid,
          type: event.type,
          cause,
        }),
      ),
    );

  const pumpProcess = (entry: ClaudeBridgeProcess, live: boolean) =>
    Effect.gen(function* () {
      const eventsDir = path.join(entry.dir, "events");
      const files = (yield* fs
        .readDirectory(eventsDir)
        .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => [])))
        .filter((name) => name.endsWith(".json"))
        .sort();
      for (const name of files) {
        const file = path.join(eventsDir, name);
        const event = parseClaudeBridgeEvent(yield* readJson(file));
        yield* fs.remove(file).pipe(Effect.ignore);
        if (event === null) continue;
        // A dead process's leftovers are history the transcript already holds.
        if (!live && event.type !== "ended") continue;
        // A mod that does not know the record yet reports every turn from 0.
        const located =
          event.type === "turn.start" &&
          recordUnknownToMod.has(entry.sid) &&
          entry.session.transcriptPath.length > 0
            ? {
                ...event,
                transcriptOffset: yield* turnOffset(
                  entry.sid,
                  event.turnId,
                  entry.session.transcriptPath,
                  event.at,
                ),
              }
            : event;
        yield* dispatch(located, entry);
      }
    });

  const sweepDead = (entry: ClaudeBridgeProcess) =>
    Effect.gen(function* () {
      missedTurns.delete(entry.sid);
      knownTranscripts.delete(entry.sid);
      recordUnknownToMod.delete(entry.sid);
      yield* fs.remove(entry.dir, { recursive: true }).pipe(Effect.ignore);
      const sidDir = path.join(claudeDir, entry.sid);
      const remaining = yield* listDirectories(sidDir);
      if (remaining.length === 0) {
        yield* fs.remove(sidDir, { recursive: true }).pipe(Effect.ignore);
      }
    });

  const pumpOnce = Effect.gen(function* () {
    const processes = yield* listProcesses;
    const now = yield* Clock.currentTimeMillis;
    for (const entry of processes) {
      const live = isClaudeBridgeSessionLive(entry.session, now);
      yield* pumpProcess(entry, live);
      if (!live && now - entry.session.heartbeatAt > DEAD_PROCESS_SWEEP_MS) {
        yield* sweepDead(entry);
      }
    }
  }).pipe(Effect.catchCause((cause) => Effect.logWarning("claude-bridge.pump-failed", { cause })));

  yield* Effect.forkScoped(
    pumpOnce.pipe(Effect.andThen(Effect.sleep(`${EVENT_PUMP_INTERVAL_MS} millis`)), Effect.forever),
  );

  const subscribe = (sid: string, handler: ClaudeBridgeEventHandler) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        subscribers.set(sid, handler);
      }),
      () =>
        Effect.sync(() => {
          if (subscribers.get(sid) === handler) subscribers.delete(sid);
        }),
    ).pipe(Effect.asVoid);

  const onUnattachedEvent = (handler: ClaudeBridgeEventHandler) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        unattachedHandlers.add(handler);
      }),
      () =>
        Effect.sync(() => {
          unattachedHandlers.delete(handler);
        }),
    ).pipe(Effect.asVoid);

  const onRecordChange = (handler: ClaudeBridgeRecordHandler) =>
    Effect.acquireRelease(
      Effect.sync(() => {
        recordHandlers.add(handler);
      }),
      () =>
        Effect.sync(() => {
          recordHandlers.delete(handler);
        }),
    ).pipe(Effect.asVoid);

  const isPluginInstalled = (configDir: string) =>
    Effect.gen(function* () {
      const installed = yield* readJson(path.join(configDir, "plugins", "installed_plugins.json"));
      if (!isRecord(installed) || !isRecord(installed.plugins)) return false;
      if (!(CLAUDE_BRIDGE_PLUGIN_ID in installed.plugins)) return false;
      const settings = yield* readJson(path.join(configDir, "settings.json"));
      const enabled =
        isRecord(settings) && isRecord(settings.enabledPlugins)
          ? settings.enabledPlugins[CLAUDE_BRIDGE_PLUGIN_ID]
          : undefined;
      return enabled !== false;
    });

  const runClaude = (input: {
    readonly binaryPath: string;
    readonly args: ReadonlyArray<string>;
    readonly cwd: string;
    readonly environment: NodeJS.ProcessEnv;
  }) =>
    processRunner.run({
      command: input.binaryPath,
      args: input.args,
      cwd: input.cwd,
      env: claudeLaunchEnvironment(input.environment, bridgeDir),
      timeout: "90 seconds",
    });

  const isListedBackgroundJob = (
    sid: string,
    input: Pick<ClaudeBridgeLaunchInput, "binaryPath" | "environment" | "cwd">,
  ) =>
    runClaude({ ...input, args: ["agents", "--json"] }).pipe(
      Effect.map((output) => {
        const parsed = parseJsonlLine(output.stdout);
        return (
          Array.isArray(parsed) &&
          parsed.some((agent) => isRecord(agent) && agent.sessionId === sid)
        );
      }),
      Effect.orElseSucceed(() => false),
    );

  /** Sibling `.claude*` directories of the configured home that keep a `sessions` folder. */
  const siblingClaudeHomes = (configDir: string) =>
    Effect.gen(function* () {
      const parent = path.dirname(configDir);
      const entries = yield* fs
        .readDirectory(parent)
        .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
      const homes: Array<string> = [];
      for (const entry of [...entries].sort()) {
        if (!entry.startsWith(".claude")) continue;
        const home = path.join(parent, entry);
        const hasSessions = yield* fs
          .exists(path.join(home, "sessions"))
          .pipe(Effect.orElseSucceed(() => false));
        if (hasSessions) homes.push(home);
      }
      return homes;
    });

  const listsLiveTerminalProcess = (
    sid: string,
    home: string,
    input: Pick<ClaudeBridgeLaunchInput, "binaryPath" | "environment" | "cwd">,
  ) =>
    runClaude({
      binaryPath: input.binaryPath,
      cwd: input.cwd,
      environment: homeEnvironment(input.environment, home),
      args: ["agents", "--json"],
    }).pipe(
      Effect.map((output) => {
        const parsed = parseJsonlLine(output.stdout);
        if (!Array.isArray(parsed)) return false;
        return parsed.some(
          (agent) =>
            isRecord(agent) &&
            agent.sessionId === sid &&
            agent.kind === "interactive" &&
            typeof agent.pid === "number" &&
            isProcessAlive(agent.pid),
        );
      }),
      Effect.orElseSucceed(() => false),
    );

  const findTerminalHome = (
    sid: string,
    input: Pick<ClaudeBridgeLaunchInput, "binaryPath" | "environment" | "cwd" | "configDir">,
  ) =>
    Effect.gen(function* () {
      const homes = claudeHomesToScan(input.configDir, yield* siblingClaudeHomes(input.configDir));
      for (const home of homes) {
        if (yield* listsLiveTerminalProcess(sid, home, input)) return home;
      }
      return null;
    });

  /** Newest `history.jsonl` timestamp a home recorded for the session, or null. */
  const lastPromptAt = (home: string, sid: string) =>
    fs.readFileString(path.join(home, "history.jsonl")).pipe(
      Effect.map((content) => {
        let newest: number | null = null;
        for (const line of content.split("\n")) {
          if (!line.includes(sid)) continue;
          const row = parseJsonlLine(line);
          if (!isRecord(row) || row.sessionId !== sid) continue;
          const at = typeof row.timestamp === "number" ? row.timestamp : 0;
          if (newest === null || at > newest) newest = at;
        }
        return newest;
      }),
      Effect.orElseSucceed((): number | null => null),
    );

  const findSessionHome = (sid: string, configDir: string) =>
    Effect.gen(function* () {
      const evidence: Array<ClaudeHomeEvidence> = [];
      for (const home of claudeHomesToScan(configDir, yield* siblingClaudeHomes(configDir))) {
        evidence.push({
          home,
          lastPromptAt: yield* lastPromptAt(home, sid),
          hasJob: yield* fs
            .exists(path.join(home, "jobs", sid.slice(0, 8), "state.json"))
            .pipe(Effect.orElseSucceed(() => false)),
        });
      }
      return pickSessionHome(evidence);
    });

  /** `claude --bg` records a job's flags in `<config dir>/jobs/<first 8 of the id>/state.json`. */
  const hasSavedJobOptions = (input: ClaudeBridgeLaunchInput) =>
    fs
      .exists(path.join(input.configDir, "jobs", input.sid.slice(0, 8), "state.json"))
      .pipe(Effect.orElseSucceed(() => false));

  /**
   * Records the thread's folder as trusted in the home's global state file.
   * T3 already runs Claude in that folder over the SDK, which asks for no
   * trust; a background launch reads the saved answer instead of asking, so a
   * folder nobody opened in a terminal would otherwise never start.
   */
  const trustWorkspace = (input: ClaudeBridgeLaunchInput) =>
    Effect.gen(function* () {
      const defaultHome = defaultHomeFor(input.environment);
      const stateFile =
        input.configDir === defaultHome
          ? path.join(path.dirname(defaultHome), ".claude.json")
          : path.join(input.configDir, ".claude.json");
      const folder = yield* fs.realPath(input.cwd).pipe(Effect.orElseSucceed(() => input.cwd));
      const next = withTrustedClaudeFolder(yield* readJson(stateFile), folder);
      if (next === null) return false;
      yield* writeFileStringAtomically({
        filePath: stateFile,
        contents: `${encodeJson(next)}\n`,
      }).pipe(
        Effect.provideService(FileSystem.FileSystem, fs),
        Effect.provideService(Path.Path, path),
      );
      yield* Effect.logInfo("claude-bridge.workspace-trusted", { folder, stateFile });
      return true;
    }).pipe(Effect.orElseSucceed(() => false));

  const launch = (input: ClaudeBridgeLaunchInput) =>
    Effect.gen(function* () {
      const hasSavedOptions = yield* hasSavedJobOptions(input);
      // The process runs under the session's home, so it keeps that home's account.
      const environment = homeEnvironment(input.environment, input.configDir);
      const start = runClaude({
        binaryPath: input.binaryPath,
        args: claudeLaunchArgs(input, { hasSavedOptions }),
        cwd: input.cwd,
        environment,
      }).pipe(
        Effect.mapError(
          (cause) =>
            new ClaudeBridgeLaunchError({
              sid: input.sid,
              detail: `Could not start \`claude --bg\` for session ${input.sid}.`,
              cause,
            }),
        ),
      );
      let output = yield* start;
      if (
        /not trusted/i.test(`${output.stdout}\n${output.stderr}`) &&
        (yield* trustWorkspace(input))
      ) {
        output = yield* start;
      }
      const text = `${output.stdout}\n${output.stderr}`;
      const copy = /started a copy[^\n]*? as ([0-9a-f]{8})/i.exec(text);
      if (copy !== null) {
        // The CLI made a copy: the session already runs somewhere without the
        // mod, or (unexpectedly) its saved options rejected the flags. The
        // copy must not live on under a new id.
        for (const subcommand of ["stop", "rm"]) {
          yield* runClaude({
            binaryPath: input.binaryPath,
            args: [subcommand, copy[1]!],
            cwd: input.cwd,
            environment,
          }).pipe(Effect.ignore);
        }
        const note = /note:[^\n]*/i.exec(text)?.[0].trim() ?? "";
        const detail = /saved options/i.test(note)
          ? `Claude Code started a copy of session ${input.sid} instead of resuming it (${note}). The copy was stopped and removed; its transcript ${copy[1]} may remain under ${input.configDir}/projects.`
          : `Session ${input.sid} is already open in a Claude Code process without the T3 bridge plugin. Close it, or install the plugin (\`claude plugin marketplace add ${plugin.marketplaceDir}\` then \`claude plugin install ${CLAUDE_BRIDGE_PLUGIN_ID}\`) and restart it.`;
        return yield* new ClaudeBridgeLaunchError({ sid: input.sid, detail });
      }
      const untrusted = /not trusted[^\n]*/i.exec(text);
      if (untrusted !== null) {
        return yield* new ClaudeBridgeLaunchError({
          sid: input.sid,
          detail: `Claude Code refused to start in ${input.cwd}: ${untrusted[0].trim()}`,
        });
      }
      if (output.code !== 0 && output.code !== null) {
        return yield* new ClaudeBridgeLaunchError({
          sid: input.sid,
          detail: `\`claude --bg\` exited with code ${output.code}: ${text.trim().slice(0, 500)}`,
        });
      }
      // A job the service cannot resume (no transcript under that id) is
      // listed for a few seconds and then disappears; the mod never reports.
      const deadline = (yield* Clock.currentTimeMillis) + LAUNCH_ATTACH_TIMEOUT_MS;
      let wasListed = false;
      let polls = 0;
      while ((yield* Clock.currentTimeMillis) < deadline) {
        const entry = yield* findLiveProcess(input.sid);
        if (entry !== null) return entry;
        polls += 1;
        if (polls % 10 === 0) {
          const listed = yield* isListedBackgroundJob(input.sid, { ...input, environment });
          if (wasListed && !listed) {
            return yield* new ClaudeBridgeLaunchError({
              sid: input.sid,
              detail: `Claude Code dropped the background job for session ${input.sid}: it could not resume it. The session's transcript may be missing from ${input.configDir}.`,
            });
          }
          wasListed = wasListed || listed;
        }
        yield* Effect.sleep("300 millis");
      }
      return yield* new ClaudeBridgeLaunchError({
        sid: input.sid,
        detail: `Claude Code started session ${input.sid} in the background, but the T3 bridge plugin never reported in. Is the plugin installed and enabled in this Claude home?`,
      });
    });

  return ClaudeBridgeRegistry.of({
    bridgeDir,
    plugin,
    isPluginInstalled,
    listLiveProcesses,
    findLiveProcess,
    readSession,
    readCommands,
    send,
    heartbeat,
    clearHeartbeat,
    subscribe,
    onUnattachedEvent,
    onRecordChange,
    noteRecordChange,
    readClientSelection,
    writeClientSelection,
    peekMissedTurn,
    takeMissedTurn,
    launch,
    findTerminalHome,
    findSessionHome,
  });
});

export const layer: Layer.Layer<
  ClaudeBridgeRegistry,
  never,
  | ServerConfig.ServerConfig
  | FileSystem.FileSystem
  | Path.Path
  | Crypto.Crypto
  | ChildProcessSpawner.ChildProcessSpawner
> = Layer.effect(ClaudeBridgeRegistry, make);

/** A registry that sees no processes and bridges nothing, so adapters take the SDK path. */
export const layerTest = Layer.succeed(
  ClaudeBridgeRegistry,
  ClaudeBridgeRegistry.of({
    bridgeDir: "",
    plugin: { marketplaceDir: "", pluginDir: "" },
    isPluginInstalled: () => Effect.succeed(false),
    listLiveProcesses: Effect.succeed([]),
    findLiveProcess: () => Effect.succeed(null),
    readSession: () => Effect.succeed(null),
    readCommands: () => Effect.succeed(null),
    send: () => Effect.succeed("unused"),
    heartbeat: () => Effect.void,
    clearHeartbeat: () => Effect.void,
    subscribe: () => Effect.void,
    onUnattachedEvent: () => Effect.void,
    onRecordChange: () => Effect.void,
    noteRecordChange: () => Effect.void,
    readClientSelection: () => Effect.succeed(null),
    writeClientSelection: () => Effect.void,
    peekMissedTurn: () => Effect.succeed(null),
    takeMissedTurn: () => Effect.succeed(null),
    launch: (input) =>
      Effect.fail(
        new ClaudeBridgeLaunchError({ sid: input.sid, detail: "Claude bridge disabled in tests." }),
      ),
    findTerminalHome: () => Effect.succeed(null),
    findSessionHome: () => Effect.succeed(null),
  }),
);
