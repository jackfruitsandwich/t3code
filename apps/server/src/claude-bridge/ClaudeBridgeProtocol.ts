/**
 * ClaudeBridgeProtocol - the file protocol between a running Claude Code
 * session (through the t3bridge mod, ClaudeBridgePlugin.ts) and this server,
 * and the pure translation of transcript rows and mod events into the Agent
 * SDK frames ClaudeAdapterV2 already understands.
 *
 * One directory per process of a session:
 * `<bridge dir>/claude/<session id>/<instance id>/`
 * - `session.json`: the mod's heartbeat and what it knows about the process.
 * - `inbox.json`: commands this server writes; `inbox-cursor.json` is the
 *   mod's progress through them.
 * - `events/<seq>.json`: one event per file from the mod; this server deletes
 *   each one it has consumed.
 * - `t3.json`: this server's heartbeat while it is attached.
 *
 * A process is live while its heartbeat is fresh; two live processes of one
 * session are possible (a stale terminal copy), and the mod's stale-tip guard
 * keeps either from forking the conversation.
 */
import type {
  PermissionMode,
  SDKAssistantMessage,
  SDKCompactBoundaryMessage,
  SDKMessage,
  SDKResultMessage,
  SDKSystemMessage,
  SDKUserMessage,
} from "@anthropic-ai/claude-agent-sdk";

import { isRecord, parseJsonlLine } from "../import/sessionImportTypes.ts";

export const CLAUDE_BRIDGE_PROTOCOL_VERSION = 2;
export const CLAUDE_BRIDGE_MOD_VERSION = "0.3.12";
/**
 * The first protocol whose mod runs slash commands, answers questions with
 * their answers and reports the session's own commands. A process still on an
 * older mod keeps working for prompts, approvals and interrupts.
 */
export const CLAUDE_BRIDGE_WINDOW_PROTOCOL = 2;
export const CLAUDE_BRIDGE_PLUGIN_NAME = "t3bridge";
export const CLAUDE_BRIDGE_MARKETPLACE_NAME = "t3-bridge";
/** `plugin@marketplace`, the key Claude's plugin registry files use. */
export const CLAUDE_BRIDGE_PLUGIN_ID = `${CLAUDE_BRIDGE_PLUGIN_NAME}@${CLAUDE_BRIDGE_MARKETPLACE_NAME}`;
/** Environment variable that overrides the bridge directory, for the mod and the server alike. */
export const CLAUDE_BRIDGE_DIR_ENV = "T3CODE_BRIDGE_DIR";
/**
 * Set to "background" in the environment of a process T3 starts itself
 * (`claude --bg`). Such a process draws on a terminal nobody is at: Claude
 * Code reports a terminal surface for it all the same, so the mod is told
 * here that its dialogs are seen by no one until a client attaches.
 */
export const CLAUDE_BRIDGE_LAUNCH_ENV = "T3CODE_BRIDGE_LAUNCH";
/** A heartbeat older than this means the process is gone. */
export const CLAUDE_BRIDGE_LIVE_WINDOW_MS = 12_000;
/** How often this server renews `t3.json` while attached. */
export const CLAUDE_BRIDGE_T3_HEARTBEAT_MS = 3_000;
/**
 * Set on the synthesized `user` frame that opens a turn typed outside T3; the
 * adapter uses it as the continuation message's text. Never present on frames
 * from the real SDK.
 */
export const CLAUDE_BRIDGE_WAKE_DETAIL_KEY = "t3_wake_detail";
/**
 * Set beside it when the turn is one T3 opened itself, to carry a request the
 * session raised while it had no turn of its own (a subagent sent to the
 * background asks after the turn that started it has ended). Its value is
 * who asks. Nobody typed such a turn: the adapter files it as activity.
 */
export const CLAUDE_BRIDGE_WAKE_REQUEST_KEY = "t3_wake_request";
export type ClaudeBridgeRequestTurnBy = "subagent" | "session";
/**
 * Set on the synthesized `assistant` frame that carries a slash command's
 * printed output, so the runner can keep it for the turn or command it
 * belongs to.
 */
export const CLAUDE_BRIDGE_COMMAND_OUTPUT_KEY = "t3_command_output";

export interface ClaudeBridgeActiveTurn {
  readonly turnId: string;
  readonly startedAt: number;
  /** Transcript size when the turn began: rows at or past it belong to the turn. */
  readonly transcriptOffset: number;
  /** Set when a T3 inbox prompt started the turn. */
  readonly commandId?: string;
  readonly promptUuid?: string;
}

/** Something the session is holding for the person's answer, as its mod lists it. */
export interface ClaudeBridgeWaitingRequest {
  readonly toolUseId: string;
  readonly kind: "approval" | "question";
  /** The kind of subagent that asks, when one does. */
  readonly agentType?: string;
}

export interface ClaudeBridgeSessionFile {
  readonly protocol: number;
  readonly sid: string;
  readonly instanceId: string;
  readonly cwd: string;
  readonly configDir: string;
  readonly transcriptPath: string;
  readonly surfaces: ReadonlyArray<string>;
  readonly isInteractive: boolean;
  readonly startedAt: number;
  readonly heartbeatAt: number;
  readonly model: string | null;
  /** The session's live permission mode, once a hook payload has named it. */
  readonly permissionMode: string | null;
  readonly effort: string | null;
  /** Bumped each time the mod rewrites `commands.json`. */
  readonly commandsRev: number;
  /** Whether a person can see this process's own dialogs (a terminal runs or is attached to it). */
  readonly terminalPresent: boolean;
  readonly activeTurn: ClaudeBridgeActiveTurn | null;
  /**
   * What the session is waiting to ask T3 about right now. Events tell an
   * attachment; this tells a T3 that is running with nothing attached (it
   * was restarted, or let the session go) that there is something to attach
   * for. Absent when nothing waits, and from a mod that does not list it.
   */
  readonly waiting?: ReadonlyArray<ClaudeBridgeWaitingRequest>;
  readonly modVersion: string;
  readonly ended: {
    readonly reason: string;
    readonly sessionId: string;
    readonly at: number;
  } | null;
}

export type ClaudeBridgeCommandBody =
  | {
      readonly type: "prompt";
      readonly text: string;
      readonly promptUuid: string;
      /**
       * Files T3 saved for this prompt (an image the person attached): the
       * prompt tells the model where they are, and the session may read
       * exactly these without asking.
       */
      readonly attachments?: ReadonlyArray<string>;
    }
  /** A slash command, run in the session as if the person typed `/name args` there. */
  | {
      readonly type: "command";
      readonly name: string;
      readonly args: string;
      readonly promptUuid: string;
    }
  /** A shell command run in the session without asking the model, as `!` does in the terminal. */
  | { readonly type: "bash"; readonly command: string; readonly promptUuid: string }
  | {
      readonly type: "approval";
      readonly toolUseId: string;
      readonly decision: "allow" | "deny" | "ask";
      readonly reason?: string;
      /** `session`: the same call is not asked again in this process. */
      readonly scope?: "session";
      /**
       * The permission rules that go with an allow for the session: Claude
       * Code's own suggestion for the call (what its dialog's "don't ask
       * again" option would add), applied by Claude Code itself.
       */
      readonly updatedPermissions?: ReadonlyArray<unknown>;
      /**
       * A refusal given through Claude Code's own dialog, where one is open:
       * it refuses the call and ends the turn, as the dialog's "No" does, and
       * leaves the rows that leaves.
       */
      readonly stopTurn?: boolean;
    }
  /** The person's answers to a question Claude asked, keyed by question text. */
  | {
      readonly type: "answer";
      readonly toolUseId: string;
      readonly decision: "answer" | "deny" | "ask";
      readonly answers?: Readonly<Record<string, string>>;
      readonly reason?: string;
    }
  /** One row of the session's `/config`, changed as if the person picked it there. */
  | { readonly type: "config"; readonly key: string; readonly value: string | boolean | number }
  | { readonly type: "interrupt"; readonly turnId?: string };

/**
 * Built-in commands that open a panel in the terminal when typed with no
 * argument. The first group was observed on Claude Code 2.1.296 by running
 * every listed built-in bare in a throwaway session and seeing which never
 * returned; the second group are pickers, sign-in and setup screens by their
 * nature. A panel can be closed only at that terminal, and everything queued
 * behind it waits, so the mod does not run these bare for T3. Any other command
 * found to wait this way is remembered by the mod itself.
 */
export const CLAUDE_BARE_PANEL_COMMANDS: ReadonlyArray<string> = [
  "add-dir",
  "advisor",
  "autocompact",
  "config",
  "effort",
  "fast",
  "help",
  "mcp",
  "model",
  "plugin",
  "skills",
  "status",
  "theme",
  "scroll-speed",
  "usage",
  "permissions",
  "goal",
  "hooks",
  "tasks",
  "workflows",
  "artifacts",
  "resume",
  "rewind",
  "memory",
  "export",
  "copy",
  "sandbox",
  "keybindings",
  "login",
  "logout",
  "ide",
  "bug",
  "feedback",
  "upgrade",
  "privacy-settings",
  "terminal-setup",
  "mobile",
  "desktop",
  "chrome",
  "remote-control",
  "install-github-app",
  "install-slack-app",
  "release-notes",
  "skill-doctor",
];

/** One slash command of a session, as its own typeahead lists it. */
export interface ClaudeBridgeSlashCommand {
  readonly name: string;
  readonly description: string;
  readonly source: string;
}

export type ClaudeBridgeCommand = ClaudeBridgeCommandBody & {
  readonly seq: number;
  readonly id: string;
};

export interface ClaudeBridgeInbox {
  readonly seq: number;
  readonly commands: ReadonlyArray<ClaudeBridgeCommand>;
}

export interface ClaudeBridgeTurnUsage {
  readonly model?: string;
  readonly inputTokens?: number;
  readonly outputTokens?: number;
  readonly cacheReadInputTokens?: number;
  readonly cacheCreationInputTokens?: number;
  readonly costUSD?: number;
}

export type ClaudeBridgeEvent = { readonly seq: number; readonly at: number } & (
  | {
      readonly type: "turn.start";
      readonly turnId: string;
      readonly text: string;
      readonly transcriptOffset: number;
      readonly commandId?: string;
      readonly promptUuid?: string;
    }
  | {
      readonly type: "turn.complete";
      readonly turnId: string;
      readonly reason: "answer" | "aborted" | "refusal" | "error";
      readonly answer: string;
      readonly isAborted: boolean;
      readonly durationMs: number;
      readonly usage?: ClaudeBridgeTurnUsage;
      readonly transcriptOffset: number;
      readonly commandId?: string;
      readonly promptUuid?: string;
    }
  | { readonly type: "prompt.dropped"; readonly commandId: string; readonly reason: string }
  | {
      readonly type: "approval.request";
      readonly toolUseId: string;
      readonly tool: string;
      readonly input: unknown;
      readonly reason?: string;
      readonly turnId?: string;
      /**
       * The call is being asked through Claude Code's own permission request,
       * which takes the answer: its dialog is open in the terminal, where
       * the session has one.
       */
      readonly alsoInTerminal?: boolean;
      /** The rules that dialog's "don't ask again" option would add, as Claude Code suggests them. */
      readonly suggestions?: ReadonlyArray<unknown>;
      /** The kind of subagent whose call this is (`general-purpose`), when a subagent asks. */
      readonly agentType?: string;
    }
  | {
      readonly type: "approval.resolved";
      readonly toolUseId: string;
      readonly decision: string;
      readonly by: string;
    }
  | {
      readonly type: "question.request";
      readonly toolUseId: string;
      readonly questions: unknown;
      readonly turnId?: string;
    }
  | { readonly type: "question.resolved"; readonly toolUseId: string; readonly by: string }
  | {
      readonly type: "config.result";
      readonly commandId: string;
      readonly key: string;
      readonly ok: boolean;
      readonly text: string;
    }
  /** How a slash command T3 asked for ended, when it started no turn of its own. */
  | {
      readonly type: "command.result";
      readonly commandId: string;
      readonly promptUuid?: string;
      readonly ok: boolean;
      readonly startedTurn: boolean;
      readonly waitingInTerminal: boolean;
      readonly text: string;
    }
  | { readonly type: "ended"; readonly reason: string; readonly sessionId: string }
  | { readonly type: "error"; readonly message: string; readonly commandId?: string }
  /**
   * A message Claude finished writing, as the terminal drew it. The record
   * does not keep every one (text written before a tool call in a turn that
   * also thinks is shown and not stored), so the thread takes it from here
   * when the record has none.
   */
  | {
      readonly type: "message.display";
      readonly turnId: string;
      readonly messageId: string;
      readonly text: string;
    }
);

const readNumber = (value: unknown): number | undefined =>
  typeof value === "number" && Number.isFinite(value) ? value : undefined;
const readOptionalString = (value: unknown): string | undefined =>
  typeof value === "string" ? value : undefined;

function parseTurnUsage(usage: Record<string, unknown>): ClaudeBridgeTurnUsage {
  const parsed: {
    model?: string;
    inputTokens?: number;
    outputTokens?: number;
    cacheReadInputTokens?: number;
    cacheCreationInputTokens?: number;
    costUSD?: number;
  } = {};
  if (typeof usage.model === "string") parsed.model = usage.model;
  for (const key of [
    "inputTokens",
    "outputTokens",
    "cacheReadInputTokens",
    "cacheCreationInputTokens",
    "costUSD",
  ] as const) {
    const number = readNumber(usage[key]);
    if (number !== undefined) parsed[key] = number;
  }
  return parsed;
}

export function parseClaudeBridgeSessionFile(value: unknown): ClaudeBridgeSessionFile | null {
  if (!isRecord(value)) return null;
  const sid = readOptionalString(value.sid);
  const instanceId = readOptionalString(value.instanceId);
  const heartbeatAt = readNumber(value.heartbeatAt);
  if (sid === undefined || instanceId === undefined || heartbeatAt === undefined) return null;
  const activeTurn = isRecord(value.activeTurn) ? value.activeTurn : null;
  const parsedTurn: ClaudeBridgeActiveTurn | null =
    activeTurn !== null && typeof activeTurn.turnId === "string"
      ? {
          turnId: activeTurn.turnId,
          startedAt: readNumber(activeTurn.startedAt) ?? heartbeatAt,
          transcriptOffset: readNumber(activeTurn.transcriptOffset) ?? 0,
          ...(typeof activeTurn.commandId === "string" ? { commandId: activeTurn.commandId } : {}),
          ...(typeof activeTurn.promptUuid === "string"
            ? { promptUuid: activeTurn.promptUuid }
            : {}),
        }
      : null;
  const ended = isRecord(value.ended) ? value.ended : null;
  const waiting: Array<ClaudeBridgeWaitingRequest> = [];
  for (const entry of Array.isArray(value.waiting) ? value.waiting : []) {
    if (!isRecord(entry) || typeof entry.toolUseId !== "string") continue;
    waiting.push({
      toolUseId: entry.toolUseId,
      kind: entry.kind === "question" ? "question" : "approval",
      ...(typeof entry.agentType === "string" ? { agentType: entry.agentType } : {}),
    });
  }
  return {
    protocol: readNumber(value.protocol) ?? 0,
    sid,
    instanceId,
    cwd: readOptionalString(value.cwd) ?? "",
    configDir: readOptionalString(value.configDir) ?? "",
    transcriptPath: readOptionalString(value.transcriptPath) ?? "",
    surfaces: Array.isArray(value.surfaces)
      ? value.surfaces.filter((entry): entry is string => typeof entry === "string")
      : [],
    isInteractive: value.isInteractive === true,
    startedAt: readNumber(value.startedAt) ?? heartbeatAt,
    heartbeatAt,
    model: readOptionalString(value.model) ?? null,
    permissionMode: readOptionalString(value.permissionMode) ?? null,
    effort: readOptionalString(value.effort) ?? null,
    commandsRev: readNumber(value.commandsRev) ?? 0,
    // An older mod does not say; its interactive flag is the best reading.
    terminalPresent:
      typeof value.terminalPresent === "boolean"
        ? value.terminalPresent
        : value.isInteractive === true,
    activeTurn: parsedTurn,
    ...(waiting.length === 0 ? {} : { waiting }),
    modVersion: readOptionalString(value.modVersion) ?? "",
    ended:
      ended === null
        ? null
        : {
            reason: readOptionalString(ended.reason) ?? "other",
            sessionId: readOptionalString(ended.sessionId) ?? sid,
            at: readNumber(ended.at) ?? heartbeatAt,
          },
  };
}

export function isClaudeBridgeSessionLive(session: ClaudeBridgeSessionFile, now: number): boolean {
  return session.ended === null && now - session.heartbeatAt < CLAUDE_BRIDGE_LIVE_WINDOW_MS;
}

export function parseClaudeBridgeEvent(value: unknown): ClaudeBridgeEvent | null {
  if (!isRecord(value)) return null;
  const seq = readNumber(value.seq);
  const at = readNumber(value.at);
  if (seq === undefined || at === undefined || typeof value.type !== "string") return null;
  const base = { seq, at };
  switch (value.type) {
    case "turn.start": {
      if (typeof value.turnId !== "string") return null;
      return {
        ...base,
        type: "turn.start",
        turnId: value.turnId,
        text: readOptionalString(value.text) ?? "",
        transcriptOffset: readNumber(value.transcriptOffset) ?? 0,
        ...(typeof value.commandId === "string" ? { commandId: value.commandId } : {}),
        ...(typeof value.promptUuid === "string" ? { promptUuid: value.promptUuid } : {}),
      };
    }
    case "turn.complete": {
      if (typeof value.turnId !== "string") return null;
      const reason = value.reason;
      const usage = isRecord(value.usage) ? parseTurnUsage(value.usage) : undefined;
      return {
        ...base,
        type: "turn.complete",
        turnId: value.turnId,
        reason:
          reason === "aborted" || reason === "refusal" || reason === "error" ? reason : "answer",
        answer: readOptionalString(value.answer) ?? "",
        isAborted: value.isAborted === true,
        durationMs: readNumber(value.durationMs) ?? 0,
        ...(usage === undefined ? {} : { usage }),
        transcriptOffset: readNumber(value.transcriptOffset) ?? 0,
        ...(typeof value.commandId === "string" ? { commandId: value.commandId } : {}),
        ...(typeof value.promptUuid === "string" ? { promptUuid: value.promptUuid } : {}),
      };
    }
    case "prompt.dropped":
      if (typeof value.commandId !== "string") return null;
      return {
        ...base,
        type: "prompt.dropped",
        commandId: value.commandId,
        reason: readOptionalString(value.reason) ?? "dropped",
      };
    case "approval.request":
      if (typeof value.toolUseId !== "string" || typeof value.tool !== "string") return null;
      return {
        ...base,
        type: "approval.request",
        toolUseId: value.toolUseId,
        tool: value.tool,
        input: value.input,
        ...(typeof value.reason === "string" ? { reason: value.reason } : {}),
        ...(typeof value.turnId === "string" ? { turnId: value.turnId } : {}),
        ...(value.alsoInTerminal === true ? { alsoInTerminal: true } : {}),
        ...(Array.isArray(value.suggestions) && value.suggestions.length > 0
          ? { suggestions: value.suggestions }
          : {}),
        ...(typeof value.agentType === "string" && value.agentType.length > 0
          ? { agentType: value.agentType }
          : {}),
      };
    case "approval.resolved":
      if (typeof value.toolUseId !== "string") return null;
      return {
        ...base,
        type: "approval.resolved",
        toolUseId: value.toolUseId,
        decision: readOptionalString(value.decision) ?? "ask",
        by: readOptionalString(value.by) ?? "unknown",
      };
    case "question.request":
      if (typeof value.toolUseId !== "string") return null;
      return {
        ...base,
        type: "question.request",
        toolUseId: value.toolUseId,
        questions: value.questions,
        ...(typeof value.turnId === "string" ? { turnId: value.turnId } : {}),
      };
    case "question.resolved":
      if (typeof value.toolUseId !== "string") return null;
      return {
        ...base,
        type: "question.resolved",
        toolUseId: value.toolUseId,
        by: readOptionalString(value.by) ?? "unknown",
      };
    case "command.result":
      if (typeof value.commandId !== "string") return null;
      return {
        ...base,
        type: "command.result",
        commandId: value.commandId,
        ...(typeof value.promptUuid === "string" ? { promptUuid: value.promptUuid } : {}),
        ok: value.ok === true,
        startedTurn: value.startedTurn === true,
        waitingInTerminal: value.waitingInTerminal === true,
        text: readOptionalString(value.text) ?? "",
      };
    case "config.result":
      if (typeof value.commandId !== "string") return null;
      return {
        ...base,
        type: "config.result",
        commandId: value.commandId,
        key: readOptionalString(value.key) ?? "",
        ok: value.ok === true,
        text: readOptionalString(value.text) ?? "",
      };
    case "message.display":
      if (typeof value.messageId !== "string" || typeof value.text !== "string") return null;
      return {
        ...base,
        type: "message.display",
        turnId: readOptionalString(value.turnId) ?? "",
        messageId: value.messageId,
        text: value.text,
      };
    case "ended":
      return {
        ...base,
        type: "ended",
        reason: readOptionalString(value.reason) ?? "other",
        sessionId: readOptionalString(value.sessionId) ?? "",
      };
    case "error":
      return {
        ...base,
        type: "error",
        message: readOptionalString(value.message) ?? "unknown error",
        ...(typeof value.commandId === "string" ? { commandId: value.commandId } : {}),
      };
    default:
      return null;
  }
}

export function parseClaudeBridgeSlashCommands(
  value: unknown,
): ReadonlyArray<ClaudeBridgeSlashCommand> | null {
  if (!Array.isArray(value)) return null;
  const commands: Array<ClaudeBridgeSlashCommand> = [];
  for (const entry of value) {
    if (!isRecord(entry) || typeof entry.name !== "string" || entry.name.length === 0) continue;
    commands.push({
      name: entry.name,
      description: readOptionalString(entry.description) ?? "",
      source: readOptionalString(entry.source) ?? "builtin",
    });
  }
  return commands;
}

/**
 * Splits `/name args` the way Claude Code's prompt box does: the name is the
 * first word after the slash, the rest is the arguments as typed. Null for
 * anything that is not one slash command (a path, plain text, `/` alone).
 */
export function parseClaudeSlashCommandText(
  text: string,
): { readonly name: string; readonly args: string } | null {
  const match = /^\/([A-Za-z0-9][A-Za-z0-9:_-]*)(?:[ \t]+([\s\S]*))?$/u.exec(text.trim());
  if (match === null || match[1] === undefined) return null;
  return { name: match[1], args: (match[2] ?? "").trim() };
}

const LOCAL_COMMAND_STDOUT = /^\s*<local-command-stdout>([\s\S]*)<\/local-command-stdout>\s*$/u;
const COMMAND_NAME = /<command-name>\s*(\/[^<\s]+)\s*<\/command-name>/u;
/** What `!` leaves in the record: the command, then what it printed. */
const BASH_INPUT = /^\s*<bash-input>([\s\S]*)<\/bash-input>\s*$/u;
const BASH_OUTPUT =
  /^\s*<bash-stdout>([\s\S]*?)<\/bash-stdout>(?:\s*<bash-stderr>([\s\S]*?)<\/bash-stderr>)?\s*$/u;
/**
 * Whether a permission callback's options say that Claude Code's own dialog
 * for the call is open in a terminal as well: T3 then asks the same thing
 * beside it and never answers in the person's place.
 */
export const claudeBridgeAlsoInTerminal = (options: unknown): boolean =>
  isRecord(options) && options.alsoInTerminal === true;
/**
 * The kind of subagent a permission callback's call belongs to, when a bridged
 * session says a subagent asks. Its calls are kept in a record of its own, so
 * the thread holds no row for them: the request is all T3 shows of the call.
 */
export const claudeBridgeAskingAgent = (options: unknown): string | null =>
  isRecord(options) && typeof options.askedByAgent === "string" ? options.askedByAgent : null;
/**
 * Commands that move a Claude Code session on to another conversation: the
 * terminal then shows that conversation, and it gets a thread of its own. A
 * thread is one conversation and cannot follow, so sent from T3 they are not
 * run; the person is told where to run them.
 */
const CLAUDE_SESSION_MOVING_COMMANDS: ReadonlySet<string> = new Set(["clear", "resume", "branch"]);
/** What T3 says instead of running such a command, or null for any other command. */
export function claudeSessionMovingNotice(name: string): string | null {
  return CLAUDE_SESSION_MOVING_COMMANDS.has(name)
    ? `Not run: /${name} moves this Claude Code session on to another conversation, and this thread cannot follow it yet. Run it in the session's terminal: the conversation it opens shows up here as its own thread. For a fresh conversation from T3, start a new chat.`
    : null;
}
/**
 * The session a record says its conversation continued in, read from the
 * record's last rows. Claude Code sending a session to the background carries
 * the conversation on under a new id and closes the old record with this row;
 * it then keeps the old session out of /resume. Null when the record says no
 * such thing, or was written to again afterwards (the old session resumed).
 */
export function claudeContinuedInSession(recordTail: string): string | null {
  let continuedIn: string | null = null;
  for (const line of recordTail.split("\n")) {
    const row = parseJsonlLine(line);
    if (!isRecord(row)) continue;
    if (row.type === "continued-in" && typeof row.continuedInSessionId === "string") {
      continuedIn = row.continuedInSessionId.length > 0 ? row.continuedInSessionId : null;
    } else if (row.type === "user" || row.type === "assistant") {
      continuedIn = null;
    }
  }
  return continuedIn;
}
/**
 * What T3 says instead of sending a message into a session whose conversation
 * continued under another id: sent there, it would start a second, diverging
 * copy of the conversation in the process that was left behind.
 */
export const claudeSessionContinuedNotice = (continuedIn: string): string =>
  `Not sent: this conversation was sent to the background in its terminal and carries on as Claude Code session ${continuedIn}, which has its own thread here. Send your message in that thread.`;
/** Where T3 says, in a prompt, that it saved a file the person attached (see AttachmentPrompt). */
const SAVED_ATTACHMENT = /\[Attached [^\]\n]*? is saved at: ([^\]\n]+)\]/gu;
/** The paths of the files a prompt from T3 says were saved for it. */
export const claudeBridgeAttachmentPaths = (text: string): ReadonlyArray<string> =>
  Array.from(text.matchAll(SAVED_ATTACHMENT), (match) => match[1]!.trim()).filter(
    (path) => path.startsWith("/") && !path.includes("/../"),
  );
/** The first mod that runs `!` commands sent from T3. */
export const CLAUDE_BRIDGE_BASH_MOD_VERSION = "0.3.4";
/** The first mod that refuses a call through Claude Code's own dialog (`stopTurn`). */
export const CLAUDE_BRIDGE_DIALOG_REFUSAL_MOD_VERSION = "0.3.7";
/** Whether a mod version is at least `wanted` (`major.minor.patch`). */
export function claudeBridgeModAtLeast(
  version: string | null | undefined,
  wanted: string,
): boolean {
  const parts = (text: string) => text.split(".").map((part) => Number.parseInt(part, 10) || 0);
  const [have, want] = [parts(version ?? "0.0.0"), parts(wanted)];
  for (let index = 0; index < 3; index += 1) {
    if ((have[index] ?? 0) !== (want[index] ?? 0)) return (have[index] ?? 0) > (want[index] ?? 0);
  }
  return true;
}
const COMMAND_ROW_HEAD = /^\s*<command-(?:name|message)>/u;
/**
 * How a turn is named when all that is known of its start is its prompt. A
 * turn that follows a `!` command starts with what the command printed; the
 * mod names it by the command, and one that could not is named plainly.
 */
export const claudeTurnPromptLabel = (text: string): string => {
  if (BASH_OUTPUT.test(text)) return "! (shell command)";
  // A slash command that starts a turn (a project command, a skill) is
  // recorded as the markup Claude Code wraps it in; what the person typed,
  // and what the terminal shows, is the line.
  if (!COMMAND_ROW_HEAD.test(text)) return text;
  const name = COMMAND_NAME.exec(text)?.[1];
  if (name === undefined) return text;
  const args = COMMAND_ARGS.exec(text)?.[1]?.trim() ?? "";
  return args.length === 0 ? name : `${name} ${args}`;
};
/** `! command` of a row `!` wrote, else null. */
const bashCommandText = (text: string): string | null => {
  const command = BASH_INPUT.exec(text)?.[1]?.trim();
  return command === undefined || command.length === 0 ? null : `! ${command}`;
};
/** What a `!` command printed, both streams, when the row is its output; else null. */
const bashOutputText = (text: string): string | null => {
  const match = BASH_OUTPUT.exec(text);
  if (match === null) return null;
  return [match[1] ?? "", match[2] ?? ""].filter((part) => part.trim().length > 0).join("\n");
};
const COMMAND_ARGS = /<command-args>([\s\S]*?)<\/command-args>/u;
const ESCAPE = String.fromCharCode(27);
const TERMINAL_STYLING = new RegExp(
  `${ESCAPE}\\[[0-9;?]*[ -/]*[@-~]|${ESCAPE}\\][^${ESCAPE}\\u0007]*(?:\\u0007|${ESCAPE}\\\\)`,
  "gu",
);
/**
 * Command output as the terminal shows it: the record keeps the colour and
 * dimming codes the terminal turns into styling, which are not text.
 */
export const stripTerminalStyling = (text: string): string => text.replace(TERMINAL_STYLING, "");

const rowText = (row: Record<string, unknown>): string | null => {
  if (!isRecord(row.message)) return null;
  const content = row.message.content;
  if (typeof content === "string") return content;
  if (!Array.isArray(content)) return null;
  const parts: Array<string> = [];
  for (const block of content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") {
      parts.push(block.text);
    }
  }
  return parts.length === 0 ? null : parts.join("");
};

/**
 * The text of a row a slash command leaves. A command that feeds the
 * conversation (`/goal`) writes `user` rows; one that only acts locally
 * (`/rename`, `/model`) writes `system` rows of subtype `local_command`.
 */
const commandRowText = (row: Record<string, unknown>): string | null => {
  if (row.type === "user") return rowText(row);
  if (row.type === "system" && row.subtype === "local_command") {
    return typeof row.content === "string" ? row.content : null;
  }
  return null;
};

/** `/name args` of the transcript row Claude Code writes when a slash command runs, else null. */
export function claudeTranscriptCommandText(row: unknown): string | null {
  if (!isRecord(row)) return null;
  const text = commandRowText(row);
  if (text === null) return null;
  const name = COMMAND_NAME.exec(text)?.[1];
  if (name === undefined) return bashCommandText(text);
  const args = COMMAND_ARGS.exec(text)?.[1]?.trim() ?? "";
  return args.length === 0 ? name : `${name} ${args}`;
}

/** Whether the row came into the session through the mod (a prompt or command T3 sent, or the mod's own). */
export function isClaudeBridgeModRow(row: unknown): boolean {
  return (
    isRecord(row) &&
    isRecord(row.origin) &&
    row.origin.kind === "plugin" &&
    row.origin.name === CLAUDE_BRIDGE_PLUGIN_NAME
  );
}

/** What a slash command printed, when the row is Claude Code's record of that, else null. */
export function claudeTranscriptCommandOutput(row: unknown): string | null {
  if (!isRecord(row)) return null;
  const text = commandRowText(row);
  const output =
    text === null ? null : (LOCAL_COMMAND_STDOUT.exec(text)?.[1] ?? bashOutputText(text));
  return output === null ? null : stripTerminalStyling(output);
}

/**
 * The command the mod runs in a session to load a newer version of itself.
 * That is the bridge's own upkeep, not something the person did, so its rows
 * are not shown. (When the person sends the same command from T3, T3 shows
 * what they sent and what came back by its own means.)
 */
export const CLAUDE_BRIDGE_UPKEEP_COMMAND = "/reload-plugins";

/** The session's permission mode as a transcript bookkeeping row records a change of it. */
export function claudeTranscriptPermissionMode(row: unknown): string | null {
  if (!isRecord(row) || row.type !== "permission-mode") return null;
  return typeof row.permissionMode === "string" ? row.permissionMode : null;
}

export function isClaudeBridgeCommandOutputFrame(message: SDKMessage): boolean {
  return Reflect.get(message, CLAUDE_BRIDGE_COMMAND_OUTPUT_KEY) === true;
}

/**
 * The name `/config`'s model row takes for a model id T3 names in full
 * (`claude-opus-5-5[1m]` → `opus[1m]`), or null when the id names no family
 * that row offers.
 */
export function claudeConfigModelChoice(modelId: string): string | null {
  const family = /(opus|sonnet|haiku|fable)/u.exec(modelId.toLowerCase())?.[1];
  if (family === undefined) return null;
  return /\[1m\]\s*$/u.test(modelId) ? `${family}[1m]` : family;
}

/** A session's permission mode as the two settings a T3 thread shows it with. */
export function threadModesForClaudePermissionMode(mode: string): {
  readonly runtimeMode?: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
  readonly interactionMode: "default" | "plan";
} | null {
  switch (mode) {
    case "plan":
      return { interactionMode: "plan" };
    case "default":
      return { runtimeMode: "approval-required", interactionMode: "default" };
    case "acceptEdits":
      return { runtimeMode: "auto-accept-edits", interactionMode: "default" };
    case "auto":
      return { runtimeMode: "auto", interactionMode: "default" };
    case "bypassPermissions":
      return { runtimeMode: "full-access", interactionMode: "default" };
    default:
      return null;
  }
}

/**
 * A session's model and permission mode as a T3 thread shows them. The
 * thread follows the session, never the reverse; what the session does not
 * say is left out.
 */
export function threadSettingsForClaudeSession(session: {
  readonly model: string | null;
  readonly permissionMode: string | null;
}): {
  readonly model?: string;
  readonly contextWindow?: "1m";
  readonly runtimeMode?: "approval-required" | "auto-accept-edits" | "auto" | "full-access";
  readonly interactionMode?: "default" | "plan";
} {
  const model = session.model === null ? "" : session.model.replace(/\[[^\]]*\]\s*$/u, "").trim();
  const modes =
    session.permissionMode === null
      ? null
      : threadModesForClaudePermissionMode(session.permissionMode);
  return {
    ...(model.length === 0 ? {} : { model }),
    ...(session.model !== null && /\[1m\]\s*$/u.test(session.model)
      ? { contextWindow: "1m" as const }
      : {}),
    ...(modes?.runtimeMode === undefined ? {} : { runtimeMode: modes.runtimeMode }),
    ...(modes === null ? {} : { interactionMode: modes.interactionMode }),
  };
}

/**
 * Whether a turn's prompt came from the Claude Code harness rather than the
 * person: task-completion notices, hook feedback, interruptions.
 */
export function isClaudeHarnessPrompt(text: string): boolean {
  const head = text.trimStart();
  return (
    head.startsWith("<task-notification>") ||
    head.startsWith("<system-reminder>") ||
    head.startsWith("Stop hook feedback:") ||
    head.startsWith("[Request interrupted")
  );
}

/** The continuation text a bridge `user` frame carries, if any. */
export function claudeBridgeWakeDetail(message: SDKMessage): string | null {
  const detail = Reflect.get(message, CLAUDE_BRIDGE_WAKE_DETAIL_KEY);
  return typeof detail === "string" && detail.length > 0 ? detail : null;
}

/** Who asks, when a bridge `user` frame opens a turn T3 made to carry a request; null for any other frame. */
export function claudeBridgeWakeRequest(message: SDKMessage): ClaudeBridgeRequestTurnBy | null {
  const by = Reflect.get(message, CLAUDE_BRIDGE_WAKE_REQUEST_KEY);
  return by === "subagent" || by === "session" ? by : null;
}

/**
 * What the thread says when a turn is opened to carry a request: who is
 * asking. `agentType` is the kind of subagent as Claude Code names it.
 */
export function claudeBridgeRequestTurnText(input: {
  readonly agentType?: string | undefined;
  readonly question?: boolean;
}): string {
  const who =
    input.agentType === undefined
      ? "Claude Code"
      : input.agentType === "subagent"
        ? "A subagent"
        : `The ${input.agentType} subagent`;
  return `${who} is ${input.question === true ? "asking a question" : "asking for permission"}`;
}

// ---------------------------------------------------------------------------
// Transcript rows and mod events -> SDK frames
// ---------------------------------------------------------------------------

type SdkUuid = SDKAssistantMessage["uuid"];
const asUuid = (value: string): SdkUuid => value as SdkUuid;

export interface ClaudeBridgeFrameTranslatorOptions {
  readonly sessionId: string;
  readonly cwd: string;
  readonly model: string;
  readonly permissionMode: PermissionMode;
}

/**
 * A turn's first row is written a moment either side of the instant the turn
 * is reported. A row stamped longer than this before a turn began is not that
 * turn's: the importer takes it as history, and the attachment leaves it out.
 */
export const CLAUDE_TURN_IN_FLIGHT_MARGIN_MS = 2_000;

interface TranslatorTurn {
  readonly turnId: string;
  /** When the turn began, as the session reported it; 0 when nothing said. */
  readonly startedAt: number;
  /** The T3 prompt this turn answers, or null for a turn started elsewhere. */
  readonly promptUuid: string | null;
  assistantStamped: boolean;
  assistantMessages: number;
  lastUsage: Record<string, unknown> | null;
  lastModel: string | null;
}

export interface ClaudeBridgeFrameTranslator {
  /**
   * Frames for a turn that began: a wake `user` frame for a foreign turn, then
   * `init`. `request` marks a turn T3 opens itself to carry a request, with
   * who asks.
   */
  readonly onTurnStart: (
    event: Extract<ClaudeBridgeEvent, { type: "turn.start" }>,
    options?: { readonly request?: ClaudeBridgeRequestTurnBy },
  ) => ReadonlyArray<SDKMessage>;
  /** Frames for one transcript line, already parsed from JSON. */
  readonly onTranscriptRow: (row: unknown) => ReadonlyArray<SDKMessage>;
  /**
   * Whether a row was written before the open turn began. A record can gain
   * such rows after the turn's starting point: Claude Code continuing a
   * session under a new id (sent to the background, say) copies the old
   * conversation into the new record once the first turn is already running.
   * They are history, never this turn's output.
   */
  readonly isFromBeforeTurn: (row: unknown) => boolean;
  /** The `result` frame closing a turn. */
  readonly onTurnComplete: (
    event: Extract<ClaudeBridgeEvent, { type: "turn.complete" }>,
  ) => ReadonlyArray<SDKMessage>;
  /** A `result` for a turn this server interrupted and the mod never reported. */
  readonly onEnded: (reason: string) => ReadonlyArray<SDKMessage>;
  /**
   * Takes over a turn already running when this server attached. No frames:
   * the adapter's continuation turn is already open for them, and a second
   * `init` would request another continuation.
   */
  readonly adoptTurn: (turn: ClaudeBridgeActiveTurn) => void;
  readonly setModel: (model: string) => void;
  readonly hasOpenTurn: () => boolean;
}

const zeroUsage = (): SDKResultMessage["usage"] =>
  ({
    input_tokens: 0,
    output_tokens: 0,
    cache_creation_input_tokens: 0,
    cache_read_input_tokens: 0,
    cache_creation: { ephemeral_1h_input_tokens: 0, ephemeral_5m_input_tokens: 0 },
    server_tool_use: { web_search_requests: 0, web_fetch_requests: 0 },
    service_tier: "standard",
  }) as unknown as SDKResultMessage["usage"];

const usageFromAssistant = (usage: Record<string, unknown> | null): SDKResultMessage["usage"] => {
  if (usage === null) return zeroUsage();
  const number = (key: string) => readNumber(usage[key]) ?? 0;
  return {
    ...zeroUsage(),
    ...usage,
    input_tokens: number("input_tokens"),
    output_tokens: number("output_tokens"),
    cache_creation_input_tokens: number("cache_creation_input_tokens"),
    cache_read_input_tokens: number("cache_read_input_tokens"),
  } as unknown as SDKResultMessage["usage"];
};

const hasToolResultBlock = (content: unknown): boolean =>
  Array.isArray(content) &&
  content.some((block) => isRecord(block) && block.type === "tool_result");

export function makeClaudeBridgeFrameTranslator(
  options: ClaudeBridgeFrameTranslatorOptions,
): ClaudeBridgeFrameTranslator {
  let model = options.model;
  let turn: TranslatorTurn | null = null;
  let costState: { totalCostUSD: number; modelUsage: Record<string, unknown> } | null = null;
  const sessionId = options.sessionId;

  const initFrame = (turnId: string): SDKSystemMessage =>
    ({
      type: "system",
      subtype: "init",
      apiKeySource: "none",
      claude_code_version: "t3bridge",
      cwd: options.cwd,
      tools: [],
      mcp_servers: [],
      model,
      permissionMode: options.permissionMode,
      slash_commands: [],
      output_style: "default",
      skills: [],
      plugins: [],
      uuid: asUuid(`bridge-init-${turnId}`),
      session_id: sessionId,
    }) as SDKSystemMessage;

  const resultFrame = (input: {
    readonly turnId: string;
    readonly reason: "answer" | "aborted" | "refusal" | "error";
    readonly answer: string;
    readonly isAborted: boolean;
    readonly durationMs: number;
  }): SDKResultMessage => {
    const current = turn;
    const promptUuid = current?.promptUuid ?? null;
    const usage = usageFromAssistant(current?.lastUsage ?? null);
    const shared = {
      duration_ms: input.durationMs,
      duration_api_ms: input.durationMs,
      num_turns: Math.max(1, current?.assistantMessages ?? 0),
      total_cost_usd: costState?.totalCostUSD ?? 0,
      usage,
      modelUsage: (costState?.modelUsage ?? {}) as SDKResultMessage["modelUsage"],
      permission_denials: [],
      uuid: asUuid(`bridge-result-${input.turnId}`),
      session_id: sessionId,
      // A turn T3 did not prompt echoes a marker of its own so the adapter never
      // takes its result for the pending prompt's.
      user_message_uuid: promptUuid ?? `bridge:${input.turnId}`,
      user_message_uuids: [promptUuid ?? `bridge:${input.turnId}`],
      ...(input.isAborted ? { terminal_reason: "aborted_streaming" as const } : {}),
    };
    if (input.reason === "error") {
      return {
        ...shared,
        type: "result",
        subtype: "error_during_execution",
        is_error: true,
        stop_reason: null,
        errors: [input.answer.length > 0 ? input.answer : "Claude Code reported an error."],
      } as SDKResultMessage;
    }
    return {
      ...shared,
      type: "result",
      subtype: "success",
      is_error: input.reason === "refusal",
      result: input.answer,
      stop_reason: input.isAborted ? null : "end_turn",
    } as SDKResultMessage;
  };

  const isFromBeforeTurn = (row: unknown): boolean => {
    if (turn === null || !isRecord(row) || typeof row.timestamp !== "string") return false;
    const at = Date.parse(row.timestamp);
    return !Number.isNaN(at) && at < turn.startedAt - CLAUDE_TURN_IN_FLIGHT_MARGIN_MS;
  };

  return {
    onTurnStart: (event, turnOptions) => {
      turn = {
        turnId: event.turnId,
        startedAt: event.at,
        promptUuid: event.promptUuid ?? null,
        assistantStamped: false,
        assistantMessages: 0,
        lastUsage: null,
        lastModel: null,
      };
      const frames: Array<SDKMessage> = [];
      if (event.promptUuid === undefined) {
        // Typed in the terminal: shown as the terminal shows the line.
        const typed = claudeTurnPromptLabel(event.text);
        const wakeUser = {
          type: "user",
          message: { role: "user", content: typed },
          parent_tool_use_id: null,
          uuid: asUuid(`bridge-user-${event.turnId}`),
          session_id: sessionId,
          [CLAUDE_BRIDGE_WAKE_DETAIL_KEY]: typed,
          ...(turnOptions?.request === undefined
            ? {}
            : { [CLAUDE_BRIDGE_WAKE_REQUEST_KEY]: turnOptions.request }),
        } as unknown as SDKUserMessage;
        frames.push(wakeUser);
      }
      frames.push(initFrame(event.turnId));
      return frames;
    },
    isFromBeforeTurn,
    onTranscriptRow: (row) => {
      if (!isRecord(row) || isFromBeforeTurn(row)) return [];
      if (row.type === "cost-state") {
        costState = {
          totalCostUSD: readNumber(row.totalCostUSD) ?? costState?.totalCostUSD ?? 0,
          modelUsage: isRecord(row.modelUsage) ? row.modelUsage : (costState?.modelUsage ?? {}),
        };
        return [];
      }
      if (row.isSidechain === true || typeof row.uuid !== "string") return [];
      if (row.type === "assistant" && isRecord(row.message)) {
        const message = row.message;
        const content = (
          Array.isArray(message.content)
            ? message.content
            : typeof message.content === "string"
              ? [{ type: "text", text: message.content }]
              : []
        )
          .map(claudeNarrationAsText)
          .map(asT3ToolCall);
        const current = turn;
        const stamp =
          current !== null && current.promptUuid !== null && !current.assistantStamped
            ? current.promptUuid
            : null;
        if (current !== null) {
          current.assistantMessages += 1;
          current.assistantStamped = current.assistantStamped || stamp !== null;
          current.lastUsage = isRecord(message.usage) ? message.usage : current.lastUsage;
          current.lastModel = typeof message.model === "string" ? message.model : current.lastModel;
        }
        const frame = {
          type: "assistant",
          message: {
            ...message,
            role: "assistant",
            content,
            ...(typeof message.model === "string" ? {} : { model }),
          },
          parent_tool_use_id: null,
          uuid: asUuid(row.uuid),
          session_id: sessionId,
          ...(typeof row.requestId === "string" ? { request_id: row.requestId } : {}),
          ...(stamp === null ? {} : { user_message_uuid: stamp, user_message_uuids: [stamp] }),
        } as unknown as SDKAssistantMessage;
        return [frame];
      }
      {
        // What a slash command printed. Claude Code reports it to an SDK host
        // as a synthetic assistant message; the same frame here shows the
        // output and feeds the adapter's goal reader ("Goal set: ...").
        const stdout = claudeTranscriptCommandOutput(row);
        if (stdout !== null) {
          if (stdout.trim().length === 0) return [];
          const frame = {
            type: "assistant",
            message: {
              id: `bridge-stdout-${row.uuid}`,
              type: "message",
              role: "assistant",
              model: "<synthetic>",
              content: [{ type: "text", text: stdout }],
              stop_reason: "stop_sequence",
              stop_sequence: "",
              usage: zeroUsage(),
            },
            parent_tool_use_id: null,
            uuid: asUuid(row.uuid),
            session_id: sessionId,
            [CLAUDE_BRIDGE_COMMAND_OUTPUT_KEY]: true,
          } as unknown as SDKAssistantMessage;
          return [frame];
        }
      }
      if (row.type === "user" && isRecord(row.message)) {
        const text = rowText(row);
        // A goal's unmet check comes back as hook feedback; the adapter counts it.
        if (row.isMeta === true && text !== null && text.startsWith("Stop hook feedback:")) {
          const frame = {
            type: "user",
            message: { role: "user", content: text },
            parent_tool_use_id: null,
            isSynthetic: true,
            uuid: asUuid(row.uuid),
            session_id: sessionId,
          } as unknown as SDKUserMessage;
          return [frame];
        }
        // Typed prompts come through the mod's turn.start; only tool results
        // carry anything else the adapter renders.
        if (row.isMeta === true || !hasToolResultBlock(row.message.content)) return [];
        const frame = {
          type: "user",
          message: { role: "user", content: row.message.content },
          parent_tool_use_id: null,
          uuid: asUuid(row.uuid),
          session_id: sessionId,
          ...("toolUseResult" in row ? { tool_use_result: row.toolUseResult } : {}),
        } as unknown as SDKUserMessage;
        return [frame];
      }
      if (row.type === "system" && row.subtype === "compact_boundary") {
        const metadata = isRecord(row.compactMetadata) ? row.compactMetadata : {};
        const frame = {
          type: "system",
          subtype: "compact_boundary",
          compact_metadata: {
            trigger: metadata.trigger === "manual" ? "manual" : "auto",
            pre_tokens: readNumber(metadata.preTokens) ?? 0,
            ...(readNumber(metadata.postTokens) === undefined
              ? {}
              : { post_tokens: readNumber(metadata.postTokens) }),
          },
          uuid: asUuid(row.uuid),
          session_id: sessionId,
        } as unknown as SDKCompactBoundaryMessage;
        return [frame];
      }
      return [];
    },
    onTurnComplete: (event) => {
      if (turn === null || turn.turnId !== event.turnId) {
        // A completion for a turn whose start this server never saw (it
        // attached mid-turn): the frame still closes whatever is open.
        turn = {
          turnId: event.turnId,
          startedAt: 0,
          promptUuid: event.promptUuid ?? null,
          assistantStamped: false,
          assistantMessages: 0,
          lastUsage: null,
          lastModel: null,
        };
      }
      if (typeof event.usage?.model === "string") {
        turn.lastModel = event.usage.model;
      }
      const frame = resultFrame({
        turnId: event.turnId,
        reason: event.reason,
        answer: event.answer,
        isAborted: event.isAborted,
        durationMs: event.durationMs,
      });
      turn = null;
      return [frame];
    },
    onEnded: (reason) => {
      if (turn === null) return [];
      const frame = resultFrame({
        turnId: turn.turnId,
        reason: "aborted",
        answer: `Claude Code session ended (${reason}).`,
        isAborted: true,
        durationMs: 0,
      });
      turn = null;
      return [frame];
    },
    adoptTurn: (active) => {
      turn = {
        turnId: active.turnId,
        startedAt: active.startedAt,
        promptUuid: active.promptUuid ?? null,
        assistantStamped: false,
        assistantMessages: 0,
        lastUsage: null,
        lastModel: null,
      };
    },
    setModel: (next) => {
      model = next;
    },
    hasOpenTurn: () => turn !== null,
  };
}

/**
 * The session calls T3's own tools under the mod's name, since the mod is
 * what declares them there (`mcp__t3bridge__html_render`). They are the same
 * tools T3 attaches to a session it starts, so the thread shows them under
 * T3's name for them: every client already knows what to draw for those.
 */
const BRIDGED_T3_TOOL_PREFIX = `mcp__${CLAUDE_BRIDGE_PLUGIN_NAME}__`;
const T3_TOOL_PREFIX = "mcp__t3-code__";
const asT3ToolCall = (block: unknown): unknown =>
  isRecord(block) &&
  block.type === "tool_use" &&
  typeof block.name === "string" &&
  block.name.startsWith(BRIDGED_T3_TOOL_PREFIX)
    ? { ...block, name: T3_TOOL_PREFIX + block.name.slice(BRIDGED_T3_TOOL_PREFIX.length) }
    : block;

/**
 * The length-delimited field `field` of a protobuf message (the last, if it
 * repeats); undefined when it is absent or the bytes are not such a message.
 */
function protobufBytesField(bytes: Uint8Array, field: number): Uint8Array | undefined {
  let found: Uint8Array | undefined;
  let at = 0;
  const varint = (): number | undefined => {
    let value = 0;
    for (let shift = 0; shift <= 63; shift += 7) {
      if (at >= bytes.length) return undefined;
      const byte = bytes[at++]!;
      value += (byte & 0x7f) * 2 ** shift;
      if ((byte & 0x80) === 0) return value;
    }
    return undefined;
  };
  while (at < bytes.length) {
    const tag = varint();
    if (tag === undefined) return undefined;
    const wire = tag % 8;
    if (wire === 0) {
      if (varint() === undefined) return undefined;
    } else if (wire === 1) {
      at += 8;
    } else if (wire === 5) {
      at += 4;
    } else if (wire === 2) {
      const length = varint();
      if (length === undefined || at + length > bytes.length) return undefined;
      if (Math.floor(tag / 8) === field) found = bytes.subarray(at, at + length);
      at += length;
    } else {
      return undefined;
    }
    if (at > bytes.length) return undefined;
  }
  return found;
}

const NARRATION_BLOCK_KIND = "narration";

/**
 * Whether a content block is narration: what the model says to the person
 * between tool calls of a turn it is still thinking in. The API returns it as
 * a thinking block, and the terminal draws it as a message, not as a thought.
 * Claude Code tells the two apart by a label inside the block's signature;
 * this reads the same label, and a block it cannot read is a thought.
 */
export function isClaudeNarrationBlock(block: unknown): boolean {
  if (
    !isRecord(block) ||
    block.type !== "thinking" ||
    typeof block.signature !== "string" ||
    typeof block.thinking !== "string" ||
    block.thinking.trim().length === 0
  ) {
    return false;
  }
  const envelope = protobufBytesField(Buffer.from(block.signature, "base64"), 2);
  const header = envelope === undefined ? undefined : protobufBytesField(envelope, 1);
  const kind = header === undefined ? undefined : protobufBytesField(header, 8);
  return kind !== undefined && Buffer.from(kind).toString("utf8") === NARRATION_BLOCK_KIND;
}

/** A narration block as the text the terminal draws it as; any other block as it is. */
export const claudeNarrationAsText = (block: unknown): unknown =>
  isClaudeNarrationBlock(block) && isRecord(block) ? { type: "text", text: block.thinking } : block;

/** The row Claude Code writes when a turn is over: every row of that turn is before it. */
export function isClaudeTurnEndRow(row: unknown): boolean {
  return isRecord(row) && row.type === "system" && row.subtype === "turn_duration";
}

/**
 * A message the person typed while a turn was running. Claude Code hands it
 * to the model inside that turn; the record holds it as this row and no user
 * row. The harness delivers its own notices the same way, with another origin.
 */
export function isClaudeTypedMidTurnRow(row: unknown): boolean {
  if (!isRecord(row) || row.type !== "attachment" || !isRecord(row.attachment)) return false;
  const attachment = row.attachment;
  return (
    attachment.type === "queued_command" &&
    isRecord(attachment.origin) &&
    attachment.origin.kind === "human"
  );
}

/**
 * Where a turn that began at `sinceMs` starts in a transcript, for a mod that
 * could not say: the byte offset of the first complete row in `bytes` written
 * at or after that moment. `bytes` is the tail of the file from `chunkStart`;
 * `null` when no such row is in it.
 */
export function claudeTranscriptOffsetAt(
  bytes: Uint8Array,
  chunkStart: number,
  sinceMs: number,
): number | null {
  const decoder = new TextDecoder();
  // The chunk may begin mid-row: the first complete row starts after its first newline.
  let lineStart = chunkStart === 0 ? 0 : bytes.indexOf(0x0a) + 1;
  if (chunkStart !== 0 && lineStart === 0) return null;
  while (lineStart < bytes.length) {
    const newline = bytes.indexOf(0x0a, lineStart);
    if (newline === -1) return null;
    const stamp = /"timestamp":"([^"]+)"/u.exec(decoder.decode(bytes.subarray(lineStart, newline)));
    const at = stamp === null ? Number.NaN : Date.parse(stamp[1]!);
    if (!Number.isNaN(at) && at >= sinceMs) return chunkStart + lineStart;
    lineStart = newline + 1;
  }
  return null;
}

/** Parses the complete JSONL lines in `chunk`, returning the rows and the bytes consumed. */
export function readTranscriptRows(chunk: string): {
  readonly rows: ReadonlyArray<unknown>;
  readonly consumedChars: number;
} {
  const lastNewline = chunk.lastIndexOf("\n");
  if (lastNewline === -1) return { rows: [], consumedChars: 0 };
  const complete = chunk.slice(0, lastNewline + 1);
  const rows: Array<unknown> = [];
  for (const line of complete.split("\n")) {
    const parsed = parseJsonlLine(line);
    if (parsed !== undefined) rows.push(parsed);
  }
  return { rows, consumedChars: complete.length };
}
