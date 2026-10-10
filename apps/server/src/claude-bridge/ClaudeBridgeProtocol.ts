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
export const CLAUDE_BRIDGE_MOD_VERSION = "0.2.0";
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
  readonly modVersion: string;
  readonly ended: {
    readonly reason: string;
    readonly sessionId: string;
    readonly at: number;
  } | null;
}

export type ClaudeBridgeCommandBody =
  | { readonly type: "prompt"; readonly text: string; readonly promptUuid: string }
  /** A slash command, run in the session as if the person typed `/name args` there. */
  | {
      readonly type: "command";
      readonly name: string;
      readonly args: string;
      readonly promptUuid: string;
    }
  | {
      readonly type: "approval";
      readonly toolUseId: string;
      readonly decision: "allow" | "deny" | "ask";
      readonly reason?: string;
      /** `session`: the same call is not asked again in this process. */
      readonly scope?: "session";
    }
  /** The person's answers to a question Claude asked, keyed by question text. */
  | {
      readonly type: "answer";
      readonly toolUseId: string;
      readonly decision: "answer" | "deny" | "ask";
      readonly answers?: Readonly<Record<string, string>>;
      readonly reason?: string;
    }
  | { readonly type: "interrupt"; readonly turnId?: string };

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
const COMMAND_ARGS = /<command-args>([\s\S]*?)<\/command-args>/u;

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

/** `/name args` of the transcript row Claude Code writes when a slash command runs, else null. */
export function claudeTranscriptCommandText(row: unknown): string | null {
  if (!isRecord(row) || row.type !== "user") return null;
  const text = rowText(row);
  if (text === null) return null;
  const name = COMMAND_NAME.exec(text)?.[1];
  if (name === undefined) return null;
  const args = COMMAND_ARGS.exec(text)?.[1]?.trim() ?? "";
  return args.length === 0 ? name : `${name} ${args}`;
}

/** The session's permission mode as a transcript bookkeeping row records a change of it. */
export function claudeTranscriptPermissionMode(row: unknown): string | null {
  if (!isRecord(row) || row.type !== "permission-mode") return null;
  return typeof row.permissionMode === "string" ? row.permissionMode : null;
}

export function isClaudeBridgeCommandOutputFrame(message: SDKMessage): boolean {
  return Reflect.get(message, CLAUDE_BRIDGE_COMMAND_OUTPUT_KEY) === true;
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

interface TranslatorTurn {
  readonly turnId: string;
  /** The T3 prompt this turn answers, or null for a turn started elsewhere. */
  readonly promptUuid: string | null;
  assistantStamped: boolean;
  assistantMessages: number;
  lastUsage: Record<string, unknown> | null;
  lastModel: string | null;
}

export interface ClaudeBridgeFrameTranslator {
  /** Frames for a turn that began: a wake `user` frame for a foreign turn, then `init`. */
  readonly onTurnStart: (
    event: Extract<ClaudeBridgeEvent, { type: "turn.start" }>,
  ) => ReadonlyArray<SDKMessage>;
  /** Frames for one transcript line, already parsed from JSON. */
  readonly onTranscriptRow: (row: unknown) => ReadonlyArray<SDKMessage>;
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

  return {
    onTurnStart: (event) => {
      turn = {
        turnId: event.turnId,
        promptUuid: event.promptUuid ?? null,
        assistantStamped: false,
        assistantMessages: 0,
        lastUsage: null,
        lastModel: null,
      };
      const frames: Array<SDKMessage> = [];
      if (event.promptUuid === undefined) {
        const wakeUser = {
          type: "user",
          message: { role: "user", content: event.text },
          parent_tool_use_id: null,
          uuid: asUuid(`bridge-user-${event.turnId}`),
          session_id: sessionId,
          [CLAUDE_BRIDGE_WAKE_DETAIL_KEY]: event.text,
        } as unknown as SDKUserMessage;
        frames.push(wakeUser);
      }
      frames.push(initFrame(event.turnId));
      return frames;
    },
    onTranscriptRow: (row) => {
      if (!isRecord(row)) return [];
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
        const content = Array.isArray(message.content)
          ? message.content
          : typeof message.content === "string"
            ? [{ type: "text", text: message.content }]
            : [];
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
      if (row.type === "user" && isRecord(row.message)) {
        const text = rowText(row);
        // What a slash command printed. Claude Code reports it to an SDK host
        // as a synthetic assistant message; the same frame here shows the
        // output and feeds the adapter's goal reader ("Goal set: ...").
        const stdout = text === null ? null : (LOCAL_COMMAND_STDOUT.exec(text)?.[1] ?? null);
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
