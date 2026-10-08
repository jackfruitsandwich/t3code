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

export const CLAUDE_BRIDGE_PROTOCOL_VERSION = 1;
export const CLAUDE_BRIDGE_MOD_VERSION = "0.1.2";
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
  | {
      readonly type: "approval";
      readonly toolUseId: string;
      readonly decision: "allow" | "deny" | "ask";
      readonly reason?: string;
    }
  | { readonly type: "interrupt"; readonly turnId?: string };

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
        // Typed prompts come through the mod's turn.start; only tool results
        // carry anything the adapter renders.
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
