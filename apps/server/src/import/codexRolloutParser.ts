/**
 * codexRolloutParser - Pure parser for Codex rollout transcripts.
 *
 * Codex stores sessions as `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`.
 * The first line is a "session_meta" record carrying the durable session id
 * and cwd; conversation text arrives as legacy "user_message"/"agent_message"
 * events or Codex Desktop "item_completed" events. The other items a turn
 * completes (commands, tool calls, file changes, searches, compactions) and
 * the turns the record says were interrupted are read as the items the live
 * adapter shows for them. Everything else (response_item, token_count, ...)
 * is protocol plumbing and is skipped silently so future line types never
 * break the importer.
 *
 * @module import/codexRolloutParser
 */
import {
  isCommandNoiseText,
  isRecord,
  parseJsonlLine,
  readString,
  splitCompleteJsonlLines,
  truncateSessionTitle,
  type ParsedCliSession,
  type ParsedRecordItem,
  type ParsedRecordItemStatus,
  type ParsedSessionGoal,
  type ParsedSessionItem,
  type ParsedSessionMessage,
} from "./sessionImportTypes.ts";

const FORK_SIGNATURE_LENGTH = 8;

/** A completed item's status, as the live adapter maps the same status (`codexItemStatus`). */
const itemStatus = (status: unknown): ParsedRecordItemStatus =>
  status === "failed" ? "failed" : status === "declined" ? "cancelled" : "completed";

/** The app-server's spelling of a tool call's status (`McpToolCallStatus`), ended. */
const toolCallStatus = (status: unknown) => (status === "failed" ? "failed" : "completed");

const SHELL_SAFE = /^[A-Za-z0-9,._+:@%/=-]+$/u;

/** One word as a shell reads it back: how Codex joins a command's words for display. */
const shellQuote = (word: string) => {
  if (word.length === 0) return "''";
  if (SHELL_SAFE.test(word)) return word;
  if (!word.includes("'")) return `'${word}'`;
  if (!/[$`"\\!]/u.test(word)) return `"${word}"`;
  return `'${word.replaceAll("'", `'"'"'`)}'`;
};

const commandText = (command: unknown): string | undefined => {
  if (typeof command === "string") return command.trim() || undefined;
  if (!Array.isArray(command)) return undefined;
  const words = command.filter((word): word is string => typeof word === "string");
  return words.length === 0 ? undefined : words.map(shellQuote).join(" ");
};

const compactStrings = (values: ReadonlyArray<unknown>) =>
  values.flatMap((value) => {
    const trimmed = typeof value === "string" ? value.trim() : "";
    return trimmed.length === 0 ? [] : [trimmed];
  });

/** What a search item searched for, in the order the live adapter lists it. */
const webSearchPatterns = (item: Record<string, unknown>): ReadonlyArray<string> => {
  const action = isRecord(item.action) ? item.action : undefined;
  switch (action?.type) {
    case "search":
      return compactStrings([
        ...(Array.isArray(action.queries) ? action.queries : []),
        action.query,
        item.query,
      ]);
    case "openPage":
    case "open_page":
      return compactStrings([action.url, item.query]);
    case "findInPage":
    case "find_in_page":
      return compactStrings([action.pattern, action.url, item.query]);
    default:
      return compactStrings([item.query]);
  }
};

/**
 * The files a patch changed, each as the app-server presents it: by path,
 * and the added or deleted file's content or the update's diff.
 */
const fileChanges = (changes: unknown) =>
  (isRecord(changes) ? Object.entries(changes) : [])
    .flatMap(([fileName, change]) => {
      if (!isRecord(change) || fileName.trim().length === 0) return [];
      const content = readString(change.unified_diff) ?? readString(change.content) ?? "";
      const movedTo = readString(change.move_path);
      return [
        {
          fileName: fileName.trim(),
          diffStr: movedTo === undefined ? content : `${content}\n\nMoved to: ${movedTo}`,
        },
      ];
    })
    .toSorted((left, right) => (left.fileName < right.fileName ? -1 : 1));

/** An MCP or dynamic tool call as the app-server sends it (`mcpToolCall` / `dynamicToolCall`). */
const toolCall = (item: Record<string, unknown>): Record<string, unknown> | undefined => {
  const id = readString(item.id);
  const tool = readString(item.tool);
  if (id === undefined || tool === undefined) return undefined;
  if (item.type === "McpToolCall") {
    const server = readString(item.server);
    if (server === undefined) return undefined;
    const connectorId = readString(item.connectorId) ?? readString(item.connector_id);
    const error = isRecord(item.error)
      ? { message: readString(item.error.message) ?? "" }
      : typeof item.error === "string"
        ? { message: item.error }
        : null;
    return {
      type: "mcpToolCall",
      id,
      server,
      tool,
      arguments: item.arguments ?? null,
      status: toolCallStatus(item.status),
      result: isRecord(item.result) ? item.result : null,
      error,
      appContext:
        connectorId === undefined
          ? null
          : {
              connectorId,
              linkId: item.linkId ?? item.link_id ?? null,
              resourceUri: item.mcpAppResourceUri ?? item.mcp_app_resource_uri ?? null,
              appName: item.appName ?? item.app_name ?? null,
              actionName: item.actionName ?? item.action_name ?? null,
            },
    };
  }
  return {
    type: "dynamicToolCall",
    id,
    namespace: readString(item.namespace) ?? null,
    tool,
    arguments: item.arguments ?? null,
    status: toolCallStatus(item.status),
    contentItems: Array.isArray(item.content_items)
      ? item.content_items
      : Array.isArray(item.contentItems)
        ? item.contentItems
        : null,
    success: typeof item.success === "boolean" ? item.success : null,
  };
};

/**
 * The items a completed record item shows as. One per changed file for a
 * patch: the record holds every file, and each shows on its own row.
 */
const completedItems = (item: Record<string, unknown>): ReadonlyArray<ParsedRecordItem> => {
  switch (item.type) {
    case "CommandExecution": {
      const input = commandText(item.command);
      if (input === undefined) return [];
      const output = readString(item.aggregated_output);
      return [
        {
          type: "command_execution",
          status: itemStatus(item.status),
          input,
          ...(output === undefined ? {} : { output }),
          ...(typeof item.exit_code === "number" && Number.isInteger(item.exit_code)
            ? { exitCode: item.exit_code }
            : {}),
        },
      ];
    }
    case "FileChange":
      return fileChanges(item.changes).map((change) => ({
        type: "file_change",
        status: itemStatus(item.status),
        ...change,
      }));
    case "McpToolCall":
    case "DynamicToolCall": {
      const call = toolCall(item);
      return call === undefined ? [] : [{ type: "tool_call", call }];
    }
    case "WebSearch":
      return [{ type: "web_search", status: "completed", patterns: webSearchPatterns(item) }];
    case "Extension":
      return item.kind === "web.search"
        ? [{ type: "web_search", status: "completed", patterns: webSearchPatterns(item) }]
        : [];
    case "ContextCompaction":
      return [{ type: "compaction", status: "completed" }];
    default:
      return [];
  }
};

const readGoal = (goal: unknown): ParsedSessionGoal | null => {
  if (!isRecord(goal)) return null;
  const objective = readString(goal.objective)?.trim();
  const status = readString(goal.status);
  if (objective === undefined || objective.length === 0 || status === undefined) return null;
  const count = (value: unknown) =>
    typeof value === "number" && Number.isFinite(value) ? Math.max(0, value) : 0;
  return {
    objective,
    status,
    tokensUsed: count(goal.tokensUsed),
    tokenBudget: typeof goal.tokenBudget === "number" ? goal.tokenBudget : null,
    timeUsedSeconds: count(goal.timeUsedSeconds),
  };
};

export interface CodexRolloutParseOptions {
  /**
   * The record is a later file of a thread (`rollout-…-<thread>_<segment>.jsonl`):
   * Codex went on writing the thread there, from a point of the earlier
   * file. Its rows' ids carry the segment, so they never meet the earlier
   * file's rows on the same lines.
   */
  readonly segmentId?: string;
}

export function parseCodexRolloutJsonl(
  content: string,
  options?: CodexRolloutParseOptions,
): ParsedCliSession {
  const lines = splitCompleteJsonlLines(content);
  const lineId = (lineIndex: number) =>
    options?.segmentId === undefined
      ? `line-${lineIndex}`
      : `segment-${options.segmentId}-line-${lineIndex}`;
  const messages: ParsedSessionMessage[] = [];
  const completedMessages: ParsedSessionMessage[] = [];
  const items: ParsedSessionItem[] = [];
  // A compaction is written as an item, an event, or both: counted once.
  const compactionEvents: ParsedSessionItem[] = [];
  let sessionId: string | undefined;
  let cwd: string | undefined;
  let hasProject = false;
  let startedAt: string | undefined;
  let launch: "human" | "machine" | "subagent" = "human";
  let originator: string | undefined;
  let model: string | undefined;
  let goal: ParsedSessionGoal | null | undefined;
  let turnId: string | undefined;
  let continuesEarlierFile = false;
  const forkSignature: string[] = [];
  let malformedLineCount = 0;

  for (let lineIndex = 0; lineIndex < lines.length; lineIndex += 1) {
    const line = lines[lineIndex] ?? "";
    if (line.trim().length === 0) {
      continue;
    }
    const entry = parseJsonlLine(line);
    if (entry === undefined) {
      malformedLineCount += 1;
      continue;
    }
    if (!isRecord(entry)) {
      continue;
    }
    const payload = isRecord(entry.payload) ? entry.payload : undefined;

    if (entry.type === "session_meta") {
      if (payload !== undefined) {
        sessionId ??= readString(payload.id) ?? readString(payload.session_id);
        cwd ??= readString(payload.cwd);
        startedAt ??= readString(payload.timestamp) ?? readString(entry.timestamp);
        originator ??= readString(payload.originator);
        hasProject = isRecord(payload.git) && Object.keys(payload.git).length > 0;
        continuesEarlierFile ||=
          isRecord(payload.history_base) &&
          typeof payload.history_base.end_ordinal_exclusive === "number" &&
          payload.history_base.end_ordinal_exclusive > 0;
        if (
          payload.thread_source === "subagent" ||
          (isRecord(payload.source) && "subagent" in payload.source)
        ) {
          launch = "subagent";
        } else if (payload.source === "exec" || payload.originator === "codex_exec") {
          launch = "machine";
        }
      }
      continue;
    }

    if (entry.type === "turn_context") {
      turnId = readString(payload?.turn_id) ?? turnId;
      model = readString(payload?.model) ?? model;
      continue;
    }

    // Provider item ids are copied verbatim when Codex forks a session, so
    // the earliest ones identify the conversation across all its branches.
    if (entry.type === "response_item" && forkSignature.length < FORK_SIGNATURE_LENGTH) {
      const itemId = payload === undefined ? undefined : readString(payload.id);
      if (itemId !== undefined) {
        forkSignature.push(itemId);
      }
    }

    if (entry.type !== "event_msg" || payload === undefined) {
      continue;
    }
    const timestamp = readString(entry.timestamp);
    if (payload.type === "task_started") {
      turnId = readString(payload.turn_id) ?? turnId;
      continue;
    }
    if (payload.type === "thread_settings_applied") {
      const settings = isRecord(payload.thread_settings) ? payload.thread_settings : undefined;
      model = readString(settings?.model) ?? model;
      continue;
    }
    if (payload.type === "thread_goal_updated") {
      goal = readGoal(payload.goal);
      continue;
    }
    if (payload.type === "thread_goal_cleared") {
      goal = null;
      continue;
    }
    const rowTurnId = readString(payload.turn_id) ?? turnId;
    const rowFields = (sourceId: string) => ({
      sourceId,
      timestamp: timestamp ?? "",
      lineIndex,
      ...(rowTurnId === undefined ? {} : { nativeTurnId: rowTurnId }),
    });
    if (timestamp !== undefined && payload.type === "turn_aborted") {
      // What ended the turn, in the record's word for it ("interrupted").
      const reason = (readString(payload.reason) ?? "interrupted").replaceAll("_", " ");
      items.push({
        ...rowFields(lineId(lineIndex)),
        item: { type: "system_notice", status: "completed", message: `Turn ${reason}` },
      });
      continue;
    }
    if (timestamp !== undefined && payload.type === "context_compacted") {
      compactionEvents.push({
        ...rowFields(lineId(lineIndex)),
        item: { type: "compaction", status: "completed" },
      });
      continue;
    }
    const completedItem =
      payload.type === "item_completed" && isRecord(payload.item) ? payload.item : undefined;
    const role =
      payload.type === "user_message" || completedItem?.type === "UserMessage"
        ? ("user" as const)
        : payload.type === "agent_message" || completedItem?.type === "AgentMessage"
          ? ("assistant" as const)
          : undefined;
    if (role === undefined) {
      if (completedItem !== undefined && timestamp !== undefined) {
        const nativeId = readString(completedItem.id);
        completedItems(completedItem).forEach((item, index) => {
          items.push({
            ...rowFields(index === 0 ? lineId(lineIndex) : `${lineId(lineIndex)}-${index}`),
            ...(nativeId === undefined ? {} : { nativeId }),
            item,
          });
        });
      }
      continue;
    }
    const text =
      completedItem !== undefined && Array.isArray(completedItem.content)
        ? completedItem.content
            .map((block) => (isRecord(block) ? (readString(block.text) ?? "") : ""))
            .join("")
            .trim()
        : (readString(payload.message)?.trim() ?? "");
    if (text.length === 0 || timestamp === undefined) {
      continue;
    }

    startedAt ??= timestamp;
    const nativeId = completedItem === undefined ? undefined : readString(completedItem.id);
    (completedItem === undefined ? messages : completedMessages).push({
      role,
      text,
      ...rowFields(lineId(lineIndex)),
      ...(nativeId === undefined ? {} : { nativeId }),
    });
  }

  // Some Codex versions emit both shapes. Prefer the established format so a
  // conversation is never duplicated.
  const parsedMessages = messages.length > 0 ? messages : completedMessages;
  const firstUserText =
    parsedMessages.find((message) => message.role === "user" && !isCommandNoiseText(message.text))
      ?.text ?? parsedMessages.find((message) => message.role === "user")?.text;
  const title = firstUserText !== undefined ? truncateSessionTitle(firstUserText) : undefined;
  const parsedItems = items.some((item) => item.item.type === "compaction")
    ? items
    : [...items, ...compactionEvents].toSorted((left, right) => left.lineIndex - right.lineIndex);

  return {
    sessionId,
    cwd,
    title,
    startedAt,
    messages: parsedMessages,
    lineCount: lines.length,
    malformedLineCount,
    hasProject,
    launch,
    ...(originator === undefined ? {} : { originator }),
    // The earliest history is the earlier file's.
    forkSignature: continuesEarlierFile ? [] : forkSignature,
    items: parsedItems,
    ...(continuesEarlierFile ? { continuesEarlierFile: true as const } : {}),
    ...(model === undefined ? {} : { model }),
    ...(goal === undefined ? {} : { goal }),
  };
}

/** A name Codex gave a thread, or the person gave it in Codex. */
export interface CodexThreadName {
  readonly name: string;
  readonly updatedAt: string;
}

/**
 * Codex's own names for its threads, from `CODEX_HOME/session_index.jsonl`
 * (what its thread list and `codex resume` show). The file is append-only:
 * the last line for a thread wins, and a blank name takes the name away.
 */
export function parseCodexSessionIndex(content: string): ReadonlyMap<string, CodexThreadName> {
  const names = new Map<string, CodexThreadName>();
  for (const line of splitCompleteJsonlLines(content)) {
    const entry = parseJsonlLine(line);
    if (!isRecord(entry)) continue;
    const id = readString(entry.id);
    if (id === undefined) continue;
    const name = typeof entry.thread_name === "string" ? entry.thread_name.trim() : "";
    if (name.length === 0) {
      names.delete(id);
      continue;
    }
    names.set(id, { name, updatedAt: readString(entry.updated_at) ?? "" });
  }
  return names;
}
