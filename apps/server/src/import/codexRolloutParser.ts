/**
 * codexRolloutParser - Pure parser for Codex rollout transcripts.
 *
 * Codex stores sessions as `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`.
 * The first line is a "session_meta" record carrying the durable session id
 * and cwd; conversation text arrives as legacy "user_message"/"agent_message"
 * events or Codex Desktop "item_completed" events. Everything else
 * (response_item, turn_context, token_count, ...) is protocol plumbing and is
 * skipped silently so future line types never break the importer.
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
  type ParsedSessionMessage,
} from "./sessionImportTypes.ts";

const FORK_SIGNATURE_LENGTH = 8;

export function parseCodexRolloutJsonl(content: string): ParsedCliSession {
  const lines = splitCompleteJsonlLines(content);
  const messages: ParsedSessionMessage[] = [];
  const completedMessages: ParsedSessionMessage[] = [];
  let sessionId: string | undefined;
  let cwd: string | undefined;
  let hasProject = false;
  let startedAt: string | undefined;
  let launch: "human" | "machine" | "subagent" = "human";
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
        hasProject = isRecord(payload.git) && Object.keys(payload.git).length > 0;
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
    const completedItem =
      payload.type === "item_completed" && isRecord(payload.item) ? payload.item : undefined;
    const role =
      payload.type === "user_message" || completedItem?.type === "UserMessage"
        ? ("user" as const)
        : payload.type === "agent_message" || completedItem?.type === "AgentMessage"
          ? ("assistant" as const)
          : undefined;
    if (role === undefined) {
      continue;
    }
    const text =
      completedItem !== undefined && Array.isArray(completedItem.content)
        ? completedItem.content
            .map((block) => (isRecord(block) ? (readString(block.text) ?? "") : ""))
            .join("")
            .trim()
        : (readString(payload.message)?.trim() ?? "");
    const timestamp = readString(entry.timestamp);
    if (text.length === 0 || timestamp === undefined) {
      continue;
    }

    startedAt ??= timestamp;
    (completedItem === undefined ? messages : completedMessages).push({
      role,
      sourceId: `line-${lineIndex}`,
      text,
      timestamp,
      lineIndex,
    });
  }

  // Some Codex versions emit both shapes. Prefer the established format so a
  // conversation is never duplicated.
  const parsedMessages = messages.length > 0 ? messages : completedMessages;
  const firstUserText =
    parsedMessages.find((message) => message.role === "user" && !isCommandNoiseText(message.text))
      ?.text ?? parsedMessages.find((message) => message.role === "user")?.text;
  const title = firstUserText !== undefined ? truncateSessionTitle(firstUserText) : undefined;

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
    forkSignature,
  };
}
