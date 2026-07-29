/**
 * codexRolloutParser - Pure parser for Codex rollout transcripts.
 *
 * Codex stores sessions as `~/.codex/sessions/YYYY/MM/DD/rollout-*.jsonl`.
 * The first line is a "session_meta" record carrying the durable session id
 * and cwd; conversation text arrives as "event_msg" lines whose payload type
 * is "user_message" or "agent_message". Everything else (response_item,
 * turn_context, token_count, ...) is protocol plumbing and is skipped
 * silently so future line types never break the importer.
 *
 * @module import/codexRolloutParser
 */
import {
  isRecord,
  parseJsonlLine,
  readString,
  splitCompleteJsonlLines,
  truncateSessionTitle,
  type ParsedCliSession,
  type ParsedSessionMessage,
} from "./sessionImportTypes.ts";

export function parseCodexRolloutJsonl(content: string): ParsedCliSession {
  const lines = splitCompleteJsonlLines(content);
  const messages: ParsedSessionMessage[] = [];
  let sessionId: string | undefined;
  let cwd: string | undefined;
  let startedAt: string | undefined;
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
      }
      continue;
    }

    if (entry.type !== "event_msg" || payload === undefined) {
      continue;
    }
    const role =
      payload.type === "user_message"
        ? ("user" as const)
        : payload.type === "agent_message"
          ? ("assistant" as const)
          : undefined;
    if (role === undefined) {
      continue;
    }
    const text = readString(payload.message)?.trim() ?? "";
    const timestamp = readString(entry.timestamp);
    if (text.length === 0 || timestamp === undefined) {
      continue;
    }

    startedAt ??= timestamp;
    messages.push({
      role,
      sourceId: `line-${lineIndex}`,
      text,
      timestamp,
      lineIndex,
    });
  }

  const firstUserText = messages.find((message) => message.role === "user")?.text;
  const title = firstUserText !== undefined ? truncateSessionTitle(firstUserText) : undefined;

  return {
    sessionId,
    cwd,
    title,
    startedAt,
    messages,
    lineCount: lines.length,
    malformedLineCount,
  };
}
