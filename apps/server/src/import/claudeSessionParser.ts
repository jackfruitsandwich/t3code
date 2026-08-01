/**
 * claudeSessionParser - Pure parser for Claude Code session transcripts.
 *
 * Claude Code stores one JSONL file per session under
 * `<config dir>/projects/<encoded-cwd>/<session uuid>.jsonl`. Only "user" and
 * "assistant" entries carry conversation text; everything else (mode changes,
 * file-history snapshots, attachments, system lines, unknown future types) is
 * skipped silently so a format drift never breaks the importer. Sidechain and
 * meta entries are agent plumbing, not the human conversation, and are
 * skipped too.
 *
 * @module import/claudeSessionParser
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

const FORK_SIGNATURE_LENGTH = 5;

/**
 * Extract the concatenated text-block content from a Claude message
 * `content` value, which is either a plain string or an array of typed
 * blocks. Non-text blocks (tool_use, tool_result, thinking, images) are
 * dropped; an entry whose content has no text block yields "".
 */
function extractTextContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  const parts: string[] = [];
  for (const block of content) {
    if (isRecord(block) && block.type === "text" && typeof block.text === "string") {
      parts.push(block.text);
    }
  }
  return parts.join("\n\n");
}

export function parseClaudeSessionJsonl(content: string): ParsedCliSession {
  const lines = splitCompleteJsonlLines(content);
  const messages: ParsedSessionMessage[] = [];
  let sessionId: string | undefined;
  let cwd: string | undefined;
  let aiTitle: string | undefined;
  let startedAt: string | undefined;
  let entrypoint: string | undefined;
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

    if (entry.type === "ai-title") {
      const title = readString(entry.aiTitle);
      if (title !== undefined) {
        aiTitle = title;
      }
      continue;
    }

    if (entry.type !== "user" && entry.type !== "assistant") {
      continue;
    }
    if (entry.isSidechain === true || entry.isMeta === true) {
      continue;
    }

    sessionId ??= readString(entry.sessionId);
    cwd ??= readString(entry.cwd);
    entrypoint ??= readString(entry.entrypoint);

    const timestamp = readString(entry.timestamp);
    const message = isRecord(entry.message) ? entry.message : undefined;
    if (timestamp === undefined || message === undefined) {
      continue;
    }
    const text = extractTextContent(message.content).trim();
    if (text.length === 0) {
      continue;
    }

    startedAt ??= timestamp;
    messages.push({
      role: entry.type,
      sourceId: readString(entry.uuid) ?? `line-${lineIndex}`,
      text,
      timestamp,
      lineIndex,
    });
  }

  const firstUserText =
    messages.find((message) => message.role === "user" && !isCommandNoiseText(message.text))
      ?.text ?? messages.find((message) => message.role === "user")?.text;
  const title =
    (aiTitle !== undefined ? truncateSessionTitle(aiTitle) : undefined) ??
    (firstUserText !== undefined ? truncateSessionTitle(firstUserText) : undefined);
  // Entry uuids are copied verbatim when Claude forks a session, so the
  // earliest ones identify the conversation across all its branches.
  const forkSignature = messages
    .slice(0, FORK_SIGNATURE_LENGTH)
    .map((message) => message.sourceId)
    .filter((sourceId) => !sourceId.startsWith("line-"));

  return {
    sessionId,
    cwd,
    title,
    startedAt,
    messages,
    lineCount: lines.length,
    malformedLineCount,
    launch: entrypoint === undefined || entrypoint === "cli" ? "human" : "machine",
    forkSignature,
  };
}
