/**
 * claudeSessionParser - Pure parser for Claude Code session transcripts.
 *
 * Claude Code stores one JSONL file per session under
 * `<config dir>/projects/<encoded-cwd>/<session uuid>.jsonl`. "user" and
 * "assistant" entries carry the conversation, and slash commands are read the
 * way the terminal shows them (`/name args`, then what they printed), whether
 * they wrote user rows or local system rows; everything else (mode changes,
 * file-history snapshots, attachments, system lines, unknown future types) is
 * skipped silently so a format drift never breaks the importer. Sidechain and
 * meta entries are agent plumbing, not the human conversation, and are
 * skipped too.
 *
 * @module import/claudeSessionParser
 */
import { claudeTranscriptCommandText } from "../claude-bridge/ClaudeBridgeProtocol.ts";
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
const LOCAL_COMMAND_STDOUT = /^\s*<local-command-stdout>([\s\S]*)<\/local-command-stdout>\s*$/u;

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
  let customTitle: string | undefined;
  /** Rows that are slash commands: never a session's title. */
  const commandSourceIds = new Set<string>();
  let bridgeUpkeepOutputDue = false;
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
      if (title !== undefined && customTitle === undefined) {
        aiTitle = title;
      }
      continue;
    }
    // The name the person gave the session with /rename outranks a generated one.
    if (entry.type === "custom-title") {
      const title = readString(entry.customTitle);
      if (title !== undefined) {
        customTitle = title;
        aiTitle = title;
      }
      continue;
    }

    // What the person typed while a turn was running is handed to the model
    // inside that turn and has no user row of its own. The harness delivers
    // its own notices the same way; only the person's are conversation.
    if (entry.type === "attachment" && entry.isSidechain !== true && isRecord(entry.attachment)) {
      const attachment = entry.attachment;
      const timestamp = readString(entry.timestamp);
      const text =
        attachment.type === "queued_command" &&
        isRecord(attachment.origin) &&
        attachment.origin.kind === "human"
          ? extractTextContent(attachment.prompt).trim()
          : "";
      if (timestamp !== undefined && text.length > 0) {
        startedAt ??= timestamp;
        messages.push({
          role: "user",
          sourceId: readString(entry.uuid) ?? `line-${lineIndex}`,
          text,
          timestamp,
          lineIndex,
          midTurn: true,
        });
      }
      continue;
    }

    // A slash command that acts only locally (/rename, /model) leaves system
    // rows; shown the way the terminal shows them, like the ones below.
    const localCommand =
      entry.type === "system" && entry.subtype === "local_command" && entry.isSidechain !== true;
    if (entry.type !== "user" && entry.type !== "assistant" && !localCommand) {
      continue;
    }
    // A compaction summary is context for the model; the terminal does not
    // show it as something the person said.
    if (entry.isCompactSummary === true || entry.isVisibleInTranscriptOnly === true) {
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
    if (timestamp === undefined || (message === undefined && !localCommand)) {
      continue;
    }
    const rawText = (
      localCommand ? (readString(entry.content) ?? "") : extractTextContent(message?.content)
    ).trim();
    if (rawText.length === 0) {
      continue;
    }
    const sourceId = readString(entry.uuid) ?? `line-${lineIndex}`;
    // The terminal shows a slash command as `/name args` and what it printed
    // as output, not as the markup the transcript wraps them in.
    const commandText = claudeTranscriptCommandText(entry);
    const commandOutput = LOCAL_COMMAND_STDOUT.exec(rawText)?.[1]?.trim();
    if (commandText === null && commandOutput === undefined && localCommand) {
      continue;
    }
    if (commandOutput !== undefined && commandOutput.length === 0) {
      continue;
    }
    if (rawText.startsWith("<local-command-caveat>")) {
      continue;
    }
    // The bridge mod loading a newer version of itself is its own upkeep, not
    // something the person did: neither the command nor what it printed.
    const fromBridgeMod =
      isRecord(entry.origin) && entry.origin.kind === "plugin" && entry.origin.name === "t3bridge";
    if (fromBridgeMod && commandText !== null && commandText.startsWith("/reload-plugins")) {
      bridgeUpkeepOutputDue = true;
      continue;
    }
    // What it printed is the row after it, with nothing on it to say who ran the command.
    if (bridgeUpkeepOutputDue && commandOutput !== undefined) {
      bridgeUpkeepOutputDue = false;
      continue;
    }
    if (commandText !== null) commandSourceIds.add(sourceId);

    startedAt ??= timestamp;
    // /compact leaves the typed line as a row of its own and then the
    // command's row: one thing the person did, shown once.
    const previous = messages[messages.length - 1];
    if (
      commandText !== null &&
      previous !== undefined &&
      previous.role === "user" &&
      previous.command !== true &&
      previous.text.trim() === commandText &&
      Math.abs(Date.parse(timestamp) - Date.parse(previous.timestamp)) <= 2_000
    ) {
      messages[messages.length - 1] = { ...previous, command: true };
      commandSourceIds.add(previous.sourceId);
      continue;
    }
    messages.push({
      role:
        commandOutput !== undefined
          ? "assistant"
          : commandText !== null
            ? "user"
            : entry.type === "assistant"
              ? "assistant"
              : "user",
      sourceId,
      text: commandOutput ?? commandText ?? rawText,
      timestamp,
      lineIndex,
      ...(commandOutput !== undefined || commandText !== null ? { command: true as const } : {}),
    });
  }

  const firstUserText =
    messages.find(
      (message) =>
        message.role === "user" &&
        !commandSourceIds.has(message.sourceId) &&
        !isCommandNoiseText(message.text),
    )?.text ?? messages.find((message) => message.role === "user")?.text;
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
