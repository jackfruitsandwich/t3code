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
import type { OrchestrationV2Notification } from "@t3tools/contracts";
import { claudePromptAsDrawn } from "@t3tools/shared/claudePastedContent";

import {
  claudeBridgeMidTurnWords,
  claudeNarrationAsText,
  claudeTranscriptCommandOutput,
  claudeTranscriptCommandText,
  claudeTranscriptPermissionMode,
  isClaudeBridgeModRow,
  makeClaudePlainCommandReader,
  stripTerminalStyling,
} from "../claude-bridge/ClaudeBridgeProtocol.ts";
import {
  isCommandNoiseText,
  isRecord,
  parseJsonlLine,
  readString,
  splitCompleteJsonlLines,
  truncateSessionTitle,
  type ParsedCliSession,
  type ParsedSessionMessage,
  type ParsedSessionNotice,
} from "./sessionImportTypes.ts";

const FORK_SIGNATURE_LENGTH = 5;
// What a command printed. A skill that runs in the background follows it, in the
// same row, with a tag of its own that names the agent: that part is not text.
const LOCAL_COMMAND_STDOUT =
  /^\s*<local-command-stdout>([\s\S]*?)<\/local-command-stdout>\s*(?:<forked-skill-launch>[\s\S]*)?$/u;

const TASK_NOTICE_HEAD = "<task-notification>";
const NOTICE_SUMMARY = /<summary>([\s\S]*?)<\/summary>/u;
const NOTICE_STATUS = /<status>\s*([A-Za-z_-]+)\s*<\/status>/u;
const NOTICE_FIRST_SENTENCE = /^(.+?[.!?])\s+(?=[A-Z])/u;
const NOTICE_SUMMARY_MAX = 200;
/** What a wake shows when nothing is known of what woke the session. */
const NOTICE_WITHOUT_WORDS = "Background activity updated";
const NOTICE_ENTITIES: Readonly<Record<string, string>> = {
  "&lt;": "<",
  "&gt;": ">",
  "&quot;": '"',
  "&#39;": "'",
  "&apos;": "'",
  "&amp;": "&",
};

/**
 * Whether a prompt's text is a background task reporting back, as the
 * harness writes one: rows from before Claude Code stamped who wrote a
 * prompt are told by how they open.
 */
export const isClaudeTaskNoticeText = (text: string): boolean =>
  text.trimStart().startsWith(TASK_NOTICE_HEAD);

/**
 * The notice a background task's report is, in the report's own words: the
 * line the terminal draws for it (its summary), and how the task ended where
 * the report says. A report with no summary (a copy's note of where it came
 * from) is its first sentence.
 */
export function claudeTaskNotice(text: string): ParsedSessionNotice {
  const status = NOTICE_STATUS.exec(text)?.[1];
  const outcome =
    status === "completed"
      ? "completed"
      : status === "failed"
        ? "failed"
        : status === "stopped" || status === "killed"
          ? "cancelled"
          : "updated";
  const plain = (words: string) =>
    words
      .replace(/&(?:lt|gt|quot|apos|amp|#39);/gu, (entity) => NOTICE_ENTITIES[entity] ?? entity)
      .replace(/\s+/gu, " ")
      .trim();
  const summary = NOTICE_SUMMARY.exec(text)?.[1];
  const body = plain(summary ?? text.replace(/<[^>]*>/gu, " "));
  const said = summary === undefined ? (NOTICE_FIRST_SENTENCE.exec(body)?.[1] ?? body) : body;
  const clipped =
    said.length > NOTICE_SUMMARY_MAX ? `${said.slice(0, NOTICE_SUMMARY_MAX - 1).trimEnd()}…` : said;
  return { summary: clipped.length > 0 ? clipped : NOTICE_WITHOUT_WORDS, outcome };
}

/**
 * The notice a turn the harness opened shows, when what opened it is a
 * background task reporting back: the task's own line, as the terminal draws
 * it. Null for any other prompt.
 */
export function claudeTaskNotification(text: string | null): OrchestrationV2Notification | null {
  if (text === null || !isClaudeTaskNoticeText(text)) return null;
  const notice = claudeTaskNotice(text);
  return { source: { kind: "background_task" }, outcome: notice.outcome, summary: notice.summary };
}

const PEER_MESSAGE = /^\s*<agent-message from="([^"]*)">\n([\s\S]*?)\n?<\/agent-message>\s*$/u;
/** The line Claude Code puts before a subagent's report when it hands one back. */
const HAND_BACK_HEAD = /^\[Subagent hand-back\][^\n]*\n/u;
const PEER_NAME_MAX = 64;

/**
 * The notice a turn shows when another agent's message opened it: the line
 * the terminal draws for one, "Message from @<name>", with what was sent as
 * its detail. A subagent handing back its report sends the report under a
 * line of the harness's, every line indented; the detail is the report as the
 * terminal shows it opened, without either. `sender` is the agent's name
 * where the session gave one; without it the name is the id the message
 * carries, as in the terminal. Null for any other prompt.
 */
export function claudePeerMessageNotification(
  text: string | null,
  sender: string | null,
): OrchestrationV2Notification | null {
  const match = text === null ? null : PEER_MESSAGE.exec(text);
  if (match === null) return null;
  const sent = match[2] ?? "";
  const handBack = HAND_BACK_HEAD.test(sent);
  const lines = sent.replace(HAND_BACK_HEAD, "").split("\n");
  const body = (
    handBack && lines.every((line) => line.startsWith("  "))
      ? lines.map((line) => line.slice(2))
      : lines
  ).join("\n");
  const name = [...(sender ?? match[1] ?? "").replace(/[\p{Cc}\p{Cf}]/gu, "").trim()]
    .slice(0, PEER_NAME_MAX)
    .join("");
  return {
    source: { kind: handBack ? "subagent" : "background_task" },
    outcome: "updated",
    summary: `Message from @${name.length > 0 ? name : "agent"}`,
    ...(body.trim().length === 0 ? {} : { detail: body }),
  };
}

/**
 * Extract the concatenated text-block content from a Claude message
 * `content` value, which is either a plain string or an array of typed
 * blocks. Non-text blocks (tool_use, tool_result, thinking, images) are
 * dropped; an entry whose content has no text block yields "". Narration,
 * which the terminal draws as a message, is text here too.
 */
function extractTextContent(content: unknown): string {
  if (typeof content === "string") {
    return content;
  }
  if (!Array.isArray(content)) {
    return "";
  }
  const parts: string[] = [];
  for (const raw of content) {
    const block = claudeNarrationAsText(raw);
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
  const plainCommand = makeClaudePlainCommandReader();
  let startedAt: string | undefined;
  let entrypoint: string | undefined;
  let permissionMode: string | undefined;
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
    // A command whose rows carry no markup: told by where its rows sit.
    const plain = plainCommand(entry);

    // The record notes each change of permission mode in a row of its own.
    const recordedMode = claudeTranscriptPermissionMode(entry);
    if (recordedMode !== null) {
      permissionMode = recordedMode;
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

    // A message sent from T3 to steer a running turn has no row of its own
    // either: the bridge mod hands it to the model inside that turn, in a
    // row that holds the person's words within Claude Code's own for these.
    const steered = entry.isSidechain === true ? null : claudeBridgeMidTurnWords(entry);
    if (steered !== null) {
      const timestamp = readString(entry.timestamp);
      const text = steered.trim();
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
    // A command is the person's line whoever wrote its row. The bridge mod
    // writes the rows of one sent from T3 that Claude Code would not write
    // itself (a `!` line, `/model`), and what it writes are meta rows.
    const shellInput = claudeTranscriptCommandText(entry)?.startsWith("! ") === true;
    const bridgeCommandRow =
      isClaudeBridgeModRow(entry) &&
      (claudeTranscriptCommandText(entry) !== null ||
        claudeTranscriptCommandOutput(entry) !== null);
    if (entry.isSidechain === true || (entry.isMeta === true && !shellInput && !bridgeCommandRow)) {
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
    const commandText =
      claudeTranscriptCommandText(entry) ??
      (plain !== null && "command" in plain ? plain.command : null);
    // A slash command's output, or what a `!` command printed.
    const localOutput = LOCAL_COMMAND_STDOUT.exec(rawText)?.[1];
    const commandOutput = (
      plain !== null && "output" in plain
        ? plain.output
        : localOutput === undefined
          ? (claudeTranscriptCommandOutput(entry) ?? undefined)
          : stripTerminalStyling(localOutput)
    )?.trim();
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
    // A background task reporting back is the harness's row, which the
    // terminal draws as a notice. The row's own stamp says who wrote it;
    // rows from before the stamp are told by how they open.
    const writtenBy = isRecord(entry.origin) ? entry.origin.kind : undefined;
    const taskNotice =
      entry.type === "user" &&
      commandOutput === undefined &&
      commandText === null &&
      (writtenBy === "task-notification" ||
        (writtenBy === undefined && isClaudeTaskNoticeText(rawText)));
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
      ...(taskNotice ? { notice: claudeTaskNotice(rawText) } : {}),
    });
  }

  const firstUserText =
    messages.find(
      (message) =>
        message.role === "user" &&
        message.notice === undefined &&
        !commandSourceIds.has(message.sourceId) &&
        !isCommandNoiseText(message.text),
    )?.text ?? messages.find((message) => message.role === "user")?.text;
  const title =
    (aiTitle !== undefined ? truncateSessionTitle(aiTitle) : undefined) ??
    (firstUserText !== undefined
      ? truncateSessionTitle(claudePromptAsDrawn(firstUserText))
      : undefined);
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
    ...(permissionMode === undefined ? {} : { permissionMode }),
  };
}
