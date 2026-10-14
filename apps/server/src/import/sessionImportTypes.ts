/**
 * sessionImportTypes - Shared shapes for CLI session transcript parsing.
 *
 * Both the Claude Code and Codex rollout parsers reduce a provider-native
 * JSONL transcript to this provider-agnostic shape so the importer can treat
 * every source session uniformly: text-only messages with original
 * timestamps, plus enough metadata (session id, cwd, title) to place the
 * session in a T3 project/thread.
 *
 * @module import/sessionImportTypes
 */

export interface ParsedSessionMessage {
  readonly role: "user" | "assistant";
  /**
   * Stable per-source identifier for the message: the entry `uuid` for
   * Claude transcripts, the raw line index for Codex rollouts. Used to derive
   * deterministic command/message ids so re-imports dedupe via receipts.
   */
  readonly sourceId: string;
  readonly text: string;
  readonly timestamp: string;
  /** Zero-based raw line index in the source file, for incremental cursors. */
  readonly lineIndex: number;
  /** A slash command or what it printed: shown, but not conversation. */
  readonly command?: true;
  /** Typed while a turn was running and delivered inside it: it started no turn of its own. */
  readonly midTurn?: true;
  /**
   * Set on a row the harness wrote for itself and the terminal draws as a
   * notice, not as the person's line (a background task reporting back):
   * what the notice says, in the row's own words. `text` stays the row's.
   */
  readonly notice?: ParsedSessionNotice;
  /**
   * A prompt the session took back before answering it (Esc put it back in
   * the input box): a later prompt or command goes on from where this one
   * did, and nothing answered it. It is in the record but not in the
   * conversation, and the terminal no longer draws it (Claude).
   */
  readonly withdrawn?: true;
}

export interface ParsedSessionNotice {
  readonly summary: string;
  readonly outcome: "completed" | "failed" | "cancelled" | "updated";
}

/**
 * A row the harness writes between turns and the terminal draws in the
 * conversation as a dim line of its own: the recap after time away, a notice
 * (Remote Control disconnected), a scheduled task firing. Not a message: it
 * is kept apart so titles, turn counts and imports never see it.
 */
export interface ParsedSessionSystemNotice {
  /** The row's uuid. */
  readonly sourceId: string;
  readonly subtype: "away_summary" | "informational" | "scheduled_task_fire";
  /** What the terminal draws, in the row's own words. */
  readonly text: string;
  readonly timestamp: string;
  readonly lineIndex: number;
}

export interface ParsedCliSession {
  readonly sessionId: string | undefined;
  readonly cwd: string | undefined;
  readonly title: string | undefined;
  readonly startedAt: string | undefined;
  readonly messages: ReadonlyArray<ParsedSessionMessage>;
  /**
   * Number of raw lines fully consumed. A trailing line without a newline
   * that fails to parse is treated as an in-flight partial write and is NOT
   * counted, so the incremental cursor re-reads it once it completes.
   */
  readonly lineCount: number;
  /** Lines that failed to parse as JSON (excluding an in-flight tail). */
  readonly malformedLineCount: number;
  /**
   * Whether the source session belongs to a project. Codex rollouts carry
   * git metadata in `session_meta` only for repository-backed sessions;
   * project-less "chats" omit it and group into one shared Chats project.
   * Undefined for providers without the distinction (Claude).
   */
  readonly hasProject?: boolean;
  /**
   * How the session was launched, from markers the provider writes itself:
   * Claude stamps every entry with an `entrypoint` ("cli" for the interactive
   * TUI, "sdk-*" for programmatic runs); Codex marks `exec`/`codex_exec`
   * automation and agent-spawned `subagent` sessions in `session_meta`.
   * Absent marker means human.
   */
  readonly launch?: "human" | "machine" | "subagent";
  /**
   * The app that wrote the session, verbatim from the provider's own stamp
   * (Codex `session_meta.originator`: "Codex Desktop", "codex-tui",
   * "codex_exec"). Undefined when the provider leaves none (Claude).
   */
  readonly originator?: string;
  /**
   * Stable ids from the session's earliest history. Forks copy history, so
   * two sessions sharing any of these ids are branches of one conversation;
   * used to group fork families and to keep machine-launched forks of a
   * human session out of the Automations project.
   */
  readonly forkSignature?: ReadonlyArray<string>;
  /** The permission mode the session's record last names (Claude). */
  readonly permissionMode?: string;
  /** The harness's notices between turns, in record order (Claude). */
  readonly systemNotices?: ReadonlyArray<ParsedSessionSystemNotice>;
}

const MAX_TITLE_LENGTH = 60;

/** Trim and cap a candidate thread title; undefined when nothing remains. */
export function truncateSessionTitle(text: string): string | undefined {
  const trimmed = text.trim().slice(0, MAX_TITLE_LENGTH).trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * Whether a message is CLI slash-command plumbing (`<command-name>/model...`,
 * `<local-command-stdout>`...) rather than something the user said. Sessions
 * that open with one would otherwise all get the same XML-noise title.
 */
export function isCommandNoiseText(text: string): boolean {
  return text.startsWith("<command-") || text.startsWith("<local-command-");
}

/**
 * A session is worth importing only when a human actually conversed in it:
 * at least one user message and one assistant reply after filtering. A slash
 * command and its output are neither: a session that only ran `/login` is not
 * a conversation.
 */
export function isEligibleCliSession(session: ParsedCliSession): boolean {
  return (
    session.messages.some((message) => message.role === "user" && message.command !== true) &&
    session.messages.some((message) => message.role === "assistant" && message.command !== true)
  );
}

/**
 * Split JSONL content into complete lines. The final segment is only
 * included when the content ends with a newline; otherwise it is an
 * in-flight partial write by the CLI and must not be consumed yet.
 */
export function splitCompleteJsonlLines(content: string): ReadonlyArray<string> {
  const segments = content.split("\n");
  // A trailing "\n" yields an empty final segment; without one the final
  // segment is a partial line. Either way the last segment is dropped.
  return segments.slice(0, -1);
}

export function parseJsonlLine(line: string): unknown | undefined {
  const trimmed = line.trim();
  if (trimmed.length === 0) {
    return undefined;
  }
  try {
    return JSON.parse(trimmed) as unknown;
  } catch {
    return undefined;
  }
}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function readString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}
