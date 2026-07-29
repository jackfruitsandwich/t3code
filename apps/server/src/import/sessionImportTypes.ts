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
}

const MAX_TITLE_LENGTH = 60;

/** Trim and cap a candidate thread title; undefined when nothing remains. */
export function truncateSessionTitle(text: string): string | undefined {
  const trimmed = text.trim().slice(0, MAX_TITLE_LENGTH).trim();
  return trimmed.length > 0 ? trimmed : undefined;
}

/**
 * A session is worth importing only when a human actually conversed in it:
 * at least one user message and one assistant reply after filtering.
 */
export function isEligibleCliSession(session: ParsedCliSession): boolean {
  return (
    session.messages.some((message) => message.role === "user") &&
    session.messages.some((message) => message.role === "assistant")
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
