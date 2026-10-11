import * as Context from "effect/Context";
import type * as Effect from "effect/Effect";
import type * as Scope from "effect/Scope";

export interface SessionImporterShape {
  /**
   * Start the background CLI session importer within the provided scope.
   */
  readonly start: () => Effect.Effect<void, never, Scope.Scope>;

  /**
   * Run a single import sweep immediately. Errors and defects are swallowed
   * and logged, mirroring the scheduled sweep. Exposed for tests and manual
   * triggers; the scheduled loop uses the same underlying sweep.
   */
  readonly sweepNow: () => Effect.Effect<void>;

  /**
   * Give a Claude Code session that is running right now, and has no thread
   * yet, its thread at once: the sweep waits for a transcript to go quiet and
   * to hold a full exchange, which hides a new session for its whole first
   * turn. Rows written before `turnOffset` (the transcript size when the turn
   * in flight began) come in as history; the turn itself arrives through the
   * live attachment, its own prompt included when that was written before the
   * size was read. Does nothing when the session already has a thread.
   */
  readonly adoptLiveClaudeSession: (input: {
    readonly sessionId: string;
    readonly transcriptPath: string;
    readonly cwd: string;
    readonly turnOffset: number;
    /**
     * The turn's prompt: it names the thread when the session has no earlier
     * prompt to name it, and tells the turn's own row from history.
     */
    readonly promptText: string;
  }) => Effect.Effect<void>;

  /**
   * Compare a running Claude Code session's record with its thread now and
   * add what the thread does not show, instead of waiting for the sweep to
   * reach the file. Rows of the turn in flight are left to the attachment,
   * and so are a command's while it runs.
   */
  readonly healLiveClaudeSession: (input: {
    readonly sessionId: string;
    readonly transcriptPath: string;
    /**
     * When a turn began that ended with nothing attached and that the run
     * offered for it is still to show from its start: its rows are left to
     * that run. Absent once an attachment has taken the turn, or once it is
     * too old for one to show it.
     */
    readonly turnAwaitingRunSinceMs?: number;
  }) => Effect.Effect<void>;

  /**
   * The session under whose id T3 holds the conversation a Claude Code
   * session is part of: the session itself, or an earlier one whose record
   * says the conversation continued here (a session sent to the background
   * in its terminal carries on under a new id, and its thread goes with it).
   * Null when no thread holds the conversation.
   */
  readonly claudeConversationHolder: (input: {
    readonly sessionId: string;
    readonly transcriptPath: string;
  }) => Effect.Effect<string | null>;

  /**
   * Whether a thread other than the one holding session `from` already holds
   * session `to`, which `from`'s conversation continued in (both were taken
   * in as conversations of their own by an earlier build): each keeps its
   * thread.
   */
  readonly claudeSessionHeldElsewhere: (input: {
    readonly from: string;
    readonly to: string;
  }) => Effect.Effect<boolean>;
}

export class SessionImporter extends Context.Service<SessionImporter, SessionImporterShape>()(
  "t3/import/Services/SessionImporter",
) {}
