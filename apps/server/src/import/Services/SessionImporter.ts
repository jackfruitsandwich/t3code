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
   * live attachment. Does nothing when the session already has a thread.
   */
  readonly adoptLiveClaudeSession: (input: {
    readonly sessionId: string;
    readonly transcriptPath: string;
    readonly cwd: string;
    readonly turnOffset: number;
    /** Names the thread when the session has no earlier prompt to name it. */
    readonly promptText: string;
  }) => Effect.Effect<void>;
}

export class SessionImporter extends Context.Service<SessionImporter, SessionImporterShape>()(
  "t3/import/Services/SessionImporter",
) {}
