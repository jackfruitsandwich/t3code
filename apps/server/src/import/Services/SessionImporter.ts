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
}

export class SessionImporter extends Context.Service<SessionImporter, SessionImporterShape>()(
  "t3/import/Services/SessionImporter",
) {}
