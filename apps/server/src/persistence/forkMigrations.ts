/**
 * Personal-fork schema changes, kept out of upstream's migration ledger.
 *
 * Upstream's migrator compares ids only, so a fork migration recorded in
 * `effect_sql_migrations` at an id upstream later assigns masks upstream's
 * migration forever (docs/internals/legacy-orchestration-migration.md). Earlier
 * fork builds did exactly that at ids 35 and 52. Fork schema changes are
 * therefore recorded in their own ledger, `fork_sql_migrations`.
 *
 * `reconcileForkMigrationLedger` runs before upstream's migrator. For each id a
 * fork build claimed, it applies upstream's migration at that id (idempotently,
 * the fork build may have applied it already), renames the ledger row to
 * upstream's name and moves the fork's record into the fork ledger. Thread
 * data and conversation events are never rewritten.
 */
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";

const ensureForkLedger = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE IF NOT EXISTS fork_sql_migrations (
      migration_id INTEGER PRIMARY KEY NOT NULL,
      name TEXT NOT NULL,
      created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
  `;
});

const addColumnIfMissing = (table: string, column: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const columns = yield* sql<{ readonly name: string }>`
      SELECT name FROM pragma_table_info(${table})
    `;
    if (!columns.some((row) => row.name === column)) {
      yield* sql.unsafe(`ALTER TABLE ${table} ADD COLUMN ${column} TEXT`);
    }
  });

interface ClaimedUpstreamId {
  readonly upstreamId: number;
  readonly upstreamName: string;
  readonly forkName: string;
  readonly forkId: number;
  // Upstream's migration at this id, made idempotent.
  readonly applyUpstream: Effect.Effect<unknown, SqlError.SqlError, SqlClient.SqlClient>;
}

const CLAIMED_UPSTREAM_IDS: ReadonlyArray<ClaimedUpstreamId> = [
  {
    upstreamId: 35,
    upstreamName: "ProjectionThreadTitleRegeneration",
    forkName: "ProjectionThreadsForkFamily",
    forkId: 1,
    applyUpstream: Effect.all([
      addColumnIfMissing("projection_threads", "title_regeneration_request_id"),
      addColumnIfMissing("projection_threads", "title_regeneration_started_at"),
    ]),
  },
  {
    upstreamId: 52,
    upstreamName: "ProjectionThreadTitleState",
    forkName: "ProjectionThreadsForkCompatibility",
    forkId: 2,
    applyUpstream: addColumnIfMissing("projection_threads", "title_state_json"),
  },
];

export const reconcileForkMigrationLedger = Effect.fn("reconcileForkMigrationLedger")(function* () {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const tables = yield* sql`
          SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'effect_sql_migrations'
        `;
      if (tables.length === 0) return [];
      const history = yield* sql<{ readonly migration_id: number; readonly name: string }>`
          SELECT migration_id, name FROM effect_sql_migrations
        `;
      const executed: Array<readonly [number, string]> = [];
      for (const claim of CLAIMED_UPSTREAM_IDS) {
        const row = history.find(
          (entry) => entry.migration_id === claim.upstreamId && entry.name === claim.forkName,
        );
        if (!row) continue;
        yield* ensureForkLedger;
        yield* claim.applyUpstream;
        yield* sql`
            UPDATE effect_sql_migrations SET name = ${claim.upstreamName}
            WHERE migration_id = ${claim.upstreamId} AND name = ${claim.forkName}
          `;
        yield* sql`
            INSERT OR IGNORE INTO fork_sql_migrations (migration_id, name)
            VALUES (${claim.forkId}, ${claim.forkName})
          `;
        executed.push([claim.upstreamId, claim.upstreamName]);
      }
      return executed;
    }),
  );
});

/**
 * Fork schema every database gets after upstream's migrations, recorded in the
 * fork ledger: `projection_threads.fork_family_id`, which the v1 → v2
 * migration reads (orchestration-v2/legacy/LegacyV1ForkCarryover.ts).
 */
export const runForkSchemaMigrations = Effect.fn("runForkSchemaMigrations")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const threads = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'projection_threads'
  `;
  if (threads.length === 0) return;
  yield* ensureForkLedger;
  const recorded = yield* sql<{ readonly migration_id: number }>`
    SELECT migration_id FROM fork_sql_migrations WHERE migration_id = 1
  `;
  if (recorded.length > 0) return;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      yield* addColumnIfMissing("projection_threads", "fork_family_id");
      yield* sql`
        INSERT OR IGNORE INTO fork_sql_migrations (migration_id, name)
        VALUES (1, 'ProjectionThreadsForkFamily')
      `;
    }),
  );
});
