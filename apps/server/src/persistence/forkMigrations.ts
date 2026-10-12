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
 * Fork schema every database gets after upstream's migrations, each recorded
 * in the fork ledger once it has run:
 *
 * 1. `projection_threads.fork_family_id`, which the v1 → v2 migration reads
 *    (orchestration-v2/legacy/LegacyV1ForkCarryover.ts).
 * 3. An index that answers "when did the person last write in this thread"
 *    without reading a message. The thread list asks that for every thread;
 *    with imported sessions that is thousands of threads, and each answer
 *    read a row of the messages table, which is hundreds of megabytes. Those
 *    reads were two thirds of the list's query, and several seconds of a
 *    blocked server whenever the file was not in memory.
 */
const FORK_SCHEMA_MIGRATIONS: ReadonlyArray<{
  readonly id: number;
  readonly name: string;
  /** The table this migration changes; a database without it is left alone. */
  readonly table: string;
  readonly apply: Effect.Effect<unknown, SqlError.SqlError, SqlClient.SqlClient>;
}> = [
  {
    id: 1,
    name: "ProjectionThreadsForkFamily",
    table: "projection_threads",
    apply: addColumnIfMissing("projection_threads", "fork_family_id"),
  },
  {
    id: 3,
    name: "MessagesLatestUserAuthoredIndex",
    table: "orchestration_v2_projection_messages",
    apply: Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // The WHERE clause is spelled as the thread list's query spells it
      // (ProjectionStore selectShellThreadRows): SQLite uses a partial index
      // only for a query whose own terms match the index's.
      yield* sql`
        CREATE INDEX IF NOT EXISTS orchestration_v2_projection_messages_latest_user_authored_idx
        ON orchestration_v2_projection_messages(thread_id, updated_at DESC, message_id DESC)
        WHERE role = 'user' AND json_extract(payload_json, '$.createdBy') = 'user'
      `;
    }),
  },
];

export const runForkSchemaMigrations = Effect.fn("runForkSchemaMigrations")(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const migration of FORK_SCHEMA_MIGRATIONS) {
    const tables = yield* sql`
      SELECT name FROM sqlite_master WHERE type = 'table' AND name = ${migration.table}
    `;
    if (tables.length === 0) continue;
    yield* ensureForkLedger;
    const recorded = yield* sql<{ readonly migration_id: number }>`
      SELECT migration_id FROM fork_sql_migrations WHERE migration_id = ${migration.id}
    `;
    if (recorded.length > 0) continue;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* migration.apply;
        yield* sql`
          INSERT OR IGNORE INTO fork_sql_migrations (migration_id, name)
          VALUES (${migration.id}, ${migration.name})
        `;
      }),
    );
  }
});
