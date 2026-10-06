import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "./Migrations.ts";

it.layer(NodeSqliteClient.layer({ filename: ":memory:" }))("fork migration ledger", (it) => {
  it.effect("gives a personal-fork database upstream's migrations at ids 35 and 52", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      // A fork install: upstream through 51, with the fork's names at 35 and 52.
      yield* runMigrations({ toMigrationInclusive: 51 });
      yield* sql`ALTER TABLE projection_threads ADD COLUMN fork_family_id TEXT`;
      yield* sql`UPDATE effect_sql_migrations SET name = 'ProjectionThreadsForkFamily'
        WHERE migration_id = 35`;
      yield* sql`INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (52, 'ProjectionThreadsForkCompatibility')`;
      const now = "2026-08-01T00:00:00.000Z";
      yield* sql`INSERT INTO projection_threads (
        thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at,
        fork_family_id
      ) VALUES ('thread-1', 'project-1', 'My original title',
        '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', ${now}, ${now}, 'family-1')`;

      yield* runMigrations();

      const columns = yield* sql<{ readonly name: string }>`PRAGMA table_info(projection_threads)`;
      const names = new Set(columns.map((column) => column.name));
      assert.ok(names.has("title_state_json"));
      assert.ok(names.has("fork_family_id"));
      const ledger = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM effect_sql_migrations WHERE migration_id IN (35, 52)
        ORDER BY migration_id`;
      assert.deepEqual(ledger, [
        { migration_id: 35, name: "ProjectionThreadTitleRegeneration" },
        { migration_id: 52, name: "ProjectionThreadTitleState" },
      ]);
      const fork = yield* sql<{ readonly migration_id: number; readonly name: string }>`
        SELECT migration_id, name FROM fork_sql_migrations ORDER BY migration_id`;
      assert.deepEqual(fork, [
        { migration_id: 1, name: "ProjectionThreadsForkFamily" },
        { migration_id: 2, name: "ProjectionThreadsForkCompatibility" },
      ]);
      const rows = yield* sql`SELECT title, fork_family_id FROM projection_threads`;
      assert.deepEqual(rows, [{ title: "My original title", fork_family_id: "family-1" }]);
      assert.deepEqual(yield* runMigrations(), []);
    }),
  );
});
