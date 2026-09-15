import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/unstable/sql/SqlClient";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";

import { runMigrations } from "../Migrations.ts";
import migrateForkFamily from "./035_ProjectionThreadsForkFamily.ts";
import migrateCompatibility from "./052_ProjectionThreadsForkCompatibility.ts";

for (const baseline of ["personal-fork-35", "upstream-51", "fresh"] as const) {
  it.layer(NodeSqliteClient.layerMemory())(`052 compatibility from ${baseline}`, (it) => {
    it.effect("preserves history and supports both title regeneration and fork families", () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: baseline === "upstream-51" ? 51 : 34 });
        if (baseline === "personal-fork-35") {
          yield* migrateForkFamily;
          yield* sql`INSERT INTO effect_sql_migrations (migration_id, name)
            VALUES (35, 'ProjectionThreadsForkFamily')`;
        }
        const now = "2026-08-01T00:00:00.000Z";
        yield* sql`INSERT INTO projection_threads (
          thread_id, project_id, title, model_selection_json, runtime_mode, created_at, updated_at
        ) VALUES ('thread-1', 'project-1', 'My original title',
          '{"instanceId":"codex","model":"gpt-5.4"}', 'full-access', ${now}, ${now})`;
        if (baseline === "personal-fork-35") {
          yield* sql`UPDATE projection_threads SET fork_family_id = 'original-family'
            WHERE thread_id = 'thread-1'`;
        }
        yield* runMigrations();
        const columns = yield* sql<{
          readonly name: string;
        }>`PRAGMA table_info(projection_threads)`;
        const names = new Set(columns.map((column) => column.name));
        assert.ok(names.has("fork_family_id"));
        assert.ok(names.has("title_regeneration_request_id"));
        assert.ok(names.has("title_regeneration_started_at"));
        const rows = yield* sql`SELECT title, created_at, updated_at, fork_family_id
          FROM projection_threads WHERE thread_id = 'thread-1'`;
        assert.deepEqual(rows, [
          {
            title: "My original title",
            created_at: now,
            updated_at: now,
            fork_family_id: baseline === "personal-fork-35" ? "original-family" : null,
          },
        ]);
        yield* sql`UPDATE projection_threads SET title_regeneration_request_id = 'request-1',
          title_regeneration_started_at = ${now}, fork_family_id = 'kept-family'
          WHERE thread_id = 'thread-1'`;
        yield* migrateCompatibility;
        const repaired = yield* sql`SELECT title_regeneration_request_id, fork_family_id
          FROM projection_threads WHERE thread_id = 'thread-1'`;
        assert.deepEqual(repaired, [
          { title_regeneration_request_id: "request-1", fork_family_id: "kept-family" },
        ]);
        assert.deepEqual(yield* runMigrations(), []);
        if (baseline === "personal-fork-35") {
          const recorded =
            yield* sql`SELECT name FROM effect_sql_migrations WHERE migration_id = 35`;
          assert.deepEqual(recorded, [{ name: "ProjectionThreadsForkFamily" }]);
        }
      }),
    );
  });
}
