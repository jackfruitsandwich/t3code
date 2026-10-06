// @effect-diagnostics nodeBuiltinImport:off - writes a fake wuwei bundle for the importer to load.
/**
 * Fixtures for the personal-fork parts of the v1 → v2 import
 * (LegacyV1ForkCarryover.ts): a migrated in-memory database with the
 * importer, and helpers that seed v1 rows the way v1 stored them.
 */
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import type { WuweiTranscript } from "../../provider/wuwei/wuweiEmbedApi.ts";
import * as EventSink from "../EventSink.ts";
import * as EventStore from "../EventStore.ts";
import * as ProjectionMaintenance from "../ProjectionMaintenance.ts";
import * as ProjectionStore from "../ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";

const layerDatabase = SqlitePersistence.layerMemory;
const layerStores = Layer.mergeAll(
  layerDatabase,
  EventStore.layer.pipe(Layer.provideMerge(layerDatabase)),
  ProjectionStore.layer.pipe(Layer.provideMerge(layerDatabase)),
);
const layerEventSink = EventSink.layer.pipe(Layer.provide(layerStores));

export const layerTest = Layer.mergeAll(
  layerStores,
  layerEventSink,
  LegacyV1ThreadImporter.layer.pipe(Layer.provide(Layer.mergeAll(layerStores, layerEventSink))),
  ProjectionMaintenance.layer.pipe(Layer.provide(layerStores)),
);

/** v1 stored timestamps as ISO text. */
export const isoAt = (epochMs: number) => DateTime.formatIso(DateTime.makeUnsafe(epochMs));

export const PROJECT_ID = "project:fork-import";
export const CLAUDE_MODEL = '{"instanceId":"claudeAgent","model":"claude-opus-4-6"}';
export const CODEX_MODEL = '{"instanceId":"codex","model":"gpt-5.4"}';
export const WUWEI_MODEL = '{"instanceId":"wuwei","model":"gpt-6-sol"}';

export const claudeCursor = (session: string, turnCount = 3) =>
  JSON.stringify({
    threadId: "2fc3bff2-195e-46c7-96ea-a5f806469dd5",
    resume: session,
    resumeSessionAt: "61c47921-5e86-4e40-adae-17d06bc6e472",
    turnCount,
  });
export const wuweiCursor = (session: string) =>
  JSON.stringify({ schemaVersion: 1, sessionId: session });

export const insertProject = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    INSERT INTO projection_projects (
      project_id, title, workspace_root, scripts_json, created_at, updated_at
    ) VALUES (
      ${PROJECT_ID}, 'Fork import', '/tmp/fork-import', '[]',
      '2026-09-01T00:00:00.000Z', '2026-09-01T00:00:00.000Z'
    )
    ON CONFLICT(project_id) DO NOTHING
  `;
});

export const insertThread = (input: {
  readonly threadId: string;
  readonly modelSelectionJson: string;
  readonly createdAt: string;
  readonly updatedAt: string;
  readonly forkFamilyId?: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO projection_threads (
        thread_id, project_id, title, model_selection_json, runtime_mode,
        interaction_mode, created_at, updated_at, deleted_at, fork_family_id
      ) VALUES (
        ${input.threadId}, ${PROJECT_ID}, ${`Thread ${input.threadId}`},
        ${input.modelSelectionJson}, 'full-access', 'default',
        ${input.createdAt}, ${input.updatedAt}, NULL, ${input.forkFamilyId ?? null}
      )
    `;
  });

/** A v1 provider_session_runtime row; `instanceId: null` is a row from before instance ids. */
export const insertRuntime = (input: {
  readonly threadId: string;
  readonly providerName: string;
  readonly instanceId: string | null;
  readonly cursorJson: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO provider_session_runtime (
        thread_id, provider_name, adapter_key, runtime_mode, status, last_seen_at,
        resume_cursor_json, runtime_payload_json, provider_instance_id
      ) VALUES (
        ${input.threadId}, ${input.providerName}, ${input.providerName}, 'full-access',
        'stopped', '2026-09-30T00:00:00.000Z', ${input.cursorJson},
        '{"cwd":"/tmp/fork-import"}', ${input.instanceId}
      )
    `;
  });

export const insertMessage = (input: {
  readonly messageId: string;
  readonly threadId: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly createdAt: string;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`
      INSERT INTO projection_thread_messages (
        message_id, thread_id, turn_id, role, text, attachments_json, is_streaming,
        created_at, updated_at
      ) VALUES (
        ${input.messageId}, ${input.threadId}, NULL, ${input.role}, ${input.text}, '[]', 0,
        ${input.createdAt}, ${input.createdAt}
      )
    `;
  });

export const insertActivity = (input: {
  readonly activityId: string;
  readonly threadId: string;
  readonly kind: string;
  readonly summary: string;
  readonly payload: unknown;
  readonly createdAt: string;
  readonly tone?: string;
  readonly sequence?: number;
}) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    // @effect-diagnostics-next-line preferSchemaOverJson:off - v1 stored activity payloads as JSON text.
    const payloadJson = JSON.stringify(input.payload);
    yield* sql`
      INSERT INTO projection_thread_activities (
        activity_id, thread_id, turn_id, tone, kind, summary, payload_json, created_at, sequence
      ) VALUES (
        ${input.activityId}, ${input.threadId}, NULL, ${input.tone ?? "tool"}, ${input.kind},
        ${input.summary}, ${payloadJson}, ${input.createdAt},
        ${input.sequence ?? null}
      )
    `;
  });

export const legacyImportRow = (threadId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      readonly transcript_imported_at: string | null;
      readonly imported_message_count: number;
    }>`
      SELECT transcript_imported_at, imported_message_count
      FROM orchestration_v2_legacy_imports
      WHERE thread_id = ${threadId}
    `;
    return rows[0];
  });

export const threadEventIds = (threadId: string) =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{ readonly event_id: string }>`
      SELECT event_id FROM orchestration_events
      WHERE application_event_version = 2 AND stream_id = ${threadId}
      ORDER BY sequence
    `;
    return rows.map((row) => row.event_id);
  });

/**
 * Writes a stand-in for wuwei's `dist/embed.mjs` whose host replays the given
 * transcripts and throws for any other chat, like a missing session file.
 */
export function writeFakeWuweiBundle(transcripts: Record<string, WuweiTranscript>) {
  const dir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-wuwei-import-"));
  const path = NodePath.join(dir, "embed.mjs");
  NodeFS.writeFileSync(
    path,
    `export const EMBED_API_VERSION = 1;
const TRANSCRIPTS = ${JSON.stringify(transcripts)};
export async function loadWuwei() {
  return {
    apiVersion: 1,
    threadDir: "/fake/.wuwei/sessions",
    catalog: async () => ({ models: [], warnings: [] }),
    commands: () => [],
    authStatus: async () => ({ codex: "", anthropic: "", codexReady: false, anthropicReady: false }),
    createThread: () => { throw new Error("the migration never creates wuwei chats"); },
    hasThread: (id) => Object.hasOwn(TRANSCRIPTS, id),
    prompt: async () => { throw new Error("the migration never prompts wuwei"); },
    transcript: (id) => {
      if (!Object.hasOwn(TRANSCRIPTS, id)) throw new Error("ENOENT: no wuwei session file for " + id);
      return TRANSCRIPTS[id];
    },
    listThreads: () => [],
  };
}
`,
  );
  return { path, cleanup: () => NodeFS.rmSync(dir, { recursive: true, force: true }) };
}
