import { afterAll, assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as ProjectionStore from "../ProjectionStore.ts";
import * as LegacyV1ThreadImporter from "./LegacyV1ThreadImporter.ts";
import {
  WUWEI_MODEL,
  insertActivity,
  insertMessage,
  insertProject,
  insertRuntime,
  insertThread,
  isoAt,
  layerTest,
  legacyImportRow,
  wuweiCursor,
} from "./LegacyV1ForkImport.testkit.ts";

// No wuwei bundle at all (wuwei not built on this machine). Kept in its own
// file because the first successfully loaded wuwei host is kept per process.
const previousEmbed = process.env.T3CODE_WUWEI_EMBED;
process.env.T3CODE_WUWEI_EMBED = "/nonexistent/t3code-test/wuwei/dist/embed.mjs";
afterAll(() => {
  if (previousEmbed === undefined) delete process.env.T3CODE_WUWEI_EMBED;
  else process.env.T3CODE_WUWEI_EMBED = previousEmbed;
});

it.layer(layerTest)("LegacyV1ThreadImporter without a wuwei bundle", (it) => {
  it.effect("migrates a wuwei thread from its v1 messages and activities", () =>
    Effect.gen(function* () {
      const importer = yield* LegacyV1ThreadImporter.LegacyV1ThreadImporter;
      const projections = yield* ProjectionStore.ProjectionStoreV2;
      const threadId = ThreadId.make("thread:wuwei:no-bundle");
      const t = (seconds: number) => isoAt(Date.UTC(2026, 8, 24, 18, 12, 0) + seconds * 1_000);

      yield* insertProject;
      yield* insertThread({
        threadId,
        modelSelectionJson: WUWEI_MODEL,
        createdAt: t(0),
        updatedAt: t(60),
      });
      yield* insertRuntime({
        threadId,
        providerName: "wuwei",
        instanceId: "wuwei",
        cursorJson: wuweiCursor("2026-09-24-18-12-00-97tc"),
      });
      yield* insertMessage({
        messageId: "message:no-bundle:1",
        threadId,
        role: "user",
        text: "Search for the transcription model",
        createdAt: t(0),
      });
      yield* insertMessage({
        messageId: "message:no-bundle:2",
        threadId,
        role: "assistant",
        text: "Search is not configured.",
        createdAt: t(10),
      });
      yield* insertActivity({
        activityId: "activity:no-bundle:search",
        threadId,
        kind: "tool.completed",
        summary: "Searched files",
        payload: {
          itemType: "web_search",
          toolCallId: "toolu_01HZQb4xpm9DoPGwwS7ERPdE",
          status: "failed",
          title: "Searched files",
          detail: "T3 Chat iOS app voice to text speech transcription model",
          data: {
            toolCallId: "toolu_01HZQb4xpm9DoPGwwS7ERPdE",
            kind: "search",
            rawInput: { query: "T3 Chat iOS app voice to text speech transcription model" },
            content: [
              {
                type: "content",
                content: { type: "text", text: "EXA_API_KEY not set (add it to .env)" },
              },
            ],
          },
        },
        createdAt: t(5),
      });

      assert.deepStrictEqual(yield* importer.reconcileShells, {
        importedThreadCount: 1,
        importedMessageCount: 2,
      });
      const projection = yield* projections.getThreadProjection(threadId);
      assert.deepStrictEqual(
        [...projection.turnItems]
          .sort((left, right) => left.ordinal - right.ordinal)
          .map((item) => [item.type, item.ordinal, item.status]),
        [
          ["user_message", 1, "completed"],
          ["web_search", 2, "failed"],
          ["assistant_message", 3, "completed"],
        ],
      );
      assert.isNotNull(projection.thread.activeProviderThreadId);
      assert.isNotNull((yield* legacyImportRow(threadId))?.transcript_imported_at);
    }),
  );
});
