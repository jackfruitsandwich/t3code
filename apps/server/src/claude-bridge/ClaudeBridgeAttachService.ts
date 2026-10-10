/**
 * ClaudeBridgeAttachService - makes a turn typed into a Claude Code session
 * outside T3 (a terminal, `claude attach`) show up as a run on its T3 thread.
 *
 * The mod reports `turn.start` for every turn. When nobody is attached to the
 * session, this service finds the thread that owns the native session and
 * offers a provider continuation, the same path a Claude background-task wake
 * takes: the continuation run starts a turn on the adapter, which attaches the
 * bridge transport and streams the turn in. The typed text travels as the
 * continuation's `detail`, so the run shows what was asked.
 */
import { ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";

import * as SessionImporter from "../import/Services/SessionImporter.ts";
import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ProviderContinuationRequests from "../orchestration-v2/ProviderContinuationRequests.ts";
import { isClaudeHarnessPrompt } from "./ClaudeBridgeProtocol.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

const make = Effect.gen(function* () {
  const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;
  const sql = yield* SqlClient.SqlClient;
  const continuations = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const importer = yield* SessionImporter.SessionImporter;

  const lookupThread = (sid: string) =>
    sql<{ readonly thread_id: string | null; readonly provider_thread_id: string }>`
      SELECT provider_thread.provider_thread_id, provider_thread.thread_id
      FROM orchestration_v2_projection_provider_threads AS provider_thread
      JOIN orchestration_v2_projection_threads AS thread
        ON thread.thread_id = provider_thread.thread_id
      WHERE json_extract(provider_thread.payload_json, '$.nativeThreadRef.nativeId') = ${sid}
        AND json_extract(provider_thread.payload_json, '$.driver') = ${CLAUDE_PROVIDER}
        AND thread.deleted_at IS NULL
        AND thread.archived_at IS NULL
      ORDER BY provider_thread.updated_at DESC
      LIMIT 1
    `.pipe(
      Effect.map((rows) => {
        const row = rows[0];
        return row === undefined || row.thread_id === null
          ? null
          : {
              threadId: ThreadId.make(row.thread_id),
              providerThreadId: ProviderThreadId.make(row.provider_thread_id),
            };
      }),
      Effect.orElseSucceed(() => null),
    );

  // One offer per native turn: the pump can replay nothing, but a restart
  // scan and a late event could both name the same turn.
  const offeredTurns = new Set<string>();

  const offerTurn = (sid: string, turnId: string, text: string) =>
    Effect.gen(function* () {
      const key = `${sid}:${turnId}`;
      if (offeredTurns.has(key)) return;
      offeredTurns.add(key);
      if (offeredTurns.size > 512) {
        offeredTurns.delete(offeredTurns.values().next().value!);
      }
      let thread = yield* lookupThread(sid);
      if (thread === null) {
        // A session T3 has not imported yet: sweep now so a thread exists.
        yield* importer.sweepNow();
        thread = yield* lookupThread(sid);
      }
      if (thread === null) {
        yield* Effect.logInfo("claude-bridge.turn-without-thread", { sid, turnId });
        return;
      }
      yield* Effect.logInfo("claude-bridge.terminal-turn-detected", {
        sid,
        turnId,
        threadId: thread.threadId,
      });
      yield* continuations.offer({
        threadId: thread.threadId,
        providerThreadId: thread.providerThreadId,
        driver: CLAUDE_PROVIDER,
        detail: text.trim().length > 0 ? text : null,
        // A turn the person typed is theirs on every client; one the harness
        // injected (a task notice, a hook's feedback) is not their message.
        createdBy: isHarnessPrompt(text) ? "agent" : "user",
      });
    });

  yield* registry.onUnattachedEvent((event, entry) =>
    event.type === "turn.start" ? offerTurn(entry.sid, event.turnId, event.text) : Effect.void,
  );

  // A turn already running when this server came up (a restart mid-turn).
  const live = yield* registry.listLiveProcesses;
  for (const entry of live) {
    if (entry.session.activeTurn !== null && entry.session.activeTurn.promptUuid === undefined) {
      yield* offerTurn(entry.sid, entry.session.activeTurn.turnId, "");
    }
  }
});

/** Kept under this name for its callers; see `isClaudeHarnessPrompt`. */
export const isHarnessPrompt = isClaudeHarnessPrompt;

export const layer = Layer.effectDiscard(make);
