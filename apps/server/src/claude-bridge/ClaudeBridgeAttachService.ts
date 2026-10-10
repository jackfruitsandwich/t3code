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
import * as ProviderRuntimeRecoveryService from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import { isClaudeHarnessPrompt } from "./ClaudeBridgeProtocol.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

const make = Effect.gen(function* () {
  const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;
  const sql = yield* SqlClient.SqlClient;
  const continuations = yield* ProviderContinuationRequests.ProviderContinuationRequests;
  const importer = yield* SessionImporter.SessionImporter;
  const recovery = yield* ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService;
  // Startup recovery closes every run the last server process left open. A
  // run offered before it has finished would be closed with them, and the
  // turn it was for would go unshown, so every offer waits for recovery.
  const afterStartupRecovery = recovery.awaitStartupRecovery.pipe(
    Effect.timeoutOption("60 seconds"),
    Effect.asVoid,
  );

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

  const offerTurn = (
    entry: ClaudeBridgeRegistry.ClaudeBridgeProcess,
    turn: { readonly turnId: string; readonly text: string; readonly transcriptOffset: number },
  ) =>
    Effect.gen(function* () {
      yield* afterStartupRecovery;
      const sid = entry.sid;
      const { turnId, text } = turn;
      const key = `${sid}:${turnId}`;
      if (offeredTurns.has(key)) return;
      offeredTurns.add(key);
      if (offeredTurns.size > 512) {
        offeredTurns.delete(offeredTurns.values().next().value!);
      }
      let thread = yield* lookupThread(sid);
      if (thread === null) {
        // A session a person just started in a terminal: it gets its thread
        // now, so its first turn shows while it runs. What the session wrote
        // before this turn comes in as history; the turn itself arrives
        // through the attachment, also when it has already ended by then.
        // The turn's own report says where it begins: the session file may
        // not name the turn yet.
        const session = (yield* registry.readSession(entry)) ?? entry.session;
        if (session.isInteractive) {
          yield* importer.adoptLiveClaudeSession({
            sessionId: sid,
            transcriptPath: session.transcriptPath,
            cwd: session.cwd,
            turnOffset: turn.transcriptOffset,
            promptText: text,
          });
          thread = yield* lookupThread(sid);
          if (thread !== null) {
            yield* Effect.logInfo("claude-bridge.session-adopted", { sid, turnId });
          }
        }
      }
      if (thread === null) {
        // Anything else T3 has not imported yet: sweep now so a thread exists.
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
        // injected (a task notice, a hook's feedback) is not their message,
        // and neither is a turn picked up with nothing known of how it began.
        createdBy: text.trim().length === 0 || isHarnessPrompt(text) ? "agent" : "user",
      });
    });

  // The attachment carries a turn's replies and tools. What it does not carry
  // (a message typed into a running turn, a turn nothing was attached for) is
  // in the session's record: the two are compared whenever the record moves
  // on, not when the importer's sweep next reaches the file.
  const scope = yield* Effect.scope;
  const comparing = new Map<string, { passes: number; running: boolean }>();
  yield* registry.onRecordChange((entry, change) =>
    Effect.gen(function* () {
      const sid = entry.sid;
      const state = comparing.get(sid) ?? { passes: 0, running: false };
      comparing.set(sid, state);
      // A turn's own run is still closing just after it ends: once now for
      // what came before it, once more when the run is over.
      state.passes = Math.max(state.passes, change === "turn.complete" ? 2 : 1);
      if (state.running) return;
      state.running = true;
      yield* Effect.gen(function* () {
        let first = true;
        while (state.passes > 0) {
          state.passes -= 1;
          yield* Effect.sleep(first ? "1500 millis" : "6 seconds");
          first = false;
          const session = (yield* registry.readSession(entry)) ?? entry.session;
          if (session.transcriptPath.length === 0) continue;
          yield* importer.healLiveClaudeSession({
            sessionId: sid,
            transcriptPath: session.transcriptPath,
          });
        }
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            state.running = false;
            if (state.passes === 0) comparing.delete(sid);
          }),
        ),
        Effect.forkIn(scope),
      );
    }),
  );

  yield* registry.onUnattachedEvent((event, entry) =>
    event.type === "turn.start" ? offerTurn(entry, event) : Effect.void,
  );

  // A turn already running when this server came up (a restart mid-turn).
  // Whoever started it, the terminal or a prompt sent from T3: recovery has
  // closed the run that was showing it, and the session is still working.
  // Forked: recovery runs after the layers are built, and the offer waits on it.
  yield* Effect.gen(function* () {
    yield* afterStartupRecovery;
    const live = yield* registry.listLiveProcesses;
    for (const entry of live) {
      if (entry.session.activeTurn !== null) {
        yield* offerTurn(entry, { ...entry.session.activeTurn, text: "" });
      }
    }
  }).pipe(Effect.forkScoped);
});

/** Kept under this name for its callers; see `isClaudeHarnessPrompt`. */
export const isHarnessPrompt = isClaudeHarnessPrompt;

export const layer = Layer.effectDiscard(make);
