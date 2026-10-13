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
 *
 * A session can also have something to ask while it runs no turn and nothing
 * is attached (T3 was restarted while a background subagent's request waited,
 * or it let the session go and the request came later). The mod lists what it
 * holds in its session file; a run is offered for that too, and the request
 * is asked in it.
 */
import { ProviderThreadId, ThreadId } from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";

import { claudeTaskNotification } from "../import/claudeSessionParser.ts";
import * as SessionImporter from "../import/Services/SessionImporter.ts";
import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ProviderContinuationRequests from "../orchestration-v2/ProviderContinuationRequests.ts";
import * as ProviderRuntimeRecoveryService from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import {
  claudeBridgeRequestTurnText,
  claudeTurnPromptLabel,
  isClaudeHarnessPrompt,
  type ClaudeBridgeWaitingRequest,
} from "./ClaudeBridgeProtocol.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

/**
 * How long a run offered for a session is given to attach before another is
 * offered for something the session is still waiting to ask.
 */
export const CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS = 20_000;
/** How many runs are offered for one request nothing has attached for. */
export const CLAUDE_BRIDGE_REQUEST_RUN_OFFERS = 3;

/**
 * Which of the requests a session lists as waiting gets a run offered now,
 * if any. Called only for a session nothing is attached to.
 *
 * None while the session runs a turn of its own: that turn's run (being
 * offered, or starting) carries what is asked in it. None just after a run
 * was offered for the session, whatever for: it is still attaching, and the
 * request reaches it once it has. None for a request already offered its
 * share of runs: something keeps them from attaching, and each one is a line
 * in the thread.
 */
export const claudeBridgeRequestToOffer = (input: {
  readonly hasActiveTurn: boolean;
  readonly waiting: ReadonlyArray<ClaudeBridgeWaitingRequest>;
  readonly nowMs: number;
  /** When a run was last offered for the session. */
  readonly lastOfferedAtMs: number | undefined;
  /** How many runs were offered for a request so far. */
  readonly offersFor: (toolUseId: string) => number;
}): ClaudeBridgeWaitingRequest | null => {
  if (input.hasActiveTurn) return null;
  if (
    input.lastOfferedAtMs !== undefined &&
    input.nowMs - input.lastOfferedAtMs < CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS
  ) {
    return null;
  }
  return (
    input.waiting.find(
      (request) => input.offersFor(request.toolUseId) < CLAUDE_BRIDGE_REQUEST_RUN_OFFERS,
    ) ?? null
  );
};

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

  /**
   * The thread that holds a live session's conversation: its own, or the
   * thread of the session it carried on from (a session sent to the
   * background in its terminal goes on under a new id, in the same thread).
   */
  const threadOf = (entry: ClaudeBridgeRegistry.ClaudeBridgeProcess) =>
    Effect.gen(function* () {
      const own = yield* lookupThread(entry.sid);
      if (own !== null) return own;
      const session = (yield* registry.readSession(entry)) ?? entry.session;
      if (session.transcriptPath.length === 0) return null;
      const holder = yield* importer.claudeConversationHolder({
        sessionId: entry.sid,
        transcriptPath: session.transcriptPath,
      });
      if (holder === null || holder === entry.sid) return null;
      const thread = yield* lookupThread(holder);
      if (thread !== null) {
        yield* Effect.logInfo("claude-bridge.conversation-followed", {
          sid: entry.sid,
          continuedFrom: holder,
          threadId: thread.threadId,
        });
      }
      return thread;
    });

  // One offer per native turn: the pump can replay nothing, but a restart
  // scan and a late event could both name the same turn.
  const offeredTurns = new Set<string>();
  // When a run was last offered for a session, and how many for a request.
  const lastOfferedAt = new Map<string, number>();
  const requestOffers = new Map<string, number>();

  const offerTurn = (
    entry: ClaudeBridgeRegistry.ClaudeBridgeProcess,
    turn: { readonly turnId: string; readonly text: string; readonly transcriptOffset: number },
  ) =>
    Effect.gen(function* () {
      yield* afterStartupRecovery;
      const sid = entry.sid;
      const { turnId } = turn;
      const text = claudeTurnPromptLabel(turn.text);
      const key = `${sid}:${turnId}`;
      if (offeredTurns.has(key)) return;
      offeredTurns.add(key);
      if (offeredTurns.size > 512) {
        offeredTurns.delete(offeredTurns.values().next().value!);
      }
      let thread = yield* threadOf(entry);
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
      lastOfferedAt.set(sid, yield* Clock.currentTimeMillis);
      const taskNotice = claudeTaskNotification(text);
      yield* continuations.offer({
        threadId: thread.threadId,
        providerThreadId: thread.providerThreadId,
        driver: CLAUDE_PROVIDER,
        detail: text.trim().length > 0 ? text : null,
        // A turn the person typed is theirs on every client; one the harness
        // injected (a task notice, a hook's feedback) is not their message,
        // and neither is a turn picked up with nothing known of how it began.
        createdBy: text.trim().length === 0 || isHarnessPrompt(text) ? "agent" : "user",
        // A background task reporting back shows as its own line, not as
        // a notice without words.
        ...(taskNotice === null ? {} : { notification: taskNotice }),
      });
    });

  const offerRequest = (
    entry: ClaudeBridgeRegistry.ClaudeBridgeProcess,
    waiting: ReadonlyArray<ClaudeBridgeWaitingRequest>,
  ) =>
    Effect.gen(function* () {
      yield* afterStartupRecovery;
      const sid = entry.sid;
      const request = claudeBridgeRequestToOffer({
        hasActiveTurn: entry.session.activeTurn !== null,
        waiting,
        nowMs: yield* Clock.currentTimeMillis,
        lastOfferedAtMs: lastOfferedAt.get(sid),
        offersFor: (toolUseId) => requestOffers.get(`${sid}:${toolUseId}`) ?? 0,
      });
      if (request === null) return;
      const key = `${sid}:${request.toolUseId}`;
      requestOffers.set(key, (requestOffers.get(key) ?? 0) + 1);
      if (requestOffers.size > 512) requestOffers.delete(requestOffers.keys().next().value!);
      lastOfferedAt.set(sid, yield* Clock.currentTimeMillis);
      const thread = yield* threadOf(entry);
      if (thread === null) {
        yield* Effect.logInfo("claude-bridge.request-without-thread", { sid });
        return;
      }
      yield* Effect.logInfo("claude-bridge.unattached-request-detected", {
        sid,
        toolUseId: request.toolUseId,
        threadId: thread.threadId,
      });
      // The run says who is asking, as the run an attachment opens for such
      // a request does; the request itself follows in it once the session
      // sees T3 attached.
      const detail = claudeBridgeRequestTurnText({
        agentType: request.agentType,
        question: request.kind === "question",
      });
      yield* continuations.offer({
        threadId: thread.threadId,
        providerThreadId: thread.providerThreadId,
        driver: CLAUDE_PROVIDER,
        detail,
        notification: {
          source: { kind: request.agentType === undefined ? "background_task" : "subagent" },
          outcome: "updated",
          summary: detail,
        },
        // Runs queue behind whatever the thread is doing. Settled meanwhile
        // (answered in a terminal opened on the session, or the session
        // ended), there is nothing left to show a run for.
        dispatchIfCurrent: (effect) =>
          Effect.gen(function* () {
            const live = yield* registry.findLiveProcess(sid);
            const still = (live?.session.waiting ?? []).some(
              (held) => held.toolUseId === request.toolUseId,
            );
            return still ? Option.some(yield* effect) : Option.none();
          }),
      });
    });

  // The attachment carries a turn's replies and tools. What it does not carry
  // (a message typed into a running turn, a turn nothing was attached for) is
  // in the session's record: the two are compared whenever the record moves
  // on, not when the importer's sweep next reaches the file.
  const scope = yield* Effect.scope;
  const comparing = new Map<string, { passes: number; running: boolean }>();
  // A short turn can be over before the run offered for it has attached, and
  // that run then shows it from its start. Compared before then, the turn
  // would be taken in as history and be there twice.
  const heal = (sid: string, transcriptPath: string) =>
    Effect.gen(function* () {
      const awaitingRunSinceMs = ClaudeBridgeRegistry.claudeBridgeTurnAwaitingRunSince(
        yield* registry.peekMissedTurn(sid),
        yield* Clock.currentTimeMillis,
      );
      yield* importer.healLiveClaudeSession({
        sessionId: sid,
        transcriptPath,
        ...(awaitingRunSinceMs === undefined ? {} : { turnAwaitingRunSinceMs: awaitingRunSinceMs }),
      });
    });
  yield* registry.onRecordChange((entry, change) =>
    Effect.gen(function* () {
      const sid = entry.sid;
      // The attachment is waiting on this one: the message goes into the
      // thread before the rows written after it are shown.
      if (change === "typed-mid-turn") {
        const session = (yield* registry.readSession(entry)) ?? entry.session;
        if (session.transcriptPath.length > 0) {
          yield* heal(sid, session.transcriptPath);
        }
        return;
      }
      const state = comparing.get(sid) ?? { passes: 0, running: false };
      comparing.set(sid, state);
      // A turn's own run is still closing just after it ends: once now for
      // what came before it, once more when the run is over. (A reply the
      // record has caught up on takes one pass: its drawn copy is emptied.)
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
          yield* heal(sid, session.transcriptPath);
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

  yield* registry.judgeContinuations((input) => importer.claudeSessionHeldElsewhere(input));
  yield* registry.onUnattachedEvent((event, entry) =>
    event.type === "turn.start" ? offerTurn(entry, event) : Effect.void,
  );
  yield* registry.onUnattachedRequest(offerRequest);

  // A turn already running when this server came up (a restart mid-turn).
  // Whoever started it, the terminal or a prompt sent from T3: recovery has
  // closed the run that was showing it, and the session is still working.
  // Forked: recovery runs after the layers are built, and the offer waits on it.
  yield* Effect.gen(function* () {
    yield* afterStartupRecovery;
    const live = yield* registry.listLiveProcesses;
    yield* Effect.logInfo("claude-bridge.startup-scan", {
      live: live.length,
      running: live.filter((entry) => entry.session.activeTurn !== null).map((entry) => entry.sid),
    });
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
