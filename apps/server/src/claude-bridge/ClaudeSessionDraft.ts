import type {
  SessionDraftOfferInput,
  SessionDraftOfferResult,
  SessionDraftStreamEvent,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schedule from "effect/Schedule";
import * as Stream from "effect/Stream";

import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import { ProjectionStoreV2 } from "../orchestration-v2/ProjectionStore.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

/** How often a watched thread looks at its session's prompt box again. */
export const SESSION_DRAFT_POLL_MS = 300;

/**
 * The unsent draft in the prompt box of the session behind a thread, shared
 * with the thread's message box. Only a bridged Claude Code session with a
 * terminal has a box; the default knows of none.
 */
export interface ClaudeSessionDraftShape {
  /** The session's draft, first as it stands and then after every change; null while it has no box to share. */
  readonly stream: (threadId: ThreadId) => Stream.Stream<SessionDraftStreamEvent>;
  readonly offer: (input: SessionDraftOfferInput) => Effect.Effect<SessionDraftOfferResult>;
}

export class ClaudeSessionDraft extends Context.Reference<ClaudeSessionDraftShape>(
  "t3/claude-bridge/ClaudeSessionDraft",
  {
  defaultValue: () => ({
    stream: () => Stream.make(null),
    offer: () => Effect.succeed({ sent: false }),
  }),
  },
) {}

const sameDraft = (a: SessionDraftStreamEvent, b: SessionDraftStreamEvent) =>
  a === b || (a !== null && b !== null && a.rev === b.rev && a.text === b.text);

/**
 * The service over the bridge: `sessionIdOf` names the Claude Code session a
 * thread runs on now (null for none, or another provider's).
 */
export const make = (input: {
  readonly sessionIdOf: (threadId: ThreadId) => Effect.Effect<string | null>;
  readonly registry: Pick<ClaudeBridgeRegistry.ClaudeBridgeRegistry["Service"], "findLiveProcess" | "send">;
}) => {
  const liveProcess = (threadId: ThreadId) =>
    Effect.gen(function* () {
      const sid = yield* input.sessionIdOf(threadId);
      return sid === null ? null : yield* input.registry.findLiveProcess(sid);
    });
  const service: ClaudeSessionDraftShape = {
    stream: (threadId) =>
      Stream.fromEffectSchedule(
        liveProcess(threadId).pipe(
          Effect.map((process): SessionDraftStreamEvent => process?.session.draft ?? null),
        ),
        Schedule.spaced(`${SESSION_DRAFT_POLL_MS} millis`),
      ).pipe(Stream.changesWith(sameDraft)),
    offer: (offer) =>
      Effect.gen(function* () {
        const process = yield* liveProcess(offer.threadId);
        // A mod that shares no draft has no command for one either.
        if (process === null || process.session.draft == null) return { sent: false };
        yield* input.registry.send(process, { type: "draft", text: offer.text, base: offer.base });
        return { sent: true };
      }),
  };
  return service;
};

export const layer = Layer.effect(
  ClaudeSessionDraft,
  Effect.gen(function* () {
    const registry = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry;
    const store = yield* ProjectionStoreV2;
    return make({
      registry,
      sessionIdOf: (threadId) =>
        Effect.gen(function* () {
          const thread = yield* store.getThread(threadId);
          if (thread.activeProviderThreadId === null) return null;
          const context = yield* store.getThreadProviderContext(
            threadId,
            thread.providerInstanceId,
          );
          const active = context.providerThreads.find(
            (providerThread) => providerThread.id === thread.activeProviderThreadId,
          );
          return active?.driver === CLAUDE_PROVIDER
            ? (active.nativeThreadRef?.nativeId ?? null)
            : null;
        }).pipe(Effect.orElseSucceed(() => null)),
    });
  }),
);
