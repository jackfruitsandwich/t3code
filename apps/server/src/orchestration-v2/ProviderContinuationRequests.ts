import {
  type OrchestrationV2Actor,
  type OrchestrationV2Notification,
  MessageId,
  ProviderDriverKind,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";

/** What a continuation run's opening message says when nothing is known of what woke the session. */
export const CONTINUATION_MESSAGE_TEXT = "Background task completed.";

export interface ProviderContinuationRequest {
  readonly threadId: ThreadId;
  readonly providerThreadId: ProviderThreadId;
  readonly driver: ProviderDriverKind;
  readonly detail: string | null;
  readonly notification?: OrchestrationV2Notification;
  /**
   * Durable ownership for an app-owned delegated-task completion delivery.
   * The continuation worker re-reads the cohort before dispatching so a later
   * task can join a queued wake and a stopped or acknowledged cohort is dropped.
   */
  readonly delegatedCompletion?: {
    readonly parentRunId: RunId;
    readonly generation: number;
    readonly messageId: MessageId;
  };
  /**
   * How the continuation turn gets its content.
   *
   * `adapter_buffered` (default) is the provider-native wake: the adapter has
   * already buffered the CLI's wake output, and the dispatched message only
   * triggers ingestion. `ClaudeAdapterV2` deliberately discards the message
   * text on that path.
   *
   * `message_text` is for app-owned work with no buffered provider output, such
   * as a delegated child finishing. The text is the entire wake, so it must
   * reach the provider as a real prompt.
   */
  readonly delivery?: "adapter_buffered" | "message_text";
  /**
   * Who wrote the continuation's prompt. A turn the person typed into a
   * terminal, which the Claude bridge adopts, is theirs: it orders their inbox
   * and holds the thread open like a message sent from any client, and it
   * carries no background-activity notice. Default: the agent (a wake the
   * provider produced).
   */
  readonly createdBy?: OrchestrationV2Actor;
  /**
   * The session was already running its turn when this server came up. When
   * the thread still has a run running on the provider thread (recovery left
   * it open: its session ran on), that run picks the turn back up on a new
   * attempt instead of a continuation run being started for it.
   */
  readonly reattachRun?: boolean;
  readonly dispatchIfCurrent?: <A, E, R>(
    effect: Effect.Effect<A, E, R>,
  ) => Effect.Effect<Option.Option<A>, E, R>;
  /** Clears a pending offer that the continuation worker intentionally drops. */
  readonly clearIfCurrent?: () => Effect.Effect<void>;
}

/**
 * Adapters offer a continuation request when provider-native work completes
 * outside an active turn (for example a Claude background task wake turn) so
 * the orchestrator can start a run that ingests it. The default reference
 * drops requests, keeping adapter construction dependency-free in tests; the
 * live layer must be shared with the ProviderContinuationService worker that
 * drains it.
 */
export class ProviderContinuationRequests extends Context.Reference<{
  readonly offer: (request: ProviderContinuationRequest) => Effect.Effect<void>;
  readonly take: Effect.Effect<ProviderContinuationRequest>;
}>("t3/orchestration-v2/ProviderContinuationRequests", {
  defaultValue: () => ({ offer: () => Effect.void, take: Effect.never }),
}) {}

export const layer = Layer.effect(
  ProviderContinuationRequests,
  Effect.gen(function* () {
    const queue = yield* Queue.unbounded<ProviderContinuationRequest>();
    return {
      offer: (request: ProviderContinuationRequest) =>
        Queue.offer(queue, request).pipe(Effect.asVoid),
      take: Queue.take(queue),
    };
  }),
);
