import { assert, it } from "@effect/vitest";
import {
  NodeId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  ProviderTurnId,
  RunAttemptId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  TurnItemId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";

import * as EffectWorker from "./EffectWorker.ts";
import * as EffectOutbox from "./EffectOutbox.ts";
import * as EventSink from "./EventSink.ts";
import * as IdAllocator from "./IdAllocator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";
import * as ProviderRuntimeRecovery from "./ProviderRuntimeRecoveryService.ts";
import {
  ProviderSessionLiveness,
  type ProviderSessionLivenessState,
} from "./ProviderSessionLiveness.ts";
import * as ServerSettings from "../serverSettings.ts";

const claude = ProviderInstanceId.make("claude");
const threadId = ThreadId.make("thread_live_session");
const runId = RunId.make("run_live_session");
const attemptId = RunAttemptId.make("attempt_live_session");
const rootNodeId = NodeId.make("node_live_session_root");
const toolNodeId = NodeId.make("node_live_session_tool");
const approvalNodeId = NodeId.make("node_live_session_approval");
const providerThreadId = ProviderThreadId.make("provider_thread_live_session");
const providerTurnId = ProviderTurnId.make("provider_turn_live_session");
const requestId = RuntimeRequestId.make("runtime_request_live_session");
const toolItemId = TurnItemId.make("turn_item_live_session_tool");
const approvalItemId = TurnItemId.make("turn_item_live_session_approval");
const settledRunId = RunId.make("run_live_session_settled");
const backgroundItemId = TurnItemId.make("turn_item_live_session_background");
const SID = "claude-session-live";
const turnStartedAt = DateTime.makeUnsafe("2026-10-14T10:00:00.000Z");

/**
 * A Claude thread mid-turn: a running tool, a pending approval for it, and a
 * background command an earlier settled turn left running, with its roster.
 */
const projectionFor = (options?: { readonly archived?: boolean }) =>
  ({
    thread: {
      id: threadId,
      archivedAt: options?.archived === true ? turnStartedAt : null,
      deletedAt: null,
    },
    runtimeRequests: [
      {
        id: requestId,
        nodeId: approvalNodeId,
        providerTurnId,
        nativeRequestRef: { driver: "claudeAgent", nativeId: "toolu_live", strength: "strong" },
        kind: "command",
        status: "pending",
        responseCapability: { type: "live", providerSessionId: "provider_session_live" },
        createdAt: turnStartedAt,
        resolvedAt: null,
      },
    ],
    providerSessions: [
      {
        id: ProviderSessionId.make("provider_session_live"),
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claude,
        status: "ready",
      },
    ],
    providerThreads: [
      {
        id: providerThreadId,
        ownerNodeId: null,
        driver: ProviderDriverKind.make("claudeAgent"),
        providerInstanceId: claude,
        nativeThreadRef: { driver: "claudeAgent", nativeId: SID, strength: "strong" },
        status: "active",
        pendingBackgroundTasks: [{ taskId: "bg-sleep", description: "sleep 120" }],
      },
    ],
    providerTurns: [
      {
        id: providerTurnId,
        nodeId: rootNodeId,
        providerThreadId,
        runAttemptId: attemptId,
        status: "running",
        startedAt: turnStartedAt,
      },
    ],
    runs: [
      {
        id: settledRunId,
        ordinal: 1,
        status: "completed",
        providerInstanceId: claude,
        providerThreadId,
        startedAt: turnStartedAt,
      },
      {
        id: runId,
        ordinal: 2,
        status: "running",
        providerInstanceId: claude,
        providerThreadId,
        activeAttemptId: attemptId,
        rootNodeId,
        startedAt: turnStartedAt,
      },
    ],
    attempts: [
      {
        id: attemptId,
        runId,
        rootNodeId,
        status: "running",
        startedAt: turnStartedAt,
      },
    ],
    nodes: [
      { id: rootNodeId, runId, kind: "root_turn", status: "running" },
      { id: toolNodeId, runId, kind: "tool_call", status: "running" },
      { id: approvalNodeId, runId, kind: "approval_request", status: "waiting" },
    ],
    subagents: [],
    messages: [],
    turnItems: [
      {
        id: toolItemId,
        runId,
        nodeId: toolNodeId,
        providerThreadId,
        type: "command_execution",
        status: "running",
      },
      {
        id: approvalItemId,
        runId,
        nodeId: approvalNodeId,
        providerThreadId,
        type: "approval_request",
        status: "waiting",
      },
      {
        id: backgroundItemId,
        runId: settledRunId,
        nodeId: null,
        providerThreadId,
        nativeItemRef: { driver: "claudeAgent", nativeId: "bg-sleep", strength: "strong" },
        type: "command_execution",
        title: "sleep 120",
        status: "running",
      },
    ],
  }) as unknown as OrchestrationV2ThreadProjection;

const reconcile = (input: {
  readonly trigger: "startup" | "shutdown";
  /** How the session is found; absent: the default reference, which knows of no live session. */
  readonly live?: ProviderSessionLivenessState;
  readonly archived?: boolean;
}) => {
  const committed: Array<OrchestrationV2DomainEvent> = [];
  const asked: Array<string> = [];
  const layer = ProviderRuntimeRecovery.layer.pipe(
    Layer.provide(ServerSettings.layerTest()),
    Layer.provide(
      Layer.mergeAll(
        Layer.mock(ProjectionStore.ProjectionStoreV2)({
          getRecoveryThreadIds: () => Effect.succeed([threadId]),
          getRuntimeRecoveryProjection: () =>
            Effect.succeed(projectionFor({ archived: input.archived === true })),
        }),
        Layer.mock(EventSink.EventSinkV2)({
          commitCommand: (commit) => {
            committed.push(...commit.events);
            return Effect.succeed({ committed: true, cancelledEffectCount: 0 } as never);
          },
        }),
        IdAllocator.layer,
        Layer.mock(EffectWorker.OrchestrationEffectWorkerV2)({
          runRecoveryOnce: Effect.succeed(false),
        }),
        Layer.mock(EffectOutbox.EffectOutboxV2)({
          listByCommandId: () => Effect.succeed([]),
          reconcileAfterProcessLoss: Effect.succeed({ requeued: 0, cancelled: 0 }),
        }),
        input.live === undefined
          ? Layer.empty
          : Layer.succeed(ProviderSessionLiveness, {
              ofNativeThread: ({ nativeThreadId }) => {
                asked.push(nativeThreadId);
                return Effect.succeed(
                  nativeThreadId === SID ? input.live! : ({ state: "gone" } as const),
                );
              },
            }),
      ),
    ),
  );
  return Effect.gen(function* () {
    const summary =
      yield* (yield* ProviderRuntimeRecovery.ProviderRuntimeRecoveryService).reconcile(
        input.trigger,
      );
    const statusOf = (type: OrchestrationV2DomainEvent["type"], id: string) => {
      const event = committed.find(
        (candidate) =>
          candidate.type === type && (candidate.payload as { readonly id?: string }).id === id,
      );
      return event === undefined ? null : (event.payload as { readonly status: string }).status;
    };
    return { summary, committed, asked, statusOf };
  }).pipe(Effect.provide(layer));
};

const running = {
  state: "running",
  turnStartedAt: DateTime.toEpochMillis(turnStartedAt) - 2_000,
} as const;

it.effect("leaves a live session's running run open at shutdown and closes only its request", () =>
  Effect.gen(function* () {
    const { summary, asked, statusOf } = yield* reconcile({ trigger: "shutdown", live: running });
    assert.deepEqual(asked, [SID]);
    assert.equal(summary.keptRuns, 1);
    assert.equal(summary.terminalizedRuns, 0);
    assert.equal(statusOf("run.updated", runId), null);
    assert.equal(statusOf("run-attempt.updated", attemptId), null);
    assert.equal(statusOf("provider-turn.updated", providerTurnId), null);
    assert.equal(statusOf("node.updated", rootNodeId), null);
    assert.equal(statusOf("node.updated", toolNodeId), null);
    assert.equal(statusOf("turn-item.updated", toolItemId), null);
    // Nothing can answer it while the server is down: the session asks again.
    assert.equal(statusOf("runtime-request.updated", requestId), "cancelled");
    assert.equal(statusOf("node.updated", approvalNodeId), "cancelled");
    assert.equal(statusOf("turn-item.updated", approvalItemId), "cancelled");
  }),
);

it.effect("leaves the run open at startup while the session still runs the run's turn", () =>
  Effect.gen(function* () {
    const { summary, statusOf } = yield* reconcile({ trigger: "startup", live: running });
    assert.equal(summary.keptRuns, 1);
    assert.equal(statusOf("run.updated", runId), null);
    assert.equal(statusOf("turn-item.updated", toolItemId), null);
    assert.equal(statusOf("runtime-request.updated", requestId), "expired");
  }),
);

it.effect("completes the run at startup when its turn ended while no server was there", () =>
  Effect.gen(function* () {
    const { summary, statusOf } = yield* reconcile({
      trigger: "startup",
      live: { state: "idle" },
    });
    assert.equal(summary.keptRuns, 0);
    assert.equal(summary.terminalizedRuns, 1);
    assert.equal(statusOf("run.updated", runId), "completed");
    assert.equal(statusOf("run-attempt.updated", attemptId), "completed");
    assert.equal(statusOf("provider-turn.updated", providerTurnId), "completed");
    assert.equal(statusOf("node.updated", rootNodeId), "completed");
    assert.equal(statusOf("turn-item.updated", toolItemId), "completed");
    assert.equal(statusOf("turn-item.updated", approvalItemId), "cancelled");
  }),
);

it.effect("completes the run when the turn the session runs began after it", () =>
  Effect.gen(function* () {
    // Typed into the terminal during the restart, after the run's turn ended.
    const { summary, statusOf } = yield* reconcile({
      trigger: "startup",
      live: { state: "running", turnStartedAt: DateTime.toEpochMillis(turnStartedAt) + 120_000 },
    });
    assert.equal(summary.keptRuns, 0);
    assert.equal(statusOf("run.updated", runId), "completed");
  }),
);

it.effect("tells no later turn that a live session's background work was cancelled", () =>
  Effect.gen(function* () {
    for (const live of [running, { state: "idle" } as const]) {
      const { committed } = yield* reconcile({ trigger: "startup", live });
      assert.deepEqual(
        committed.filter((event) => event.type === "run.background-work-cancelled"),
        [],
      );
    }
    // The same thread whose session ended with the server is told.
    const { committed } = yield* reconcile({ trigger: "startup" });
    const recorded = committed.flatMap((event) =>
      event.type === "run.background-work-cancelled"
        ? event.payload.restartCancelledBackgroundWork.map((work) => work.kind)
        : [],
    );
    assert.isAbove(recorded.length, 0);
  }),
);

it.effect(
  "cancels as before when the session ended with the server or the thread is archived",
  () =>
    Effect.gen(function* () {
      for (const input of [
        { trigger: "shutdown" as const },
        { trigger: "startup" as const, live: { state: "gone" } as const },
        { trigger: "startup" as const, live: running, archived: true },
      ]) {
        const { summary, statusOf } = yield* reconcile(input);
        assert.equal(summary.keptRuns, 0);
        assert.equal(statusOf("run.updated", runId), "cancelled");
        assert.equal(statusOf("turn-item.updated", toolItemId), "cancelled");
      }
    }),
);
