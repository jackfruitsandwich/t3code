import { assert, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  RunId,
  RuntimeRequestId,
  ThreadId,
  type OrchestrationV2DomainEvent,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { CodexProviderCapabilitiesV2 } from "./Adapters/CodexAdapterV2.ts";
import * as EffectWorker from "./EffectWorker.ts";
import * as EventSink from "./EventSink.ts";
import * as Orchestrator from "./Orchestrator.ts";
import type {
  ProviderAdapterV2Event,
  ProviderAdapterV2Shape,
  ProviderAdapterV2TurnInput,
} from "./ProviderAdapter.ts";
import * as ProviderAdapterRegistry from "./ProviderAdapterRegistry.ts";
import * as ProviderReplayHarness from "./testkit/ProviderReplayHarness.ts";
import { checkpointWorkspace } from "./testkit/ReplayFixtureWorkspace.ts";

const driver = ProviderDriverKind.make("codex");
const instanceId = ProviderInstanceId.make("codex");
const modelSelection = { instanceId, model: "test-model" };

// A turn the native session went on running while the server restarted: the
// run recovery left open picks it back up on a new attempt, prompting nothing.
it.effect("picks a running run back up on a provider_recovery attempt", () =>
  Effect.scoped(
    Effect.gen(function* () {
      const cwd = yield* checkpointWorkspace("run-reattach");
      const events = yield* Queue.unbounded<ProviderAdapterV2Event>();
      const started: ProviderAdapterV2TurnInput[] = [];
      const adapter: ProviderAdapterV2Shape = {
        instanceId,
        driver,
        getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
        planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
        openSession: (input) =>
          Effect.gen(function* () {
            const now = yield* DateTime.now;
            return {
              instanceId,
              driver,
              providerSessionId: input.providerSessionId,
              providerSession: {
                id: input.providerSessionId,
                driver,
                providerInstanceId: instanceId,
                status: "ready",
                cwd,
                model: modelSelection.model,
                capabilities: CodexProviderCapabilitiesV2,
                createdAt: now,
                updatedAt: now,
                lastError: null,
              },
              events: Stream.fromQueue(events),
              ensureThread: ({ threadId }) =>
                Effect.succeed({
                  id: ProviderThreadId.make(`provider-thread:codex:${threadId}`),
                  driver,
                  providerInstanceId: instanceId,
                  providerSessionId: input.providerSessionId,
                  appThreadId: threadId,
                  ownerNodeId: null,
                  nativeThreadRef: { driver, nativeId: "native-thread", strength: "strong" },
                  nativeConversationHeadRef: null,
                  status: "idle",
                  firstRunOrdinal: null,
                  lastRunOrdinal: null,
                  handoffIds: [],
                  forkedFrom: null,
                  createdAt: now,
                  updatedAt: now,
                }),
              resumeThread: ({ providerThread }) => Effect.succeed(providerThread),
              startTurn: (turn) =>
                Effect.gen(function* () {
                  started.push(turn);
                  yield* Queue.offer(events, {
                    type: "provider_turn.updated",
                    driver,
                    providerTurn: {
                      id: ProviderTurnId.make(`provider-turn:${turn.attemptId}`),
                      providerThreadId: turn.providerThread.id,
                      nodeId: turn.rootNodeId,
                      runAttemptId: turn.attemptId,
                      nativeTurnRef: {
                        driver,
                        nativeId: `native:${turn.attemptId}`,
                        strength: "strong",
                      },
                      ordinal: turn.providerTurnOrdinal,
                      status: "running",
                      startedAt: now,
                      completedAt: null,
                    },
                  });
                }),
              steerTurn: () => Effect.die("unused"),
              interruptTurn: () => Effect.die("unused"),
              respondToRuntimeRequest: () => Effect.die("unused"),
              readThreadSnapshot: () => Effect.die("unused"),
              rollbackThread: () => Effect.die("unused"),
              forkThread: () => Effect.die("unused"),
            };
          }),
      };
      yield* Effect.gen(function* () {
        const orchestrator = yield* Orchestrator.OrchestratorV2;
        const worker = yield* EffectWorker.OrchestrationEffectWorkerV2;
        const sink = yield* EventSink.EventSinkV2;
        const threadId = ThreadId.make("thread:run-reattach");
        const watch = (predicate: (event: OrchestrationV2DomainEvent) => boolean) =>
          orchestrator.streamDomainEvents.pipe(
            Stream.filter(predicate),
            Stream.take(1),
            Stream.runDrain,
            Effect.forkScoped,
          );
        yield* orchestrator.dispatch({
          type: "thread.create",
          commandId: CommandId.make("create"),
          threadId,
          projectId: ProjectId.make("project:run-reattach"),
          title: "Run reattach",
          modelSelection,
          runtimeMode: "full-access",
          interactionMode: "default",
          branch: null,
          worktreePath: cwd,
          createdBy: "user",
          creationSource: "web",
        });
        const firstRunning = yield* watch(
          (event) => event.type === "provider-turn.updated" && event.payload.status === "running",
        );
        yield* orchestrator.dispatch({
          type: "message.dispatch",
          commandId: CommandId.make("prompt"),
          threadId,
          messageId: MessageId.make("message:prompt"),
          text: "Run the long build",
          attachments: [],
          dispatchMode: { type: "start_immediately" },
          createdBy: "user",
          creationSource: "web",
        });
        yield* worker.drain();
        yield* Fiber.join(firstRunning);
        const first = started[0]!;
        const firstTurn = (yield* orchestrator.getThreadProjection(threadId)).providerTurns[0]!;
        // The approval the session was asking when the server went down,
        // closed by recovery; the session asks it again once attached.
        const requestId = RuntimeRequestId.make("runtime-request:reask");
        const now = yield* DateTime.now;
        yield* sink.write({
          events: [
            {
              id: EventId.make("closed-request"),
              type: "runtime-request.updated",
              threadId,
              occurredAt: now,
              payload: {
                id: requestId,
                nodeId: NodeId.make("node:reask"),
                providerTurnId: firstTurn.id,
                nativeRequestRef: { driver, nativeId: "toolu_reask", strength: "strong" },
                kind: "command",
                status: "expired",
                responseCapability: { type: "not_resumable", reason: "The server restarted." },
                createdAt: now,
                resolvedAt: now,
              },
            },
          ],
        });

        const secondRunning = yield* watch(
          (event) =>
            event.type === "provider-turn.updated" &&
            event.payload.status === "running" &&
            event.payload.id !== firstTurn.id,
        );
        yield* orchestrator.dispatch({
          type: "run.reattach",
          commandId: CommandId.make("reattach"),
          threadId,
          runId: first.runId,
        });
        yield* worker.drain();
        yield* Fiber.join(secondRunning);

        assert.lengthOf(started, 2);
        const second = started[1]!;
        assert.equal(second.runId, first.runId);
        assert.notEqual(second.attemptId, first.attemptId);
        // The same message, sent as the provider's: nothing is prompted again.
        assert.equal(second.message.messageId, first.message.messageId);
        assert.equal(second.message.creationSource, "provider");
        assert.equal(second.message.text, "Run the long build");
        assert.deepEqual(
          second.recoveredRuntimeRequests?.map((request) => request.id),
          [requestId],
        );

        const after = yield* orchestrator.getThreadProjection(threadId);
        assert.deepEqual(
          after.runs.map((run) => [run.id, run.status, run.activeAttemptId]),
          [[first.runId, "running", second.attemptId]],
        );
        assert.deepEqual(
          after.attempts.map((attempt) => [attempt.id, attempt.reason, attempt.status]),
          [
            [first.attemptId, "initial", "interrupted"],
            [second.attemptId, "provider_recovery", "running"],
          ],
        );
        assert.deepEqual(
          after.providerTurns.map((turn) => [turn.runAttemptId, turn.status]),
          [
            [first.attemptId, "interrupted"],
            [second.attemptId, "running"],
          ],
        );
        // The new root shares the run's checkpoint scope: one baseline, one diff.
        const roots = [first.rootNodeId, second.rootNodeId].map((id) =>
          after.nodes.find((node) => node.id === id)!,
        );
        assert.deepEqual(
          roots.map((node) => node.status),
          ["interrupted", "running"],
        );
        assert.equal(roots[0]!.checkpointScopeId, roots[1]!.checkpointScopeId);
        assert.deepEqual(
          after.messages.filter((message) => message.role === "user").map((message) => message.id),
          [first.message.messageId],
        );

        // Only a running run is picked back up.
        const rejected = yield* orchestrator
          .dispatch({
            type: "run.reattach",
            commandId: CommandId.make("reattach-missing"),
            threadId,
            runId: RunId.make("run:missing"),
          })
          .pipe(Effect.flip);
        assert.equal(rejected._tag, "OrchestratorDispatchError");
      }).pipe(
        Effect.provide(
          ProviderReplayHarness.layerWithRegistry(
            { name: "run-reattach" },
            ProviderAdapterRegistry.layerSingle(adapter),
            { runEffectWorker: false },
          ),
        ),
      );
    }),
  ),
);
