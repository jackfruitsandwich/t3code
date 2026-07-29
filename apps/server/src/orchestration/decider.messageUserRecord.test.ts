import {
  CommandId,
  MessageId,
  ProjectId,
  ProviderInstanceId,
  ThreadId,
  type OrchestrationReadModel,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";

import { decideOrchestrationCommand } from "./decider.ts";

const NOW = "2026-01-01T00:00:00.000Z";
const HISTORICAL_AT = "2025-11-20T09:30:00.000Z";

function makeReadModel(): OrchestrationReadModel {
  return {
    snapshotSequence: 0,
    projects: [],
    threads: [
      {
        id: ThreadId.make("thread-1"),
        projectId: ProjectId.make("project-1"),
        title: "Thread",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "gpt-5.4" },
        runtimeMode: "full-access",
        interactionMode: "default",
        branch: null,
        worktreePath: null,
        latestTurn: null,
        createdAt: NOW,
        updatedAt: NOW,
        archivedAt: null,
        settledOverride: null,
        settledAt: null,
        deletedAt: null,
        messages: [],
        proposedPlans: [],
        activities: [],
        checkpoints: [],
        session: null,
      },
    ],
    updatedAt: NOW,
  };
}

it.layer(NodeServices.layer)("thread.message.user.record decider", (it) => {
  it.effect("emits a non-streaming user thread.message-sent with the command's timestamp", () =>
    Effect.gen(function* () {
      const event = yield* decideOrchestrationCommand({
        command: {
          type: "thread.message.user.record",
          commandId: CommandId.make("cmd-user-record"),
          threadId: ThreadId.make("thread-1"),
          messageId: MessageId.make("msg-user-1"),
          text: "imported user message",
          createdAt: HISTORICAL_AT,
        },
        readModel: makeReadModel(),
      });

      assert.isFalse(Array.isArray(event));
      const single = Array.isArray(event) ? event[0]! : event;
      assert.equal(single.type, "thread.message-sent");
      assert.equal(single.occurredAt, HISTORICAL_AT);
      expect(single.payload).toEqual({
        threadId: ThreadId.make("thread-1"),
        messageId: MessageId.make("msg-user-1"),
        role: "user",
        text: "imported user message",
        turnId: null,
        streaming: false,
        createdAt: HISTORICAL_AT,
        updatedAt: HISTORICAL_AT,
      });
    }),
  );

  it.effect("rejects when the thread does not exist", () =>
    Effect.gen(function* () {
      const result = yield* decideOrchestrationCommand({
        command: {
          type: "thread.message.user.record",
          commandId: CommandId.make("cmd-user-record-missing"),
          threadId: ThreadId.make("thread-missing"),
          messageId: MessageId.make("msg-user-1"),
          text: "imported user message",
          createdAt: HISTORICAL_AT,
        },
        readModel: makeReadModel(),
      }).pipe(Effect.flip);

      assert.equal(result._tag, "OrchestrationCommandInvariantError");
    }),
  );
});
