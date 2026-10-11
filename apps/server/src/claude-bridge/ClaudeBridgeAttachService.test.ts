import { assert, describe, it } from "@effect/vitest";

import {
  CLAUDE_BRIDGE_REQUEST_RUN_OFFERS,
  CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS,
  claudeBridgeRequestToOffer,
  isHarnessPrompt,
} from "./ClaudeBridgeAttachService.ts";
import { parseClaudeBridgeSessionFile } from "./ClaudeBridgeProtocol.ts";

describe("isHarnessPrompt", () => {
  it("recognises prompts the harness injects into a terminal session", () => {
    assert.isTrue(isHarnessPrompt("<task-notification>\n<task-id>abc</task-id>"));
    assert.isTrue(isHarnessPrompt("  <system-reminder>\nGoal check-in"));
    assert.isTrue(isHarnessPrompt("Stop hook feedback:\n[please ship]: not yet"));
    assert.isTrue(isHarnessPrompt("[Request interrupted by user]"));
  });

  it("leaves what the person typed as theirs", () => {
    assert.isFalse(isHarnessPrompt("retry"));
    assert.isFalse(isHarnessPrompt("the <task-notification> tag is what I mean"));
  });
});

describe("a run for what a session nothing is attached to is waiting to ask", () => {
  const now = 1_750_000_000_000;
  const late = { toolUseId: "tool-late", kind: "approval", agentType: "general-purpose" } as const;
  const question = { toolUseId: "tool-question", kind: "question" } as const;
  const offer = (
    over: Partial<Parameters<typeof claudeBridgeRequestToOffer>[0]> = {},
  ): ReturnType<typeof claudeBridgeRequestToOffer> =>
    claudeBridgeRequestToOffer({
      hasActiveTurn: false,
      waiting: [late],
      nowMs: now,
      lastOfferedAtMs: undefined,
      offersFor: () => 0,
      ...over,
    });

  it("is offered for a request raised with no turn running (T3 restarted, or had let go)", () => {
    assert.deepStrictEqual(offer(), late);
    assert.deepStrictEqual(offer({ waiting: [question] }), question);
  });

  it("is not offered while the session runs a turn of its own: that turn's run carries it", () => {
    assert.isNull(offer({ hasActiveTurn: true }));
  });

  it("is not offered while a run offered a moment ago is still attaching", () => {
    assert.isNull(offer({ lastOfferedAtMs: now - 1_000 }));
    assert.isNull(offer({ lastOfferedAtMs: now - CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS + 1 }));
    // Still nothing attached after that: the run did not take, another is offered.
    assert.deepStrictEqual(
      offer({ lastOfferedAtMs: now - CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS }),
      late,
    );
  });

  it("is offered once per request and pass, not once per look at the session", () => {
    // Offered just now: every later look within the span finds nothing to do.
    const offered = new Map<string, number>([[late.toolUseId, 1]]);
    assert.isNull(
      offer({ lastOfferedAtMs: now, offersFor: (id) => offered.get(id) ?? 0, nowMs: now + 400 }),
    );
  });

  it("stops after a few runs that did not attach, and still serves another request", () => {
    const offersFor = (id: string) =>
      id === late.toolUseId ? CLAUDE_BRIDGE_REQUEST_RUN_OFFERS : 0;
    assert.isNull(offer({ offersFor }));
    assert.deepStrictEqual(offer({ waiting: [late, question], offersFor }), question);
  });

  it("is never offered when the session lists nothing", () => {
    assert.isNull(offer({ waiting: [] }));
  });
});

describe("what a session lists as waiting", () => {
  const file = (waiting: unknown) =>
    parseClaudeBridgeSessionFile({
      sid: "session-1",
      instanceId: "instance-1",
      heartbeatAt: 1_750_000_000_000,
      waiting,
    });

  it("is read from its file: who asks, and whether it is a question", () => {
    assert.deepStrictEqual(
      file([
        { toolUseId: "tool-1", kind: "approval", agentType: "general-purpose" },
        { toolUseId: "tool-2", kind: "question" },
      ])?.waiting,
      [
        { toolUseId: "tool-1", kind: "approval", agentType: "general-purpose" },
        { toolUseId: "tool-2", kind: "question" },
      ],
    );
  });

  it("is absent for a session that waits for nothing, or whose mod does not say", () => {
    assert.isUndefined(file([])?.waiting);
    assert.isUndefined(file(undefined)?.waiting);
    assert.isUndefined(file([{ kind: "approval" }, "noise"])?.waiting);
  });
});
