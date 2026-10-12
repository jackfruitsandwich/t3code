import { describe, expect, it } from "@effect/vitest";

import { resolveUserMessageIntentBadge } from "./userMessageIntentBadge";

describe("user message intent badge", () => {
  it("does not label ordinary turn-start messages", () => {
    expect(resolveUserMessageIntentBadge(undefined)).toBeNull();
    expect(resolveUserMessageIntentBadge("turn_start")).toBeNull();
  });

  it("labels messages waiting behind the active turn", () => {
    expect(resolveUserMessageIntentBadge("queued_turn")).toEqual({
      label: "queued",
      accessibilityLabel: "Queued behind the active turn",
      tone: "queued",
    });
  });

  it("labels messages that steer the active turn", () => {
    expect(resolveUserMessageIntentBadge("steer")).toEqual({
      label: "steer",
      accessibilityLabel: "Steered the active turn",
      tone: "steer",
    });
  });

  it("says a steer is waiting until its session has taken it", () => {
    for (const intent of ["steer", "promoted_queued_to_steer"] as const) {
      expect(resolveUserMessageIntentBadge(intent, "waiting")).toEqual({
        label: "steer · waiting",
        accessibilityLabel:
          "The session has not taken this message yet. It reads it at its next tool call, or when its turn ends.",
        tone: "steer",
      });
      expect(resolveUserMessageIntentBadge(intent, "not_taken")).toMatchObject({
        label: "steer · not taken",
        tone: "steer",
      });
      // Taken: labelled as it always was.
      expect(resolveUserMessageIntentBadge(intent, undefined)).toEqual(
        resolveUserMessageIntentBadge(intent),
      );
    }
  });

  it("leaves other messages' labels alone whatever is said of their delivery", () => {
    expect(resolveUserMessageIntentBadge("queued_turn", "waiting")?.label).toBe("queued");
    expect(resolveUserMessageIntentBadge("turn_start", "waiting")).toBeNull();
  });

  it("preserves the queued origin after promotion to steer", () => {
    expect(resolveUserMessageIntentBadge("promoted_queued_to_steer")).toEqual({
      label: "queued → steer",
      accessibilityLabel: "Originally queued, then promoted to steer the active turn",
      tone: "steer",
    });
  });
});
