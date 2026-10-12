import type { OrchestrationV2UserMessageInputIntent } from "@t3tools/contracts";
import type { OrchestrationV2SteerDelivery } from "@t3tools/shared/orchestrationV2Timeline";

export interface UserMessageIntentBadgePresentation {
  readonly label: string;
  readonly accessibilityLabel: string;
  readonly tone: "queued" | "steer";
}

/** `delivery` is set while a message sent to steer a turn has not been taken by its session. */
export function resolveUserMessageIntentBadge(
  intent: OrchestrationV2UserMessageInputIntent | undefined,
  delivery?: OrchestrationV2SteerDelivery,
): UserMessageIntentBadgePresentation | null {
  if (intent === "steer" || intent === "promoted_queued_to_steer") {
    if (delivery === "waiting") {
      return {
        label: "steer · waiting",
        accessibilityLabel:
          "The session has not taken this message yet. It reads it at its next tool call, or when its turn ends.",
        tone: "steer",
      };
    }
    if (delivery === "not_taken") {
      return {
        label: "steer · not taken",
        accessibilityLabel:
          "The session was not seen taking this message: it dropped it or ended, or T3 stopped following it first.",
        tone: "steer",
      };
    }
  }
  switch (intent) {
    case "queued_turn":
      return {
        label: "queued",
        accessibilityLabel: "Queued behind the active turn",
        tone: "queued",
      };
    case "steer":
      return {
        label: "steer",
        accessibilityLabel: "Steered the active turn",
        tone: "steer",
      };
    case "promoted_queued_to_steer":
      return {
        label: "queued → steer",
        accessibilityLabel: "Originally queued, then promoted to steer the active turn",
        tone: "steer",
      };
    case "turn_start":
    case undefined:
      return null;
  }
}
