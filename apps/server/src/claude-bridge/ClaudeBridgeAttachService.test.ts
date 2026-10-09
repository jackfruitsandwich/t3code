import { assert, describe, it } from "@effect/vitest";

import { isHarnessPrompt } from "./ClaudeBridgeAttachService.ts";

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
