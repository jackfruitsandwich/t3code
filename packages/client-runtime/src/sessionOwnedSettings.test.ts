import { ProviderInstanceId } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import {
  resolveStartedThreadSettings,
  sessionModelCommand,
  threadSettingsBelongToSession,
} from "./sessionOwnedSettings.ts";

describe("threadSettingsBelongToSession", () => {
  it("holds for a started thread of a provider whose sessions keep their settings", () => {
    expect(
      threadSettingsBelongToSession({
        hasStartedThread: true,
        provider: { sessionOwnsThreadSettings: true },
      }),
    ).toBe(true);
  });

  it("leaves a thread that has not started to the composer", () => {
    expect(
      threadSettingsBelongToSession({
        hasStartedThread: false,
        provider: { sessionOwnsThreadSettings: true },
      }),
    ).toBe(false);
  });

  it("leaves other providers, and an unknown one, as they were", () => {
    expect(threadSettingsBelongToSession({ hasStartedThread: true, provider: {} })).toBe(false);
    expect(threadSettingsBelongToSession({ hasStartedThread: true, provider: null })).toBe(false);
  });
});

describe("sessionModelCommand", () => {
  const claude = ProviderInstanceId.make("claudeAgent");
  const current = { instanceId: claude, model: "claude-opus-5-5" };

  it("is the session's own /model for a different model", () => {
    expect(
      sessionModelCommand({ current, picked: { instanceId: claude, model: "claude-sonnet-5-5" } }),
    ).toBe("/model claude-sonnet-5-5");
  });

  it("sends nothing for the model the session already runs", () => {
    expect(sessionModelCommand({ current, picked: current })).toBeNull();
  });
});

describe("resolveStartedThreadSettings", () => {
  const claude = ProviderInstanceId.make("claudeAgent");
  const codex = ProviderInstanceId.make("codex");
  const thread = {
    modelSelection: { instanceId: claude, model: "claude-opus-5-5" },
    runtimeMode: "approval-required",
    interactionMode: "plan",
  } as const;
  const staleCopy = {
    modelSelection: { instanceId: claude, model: "claude-sonnet-5-5" },
    runtimeMode: "full-access",
    interactionMode: "default",
  } as const;

  it("shows and sends the thread's own values when the session keeps them", () => {
    expect(
      resolveStartedThreadSettings({
        thread,
        draft: staleCopy,
        providers: [{ instanceId: claude, sessionOwnsThreadSettings: true }],
      }),
    ).toEqual({ belongToSession: true, ...thread });
  });

  it("keeps the composer's copy for a provider whose threads T3 drives itself", () => {
    expect(
      resolveStartedThreadSettings({
        thread,
        draft: staleCopy,
        providers: [{ instanceId: claude }],
      }),
    ).toEqual({ belongToSession: false, ...staleCopy });
  });

  it("leaves a pick of another provider to the composer: that is a handoff", () => {
    const handoff = { modelSelection: { instanceId: codex, model: "gpt-6" } };
    expect(
      resolveStartedThreadSettings({
        thread,
        draft: handoff,
        providers: [{ instanceId: claude, sessionOwnsThreadSettings: true }],
      }),
    ).toEqual({
      belongToSession: false,
      modelSelection: handoff.modelSelection,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
    });
  });
});
