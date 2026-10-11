import type { ServerProvider } from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { CLAUDE_BRIDGE_PLUGIN_ID } from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { claudeHomeIsBridged, withSessionOwnedSettings } from "./ClaudeSessionWindow.ts";

const installedPlugins = { plugins: { [CLAUDE_BRIDGE_PLUGIN_ID]: [{ scope: "user" }] } };

describe("claudeHomeIsBridged", () => {
  it("is bridged when the home has the plugin and nothing forces the SDK", () => {
    expect(claudeHomeIsBridged({ transport: undefined, installedPlugins, settings: {} })).toBe(
      true,
    );
    expect(
      claudeHomeIsBridged({
        transport: undefined,
        installedPlugins,
        settings: { enabledPlugins: { [CLAUDE_BRIDGE_PLUGIN_ID]: true } },
      }),
    ).toBe(true);
  });

  it("is not bridged when the transport is forced to the SDK", () => {
    expect(claudeHomeIsBridged({ transport: " sdk ", installedPlugins, settings: {} })).toBe(false);
  });

  it("is not bridged for a home without the plugin, or with it switched off", () => {
    expect(
      claudeHomeIsBridged({ transport: undefined, installedPlugins: null, settings: null }),
    ).toBe(false);
    expect(
      claudeHomeIsBridged({
        transport: undefined,
        installedPlugins: { plugins: { "other@market": [] } },
        settings: {},
      }),
    ).toBe(false);
    expect(
      claudeHomeIsBridged({
        transport: undefined,
        installedPlugins,
        settings: { enabledPlugins: { [CLAUDE_BRIDGE_PLUGIN_ID]: false } },
      }),
    ).toBe(false);
  });
});

describe("withSessionOwnedSettings", () => {
  const snapshot = { showInteractionModeToggle: true } as ServerProvider;

  it("says the session keeps a started thread's settings, and leaves the plan toggle for a new chat", () => {
    expect(withSessionOwnedSettings(snapshot, true)).toMatchObject({
      showInteractionModeToggle: true,
      sessionOwnsThreadSettings: true,
    });
  });

  it("leaves an SDK home's snapshot as built", () => {
    expect(withSessionOwnedSettings(snapshot, false)).toBe(snapshot);
  });
});
