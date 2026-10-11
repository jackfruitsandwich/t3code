/**
 * Whether a Claude home's threads are windows onto Claude Code sessions.
 *
 * Reached through the t3bridge mod, a session keeps its own model and
 * permission mode, and T3 shows them. Reached through the SDK
 * (`T3CODE_CLAUDE_TRANSPORT=sdk`, or a home without the plugin), T3 starts
 * every turn with the thread's settings and its own modes still apply.
 * ClaudeBridgeQueryRunner makes the same two checks for each turn it opens.
 */
import type { ServerProvider } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { CLAUDE_BRIDGE_PLUGIN_ID } from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import { isRecord, parseJsonlLine } from "../../import/sessionImportTypes.ts";

/** Named here as the query runner names it; importing the runner would tie the driver to it. */
const CLAUDE_TRANSPORT_ENV = "T3CODE_CLAUDE_TRANSPORT";

export function claudeHomeIsBridged(input: {
  readonly transport: string | undefined;
  /** `<home>/plugins/installed_plugins.json`, parsed; null when unreadable. */
  readonly installedPlugins: unknown;
  /** `<home>/settings.json`, parsed; null when unreadable. */
  readonly settings: unknown;
}): boolean {
  if (input.transport?.trim() === "sdk") return false;
  const installed = input.installedPlugins;
  if (!isRecord(installed) || !isRecord(installed.plugins)) return false;
  if (!(CLAUDE_BRIDGE_PLUGIN_ID in installed.plugins)) return false;
  const enabled =
    isRecord(input.settings) && isRecord(input.settings.enabledPlugins)
      ? input.settings.enabledPlugins[CLAUDE_BRIDGE_PLUGIN_ID]
      : undefined;
  return enabled !== false;
}

/**
 * The snapshot as clients should read it for a bridged home: a started
 * thread's model and modes are the session's to keep. T3's plan toggle stays
 * on offer for a chat that has not started, where it is how the session will
 * be created; clients withdraw it on a started thread.
 */
export function withSessionOwnedSettings<Snapshot extends ServerProvider>(
  snapshot: Snapshot,
  isBridged: boolean,
): Snapshot {
  return isBridged ? { ...snapshot, sessionOwnsThreadSettings: true } : snapshot;
}

export const readClaudeHomeIsBridged = Effect.fn("readClaudeHomeIsBridged")(function* (
  configDir: string,
  hostEnvironment: NodeJS.ProcessEnv,
): Effect.fn.Return<boolean, never, FileSystem.FileSystem | Path.Path> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const readJson = (file: string) =>
    fs.readFileString(file).pipe(
      Effect.map((content): unknown => parseJsonlLine(content) ?? null),
      Effect.orElseSucceed((): unknown => null),
    );
  return claudeHomeIsBridged({
    transport: hostEnvironment[CLAUDE_TRANSPORT_ENV],
    installedPlugins: yield* readJson(path.join(configDir, "plugins", "installed_plugins.json")),
    settings: yield* readJson(path.join(configDir, "settings.json")),
  });
});
