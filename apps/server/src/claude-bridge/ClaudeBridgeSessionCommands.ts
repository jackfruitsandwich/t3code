/**
 * The slash commands of the Claude Code sessions that are live in a folder,
 * as their own typeahead lists them (the t3bridge mod writes each session's
 * list beside its heartbeat). The provider's probe lists what a fresh SDK
 * session would have; a running terminal session can have more, and what the
 * person can run from T3 is what the session they are talking to has.
 */
import * as Clock from "effect/Clock";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { parseJsonlLine } from "../import/sessionImportTypes.ts";
import {
  isClaudeBridgeSessionLive,
  parseClaudeBridgeSessionFile,
  parseClaudeBridgeSlashCommands,
  type ClaudeBridgeSlashCommand,
} from "./ClaudeBridgeProtocol.ts";

export const liveClaudeCommandsForCwd = Effect.fn("liveClaudeCommandsForCwd")(function* (
  bridgeDir: string,
  cwd: string,
): Effect.fn.Return<
  ReadonlyArray<ClaudeBridgeSlashCommand>,
  never,
  FileSystem.FileSystem | Path.Path
> {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const readJson = (file: string) =>
    fs.readFileString(file).pipe(
      Effect.map((content): unknown => parseJsonlLine(content) ?? null),
      Effect.orElseSucceed((): unknown => null),
    );
  const list = (dir: string) =>
    fs.readDirectory(dir).pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
  const real = (folder: string) => fs.realPath(folder).pipe(Effect.orElseSucceed(() => folder));

  const claudeDir = path.join(bridgeDir, "claude");
  const wanted = yield* real(cwd);
  const now = yield* Clock.currentTimeMillis;
  const byName = new Map<string, ClaudeBridgeSlashCommand>();
  for (const sid of yield* list(claudeDir)) {
    for (const instanceId of yield* list(path.join(claudeDir, sid))) {
      const dir = path.join(claudeDir, sid, instanceId);
      const session = parseClaudeBridgeSessionFile(yield* readJson(path.join(dir, "session.json")));
      if (session === null || !isClaudeBridgeSessionLive(session, now)) continue;
      if (session.cwd !== cwd && (yield* real(session.cwd)) !== wanted) continue;
      const commands = parseClaudeBridgeSlashCommands(
        yield* readJson(path.join(dir, "commands.json")),
      );
      for (const command of commands ?? []) {
        const seen = byName.get(command.name);
        // Listed in T3's menu where any session here lists it in its own.
        if (seen === undefined || (seen.hidden === true && command.hidden !== true)) {
          byName.set(command.name, command);
        }
      }
    }
  }
  return [...byName.values()];
});
