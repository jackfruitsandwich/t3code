import { assert, describe, it } from "@effect/vitest";

import {
  claudeLaunchArgs,
  claudeLaunchEnvironment,
  type ClaudeBridgeLaunchInput,
} from "./ClaudeBridgeRegistry.ts";

const input: ClaudeBridgeLaunchInput = {
  sid: "0d7a9a6e-0e4b-4a45-9e2b-1b8d0a0f6c11",
  cwd: "/work/project",
  configDir: "/home/me/.claude",
  binaryPath: "claude",
  environment: {},
  model: "claude-sonnet-4-6",
  permissionMode: "bypassPermissions",
  extraArgs: { settings: "/tmp/s.json" },
};

describe("claudeLaunchArgs", () => {
  it("passes the thread's model, permission mode and extra flags on a job's first launch", () => {
    assert.deepStrictEqual(claudeLaunchArgs(input, { hasSavedOptions: false }), [
      "--bg",
      "--resume",
      input.sid,
      "--model",
      "claude-sonnet-4-6",
      "--permission-mode",
      "bypassPermissions",
      "--settings",
      "/tmp/s.json",
    ]);
  });

  it("passes no flags when the job already has saved options, which flags would turn into a copy", () => {
    assert.deepStrictEqual(claudeLaunchArgs(input, { hasSavedOptions: true }), [
      "--bg",
      "--resume",
      input.sid,
    ]);
  });

  it("omits the default permission mode and an empty model", () => {
    assert.deepStrictEqual(
      claudeLaunchArgs(
        { ...input, model: "", permissionMode: "default", extraArgs: {} },
        { hasSavedOptions: false },
      ),
      ["--bg", "--resume", input.sid],
    );
  });
});

describe("claudeLaunchEnvironment", () => {
  it("drops the nested-session markers and names the bridge directory", () => {
    const environment = claudeLaunchEnvironment(
      { CLAUDECODE: "1", CLAUDE_CODE_ENTRYPOINT: "cli", CLAUDE_CODE_SSE_PORT: "1", PATH: "/bin" },
      "/state/bridge",
    );
    assert.deepStrictEqual(environment, { PATH: "/bin", T3CODE_BRIDGE_DIR: "/state/bridge" });
  });
});
