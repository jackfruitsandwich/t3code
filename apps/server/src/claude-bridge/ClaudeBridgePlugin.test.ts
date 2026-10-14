import { assert, describe, it } from "@effect/vitest";

import { claudeBridgeRegisterJs } from "./ClaudeBridgePlugin.ts";

type Hook = ($: unknown, e: Record<string, unknown>, next: (e: unknown) => unknown) => unknown;

/**
 * The mod as Claude Code loads it, with a stand-in for its `$`: every call the
 * mod makes resolves to nothing unless named here, and what it writes is kept.
 */
const loadMod = (session: { readonly usage?: unknown } = {}) => {
  const register = new Function(
    `${claudeBridgeRegisterJs([]).replace("export function register(on)", "function register(on)")}\nreturn register`,
  )() as (on: (event: string, ...rest: ReadonlyArray<unknown>) => void) => void;
  const hooks = new Map<string, Hook>();
  register((event, ...rest) => {
    // Hooks narrowed by a matcher (one tool's calls) are not exercised here.
    if (rest.length === 1) hooks.set(event, rest[0] as Hook);
  });
  const writes = new Map<string, string>();
  const named: Record<string, Record<string, (...args: ReadonlyArray<unknown>) => unknown>> = {
    env: { get: (key) => (key === "T3CODE_BRIDGE_DIR" ? "/bridge" : undefined) },
    fs: {
      write: (path, body) => {
        writes.set(String(path), String(body));
      },
      read: (path) => {
        const body = writes.get(String(path));
        if (body === undefined) throw new Error("ENOENT");
        return body;
      },
      stat: () => {
        throw new Error("ENOENT");
      },
      exists: (path) => writes.has(String(path)),
    },
    session: {
      id: () => "session-1",
      surfaces: () => ["terminal"],
      model: () => "claude-opus-5-5",
      usage: () => {
        if (session.usage === undefined) throw new Error("not bound");
        return session.usage;
      },
    },
    command: { list: () => [] },
    agent: { list: () => [] },
  };
  const $ = new Proxy(
    {},
    {
      get: (_target, noun: string) =>
        new Proxy(
          {},
          {
            get:
              (_inner, verb: string) =>
              async (...args: ReadonlyArray<unknown>) =>
                named[noun]?.[verb]?.(...args),
          },
        ),
    },
  );
  const fire = async (event: string, e: Record<string, unknown>) => {
    const hook = hooks.get(event);
    assert.isDefined(hook, `the mod hooks ${event}`);
    return hook?.($, e, async (passed) => passed);
  };
  /** What the mod last wrote as the session's file. */
  const sessionFile = () => {
    const path = [...writes.keys()].findLast((key) => key.endsWith("/session.json"));
    assert.isDefined(path, "the mod wrote its session file");
    return JSON.parse(writes.get(path ?? "") ?? "{}") as Record<string, unknown>;
  };
  return { fire, sessionFile };
};

const effort = (level: string) => ({ effort: { level } });

describe("the mod's report of the session's effort", () => {
  it("reads the session's own level in a turn no command started, and the turn's level in every turn", async () => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    // A plain prompt: its turn runs at the session's level.
    await mod.fire("prompt.submit", { text: "fix the bug", origin: { kind: "composer" } });
    await mod.fire("turn.start", { turnId: "t1", text: "fix the bug" });
    await mod.fire("classic.PreToolUse", effort("low"));
    await mod.fire("classic.Stop", effort("low"));
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer" });
    assert.include(mod.sessionFile(), { effort: "low", sessionEffort: "low" });
    // /high <message>: the command's turn runs high; the session stays low.
    await mod.fire("prompt.submit", { text: "/high think hard", origin: { kind: "composer" } });
    await mod.fire("command.run", {
      command: "high",
      args: "think hard",
      origin: { kind: "composer" },
    });
    await mod.fire("turn.start", { turnId: "t2", text: "think hard" });
    await mod.fire("classic.PreToolUse", effort("high"));
    await mod.fire("classic.Stop", effort("high"));
    await mod.fire("turn.complete", { turnId: "t2", reason: "answer" });
    assert.include(mod.sessionFile(), { effort: "high", sessionEffort: "low" });
  });

  it("takes only the first reading of a turn, and none from a subagent", async () => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    await mod.fire("prompt.submit", { text: "go", origin: { kind: "composer" } });
    await mod.fire("turn.start", { turnId: "t1", text: "go" });
    // A subagent's hooks report its own level, before and after the turn's own.
    await mod.fire("classic.PreToolUse", { ...effort("max"), agent_id: "agent-1" });
    await mod.fire("classic.PreToolUse", effort("medium"));
    // A skill with a level of its own may raise the rest of the turn.
    await mod.fire("classic.PreToolUse", effort("xhigh"));
    await mod.fire("classic.PreToolUse", { ...effort("max"), agent_id: "agent-1" });
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer" });
    assert.include(mod.sessionFile(), { effort: "xhigh", sessionEffort: "medium" });
  });

  it("reads the session's level again after a command that started no turn", async () => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    await mod.fire("prompt.submit", { text: "/model sonnet", origin: { kind: "composer" } });
    await mod.fire("command.run", {
      command: "model",
      args: "sonnet",
      origin: { kind: "composer" },
    });
    await mod.fire("prompt.submit", { text: "carry on", origin: { kind: "composer" } });
    await mod.fire("turn.start", { turnId: "t1", text: "carry on" });
    await mod.fire("classic.Stop", effort("medium"));
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer" });
    assert.include(mod.sessionFile(), { sessionEffort: "medium" });
  });
});

describe("the mod's report of the session's status line", () => {
  const measured = {
    context: { tokens: 52_000, window: 200_000, percent: 26, breakdown: { categories: [] } },
    rateLimits: [
      { kind: "five_hour", percentUsed: 12.5, resetsAt: "2026-10-14T15:00:00Z" },
      { kind: "seven_day", percentUsed: 40 },
    ],
    cost: { usd: 1.25 },
    changed: ["context", "rateLimits", "cost"],
  };

  it("reads the figures when it starts, and writes each measurement into the session file", async () => {
    const mod = loadMod({ usage: { context: { window: 200_000 }, rateLimits: [] } });
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    assert.deepEqual(mod.sessionFile().usage, { context: { window: 200_000 }, rateLimits: [] });
    await mod.fire("session.measure", measured);
    assert.deepEqual(mod.sessionFile().usage, {
      context: { tokens: 52_000, window: 200_000, percent: 26 },
      rateLimits: [
        { kind: "five_hour", percentUsed: 12.5, resetsAt: "2026-10-14T15:00:00Z" },
        { kind: "seven_day", percentUsed: 40 },
      ],
      cost: { usd: 1.25 },
    });
  });
});
