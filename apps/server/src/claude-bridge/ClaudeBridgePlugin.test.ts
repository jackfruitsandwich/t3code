import { assert, describe, it } from "@effect/vitest";

import { claudeBridgeRegisterJs } from "./ClaudeBridgePlugin.ts";

type Hook = ($: unknown, e: Record<string, unknown>, next: (e: unknown) => unknown) => unknown;

/**
 * The mod as Claude Code loads it, with a stand-in for its `$`: every call the
 * mod makes resolves to nothing unless named here, and what it writes is kept.
 */
const loadMod = (session: { readonly usage?: unknown; readonly turns?: number } = {}) => {
  const register = new Function(
    `${claudeBridgeRegisterJs([]).replace("export function register(on)", "function register(on)")}\nreturn register`,
  )() as (on: (event: string, ...rest: ReadonlyArray<unknown>) => void) => void;
  const hooks = new Map<string, Hook>();
  register((event, ...rest) => {
    // A hook narrowed by a matcher (one tool's calls) is kept under both.
    hooks.set(
      rest.length === 1 ? event : `${event} ${JSON.stringify(rest[0])}`,
      rest.at(-1) as Hook,
    );
  });
  const writes = new Map<string, string>();
  const timers = new Map<number, () => void>();
  const submits: Array<unknown> = [];
  const commandRuns: Array<unknown> = [];
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
      turns: () => session.turns ?? 0,
      usage: () => {
        if (session.usage === undefined) throw new Error("not bound");
        return session.usage;
      },
    },
    command: {
      list: () => [],
      run: (input) => {
        commandRuns.push(input);
      },
    },
    agent: { list: () => [] },
    clock: {
      every: (ms, tick) => {
        timers.set(Number(ms), tick as () => void);
      },
    },
    prompt: {
      submit: (input) => {
        submits.push(input);
        return {};
      },
    },
    // A wait gives way to the test, as a second does to everything else.
    process: { run: () => new Promise((resolve) => setTimeout(resolve, 0)) },
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
  const fire = async (
    event: string,
    e: Record<string, unknown>,
    options: { readonly matcher?: unknown; readonly next?: (e: unknown) => unknown } = {},
  ) => {
    const key =
      options.matcher === undefined ? event : `${event} ${JSON.stringify(options.matcher)}`;
    const hook = hooks.get(key);
    assert.isDefined(hook, `the mod hooks ${key}`);
    return hook?.($, e, options.next ?? (async (passed) => passed));
  };
  /** What the mod last wrote as the session's file. */
  const sessionFile = () => {
    const path = [...writes.keys()].findLast((key) => key.endsWith("/session.json"));
    assert.isDefined(path, "the mod wrote its session file");
    return JSON.parse(writes.get(path ?? "") ?? "{}") as Record<string, unknown>;
  };
  /** What the mod told T3, in order. */
  const events = () =>
    [...writes.keys()]
      .filter((key) => key.includes("/events/"))
      .toSorted()
      .map((key) => JSON.parse(writes.get(key) ?? "{}") as Record<string, unknown>);
  /** T3 drops a request in the session's inbox, and the mod's next poll takes it up. */
  let seq = 0;
  const send = async (command: Record<string, unknown>) => {
    const path = [...writes.keys()].find((key) => key.endsWith("/session.json")) ?? "";
    seq += 1;
    writes.set(
      path.replace(/session\.json$/, "inbox.json"),
      JSON.stringify({ commands: [{ ...command, seq }] }),
    );
    timers.get(500)?.();
    await new Promise((resolve) => setTimeout(resolve, 0));
  };
  /** T3 writes a file of its own beside the session's (t3.json: it is attached). */
  const put = (name: string, value: unknown) => {
    const path = [...writes.keys()].find((key) => key.endsWith("/session.json")) ?? "";
    writes.set(path.replace(/session\.json$/, name), JSON.stringify(value));
  };
  return { fire, sessionFile, events, send, submits, commandRuns, put };
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

describe("the mod after the model ends the conversation", () => {
  const ENDED = "Claude ended this conversation. Start a new session (or /clear) to continue.";
  const endCall = (ended: boolean) => ({
    matcher: { tool: "EndConversation" },
    next: async () => ({ ref: "r", text: "", result: { ended, message: "" } }),
  });
  const started = async () => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    await mod.fire("prompt.submit", { text: "end it", origin: { kind: "composer" } });
    await mod.fire("turn.start", { turnId: "t1", text: "end it" });
    return mod;
  };
  const lastOf = (mod: ReturnType<typeof loadMod>, type: string) =>
    mod.events().findLast((event) => event.type === type);
  const prompt = { type: "prompt", id: "c1", text: "hello", promptUuid: "p1" };

  it("reports the ending turn interrupted, and refuses T3's next prompt in Claude Code's words", async () => {
    const mod = await started();
    // The first call is answered with a request to call again; the second ends it.
    await mod.fire("tool.call", { tool: "EndConversation" }, endCall(false));
    await mod.fire("tool.call", { tool: "EndConversation" }, endCall(true));
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer", isAborted: false });
    assert.include(lastOf(mod, "turn.complete"), { turnId: "t1", isAborted: true });
    await mod.send(prompt);
    assert.include(lastOf(mod, "prompt.dropped"), { commandId: "c1", reason: ENDED });
    assert.deepEqual(mod.submits, []);
  });

  it("ends nothing on the call answered with a request to call again, or on a subagent's", async () => {
    const mod = await started();
    await mod.fire("tool.call", { tool: "EndConversation" }, endCall(false));
    await mod.fire("tool.call", { tool: "EndConversation", agentId: "agent-1" }, endCall(true));
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer", isAborted: false });
    assert.include(lastOf(mod, "turn.complete"), { isAborted: false });
    await mod.send(prompt);
    assert.isUndefined(lastOf(mod, "prompt.dropped"));
    assert.deepEqual(mod.submits, [{ text: "hello", asUser: true }]);
  });

  it("takes the ending from Claude Code's note of the abort right after the call's result", async () => {
    const result = {
      door: "tool-result",
      origin: { kind: "tool", tool: "EndConversation" },
      message: { content: [] },
    };
    const note = {
      door: "note",
      origin: { kind: "engine" },
      message: { content: [{ type: "text", text: "[Request interrupted by user]" }] },
    };
    const mod = await started();
    await mod.fire("session.append", result);
    await mod.fire("session.append", note);
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer", isAborted: false });
    assert.include(lastOf(mod, "turn.complete"), { isAborted: true });
    await mod.send(prompt);
    assert.include(lastOf(mod, "prompt.dropped"), { commandId: "c1", reason: ENDED });
    // Esc pressed right after the call's result: the turn is reported aborted, and nothing ended.
    const pressed = await started();
    await pressed.fire("session.append", result);
    await pressed.fire("session.append", note);
    await pressed.fire("turn.complete", { turnId: "t1", reason: "aborted", isAborted: true });
    await pressed.send(prompt);
    assert.isUndefined(lastOf(pressed, "prompt.dropped"));
    assert.lengthOf(pressed.submits, 1);
  });

  it("takes T3's prompts again after /clear", async () => {
    const mod = await started();
    await mod.fire("tool.call", { tool: "EndConversation" }, endCall(true));
    await mod.fire("turn.complete", { turnId: "t1", reason: "answer", isAborted: false });
    await mod.fire("command.run", { command: "clear", args: "", origin: { kind: "composer" } });
    await mod.send(prompt);
    assert.isUndefined(lastOf(mod, "prompt.dropped"));
    assert.deepEqual(mod.submits, [{ text: "hello", asUser: true }]);
  });
});

describe("the mod's requests for a choice only the terminal's dialog can give", () => {
  const request = async (tool: string) => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    mod.put("t3.json", { heartbeatAt: Date.now(), pid: 1 });
    await mod.fire("prompt.submit", { text: "go", origin: { kind: "composer" } });
    await mod.fire("turn.start", { turnId: "t1", text: "go" });
    await mod.fire(
      "tool.check",
      { tool, tool_use_id: "tu1", input: {} },
      { next: async () => ({ decision: "ask" }) },
    );
    const answer = mod.fire(
      "classic.PermissionRequest",
      { tool_name: tool, tool_input: {} },
      { next: async () => "the terminal's dialog" },
    );
    await new Promise((resolve) => setTimeout(resolve, 0));
    const asked = mod.events().some((event) => event.type === "approval.request");
    // The call's result lets go of a request still held for T3.
    await mod.fire("session.append", {
      message: { content: [{ type: "tool_result", tool_use_id: "tu1" }] },
    });
    return { asked, answer: await answer };
  };

  it("holds no card in T3 for them: the terminal's dialog takes the call", async () => {
    for (const tool of ["OfferChromeSetup", "ShowOnboardingRolePicker"]) {
      assert.deepEqual(await request(tool), { asked: false, answer: "the terminal's dialog" });
    }
    // Any other tool's request reaches T3.
    assert.deepEqual(await request("Write"), { asked: true, answer: "the terminal's dialog" });
  });
});

describe("the mod's report of another plugin's lines", () => {
  const notices = (mod: ReturnType<typeof loadMod>) =>
    mod
      .events()
      .filter((event) => event.type === "notification")
      .map((event) => [event.message, event.dialog]);
  const started = async () => {
    const mod = loadMod();
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    return mod;
  };

  it("passes on a transcript line and a toast in its words, not a line kept to the debug log", async () => {
    const mod = await started();
    await mod.fire("ui.log", { text: "prompt from composer: 12 chars", to: "transcript" });
    await mod.fire("ui.log", { text: "cache miss for tu1", to: "debug" });
    await mod.fire("ui.toast", { text: "turn took 4 s" });
    // Refused by a hook below: the terminal draws nothing.
    await mod.fire("ui.toast", { text: "refused" }, { next: async () => ({ deny: "no" }) });
    assert.deepEqual(notices(mod), [
      ["prompt from composer: 12 chars", false],
      ["turn took 4 s", false],
    ]);
  });

  it("passes on each new line under a call's open dialog once", async () => {
    const mod = await started();
    await mod.fire("ui.notice", { tool_use_id: "tu1", text: "checked by my-plugin" });
    await mod.fire("ui.notice", { tool_use_id: "tu1", text: "checked by my-plugin" });
    await mod.fire("ui.notice", { tool_use_id: "tu1", text: undefined });
    await mod.fire("ui.notice", { tool_use_id: "tu1", text: "checked again" });
    assert.deepEqual(notices(mod), [
      ["checked by my-plugin", false],
      ["checked again", false],
    ]);
  });

  it("says a pane is shown in the terminal only, once while it is open", async () => {
    const mod = await started();
    await mod.fire("ui.open", { id: "clock", title: "Clock" });
    // Opening it again retitles it.
    await mod.fire("ui.open", { id: "clock", title: "Clock 12:01" });
    await mod.fire("ui.close", { id: "clock", origin: "person" });
    await mod.fire("ui.open", { id: "clock" });
    await mod.fire("ui.open", { id: "ask" }, { next: async () => ({ deny: "not now" }) });
    assert.deepEqual(notices(mod), [
      ["Clock: it is shown in the Claude Code terminal only.", false],
      ["clock: it is shown in the Claude Code terminal only.", false],
    ]);
  });
});

describe("the mod's run of /effort <level> sent from T3", () => {
  const sent = { type: "command", id: "c1", name: "effort", args: "high", promptUuid: "p1" };
  const started = async (turns: number) => {
    const mod = loadMod({ turns });
    await mod.fire("session.start", { cwd: "/repo", surface: "terminal", isInteractive: true });
    return mod;
  };

  it("refuses it mid-conversation in the words of the terminal's question, and runs nothing", async () => {
    const mod = await started(2);
    await mod.send(sent);
    const result = mod.events().findLast((event) => event.type === "command.result");
    assert.include(result, { commandId: "c1", ok: true });
    assert.include(
      String(result?.text),
      "This conversation is cached for the current effort level. Switching to high means the full history gets re-read on your next message.",
    );
    assert.deepEqual(mod.commandRuns, []);
  });

  it("runs it before the first turn, when the terminal asks nothing", async () => {
    const mod = await started(0);
    await mod.send(sent);
    assert.deepEqual(mod.commandRuns, [{ command: "effort", args: "high" }]);
  });
});
