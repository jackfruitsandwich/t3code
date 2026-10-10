import type { PermissionResult, SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import { HostProcessEnvironment, HostProcessWorkingDirectory } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";

import * as ClaudeBridgeRegistry from "../../claude-bridge/ClaudeBridgeRegistry.ts";
import type { ClaudeBridgeCommandBody } from "../../claude-bridge/ClaudeBridgeProtocol.ts";
import * as ProviderEventLoggers from "../../provider/ProviderEventLoggers.ts";
import * as ClaudeAdapterV2 from "./ClaudeAdapterV2.ts";
import * as ClaudeBridgeQueryRunner from "./ClaudeBridgeQueryRunner.ts";
import { claudeBridgeImageNote, claudeBridgePromptText } from "./ClaudeBridgeQueryRunner.ts";

const userMessage = (content: SDKUserMessage["message"]["content"]): SDKUserMessage => ({
  type: "user",
  message: { role: "user", content },
  parent_tool_use_id: null,
});

/** A runner attached to one fake live process; what T3 sends to the mod lands in `commands`. */
const attach = (input: {
  readonly protocol: number;
  readonly slashCommands?: ReadonlyArray<string>;
  /** The bridge mod the session runs. */
  readonly modVersion?: string;
  /** What the session's transcript holds when the runner attaches. */
  readonly transcript?: string;
  /** A turn the session began, and maybe ended, while nothing was attached. */
  readonly missedTurn?: (now: number) => ClaudeBridgeRegistry.ClaudeBridgeMissedTurn;
  readonly bridgeCanUseTool: (
    tool: string,
    toolInput: Record<string, unknown>,
    toolUseId: string,
    signal: AbortSignal,
  ) => PermissionResult | null | Promise<PermissionResult | null>;
}) =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bridge-runner-" });
    const transcriptPath = `${root}/transcript.jsonl`;
    yield* fs.writeFileString(transcriptPath, input.transcript ?? "");
    const now = yield* Clock.currentTimeMillis;
    let missedTurn = input.missedTurn?.(now) ?? null;
    const process: ClaudeBridgeRegistry.ClaudeBridgeProcess = {
      sid: "bridge-session",
      instanceId: "bridge-instance",
      dir: root,
      session: {
        protocol: input.protocol,
        sid: "bridge-session",
        instanceId: "bridge-instance",
        cwd: root,
        configDir: root,
        transcriptPath,
        surfaces: ["terminal"],
        isInteractive: true,
        terminalPresent: true,
        startedAt: now,
        heartbeatAt: now,
        model: "claude-sonnet-4-6",
        permissionMode: "default",
        effort: null,
        commandsRev: 1,
        activeTurn: null,
        modVersion: input.modVersion ?? "test",
        ended: null,
      },
    };
    const commands = yield* Queue.unbounded<ClaudeBridgeCommandBody>();
    const subscribed = yield* Deferred.make<ClaudeBridgeRegistry.ClaudeBridgeEventHandler>();
    const registry = ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
      bridgeDir: root,
      plugin: { marketplaceDir: root, pluginDir: root },
      isPluginInstalled: () => Effect.succeed(true),
      listLiveProcesses: Effect.succeed([process]),
      findLiveProcess: () => Effect.succeed(process),
      readSession: () => Effect.succeed(process.session),
      readCommands: () =>
        Effect.succeed(
          input.slashCommands === undefined
            ? null
            : input.slashCommands.map((name) => ({ name, description: "", source: "builtin" })),
        ),
      send: (_process, command) => Queue.offer(commands, command).pipe(Effect.as("command-1")),
      heartbeat: () => Effect.void,
      clearHeartbeat: () => Effect.void,
      subscribe: (_sid, handler) => Deferred.succeed(subscribed, handler).pipe(Effect.asVoid),
      onUnattachedEvent: () => Effect.void,
      onRecordChange: () => Effect.void,
      noteRecordChange: () => Effect.void,
      readClientSelection: () => Effect.succeed(null),
      writeClientSelection: () => Effect.void,
      peekMissedTurn: () => Effect.sync(() => missedTurn),
      takeMissedTurn: () =>
        Effect.sync(() => {
          const taken = missedTurn;
          missedTurn = null;
          return taken;
        }),
      launch: () => Effect.die("This test must attach without launching Claude."),
      findTerminalHome: () => Effect.succeed(null),
      findSessionHome: () => Effect.succeed(root),
    });
    const services = yield* Layer.build(ClaudeBridgeQueryRunner.layer).pipe(
      Effect.provideService(ClaudeBridgeRegistry.ClaudeBridgeRegistry, registry),
      Effect.provideService(
        ProviderEventLoggers.ProviderEventLoggers,
        ProviderEventLoggers.NoOpProviderEventLoggers,
      ),
      Effect.provideService(HostProcessEnvironment, { CLAUDE_CONFIG_DIR: root }),
      Effect.provideService(HostProcessWorkingDirectory, root),
    );
    const runner = Context.get(services, ClaudeAdapterV2.ClaudeAgentSdkQueryRunner);
    const counts = { sdkCalls: 0 };
    const bridgeCalls: Array<{
      tool: string;
      input: Record<string, unknown>;
      toolUseId: string;
    }> = [];
    const session = yield* runner.open({
      threadId: ThreadId.make("bridge-thread"),
      providerSessionId: ProviderSessionId.make("bridge-provider-session"),
      options: {
        resume: process.sid,
        cwd: root,
        model: "claude-sonnet-4-6",
        tools: [],
        permissionMode: "default",
        canUseTool: () => {
          counts.sdkCalls += 1;
          return Promise.resolve({ behavior: "deny", message: "SDK callback must not run." });
        },
      },
      bridgeCanUseTool: (tool, toolInput, options) => {
        bridgeCalls.push({ tool, input: toolInput, toolUseId: options.toolUseID });
        return Promise.resolve(
          input.bridgeCanUseTool(tool, toolInput, options.toolUseID, options.signal),
        );
      },
    });
    yield* Effect.addFinalizer(() => session.close.pipe(Effect.orDie));
    const handler = yield* Deferred.await(subscribed);
    return { session, handler, commands, process, bridgeCalls, counts, now };
  });

describe("ClaudeBridgeQueryRunner permissions", () => {
  const cases: ReadonlyArray<{
    name: string;
    result: PermissionResult | null;
    expected: ReadonlyArray<ClaudeBridgeCommandBody>;
  }> = [
    {
      name: "returns an unowned request to native permission handling",
      result: null,
      expected: [{ type: "approval", toolUseId: "tool-1", decision: "ask" }],
    },
    {
      name: "preserves a real denial and its reason",
      result: { behavior: "deny", message: "The user denied this command." },
      expected: [
        {
          type: "approval",
          toolUseId: "tool-1",
          decision: "deny",
          reason: "The user denied this command.",
        },
      ],
    },
    {
      name: "forwards an approved request",
      result: { behavior: "allow" },
      expected: [{ type: "approval", toolUseId: "tool-1", decision: "allow" }],
    },
    {
      name: "keeps an approval for the session as a session grant",
      result: {
        behavior: "allow",
        updatedPermissions: [
          {
            type: "addRules",
            rules: [{ toolName: "Bash" }],
            behavior: "allow",
            destination: "session",
          },
        ],
      },
      expected: [{ type: "approval", toolUseId: "tool-1", decision: "allow", scope: "session" }],
    },
    {
      name: "stops the turn when the person cancels instead of declining",
      result: { behavior: "deny", message: "User cancelled tool execution.", interrupt: true },
      expected: [
        {
          type: "approval",
          toolUseId: "tool-1",
          decision: "deny",
          reason: "User cancelled tool execution.",
        },
        { type: "interrupt" },
      ],
    },
  ];

  it.effect.each(cases)("$name", (testCase) =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => testCase.result });
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "tool-1",
          tool: "Bash",
          input: { command: "pwd" },
        },
        bridge.process,
      );
      for (const expected of testCase.expected) {
        assert.deepStrictEqual(yield* Queue.take(bridge.commands), expected);
      }
      assert.deepStrictEqual(bridge.bridgeCalls, [
        { tool: "Bash", input: { command: "pwd" }, toolUseId: "tool-1" },
      ]);
      assert.strictEqual(bridge.counts.sdkCalls, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("hands Claude Code's own suggested rules back with an approval for the session", () =>
    Effect.gen(function* () {
      const suggested = {
        type: "addRules" as const,
        rules: [{ toolName: "Bash", ruleContent: "echo:*" }],
        behavior: "allow" as const,
        destination: "localSettings" as const,
      };
      const forSession = { ...suggested, destination: "session" as const };
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => ({
          behavior: "allow",
          updatedPermissions: [forSession],
        }),
      });
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "tool-1",
          tool: "Bash",
          input: { command: "echo hi" },
          alsoInTerminal: true,
          suggestions: [suggested],
        },
        bridge.process,
      );
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-1",
        decision: "allow",
        scope: "session",
        updatedPermissions: [forSession],
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("sends nothing back for a request the session settled without T3", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, _input, toolUseId, signal) =>
          toolUseId === "tool-1"
            ? // The person has not answered in T3; the wait ends only when aborted.
              new Promise<PermissionResult>((resolve) => {
                signal.addEventListener("abort", () =>
                  resolve({ behavior: "deny", message: "User cancelled tool execution." }),
                );
              })
            : { behavior: "allow" },
      });
      const request = (toolUseId: string) =>
        bridge.handler(
          {
            type: "approval.request",
            seq: 1,
            at: bridge.now,
            toolUseId,
            tool: "Bash",
            input: { command: "pwd" },
          },
          bridge.process,
        );
      yield* request("tool-1");
      yield* bridge.handler(
        {
          type: "approval.resolved",
          seq: 2,
          at: bridge.now,
          toolUseId: "tool-1",
          decision: "allow",
          by: "terminal",
        },
        bridge.process,
      );
      yield* request("tool-2");
      // The first thing T3 says is about the second request: the settled one got no reply.
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-2",
        decision: "allow",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps its one request when the mod loses sight of T3 for a moment", () =>
    Effect.gen(function* () {
      // The person answers in T3 only when the test says so.
      const answer = yield* Deferred.make<PermissionResult>();
      let asked = 0;
      let aborted = false;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, _input, _toolUseId, signal) => {
          asked += 1;
          signal.addEventListener("abort", () => {
            aborted = true;
          });
          return Effect.runPromise(Deferred.await(answer));
        },
      });
      const request = bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "tool-1",
          tool: "Bash",
          input: { command: "pwd" },
          alsoInTerminal: true,
        },
        bridge.process,
      );
      yield* request;
      // The mod's heartbeat read failed once: it says T3 is gone, then asks again.
      yield* bridge.handler(
        {
          type: "approval.resolved",
          seq: 2,
          at: bridge.now,
          toolUseId: "tool-1",
          decision: "ask",
          by: "detached",
        },
        bridge.process,
      );
      yield* request;
      yield* Effect.yieldNow;
      assert.strictEqual(asked, 1);
      assert.isFalse(aborted);
      // The answer given to that one request still reaches the session.
      yield* Deferred.succeed(answer, { behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-1",
        decision: "allow",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner questions", () => {
  it.effect("answers a question with the person's answers, not with an approval", () =>
    Effect.gen(function* () {
      const questions = [
        { question: "Which one?", header: "Pick", multiSelect: false, options: [] },
      ];
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, toolInput) => ({
          behavior: "allow",
          updatedInput: { questions: toolInput.questions, answers: { "Which one?": "Beta" } },
        }),
      });
      yield* bridge.handler(
        { type: "question.request", seq: 1, at: bridge.now, toolUseId: "ask-1", questions },
        bridge.process,
      );
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "answer",
        toolUseId: "ask-1",
        decision: "answer",
        answers: { "Which one?": "Beta" },
      });
      assert.deepStrictEqual(bridge.bridgeCalls, [
        { tool: "AskUserQuestion", input: { questions }, toolUseId: "ask-1" },
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("hands a question nobody in T3 owns back to the terminal dialog", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null });
      yield* bridge.handler(
        { type: "question.request", seq: 1, at: bridge.now, toolUseId: "ask-2", questions: [] },
        bridge.process,
      );
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "answer",
        toolUseId: "ask-2",
        decision: "ask",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner slash commands", () => {
  it.effect("runs a slash command as a command in the session, not as prompt text", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        slashCommands: ["model", "goal"],
        bridgeCanUseTool: () => null,
      });
      yield* bridge.session.offer({
        ...userMessage("/model claude-opus-5-5"),
        uuid: "00000000-0000-4000-8000-000000000001",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "command",
        name: "model",
        args: "claude-opus-5-5",
        promptUuid: "00000000-0000-4000-8000-000000000001",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("runs a line that starts with ! as a shell command in the session", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        modVersion: "0.3.4",
        bridgeCanUseTool: () => null,
      });
      yield* bridge.session.offer({
        ...userMessage("! git status --short"),
        uuid: "00000000-0000-4000-8000-000000000011",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "bash",
        command: "git status --short",
        promptUuid: "00000000-0000-4000-8000-000000000011",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("sends such a line as written to a session whose mod cannot run it", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        modVersion: "0.3.3",
        bridgeCanUseTool: () => null,
      });
      yield* bridge.session.offer({
        ...userMessage("! git status --short"),
        uuid: "00000000-0000-4000-8000-000000000012",
      });
      const sent = yield* Queue.take(bridge.commands);
      assert.strictEqual(sent.type, "prompt");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("sends text that only looks like a command to the model as written", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        slashCommands: ["model"],
        bridgeCanUseTool: () => null,
      });
      yield* bridge.session.offer({
        ...userMessage("/Users/jack/notes.md is stale"),
        uuid: "00000000-0000-4000-8000-000000000002",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "prompt",
        text: " /Users/jack/notes.md is stale",
        promptUuid: "00000000-0000-4000-8000-000000000002",
      });
      yield* bridge.session.offer({
        ...userMessage("/nosuchcommand now"),
        uuid: "00000000-0000-4000-8000-000000000003",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "prompt",
        text: " /nosuchcommand now",
        promptUuid: "00000000-0000-4000-8000-000000000003",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("says so when the session still runs a mod that cannot run commands", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 1, bridgeCanUseTool: () => null });
      const result = yield* Effect.result(
        bridge.session.offer({
          ...userMessage("/compact"),
          uuid: "00000000-0000-4000-8000-000000000004",
        }),
      );
      assert.strictEqual(result._tag, "Failure");
      assert.strictEqual(yield* Queue.size(bridge.commands), 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner turns that follow at once", () => {
  // A message queued behind a turn starts the next one the instant the first
  // ends, before the first has been closed out.
  it.live("closes the turn that ended before starting the one that follows it", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
      yield* bridge.handler(
        { ...base, type: "turn.start", turnId: "turn-a", text: "first" },
        bridge.process,
      );
      yield* bridge.handler(
        {
          ...base,
          seq: 2,
          type: "turn.complete",
          turnId: "turn-a",
          reason: "answer",
          answer: "first done",
          isAborted: false,
          durationMs: 10,
        },
        bridge.process,
      );
      yield* bridge.handler(
        { ...base, seq: 3, type: "turn.start", turnId: "turn-b", text: "second" },
        bridge.process,
      );
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(5), Stream.runCollect),
      );
      assert.deepStrictEqual(
        frames.map((frame) => frame.type),
        ["user", "system", "result", "user", "system"],
      );
      const result = frames[2];
      assert.strictEqual(
        result?.type === "result" && result.subtype === "success" ? result.result : null,
        "first done",
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner messages the terminal drew", () => {
  // Claude Code shows text written before a tool call and, in a turn that
  // also thinks, does not keep it in the record. The mod reports each message
  // as drawn; the thread takes it from there when the record has none.
  const row = (uuid: string, content: ReadonlyArray<Record<string, unknown>>) =>
    `${JSON.stringify({
      type: "assistant",
      uuid,
      parentUuid: null,
      isSidechain: false,
      timestamp: "2026-07-01T10:00:01.000Z",
      message: {
        id: "msg_1",
        type: "message",
        role: "assistant",
        model: "claude-sonnet-4-6",
        content,
        stop_reason: "tool_use",
        usage: { input_tokens: 1, output_tokens: 1 },
      },
    })}\n`;
  const TEXT_ROW = "00000000-0000-4000-8000-0000000000b1";
  const TOOL_ROW = "00000000-0000-4000-8000-0000000000b2";
  const DRAWN = "44444444-4444-4444-8444-444444444444";
  const toolUse = row(TOOL_ROW, [
    { type: "tool_use", id: "toolu_1", name: "Bash", input: { command: "ls" } },
  ]);
  const start = (bridge: { readonly now: number }) => ({
    seq: 1,
    at: bridge.now,
    type: "turn.start" as const,
    turnId: "turn-a",
    text: "go",
    transcriptOffset: 0,
  });
  const drawn = (bridge: { readonly now: number }, text: string) => ({
    seq: 2,
    at: bridge.now,
    type: "message.display" as const,
    turnId: "turn-a",
    messageId: DRAWN,
    text,
  });
  const shape = (frames: ReadonlyArray<SDKMessage>) =>
    frames.map((frame) =>
      frame.type === "assistant"
        ? `${frame.uuid}:${frame.message.content.map((block) => block.type).join("+")}`
        : frame.type,
    );

  it.live("shows one the record does not hold, before the tool call it was written before", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      yield* bridge.handler(start(bridge), bridge.process);
      yield* bridge.handler(drawn(bridge, "About to list the files."), bridge.process);
      yield* fs.writeFileString(bridge.process.session.transcriptPath, toolUse, { flag: "a" });
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(4), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(frames), [
        "user",
        "system",
        `${DRAWN}:text`,
        `${TOOL_ROW}:tool_use`,
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("leaves one the record holds to the record, whichever arrives first", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const file = bridge.process.session.transcriptPath;
      yield* bridge.handler(start(bridge), bridge.process);
      // Drawn first, then its row.
      yield* bridge.handler(drawn(bridge, "About to list the files."), bridge.process);
      yield* fs.writeFileString(
        file,
        row(TEXT_ROW, [{ type: "text", text: "About to list the files." }]) + toolUse,
        { flag: "a" },
      );
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(4), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(frames), [
        "user",
        "system",
        `${TEXT_ROW}:text`,
        `${TOOL_ROW}:tool_use`,
      ]);
      // Its row first, then drawn: nothing more before the turn ends.
      yield* bridge.handler(
        { ...drawn(bridge, "About to list the files."), seq: 3 },
        bridge.process,
      );
      yield* bridge.handler(
        {
          seq: 4,
          at: bridge.now,
          type: "turn.complete",
          turnId: "turn-a",
          reason: "answer",
          answer: "",
          isAborted: false,
          durationMs: 10,
          transcriptOffset: 0,
        },
        bridge.process,
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(next), ["result"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
  it.live("shows one once when the record's copy of it arrives seconds later", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const file = bridge.process.session.transcriptPath;
      yield* bridge.handler(start(bridge), bridge.process);
      yield* bridge.handler(drawn(bridge, "About to list the files."), bridge.process);
      // Nothing in the record yet: the drawn message is shown after its wait.
      const first = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(3), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(first), ["user", "system", `${DRAWN}:text`]);
      yield* fs.writeFileString(
        file,
        row(TEXT_ROW, [{ type: "text", text: "About to list the files." }]) + toolUse,
        { flag: "a" },
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(next), [`${TOOL_ROW}:tool_use`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner missed turns", () => {
  const reply = JSON.stringify({
    type: "assistant",
    uuid: "00000000-0000-4000-8000-0000000000a1",
    parentUuid: null,
    isSidechain: false,
    requestId: "req_1",
    timestamp: "2026-07-01T10:00:01.000Z",
    message: {
      id: "msg_1",
      type: "message",
      role: "assistant",
      model: "claude-sonnet-4-6",
      content: [{ type: "text", text: "quick answer" }],
      stop_reason: "end_turn",
      usage: { input_tokens: 3, output_tokens: 2 },
    },
  });
  const transcript = `${reply}\n`;
  const turn = (now: number, endedAgoMs: number): ClaudeBridgeRegistry.ClaudeBridgeMissedTurn => ({
    start: {
      type: "turn.start",
      seq: 1,
      at: now - endedAgoMs - 1_000,
      turnId: "turn-quick",
      text: "quick question",
      transcriptOffset: 0,
    },
    complete: {
      type: "turn.complete",
      seq: 2,
      at: now - endedAgoMs,
      turnId: "turn-quick",
      reason: "answer",
      answer: "quick answer",
      isAborted: false,
      durationMs: 1_000,
      transcriptOffset: Buffer.byteLength(transcript),
    },
  });
  const framesUntilResult = (bridge: {
    readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession;
  }) =>
    bridge.session.messages.pipe(
      Stream.takeUntil((message) => message.type === "result"),
      Stream.runCollect,
      Effect.map((frames) => Array.from(frames)),
    );

  // The runner reads the transcript from disk on its own clock; these wait
  // for the frames it produces, on the live clock.
  it.live("shows a turn that was over before the attachment, from its start", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript,
        missedTurn: (now) => turn(now, 500),
      });
      const frames = yield* framesUntilResult(bridge);
      const texts = frames.flatMap((frame) =>
        frame.type === "assistant"
          ? frame.message.content.flatMap((block) => (block.type === "text" ? [block.text] : []))
          : [],
      );
      assert.deepStrictEqual(texts, ["quick answer"]);
      assert.strictEqual(frames.at(-1)?.type, "result");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("leaves an older finished turn to the thread's history", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript,
        missedTurn: (now) => turn(now, 60_000),
      });
      assert.isFalse(yield* bridge.session.hasPendingWake ?? Effect.succeed(true));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

const image = {
  type: "image" as const,
  source: { type: "base64" as const, media_type: "image/png" as const, data: "iVBORw0KGgo=" },
};

describe("claudeBridgePromptText", () => {
  it("passes a string prompt through and joins text blocks", () => {
    assert.strictEqual(claudeBridgePromptText(userMessage("hello")), "hello");
    assert.strictEqual(
      claudeBridgePromptText(
        userMessage([
          { type: "text", text: "first" },
          { type: "text", text: "second" },
        ]),
      ),
      "first\n\nsecond",
    );
  });

  it("replaces image blocks with one note that points the model at the saved files", () => {
    const text = claudeBridgePromptText(
      userMessage([
        image,
        image,
        {
          type: "text",
          text: 'Compare these.\n\n[Attached image "a.png" is saved at: /tmp/a.png]',
        },
      ]),
    );
    assert.strictEqual(
      text,
      `Compare these.\n\n[Attached image "a.png" is saved at: /tmp/a.png]\n\n${claudeBridgeImageNote(2)}`,
    );
    assert.match(claudeBridgeImageNote(2), /2 images/);
    assert.match(claudeBridgeImageNote(1), /an image/);
    assert.match(claudeBridgeImageNote(1), /Read tool/);
  });

  it("sends the note alone for an image-only message, and nothing for tool results", () => {
    assert.strictEqual(claudeBridgePromptText(userMessage([image])), claudeBridgeImageNote(1));
    assert.strictEqual(
      claudeBridgePromptText(
        userMessage([{ type: "tool_result", tool_use_id: "toolu_1", content: "done" }]),
      ),
      null,
    );
  });
});
