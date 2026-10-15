import type { PermissionResult, SDKMessage, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import { ProviderSessionId, ThreadId } from "@t3tools/contracts";
import { HostProcessEnvironment, HostProcessWorkingDirectory } from "@t3tools/shared/hostProcess";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import * as ClaudeBridgeRegistry from "../../claude-bridge/ClaudeBridgeRegistry.ts";
import {
  claudeBridgeCompactSummary,
  claudeBridgeSettledElsewhere,
  claudeBridgeWakeDetail,
  claudeBridgeWakeRequest,
  claudeContinuedInSession,
  type ClaudeBridgeCommandBody,
  claudeBridgeApproveWithMode,
  claudeBridgeReplacedDrawnId,
  isClaudeBridgeNoticeFrame,
} from "../../claude-bridge/ClaudeBridgeProtocol.ts";
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
  /** What the session lists in its file as waiting for T3's answer. */
  readonly waiting?: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"]["waiting"];
  /** Another thread already holds the session this one's conversation continued in. */
  readonly heldElsewhere?: boolean;
  /** The turn the session is running when the runner attaches. */
  readonly activeTurn?: ClaudeBridgeRegistry.ClaudeBridgeProcess["session"]["activeTurn"];
  /** Whether the session's file reads as unreadable (caught mid-write) at the read numbered `read`, from 1. */
  readonly unreadable?: (read: number) => boolean;
  /** What the session's mod reports of its effort and status line. */
  readonly reported?: Pick<
    ClaudeBridgeRegistry.ClaudeBridgeProcess["session"],
    "effort" | "sessionEffort" | "usage"
  >;
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
        ...input.reported,
        commandsRev: 1,
        activeTurn: input.activeTurn ?? null,
        ...(input.waiting === undefined ? {} : { waiting: input.waiting }),
        modVersion: input.modVersion ?? "test",
        ended: null,
      },
    };
    const commands = yield* Queue.unbounded<ClaudeBridgeCommandBody>();
    const recordChanges: Array<ClaudeBridgeRegistry.ClaudeBridgeRecordChange> = [];
    /** The rows the runner left to the record to place, by uuid. */
    const leftToRecord: Array<string> = [];
    const subscribed = yield* Deferred.make<ClaudeBridgeRegistry.ClaudeBridgeEventHandler>();
    /** The sessions whose live process the runner asked for. */
    const asked: Array<string> = [];
    let sessionReads = 0;
    const registry = ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
      bridgeDir: root,
      plugin: { marketplaceDir: root, pluginDir: root },
      isPluginInstalled: () => Effect.succeed(true),
      listLiveProcesses: Effect.succeed([process]),
      findLiveProcess: (sid) =>
        Effect.sync(() => {
          asked.push(sid);
          return process;
        }),
      // The process keeps reporting, however far a test moves its clock.
      readSession: () =>
        Effect.suspend(() => {
          sessionReads += 1;
          return input.unreadable?.(sessionReads) === true
            ? Effect.succeed(null)
            : Clock.currentTimeMillis.pipe(
                Effect.map((heartbeatAt) => ({ ...process.session, heartbeatAt })),
              );
        }),
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
      onUnattachedRequest: () => Effect.void,
      onRecordChange: () => Effect.void,
      noteRecordChange: (_process, change, rows) =>
        Effect.sync(() => {
          recordChanges.push(change);
          leftToRecord.push(...(rows ?? []));
        }),
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
      // The record of the session the test attaches to says where its conversation went.
      continuationOf: (sid) =>
        sid !== process.sid
          ? Effect.succeed(null)
          : fs.readFileString(transcriptPath).pipe(
              Effect.map((record) => {
                const to = claudeContinuedInSession(record);
                return to === null
                  ? null
                  : { sid: to, heldElsewhere: input.heldElsewhere === true };
              }),
              Effect.orElseSucceed(() => null),
            ),
      liveSessionsContinuedIn: () => Effect.succeed([]),
      judgeContinuations: () => Effect.void,
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
    /** What the runner said the session did with messages sent to steer its turn, in order. */
    const deliveries: Array<string> = [];
    /** The permission mode each request was said to offer with an approval. */
    const offeredModes: Array<string | undefined> = [];
    /** What the runner passed on of the session's effort and status line, in order. */
    const nativeStates = yield* Queue.unbounded<ClaudeAdapterV2.ClaudeBridgeNativeState>();
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
        offeredModes.push(claudeBridgeApproveWithMode(options));
        return Promise.resolve(
          input.bridgeCanUseTool(tool, toolInput, options.toolUseID, options.signal),
        );
      },
      bridgeOnPromptDelivery: (delivery) =>
        Effect.sync(() => {
          deliveries.push(`${delivery.delivery} ${delivery.promptUuid}`);
        }),
      bridgeOnNativeState: (state) => {
        Queue.offerUnsafe(nativeStates, state);
      },
    });
    yield* Effect.addFinalizer(() => session.close.pipe(Effect.orDie));
    const handler = yield* Deferred.await(subscribed);
    return {
      session,
      handler,
      commands,
      process,
      bridgeCalls,
      counts,
      now,
      recordChanges,
      leftToRecord,
      asked,
      deliveries,
      offeredModes,
      nativeStates,
    };
  });

/** Everything the thread is given, as it arrives. */
const shownFrames = (bridge: { readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession }) =>
  Effect.gen(function* () {
    const frames: Array<SDKMessage> = [];
    yield* bridge.session.messages.pipe(
      Stream.runForEach((frame) =>
        Effect.sync(() => {
          frames.push(frame);
        }),
      ),
      Effect.forkScoped,
    );
    return frames;
  });

const until = (predicate: () => boolean, label: string): Effect.Effect<void> =>
  Effect.gen(function* () {
    for (let attempt = 0; attempt < 5000; attempt += 1) {
      if (predicate()) return;
      yield* Effect.yieldNow;
    }
    return yield* Effect.die(`Timed out waiting for ${label}.`);
  });

/** The time a record row carries, as Claude Code writes it. A turn's rows are stamped inside the turn. */
const stamp = (atMs: number) => DateTime.formatIso(DateTime.makeUnsafe(atMs));

describe("ClaudeBridgeQueryRunner permissions", () => {
  const cases: ReadonlyArray<{
    name: string;
    result: PermissionResult;
    expected: ReadonlyArray<ClaudeBridgeCommandBody>;
  }> = [
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

  // The dialog's "Yes, and switch to auto mode", and a plan's "Yes, and use
  // auto mode": the approval carries the mode change, whether or not the
  // session suggested any rules for the call.
  it.effect("hands an approval that switches the permission mode to the session as it is", () =>
    Effect.gen(function* () {
      const toAuto = { type: "setMode" as const, mode: "auto", destination: "session" as const };
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () =>
          ({ behavior: "allow", updatedPermissions: [toAuto], mode: "auto" }) as PermissionResult,
      });
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "plan-1",
          tool: "ExitPlanMode",
          input: { plan: "1. do it" },
          alsoInTerminal: true,
          approveWithMode: "auto",
        },
        bridge.process,
      );
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "plan-1",
        decision: "allow",
        updatedPermissions: [toAuto],
      });
      // The person was offered what the session said it offers.
      assert.deepStrictEqual(bridge.offeredModes, ["auto"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Claude Code's dialog has one refusal, "No": the call is refused and the
  // turn ends. Either refusal from T3 is that answer when the dialog is open.
  it.effect.each([
    { name: "declined", result: { behavior: "deny", message: "User declined tool execution." } },
    {
      name: "cancelled",
      result: { behavior: "deny", message: "User cancelled tool execution.", interrupt: true },
    },
  ] satisfies ReadonlyArray<{ name: string; result: PermissionResult }>)(
    "refuses through Claude Code's own dialog when one is open ($name)",
    (testCase) =>
      Effect.gen(function* () {
        const bridge = yield* attach({
          protocol: 2,
          modVersion: "0.3.7",
          bridgeCanUseTool: () => testCase.result,
        });
        yield* bridge.handler(
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
        assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
          type: "approval",
          toolUseId: "tool-1",
          decision: "deny",
          reason: testCase.result.message,
          stopTurn: true,
        });
        // No separate stop follows: the dialog's answer already ended the turn.
        yield* bridge.handler(
          {
            type: "approval.request",
            seq: 2,
            at: bridge.now,
            toolUseId: "tool-2",
            tool: "Bash",
            input: { command: "ls" },
            alsoInTerminal: true,
          },
          bridge.process,
        );
        assert.strictEqual((yield* Queue.take(bridge.commands)).type, "approval");
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // The dialog's other refusal, "No, and tell Claude what to do differently":
  // the call is refused with the person's words and the turn goes on.
  it.effect("refuses with the person's words without ending the turn", () =>
    Effect.gen(function* () {
      const refusal = ClaudeAdapterV2.permissionResultFromDecision({
        toolName: "Bash",
        decision: "decline",
        toolInput: { command: "pwd" },
        toolUseID: "tool-1",
        note: "list the files instead",
      });
      const bridge = yield* attach({
        protocol: 2,
        modVersion: "0.3.7",
        bridgeCanUseTool: () => refusal,
      });
      yield* bridge.handler(
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
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-1",
        decision: "deny",
        reason: ClaudeAdapterV2.claudeRefusalWithNote("list the files instead"),
      });
      // Nothing follows it: no stop is sent after the refusal.
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 2,
          at: bridge.now,
          toolUseId: "tool-2",
          tool: "Bash",
          input: { command: "ls" },
          alsoInTerminal: true,
        },
        bridge.process,
      );
      assert.strictEqual((yield* Queue.take(bridge.commands)).type, "approval");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("refuses and then stops, as before, for a session whose mod cannot do that", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        modVersion: "0.3.6",
        bridgeCanUseTool: () => ({
          behavior: "deny",
          message: "User cancelled tool execution.",
          interrupt: true,
        }),
      });
      yield* bridge.handler(
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
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-1",
        decision: "deny",
        reason: "User cancelled tool execution.",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), { type: "interrupt" });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("sends nothing back for a request the session settled without T3", () =>
    Effect.gen(function* () {
      let settledElsewhere = false;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, _input, toolUseId, signal) =>
          toolUseId === "tool-1"
            ? // The person has not answered in T3; the wait ends only when aborted.
              new Promise<PermissionResult>((resolve) => {
                signal.addEventListener("abort", () => {
                  settledElsewhere = claudeBridgeSettledElsewhere(signal);
                  resolve({ behavior: "deny", message: "User cancelled tool execution." });
                });
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
      const answeredInTerminal = (toolUseId: string) =>
        bridge.handler(
          {
            type: "approval.resolved",
            seq: 2,
            at: bridge.now,
            toolUseId,
            decision: "allow",
            by: "terminal",
          },
          bridge.process,
        );
      yield* request("tool-1");
      yield* until(() => bridge.bridgeCalls.length === 1, "the request asked");
      yield* answeredInTerminal("tool-1");
      // The wait is ended as settled, which is what puts the request away in the thread.
      assert.isTrue(settledElsewhere);

      // Read together with its settling (T3 was not reading for a while), a
      // request is not asked at all.
      yield* request("tool-0");
      yield* answeredInTerminal("tool-0");

      yield* request("tool-2");
      // The first thing T3 says is about the last request: the settled ones got no reply.
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-2",
        decision: "allow",
      });
      assert.deepStrictEqual(
        bridge.bridgeCalls.map((call) => call.toolUseId),
        ["tool-1", "tool-2"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps its one request when the mod loses sight of T3 for a moment", () =>
    Effect.gen(function* () {
      // The person answers in T3 only when the test says so.
      let answerNow: (result: PermissionResult) => void = () => {};
      const answered = new Promise<PermissionResult>((resolve) => {
        answerNow = resolve;
      });
      let asked = 0;
      let aborted = false;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, _input, _toolUseId, signal) => {
          asked += 1;
          signal.addEventListener("abort", () => {
            aborted = true;
          });
          return answered;
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
      answerNow({ behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "tool-1",
        decision: "allow",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // A subagent sent to the background outlives the turn that started it, and
  // its request with it. The main loop's own request cannot.
  it.effect("keeps a subagent's request open when the turn that started it ends", () =>
    Effect.gen(function* () {
      let answerNow: (result: PermissionResult) => void = () => {};
      const answered = new Promise<PermissionResult>((resolve) => {
        answerNow = resolve;
      });
      const aborted: Array<string> = [];
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: (_tool, _input, toolUseId, signal) => {
          signal.addEventListener("abort", () => {
            aborted.push(toolUseId);
          });
          return toolUseId === "agent-tool" ? answered : new Promise(() => {});
        },
      });
      const ask = (seq: number, toolUseId: string, agentType?: string) =>
        bridge.handler(
          {
            type: "approval.request",
            seq,
            at: bridge.now,
            toolUseId,
            tool: "Bash",
            input: { command: toolUseId },
            alsoInTerminal: true,
            ...(agentType === undefined ? {} : { agentType }),
          },
          bridge.process,
        );
      yield* ask(1, "main-tool");
      yield* ask(2, "agent-tool", "general-purpose");
      yield* Effect.yieldNow;
      yield* bridge.handler(
        {
          type: "turn.complete",
          seq: 3,
          at: bridge.now,
          turnId: "turn-1",
          reason: "answer",
          answer: "The subagent is still running.",
          isAborted: false,
          durationMs: 1,
          transcriptOffset: 0,
        },
        bridge.process,
      );
      assert.deepStrictEqual(aborted, ["main-tool"]);
      answerNow({ behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

// The thread shows a request inside a run. A subagent sent to the background
// asks after the turn that started it has ended and closed: the session has
// no turn, the thread no run. T3 then opens a turn of its own for the request.
describe("ClaudeBridgeQueryRunner requests with no turn open", () => {
  const REQUEST_TEXT = "The general-purpose subagent is asking for permission";
  const ask = (
    bridge: {
      readonly handler: ClaudeBridgeRegistry.ClaudeBridgeEventHandler;
      readonly process: ClaudeBridgeRegistry.ClaudeBridgeProcess;
      readonly now: number;
    },
    seq = 1,
  ) =>
    bridge.handler(
      {
        type: "approval.request",
        seq,
        at: bridge.now,
        toolUseId: "agent-tool",
        tool: "Bash",
        input: { command: "echo late" },
        alsoInTerminal: true,
        agentType: "general-purpose",
      },
      bridge.process,
    );
  /** No run is there for the first try; from the second on the person is asked, and answers when the test says. */
  const personAnswersLater = () => {
    let answerNow: (result: PermissionResult) => void = () => {};
    const answered = new Promise<PermissionResult>((resolve) => {
      answerNow = resolve;
    });
    const seen = { calls: 0, aborted: 0 };
    const bridgeCanUseTool = (
      _tool: string,
      _input: Record<string, unknown>,
      _toolUseId: string,
      signal: AbortSignal,
    ): PermissionResult | null | Promise<PermissionResult | null> => {
      seen.calls += 1;
      if (seen.calls === 1) return null;
      return new Promise((resolve) => {
        signal.addEventListener("abort", () => {
          seen.aborted += 1;
          resolve({ behavior: "deny", message: "User cancelled tool execution." });
        });
        void answered.then(resolve);
      });
    };
    return { seen, bridgeCanUseTool, answer: (result: PermissionResult) => answerNow(result) };
  };
  const types = (frames: ReadonlyArray<SDKMessage>) => frames.map((frame) => frame.type);
  const closesEmpty = (frame: SDKMessage | undefined) =>
    frame?.type === "result" && frame.subtype === "success" && frame.result === "";

  it.effect.each([
    {
      name: "accepted",
      result: { behavior: "allow" },
      sent: { type: "approval", toolUseId: "agent-tool", decision: "allow" },
    },
    {
      name: "declined",
      result: { behavior: "deny", message: "User declined tool execution." },
      sent: {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "deny",
        reason: "User declined tool execution.",
      },
    },
  ] satisfies ReadonlyArray<{
    name: string;
    result: PermissionResult;
    sent: ClaudeBridgeCommandBody;
  }>)("opens a turn for the request, and closes it once it is answered ($name)", (testCase) =>
    Effect.gen(function* () {
      const person = personAnswersLater();
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: person.bridgeCanUseTool });
      const frames = yield* shownFrames(bridge);
      yield* ask(bridge);
      yield* until(() => frames.length === 2, "the request's turn");
      // Opened as a turn typed in the terminal is, marked as nobody's prompt.
      assert.deepStrictEqual(types(frames), ["user", "system"]);
      assert.strictEqual(claudeBridgeWakeDetail(frames[0]!), REQUEST_TEXT);
      assert.strictEqual(claudeBridgeWakeRequest(frames[0]!), "subagent");
      yield* TestClock.adjust("300 millis");
      yield* until(() => person.seen.calls === 2, "the request asked again");
      // Asked and not answered yet: the turn stays open.
      assert.lengthOf(frames, 2);
      person.answer(testCase.result);
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), testCase.sent);
      yield* until(() => frames.length === 3, "the request's turn closing");
      assert.isTrue(closesEmpty(frames[2]));
      assert.strictEqual(person.seen.aborted, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("puts the request away and closes its turn when the session settles it", () =>
    Effect.gen(function* () {
      const person = personAnswersLater();
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: person.bridgeCanUseTool });
      const frames = yield* shownFrames(bridge);
      yield* ask(bridge);
      yield* until(() => frames.length === 2, "the request's turn");
      yield* TestClock.adjust("300 millis");
      yield* until(() => person.seen.calls === 2, "the request asked again");
      yield* bridge.handler(
        {
          type: "approval.resolved",
          seq: 2,
          at: bridge.now,
          toolUseId: "agent-tool",
          decision: "allow",
          by: "terminal",
        },
        bridge.process,
      );
      yield* until(() => frames.length === 3, "the request's turn closing");
      assert.isTrue(closesEmpty(frames[2]));
      assert.strictEqual(person.seen.aborted, 1);
      // Nothing is sent back for it.
      assert.strictEqual(yield* Queue.size(bridge.commands), 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("gives way to a turn of the session's own, where the request is asked again", () =>
    Effect.gen(function* () {
      const person = personAnswersLater();
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: person.bridgeCanUseTool });
      const frames = yield* shownFrames(bridge);
      yield* ask(bridge);
      yield* until(() => frames.length === 2, "the request's turn");
      yield* TestClock.adjust("300 millis");
      yield* until(() => person.seen.calls === 2, "the request asked again");
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "turn.start",
          turnId: "turn-next",
          text: "what the person typed",
          transcriptOffset: 0,
        },
        bridge.process,
      );
      yield* until(() => frames.length === 5, "the session's own turn");
      // The request's turn is closed before the next one opens.
      assert.deepStrictEqual(types(frames), ["user", "system", "result", "user", "system"]);
      assert.isTrue(closesEmpty(frames[2]));
      assert.isNull(claudeBridgeWakeRequest(frames[3]!));
      assert.strictEqual(person.seen.aborted, 1);
      assert.strictEqual(yield* Queue.size(bridge.commands), 0);
      // The mod repeats a request that is still open: asked again, in the turn now open.
      for (let repeat = 0; repeat < 50 && person.seen.calls < 3; repeat += 1) {
        yield* ask(bridge, 3 + repeat);
        yield* Effect.yieldNow;
      }
      assert.strictEqual(person.seen.calls, 3);
      person.answer({ behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
      // No turn of T3's own was opened for it this time.
      assert.lengthOf(frames, 5);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("keeps a request no turn has taken yet for the turn that follows", () =>
    Effect.gen(function* () {
      let calls = 0;
      let aborted = 0;
      const bridge = yield* attach({
        protocol: 2,
        // Nothing takes it until the session's own turn is open.
        bridgeCanUseTool: (_tool, _input, _toolUseId, signal) => {
          calls += 1;
          signal.addEventListener("abort", () => {
            aborted += 1;
          });
          return calls < 3 ? null : { behavior: "allow" };
        },
      });
      const frames = yield* shownFrames(bridge);
      yield* ask(bridge);
      yield* until(() => frames.length === 2, "the request's turn");
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "turn.start",
          turnId: "turn-next",
          text: "what the person typed",
          transcriptOffset: 0,
        },
        bridge.process,
      );
      yield* until(() => frames.length === 5, "the session's own turn");
      assert.strictEqual(aborted, 0);
      yield* TestClock.adjust("300 millis");
      yield* until(() => calls >= 2, "the second try");
      yield* TestClock.adjust("300 millis");
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
      assert.strictEqual(aborted, 0);
      assert.lengthOf(frames, 5);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("opens no turn of its own while a prompt sent from T3 is about to start one", () =>
    Effect.gen(function* () {
      let calls = 0;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => {
          calls += 1;
          return calls === 1 ? null : { behavior: "allow" };
        },
      });
      const frames = yield* shownFrames(bridge);
      yield* bridge.session.offer({
        ...userMessage("carry on"),
        uuid: "00000000-0000-4000-8000-000000000031",
      });
      assert.strictEqual((yield* Queue.take(bridge.commands)).type, "prompt");
      yield* ask(bridge);
      yield* until(() => calls === 1, "the first try");
      yield* Effect.yieldNow;
      yield* TestClock.adjust("300 millis");
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
      assert.lengthOf(frames, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("hands the request back to Claude Code's own dialog when no run takes it", () =>
    Effect.gen(function* () {
      let calls = 0;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => {
          calls += 1;
          return null;
        },
      });
      const frames = yield* shownFrames(bridge);
      yield* ask(bridge);
      yield* until(() => frames.length === 2, "the request's turn");
      yield* TestClock.adjust("16 seconds");
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "ask",
      });
      assert.isAtLeast(calls, 2);
      yield* until(() => frames.length === 3, "the request's turn closing");
      assert.isTrue(closesEmpty(frames[2]));
      assert.strictEqual(bridge.counts.sdkCalls, 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner a run started for what the session is waiting to ask", () => {
  const waiting = [
    { toolUseId: "agent-tool", kind: "approval", agentType: "general-purpose" },
  ] as const;
  const closesEmpty = (frame: SDKMessage | undefined) =>
    frame?.type === "result" && frame.subtype === "success" && frame.result === "";
  /** The run found nothing buffered for it and asks whether anything is coming. */
  const anythingComing = (bridge: {
    readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession;
  }) => bridge.session.hasPendingWake ?? Effect.succeed(false);

  it.effect("stays open for the request, asks it there and closes once it is answered", () =>
    Effect.gen(function* () {
      let answerNow: (result: PermissionResult) => void = () => {};
      const answered = new Promise<PermissionResult>((resolve) => {
        answerNow = resolve;
      });
      const bridge = yield* attach({
        protocol: 2,
        waiting,
        bridgeCanUseTool: () => answered,
      });
      const frames = yield* shownFrames(bridge);
      assert.isTrue(yield* anythingComing(bridge));
      // The run is the request's already: nothing asks for another one.
      assert.lengthOf(frames, 0);
      // The session sees T3 attached and tells the request itself.
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "agent-tool",
          tool: "Bash",
          input: { command: "echo late" },
          alsoInTerminal: true,
          agentType: "general-purpose",
        },
        bridge.process,
      );
      yield* until(() => bridge.bridgeCalls.length === 1, "the request asked");
      assert.lengthOf(frames, 0);
      answerNow({ behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
      yield* until(() => frames.length === 1, "the request's turn closing");
      assert.isTrue(closesEmpty(frames[0]));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect(
    "closes again when the session never tells the request (it was settled meanwhile)",
    () =>
      Effect.gen(function* () {
        const bridge = yield* attach({
          protocol: 2,
          waiting,
          bridgeCanUseTool: () => ({ behavior: "allow" }),
        });
        const frames = yield* shownFrames(bridge);
        assert.isTrue(yield* anythingComing(bridge));
        yield* TestClock.adjust("14 seconds");
        assert.lengthOf(frames, 0);
        yield* TestClock.adjust("2 seconds");
        yield* until(() => frames.length === 1, "the unused turn closing");
        assert.isTrue(closesEmpty(frames[0]));
        assert.lengthOf(bridge.bridgeCalls, 0);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.effect("is an empty run as before when the session lists nothing", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => ({ behavior: "allow" }),
      });
      const frames = yield* shownFrames(bridge);
      assert.isFalse(yield* anythingComing(bridge));
      yield* TestClock.adjust("20 seconds");
      assert.lengthOf(frames, 0);
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

  it.effect("hands a question no turn of the thread takes back to the terminal dialog", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null });
      const frames = yield* shownFrames(bridge);
      yield* bridge.handler(
        { type: "question.request", seq: 1, at: bridge.now, toolUseId: "ask-2", questions: [] },
        bridge.process,
      );
      yield* until(() => frames.length === 2, "the turn opened for the question");
      yield* TestClock.adjust("16 seconds");
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

  // /clear, /resume, /branch move the terminal on to another conversation; a
  // thread is one conversation. Sent from T3 they are not run, and say so.
  const describe = (frame: unknown) => JSON.stringify(frame).slice(0, 300);

  it.live(
    "does not run a command that moves the session to another conversation, and says why",
    () =>
      Effect.gen(function* () {
        const bridge = yield* attach({
          protocol: 2,
          slashCommands: ["clear", "model"],
          bridgeCanUseTool: () => null,
        });
        yield* bridge.session.offer({
          ...userMessage("/clear"),
          uuid: "00000000-0000-4000-8000-000000000031",
        });
        const frames = Array.from(
          yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
        );
        const result = frames[1];
        // The thread already holds what the person sent: the turn opens and ends.
        assert.deepStrictEqual(
          frames.map((frame) => frame.type),
          ["system", "result"],
        );
        assert.strictEqual(
          result?.type === "result" && result.subtype === "success"
            ? result.result.slice(0, 46)
            : describe(result),
          "Not run: /clear moves this Claude Code session",
        );
        // Nothing went to the session; the next command does.
        yield* bridge.session.offer({
          ...userMessage("/model sonnet"),
          uuid: "00000000-0000-4000-8000-000000000032",
        });
        assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
          type: "command",
          name: "model",
          args: "sonnet",
          promptUuid: "00000000-0000-4000-8000-000000000032",
        });
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // A skill that runs in the background starts an agent and no turn. Its
  // first request can reach T3 while the command's own run is still open.
  it.effect("keeps a command's run open while the agent it started asks in it", () =>
    Effect.gen(function* () {
      let answerNow: (result: PermissionResult) => void = () => {};
      const answered = new Promise<PermissionResult>((resolve) => {
        answerNow = resolve;
      });
      const bridge = yield* attach({
        protocol: 2,
        slashCommands: ["code-review"],
        bridgeCanUseTool: () => answered,
      });
      const frames = yield* shownFrames(bridge);
      const promptUuid = "00000000-0000-4000-8000-000000000041";
      yield* bridge.session.offer({ ...userMessage("/code-review"), uuid: promptUuid });
      yield* Queue.take(bridge.commands);
      yield* bridge.handler(
        {
          type: "approval.request",
          seq: 1,
          at: bridge.now,
          toolUseId: "agent-tool",
          tool: "Bash",
          input: { command: "git status" },
          alsoInTerminal: true,
          agentType: "code-review",
        },
        bridge.process,
      );
      // Nothing is asked before the command's own line is shown, as in the terminal.
      yield* Effect.yieldNow;
      assert.lengthOf(bridge.bridgeCalls, 0);
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "command.result",
          commandId: "command-1",
          promptUuid,
          ok: true,
          startedTurn: false,
          waitingInTerminal: false,
          interim: false,
          text: "",
        },
        bridge.process,
      );
      yield* TestClock.adjust("3 seconds");
      yield* until(() => bridge.bridgeCalls.length === 1, "the request asked");
      // The command has run; the request waits in its run, which stays open.
      assert.deepStrictEqual(
        frames.map((frame) => frame.type),
        ["system"],
      );
      answerNow({ behavior: "allow" });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "approval",
        toolUseId: "agent-tool",
        decision: "allow",
      });
      yield* TestClock.adjust("3 seconds");
      yield* until(() => frames.some((frame) => frame.type === "result"), "the run ending");
      assert.strictEqual(frames.filter((frame) => frame.type === "result").length, 1);
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

  it.live("marks a message sent to steer the running turn, and no other", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null });
      yield* bridge.session.offer({
        ...userMessage("also check the logs"),
        uuid: "00000000-0000-4000-8000-000000000013",
        priority: "now",
      });
      const steer = yield* Queue.take(bridge.commands);
      assert.strictEqual(steer.type === "prompt" ? steer.midTurn : undefined, true);
      yield* bridge.session.offer({
        ...userMessage("and then the tests"),
        uuid: "00000000-0000-4000-8000-000000000014",
      });
      const plain = yield* Queue.take(bridge.commands);
      assert.strictEqual(plain.type, "prompt");
      assert.isFalse("midTurn" in plain);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Claude Code sent the session to the background: the conversation carries
  // on under another id, and the process this thread knows was left behind.
  const saidRow = (uuid: string, text: string) =>
    `${JSON.stringify({ type: "user", uuid, timestamp: "2026-07-01T10:00:01.000Z", message: { role: "user", content: text } })}\n`;
  const continuedInRow = `${JSON.stringify({ type: "continued-in", sessionId: "old", continuedInSessionId: "99999999-9999-4999-8999-999999999999" })}\n`;
  const continuedTo = "99999999-9999-4999-8999-999999999999";
  it.live("opens the session its conversation continued in", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript: saidRow("00000000-0000-4000-8000-0000000000d1", "before") + continuedInRow,
      });
      assert.isAbove(bridge.asked.length, 0);
      assert.deepStrictEqual(Array.from(new Set(bridge.asked)), [continuedTo]);
      yield* bridge.session.offer({
        ...userMessage("are you there"),
        uuid: "00000000-0000-4000-8000-000000000041",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "prompt",
        text: "are you there",
        promptUuid: "00000000-0000-4000-8000-000000000041",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("sends nothing into a session whose continuation another thread holds", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        heldElsewhere: true,
        transcript: saidRow("00000000-0000-4000-8000-0000000000d1", "before") + continuedInRow,
      });
      assert.deepStrictEqual(Array.from(new Set(bridge.asked)), ["bridge-session"]);
      assert.isFalse(yield* bridge.session.continuedElsewhere ?? Effect.succeed(true));
      yield* bridge.session.offer({
        ...userMessage("are you there"),
        uuid: "00000000-0000-4000-8000-000000000041",
      });
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      const result = frames[1];
      const said = result?.type === "result" && result.subtype === "success" ? result.result : "";
      assert.strictEqual(
        said.slice(0, 96),
        "Not sent: this conversation was sent to the background in its terminal and carries on as Claude ",
        describe(result),
      );
      assert.include(said, continuedTo);
      assert.include(said, "has its own thread here");
      // The old session taken up again in its own right (rows after the mark) is sent to as before.
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        saidRow("00000000-0000-4000-8000-0000000000d2", "resumed"),
        { flag: "a" },
      );
      yield* bridge.session.offer({
        ...userMessage("and now"),
        uuid: "00000000-0000-4000-8000-000000000042",
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.commands), {
        type: "prompt",
        text: "and now",
        promptUuid: "00000000-0000-4000-8000-000000000042",
      });
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("says its conversation moved on once the record does, and sends nothing after it", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript: saidRow("00000000-0000-4000-8000-0000000000d1", "before"),
      });
      const movedOn = bridge.session.continuedElsewhere ?? Effect.succeed(false);
      assert.isFalse(yield* movedOn);
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(bridge.process.session.transcriptPath, continuedInRow, {
        flag: "a",
      });
      // The adapter lets go of such a session before the next turn; a prompt
      // that still lands here is not sent into the process left behind.
      assert.isTrue(yield* movedOn);
      yield* bridge.session.offer({
        ...userMessage("are you there"),
        uuid: "00000000-0000-4000-8000-000000000041",
      });
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      const result = frames[1];
      const said = result?.type === "result" && result.subtype === "success" ? result.result : "";
      assert.include(said, "Not sent: this conversation was just sent to the background");
      assert.include(said, continuedTo);
      assert.include(said, "This thread follows it there");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Sent to the background while a tool ran, the process reports no end for
  // the turn it was running: the new session carries the turn on.
  it.live("closes the turn its conversation left behind, so the thread can follow it", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
      yield* bridge.handler(
        { ...base, type: "turn.start", turnId: "turn-a", text: "count to 5000" },
        bridge.process,
      );
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(bridge.process.session.transcriptPath, continuedInRow, {
        flag: "a",
      });
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(3), Stream.runCollect),
      );
      assert.deepStrictEqual(
        frames.map((frame) => frame.type),
        ["user", "system", "result"],
      );
      const closed = frames[2];
      assert.strictEqual(
        closed?.type === "result" && closed.subtype === "success" ? closed.result : null,
        "",
        describe(closed),
      );
      assert.notProperty(closed, "terminal_reason");
      // The process's own late word of that turn closes nothing a second time.
      yield* bridge.handler(
        {
          ...base,
          seq: 2,
          type: "turn.complete",
          turnId: "turn-a",
          reason: "aborted",
          answer: "",
          isAborted: true,
          durationMs: 10,
        },
        bridge.process,
      );
      yield* Effect.sleep("900 millis");
      yield* bridge.handler(
        { ...base, seq: 3, type: "turn.start", turnId: "turn-b", text: "next" },
        bridge.process,
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      assert.deepStrictEqual(
        next.map((frame) => frame.type),
        ["user", "system"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("leaves a turn the process itself reports ended to that report", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
      yield* bridge.handler(
        { ...base, type: "turn.start", turnId: "turn-a", text: "count to 5000" },
        bridge.process,
      );
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(bridge.process.session.transcriptPath, continuedInRow, {
        flag: "a",
      });
      yield* bridge.handler(
        {
          ...base,
          seq: 2,
          type: "turn.complete",
          turnId: "turn-a",
          reason: "aborted",
          answer: "1 2 3",
          isAborted: true,
          durationMs: 10,
        },
        bridge.process,
      );
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(3), Stream.runCollect),
      );
      const closed = frames[2];
      assert.strictEqual(
        closed?.type === "result" && closed.subtype === "success" ? closed.result : null,
        "1 2 3",
        describe(closed),
      );
      yield* Effect.sleep("2600 millis");
      yield* bridge.handler(
        { ...base, seq: 3, type: "turn.start", turnId: "turn-b", text: "next" },
        bridge.process,
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      assert.deepStrictEqual(
        next.map((frame) => frame.type),
        ["user", "system"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live(
    "keeps the turn when the session's file reads cut once, as it does caught mid-write",
    () =>
      Effect.gen(function* () {
        let tearNext = false;
        const bridge = yield* attach({
          protocol: 2,
          bridgeCanUseTool: () => null,
          transcript: "",
          unreadable: () => {
            const torn = tearNext;
            tearNext = false;
            return torn;
          },
        });
        const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
        yield* bridge.handler(
          { ...base, type: "turn.start", turnId: "turn-a", text: "count to 5" },
          bridge.process,
        );
        const opened = Array.from(
          yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
        );
        assert.deepStrictEqual(
          opened.map((frame) => frame.type),
          ["user", "system"],
        );
        // The next read of the file, and only it, comes back cut.
        tearNext = true;
        yield* Effect.sleep("1500 millis");
        assert.strictEqual(tearNext, false, "the runner read the file again meanwhile");
        yield* bridge.handler(
          {
            ...base,
            seq: 2,
            type: "turn.complete",
            turnId: "turn-a",
            reason: "answer",
            answer: "1 2 3 4 5",
            isAborted: false,
            durationMs: 10,
          },
          bridge.process,
        );
        const closed = Array.from(
          yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
        )[0];
        assert.strictEqual(
          closed?.type === "result" && closed.subtype === "success" ? closed.result : null,
          "1 2 3 4 5",
          describe(closed),
        );
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("ends the turn as the process gone when the session's file stays unreadable", () =>
    Effect.gen(function* () {
      let gone = false;
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript: "",
        unreadable: () => gone,
      });
      const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
      yield* bridge.handler(
        { ...base, type: "turn.start", turnId: "turn-a", text: "count to 5" },
        bridge.process,
      );
      yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect);
      gone = true;
      const closed = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
      )[0];
      assert.strictEqual(
        closed?.type === "result" && closed.subtype === "success" ? closed.result : null,
        "Claude Code session ended (process gone).",
        describe(closed),
      );
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

describe("ClaudeBridgeQueryRunner commands the mod writes into the record", () => {
  // `/model` sent from T3 is applied through the session's settings row,
  // which leaves no rows; the mod writes the rows the terminal's own /model
  // leaves, then reports. The thread shows the command once, and what it
  // printed is the record's own row.
  const holds = (shown: unknown, text: string) => JSON.stringify(shown).includes(text);

  it.live("shows /model sent from T3 once, with the record's row for what it printed", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        slashCommands: ["model"],
        bridgeCanUseTool: () => null,
        transcript: "",
      });
      const fs = yield* FileSystem.FileSystem;
      const promptUuid = "00000000-0000-4000-8000-000000000021";
      yield* bridge.session.offer({ ...userMessage("/model sonnet"), uuid: promptUuid });
      yield* Queue.take(bridge.commands);
      const modRow = (uuid: string, text: string) =>
        `${JSON.stringify({
          type: "user",
          uuid,
          parentUuid: null,
          isSidechain: false,
          isMeta: true,
          origin: { kind: "plugin", name: "t3bridge" },
          timestamp: stamp(bridge.now),
          message: { role: "user", content: [{ type: "text", text }] },
        })}\n`;
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        modRow(
          "00000000-0000-4000-8000-0000000000c0",
          "<local-command-caveat>The command below was run directly in Claude Code.</local-command-caveat>",
        ) +
          modRow(
            "00000000-0000-4000-8000-0000000000c1",
            "<command-name>/model</command-name>\n            <command-message>model</command-message>\n            <command-args>sonnet</command-args>",
          ) +
          modRow(
            "00000000-0000-4000-8000-0000000000c2",
            "<local-command-stdout>Set model to claude-sonnet-5-5</local-command-stdout>",
          ),
        { flag: "a" },
      );
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "command.result",
          commandId: "command-1",
          promptUuid,
          ok: true,
          startedTurn: false,
          waitingInTerminal: false,
          interim: false,
          text: "Set model to claude-sonnet-5-5",
        },
        bridge.process,
      );
      // Everything the thread is given in the next seconds: one turn.
      const frames: Array<SDKMessage> = [];
      yield* bridge.session.messages.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            frames.push(frame);
          }),
        ),
        Effect.forkScoped,
      );
      yield* Effect.sleep("3500 millis");
      assert.strictEqual(frames.filter((frame) => frame.type === "result").length, 1);
      const printed = frames.filter((frame) => holds(frame, "Set model to claude-sonnet-5-5"));
      assert.strictEqual(printed.length, 1);
      // The note for the model and the command's markup are not shown.
      assert.isFalse(holds(frames, "local-command-caveat"));
      assert.isFalse(holds(frames, "command-name"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Since Claude Code 2.1.299, `/effort <level>` mid-conversation asks in a
  // dialog in the terminal. The mod says the command waits there, and its
  // result follows once the dialog closes, when the session has written the
  // command's rows. Those rows are the command sent from T3, not a second one.
  it.live("keeps a command that waits in a terminal dialog open, and shows it once", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        slashCommands: ["effort"],
        bridgeCanUseTool: () => null,
        transcript: "",
      });
      const fs = yield* FileSystem.FileSystem;
      const promptUuid = "00000000-0000-4000-8000-000000000022";
      yield* bridge.session.offer({ ...userMessage("/effort high"), uuid: promptUuid });
      yield* Queue.take(bridge.commands);
      const frames: Array<SDKMessage> = [];
      yield* bridge.session.messages.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            frames.push(frame);
          }),
        ),
        Effect.forkScoped,
      );
      const waiting =
        "/effort high opened a panel in the Claude Code terminal and is waiting there";
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "command.result",
          commandId: "command-1",
          promptUuid,
          ok: true,
          startedTurn: false,
          waitingInTerminal: true,
          interim: true,
          text: waiting,
        },
        bridge.process,
      );
      yield* Effect.sleep("3500 millis");
      // The run is open and says where the command waits; it has not ended.
      assert.strictEqual(frames.filter((frame) => holds(frame, waiting)).length, 1);
      assert.strictEqual(frames.filter((frame) => frame.type === "result").length, 0);
      const row = (uuid: string, text: string) =>
        `${JSON.stringify({
          type: "user",
          uuid,
          parentUuid: null,
          isSidechain: false,
          timestamp: stamp(bridge.now),
          message: { role: "user", content: text },
        })}\n`;
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        row(
          "00000000-0000-4000-8000-0000000000d0",
          "<local-command-caveat>Caveat: The messages below were generated by the user while running local commands.</local-command-caveat>",
        ) +
          row(
            "00000000-0000-4000-8000-0000000000d1",
            "<command-name>/effort</command-name>\n            <command-message>effort</command-message>\n            <command-args>high</command-args>",
          ) +
          row(
            "00000000-0000-4000-8000-0000000000d2",
            "<local-command-stdout>Kept effort level as low</local-command-stdout>",
          ),
        { flag: "a" },
      );
      yield* Effect.sleep("1500 millis");
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "command.result",
          commandId: "command-1",
          promptUuid,
          ok: true,
          startedTurn: false,
          waitingInTerminal: false,
          interim: false,
          text: "Kept effort level as low",
        },
        bridge.process,
      );
      yield* Effect.sleep("5000 millis");
      // One run: what it printed once, no second prompt for the record's rows.
      assert.strictEqual(frames.filter((frame) => frame.type === "result").length, 1);
      assert.strictEqual(
        frames.filter((frame) => holds(frame, "Kept effort level as low")).length,
        1,
      );
      // A prompt the thread would show as typed in the terminal carries this key.
      assert.isFalse(holds(frames, "t3_wake_detail"));
      assert.isFalse(holds(frames, "command-name"));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner messages sent to steer a turn", () => {
  const FIRST = "00000000-0000-4000-8000-0000000000f1";
  const SECOND = "00000000-0000-4000-8000-0000000000f2";
  const steer = (
    bridge: { readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession },
    uuid: NonNullable<SDKUserMessage["uuid"]>,
    text: string,
  ) => bridge.session.offer({ ...userMessage(text), uuid, priority: "now" });
  const attached = attach({ protocol: 2, bridgeCanUseTool: () => null });

  it.live("waits until the session hands it to the model inside the turn", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* steer(bridge, FIRST, "also check the logs");
      yield* steer(bridge, SECOND, "and the config");
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`, `waiting ${SECOND}`]);
      // The mod names the prompt it handed over; the other still waits.
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "prompt.delivered",
          commandId: "command-1",
          turnId: "turn-a",
          promptUuid: SECOND,
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries.slice(2), [`taken ${SECOND}`]);
      // Said twice (an event read again): once is enough for the one that waits.
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "prompt.delivered",
          commandId: "command-1",
          turnId: "turn-a",
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries.slice(3), [`taken ${FIRST}`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("waits until its own turn starts when the turn met no tool call", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* steer(bridge, FIRST, "also check the logs");
      // A turn the terminal started is not the session taking it.
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "turn.start",
          turnId: "typed",
          text: "hi",
          transcriptOffset: 0,
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`]);
      yield* bridge.handler(
        {
          seq: 2,
          at: bridge.now,
          type: "turn.start",
          turnId: "turn-b",
          text: "also check the logs",
          transcriptOffset: 0,
          commandId: "command-1",
          promptUuid: FIRST,
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`, `taken ${FIRST}`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("is not taken when the session drops it", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* steer(bridge, FIRST, "also check the logs");
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "prompt.dropped",
          commandId: "command-1",
          reason: "The session is shutting down.",
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`, `dropped ${FIRST}`]);
      // Nothing is left to let go of when T3 stops following the session.
      yield* bridge.session.close;
      assert.strictEqual(bridge.deliveries.length, 2);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("is not taken when the session ends while it waits", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* steer(bridge, FIRST, "also check the logs");
      yield* bridge.handler(
        { seq: 1, at: bridge.now, type: "ended", reason: "exit", sessionId: "bridge-session" },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`, `dropped ${FIRST}`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("is not taken when T3 stops following the session while it waits", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* steer(bridge, FIRST, "also check the logs");
      yield* bridge.session.close;
      assert.deepStrictEqual(bridge.deliveries, [`waiting ${FIRST}`, `dropped ${FIRST}`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("says nothing of a prompt that was not sent to steer a turn", () =>
    Effect.gen(function* () {
      const bridge = yield* attached;
      yield* bridge.session.offer({ ...userMessage("a prompt of its own"), uuid: FIRST });
      assert.deepStrictEqual(bridge.deliveries, []);
      yield* bridge.session.close;
      assert.deepStrictEqual(bridge.deliveries, []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // T3 restarted while the session held the message: the attachment that
  // sent it is gone, and this one hears of a prompt it never sent.
  it.live("passes on a prompt it never sent that the session says it took", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        activeTurn: { turnId: "turn-b", startedAt: 1, transcriptOffset: 0, promptUuid: SECOND },
      });
      assert.deepStrictEqual(bridge.deliveries, [`taken ${SECOND}`]);
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "prompt.delivered",
          commandId: "command-9",
          turnId: "turn-b",
          promptUuid: FIRST,
        },
        bridge.process,
      );
      assert.deepStrictEqual(bridge.deliveries, [`taken ${SECOND}`, `taken ${FIRST}`]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner turns that follow at once", () => {
  // A message queued behind a turn starts the next one the instant the first
  // ends, before the first has been closed out.
  // A session over its limit compacts before the turn's first model call,
  // and reports the compaction before it reports the turn.
  it.live("says a turn that begins with a compaction is compacting, once the turn has opened", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const base = { seq: 1, at: bridge.now, transcriptOffset: 0 };
      yield* bridge.handler({ ...base, type: "compact.started", trigger: "auto" }, bridge.process);
      yield* bridge.handler(
        { ...base, seq: 2, type: "turn.start", turnId: "turn-a", text: "and the rest" },
        bridge.process,
      );
      yield* bridge.handler({ ...base, seq: 3, type: "compact.ended" }, bridge.process);
      const frames = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(4), Stream.runCollect),
      );
      assert.deepStrictEqual(
        frames.map((frame) =>
          frame.type === "system" && frame.subtype === "status"
            ? `status:${frame.status}`
            : frame.type,
        ),
        ["user", "system", "status:compacting", "status:null"],
      );
      // One that began and stopped between turns is no turn's state.
      yield* bridge.handler(
        {
          ...base,
          seq: 4,
          type: "turn.complete",
          turnId: "turn-a",
          reason: "answer",
          answer: "done",
          isAborted: false,
          durationMs: 10,
        },
        bridge.process,
      );
      const ended = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
      );
      assert.strictEqual(ended[0]?.type, "result");
      yield* bridge.handler(
        { ...base, seq: 5, type: "compact.started", trigger: "manual" },
        bridge.process,
      );
      yield* bridge.handler({ ...base, seq: 6, type: "compact.ended" }, bridge.process);
      yield* bridge.handler(
        { ...base, seq: 7, type: "turn.start", turnId: "turn-b", text: "next" },
        bridge.process,
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      assert.deepStrictEqual(
        next.map((frame) => frame.type),
        ["user", "system"],
      );
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

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
  const row = (uuid: string, content: ReadonlyArray<Record<string, unknown>>, atMs: number) =>
    `${JSON.stringify({
      type: "assistant",
      uuid,
      parentUuid: null,
      isSidechain: false,
      timestamp: stamp(atMs),
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
  const THOUGHT_ROW = "00000000-0000-4000-8000-0000000000b0";
  const TEXT_ROW = "00000000-0000-4000-8000-0000000000b1";
  const TOOL_ROW = "00000000-0000-4000-8000-0000000000b2";
  const DRAWN = "44444444-4444-4444-8444-444444444444";
  /** A thinking block's signature as the API writes one, with `kind` where Claude Code reads it. */
  const signatureOf = (kind: string): string => {
    const field = (number: number, body: Buffer) =>
      Buffer.concat([Buffer.from([(number << 3) | 2, body.length]), body]);
    const header = Buffer.concat([Buffer.from([0x08, 0x01]), field(8, Buffer.from(kind, "utf8"))]);
    return field(2, field(1, header)).toString("base64");
  };
  // Built when it is written, inside the turn: a row carries the time it was made.
  const toolUse = (atMs: number) =>
    row(
      TOOL_ROW,
      [{ type: "tool_use", id: "toolu_1", name: "Bash", input: { command: "ls" } }],
      atMs,
    );
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
      yield* fs.writeFileString(bridge.process.session.transcriptPath, toolUse(bridge.now), {
        flag: "a",
      });
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
        row(TEXT_ROW, [{ type: "text", text: "About to list the files." }], bridge.now) +
          toolUse(bridge.now),
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
  it.live("follows the record when the session moves its file, as /cd does", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const file = bridge.process.session.transcriptPath;
      yield* bridge.handler(start(bridge), bridge.process);
      yield* fs.writeFileString(
        file,
        row(TEXT_ROW, [{ type: "text", text: "Before the move." }], bridge.now),
        { flag: "a" },
      );
      const before = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(3), Stream.runCollect),
      );
      // The same file under another path, and the session naming it.
      const moved = file.replace("transcript.jsonl", "moved.jsonl");
      yield* fs.rename(file, moved);
      Object.assign(bridge.process.session, { transcriptPath: moved });
      yield* fs.writeFileString(moved, toolUse(bridge.now), { flag: "a" });
      const after = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(1), Stream.runCollect),
      );
      // The row read before the move is not shown again; the one written after it is.
      assert.deepStrictEqual(shape([...before, ...after]), [
        "user",
        "system",
        `${TEXT_ROW}:text`,
        `${TOOL_ROW}:tool_use`,
      ]);
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
        row(TEXT_ROW, [{ type: "text", text: "About to list the files." }], bridge.now) +
          toolUse(bridge.now),
        { flag: "a" },
      );
      // The record's row is the reply: it is shown under its own id, which is
      // what a restarted T3 would read again, and names the drawn copy whose
      // place it takes. The repair is asked for as well.
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(next), [`${TEXT_ROW}:text`, `${TOOL_ROW}:tool_use`]);
      assert.deepStrictEqual(next.map(claudeBridgeReplacedDrawnId), [DRAWN, null]);
      assert.include(bridge.recordChanges, "reply-recorded");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("leaves one to the record when a thought of its message is read before its row", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const file = bridge.process.session.transcriptPath;
      yield* bridge.handler(start(bridge), bridge.process);
      yield* bridge.handler(drawn(bridge, "About to list the files."), bridge.process);
      // A message's thought is the row before its text: it does not say the
      // record has gone on without the text, read with that row or before it.
      yield* fs.writeFileString(
        file,
        row(
          THOUGHT_ROW,
          [{ type: "thinking", thinking: "Which first?", signature: signatureOf("thinking") }],
          bridge.now,
        ),
        { flag: "a" },
      );
      const first = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(3), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(first), ["user", "system", `${THOUGHT_ROW}:thinking`]);
      yield* fs.writeFileString(
        file,
        row(TEXT_ROW, [{ type: "text", text: "About to list the files." }], bridge.now) +
          toolUse(bridge.now),
        { flag: "a" },
      );
      const next = Array.from(
        yield* bridge.session.messages.pipe(Stream.take(2), Stream.runCollect),
      );
      assert.deepStrictEqual(shape(next), [`${TEXT_ROW}:text`, `${TOOL_ROW}:tool_use`]);
      assert.deepStrictEqual(next.map(claudeBridgeReplacedDrawnId), [null, null]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("knows the record's row of what the model said between tool calls", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      yield* bridge.handler(start(bridge), bridge.process);
      yield* bridge.handler(drawn(bridge, "Tests pass; building now."), bridge.process);
      // The record keeps it as a thinking block; the thread shows it as the
      // message the terminal drew, and that row is the drawn message's own.
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        row(
          TEXT_ROW,
          [
            {
              type: "thinking",
              thinking: "Tests pass; building now.",
              signature: signatureOf("narration"),
            },
          ],
          bridge.now,
        ) + toolUse(bridge.now),
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
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner notices between turns", () => {
  // Claude Code writes a recap when the person comes back to the terminal,
  // with no turn running: no run shows it, so the thread takes it from the record.
  const recap = (uuid: string, atMs: number) =>
    `${JSON.stringify({
      type: "system",
      subtype: "away_summary",
      content: "Goal was to fix the login bug; it is fixed. (disable recaps in /config)",
      uuid,
      parentUuid: null,
      isSidechain: false,
      isMeta: false,
      timestamp: stamp(atMs),
    })}\n`;
  const noticeShown = (bridge: { readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession }) =>
    bridge.session.messages.pipe(
      Stream.filter(isClaudeBridgeNoticeFrame),
      Stream.take(1),
      Stream.runCollect,
    );

  it.live("has the thread compared with the record when one is written with no turn running", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        recap("00000000-0000-4000-8000-0000000000c1", bridge.now),
        { flag: "a" },
      );
      yield* noticeShown(bridge);
      yield* until(() => bridge.recordChanges.includes("older-rows"), "the comparison");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("leaves one written inside a turn to that turn's run", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const file = bridge.process.session.transcriptPath;
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "turn.start",
          turnId: "turn-a",
          text: "go",
          transcriptOffset: 0,
        },
        bridge.process,
      );
      yield* fs.writeFileString(file, recap("00000000-0000-4000-8000-0000000000c2", bridge.now), {
        flag: "a",
      });
      yield* noticeShown(bridge);
      // A later row is read on a later pass, after this one's is over.
      yield* fs.writeFileString(file, recap("00000000-0000-4000-8000-0000000000c3", bridge.now), {
        flag: "a",
      });
      yield* noticeShown(bridge);
      assert.notInclude(bridge.recordChanges, "older-rows");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner a /compact typed in the terminal", () => {
  // The terminal's /compact writes its boundary and summary first, then the
  // command's rows and what it printed. No turn is open then: the run that
  // shows the command shows the compaction too, with its summary.
  const line = (fields: Record<string, unknown>, atMs: number) =>
    `${JSON.stringify({ parentUuid: null, isSidechain: false, timestamp: stamp(atMs), ...fields })}\n`;
  const said = (uuid: string, text: string, atMs: number, meta = false) =>
    line(
      {
        type: "user",
        uuid,
        ...(meta ? { isMeta: true } : {}),
        message: { role: "user", content: text },
      },
      atMs,
    );

  it.live("shows the compaction and its summary in the command's run", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const at = bridge.now;
      const frames: Array<SDKMessage> = [];
      yield* bridge.session.messages.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            frames.push(frame);
          }),
        ),
        Effect.forkScoped,
      );
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        line(
          {
            type: "system",
            subtype: "compact_boundary",
            uuid: "00000000-0000-4000-8000-0000000000d0",
            content: "Conversation compacted",
            compactMetadata: { trigger: "manual", preTokens: 125550, postTokens: 2751 },
          },
          at,
        ) +
          line(
            {
              type: "user",
              uuid: "00000000-0000-4000-8000-0000000000d1",
              isCompactSummary: true,
              message: { role: "user", content: "Summary: the login bug is fixed." },
            },
            at,
          ) +
          said(
            "00000000-0000-4000-8000-0000000000d2",
            "<local-command-caveat>The command below was run directly in Claude Code.</local-command-caveat>",
            at,
            true,
          ) +
          said(
            "00000000-0000-4000-8000-0000000000d3",
            "<command-name>/compact</command-name>\n            <command-message>compact</command-message>\n            <command-args></command-args>",
            at,
          ) +
          said(
            "00000000-0000-4000-8000-0000000000d4",
            "<local-command-stdout>\u001b[2mCompacted (ctrl+o to see full summary)\u001b[22m</local-command-stdout>",
            at,
          ),
        { flag: "a" },
      );
      // The run opens once the record has been quiet a moment (LOCAL_COMMAND_QUIET_MS).
      yield* Effect.sleep("3500 millis");
      const kinds = frames.map((frame) =>
        frame.type === "system" && frame.subtype === "compact_boundary"
          ? claudeBridgeCompactSummary(frame) === undefined
            ? "compaction"
            : `compaction:${claudeBridgeCompactSummary(frame)}`
          : frame.type === "system"
            ? `system:${frame.subtype}`
            : frame.type,
      );
      // One run: it opens, shows the compaction, its summary, then what /compact printed.
      assert.deepStrictEqual(kinds.slice(kinds.indexOf("system:init")), [
        "system:init",
        "compaction",
        "compaction:Summary: the login bug is fixed.",
        "assistant",
        "result",
      ]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner a command typed in the terminal", () => {
  // Claude Code writes a typed command that starts no turn as two rows at
  // once: its line, then what it printed (2.1.300, `/focus`).
  const typed = (atMs: number) =>
    [
      {
        uuid: "00000000-0000-4000-8000-0000000000f0",
        content:
          "<command-name>/focus</command-name>\n            <command-message>focus</command-message>\n            <command-args></command-args>",
      },
      {
        uuid: "00000000-0000-4000-8000-0000000000f1",
        content: "<local-command-stdout>Focus view disabled</local-command-stdout>",
        commandRun: { command: "focus", args: "" },
      },
    ]
      .map(
        (row) =>
          `${JSON.stringify({ parentUuid: null, isSidechain: false, type: "system", subtype: "local_command", level: "info", isMeta: false, timestamp: stamp(atMs), ...row })}\n`,
      )
      .join("");
  const said = (uuid: string, text: string, atMs: number) =>
    `${JSON.stringify({ parentUuid: null, isSidechain: false, type: "user", uuid, timestamp: stamp(atMs), message: { role: "user", content: text } })}\n`;
  const replied = (uuid: string, text: string, atMs: number) =>
    `${JSON.stringify({
      type: "assistant",
      uuid,
      parentUuid: null,
      isSidechain: false,
      requestId: `req_${uuid}`,
      timestamp: stamp(atMs),
      message: {
        id: `msg_${uuid}`,
        type: "message",
        role: "assistant",
        model: "claude-sonnet-4-6",
        content: [{ type: "text", text }],
        stop_reason: "end_turn",
        usage: { input_tokens: 3, output_tokens: 2 },
      },
    })}\n`;
  const turnEnded = (uuid: string, atMs: number) =>
    `${JSON.stringify({ type: "system", subtype: "turn_duration", uuid, timestamp: stamp(atMs), durationMs: 1_000 })}\n`;
  const kinds = (frames: ReadonlyArray<SDKMessage>) =>
    frames.map((frame) => (frame.type === "system" ? `system:${frame.subtype}` : frame.type));
  const holds = (shown: unknown, text: string) => JSON.stringify(shown).includes(text);

  const typedRowUuids = ["00000000-0000-4000-8000-0000000000f1"];
  const sendAfter = (bridge: { readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession }) =>
    bridge.session.offer({
      ...userMessage("Reply with exactly: after"),
      uuid: "00000000-0000-4000-8000-0000000000f2",
    });
  const turnStarts = (
    bridge: {
      readonly handler: ClaudeBridgeRegistry.ClaudeBridgeEventHandler;
      readonly process: ClaudeBridgeRegistry.ClaudeBridgeProcess;
      readonly now: number;
    },
    turn: { readonly turnId: string; readonly text: string; readonly fromT3?: boolean },
  ) =>
    bridge.handler(
      {
        seq: 2,
        at: bridge.now,
        type: "turn.start",
        turnId: turn.turnId,
        text: turn.text,
        transcriptOffset: 0,
        ...(turn.fromT3 === true
          ? { commandId: "command-1", promptUuid: "00000000-0000-4000-8000-0000000000f2" }
          : {}),
      },
      bridge.process,
    );

  // Gate 0.0.89 (amend.py, refused.py B) and typedrace.py: a turn shown here
  // for the command sat after the message T3 sent meanwhile (its run is made
  // as it is sent), and the record's comparison showed the command again.
  it.live("leaves one to the record as it is read, and shows no turn of its own", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const frames = yield* shownFrames(bridge);
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        typed(yield* Clock.currentTimeMillis),
        { flag: "a" },
      );
      yield* Effect.sleep("700 millis");
      assert.deepStrictEqual(bridge.leftToRecord, typedRowUuids);
      assert.include(bridge.recordChanges, "commands-left-to-record");
      // Long past the quiet a held command waits for: nothing follows.
      yield* Effect.sleep("2500 millis");
      assert.deepStrictEqual(kinds(frames), []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // typedrace.py: rows read as they were written, then the server stopped
  // for 7 s; on waking, the quiet was over as T3's message arrived.
  it.live(
    "leaves one read on time to the record, so a message T3 sends after its quiet opens alone",
    () =>
      Effect.gen(function* () {
        const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
        const fs = yield* FileSystem.FileSystem;
        const frames = yield* shownFrames(bridge);
        yield* fs.writeFileString(
          bridge.process.session.transcriptPath,
          typed(yield* Clock.currentTimeMillis),
          { flag: "a" },
        );
        yield* Effect.sleep("2 seconds");
        yield* sendAfter(bridge);
        yield* turnStarts(bridge, {
          turnId: "turn-after",
          text: "Reply with exactly: after",
          fromT3: true,
        });
        yield* Effect.sleep("1 second");
        assert.deepStrictEqual(bridge.leftToRecord, typedRowUuids);
        // The message's turn opens (T3 shows the message itself) with nothing
        // of the command in it, and no turn of the command's own comes before or after.
        assert.deepStrictEqual(kinds(frames), ["system:init"]);
        assert.isFalse(holds(frames, "Focus view disabled"));
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // typedrace.py t3: the record's turn ended and the command came after it,
  // all read in one pass while that turn's end was still to come here. The
  // record places the command once that turn has closed here, after its rows.
  it.live(
    "leaves one typed after a turn the record has closed to the record once that turn closes here",
    () =>
      Effect.gen(function* () {
        const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
        const fs = yield* FileSystem.FileSystem;
        const frames = yield* shownFrames(bridge);
        yield* turnStarts(bridge, { turnId: "turn-one", text: "Reply with exactly: one" });
        const now = yield* Clock.currentTimeMillis;
        yield* fs.writeFileString(
          bridge.process.session.transcriptPath,
          replied("00000000-0000-4000-8000-0000000000e1", "one", now) +
            turnEnded("00000000-0000-4000-8000-0000000000e2", now) +
            typed(now),
          { flag: "a" },
        );
        yield* Effect.sleep("700 millis");
        assert.isTrue(holds(frames, '"one"'));
        assert.isFalse(holds(frames, "Focus view disabled"));
        assert.deepStrictEqual(bridge.leftToRecord, []);
        yield* bridge.handler(
          {
            type: "turn.complete",
            seq: 3,
            at: bridge.now,
            turnId: "turn-one",
            reason: "answer",
            answer: "one",
            isAborted: false,
            durationMs: 1,
            transcriptOffset: 0,
          },
          bridge.process,
        );
        yield* Effect.sleep("1500 millis");
        assert.strictEqual(kinds(frames).at(-1), "result");
        assert.isFalse(holds(frames, "Focus view disabled"));
        assert.deepStrictEqual(bridge.leftToRecord, typedRowUuids);
      }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("shows one written inside a turn the record is still running in that turn", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const frames = yield* shownFrames(bridge);
      yield* turnStarts(bridge, { turnId: "turn-one", text: "Reply with exactly: one" });
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        typed(yield* Clock.currentTimeMillis),
        { flag: "a" },
      );
      yield* Effect.sleep("700 millis");
      assert.isTrue(holds(frames, "Focus view disabled"));
      assert.deepStrictEqual(bridge.leftToRecord, []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // The adapter learns a goal from what /goal printed, inside a run.
  it.live("holds what a typed /goal printed for the turn it starts", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const frames = yield* shownFrames(bridge);
      const now = yield* Clock.currentTimeMillis;
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        said(
          "00000000-0000-4000-8000-0000000000a8",
          "<command-name>/goal</command-name>\n            <command-message>goal</command-message>\n            <command-args>ship it</command-args>",
          now,
        ) +
          said(
            "00000000-0000-4000-8000-0000000000a9",
            "<local-command-stdout>Goal set: ship it</local-command-stdout>",
            now,
          ),
        { flag: "a" },
      );
      yield* Effect.sleep("700 millis");
      assert.deepStrictEqual(kinds(frames), []);
      yield* turnStarts(bridge, { turnId: "turn-goal", text: "ship it" });
      yield* Effect.sleep("300 millis");
      assert.deepStrictEqual(kinds(frames), ["user", "system:init", "assistant"]);
      assert.isTrue(holds(frames, "Goal set: ship it"));
      assert.deepStrictEqual(bridge.leftToRecord, []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner a prompt's hook result", () => {
  // Claude Code writes a prompt's hook result as the prompt goes in, and the
  // mod's word that the turn began can come after the row is read.
  const hookError = (uuid: string, atMs: number) =>
    `${JSON.stringify({
      type: "attachment",
      uuid,
      parentUuid: null,
      isSidechain: false,
      timestamp: stamp(atMs),
      attachment: {
        type: "hook_non_blocking_error",
        hookName: "UserPromptSubmit",
        hookEvent: "UserPromptSubmit",
        stderr: "lint-hook-failed",
        stdout: "",
        exitCode: 1,
      },
    })}\n`;
  const collect = (bridge: { readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession }) =>
    Effect.gen(function* () {
      const frames: Array<SDKMessage> = [];
      yield* bridge.session.messages.pipe(
        Stream.runForEach((frame) =>
          Effect.sync(() => {
            frames.push(frame);
          }),
        ),
        Effect.forkScoped,
      );
      return frames;
    });
  const kinds = (frames: ReadonlyArray<SDKMessage>) =>
    frames.map((frame) =>
      isClaudeBridgeNoticeFrame(frame)
        ? "notice"
        : frame.type === "system"
          ? `system:${frame.subtype}`
          : frame.type,
    );

  it.live("keeps one read before its turn began for that turn", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const frames = yield* collect(bridge);
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        hookError("00000000-0000-4000-8000-0000000000e0", bridge.now),
        { flag: "a" },
      );
      yield* Effect.sleep("800 millis");
      assert.deepStrictEqual(kinds(frames), []);
      yield* bridge.handler(
        {
          seq: 1,
          at: bridge.now,
          type: "turn.start",
          turnId: "turn-a",
          text: "go",
          transcriptOffset: 0,
        },
        bridge.process,
      );
      yield* Effect.sleep("300 millis");
      assert.deepStrictEqual(kinds(frames), ["user", "system:init", "notice"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("lets one go on as before when no turn follows", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({ protocol: 2, bridgeCanUseTool: () => null, transcript: "" });
      const fs = yield* FileSystem.FileSystem;
      const frames = yield* collect(bridge);
      yield* fs.writeFileString(
        bridge.process.session.transcriptPath,
        hookError("00000000-0000-4000-8000-0000000000e1", bridge.now),
        { flag: "a" },
      );
      yield* Effect.sleep("6500 millis");
      assert.deepStrictEqual(kinds(frames), ["notice"]);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner what the session reports of itself", () => {
  const usage = (tokens: number, fiveHour: number) => ({
    context: { tokens, window: 200_000, percent: Math.round((tokens / 200_000) * 100) },
    rateLimits: [{ kind: "five_hour", percentUsed: fiveHour, resetsAt: "2026-10-14T15:00:00Z" }],
    cost: { usd: 1.25 },
  });
  const report = (
    bridge: { readonly process: ClaudeBridgeRegistry.ClaudeBridgeProcess },
    reported: Partial<ClaudeBridgeRegistry.ClaudeBridgeProcess["session"]>,
  ) => Object.assign(bridge.process, { session: { ...bridge.process.session, ...reported } });
  const recapRow = (uuid: string, atMs: number) =>
    `${JSON.stringify({
      type: "system",
      subtype: "away_summary",
      content: "Back to the login bug.",
      uuid,
      parentUuid: null,
      isSidechain: false,
      isMeta: false,
      timestamp: stamp(atMs),
    })}\n`;
  /**
   * A row the tail shows as a notice, waited for: the pass that read it read
   * the session's report first.
   */
  const passRead = (
    bridge: {
      readonly process: ClaudeBridgeRegistry.ClaudeBridgeProcess;
      readonly session: ClaudeAdapterV2.ClaudeAgentSdkQuerySession;
      readonly now: number;
    },
    uuid: string,
  ) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(bridge.process.session.transcriptPath, recapRow(uuid, bridge.now), {
        flag: "a",
      });
      yield* bridge.session.messages.pipe(
        Stream.filter(isClaudeBridgeNoticeFrame),
        Stream.take(1),
        Stream.runCollect,
      );
    });

  it.live("passes on its effort and status line at attach, and again only when they change", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript: "",
        reported: { effort: "high", sessionEffort: "low", usage: usage(40_000, 12) },
      });
      assert.deepStrictEqual(yield* Queue.take(bridge.nativeStates), {
        sessionEffort: "low",
        lastTurnEffort: "high",
        usage: usage(40_000, 12),
      });
      report(bridge, { effort: "low", usage: usage(52_000, 13) });
      assert.deepStrictEqual(yield* Queue.take(bridge.nativeStates), {
        sessionEffort: "low",
        lastTurnEffort: "low",
        usage: usage(52_000, 13),
      });
      // Later passes read the same report and pass on nothing.
      yield* passRead(bridge, "00000000-0000-4000-8000-0000000000d1");
      yield* passRead(bridge, "00000000-0000-4000-8000-0000000000d2");
      assert.strictEqual(yield* Queue.size(bridge.nativeStates), 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("passes on nothing from a mod that does not report it", () =>
    Effect.gen(function* () {
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        reported: { effort: "high" },
        transcript: "",
      });
      yield* passRead(bridge, "00000000-0000-4000-8000-0000000000d3");
      assert.strictEqual(yield* Queue.size(bridge.nativeStates), 0);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});

describe("ClaudeBridgeQueryRunner missed turns", () => {
  const replyRow = (uuid: string, text: string, at: number) =>
    `${JSON.stringify({
      type: "assistant",
      uuid,
      parentUuid: null,
      isSidechain: false,
      requestId: `req_${uuid}`,
      timestamp: stamp(at),
      message: {
        id: `msg_${uuid}`,
        type: "message",
        role: "assistant",
        model: "claude-sonnet-4-6",
        content: [{ type: "text", text }],
        stop_reason: "end_turn",
        usage: { input_tokens: 3, output_tokens: 2 },
      },
    })}\n`;
  /** The record of a turn that has just ended: its one reply, stamped inside it. */
  const recordAt = (now: number) =>
    replyRow("00000000-0000-4000-8000-0000000000a1", "quick answer", now);
  const turnEndRow = (uuid: string, at: number) =>
    `${JSON.stringify({ type: "system", subtype: "turn_duration", uuid, timestamp: stamp(at), durationMs: 1_000 })}\n`;
  const turn = (
    now: number,
    endedAgoMs: number,
    transcript: string,
  ): ClaudeBridgeRegistry.ClaudeBridgeMissedTurn => ({
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
      const transcript = recordAt(yield* Clock.currentTimeMillis);
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript,
        missedTurn: (now) => turn(now, 500, transcript),
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
      const transcript = recordAt(yield* Clock.currentTimeMillis);
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript,
        missedTurn: (now) => turn(now, 60_000, transcript),
      });
      assert.isFalse(yield* bridge.session.hasPendingWake ?? Effect.succeed(true));
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  // Claude Code continuing a session under a new id (one sent to the
  // background) starts the first turn and only then copies the old
  // conversation into the new record: rows older than the turn, after the
  // place the turn starts at.
  it.live("leaves another session's rows, copied in after the turn began, to history", () =>
    Effect.gen(function* () {
      const now = yield* Clock.currentTimeMillis;
      const transcript =
        replyRow("00000000-0000-4000-8000-0000000000a2", "said in the old session", now - 120_000) +
        turnEndRow("00000000-0000-4000-8000-0000000000a3", now - 119_000) +
        replyRow("00000000-0000-4000-8000-0000000000a1", "quick answer", now);
      const bridge = yield* attach({
        protocol: 2,
        bridgeCanUseTool: () => null,
        transcript,
        missedTurn: (at) => turn(at, 500, transcript),
      });
      const frames = yield* framesUntilResult(bridge);
      const texts = frames.flatMap((frame) =>
        frame.type === "assistant"
          ? frame.message.content.flatMap((block) => (block.type === "text" ? [block.text] : []))
          : [],
      );
      assert.deepStrictEqual(texts, ["quick answer"]);
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
