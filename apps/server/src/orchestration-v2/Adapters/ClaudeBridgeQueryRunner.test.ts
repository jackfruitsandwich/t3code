import type { PermissionResult, SDKUserMessage } from "@anthropic-ai/claude-agent-sdk";
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

describe("ClaudeBridgeQueryRunner permissions", () => {
  const cases: ReadonlyArray<{
    name: string;
    result: PermissionResult | null;
    expected: ClaudeBridgeCommandBody;
  }> = [
    {
      name: "returns an unowned request to native permission handling",
      result: null,
      expected: { type: "approval", toolUseId: "tool-1", decision: "ask" },
    },
    {
      name: "preserves a real denial and its reason",
      result: { behavior: "deny", message: "The user denied this command." },
      expected: {
        type: "approval",
        toolUseId: "tool-1",
        decision: "deny",
        reason: "The user denied this command.",
      },
    },
    {
      name: "forwards an approved request",
      result: { behavior: "allow" },
      expected: { type: "approval", toolUseId: "tool-1", decision: "allow" },
    },
  ];

  it.effect.each(cases)("$name", (testCase) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      const root = yield* fs.makeTempDirectoryScoped({ prefix: "t3-bridge-permission-" });
      const transcriptPath = `${root}/transcript.jsonl`;
      yield* fs.writeFileString(transcriptPath, "");
      const now = yield* Clock.currentTimeMillis;
      const process: ClaudeBridgeRegistry.ClaudeBridgeProcess = {
        sid: "bridge-permission-session",
        instanceId: "bridge-permission-instance",
        dir: root,
        session: {
          protocol: 1,
          sid: "bridge-permission-session",
          instanceId: "bridge-permission-instance",
          cwd: root,
          configDir: root,
          transcriptPath,
          surfaces: ["terminal"],
          isInteractive: true,
          startedAt: now,
          heartbeatAt: now,
          model: "claude-sonnet-4-6",
          activeTurn: null,
          modVersion: "test",
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
        send: (_process, command) => Queue.offer(commands, command).pipe(Effect.as("command-1")),
        heartbeat: () => Effect.void,
        clearHeartbeat: () => Effect.void,
        subscribe: (_sid, handler) => Deferred.succeed(subscribed, handler).pipe(Effect.asVoid),
        onUnattachedEvent: () => Effect.void,
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
      let sdkCalls = 0;
      const bridgeCalls: Array<{
        tool: string;
        input: Record<string, unknown>;
        toolUseId: string;
      }> = [];
      const session = yield* runner.open({
        threadId: ThreadId.make("bridge-permission-thread"),
        providerSessionId: ProviderSessionId.make("bridge-permission-provider-session"),
        options: {
          resume: process.sid,
          cwd: root,
          model: "claude-sonnet-4-6",
          tools: [],
          permissionMode: "default",
          canUseTool: () => {
            sdkCalls += 1;
            return Promise.resolve({ behavior: "deny", message: "SDK callback must not run." });
          },
        },
        bridgeCanUseTool: (tool, input, options) => {
          bridgeCalls.push({ tool, input, toolUseId: options.toolUseID });
          return Promise.resolve(testCase.result);
        },
      });
      yield* Effect.addFinalizer(() => session.close.pipe(Effect.orDie));
      const handler = yield* Deferred.await(subscribed);
      yield* handler(
        {
          type: "approval.request",
          seq: 1,
          at: now,
          toolUseId: "tool-1",
          tool: "Bash",
          input: { command: "pwd" },
        },
        process,
      );
      assert.deepStrictEqual(yield* Queue.take(commands), testCase.expected);
      assert.deepStrictEqual(bridgeCalls, [
        { tool: "Bash", input: { command: "pwd" }, toolUseId: "tool-1" },
      ]);
      assert.strictEqual(sdkCalls, 0);
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
