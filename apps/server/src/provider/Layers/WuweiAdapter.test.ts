// @effect-diagnostics nodeBuiltinImport:off
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import { createModelSelection } from "@t3tools/shared/model";
import {
  ProviderDriverKind,
  ProviderInstanceId,
  type ProviderRuntimeEvent,
  ThreadId,
  WuweiSettings,
} from "@t3tools/contracts";

import { ServerConfig } from "../../config.ts";
import type { ProviderAdapterError } from "../Errors.ts";
import type { ProviderAdapterShape } from "../Services/ProviderAdapter.ts";
import { makeWuweiAdapter } from "./WuweiAdapter.ts";

const decodeWuweiSettings = Schema.decodeSync(WuweiSettings);

class WuweiAdapter extends Context.Service<
  WuweiAdapter,
  ProviderAdapterShape<ProviderAdapterError>
>()("t3/provider/Layers/WuweiAdapter.test/WuweiAdapter") {}

// Stands in for wuwei's src/acp/main.ts: one prompt produces thinking, a bash
// tool call, an answer, and per-step usage, and the prompt request is logged.
const FAKE_AGENT = String.raw`
import { appendFileSync } from "node:fs";
import { createInterface } from "node:readline";
const log = process.env.WUWEI_FAKE_LOG;
const send = (message) => process.stdout.write(JSON.stringify({ jsonrpc: "2.0", ...message }) + "\n");
const update = (sessionId, payload) => send({ method: "session/update", params: { sessionId, update: payload } });
createInterface({ input: process.stdin }).on("line", (line) => {
  const message = JSON.parse(line);
  if (message.id === undefined) return;
  const params = message.params ?? {};
  switch (message.method) {
    case "initialize":
      return send({ id: message.id, result: { protocolVersion: 1, agentCapabilities: { loadSession: true }, authMethods: [{ id: "wuwei", name: "wuwei" }] } });
    case "authenticate":
    case "session/load":
      return send({ id: message.id, result: {} });
    case "session/new":
      return send({ id: message.id, result: { sessionId: "fake-thread-1" } });
    case "session/prompt": {
      appendFileSync(log, JSON.stringify(params) + "\n");
      const id = params.sessionId;
      update(id, { sessionUpdate: "agent_thought_chunk", content: { type: "text", text: "checking the folder" } });
      update(id, { sessionUpdate: "tool_call", toolCallId: "call-1", title: "pwd", kind: "execute", status: "in_progress", rawInput: { command: "pwd" } });
      update(id, { sessionUpdate: "tool_call_update", toolCallId: "call-1", status: "completed", content: [{ type: "content", content: { type: "text", text: "/tmp/project" } }] });
      update(id, { sessionUpdate: "agent_message_chunk", content: { type: "text", text: "It is /tmp/project." } });
      update(id, { sessionUpdate: "usage_update", used: 1200, size: 372000, _meta: { wuwei: { input: 1000, cacheRead: 150, cacheWrite: 0, output: 50, reasoning: 10 } } });
      return send({ id: message.id, result: { stopReason: "end_turn", usage: { inputTokens: 1150, outputTokens: 50, cachedReadTokens: 150, cachedWriteTokens: 0, thoughtTokens: 10, totalTokens: 1200 } } });
    }
    default:
      return send({ id: message.id, error: { code: -32601, message: "method not found: " + message.method } });
  }
});
`;

const fakeAgent = Effect.promise(async () => {
  const dir = await NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "wuwei-acp-fake-"));
  const entryPath = NodePath.join(dir, "fake-wuwei.mjs");
  const logPath = NodePath.join(dir, "prompts.ndjson");
  await NodeFSP.writeFile(entryPath, FAKE_AGENT, "utf8");
  await NodeFSP.writeFile(logPath, "", "utf8");
  return { entryPath, logPath };
});

const wuweiAdapterTestLayer = it.layer(
  Layer.effect(
    WuweiAdapter,
    Effect.gen(function* () {
      const { entryPath, logPath } = yield* fakeAgent;
      const settings = decodeWuweiSettings({
        binaryPath: process.execPath,
        entryPath,
      });
      return yield* makeWuweiAdapter(settings, {
        environment: { ...process.env, WUWEI_FAKE_LOG: logPath },
      }).pipe(Effect.tap(() => Effect.sync(() => (lastLogPath = logPath))));
    }),
  ).pipe(
    Layer.provideMerge(
      ServerConfig.layerTest(process.cwd(), { prefix: "t3code-wuwei-adapter-test-" }),
    ),
    Layer.provideMerge(NodeServices.layer),
  ),
);

let lastLogPath = "";

wuweiAdapterTestLayer("WuweiAdapter", (it) => {
  it.effect("sends model and effort on the prompt and maps wuwei's stream to runtime events", () =>
    Effect.gen(function* () {
      const adapter = yield* WuweiAdapter;
      const threadId = ThreadId.make("wuwei-fake-thread");
      const instanceId = ProviderInstanceId.make("wuwei");
      const events = yield* adapter.streamEvents.pipe(
        Stream.takeUntil((event) => event.type === "turn.completed"),
        Stream.runCollect,
        Effect.forkChild,
      );

      const session = yield* adapter.startSession({
        threadId,
        provider: ProviderDriverKind.make("wuwei"),
        cwd: process.cwd(),
        runtimeMode: "full-access",
      });
      assert.deepStrictEqual(session.resumeCursor, {
        schemaVersion: 1,
        sessionId: "fake-thread-1",
      });

      yield* adapter.sendTurn({
        threadId,
        input: "where am I?",
        attachments: [],
        modelSelection: createModelSelection(instanceId, "gpt-6-astra", [
          { id: "reasoningEffort", value: "high" },
        ]),
      });
      const collected: ReadonlyArray<ProviderRuntimeEvent> = Array.from(yield* Fiber.join(events));

      const prompts = (yield* Effect.promise(() => NodeFSP.readFile(lastLogPath, "utf8")))
        .trim()
        .split("\n")
        .map((line) => JSON.parse(line));
      assert.deepStrictEqual(prompts[0]._meta, { wuwei: { model: "gpt-6-astra", effort: "high" } });
      // The model gets the user's text only: no T3 runtime instructions ride along.
      assert.deepStrictEqual(prompts[0].prompt, [{ type: "text", text: "where am I?" }]);

      const deltas = collected.flatMap((event) =>
        event.type === "content.delta" ? [[event.payload.streamKind, event.payload.delta]] : [],
      );
      assert.deepStrictEqual(deltas, [
        ["reasoning_text", "checking the folder"],
        ["assistant_text", "It is /tmp/project."],
      ]);

      // Thinking reaches the work log as an inert task row once its block ends.
      const thinking = collected.find((event) => event.type === "task.progress");
      assert.isDefined(thinking);
      if (thinking?.type === "task.progress") {
        assert.deepInclude(thinking.payload, {
          description: "Thinking",
          summary: "checking the folder",
          taskType: "dream",
        });
      }

      const tool = collected.find(
        (event) =>
          event.type === "item.completed" && event.payload.itemType === "command_execution",
      );
      assert.isDefined(tool);

      const usage = collected.find((event) => event.type === "thread.token-usage.updated");
      assert.isDefined(usage);
      if (usage?.type === "thread.token-usage.updated") {
        assert.deepInclude(usage.payload.usage, {
          usedTokens: 1200,
          maxTokens: 372000,
          lastInputTokens: 1150,
          lastCachedInputTokens: 150,
          lastOutputTokens: 50,
          lastReasoningOutputTokens: 10,
        });
      }

      const completed = collected.at(-1);
      assert.equal(completed?.type, "turn.completed");
      if (completed?.type === "turn.completed") {
        assert.equal(completed.payload.state, "completed");
        assert.deepInclude(completed.payload.tokenUsage, {
          inputTokens: 1150,
          cachedInputTokens: 150,
          outputTokens: 50,
        });
      }
      yield* adapter.stopSession(threadId);
    }),
  );
});
