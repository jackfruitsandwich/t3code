// @effect-diagnostics nodeBuiltinImport:off - writes the fake wuwei bundle the adapter imports.
import * as NodeFS from "node:fs";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";

import { afterAll, assert, describe, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  NodeId,
  ProviderInstanceId,
  ProviderSessionId,
  ProviderThreadId,
  RunAttemptId,
  RunId,
  ThreadId,
  WuweiSettings,
  type MessageId,
  type ModelSelection,
  type OrchestrationV2AppThread,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2TurnItem,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as Stream from "effect/Stream";

import * as ServerConfig from "../../config.ts";
import type {
  WuweiHost,
  WuweiPromptInput,
  WuweiPromptResult,
  WuweiTurnEvent,
} from "../../provider/wuwei/wuweiEmbedApi.ts";
import * as IdAllocator from "../IdAllocator.ts";
import {
  ProviderAdapterV2Event,
  ProviderAdapterV2RuntimePolicy,
  type ProviderAdapterV2SessionRuntime,
} from "../ProviderAdapter.ts";
import { makeWuweiAdapterV2, WUWEI_PROVIDER } from "./WuweiAdapterV2.ts";

// The adapter imports wuwei's bundle by path and keeps the first host it loads
// for the whole module instance, so this file registers one fake host up
// front and each test resets what it does.
const HOST_KEY = Symbol.for("t3code.test.wuweiAdapterHost");
const bundleDir = NodeFS.mkdtempSync(NodePath.join(NodeOS.tmpdir(), "t3-wuwei-adapter-"));
const BUNDLE_PATH = NodePath.join(bundleDir, "embed.mjs");
NodeFS.writeFileSync(
  BUNDLE_PATH,
  `export const EMBED_API_VERSION = 1;
export async function loadWuwei() {
  const host = globalThis[Symbol.for("t3code.test.wuweiAdapterHost")];
  if (host === undefined) throw new Error("no fake wuwei host registered");
  return host;
}
`,
);
afterAll(() => NodeFS.rmSync(bundleDir, { recursive: true, force: true }));

const ZERO_TOTALS = { input: 0, cacheRead: 0, cacheWrite: 0, output: 0, reasoning: 0 };

type PromptScript = (input: WuweiPromptInput) => Promise<WuweiPromptResult>;

interface FakeWuwei {
  chats: Set<string>;
  created: Array<{ readonly id: string; readonly cwd: string }>;
  prompts: Array<WuweiPromptInput>;
  script: PromptScript;
}

const fake: FakeWuwei = {
  chats: new Set(),
  created: [],
  prompts: [],
  script: () => Promise.reject(new Error("no prompt script")),
};

const fakeHost: WuweiHost = {
  apiVersion: 1,
  threadDir: "/fake/.wuwei/sessions",
  catalog: async () => ({
    models: [
      {
        slug: "gpt-6-sol",
        name: "GPT-6 Sol",
        backend: "codex-direct",
        backendLabel: "Codex",
        reasoningEfforts: ["low", "medium", "high"],
        contextWindow: 372_000,
      },
    ],
    warnings: [],
  }),
  commands: () => [],
  authStatus: async () => ({
    codex: "Signed in",
    anthropic: "Signed in",
    codexReady: true,
    anthropicReady: true,
  }),
  createThread: (cwd) => {
    const id = `2026-10-06-12-00-0${fake.created.length}-fake`;
    fake.created.push({ id, cwd });
    fake.chats.add(id);
    return id;
  },
  hasThread: (threadId) => fake.chats.has(threadId),
  prompt: (input) => {
    fake.prompts.push(input);
    return fake.script(input);
  },
  transcript: (threadId) => {
    throw new Error(`no session file for ${threadId}`);
  },
  listThreads: () => [],
};
(globalThis as Record<symbol, unknown>)[HOST_KEY] = fakeHost;

const resetFake = (script: PromptScript) => {
  fake.chats = new Set();
  fake.created = [];
  fake.prompts = [];
  fake.script = script;
};

/** A prompt that replays `events` in order, then settles with `result`. */
const scripted =
  (events: ReadonlyArray<WuweiTurnEvent>, result: WuweiPromptResult): PromptScript =>
  async (input) => {
    for (const event of events) input.onEvent(event);
    return result;
  };

/** A prompt that runs `before`, then waits for T3 to abort it, like a long wuwei tool call. */
const untilAborted =
  (before: ReadonlyArray<WuweiTurnEvent>, onAbort: ReadonlyArray<WuweiTurnEvent>): PromptScript =>
  (input) => {
    for (const event of before) input.onEvent(event);
    return new Promise((resolve) => {
      input.signal.addEventListener("abort", () => {
        for (const event of onAbort) input.onEvent(event);
        resolve({ stopReason: "cancelled", totals: ZERO_TOTALS });
      });
    });
  };

const layerServerConfig = ServerConfig.layerTest(process.cwd(), {
  prefix: "t3-wuwei-v2-adapter-",
}).pipe(Layer.provide(NodeServices.layer));
const layerTest = Layer.mergeAll(NodeServices.layer, IdAllocator.layer, layerServerConfig);

const WUWEI_INSTANCE_ID = ProviderInstanceId.make("wuwei");
const THREAD_ID = ThreadId.make("thread-wuwei-test");
const SESSION_ID = ProviderSessionId.make("provider-session-wuwei-test");
const MODEL = "gpt-6-sol";

const runtimePolicy = ProviderAdapterV2RuntimePolicy.make({
  runtimeMode: "full-access",
  interactionMode: "default",
  cwd: null,
  reasoningEffort: "high",
});
const modelSelection: ModelSelection = { instanceId: WUWEI_INSTANCE_ID, model: MODEL };
const settings = Schema.decodeSync(WuweiSettings)({ embedPath: BUNDLE_PATH });
const isAdapterEvent = Schema.is(ProviderAdapterV2Event);

const makeAdapter = Effect.gen(function* () {
  return makeWuweiAdapterV2({
    instanceId: WUWEI_INSTANCE_ID,
    settings,
    fileSystem: yield* FileSystem.FileSystem,
    idAllocator: yield* IdAllocator.IdAllocatorV2,
    serverConfig: yield* ServerConfig.ServerConfig,
  });
});

/** Opens a session in the ambient scope and buffers everything it emits. */
const openRuntime = Effect.gen(function* () {
  const adapter = yield* makeAdapter;
  const runtime = yield* adapter.openSession({
    threadId: THREAD_ID,
    providerSessionId: SESSION_ID,
    modelSelection,
    runtimePolicy,
  });
  const emitted = yield* Queue.unbounded<ProviderAdapterV2Event>();
  yield* runtime.events.pipe(
    Stream.runForEach((event) => Queue.offer(emitted, event)),
    Effect.forkScoped,
  );
  /** Every event up to and including the first one matching `predicate`. */
  const takeThrough = (predicate: (event: ProviderAdapterV2Event) => boolean) =>
    Effect.gen(function* () {
      const taken: Array<ProviderAdapterV2Event> = [];
      while (true) {
        const event = yield* Queue.take(emitted);
        taken.push(event);
        if (predicate(event)) return taken;
      }
    });
  return { runtime, takeThrough };
});

const appThread = (now: DateTime.Utc): OrchestrationV2AppThread => ({
  createdBy: "user",
  creationSource: "web",
  id: THREAD_ID,
  projectId: "project:fixture:wuwei" as OrchestrationV2AppThread["projectId"],
  title: "wuwei test thread",
  providerInstanceId: WUWEI_INSTANCE_ID,
  modelSelection,
  runtimeMode: "full-access",
  interactionMode: "default",
  branch: null,
  worktreePath: null,
  activeProviderThreadId: null,
  lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: THREAD_ID },
  forkedFrom: null,
  createdAt: now,
  updatedAt: now,
  archivedAt: null,
  settledOverride: null,
  settledAt: null,
  lastVisitedAt: null,
  deletedAt: null,
});

const startTurn = Effect.fnUntraced(function* (
  runtime: ProviderAdapterV2SessionRuntime,
  providerThread: OrchestrationV2ProviderThread,
  runOrdinal = 1,
) {
  const runId = RunId.make(`run:${THREAD_ID}:${runOrdinal}`);
  const attemptId = RunAttemptId.make(`run-attempt:${runId}:1`);
  yield* runtime.startTurn({
    appThread: appThread(yield* DateTime.now),
    threadId: THREAD_ID,
    runId,
    runOrdinal,
    providerTurnOrdinal: runOrdinal,
    attemptId,
    rootNodeId: NodeId.make(`node:${runId}:root`),
    providerThread,
    message: {
      messageId: `message:${THREAD_ID}:${runOrdinal}` as MessageId,
      text: "Hello wuwei",
      attachments: [],
      createdBy: "user",
      creationSource: "web",
    },
    modelSelection,
    runtimePolicy,
  });
  return { runId, attemptId };
});

const isTerminal = (event: ProviderAdapterV2Event) => event.type === "turn.terminal";

/** The last update of each turn item, by native item id, in the order items first appeared. */
const finalTurnItems = (events: ReadonlyArray<ProviderAdapterV2Event>) => {
  const items = new Map<string, OrchestrationV2TurnItem>();
  for (const event of events) {
    if (event.type === "turn_item.updated") {
      items.set(event.turnItem.nativeItemRef?.nativeId ?? event.turnItem.id, event.turnItem);
    }
  }
  return items;
};

describe("WuweiAdapterV2", () => {
  it.effect("creates a wuwei chat for a new thread and adopts the row it was handed", () =>
    Effect.gen(function* () {
      resetFake(scripted([], { stopReason: "end_turn", totals: ZERO_TOTALS }));
      const serverConfig = yield* ServerConfig.ServerConfig;
      const { runtime } = yield* openRuntime;

      const created = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      assert.deepStrictEqual(fake.created, [
        { id: "2026-10-06-12-00-00-fake", cwd: serverConfig.cwd },
      ]);
      // The same id the v1 migration derives for an imported wuwei chat.
      assert.equal(
        created.id,
        IdAllocator.deriveProviderThread({
          driver: WUWEI_PROVIDER,
          nativeThreadId: "2026-10-06-12-00-00-fake",
        }),
      );
      assert.deepStrictEqual(created.nativeThreadRef, {
        driver: WUWEI_PROVIDER,
        nativeId: "2026-10-06-12-00-00-fake",
        strength: "strong",
      });
      assert.equal(created.providerSessionId, SESSION_ID);
      assert.equal(created.appThreadId, THREAD_ID);
      assert.equal(runtime.getModelContextWindow?.(modelSelection), 372_000);

      // A queued row without a native chat keeps its identity and gets a new chat.
      const queuedRow: OrchestrationV2ProviderThread = {
        ...created,
        id: ProviderThreadId.make("provider-thread:queued:wuwei"),
        providerSessionId: null,
        nativeThreadRef: null,
        status: "not_loaded",
      };
      const adopted = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
        existingProviderThread: queuedRow,
      });
      assert.equal(adopted.id, queuedRow.id);
      assert.equal(adopted.nativeThreadRef?.nativeId, "2026-10-06-12-00-01-fake");
      assert.equal(adopted.providerSessionId, SESSION_ID);
      assert.equal(adopted.status, "idle");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("resumes an existing wuwei chat and refuses one that no longer exists", () =>
    Effect.gen(function* () {
      resetFake(scripted([], { stopReason: "end_turn", totals: ZERO_TOTALS }));
      fake.chats.add("2026-10-04-05-32-44-97tc");
      const { runtime } = yield* openRuntime;
      const now = yield* DateTime.now;
      const imported: OrchestrationV2ProviderThread = {
        id: IdAllocator.deriveProviderThread({
          driver: WUWEI_PROVIDER,
          nativeThreadId: "2026-10-04-05-32-44-97tc",
        }),
        driver: WUWEI_PROVIDER,
        providerInstanceId: WUWEI_INSTANCE_ID,
        providerSessionId: null,
        appThreadId: THREAD_ID,
        ownerNodeId: null,
        nativeThreadRef: {
          driver: WUWEI_PROVIDER,
          nativeId: "2026-10-04-05-32-44-97tc",
          strength: "strong",
        },
        nativeConversationHeadRef: null,
        status: "idle",
        firstRunOrdinal: null,
        lastRunOrdinal: null,
        handoffIds: [],
        forkedFrom: null,
        pendingBackgroundTasks: [],
        createdAt: now,
        updatedAt: now,
      };

      const resumed = yield* runtime.resumeThread({ providerThread: imported });
      assert.equal(resumed.id, imported.id);
      assert.equal(resumed.nativeThreadRef?.nativeId, "2026-10-04-05-32-44-97tc");
      assert.equal(resumed.providerSessionId, SESSION_ID);
      assert.deepStrictEqual(fake.created, []);

      const missing = yield* runtime
        .resumeThread({
          providerThread: {
            ...imported,
            nativeThreadRef: { ...imported.nativeThreadRef!, nativeId: "2026-01-01-deleted" },
          },
        })
        .pipe(Effect.flip);
      assert.equal(missing._tag, "ProviderAdapterResumeThreadError");
      assert.match(String(missing.cause), /wuwei chat 2026-01-01-deleted no longer exists/);
      assert.deepStrictEqual(fake.created, []);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("maps one wuwei turn onto turn items, context usage and a completed terminal", () =>
    Effect.gen(function* () {
      resetFake(
        scripted(
          [
            { type: "step_start", step: 0 },
            { type: "thinking_end", step: 0, index: 0, text: "Check the date first." },
            { type: "text_end", step: 0, index: 1, text: "Checking the date." },
            {
              type: "tool_start",
              callId: "call_bash",
              name: "bash",
              title: "Ran command",
              input: { command: "date" },
            },
            {
              type: "tool_end",
              callId: "call_bash",
              name: "bash",
              isError: false,
              output: "Tue Oct  6 12:00:00 PDT 2026",
            },
            {
              type: "tool_start",
              callId: "call_write",
              name: "write",
              title: "Wrote notes.md",
              input: { path: "/repo/notes.md", content: "# Notes\n" },
            },
            {
              type: "tool_end",
              callId: "call_write",
              name: "write",
              isError: false,
              output: "wrote /repo/notes.md",
            },
            {
              type: "tool_start",
              callId: "call_edit",
              name: "edit",
              title: "Edited memory.md",
              input: { path: "/repo/memory.md", oldText: "- old", newText: "- new" },
            },
            {
              type: "tool_end",
              callId: "call_edit",
              name: "edit",
              isError: false,
              output: "edited /repo/memory.md",
            },
            {
              type: "tool_start",
              callId: "call_exa",
              name: "exa",
              title: "Searched the web",
              input: { query: "wuwei agent harness", numResults: 8 },
            },
            {
              type: "tool_end",
              callId: "call_exa",
              name: "exa",
              isError: false,
              output: "8 results",
            },
            {
              type: "tool_start",
              callId: "call_look",
              name: "look",
              title: "Looked at screen.png",
              input: { path: "/repo/screen.png" },
            },
            {
              type: "tool_end",
              callId: "call_look",
              name: "look",
              isError: false,
              output: "[image]",
            },
            {
              type: "tool_start",
              callId: "call_fail",
              name: "bash",
              title: "Ran command",
              input: { command: "false" },
            },
            {
              type: "tool_end",
              callId: "call_fail",
              name: "bash",
              isError: true,
              output: "exit code 1",
            },
            {
              type: "usage",
              step: 0,
              model: MODEL,
              contextWindow: 372_000,
              input: 1_200,
              cacheRead: 30_000,
              cacheWrite: 500,
              output: 80,
              reasoning: 20,
              contextTokens: 31_780,
            },
            { type: "step_start", step: 1 },
            { type: "text_end", step: 1, index: 0, text: "It is Tuesday." },
          ],
          {
            stopReason: "end_turn",
            totals: { input: 1_200, cacheRead: 30_000, cacheWrite: 500, output: 80, reasoning: 20 },
            model: MODEL,
          },
        ),
      );
      const serverConfig = yield* ServerConfig.ServerConfig;
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* takeThrough((event) => event.type === "provider_thread.updated");

      const { runId, attemptId } = yield* startTurn(runtime, providerThread);
      const events = yield* takeThrough(isTerminal);
      for (const event of events) assert.isTrue(isAdapterEvent(event), event.type);

      assert.equal(fake.prompts.length, 1);
      const prompt = fake.prompts[0]!;
      assert.equal(prompt.threadId, providerThread.nativeThreadRef?.nativeId);
      assert.equal(prompt.cwd, serverConfig.cwd);
      assert.equal(prompt.content, "Hello wuwei");
      assert.equal(prompt.model, MODEL);
      assert.equal(prompt.effort, "high");

      const started = events[0];
      assert.isTrue(started?.type === "provider_turn.updated");
      if (started?.type !== "provider_turn.updated") return;
      const providerTurn = started.providerTurn;
      assert.equal(providerTurn.status, "running");
      assert.equal(providerTurn.runAttemptId, attemptId);
      assert.equal(providerTurn.providerThreadId, providerThread.id);
      assert.equal(started.threadId, THREAD_ID);

      const items = finalTurnItems(events);
      const blockId = (step: number, index: number) => `${providerTurn.id}:s${step}:c${index}`;
      // Every item belongs to this run and turn, in stream order from ordinal 101.
      for (const item of items.values()) {
        assert.equal(item.runId, runId);
        assert.equal(item.providerTurnId, providerTurn.id);
        assert.equal(item.providerThreadId, providerThread.id);
      }
      assert.deepStrictEqual(
        Array.from(items.entries()).map(([nativeId, item]) => [nativeId, item.ordinal]),
        [
          [blockId(0, 0), 101],
          [blockId(0, 1), 102],
          ["call_bash", 103],
          ["call_write", 104],
          ["call_edit", 105],
          ["call_exa", 106],
          ["call_look", 107],
          ["call_fail", 108],
          [blockId(1, 0), 109],
        ],
      );
      assert.deepInclude(items.get(blockId(0, 0)), {
        type: "reasoning",
        status: "completed",
        title: "Thinking",
        text: "Check the date first.",
        streaming: false,
      });
      assert.deepInclude(items.get(blockId(1, 0)), {
        type: "assistant_message",
        status: "completed",
        text: "It is Tuesday.",
        streaming: false,
      });
      assert.deepInclude(items.get("call_bash"), {
        type: "command_execution",
        status: "completed",
        title: "Ran command",
        input: "date",
        output: "Tue Oct  6 12:00:00 PDT 2026",
      });
      assert.deepInclude(items.get("call_write"), {
        type: "file_change",
        status: "completed",
        fileName: "/repo/notes.md",
        newStr: "# Notes\n",
      });
      assert.deepInclude(items.get("call_edit"), {
        type: "file_change",
        status: "completed",
        fileName: "/repo/memory.md",
        oldStr: "- old",
        newStr: "- new",
      });
      assert.deepInclude(items.get("call_exa"), {
        type: "web_search",
        status: "completed",
        patterns: ["wuwei agent harness"],
      });
      assert.deepInclude(items.get("call_look"), {
        type: "dynamic_tool",
        status: "completed",
        toolName: "look",
        input: { path: "/repo/screen.png" },
        output: "[image]",
      });
      assert.deepInclude(items.get("call_fail"), {
        type: "command_execution",
        status: "failed",
        input: "false",
        output: "exit code 1",
        outputIndicatesFailure: true,
      });

      // A tool shows as running before its result arrives.
      const bashUpdates = events.flatMap((event) =>
        event.type === "turn_item.updated" && event.turnItem.nativeItemRef?.nativeId === "call_bash"
          ? [event.turnItem.status]
          : [],
      );
      assert.deepStrictEqual(bashUpdates, ["running", "completed"]);

      // Each completed assistant block is also a conversation message.
      const messages = events.flatMap((event) =>
        event.type === "message.updated" ? [event.message] : [],
      );
      assert.deepStrictEqual(
        messages.map((message) => [message.role, message.text, message.streaming]),
        [
          ["assistant", "Checking the date.", false],
          ["assistant", "It is Tuesday.", false],
        ],
      );
      const finalText = items.get(blockId(1, 0));
      assert.equal(
        finalText?.type === "assistant_message" ? finalText.messageId : undefined,
        messages[1]?.id,
      );

      const usage = events.flatMap((event) =>
        event.type === "provider_thread.updated" && event.providerThread.contextUsage != null
          ? [event.providerThread.contextUsage]
          : [],
      );
      assert.deepStrictEqual(usage.at(-1), {
        usedTokens: 31_780,
        maxTokens: 372_000,
        inputTokens: 31_700,
        cachedInputTokens: 30_000,
        outputTokens: 80,
        reasoningOutputTokens: 20,
        lastUsedTokens: 31_780,
        lastInputTokens: 31_700,
        lastCachedInputTokens: 30_000,
        lastOutputTokens: 80,
        lastReasoningOutputTokens: 20,
      });

      const finished = events.findLast((event) => event.type === "provider_turn.updated");
      assert.isTrue(
        finished?.type === "provider_turn.updated" &&
          finished.providerTurn.id === providerTurn.id &&
          finished.providerTurn.status === "completed" &&
          finished.providerTurn.completedAt !== null,
      );
      const terminal = events.at(-1);
      assert.deepInclude(terminal, {
        type: "turn.terminal",
        driver: WUWEI_PROVIDER,
        providerThreadId: providerThread.id,
        providerTurnId: providerTurn.id,
        runOrdinal: 1,
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
      assert.equal(runtime.providerSession.status, "ready");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("fails the turn with wuwei's error and records it as an error item", () =>
    Effect.gen(function* () {
      resetFake(
        scripted(
          [
            { type: "step_start", step: 0 },
            { type: "error", message: "429 Too Many Requests: usage limit reached" },
          ],
          {
            stopReason: "error",
            errorMessage: "429 Too Many Requests: usage limit reached",
            totals: ZERO_TOTALS,
          },
        ),
      );
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const events = yield* takeThrough(isTerminal);

      const errorItem = events.findLast(
        (event) => event.type === "turn_item.updated" && event.turnItem.type === "error",
      );
      assert.isTrue(errorItem?.type === "turn_item.updated");
      if (errorItem?.type !== "turn_item.updated" || errorItem.turnItem.type !== "error") return;
      assert.equal(errorItem.turnItem.status, "failed");
      assert.equal(errorItem.turnItem.failure.class, "provider_error");
      assert.equal(
        errorItem.turnItem.failure.message,
        "429 Too Many Requests: usage limit reached",
      );

      const session = events.findLast((event) => event.type === "provider_session.updated");
      assert.isTrue(
        session?.type === "provider_session.updated" &&
          session.providerSession.status === "error" &&
          session.providerSession.lastError === "429 Too Many Requests: usage limit reached",
      );
      const terminal = events.at(-1);
      assert.isTrue(terminal?.type === "turn.terminal" && terminal.status === "failed");
      if (terminal?.type !== "turn.terminal" || terminal.status !== "failed") return;
      assert.equal(terminal.failure.message, "429 Too Many Requests: usage limit reached");
      assert.equal(terminal.failureItemOrdinal, errorItem.turnItem.ordinal);
      assert.equal(terminal.providerTurnId, errorItem.turnItem.providerTurnId);
      assert.equal(terminal.threadDisposition, "reusable");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // wuwei rejects a prompt only for setup reasons; the user must see the reason.
  it.effect("fails the turn with wuwei's reason when it rejects the prompt", () =>
    Effect.gen(function* () {
      resetFake(() =>
        Promise.reject(new Error("wuwei chat 2026-10-06-12-00-00-fake is open in the wuwei TUI")),
      );
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const terminal = (yield* takeThrough(isTerminal)).at(-1);
      assert.isTrue(terminal?.type === "turn.terminal" && terminal.status === "failed");
      if (terminal?.type !== "turn.terminal" || terminal.status !== "failed") return;
      assert.match(terminal.failure.message, /is open in the wuwei TUI/);
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  // The two tests below stream deltas, whose 50ms flush timers would never
  // fire under the TestClock; the turns themselves settle in microseconds.
  it.live("completes a block that streamed deltas but never got its closing event", () =>
    Effect.gen(function* () {
      resetFake(
        scripted(
          [
            { type: "step_start", step: 0 },
            { type: "thinking_delta", step: 0, index: 0, delta: "Plan: " },
            { type: "thinking_delta", step: 0, index: 0, delta: "answer briefly." },
            { type: "thinking_end", step: 0, index: 0, text: "Plan: answer briefly." },
            { type: "text_delta", step: 0, index: 1, delta: "Hel" },
            { type: "text_delta", step: 0, index: 1, delta: "lo" },
          ],
          { stopReason: "end_turn", totals: ZERO_TOTALS },
        ),
      );
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const events = yield* takeThrough(isTerminal);

      const items = Array.from(finalTurnItems(events).values());
      assert.deepStrictEqual(
        items.map((item) => [item.type, item.status]),
        [
          ["reasoning", "completed"],
          ["assistant_message", "completed"],
        ],
      );
      assert.deepInclude(items[0], { text: "Plan: answer briefly.", streaming: false });
      assert.deepInclude(items[1], { text: "Hello", streaming: false });
      // Streaming snapshots, if any were flushed, update the same message.
      const messageIds = new Set(
        events.flatMap((event) => (event.type === "message.updated" ? [event.message.id] : [])),
      );
      assert.equal(messageIds.size, 1);
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.live("shows a wuwei slash command's output as one assistant message", () =>
    Effect.gen(function* () {
      resetFake(
        scripted(
          [
            { type: "command_output", text: "Model: gpt-6-sol\n" },
            { type: "command_output", text: "Effort: high" },
          ],
          { stopReason: "command", totals: ZERO_TOTALS },
        ),
      );
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const events = yield* takeThrough(isTerminal);

      const items = Array.from(finalTurnItems(events).values());
      assert.lengthOf(items, 1);
      assert.deepInclude(items[0], {
        type: "assistant_message",
        status: "completed",
        text: "Model: gpt-6-sol\nEffort: high",
      });
      assert.deepInclude(events.at(-1), { type: "turn.terminal", status: "completed" });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("aborts wuwei on interrupt and settles the turn as interrupted", () =>
    Effect.gen(function* () {
      resetFake(
        untilAborted(
          [
            { type: "step_start", step: 0 },
            {
              type: "tool_start",
              callId: "call_sleep",
              name: "bash",
              title: "Ran command",
              input: { command: "sleep 600" },
            },
          ],
          [
            {
              type: "tool_end",
              callId: "call_sleep",
              name: "bash",
              isError: true,
              output: "aborted",
            },
          ],
        ),
      );
      const { runtime, takeThrough } = yield* openRuntime;
      const providerThread = yield* runtime.ensureThread({
        threadId: THREAD_ID,
        modelSelection,
        runtimePolicy,
      });
      yield* startTurn(runtime, providerThread);
      const running = yield* takeThrough(
        (event) =>
          event.type === "turn_item.updated" &&
          event.turnItem.nativeItemRef?.nativeId === "call_sleep",
      );
      const started = running.find((event) => event.type === "provider_turn.updated");
      assert.isTrue(started?.type === "provider_turn.updated");
      if (started?.type !== "provider_turn.updated") return;
      assert.isFalse(fake.prompts[0]!.signal.aborted);

      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: started.providerTurn.id,
      });
      assert.isTrue(fake.prompts[0]!.signal.aborted);

      const events = yield* takeThrough(isTerminal);
      assert.equal(finalTurnItems(events).get("call_sleep")?.status, "interrupted");
      const finished = events.findLast((event) => event.type === "provider_turn.updated");
      assert.isTrue(
        finished?.type === "provider_turn.updated" &&
          finished.providerTurn.status === "interrupted",
      );
      assert.deepInclude(events.at(-1), {
        type: "turn.terminal",
        providerTurnId: started.providerTurn.id,
        status: "interrupted",
        failure: null,
      });

      // Interrupting a turn that is no longer running is an error unless it is a Stop.
      const stale = yield* runtime
        .interruptTurn({ providerThread, providerTurnId: started.providerTurn.id })
        .pipe(Effect.flip);
      assert.equal(stale._tag, "ProviderAdapterInterruptError");
      yield* runtime.interruptTurn({
        providerThread,
        providerTurnId: started.providerTurn.id,
        requestRuntimeRestart: true,
      });
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );

  it.effect("closing the session stops a running turn, reports stopped and ends the stream", () =>
    Effect.gen(function* () {
      resetFake(
        untilAborted(
          [
            {
              type: "tool_start",
              callId: "call_long",
              name: "bash",
              title: "Ran command",
              input: { command: "sleep 600" },
            },
          ],
          [],
        ),
      );
      const adapter = yield* makeAdapter;
      const sessionScope = yield* Scope.make();
      const runtime = yield* adapter
        .openSession({
          threadId: THREAD_ID,
          providerSessionId: SESSION_ID,
          modelSelection,
          runtimePolicy,
        })
        .pipe(Effect.provideService(Scope.Scope, sessionScope));
      const collected = yield* runtime.events.pipe(Stream.runCollect, Effect.forkScoped);
      const providerThread = yield* runtime
        .ensureThread({ threadId: THREAD_ID, modelSelection, runtimePolicy })
        .pipe(Effect.provideService(Scope.Scope, sessionScope));
      yield* startTurn(runtime, providerThread).pipe(
        Effect.provideService(Scope.Scope, sessionScope),
      );
      // The prompt is running once wuwei has been asked for it.
      assert.equal(fake.prompts.length, 1);

      yield* Scope.close(sessionScope, Exit.void);
      const events = Array.from(yield* Fiber.join(collected));

      assert.isTrue(fake.prompts[0]!.signal.aborted);
      const terminal = events.find(isTerminal);
      assert.isTrue(terminal?.type === "turn.terminal" && terminal.status === "interrupted");
      const last = events.at(-1);
      assert.isTrue(
        last?.type === "provider_session.updated" && last.providerSession.status === "stopped",
      );
      assert.equal(runtime.providerSession.status, "stopped");
    }).pipe(Effect.scoped, Effect.provide(layerTest)),
  );
});
