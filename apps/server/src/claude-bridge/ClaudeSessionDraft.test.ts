import { assert, it } from "@effect/vitest";
import { ThreadId } from "@t3tools/contracts";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";

import { parseClaudeBridgeSessionFile, type ClaudeBridgeCommandBody } from "./ClaudeBridgeProtocol.ts";
import type * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";
import * as ClaudeSessionDraft from "./ClaudeSessionDraft.ts";

const THREAD = ThreadId.make("thread-1");

const processWith = (draft: unknown): ClaudeBridgeRegistry.ClaudeBridgeProcess => {
  const session = parseClaudeBridgeSessionFile({
    sid: "session-1",
    instanceId: "instance-1",
    heartbeatAt: 1_750_000_000_000,
    isInteractive: true,
    ...(draft === undefined ? {} : { draft }),
  });
  assert.isNotNull(session);
  return { sid: "session-1", instanceId: "instance-1", dir: "/bridge/session-1", session: session! };
};

/** The service over a bridge whose session reads, in turn, as `readings` (the last one stays). */
const serviceOver = (input: {
  readonly sid: string | null;
  readonly readings: ReadonlyArray<ClaudeBridgeRegistry.ClaudeBridgeProcess | null>;
}) => {
  const sent: Array<ClaudeBridgeCommandBody> = [];
  let reads = 0;
  const service = ClaudeSessionDraft.make({
    sessionIdOf: () => Effect.succeed(input.sid),
    registry: {
      findLiveProcess: () =>
        Effect.sync(() => input.readings[Math.min(reads++, input.readings.length - 1)] ?? null),
      send: (_process, body) =>
        Effect.sync(() => {
          sent.push(body);
          return "command-1";
        }),
    },
  });
  return { service, sent };
};

it.effect("streams the terminal's draft once per change, and null while no box is shared", () =>
  Effect.gen(function* () {
    const { service } = serviceOver({
      sid: "session-1",
      readings: [
        processWith({ text: "fix", rev: 1 }),
        processWith({ text: "fix", rev: 1 }),
        processWith({ text: "fix it", rev: 2 }),
        // The terminal closed: the session shares no box.
        processWith(null),
        null,
      ],
    });
    const collecting = yield* service
      .stream(THREAD)
      .pipe(Stream.take(3), Stream.runCollect, Effect.forkChild({ startImmediately: true }));
    // Five looks at the session, one per poll.
    yield* TestClock.adjust(Duration.millis(ClaudeSessionDraft.SESSION_DRAFT_POLL_MS * 5));
    const seen = yield* Fiber.join(collecting);
    assert.deepEqual(Array.from(seen), [{ text: "fix", rev: 1 }, { text: "fix it", rev: 2 }, null]);
  }),
);

it.effect("is null for a thread with no Claude Code session, and for a mod that shares no draft", () =>
  Effect.gen(function* () {
    const none = serviceOver({ sid: null, readings: [processWith({ text: "x", rev: 1 })] });
    assert.deepEqual(Array.from(yield* none.service.stream(THREAD).pipe(Stream.take(1), Stream.runCollect)), [null]);
    const older = serviceOver({ sid: "session-1", readings: [processWith(undefined)] });
    assert.deepEqual(Array.from(yield* older.service.stream(THREAD).pipe(Stream.take(1), Stream.runCollect)), [null]);
    assert.deepEqual(yield* older.service.offer({ threadId: THREAD, text: "a", base: "" }), { sent: false });
    assert.deepEqual(older.sent, []);
  }),
);

it.effect("hands T3's draft to the session with the draft both boxes last held", () =>
  Effect.gen(function* () {
    const { service, sent } = serviceOver({
      sid: "session-1",
      readings: [processWith({ text: "fix", rev: 1 })],
    });
    assert.deepEqual(yield* service.offer({ threadId: THREAD, text: "fix it", base: "fix" }), {
      sent: true,
    });
    assert.deepEqual(sent, [{ type: "draft", text: "fix it", base: "fix" }]);
  }),
);
