import { assert, describe, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Queue from "effect/Queue";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import * as SessionImporter from "../import/Services/SessionImporter.ts";
import { CLAUDE_PROVIDER } from "../orchestration-v2/Adapters/ClaudeAdapterV2.ts";
import * as ProviderContinuationRequests from "../orchestration-v2/ProviderContinuationRequests.ts";
import * as ProviderRuntimeRecoveryService from "../orchestration-v2/ProviderRuntimeRecoveryService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import {
  CLAUDE_BRIDGE_REQUEST_RUN_OFFERS,
  CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS,
  claudeBridgeRequestToOffer,
  isHarnessPrompt,
  layer as attachLayer,
} from "./ClaudeBridgeAttachService.ts";
import { parseClaudeBridgeSessionFile } from "./ClaudeBridgeProtocol.ts";
import * as ClaudeBridgeRegistry from "./ClaudeBridgeRegistry.ts";

describe("isHarnessPrompt", () => {
  it("recognises prompts the harness injects into a terminal session", () => {
    assert.isTrue(isHarnessPrompt("<task-notification>\n<task-id>abc</task-id>"));
    assert.isTrue(isHarnessPrompt("  <system-reminder>\nGoal check-in"));
    assert.isTrue(isHarnessPrompt("Stop hook feedback:\n[please ship]: not yet"));
    assert.isTrue(isHarnessPrompt("[Request interrupted by user]"));
  });

  it("leaves what the person typed as theirs", () => {
    assert.isFalse(isHarnessPrompt("retry"));
    assert.isFalse(isHarnessPrompt("the <task-notification> tag is what I mean"));
  });
});

describe("a run for what a session nothing is attached to is waiting to ask", () => {
  const now = 1_750_000_000_000;
  const late = { toolUseId: "tool-late", kind: "approval", agentType: "general-purpose" } as const;
  const question = { toolUseId: "tool-question", kind: "question" } as const;
  const offer = (
    over: Partial<Parameters<typeof claudeBridgeRequestToOffer>[0]> = {},
  ): ReturnType<typeof claudeBridgeRequestToOffer> =>
    claudeBridgeRequestToOffer({
      hasActiveTurn: false,
      waiting: [late],
      nowMs: now,
      lastOfferedAtMs: undefined,
      offersFor: () => 0,
      ...over,
    });

  it("is offered for a request raised with no turn running (T3 restarted, or had let go)", () => {
    assert.deepStrictEqual(offer(), late);
    assert.deepStrictEqual(offer({ waiting: [question] }), question);
  });

  it("is not offered while the session runs a turn of its own: that turn's run carries it", () => {
    assert.isNull(offer({ hasActiveTurn: true }));
  });

  it("is not offered while a run offered a moment ago is still attaching", () => {
    assert.isNull(offer({ lastOfferedAtMs: now - 1_000 }));
    assert.isNull(offer({ lastOfferedAtMs: now - CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS + 1 }));
    // Still nothing attached after that: the run did not take, another is offered.
    assert.deepStrictEqual(
      offer({ lastOfferedAtMs: now - CLAUDE_BRIDGE_REQUEST_RUN_SETTLE_MS }),
      late,
    );
  });

  it("is offered once per request and pass, not once per look at the session", () => {
    // Offered just now: every later look within the span finds nothing to do.
    const offered = new Map<string, number>([[late.toolUseId, 1]]);
    assert.isNull(
      offer({ lastOfferedAtMs: now, offersFor: (id) => offered.get(id) ?? 0, nowMs: now + 400 }),
    );
  });

  it("stops after a few runs that did not attach, and still serves another request", () => {
    const offersFor = (id: string) =>
      id === late.toolUseId ? CLAUDE_BRIDGE_REQUEST_RUN_OFFERS : 0;
    assert.isNull(offer({ offersFor }));
    assert.deepStrictEqual(offer({ waiting: [late, question], offersFor }), question);
  });

  it("is never offered when the session lists nothing", () => {
    assert.isNull(offer({ waiting: [] }));
  });
});

describe("what a session lists as waiting", () => {
  const file = (waiting: unknown) =>
    parseClaudeBridgeSessionFile({
      sid: "session-1",
      instanceId: "instance-1",
      heartbeatAt: 1_750_000_000_000,
      waiting,
    });

  it("is read from its file: who asks, and whether it is a question", () => {
    assert.deepStrictEqual(
      file([
        { toolUseId: "tool-1", kind: "approval", agentType: "general-purpose" },
        { toolUseId: "tool-2", kind: "question" },
      ])?.waiting,
      [
        { toolUseId: "tool-1", kind: "approval", agentType: "general-purpose" },
        { toolUseId: "tool-2", kind: "question" },
      ],
    );
  });

  it("is absent for a session that waits for nothing, or whose mod does not say", () => {
    assert.isUndefined(file([])?.waiting);
    assert.isUndefined(file(undefined)?.waiting);
    assert.isUndefined(file([{ kind: "approval" }, "noise"])?.waiting);
  });
});

describe("the first turn of a session that carries a conversation on", () => {
  const at = "2026-10-13T13:39:05.000Z";
  // The session left behind has a thread; the one its conversation went on in has none of its own.
  const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
  const threadOfEarlierSession = Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const payload = yield* encodePayload({
      driver: CLAUDE_PROVIDER,
      nativeThreadRef: { nativeId: "old" },
    });
    yield* sql`
      INSERT INTO orchestration_v2_projection_threads (
        thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
        created_at, updated_at, payload_json
      ) VALUES (
        'thread-old', 'project-1', 'old', ${CLAUDE_PROVIDER}, 'full-access', 'default',
        ${at}, ${at}, '{}'
      )
    `;
    yield* sql`
      INSERT INTO orchestration_v2_projection_provider_threads (
        provider_thread_id, thread_id, provider, status, updated_at, payload_json
      ) VALUES (
        'provider-thread-old', 'thread-old', ${CLAUDE_PROVIDER}, 'idle', ${at}, ${payload}
      )
    `;
  });

  /** What became of a `turn.start` from session "new", whose own record leads back to nothing. */
  const firstTurn = (
    earlier: ReadonlyArray<ClaudeBridgeRegistry.ClaudeBridgeEarlierSession>,
    start: { readonly text?: string; readonly origin?: string; readonly sender?: string } = {},
  ) =>
    Effect.gen(function* () {
      yield* threadOfEarlierSession;
      const offered: Array<string> = [];
      const requests: Array<ProviderContinuationRequests.ProviderContinuationRequest> = [];
      const adopted: Array<string> = [];
      /** What the registry was told and offered, in order. */
      const steps: Array<string> = [];
      let onEvent: ClaudeBridgeRegistry.ClaudeBridgeEventHandler | null = null;
      const none = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry.pipe(
        Effect.provide(ClaudeBridgeRegistry.layerTest),
      );
      yield* Layer.build(attachLayer).pipe(
        Effect.provideService(
          ClaudeBridgeRegistry.ClaudeBridgeRegistry,
          ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
            ...none,
            onUnattachedEvent: (handler) =>
              Effect.sync(() => {
                onEvent = handler;
              }),
            liveSessionsContinuedIn: (sid) => Effect.succeed(sid === "new" ? earlier : []),
            noteTurnOffered: (sid, turnId) =>
              Effect.sync(() => {
                steps.push(`noted ${sid}:${turnId}`);
              }),
          }),
        ),
        Effect.provideService(
          SessionImporter.SessionImporter,
          SessionImporter.SessionImporter.of({
            start: () => Effect.void,
            sweepNow: () => Effect.void,
            adoptLiveClaudeSession: (input) =>
              Effect.sync(() => {
                adopted.push(input.sessionId);
              }),
            healLiveClaudeSession: () => Effect.void,
            // Claude Code makes the new record a moment after the session starts its turn.
            claudeConversationHolder: () => Effect.succeed(null),
            claudeSessionHeldElsewhere: () => Effect.succeed(false),
          }),
        ),
        Effect.provideService(
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService,
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService.of({
            reconcile: () => Effect.die("unused"),
            prepareForShutdown: Effect.void,
            recover: Effect.die("unused"),
            awaitStartupRecovery: Effect.void,
          }),
        ),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) =>
            Effect.sync(() => {
              offered.push(request.threadId);
              requests.push(request);
              steps.push(`offered ${request.threadId}`);
            }),
          take: Effect.never,
        }),
      );
      const session = parseClaudeBridgeSessionFile({
        sid: "new",
        instanceId: "instance-new",
        heartbeatAt: 1_750_000_000_000,
        isInteractive: true,
        transcriptPath: "/records/new.jsonl",
      });
      assert.isNotNull(session);
      assert.isNotNull(onEvent);
      if (session === null || onEvent === null) return { offered, adopted, steps, requests };
      yield* (onEvent as ClaudeBridgeRegistry.ClaudeBridgeEventHandler)(
        {
          seq: 1,
          at: 1_750_000_000_000,
          type: "turn.start",
          turnId: "turn-1",
          text: "carry on",
          transcriptOffset: 0,
          ...start,
        },
        { sid: "new", instanceId: "instance-new", dir: "/bridge/new/instance-new", session },
      );
      return { offered, adopted, steps, requests };
    }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory));

  it.effect(
    "goes to the conversation's thread although the session's record is not on disk yet",
    () =>
      Effect.gen(function* () {
        const { offered, adopted, steps } = yield* firstTurn([
          { sid: "old", transcriptPath: "/records/old.jsonl" },
        ]);
        assert.deepStrictEqual(offered, ["thread-old"]);
        assert.deepStrictEqual(adopted, []);
        // The run can tell the turn it was offered for from one begun since.
        assert.deepStrictEqual(steps, ["noted new:turn-1", "offered thread-old"]);
      }),
  );

  // Accepting a plan with "clear context" opens a new session whose first
  // turn is Claude Code's prompt to implement it (measured, 2.1.302); the
  // terminal draws the plan in a box, and a coordinating session's hand-off
  // as no line of the person's.
  it.effect("files a first turn the session says nobody typed under what the terminal draws", () =>
    Effect.gen(function* () {
      const earlier = [{ sid: "old", transcriptPath: "/records/old.jsonl" }];
      const plan = yield* firstTurn(earlier, {
        text: "Implement the following plan:\n\n1. Do it.\n\n\nIf you need specific details from before exiting plan mode (like exact code snippets), read the full transcript at: /x.jsonl",
        origin: "auto-continuation",
      });
      assert.equal(plan.requests[0]?.createdBy, "agent");
      assert.deepEqual(plan.requests[0]?.notification, {
        source: { kind: "background_task" },
        outcome: "updated",
        summary: "Plan to implement",
        detail: "1. Do it.",
      });
      const handOff = yield* firstTurn(earlier, { text: "Build it.", origin: "coordinator" });
      assert.equal(handOff.requests[0]?.createdBy, "agent");
      assert.equal(handOff.requests[0]?.notification?.summary, "coordinator");
      const typed = yield* firstTurn(earlier, { text: "Build it.", origin: "composer" });
      assert.equal(typed.requests[0]?.createdBy, "user");
      assert.isUndefined(typed.requests[0]?.notification);
    }),
  );

  it.effect("makes a session no earlier session leads to a thread of its own", () =>
    Effect.gen(function* () {
      const { offered, adopted } = yield* firstTurn([]);
      assert.deepStrictEqual(adopted, ["new"]);
      assert.deepStrictEqual(offered, []);
    }),
  );
});

describe("a turn the session was running when the server came up", () => {
  const at = "2026-10-14T10:00:00.000Z";
  const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

  /** Thread "thread-<sid>", which holds Claude session `sid`. */
  const threadHolding = (sid: string) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const payload = yield* encodePayload({
        driver: CLAUDE_PROVIDER,
        nativeThreadRef: { nativeId: sid },
      });
      yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, payload_json
        ) VALUES (
          ${`thread-${sid}`}, 'project-1', ${sid}, ${CLAUDE_PROVIDER}, 'full-access', 'default',
          ${at}, ${at}, '{}'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_v2_projection_provider_threads (
          provider_thread_id, thread_id, provider, status, updated_at, payload_json
        ) VALUES (
          ${`provider-thread-${sid}`}, ${`thread-${sid}`}, ${CLAUDE_PROVIDER}, 'idle', ${at}, ${payload}
        )
      `;
    });

  /**
   * The first `count` offers the startup scan makes for these live sessions;
   * `held` sessions have a thread of their own, and `continuedIn` names the
   * live sessions each one carried on from.
   */
  const startupOffers = (input: {
    readonly held: ReadonlyArray<string>;
    readonly live: ReadonlyArray<{ readonly sid: string; readonly running: boolean }>;
    readonly continuedIn?: Readonly<Record<string, ReadonlyArray<string>>>;
    readonly count: number;
  }) =>
    Effect.gen(function* () {
      for (const sid of input.held) yield* threadHolding(sid);
      const live = input.live.map(({ sid, running }) => {
        const session = parseClaudeBridgeSessionFile({
          sid,
          instanceId: `instance-${sid}`,
          heartbeatAt: 1_750_000_000_000,
          isInteractive: true,
          transcriptPath: `/records/${sid}.jsonl`,
          ...(running
            ? {
                activeTurn: { turnId: "turn-1", startedAt: 1_749_999_990_000, transcriptOffset: 0 },
              }
            : {}),
        });
        assert.isNotNull(session);
        return { sid, instanceId: `instance-${sid}`, dir: `/bridge/${sid}`, session: session! };
      });
      const offered =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const none = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry.pipe(
        Effect.provide(ClaudeBridgeRegistry.layerTest),
      );
      yield* Layer.build(attachLayer).pipe(
        Effect.provideService(
          ClaudeBridgeRegistry.ClaudeBridgeRegistry,
          ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
            ...none,
            listLiveProcesses: Effect.succeed(live),
            liveSessionsContinuedIn: (sid) =>
              Effect.succeed(
                (input.continuedIn?.[sid] ?? []).map((earlier) => ({
                  sid: earlier,
                  transcriptPath: `/records/${earlier}.jsonl`,
                })),
              ),
          }),
        ),
        Effect.provideService(
          SessionImporter.SessionImporter,
          SessionImporter.SessionImporter.of({
            start: () => Effect.void,
            sweepNow: () => Effect.void,
            adoptLiveClaudeSession: () => Effect.void,
            healLiveClaudeSession: () => Effect.void,
            claudeConversationHolder: () => Effect.succeed(null),
            claudeSessionHeldElsewhere: () => Effect.succeed(false),
          }),
        ),
        Effect.provideService(
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService,
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService.of({
            reconcile: () => Effect.die("unused"),
            prepareForShutdown: Effect.void,
            recover: Effect.die("unused"),
            awaitStartupRecovery: Effect.void,
          }),
        ),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
      const offers: Array<ProviderContinuationRequests.ProviderContinuationRequest> = [];
      for (let i = 0; i < input.count; i += 1) {
        const offer = yield* Queue.take(offered).pipe(Effect.timeoutOption("2 seconds"));
        if (Option.isNone(offer)) break;
        offers.push(offer.value);
      }
      return offers;
    }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory));

  /** What is offered for session "live", which T3 holds and which runs a turn at startup (or not). */
  const startupOffer = (running = true) =>
    startupOffers({ held: ["live"], live: [{ sid: "live", running }], count: 1 }).pipe(
      Effect.map((offers) => Option.fromNullishOr(offers[0])),
    );

  it.effect("is offered for the run recovery left open to pick back up", () =>
    Effect.gen(function* () {
      const offer = Option.getOrUndefined(yield* startupOffer());
      assert.deepStrictEqual(
        [offer?.threadId, offer?.providerThreadId, offer?.reattachRun, offer?.reattachOnly],
        ["thread-live", "provider-thread-live", true, undefined],
      );
    }),
  );

  // bgafter 52b3ad3a: the turn ended 6 s before the server's startup scan,
  // and the run recovery kept for it was never picked up.
  it.live("and for a turn that ended as the server came up: that run only, if one was kept", () =>
    Effect.gen(function* () {
      const offer = Option.getOrUndefined(yield* startupOffer(false));
      assert.deepStrictEqual(
        [offer?.threadId, offer?.providerThreadId, offer?.reattachRun, offer?.reattachOnly],
        ["thread-live", "provider-thread-live", true, true],
      );
    }),
  );

  // bgafter (Oct 22): the session was sent to the background and went on
  // under a new id while the process it left was still live; the scan picked
  // the thread's run up for both, and the second attempt could not start over
  // the first.
  it.effect("is offered once for a thread two live sessions lead to, by the one running", () =>
    Effect.gen(function* () {
      const offers = yield* startupOffers({
        held: ["live", "other"],
        live: [
          { sid: "live", running: false },
          { sid: "moved", running: true },
          { sid: "other", running: false },
        ],
        continuedIn: { moved: ["live"] },
        count: 2,
      });
      assert.deepStrictEqual(
        offers.map((offer) => [offer.threadId, offer.reattachRun, offer.reattachOnly]),
        [
          ["thread-live", true, undefined],
          ["thread-other", true, true],
        ],
      );
    }),
  );
});

describe("a turn T3 let go of when it lost contact with the session", () => {
  const at = "2026-10-22T20:11:21.000Z";
  const encodePayload = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

  // ddba110e (Oct 22): T3 let go of a turn its own prompt had started, and of
  // another nothing was attached for, while the turns ran on.
  it.effect("is offered to a run again once the session reports, also one offered before", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      const payload = yield* encodePayload({
        driver: CLAUDE_PROVIDER,
        nativeThreadRef: { nativeId: "live" },
      });
      yield* sql`
        INSERT INTO orchestration_v2_projection_threads (
          thread_id, project_id, title, default_provider, runtime_mode, interaction_mode,
          created_at, updated_at, payload_json
        ) VALUES (
          'thread-live', 'project-1', 'live', ${CLAUDE_PROVIDER}, 'full-access', 'default',
          ${at}, ${at}, '{}'
        )
      `;
      yield* sql`
        INSERT INTO orchestration_v2_projection_provider_threads (
          provider_thread_id, thread_id, provider, status, updated_at, payload_json
        ) VALUES (
          'provider-thread-live', 'thread-live', ${CLAUDE_PROVIDER}, 'idle', ${at}, ${payload}
        )
      `;
      const session = parseClaudeBridgeSessionFile({
        sid: "live",
        instanceId: "instance-live",
        heartbeatAt: 1_750_000_000_000,
        isInteractive: true,
        transcriptPath: "/records/live.jsonl",
        activeTurn: { turnId: "turn-1", startedAt: 1_749_999_990_000, transcriptOffset: 0 },
      });
      assert.isNotNull(session);
      const entry = {
        sid: "live",
        instanceId: "instance-live",
        dir: "/bridge/live",
        session: session!,
      };
      let onEvent: ClaudeBridgeRegistry.ClaudeBridgeEventHandler | null = null;
      let onTurnBack: ClaudeBridgeRegistry.ClaudeBridgeTurnBackHandler | null = null;
      const offered =
        yield* Queue.unbounded<ProviderContinuationRequests.ProviderContinuationRequest>();
      const none = yield* ClaudeBridgeRegistry.ClaudeBridgeRegistry.pipe(
        Effect.provide(ClaudeBridgeRegistry.layerTest),
      );
      yield* Layer.build(attachLayer).pipe(
        Effect.provideService(
          ClaudeBridgeRegistry.ClaudeBridgeRegistry,
          ClaudeBridgeRegistry.ClaudeBridgeRegistry.of({
            ...none,
            onUnattachedEvent: (handler) =>
              Effect.sync(() => {
                onEvent = handler;
              }),
            onTurnBack: (handler) =>
              Effect.sync(() => {
                onTurnBack = handler;
              }),
          }),
        ),
        Effect.provideService(
          SessionImporter.SessionImporter,
          SessionImporter.SessionImporter.of({
            start: () => Effect.void,
            sweepNow: () => Effect.void,
            adoptLiveClaudeSession: () => Effect.void,
            healLiveClaudeSession: () => Effect.void,
            claudeConversationHolder: () => Effect.succeed(null),
            claudeSessionHeldElsewhere: () => Effect.succeed(false),
          }),
        ),
        Effect.provideService(
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService,
          ProviderRuntimeRecoveryService.ProviderRuntimeRecoveryService.of({
            reconcile: () => Effect.die("unused"),
            prepareForShutdown: Effect.void,
            recover: Effect.die("unused"),
            awaitStartupRecovery: Effect.void,
          }),
        ),
        Effect.provideService(ProviderContinuationRequests.ProviderContinuationRequests, {
          offer: (request) => Queue.offer(offered, request).pipe(Effect.asVoid),
          take: Effect.never,
        }),
      );
      assert.isNotNull(onEvent);
      assert.isNotNull(onTurnBack, "the service takes up turns T3 let go of");
      if (onEvent === null || onTurnBack === null) return;
      // Typed in the terminal with nothing attached: a run is offered for it.
      yield* (onEvent as ClaudeBridgeRegistry.ClaudeBridgeEventHandler)(
        {
          seq: 1,
          at: 1_749_999_990_000,
          type: "turn.start",
          turnId: "turn-1",
          text: "go on",
          transcriptOffset: 0,
        },
        entry,
      );
      const first = yield* Queue.take(offered);
      assert.strictEqual(first.threadId, "thread-live");
      // That run let it go; the session reports again with it still running.
      yield* (onTurnBack as ClaudeBridgeRegistry.ClaudeBridgeTurnBackHandler)(
        entry,
        session!.activeTurn!,
      );
      const again = Option.getOrUndefined(
        yield* Queue.take(offered).pipe(Effect.timeoutOption("2 seconds")),
      );
      assert.deepStrictEqual(
        [again?.threadId, again?.providerThreadId, again?.createdBy, again?.reattachRun],
        ["thread-live", "provider-thread-live", "agent", undefined],
      );
    }).pipe(Effect.scoped, Effect.provide(SqlitePersistence.layerMemory)),
  );
});
