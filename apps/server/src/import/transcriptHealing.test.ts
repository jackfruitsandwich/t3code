import { assert, describe, it } from "@effect/vitest";

import type { ParsedSessionMessage, ParsedSessionSystemNotice } from "./sessionImportTypes.ts";
import {
  claudeRecordLines,
  type HealingKnownMessage,
  planImportedRowOrdering,
  planImportedRowReseating,
  planNoticeHealing,
  planRecordSeating,
  planTaskNoticeHealing,
  planTranscriptHealing,
  planWithdrawnRuns,
  recordKeyOfTurnItem,
  type SeatingRow,
  sentPromptLines,
} from "./transcriptHealing.ts";

const at = (minute: number, second = 0) =>
  `2026-07-01T10:${String(minute).padStart(2, "0")}:${String(second).padStart(2, "0")}.000Z`;
const ms = (minute: number, second = 0) => Date.parse(at(minute, second));

const row = (
  role: "user" | "assistant",
  sourceId: string,
  text: string,
  minute: number,
  command = false,
): ParsedSessionMessage => ({
  role,
  sourceId,
  text,
  timestamp: at(minute),
  lineIndex: minute,
  ...(command ? { command: true as const } : {}),
});

const shown = (
  messageId: string,
  role: "user" | "assistant",
  text: string,
  minute: number,
  runId: string | null = null,
): HealingKnownMessage => ({ messageId, role, text, atMs: ms(minute), runId });

const plan = (
  rows: ReadonlyArray<ParsedSessionMessage>,
  known: ReadonlyArray<HealingKnownMessage>,
  runs: ReadonlyArray<{ id: string; ordinal: number; minute: number }> = [],
) =>
  planTranscriptHealing({
    rows,
    known,
    runs: runs.map((run) => ({ id: run.id, ordinal: run.ordinal, startedAtMs: ms(run.minute) })),
    importedMessageId: (sourceId) => `cli-import-s-${sourceId}`,
  }).map((placement) => `${placement.band}:${placement.message.sourceId}`);

describe("planTranscriptHealing", () => {
  it("adds only the rows the thread does not show, after the run that preceded each", () => {
    const rows = [
      row("user", "u1", "first prompt", 0),
      row("assistant", "a1", "first reply", 1),
      row("user", "u2", "typed while T3 was down", 10),
      row("assistant", "a2", "reply T3 never saw", 11),
      row("user", "u3", "sent from T3", 20),
      row("assistant", "a3", "reply T3 streamed", 21),
    ];
    const known = [
      shown("cli-import-s-u1", "user", "first prompt", 0),
      shown("cli-import-s-a1", "assistant", "first reply", 1),
      shown("t3-message-9", "user", "sent from T3", 20, "run-2"),
      shown(
        "message:provider:claudeAgent:native-item:a3",
        "assistant",
        "reply T3 streamed",
        21,
        "run-2",
      ),
    ];
    const runs = [
      { id: "run-1", ordinal: 1, minute: 5 },
      { id: "run-2", ordinal: 2, minute: 20 },
    ];
    assert.deepEqual(plan(rows, known, runs), ["1:u2", "1:a2"]);
  });

  it("matches repeated prompts one to one, nearest in time", () => {
    const rows = [
      row("user", "u1", "continue", 0),
      row("user", "u2", "continue", 10),
      row("user", "u3", "continue", 20),
    ];
    const known = [shown("t3-a", "user", "continue", 0), shown("t3-b", "user", "continue", 20)];
    assert.deepEqual(plan(rows, known), ["0:u2"]);
  });

  it("counts a prompt T3 reworded as shown when T3 ran the turn it started", () => {
    const rows = [
      row("user", "u1", "look at this [Image #1]", 0),
      row("assistant", "a1", "I see a cat.", 1),
    ];
    const known = [
      shown("t3-a", "user", "look at this", 0, "run-1"),
      shown("assistant:a1", "assistant", "I see a cat.", 1, "run-1"),
    ];
    assert.deepEqual(plan(rows, known, [{ id: "run-1", ordinal: 1, minute: 0 }]), []);
    // The same turn picked up mid-flight has no prompt in T3: the row is added.
    const midFlight = [shown("assistant:a1", "assistant", "I see a cat.", 1, "run-1")];
    assert.deepEqual(plan(rows, midFlight, [{ id: "run-1", ordinal: 1, minute: 1 }]), ["0:u1"]);
  });

  it("counts it as shown too when the turn's reply kept the id the terminal drew it with", () => {
    const rows = [
      row("user", "u1", "look at this [Image #1]", 0),
      row("assistant", "a1", "I see a cat.", 1),
    ];
    // The terminal drew the reply before the record held it: T3's row for it
    // carries the terminal's id, and the record's row is that row.
    const known = [
      shown("t3-a", "user", "look at this", 0, "run-1"),
      shown(
        "message:provider:claudeAgent:native-item:drawn-1",
        "assistant",
        "I see a cat.",
        1,
        "run-1",
      ),
    ];
    assert.deepEqual(plan(rows, known, [{ id: "run-1", ordinal: 1, minute: 0 }]), []);
  });

  it("puts a reply T3 missed below its prompt when a later run shows that prompt", () => {
    // The turn was typed and answered before T3 took the session in; the run
    // that picked it up started after both rows were written, and shows the prompt.
    const rows = [row("user", "u1", "first prompt", 0), row("assistant", "a1", "first reply", 1)];
    const known = [shown("t3-adopted", "user", "first prompt", 5, "run-1")];
    assert.deepEqual(plan(rows, known, [{ id: "run-1", ordinal: 1, minute: 5 }]), ["1:a1"]);
  });

  it("shows what a command printed with the command, and never adds the harness's own rows", () => {
    const rows = [
      row("user", "c1", "/rename zed", 0, true),
      row("assistant", "o1", "Session renamed to: zed", 0, true),
      row("user", "c2", "/effort low", 5, true),
      row("assistant", "o2", "Set effort level to low", 5, true),
      row("user", "h1", "<task-notification>done</task-notification>", 6),
      row("user", "h2", "[Request interrupted by user]", 7),
      row("user", "h3", "Stop hook feedback:\nkeep going", 8),
    ];
    const known = [
      shown("t3-a", "user", "/rename zed", 0, "run-1"),
      shown("t3-b", "assistant", "Session renamed to: zed (shown differently)", 0, "run-1"),
    ];
    assert.deepEqual(plan(rows, known, [{ id: "run-1", ordinal: 1, minute: 0 }]), ["1:c2", "1:o2"]);
  });
});

describe("planTranscriptHealing: a prompt followed by a turn the harness started", () => {
  it("is added: the later run and its prompt are not this prompt's", () => {
    const rows = [
      row("user", "u1", "/compact", 10),
      // The harness started the next turn (a task notice): it leaves no row here.
      row("assistant", "a1", "picking up after the notice", 16),
    ];
    const known = [
      shown("message:run-2:prompt", "user", "an unrelated prompt", 15, "run-2"),
      shown(
        "message:provider:claudeAgent:native-item:a1",
        "assistant",
        "picking up after the notice",
        16,
        "run-2",
      ),
    ];
    assert.deepEqual(plan(rows, known, [{ id: "run-2", ordinal: 2, minute: 15 }]), ["0:u1"]);
  });
});

describe("planTranscriptHealing: a message typed into a running turn", () => {
  it("is added although the turn it was typed into ran in T3 with its own prompt", () => {
    const typed: ParsedSessionMessage = {
      ...row("user", "q1", "also check the logs", 11),
      midTurn: true,
    };
    const rows = [
      row("user", "u1", "fix the bug", 10),
      typed,
      row("assistant", "a1", "fixed, and the logs are clean", 12),
    ];
    const known = [
      shown("message:run-1:prompt", "user", "fix the bug", 10, "run-1"),
      shown(
        "message:provider:claudeAgent:native-item:a1",
        "assistant",
        "fixed, and the logs are clean",
        12,
        "run-1",
      ),
    ];
    assert.deepEqual(plan(rows, known, [{ id: "run-1", ordinal: 1, minute: 10 }]), ["1:q1"]);
  });

  it("is not added twice", () => {
    const typed: ParsedSessionMessage = {
      ...row("user", "q1", "also check the logs", 11),
      midTurn: true,
    };
    const known = [shown("cli-import-s-q1", "user", "also check the logs", 11)];
    assert.deepEqual(plan([typed], known, [{ id: "run-1", ordinal: 1, minute: 10 }]), []);
  });
});

describe("planNoticeHealing", () => {
  const notice = (sourceId: string, minute: number): ParsedSessionSystemNotice => ({
    sourceId,
    subtype: "away_summary",
    text: `recap: ${sourceId}`,
    timestamp: at(minute),
    lineIndex: minute,
  });
  const notices = [
    notice("before-t3", 2),
    notice("between-runs", 15),
    notice("a-run-showed", 25),
    notice("in-the-running-turn", 41),
  ];
  const runs = [
    { id: "run-1", ordinal: 1, startedAtMs: ms(10) },
    { id: "run-2", ordinal: 2, startedAtMs: ms(20) },
    { id: "run-3", ordinal: 3, startedAtMs: ms(40) },
  ];
  const planned = (input: { readonly fromLine: number; readonly settledBeforeMs?: number }) =>
    planNoticeHealing({ notices, shownIds: new Set(["a-run-showed"]), runs, ...input }).map(
      (placement) => `${placement.band}:${placement.notice.sourceId}`,
    );

  it("adds the notices no run showed after the run that preceded each, none from the history", () => {
    assert.deepEqual(planned({ fromLine: 5 }), ["1:between-runs", "3:in-the-running-turn"]);
    // A thread first compared after every notice was written adds none.
    assert.deepEqual(planned({ fromLine: 50 }), []);
  });

  it("leaves a running turn's notices to its run", () => {
    assert.deepEqual(planned({ fromLine: 5, settledBeforeMs: ms(40) }), ["1:between-runs"]);
  });
});

describe("planTaskNoticeHealing", () => {
  const task = (sourceId: string, minute: number, summary = `Task ${sourceId} finished`) => ({
    ...row("user", sourceId, `<task-notification>\n<summary>${summary}</summary>\n`, minute),
    notice: { summary, outcome: "completed" as const },
  });
  const runs = [
    { id: "run-1", ordinal: 1, startedAtMs: ms(10) },
    { id: "run-2", ordinal: 2, startedAtMs: ms(31) },
  ];
  const planned = (input: {
    readonly rows: ReadonlyArray<ParsedSessionMessage>;
    readonly known?: ReadonlyArray<HealingKnownMessage>;
    readonly shown?: ReadonlyArray<{ sourceId: string | null; summary: string; minute: number }>;
    readonly fromLine?: number;
  }) =>
    planTaskNoticeHealing({
      rows: input.rows,
      known: input.known ?? [],
      shown: (input.shown ?? []).map((notice) => ({
        sourceId: notice.sourceId,
        summary: notice.summary,
        atMs: ms(notice.minute),
      })),
      runs,
      fromLine: input.fromLine ?? 0,
    }).map((placement) => `${placement.band}:${placement.message.sourceId}`);

  it("adds a notice the session delivered with the next prompt, after the run before it", () => {
    // After a restart: the notice is written, then the person's prompt opens the turn T3 follows.
    const rows = [task("stopped", 30), row("user", "u2", "mac died, how long ago?", 31)];
    assert.deepEqual(planned({ rows }), ["1:stopped"]);
    // Shown already: added before, or shown by a live run in the same words.
    assert.deepEqual(
      planned({ rows, shown: [{ sourceId: "stopped", summary: "x", minute: 30 }] }),
      [],
    );
    assert.deepEqual(
      planned({ rows, shown: [{ sourceId: null, summary: "Task stopped finished", minute: 30 }] }),
      [],
    );
  });

  it("leaves a notice that opened a turn T3 ran to that run, in words or not", () => {
    const rows = [
      task("woke", 30),
      row("assistant", "a-woke", "on it", 30),
      row("user", "u2", "next", 35),
    ];
    // The run's reply carries the row's id: T3 ran the turn the notice opened.
    const known = [shown("message:thread:t:a-woke", "assistant", "on it", 30, "run-2")];
    assert.deepEqual(planned({ rows, known }), []);
    // A turn T3 never saw (it was down): its notice is added with its replies.
    assert.deepEqual(planned({ rows }), ["1:woke"]);
    // A live run showed it without the task's words, at the same moment.
    assert.deepEqual(
      planned({
        rows,
        shown: [{ sourceId: null, summary: "Background activity updated", minute: 30 }],
      }),
      [],
    );
    // One shown long before stands for some other notice.
    assert.deepEqual(
      planned({
        rows,
        shown: [{ sourceId: null, summary: "Background activity updated", minute: 5 }],
      }),
      ["1:woke"],
    );
  });

  it("pairs a notice shown in its words with its own row, not the nearest one", () => {
    const rows = [
      task("first", 30),
      task("second", 31, "Task second finished"),
      row("user", "u2", "next", 32),
    ];
    // Shown live at the second's moment, in the first's words: the second is the one missing.
    assert.deepEqual(
      planned({ rows, shown: [{ sourceId: null, summary: "Task first finished", minute: 31 }] }),
      ["2:second"],
    );
  });

  it("waits on a notice still last in the record, and adds none from the history", () => {
    assert.deepEqual(planned({ rows: [row("user", "u1", "hi", 20), task("just-now", 30)] }), []);
    const rows = [
      task("old", 5),
      row("user", "u1", "hi", 6),
      task("new", 30),
      row("user", "u2", "x", 31),
    ];
    assert.deepEqual(planned({ rows, fromLine: 20 }), ["1:new"]);
  });
});

describe("planImportedRowReseating", () => {
  it("moves imported rows that sit above the run they followed", () => {
    const rows = [
      row("user", "u1", "before any run", 0),
      row("user", "u2", "after run 1", 10),
      row("user", "u3", "after run 2", 30),
      row("user", "u4", "never imported", 40),
    ];
    const moved = planImportedRowReseating({
      rows,
      imported: [
        { sourceId: "u1", ordinal: 1 },
        { sourceId: "u2", ordinal: 2 },
        { sourceId: "u3", ordinal: 2_000_004 },
      ],
      runs: [
        { id: "run-1", ordinal: 1, startedAtMs: ms(5) },
        { id: "run-2", ordinal: 2, startedAtMs: ms(20) },
      ],
    });
    assert.deepEqual(
      moved.map((placement) => `${placement.band}:${placement.message.sourceId}`),
      ["1:u2"],
    );
  });

  it("leaves a reply below its prompt's run where healing put it", () => {
    // The turn was typed and answered before T3 took the session in; the run
    // that picked it up shows the prompt, and its reply went in below it.
    const rows = [row("user", "u1", "first prompt", 0), row("assistant", "a1", "first reply", 1)];
    assert.deepEqual(
      planImportedRowReseating({
        rows,
        imported: [{ sourceId: "a1", ordinal: 1_000_003 }],
        runs: [{ id: "run-1", ordinal: 1, startedAtMs: ms(5) }],
      }),
      [],
    );
  });
});

describe("planImportedRowOrdering", () => {
  it("puts a band's imported rows back in the order they were written", () => {
    const rows = [
      row("user", "u1", "first", 10),
      row("assistant", "a1", "reply to first", 11),
      row("user", "u2", "second, added on a later pass", 12),
      row("assistant", "a2", "reply to second, added earlier", 13),
      row("user", "u3", "another band", 30),
    ];
    const moves = planImportedRowOrdering({
      rows,
      imported: [
        { sourceId: "u1", ordinal: 1_000_010 },
        { sourceId: "a1", ordinal: 1_000_011 },
        { sourceId: "a2", ordinal: 1_000_012 },
        { sourceId: "u2", ordinal: 1_000_013 },
        { sourceId: "u3", ordinal: 2_000_004 },
      ],
    });
    assert.deepEqual(
      moves.map((placement) => `${placement.band}:${placement.message.sourceId}`),
      ["1:u2", "1:a2"],
    );
  });

  it("leaves a band alone when its imported rows already read in order", () => {
    const rows = [row("user", "u1", "first", 10), row("assistant", "a1", "reply", 11)];
    assert.deepEqual(
      planImportedRowOrdering({
        rows,
        imported: [
          { sourceId: "u1", ordinal: 1_000_010 },
          { sourceId: "a1", ordinal: 1_000_020 },
        ],
      }),
      [],
    );
  });
});

describe("planRecordSeating", () => {
  const RUNS = [
    { id: "run-1", ordinal: 1, startedAtMs: ms(30) },
    { id: "run-2", ordinal: 2, startedAtMs: ms(40) },
  ];
  /** `id@place` rows: the id's letter says the kind, its number the record line. */
  const thread = (...placed: ReadonlyArray<string>): ReadonlyArray<SeatingRow> =>
    placed.map((entry) => {
      const [id, place] = entry.split("@") as [string, string];
      const line = id.slice(1) === "" ? null : Number(id.slice(1));
      const kinds = { i: "imported", m: "message", s: "sent", t: "other", x: "other" } as const;
      return {
        turnItemId: id,
        ordinal: Number(place),
        line,
        kind: kinds[id[0] as keyof typeof kinds],
        // Written a minute a line, so before the first run started.
        atMs: line === null ? 0 : ms(line),
      };
    });
  /** The thread as it reads once the plan is carried out, with each row's band. */
  const after = (
    rows: ReadonlyArray<SeatingRow>,
    options: { only?: "sent"; frozenBand?: number },
  ) => {
    const moves = planRecordSeating({ rows, runs: RUNS, ...options });
    const placeOf = new Map(moves.map((move) => [move.turnItemId, move.ordinal]));
    const seatedRows = rows.map((row) => ({
      ...row,
      ordinal: placeOf.get(row.turnItemId) ?? row.ordinal,
    }));
    const places = seatedRows.map((row) => row.ordinal);
    assert.strictEqual(new Set(places).size, places.length, "one row to a place");
    // Carried out, the plan asks for nothing more.
    assert.deepEqual(planRecordSeating({ rows: seatedRows, runs: RUNS, ...options }), []);
    return {
      moved: moves.length,
      reads: seatedRows
        .toSorted((left, right) => left.ordinal - right.ordinal)
        .map((row) => `${Math.floor(row.ordinal / 1_000_000)}:${row.turnItemId}`)
        .join(" "),
    };
  };

  it("seats imported rows between the tool calls they were written among", () => {
    // History above the run (prompts, and a reply the run shows again), then
    // the run's own rows; `x` is a row the record does not hold.
    const rows = thread(
      "i1@1",
      "i3@2",
      "i6@3",
      "i9@4",
      "t2@1000001",
      "m3@1000002",
      "t4@1000003",
      "t5@1000004",
      "x@1000005",
      "t7@1000006",
      "m8@1000007",
    );
    const seated = after(rows, {});
    assert.strictEqual(seated.reads, "0:i1 1:t2 1:m3 1:i3 1:t4 1:t5 1:i6 1:x 1:t7 1:m8 1:i9");
    // Rows below the first one seated step down; the rows above it stay.
    assert.strictEqual(seated.moved, 8);
  });

  it("moves a message a later run read late up to where it was written", () => {
    const rows = thread("m1@1000001", "t2@1000002", "m5@1000003", "m3@2000001", "m9@2000002");
    assert.strictEqual(after(rows, {}).reads, "1:m1 1:t2 1:m3 1:m5 2:m9");
  });

  it("is not thrown by tool calls that arrived in another order than they were written", () => {
    const rows = thread("t2@1000001", "t4@1000002", "t3@1000003", "m5@1000004", "i6@1000005");
    assert.deepEqual(planRecordSeating({ rows, runs: RUNS }), []);
  });

  it("moves a message T3 sent early below the reply written before it, and nothing else", () => {
    // The steer (line 4) was shown when sent, above the reply (line 3) the
    // turn was still writing; the record took it after that reply.
    const rows = thread("m1@1000001", "s4@1000002", "m3@1000003", "m5@1000004", "i2@1000005");
    const seated = after(rows, { only: "sent" });
    assert.strictEqual(seated.reads, "1:m1 1:m3 1:s4 1:m5 1:i2");
    assert.strictEqual(seated.moved, 2);
    // A sent message that already reads in order is left where it is.
    assert.deepEqual(
      planRecordSeating({
        rows: thread("m1@1000001", "s2@1000002", "x@1000003", "m3@1000004"),
        runs: RUNS,
        only: "sent",
      }),
      [],
    );
  });

  it("leaves the rows of a run still going", () => {
    const rows = thread("i2@1", "m1@1000001", "m3@1000002", "s9@2000001", "m8@2000002");
    assert.strictEqual(after(rows, { frozenBand: 2 }).reads, "1:m1 1:i2 1:m3 2:s9 2:m8");
  });
});

describe("the record's lines", () => {
  it("names what a turn item shows of the record", () => {
    assert.strictEqual(recordKeyOfTurnItem("cli-import:v2:cli-import-e2-s:uuid-1"), "uuid-1");
    assert.strictEqual(
      recordKeyOfTurnItem("turn-item:provider:claudeAgent:native-item:uuid-2"),
      "uuid-2",
    );
    assert.strictEqual(
      recordKeyOfTurnItem("turn-item:provider:claudeAgent:native-item:toolu_01"),
      "toolu_01",
    );
    assert.strictEqual(
      recordKeyOfTurnItem("turn-item:provider:claudeAgent:native-item:msg_01%253Athinking%253A0"),
      "msg_01",
    );
    assert.strictEqual(recordKeyOfTurnItem("turn-item:message:message%3Athread%3At"), null);
  });

  it("finds the line each tool call and API message is first written on", () => {
    const entry = (extra: object, content: ReadonlyArray<object>) =>
      JSON.stringify({ type: "assistant", ...extra, message: { id: "msg_01", content } });
    const lines = claudeRecordLines(
      [
        JSON.stringify({ type: "user", message: { content: '"assistant" toolu_00' } }),
        entry({}, [{ type: "thinking" }]),
        entry({}, [{ type: "tool_use", id: "toolu_01" }]),
        entry({ isSidechain: true }, [{ type: "tool_use", id: "toolu_02" }]),
        "not json",
      ].join("\n"),
    );
    assert.deepEqual(
      [...lines],
      [
        ["msg_01", 1],
        ["toolu_01", 2],
      ],
    );
  });

  it("ties a message T3 sent to the record row written after it, one row to a message", () => {
    const rows = [
      row("user", "u1", "continue", 1),
      row("assistant", "a1", "continue", 2),
      row("user", "u2", "continue", 5),
      row("user", "u3", "continue", 20),
      row("user", "u4", "continue", 59),
    ];
    const lines = sentPromptLines({
      sent: [
        { turnItemId: "sent-1", text: "continue", sentAtMs: ms(4) },
        { turnItemId: "sent-2", text: "something else", sentAtMs: ms(4) },
        { turnItemId: "sent-3", text: "continue", sentAtMs: ms(3) },
      ],
      rows,
      shownIds: new Set(["u1"]),
    });
    // u1 is shown under its own id, and was written before either was sent;
    // u2 is closest to sent-1, which leaves u3 for sent-3; u4 is out of reach.
    assert.deepEqual(
      [...lines],
      [
        ["sent-1", { line: 5, atMs: ms(5) }],
        ["sent-3", { line: 20, atMs: ms(20) }],
      ],
    );
  });
});

describe("a prompt the session took back", () => {
  const sent = (
    sourceId: string,
    text: string,
    minute: number,
    second: number,
    withdrawn = false,
  ): ParsedSessionMessage => ({
    role: "user",
    sourceId,
    text,
    timestamp: at(minute, second),
    lineIndex: minute * 60 + second,
    ...(withdrawn ? { withdrawn: true as const } : {}),
  });
  const copy = (
    messageId: string,
    role: "user" | "assistant",
    text: string,
    minute: number,
    second: number,
    runId: string,
  ): HealingKnownMessage => ({ messageId, role, text, atMs: ms(minute, second), runId });
  // Sent, taken back with Esc, and sent again in the same words a minute on.
  const rows = [
    row("user", "u1", "first prompt", 0),
    row("assistant", "a1", "first reply", 0),
    sent("taken-back", "put the rig under git", 1, 0, true),
    sent("resent", "put the rig under git", 2, 0),
    { ...row("assistant", "resent-reply", "done", 3), lineIndex: 180 },
  ];
  const known = [
    copy("m1", "user", "first prompt", 0, 0, "run-1"),
    copy("m2", "assistant", "first reply", 0, 0, "run-1"),
    // T3 may hold the copies in any order: the one closest in time is each row's.
    copy("m5", "assistant", "done", 3, 0, "run-3"),
    copy("m4", "user", "put the rig under git", 2, 1, "run-3"),
    copy("m3", "user", "put the rig under git", 1, 1, "run-2"),
  ];
  const runsWith = (second: string) => [
    { id: "run-1", status: "completed" },
    { id: "run-2", status: second },
    { id: "run-3", status: "completed" },
  ];
  const takenBack = (
    input: {
      known?: ReadonlyArray<HealingKnownMessage>;
      second?: string;
      fromLine?: number;
    } = {},
  ) =>
    planWithdrawnRuns({
      rows,
      known: input.known ?? known,
      runs: runsWith(input.second ?? "interrupted"),
      fromLine: input.fromLine ?? 0,
    });

  it("is not added to the thread", () => {
    assert.deepEqual(plan(rows, []), ["0:u1", "0:a1", "0:resent", "0:resent-reply"]);
  });

  it("takes back the run T3 started for it, not the resend's", () => {
    assert.deepEqual(takenBack(), ["run-2"]);
    assert.deepEqual(takenBack({ second: "cancelled" }), ["run-2"]);
  });

  it("leaves a run that has not ended, ended as its own turn, or was already taken back", () => {
    assert.deepEqual(takenBack({ second: "running" }), []);
    assert.deepEqual(takenBack({ second: "completed" }), []);
    assert.deepEqual(takenBack({ second: "rolled_back" }), []);
  });

  it("leaves a run that holds more than the prompt", () => {
    assert.deepEqual(
      takenBack({ known: [...known, copy("m6", "assistant", "half a reply", 1, 20, "run-2")] }),
      [],
    );
  });

  it("takes back nothing written before the thread's floor", () => {
    assert.deepEqual(takenBack({ fromLine: 61 }), []);
    assert.deepEqual(takenBack({ fromLine: 60 }), ["run-2"]);
  });
});
