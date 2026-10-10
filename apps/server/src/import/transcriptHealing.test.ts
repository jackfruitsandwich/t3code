import { assert, describe, it } from "@effect/vitest";

import type { ParsedSessionMessage } from "./sessionImportTypes.ts";
import {
  type HealingKnownMessage,
  planImportedRowReseating,
  planTranscriptHealing,
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
});
