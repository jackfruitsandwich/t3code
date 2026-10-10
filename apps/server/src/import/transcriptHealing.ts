/**
 * transcriptHealing - works out which rows of a live Claude Code session's
 * transcript its T3 thread does not show, and where in the thread they belong.
 *
 * A thread attached to a running session gets its turns live. Anything the
 * attachment missed (the server was down, the thread did not exist yet, a turn
 * was dropped) used to stay missing while the session lived, and what was
 * caught up afterwards landed above every run instead of where it happened.
 * The session's transcript is the record; this module compares it with the
 * thread row by row and places each missing row after the run that was the
 * latest when the row was written.
 *
 * Pure: the importer feeds it the transcript and the thread's state and
 * writes what it returns.
 *
 * @module import/transcriptHealing
 */
import { isClaudeHarnessPrompt } from "../claude-bridge/ClaudeBridgeProtocol.ts";
import type { ParsedSessionMessage } from "./sessionImportTypes.ts";

/** Turn items of run N sit at N * RUN_BAND + 1 and up; items without a run below RUN_BAND. */
export const RUN_BAND = 1_000_000;

export interface HealingKnownMessage {
  readonly messageId: string;
  readonly role: "user" | "assistant";
  readonly text: string;
  readonly atMs: number;
  readonly runId: string | null;
}

export interface HealingRun {
  readonly id: string;
  readonly ordinal: number;
  readonly startedAtMs: number;
}

export interface HealingPlacement {
  readonly message: ParsedSessionMessage;
  /** The run whose items the row follows; 0 puts it before every run. */
  readonly band: number;
}

export interface HealingImportedItem {
  readonly sourceId: string;
  readonly ordinal: number;
}

/** A reply T3 streamed and the transcript row for it are written within moments of each other. */
const SAME_REPLY_WINDOW_MS = 120_000;
/** Shorter texts ("yes", "continue") only match whole. */
const MIN_PARTIAL_MATCH_LENGTH = 16;

const normalize = (text: string) => text.replace(/\s+/gu, " ").trim();

const timeMs = (iso: string) => {
  const value = Date.parse(iso);
  return Number.isNaN(value) ? 0 : value;
};

/** T3 may wrap a prompt (an effort prefix, attachment notes); the typed text stays inside it. */
const samePrompt = (shown: string, written: string) =>
  shown === written ||
  (shown.length >= MIN_PARTIAL_MATCH_LENGTH &&
    (written.endsWith(shown) || written.startsWith(shown))) ||
  (written.length >= MIN_PARTIAL_MATCH_LENGTH &&
    (shown.endsWith(written) || shown.startsWith(written)));

/** The band a row written at `atMs` belongs to: the latest run that had started by then. */
export const bandForTime = (runs: ReadonlyArray<HealingRun>, atMs: number): number => {
  let band = 0;
  for (const run of runs) {
    if (run.startedAtMs <= atMs && run.ordinal > band) band = run.ordinal;
  }
  return band;
};

/**
 * The transcript rows the thread does not show, oldest first, each with the
 * run it follows.
 *
 * A reply is identified by its row uuid, which T3's live copy carries in its
 * message id; a prompt by its text, since T3's copy of a prompt has its own
 * id. A prompt T3 reworded still counts as shown when the turn it started is
 * one T3 ran. Rows the Claude Code harness wrote for itself (task notices,
 * hook feedback, interruptions) are not conversation and are never added.
 */
export function planTranscriptHealing(input: {
  readonly rows: ReadonlyArray<ParsedSessionMessage>;
  readonly known: ReadonlyArray<HealingKnownMessage>;
  readonly runs: ReadonlyArray<HealingRun>;
  readonly importedMessageId: (sourceId: string) => string;
}): ReadonlyArray<HealingPlacement> {
  const { rows, known, runs } = input;
  const knownById = new Map(known.map((message) => [message.messageId, message]));
  // T3's own copies of native rows carry the row's uuid after the last colon.
  const knownByNative = new Map<string, HealingKnownMessage>();
  for (const message of known) {
    const colon = message.messageId.lastIndexOf(":");
    if (colon !== -1) knownByNative.set(message.messageId.slice(colon + 1), message);
  }
  const importedIds = new Set(rows.map((row) => input.importedMessageId(row.sourceId)));
  const shownAs = (row: ParsedSessionMessage) =>
    knownById.get(input.importedMessageId(row.sourceId)) ?? knownByNative.get(row.sourceId);

  // First pass: what each row is shown as, if anything. Rows T3 holds under
  // its own ids pair off with T3's messages by text, closest in time first,
  // each message standing for one row.
  const shown: Array<boolean> = rows.map((row) => shownAs(row) !== undefined);
  const candidates = known
    .filter((message) => !importedIds.has(message.messageId))
    .map((message) => ({ message, text: normalize(message.text) }));
  const pairs: Array<{
    readonly row: number;
    readonly candidate: number;
    readonly distance: number;
  }> = [];
  rows.forEach((row, index) => {
    if (shown[index] || (row.role === "assistant" && row.command === true)) return;
    const text = normalize(row.text);
    const atMs = timeMs(row.timestamp);
    candidates.forEach((candidate, candidateIndex) => {
      if (candidate.message.role !== row.role) return;
      const distance = Math.abs(candidate.message.atMs - atMs);
      const matches =
        row.role === "assistant"
          ? candidate.text === text && distance <= SAME_REPLY_WINDOW_MS
          : samePrompt(candidate.text, text);
      if (matches) pairs.push({ row: index, candidate: candidateIndex, distance });
    });
  });
  const taken = new Set<number>();
  for (const pair of pairs.toSorted((left, right) => left.distance - right.distance)) {
    if (shown[pair.row] || taken.has(pair.candidate)) continue;
    shown[pair.row] = true;
    taken.add(pair.candidate);
  }
  const runsWithPrompt = new Set(
    known.flatMap((message) =>
      message.role === "user" && message.runId !== null ? [message.runId] : [],
    ),
  );

  // Second pass: a turn's rows stand or fall together where ids cannot tell.
  const missing: Array<HealingPlacement> = [];
  let commandShown = false;
  rows.forEach((row, index) => {
    if (row.role === "user") {
      if (row.command === true) commandShown = shown[index] === true;
      if (shown[index]) return;
      if (isClaudeHarnessPrompt(row.text)) return;
      // A prompt T3 shows in other words: the turn it started ran in T3, and
      // that run has its prompt. Only a run known to have none (one that
      // picked the turn up after it began) leaves the prompt to be added.
      // What was typed into a running turn started none, so it has no such run.
      for (let next = index + 1; row.midTurn !== true && next < rows.length; next += 1) {
        const later = rows[next]!;
        if (later.role === "user" && later.command !== true) break;
        const live = later.role === "assistant" ? knownByNative.get(later.sourceId) : undefined;
        if (live !== undefined && (live.runId === null || runsWithPrompt.has(live.runId))) return;
      }
    } else {
      if (shown[index]) return;
      // What a command printed is shown with the command.
      if (row.command === true && commandShown) return;
    }
    missing.push({ message: row, band: bandForTime(runs, timeMs(row.timestamp)) });
  });
  return missing;
}

/**
 * Rows imported earlier that sit in the wrong place: they were all put before
 * the thread's first run, whenever they were written. Returns them oldest
 * first with the band each belongs to.
 */
export function planImportedRowReseating(input: {
  readonly rows: ReadonlyArray<ParsedSessionMessage>;
  readonly imported: ReadonlyArray<HealingImportedItem>;
  readonly runs: ReadonlyArray<HealingRun>;
}): ReadonlyArray<HealingPlacement> {
  if (input.runs.length === 0) return [];
  const ordinals = new Map(input.imported.map((item) => [item.sourceId, item.ordinal]));
  return input.rows.flatMap((row) => {
    const ordinal = ordinals.get(row.sourceId);
    if (ordinal === undefined) return [];
    const band = bandForTime(input.runs, timeMs(row.timestamp));
    return Math.floor(ordinal / RUN_BAND) === band ? [] : [{ message: row, band }];
  });
}

/**
 * Imported rows that sit above rows written before them in the same band. A
 * row added now goes to the end of its band, below rows added on an earlier
 * pass, whatever their times. Returns, for each such band, the rows from the
 * first one out of order to the last, oldest first: put back at the band's
 * end in that order, the band's imported rows read in the order they were
 * written.
 */
export function planImportedRowOrdering(input: {
  readonly rows: ReadonlyArray<ParsedSessionMessage>;
  readonly imported: ReadonlyArray<HealingImportedItem>;
}): ReadonlyArray<HealingPlacement> {
  const rowsById = new Map(input.rows.map((row) => [row.sourceId, row]));
  const bands = new Map<
    number,
    Array<{ readonly row: ParsedSessionMessage; readonly ordinal: number; readonly atMs: number }>
  >();
  for (const item of input.imported) {
    const row = rowsById.get(item.sourceId);
    if (row === undefined) continue;
    const band = Math.floor(item.ordinal / RUN_BAND);
    const items = bands.get(band) ?? [];
    items.push({ row, ordinal: item.ordinal, atMs: timeMs(row.timestamp) });
    bands.set(band, items);
  }
  const moves: Array<HealingPlacement> = [];
  for (const [band, items] of bands) {
    const placed = items.toSorted((left, right) => left.ordinal - right.ordinal);
    let earliestBelow = Number.POSITIVE_INFINITY;
    let firstOutOfOrder = -1;
    for (let index = placed.length - 1; index >= 0; index -= 1) {
      if (placed[index]!.atMs > earliestBelow) firstOutOfOrder = index;
      earliestBelow = Math.min(earliestBelow, placed[index]!.atMs);
    }
    if (firstOutOfOrder === -1) continue;
    for (const item of placed
      .slice(firstOutOfOrder)
      .toSorted((left, right) => left.atMs - right.atMs || left.ordinal - right.ordinal)) {
      moves.push({ message: item.row, band });
    }
  }
  return moves;
}
