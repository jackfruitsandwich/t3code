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
import type { ParsedSessionMessage, ParsedSessionSystemNotice } from "./sessionImportTypes.ts";

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
/** A run T3 starts for a prompt and that prompt's row in the record are this close in time. */
const SAME_TURN_WINDOW_MS = 90_000;
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
  // A reply shown from what the terminal drew stays under the id it was drawn
  // with: the record's row for it is that message, whatever its id.
  const shownUnderOwnId = new Map<number, HealingKnownMessage>();
  for (const pair of pairs.toSorted((left, right) => left.distance - right.distance)) {
    if (shown[pair.row] || taken.has(pair.candidate)) continue;
    shown[pair.row] = true;
    taken.add(pair.candidate);
    shownUnderOwnId.set(pair.row, candidates[pair.candidate]!.message);
  }
  const runStartsById = new Map(runs.map((run) => [run.id, run.startedAtMs]));
  const runsWithPrompt = new Set(
    known.flatMap((message) =>
      message.role === "user" && message.runId !== null ? [message.runId] : [],
    ),
  );

  // Second pass: a turn's rows stand or fall together where ids cannot tell.
  const missing: Array<HealingPlacement> = [];
  let commandShown = false;
  // A reply goes no higher than its prompt. A run that picked a turn up after
  // it ended started later than the turn's own rows were written, and shows
  // its prompt: by time alone the reply would land above it.
  const runOrdinals = new Map(runs.map((run) => [run.id, run.ordinal]));
  let promptBand = 0;
  rows.forEach((row, index) => {
    if (row.role === "user") {
      if (row.command === true) commandShown = shown[index] === true;
      else if (row.notice === undefined && !isClaudeHarnessPrompt(row.text)) {
        const holder = shownAs(row) ?? shownUnderOwnId.get(index);
        promptBand =
          holder !== undefined && holder.runId !== null
            ? (runOrdinals.get(holder.runId) ?? 0)
            : bandForTime(runs, timeMs(row.timestamp));
      }
      if (shown[index]) return;
      if (row.notice !== undefined || isClaudeHarnessPrompt(row.text)) return;
      // A prompt T3 shows in other words: the turn it started ran in T3, and
      // that run has its prompt. Only a run known to have none (one that
      // picked the turn up after it began) leaves the prompt to be added.
      // What was typed into a running turn started none, so it has no such run.
      // The run must be the one this prompt started: a turn the harness
      // started later (a task notice) has no row here to end the search, and
      // its replies belong to another run with another prompt.
      const promptAtMs = timeMs(row.timestamp);
      for (let next = index + 1; row.midTurn !== true && next < rows.length; next += 1) {
        const later = rows[next]!;
        if (later.role === "user" && later.command !== true) break;
        const live =
          later.role === "assistant"
            ? (knownByNative.get(later.sourceId) ?? shownUnderOwnId.get(next))
            : undefined;
        if (live === undefined) continue;
        if (live.runId === null) return;
        const startedAtMs = runStartsById.get(live.runId);
        if (
          runsWithPrompt.has(live.runId) &&
          startedAtMs !== undefined &&
          Math.abs(startedAtMs - promptAtMs) <= SAME_TURN_WINDOW_MS
        ) {
          return;
        }
      }
    } else {
      if (shown[index]) return;
      // What a command printed is shown with the command.
      if (row.command === true && commandShown) return;
    }
    const band = bandForTime(runs, timeMs(row.timestamp));
    missing.push({
      message: row,
      band: row.role === "assistant" ? Math.max(band, promptBand) : band,
    });
  });
  return missing;
}

export interface NoticePlacement {
  readonly notice: ParsedSessionSystemNotice;
  /** The run whose items the notice follows; 0 puts it before every run. */
  readonly band: number;
}

/**
 * The harness's notices between turns (a recap, a notice, a scheduled task
 * firing) the thread does not show, oldest first, each after the run that
 * was the latest when it was written. Only rows from line `fromLine` on: the
 * record before it is history the thread was never given, and stays out. A
 * run that was open when a notice was written shows it itself, under the
 * row's uuid (`shownIds`); while a turn is running, notices from
 * `settledBeforeMs` on are left to it.
 */
export function planNoticeHealing(input: {
  readonly notices: ReadonlyArray<ParsedSessionSystemNotice>;
  readonly fromLine: number;
  readonly shownIds: ReadonlySet<string>;
  readonly runs: ReadonlyArray<HealingRun>;
  readonly settledBeforeMs?: number;
}): ReadonlyArray<NoticePlacement> {
  return input.notices.flatMap((notice) => {
    const atMs = timeMs(notice.timestamp);
    if (notice.lineIndex < input.fromLine || input.shownIds.has(notice.sourceId)) return [];
    if (input.settledBeforeMs !== undefined && atMs >= input.settledBeforeMs) return [];
    return [{ notice, band: bandForTime(input.runs, atMs) }];
  });
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

/** One place of a thread, as the record sees it. */
export interface SeatingRow {
  readonly turnItemId: string;
  readonly ordinal: number;
  /** The record line the row shows; null when the record does not hold it. */
  readonly line: number | null;
  /**
   * `imported`: a message the importer placed itself. `message`: a message a
   * run delivered. `sent`: a message T3 sent into a running turn, shown where
   * it was sent (`line` is where the record wrote it; see `sentPromptLines`).
   * `other`: everything else (tool calls, reasoning, rows of T3's own).
   */
  readonly kind: "imported" | "message" | "sent" | "other";
  /** When the record wrote the row; 0 when it does not hold it. */
  readonly atMs: number;
}

export interface SeatingMove {
  readonly turnItemId: string;
  readonly ordinal: number;
}

/** A message sent into a turn reaches the record when the turn takes it, which can be at its end. */
const SENT_PROMPT_WINDOW_MS = 30 * 60_000;
/** The clocks of T3 and of the session's process differ by less than this. */
const SENT_PROMPT_SKEW_MS = 5_000;

/**
 * The record row of each message T3 sent into a running turn: the row with
 * its words, written closest after it was sent. T3's copy carries T3's id,
 * so the words and the time are what tie the two; each row stands for one
 * message. `shownIds` are the rows the thread shows under their own ids.
 */
export function sentPromptLines(input: {
  readonly sent: ReadonlyArray<{
    readonly turnItemId: string;
    readonly text: string;
    readonly sentAtMs: number;
  }>;
  readonly rows: ReadonlyArray<ParsedSessionMessage>;
  readonly shownIds: ReadonlySet<string>;
}): ReadonlyMap<string, { readonly line: number; readonly atMs: number }> {
  const pairs: Array<{ readonly sent: number; readonly row: number; readonly apartMs: number }> =
    [];
  input.rows.forEach((row, rowIndex) => {
    if (row.role !== "user" || row.notice !== undefined || input.shownIds.has(row.sourceId)) return;
    const text = normalize(row.text);
    const atMs = timeMs(row.timestamp);
    input.sent.forEach((message, sentIndex) => {
      const apartMs = atMs - message.sentAtMs;
      if (apartMs < -SENT_PROMPT_SKEW_MS || apartMs > SENT_PROMPT_WINDOW_MS) return;
      if (samePrompt(normalize(message.text), text)) {
        pairs.push({ sent: sentIndex, row: rowIndex, apartMs: Math.abs(apartMs) });
      }
    });
  });
  const lines = new Map<string, { readonly line: number; readonly atMs: number }>();
  const taken = new Set<number>();
  for (const pair of pairs.toSorted((left, right) => left.apartMs - right.apartMs)) {
    const message = input.sent[pair.sent]!;
    if (lines.has(message.turnItemId) || taken.has(pair.row)) continue;
    taken.add(pair.row);
    const row = input.rows[pair.row]!;
    lines.set(message.turnItemId, { line: row.lineIndex, atMs: timeMs(row.timestamp) });
  }
  return lines;
}

/** What a turn item shows of the record: a row's uuid, a tool call's id, or a reply's API message id. */
export const recordKeyOfTurnItem = (turnItemId: string): string | null => {
  let plain = turnItemId;
  for (let pass = 0; pass < 3 && plain.includes("%"); pass += 1) {
    try {
      plain = decodeURIComponent(plain);
    } catch {
      break;
    }
  }
  const native = plain.indexOf("native-item:");
  if (native === -1) {
    return plain.startsWith("cli-import:") ? plain.slice(plain.lastIndexOf(":") + 1) : null;
  }
  const tail = plain.slice(native + "native-item:".length);
  // A reply's reasoning is `<API message id>:thinking:<n>`.
  return /^(?:msg|toolu)_/u.test(tail) ? (tail.split(":")[0] ?? null) : tail;
};

/**
 * The first line of a Claude Code transcript each tool call and each API
 * message is written on, by id. Subagent rows are another conversation.
 */
export const claudeRecordLines = (content: string): ReadonlyMap<string, number> => {
  const lines = new Map<string, number>();
  content.split("\n").forEach((line, lineIndex) => {
    if (!line.includes('"assistant"')) return;
    let entry: unknown;
    try {
      entry = JSON.parse(line);
    } catch {
      return;
    }
    if (typeof entry !== "object" || entry === null) return;
    const { type, isSidechain, message } = entry as Record<string, unknown>;
    if (type !== "assistant" || isSidechain === true) return;
    if (typeof message !== "object" || message === null) return;
    const { id, content: blocks } = message as Record<string, unknown>;
    if (typeof id === "string" && !lines.has(id)) lines.set(id, lineIndex);
    if (!Array.isArray(blocks)) return;
    for (const block of blocks) {
      if (typeof block !== "object" || block === null) continue;
      const { type: blockType, id: blockId } = block as Record<string, unknown>;
      if (blockType === "tool_use" && typeof blockId === "string" && !lines.has(blockId)) {
        lines.set(blockId, lineIndex);
      }
    }
  });
  return lines;
};

/** The longest run of `lines`, by position, that never steps back. */
const longestInOrder = (lines: ReadonlyArray<number>): ReadonlyArray<number> => {
  const tails: Array<number> = [];
  const before: Array<number> = lines.map(() => -1);
  lines.forEach((line, position) => {
    let low = 0;
    let high = tails.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      if (lines[tails[middle]!]! <= line) low = middle + 1;
      else high = middle;
    }
    if (low > 0) before[position] = tails[low - 1]!;
    tails[low] = position;
  });
  const kept: Array<number> = [];
  for (let position = tails.at(-1) ?? -1; position !== -1; position = before[position]!) {
    kept.push(position);
  }
  return kept.toReversed();
};

/**
 * Where a thread's messages belong by the record's own line order, for a
 * thread whose rows the record can place: the places to change, and nothing
 * when the thread already reads in that order.
 *
 * Three kinds of message sit out of order. One the importer placed (it only
 * knew the row's time, so it put the row above the first run, or at the end
 * of a run, instead of between the tool calls it was written among). One a
 * run read late: a run that picks a turn up shows rows written while an
 * earlier run was showing that turn. And one T3 sent into a turn that took
 * it later than it was sent: it sits above rows the record wrote before it.
 * Tool calls, reasoning and every row the record does not hold stay as they
 * are, and give the order the messages are seated in: a message goes right
 * after the last such row written before it. Where that leaves a choice
 * between two runs, the row's time decides, as `bandForTime` does.
 *
 * `only: "sent"` seats the third kind alone and leaves the others where the
 * importer's other passes put them.
 *
 * A place holds one row, so rows beside a seated one step along, as far as
 * the next free place, on whichever side moves fewer. Rows of `frozenBand`
 * and later (a turn still running) are not looked at.
 */
export function planRecordSeating(input: {
  readonly rows: ReadonlyArray<SeatingRow>;
  readonly runs: ReadonlyArray<HealingRun>;
  readonly frozenBand?: number;
  readonly only?: "sent";
}): ReadonlyArray<SeatingMove> {
  const bandOf = (ordinal: number) => Math.floor(ordinal / RUN_BAND);
  const lastBand = input.frozenBand === undefined ? Number.POSITIVE_INFINITY : input.frozenBand - 1;
  const rows = input.rows
    .filter((row) => row.ordinal >= 0 && bandOf(row.ordinal) <= lastBand)
    .toSorted((left, right) => left.ordinal - right.ordinal);

  // A run's message written before a message an earlier run shows was read late.
  const rest: Array<SeatingRow> = [];
  const seated: Array<SeatingRow & { readonly line: number }> = [];
  let band = -1;
  let shownBefore = -1;
  let shownSoFar = -1;
  for (const row of rows) {
    if (bandOf(row.ordinal) !== band) {
      band = bandOf(row.ordinal);
      shownBefore = shownSoFar;
    }
    const { line } = row;
    if (
      line !== null &&
      input.only === undefined &&
      (row.kind === "imported" || (row.kind === "message" && line < shownBefore))
    ) {
      seated.push({ ...row, line });
      continue;
    }
    if (line !== null && row.kind === "message") shownSoFar = Math.max(shownSoFar, line);
    rest.push(row);
  }

  // The rows that stay, less any that are themselves out of line, are the frame.
  const held = rest.flatMap((row, index) =>
    row.line === null || row.kind === "sent" ? [] : [{ index, line: row.line }],
  );
  const kept = longestInOrder(held.map((row) => row.line)).map((position) => held[position]!);
  // A sent message stays unless the thread shows, below it, a row written before it.
  const early = new Set<number>();
  let next = 0;
  rest.forEach((row, index) => {
    if (row.kind !== "sent" || row.line === null) return;
    while (next < kept.length && kept[next]!.index < index) next += 1;
    const below = kept[next];
    if (below === undefined || below.line >= row.line) return;
    early.add(index);
    seated.push({ ...row, line: row.line });
  });
  if (seated.length === 0) return [];
  const fixed = rest.filter((_, index) => !early.has(index));
  const fixedIndex = new Map(fixed.map((row, index) => [row.turnItemId, index]));
  const frame = kept.map((row) => ({
    index: fixedIndex.get(rest[row.index]!.turnItemId)!,
    line: row.line,
  }));
  const bandEnd = (after: number) => {
    const index = fixed.findIndex((row) => bandOf(row.ordinal) > after);
    return index === -1 ? fixed.length : index;
  };
  // `slot` k: right above the k-th row that stays.
  const slots = new Map<number, Array<{ readonly row: SeatingRow; readonly band: number }>>();
  for (const row of seated.toSorted(
    (left, right) => left.line - right.line || left.ordinal - right.ordinal,
  )) {
    let low = 0;
    let high = frame.length;
    while (low < high) {
      const middle = (low + high) >> 1;
      if (frame[middle]!.line <= row.line) low = middle + 1;
      else high = middle;
    }
    const above = frame[low - 1];
    const below = frame[low];
    const aboveBand = above === undefined ? 0 : bandOf(fixed[above.index]!.ordinal);
    const belowBand = below === undefined ? lastBand : bandOf(fixed[below.index]!.ordinal);
    const rowBand = Math.min(Math.max(bandForTime(input.runs, row.atMs), aboveBand), belowBand);
    const slot =
      above !== undefined && rowBand === aboveBand
        ? above.index + 1
        : below !== undefined && rowBand === belowBand
          ? below.index
          : bandEnd(rowBand);
    slots.set(slot, [...(slots.get(slot) ?? []), { row, band: rowBand }]);
  }
  const order: Array<{
    readonly row: SeatingRow;
    readonly band: number;
    readonly stays: boolean;
  }> = [];
  for (let index = 0; index <= fixed.length; index += 1) {
    for (const entry of (slots.get(index) ?? []).toSorted(
      (left, right) => left.band - right.band,
    )) {
      // A seated row already between the rows it belongs between stays too.
      const above = fixed[index - 1];
      const below = fixed[index];
      order.push({
        ...entry,
        stays:
          bandOf(entry.row.ordinal) === entry.band &&
          (above === undefined || above.ordinal < entry.row.ordinal) &&
          (below === undefined || entry.row.ordinal < below.ordinal),
      });
    }
    const row = fixed[index];
    if (row !== undefined) order.push({ row, band: bandOf(row.ordinal), stays: true });
  }

  // Each band's rows take places in that order, keeping the place they have
  // where they can: counted from the band's top and from its bottom, and the
  // way that moves fewer rows is the one used.
  const moves: Array<SeatingMove> = [];
  for (let start = 0; start < order.length;) {
    const inBand = order[start]!.band;
    let end = start;
    while (end < order.length && order[end]!.band === inBand) end += 1;
    const members = order.slice(start, end);
    // The order runs through the bands one after another, or nothing is moved.
    if (start > 0 && order[start - 1]!.band > inBand) return [];
    const first = inBand * RUN_BAND + 1;
    const final = inBand * RUN_BAND + RUN_BAND - 1;
    const holds = (row: SeatingRow) => bandOf(row.ordinal) === inBand;
    const place = (from: "top" | "bottom"): ReadonlyArray<SeatingMove> | null => {
      const placed: Array<SeatingMove> = [];
      let edge =
        from === "top"
          ? first - 1
          : Math.max(
              first + members.length - 1,
              ...members.flatMap((member) => (holds(member.row) ? [member.row.ordinal] : [])),
            ) + 1;
      for (const { row, stays } of from === "top" ? members : members.toReversed()) {
        const keeps = stays && (from === "top" ? row.ordinal > edge : row.ordinal < edge);
        const ordinal = keeps ? row.ordinal : from === "top" ? edge + 1 : edge - 1;
        if (ordinal < first || ordinal > final) return null;
        if (ordinal !== row.ordinal) placed.push({ turnItemId: row.turnItemId, ordinal });
        edge = ordinal;
      }
      return placed;
    };
    const fromTop = place("top");
    const fromBottom = place("bottom");
    const chosen =
      fromTop === null || (fromBottom !== null && fromBottom.length < fromTop.length)
        ? fromBottom
        : fromTop;
    // A band with no room left: nothing is moved anywhere.
    if (chosen === null) return [];
    moves.push(...chosen);
    start = end;
  }
  return moves;
}
