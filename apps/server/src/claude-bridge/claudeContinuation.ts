/**
 * Where a Claude Code conversation went when it carried on under another
 * session id, read from the records themselves.
 *
 * Sending a session to the background (the left arrow at the prompt) ends its
 * record with a `continued-in` row naming the session the conversation goes
 * on in. That session's record, in the same folder, starts as a copy of the
 * old one. Claude Code shows one conversation and keeps the old id out of
 * /resume; nothing else links the two.
 *
 * @module claudeContinuation
 */
import * as Effect from "effect/Effect";
import type * as FileSystem from "effect/FileSystem";
import * as Option from "effect/Option";
import type * as Path from "effect/Path";

import { claudeContinuedInSession } from "./ClaudeBridgeProtocol.ts";

/** How much of a record's end is read for its `continued-in` row; rows can follow it. */
const CONTINUED_IN_SCAN_BYTES = 262_144;
/** A conversation sent on again and again: how many links are followed. */
const CONTINUATION_HOPS = 16;
/**
 * The new record is made in the moment the old one gets its last row, and a
 * live process keeps touching its record afterwards: the old record is one
 * made before the new one and written no earlier than this before it. Each
 * such record is asked with a read of its last rows only.
 */
const CONTINUED_FROM_WINDOW_MS = 120_000;
const CONTINUED_FROM_SCAN_BYTES = 16_384;

const RECORD_SUFFIX = ".jsonl";

export const makeClaudeContinuationReader = (fs: FileSystem.FileSystem, path: Path.Path) => {
  /** The session the record's last rows say its conversation continued in, or null. */
  const continuedIn = (file: string, scanBytes: number = CONTINUED_IN_SCAN_BYTES) =>
    Effect.gen(function* () {
      if (file.length === 0) return null;
      const size = yield* fs.stat(file).pipe(
        Effect.map((info) => Number(info.size)),
        Effect.orElseSucceed(() => 0),
      );
      if (size === 0) return null;
      const from = Math.max(0, size - scanBytes);
      const bytes = yield* Effect.scoped(
        Effect.gen(function* () {
          const handle = yield* fs.open(file, { flag: "r" });
          yield* handle.seek(BigInt(from), "start");
          return Option.getOrElse(yield* handle.readAlloc(size - from), () => new Uint8Array(0));
        }),
      ).pipe(Effect.orElseSucceed(() => new Uint8Array(0)));
      const text = new TextDecoder().decode(bytes);
      return claudeContinuedInSession(from === 0 ? text : text.slice(text.indexOf("\n") + 1));
    });

  /**
   * The session a conversation now lives in, following every link from the
   * record given; null when that record does not say it continued.
   */
  const continuationOf = (file: string) =>
    Effect.gen(function* () {
      let at = file;
      let last: string | null = null;
      for (let hop = 0; hop < CONTINUATION_HOPS; hop += 1) {
        const next = yield* continuedIn(at);
        if (next === null) break;
        last = next;
        at = path.join(path.dirname(at), `${next}${RECORD_SUFFIX}`);
      }
      return last;
    });

  /**
   * The session whose record, in the same folder, says its conversation
   * continued in the session this record belongs to; null when none does.
   */
  // A link once read stays: a record's last row does not change.
  const knownFrom = new Map<string, string>();
  const continuedFrom = (file: string) =>
    Effect.gen(function* () {
      if (file.length === 0) return null;
      const known = knownFrom.get(file);
      if (known !== undefined) return known;
      const sid = path.basename(file, RECORD_SUFFIX);
      const born = yield* fs.stat(file).pipe(
        Effect.map((info) => Option.getOrUndefined(info.birthtime)?.getTime()),
        Effect.orElseSucceed(() => undefined),
      );
      if (born === undefined) return null;
      const folder = path.dirname(file);
      const names = yield* fs
        .readDirectory(folder)
        .pipe(Effect.orElseSucceed((): ReadonlyArray<string> => []));
      for (const name of names) {
        if (!name.endsWith(RECORD_SUFFIX) || name === `${sid}${RECORD_SUFFIX}`) continue;
        const candidate = path.join(folder, name);
        const times = yield* fs.stat(candidate).pipe(
          Effect.map((info) => ({
            born: Option.getOrUndefined(info.birthtime)?.getTime(),
            written: Option.getOrUndefined(info.mtime)?.getTime(),
          })),
          Effect.orElseSucceed(() => undefined),
        );
        if (times?.written === undefined || times.written < born - CONTINUED_FROM_WINDOW_MS) {
          continue;
        }
        if (times.born !== undefined && times.born > born) continue;
        if ((yield* continuedIn(candidate, CONTINUED_FROM_SCAN_BYTES)) === sid) {
          const from = name.slice(0, -RECORD_SUFFIX.length);
          knownFrom.set(file, from);
          if (knownFrom.size > 256) knownFrom.delete(knownFrom.keys().next().value!);
          return from;
        }
      }
      return null;
    });

  /** Every session the conversation of this record came through, nearest first. */
  const continuedThrough = (file: string) =>
    Effect.gen(function* () {
      const through: Array<string> = [];
      let at = file;
      for (let hop = 0; hop < CONTINUATION_HOPS; hop += 1) {
        const before = yield* continuedFrom(at);
        if (before === null) break;
        through.push(before);
        at = path.join(path.dirname(at), `${before}${RECORD_SUFFIX}`);
      }
      return through;
    });

  return { continuedIn, continuationOf, continuedFrom, continuedThrough };
};
