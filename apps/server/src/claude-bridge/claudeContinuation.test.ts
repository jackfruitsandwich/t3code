import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, describe, it } from "@effect/vitest";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";

import { makeClaudeContinuationReader } from "./claudeContinuation.ts";

const said = (uuid: string) =>
  `${JSON.stringify({ type: "assistant", uuid, timestamp: "2026-10-11T10:29:00.000Z", message: { role: "assistant", content: [{ type: "text", text: "ok" }] } })}\n`;
const continuedIn = (from: string, to: string) =>
  `${JSON.stringify({ type: "continued-in", timestamp: "2026-10-11T10:29:18.511Z", sessionId: from, continuedInSessionId: to })}\n`;

const folderOfRecords = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const folder = yield* fs.makeTempDirectoryScoped({ prefix: "t3-continuation-" });
  const record = (sid: string) => path.join(folder, `${sid}.jsonl`);
  const write = (sid: string, rows: string) => fs.writeFileString(record(sid), rows);
  return { fs, reader: makeClaudeContinuationReader(fs, path), record, write };
});

describe("where a Claude conversation went when it carried on under another session id", () => {
  it.live("follows every link forward from a record, and none from one that did not continue", () =>
    Effect.gen(function* () {
      const { reader, record, write } = yield* folderOfRecords;
      yield* write("first", said("a1") + continuedIn("first", "second"));
      yield* write("second", said("a1") + said("a2") + continuedIn("second", "third"));
      yield* write("third", said("a1") + said("a2") + said("a3"));
      assert.strictEqual(yield* reader.continuedIn(record("first")), "second");
      assert.strictEqual(yield* reader.continuationOf(record("first")), "third");
      assert.strictEqual(yield* reader.continuationOf(record("second")), "third");
      assert.strictEqual(yield* reader.continuationOf(record("third")), null);
      assert.strictEqual(yield* reader.continuationOf(record("no-such-session")), null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("finds the sessions a conversation came through, nearest first", () =>
    Effect.gen(function* () {
      const { reader, record, write } = yield* folderOfRecords;
      yield* write("first", said("a1") + continuedIn("first", "second"));
      yield* write("bystander", said("b1"));
      yield* write("second", said("a1") + said("a2") + continuedIn("second", "third"));
      yield* write("third", said("a1") + said("a2") + said("a3"));
      assert.strictEqual(yield* reader.continuedFrom(record("third")), "second");
      assert.deepStrictEqual(yield* reader.continuedThrough(record("third")), ["second", "first"]);
      assert.deepStrictEqual(yield* reader.continuedThrough(record("first")), []);
      assert.deepStrictEqual(yield* reader.continuedThrough(record("bystander")), []);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("finds the old record although its process keeps touching the file", () =>
    Effect.gen(function* () {
      const { fs, reader, record, write } = yield* folderOfRecords;
      yield* write("old", said("a1") + continuedIn("old", "new"));
      yield* write("new", said("a1"));
      // A live process touches its record long after its last row (seen: hours later).
      const later = DateTime.toDateUtc(DateTime.add(yield* DateTime.now, { hours: 6 }));
      yield* fs.utimes(record("old"), later, later);
      assert.strictEqual(yield* reader.continuedFrom(record("new")), "old");
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );

  it.live("takes a session taken up again in its own right as not continued", () =>
    Effect.gen(function* () {
      const { reader, record, write } = yield* folderOfRecords;
      yield* write("old", said("a1") + continuedIn("old", "new") + said("a9"));
      yield* write("new", said("a1"));
      assert.strictEqual(yield* reader.continuationOf(record("old")), null);
      assert.strictEqual(yield* reader.continuedFrom(record("new")), null);
    }).pipe(Effect.scoped, Effect.provide(NodeServices.layer)),
  );
});
