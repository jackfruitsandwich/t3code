import { assert, describe, it } from "@effect/vitest";
import type { NonEmptyReadonlyArray } from "effect/Array";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as Socket from "effect/socket/Socket";

import * as CodexError from "../errors.ts";
import { makeTerminationError, makeWebSocketStdio } from "./stdio.ts";

describe("Codex App Server child process termination", () => {
  it.effect("retains the process identifier with the exit code", () =>
    Effect.gen(function* () {
      const error = yield* makeTerminationError({
        pid: ChildProcessSpawner.ProcessId(51),
        exitCode: Effect.succeed(ChildProcessSpawner.ExitCode(9)),
      });

      assert.instanceOf(error, CodexError.CodexAppServerProcessExitedError);
      assert.equal(error.pid, 51);
      assert.equal(error.code, 9);
      assert.equal(error.message, "Codex App Server process exited with code 9");
    }),
  );

  it.effect("retains the process identifier and exact exit-status cause", () =>
    Effect.gen(function* () {
      const rootCause = new Error("private process diagnostics");
      const cause = PlatformError.systemError({
        _tag: "Unknown",
        module: "ChildProcess",
        method: "exitCode",
        cause: rootCause,
      });
      const error = yield* makeTerminationError({
        pid: ChildProcessSpawner.ProcessId(52),
        exitCode: Effect.fail(cause),
      });

      assert.instanceOf(error, CodexError.CodexAppServerTransportError);
      assert.equal(error.pid, 52);
      assert.strictEqual(error.cause, cause);
      assert.equal(
        error.message,
        "Codex App Server transport operation 'read-process-exit-status' failed.",
      );
      assert.notInclude(error.message, rootCause.message);
    }),
  );
});

describe("Codex App Server WebSocket stdio", () => {
  it.effect("turns frames into lines and sends lines as frames without their newline", () =>
    Effect.gen(function* () {
      const encoder = new TextEncoder();
      const decoder = new TextDecoder();
      const frames = yield* Queue.unbounded<NonEmptyReadonlyArray<Uint8Array>>();
      const sent: Array<string | Uint8Array> = [];
      const socket = Socket.make({
        reader: Effect.succeed({
          pull: Queue.take(frames),
          upgrade: () => Effect.void,
        }),
        writer: Effect.succeed({
          write: (chunk) =>
            Effect.sync(() => {
              if (!Socket.isCloseEvent(chunk)) sent.push(chunk);
            }),
          writeAll: (chunks) =>
            Effect.sync(() => {
              sent.push(...chunks);
            }),
        }),
      });
      yield* Queue.offer(frames, [encoder.encode('{"id":1}'), encoder.encode('{"id":2}')]);

      const stdio = yield* makeWebSocketStdio(socket);
      const lines = yield* stdio.stdin.pipe(
        Stream.map((bytes) => decoder.decode(bytes)),
        Stream.take(2),
        Stream.runCollect,
      );
      assert.deepStrictEqual([...lines], ['{"id":1}\n', '{"id":2}\n']);

      yield* Stream.make('{"id":3}\n', '{"id":4}\r\n', encoder.encode('{"id":5}\n')).pipe(
        Stream.run(stdio.stdout()),
      );
      assert.deepStrictEqual(
        sent.map((chunk) => (typeof chunk === "string" ? chunk : decoder.decode(chunk))),
        ['{"id":3}', '{"id":4}', '{"id":5}'],
      );
    }).pipe(Effect.scoped),
  );
});
