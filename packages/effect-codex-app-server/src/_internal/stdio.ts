import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as PlatformError from "effect/PlatformError";
import * as Queue from "effect/Queue";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Stream from "effect/Stream";
import { ChildProcessSpawner } from "effect/process";
import * as Socket from "effect/socket/Socket";

import * as CodexError from "../errors.ts";

const encoder = new TextEncoder();

export const makeChildStdio = (handle: ChildProcessSpawner.ChildProcessHandle) =>
  Stdio.make({
    args: Effect.succeed([]),
    stdin: handle.stdout,
    stdout: () =>
      Sink.mapInput(handle.stdin, (chunk: string | Uint8Array) =>
        typeof chunk === "string" ? encoder.encode(chunk) : chunk,
      ),
    stderr: () => Sink.drain,
  });

export const makeInMemoryStdio = Effect.fn("makeInMemoryStdio")(function* () {
  const input = yield* Queue.unbounded<Uint8Array, Cause.Done<void>>();
  const output = yield* Queue.unbounded<string>();
  const decoder = new TextDecoder();

  return {
    stdio: Stdio.make({
      args: Effect.succeed([]),
      stdin: Stream.fromQueue(input),
      stdout: () =>
        Sink.forEach((chunk: string | Uint8Array) =>
          Queue.offer(
            output,
            typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true }),
          ),
        ),
      stderr: () => Sink.drain,
    }),
    input,
    output,
  };
});

type ChildProcessTerminationHandle = Pick<
  ChildProcessSpawner.ChildProcessHandle,
  "exitCode" | "pid"
>;

export const makeTerminationError = (
  handle: ChildProcessTerminationHandle,
): Effect.Effect<CodexError.CodexAppServerError> =>
  Effect.match(handle.exitCode, {
    onFailure: (cause) =>
      new CodexError.CodexAppServerTransportError({
        operation: "read-process-exit-status",
        pid: handle.pid,
        cause,
      }),
    onSuccess: (code) => new CodexError.CodexAppServerProcessExitedError({ code, pid: handle.pid }),
  });

const NEWLINE = 10;
const CARRIAGE_RETURN = 13;

const socketPlatformError = (method: "read" | "write", cause: Socket.SocketError) =>
  new PlatformError.PlatformError(
    new PlatformError.SystemError({
      _tag: "Unknown",
      module: "Socket",
      method,
      description: cause.message,
      cause,
    }),
  );

const withTrailingNewline = (frame: Uint8Array): Uint8Array => {
  const line = new Uint8Array(frame.length + 1);
  line.set(frame);
  line[frame.length] = NEWLINE;
  return line;
};

const withoutTrailingNewline = (chunk: string | Uint8Array): string | Uint8Array => {
  if (typeof chunk === "string") {
    return chunk.replace(/\r?\n$/, "");
  }
  let end = chunk.length;
  while (end > 0 && (chunk[end - 1] === NEWLINE || chunk[end - 1] === CARRIAGE_RETURN)) {
    end -= 1;
  }
  return end === chunk.length ? chunk : chunk.subarray(0, end);
};

/**
 * JSON-RPC over a WebSocket, one message per frame, shaped like the
 * newline-delimited stdio the protocol reads: each incoming frame becomes a
 * line, each outgoing line is sent without its newline. The socket closing
 * ends `stdin`, which the protocol treats like the app-server exiting.
 */
export const makeWebSocketStdio = Effect.fn("makeWebSocketStdio")(function* (
  socket: Socket.Socket,
) {
  const writer = yield* socket.writer;
  return Stdio.make({
    args: Effect.succeed([]),
    stdin: Socket.toStream(socket).pipe(
      Stream.map(withTrailingNewline),
      Stream.mapError((cause) => socketPlatformError("read", cause)),
    ),
    stdout: () =>
      Sink.forEach((chunk: string | Uint8Array) =>
        writer
          .write(withoutTrailingNewline(chunk))
          .pipe(Effect.mapError((cause) => socketPlatformError("write", cause))),
      ),
    stderr: () => Sink.drain,
  });
});
