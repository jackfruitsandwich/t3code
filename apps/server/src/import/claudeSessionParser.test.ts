import { assert, describe, it } from "@effect/vitest";

import { claudeTaskNotice, parseClaudeSessionJsonl } from "./claudeSessionParser.ts";
import { isEligibleCliSession } from "./sessionImportTypes.ts";

const SESSION_ID = "11111111-1111-4111-8111-111111111111";

function line(entry: Record<string, unknown>): string {
  return `${JSON.stringify(entry)}\n`;
}

function userEntry(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "user",
    uuid: "user-uuid-1",
    parentUuid: null,
    sessionId: SESSION_ID,
    cwd: "/repo/project",
    timestamp: "2026-07-01T10:00:00.000Z",
    isSidechain: false,
    message: { role: "user", content: "Fix the login bug" },
    ...overrides,
  };
}

function assistantEntry(overrides: Record<string, unknown> = {}): Record<string, unknown> {
  return {
    type: "assistant",
    uuid: "assistant-uuid-1",
    parentUuid: "user-uuid-1",
    sessionId: SESSION_ID,
    cwd: "/repo/project",
    timestamp: "2026-07-01T10:00:05.000Z",
    isSidechain: false,
    message: {
      role: "assistant",
      content: [
        { type: "thinking", thinking: "hmm" },
        { type: "text", text: "I found the bug." },
        { type: "tool_use", id: "tool-1", name: "Bash", input: {} },
      ],
    },
    ...overrides,
  };
}

describe("parseClaudeSessionJsonl", () => {
  it("extracts user/assistant text, session metadata, and preserves timestamps", () => {
    const parsed = parseClaudeSessionJsonl(line(userEntry()) + line(assistantEntry()));

    assert.equal(parsed.sessionId, SESSION_ID);
    assert.equal(parsed.cwd, "/repo/project");
    assert.equal(parsed.startedAt, "2026-07-01T10:00:00.000Z");
    assert.deepEqual(
      parsed.messages.map((message) => ({
        role: message.role,
        text: message.text,
        timestamp: message.timestamp,
        sourceId: message.sourceId,
      })),
      [
        {
          role: "user",
          text: "Fix the login bug",
          timestamp: "2026-07-01T10:00:00.000Z",
          sourceId: "user-uuid-1",
        },
        {
          role: "assistant",
          text: "I found the bug.",
          timestamp: "2026-07-01T10:00:05.000Z",
          sourceId: "assistant-uuid-1",
        },
      ],
    );
    assert.equal(parsed.lineCount, 2);
    assert.equal(parsed.malformedLineCount, 0);
    assert.isTrue(isEligibleCliSession(parsed));
  });

  it("handles user content as block arrays and skips tool_result-only entries", () => {
    const content =
      line(
        userEntry({
          message: {
            role: "user",
            content: [{ type: "text", text: "block text" }],
          },
        }),
      ) +
      line(
        userEntry({
          uuid: "user-uuid-2",
          message: {
            role: "user",
            content: [{ type: "tool_result", tool_use_id: "tool-1", content: "output" }],
          },
        }),
      );
    const parsed = parseClaudeSessionJsonl(content);

    assert.deepEqual(
      parsed.messages.map((message) => message.text),
      ["block text"],
    );
  });

  it("skips sidechain and meta entries", () => {
    const content =
      line(userEntry({ isSidechain: true })) +
      line(userEntry({ uuid: "user-uuid-2", isMeta: true })) +
      line(assistantEntry({ isSidechain: true }));
    const parsed = parseClaudeSessionJsonl(content);

    assert.lengthOf(parsed.messages, 0);
    assert.isFalse(isEligibleCliSession(parsed));
  });

  it("prefers the last ai-title and falls back to truncated first user text", () => {
    const withTitle =
      line(userEntry()) +
      line({ type: "ai-title", aiTitle: "First title" }) +
      line(assistantEntry()) +
      line({ type: "ai-title", aiTitle: "Login bug fix" });
    assert.equal(parseClaudeSessionJsonl(withTitle).title, "Login bug fix");

    const longText = "x".repeat(80);
    const withoutTitle = line(userEntry({ message: { role: "user", content: longText } }));
    assert.equal(parseClaudeSessionJsonl(withoutTitle).title, "x".repeat(60));
  });

  it("ignores unknown line types and counts malformed lines without throwing", () => {
    const content =
      `not-json\n` +
      line({ type: "file-history-snapshot", snapshot: {} }) +
      line({ type: "some-future-type" }) +
      `[1, 2, 3]\n` +
      line(userEntry()) +
      line(assistantEntry());
    const parsed = parseClaudeSessionJsonl(content);

    assert.lengthOf(parsed.messages, 2);
    assert.equal(parsed.malformedLineCount, 1);
    assert.equal(parsed.lineCount, 6);
  });

  it("does not consume a trailing partial line so incremental reads can resume", () => {
    const content = line(userEntry()) + `{"type":"assistant","uuid":"assist`;
    const parsed = parseClaudeSessionJsonl(content);

    assert.equal(parsed.lineCount, 1);
    assert.equal(parsed.malformedLineCount, 0);
    assert.lengthOf(parsed.messages, 1);
  });

  it("reads what the person typed into a running turn, and not the harness's own notices", () => {
    const queued = (uuid: string, origin: Record<string, unknown>, prompt: string) => ({
      type: "attachment",
      uuid,
      sessionId: SESSION_ID,
      timestamp: "2026-07-01T10:00:03.000Z",
      isSidechain: false,
      attachment: { type: "queued_command", commandMode: "prompt", origin, prompt },
    });
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(queued("queued-1", { kind: "human" }, "also check the logs")) +
        line(
          queued(
            "queued-2",
            { kind: "task-notification", producer: "session-task" },
            "<task-notification>done</task-notification>",
          ),
        ) +
        line(assistantEntry()),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.role, message.text, message.midTurn === true]),
      [
        ["user", "Fix the login bug", false],
        ["user", "also check the logs", true],
        ["assistant", "I found the bug.", false],
      ],
    );
  });

  it("leaves out the bridge mod reloading itself, and what that printed", () => {
    const origin = { kind: "plugin", name: "t3bridge" };
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(assistantEntry()) +
        line(
          userEntry({
            uuid: "reload-1",
            origin,
            message: {
              role: "user",
              content:
                "<command-name>/reload-plugins</command-name>\n<command-message>reload-plugins</command-message>\n<command-args></command-args>",
            },
          }),
        ) +
        line({
          type: "system",
          subtype: "local_command",
          uuid: "reload-2",
          sessionId: SESSION_ID,
          timestamp: "2026-07-01T10:00:06.000Z",
          isSidechain: false,
          content: "<local-command-stdout>Reloaded: 1 plugin</local-command-stdout>",
        }) +
        line(
          userEntry({
            uuid: "rename-1",
            message: {
              role: "user",
              content:
                "<command-name>/rename</command-name>\n<command-message>rename</command-message>\n<command-args>kept</command-args>",
            },
          }),
        ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => message.text),
      ["Fix the login bug", "I found the bug.", "/rename kept"],
    );
  });

  it("shows a ! command and what it printed, without the terminal's styling codes", () => {
    const escape = String.fromCharCode(27);
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(assistantEntry()) +
        line(
          userEntry({
            uuid: "bash-1",
            // As the bridge mod writes it for a command sent from T3.
            isMeta: true,
            origin: { kind: "plugin", name: "t3bridge" },
            message: { role: "user", content: "<bash-input>echo hi</bash-input>" },
          }),
        ) +
        line(
          userEntry({
            uuid: "bash-2",
            message: {
              role: "user",
              content: `<bash-stdout>${escape}[32mhi${escape}[0m\n</bash-stdout><bash-stderr></bash-stderr>`,
            },
          }),
        ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.role, message.text]),
      [
        ["user", "Fix the login bug"],
        ["assistant", "I found the bug."],
        ["user", "! echo hi"],
        ["assistant", "hi"],
      ],
    );
  });

  it("shows /model sent from T3 as the command and what it printed, not the note for the model", () => {
    // As the bridge mod writes them: Claude Code writes none for a model set
    // through the settings row.
    const fromMod = { isMeta: true, origin: { kind: "plugin", name: "t3bridge" } };
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(assistantEntry()) +
        line(
          userEntry({
            uuid: "model-0",
            ...fromMod,
            message: {
              role: "user",
              content:
                "<local-command-caveat>The command below was run directly in Claude Code.</local-command-caveat>",
            },
          }),
        ) +
        line(
          userEntry({
            uuid: "model-1",
            ...fromMod,
            message: {
              role: "user",
              content:
                "<command-name>/model</command-name>\n            <command-message>model</command-message>\n            <command-args>sonnet</command-args>",
            },
          }),
        ) +
        line(
          userEntry({
            uuid: "model-2",
            ...fromMod,
            message: {
              role: "user",
              content:
                "<local-command-stdout>Set model to claude-sonnet-5-5</local-command-stdout>",
            },
          }),
        ) +
        // Any other row the mod or the harness hides stays hidden.
        line(
          userEntry({
            uuid: "hidden-1",
            ...fromMod,
            message: { role: "user", content: "a note only the model reads" },
          }),
        ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.role, message.text]),
      [
        ["user", "Fix the login bug"],
        ["assistant", "I found the bug."],
        ["user", "/model sonnet"],
        ["assistant", "Set model to claude-sonnet-5-5"],
      ],
    );
  });

  it("shows a command once when the record keeps both the typed line and the command's row", () => {
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(assistantEntry()) +
        line(
          userEntry({
            uuid: "typed-compact",
            timestamp: "2026-07-01T10:05:00.000Z",
            message: { role: "user", content: "/compact" },
          }),
        ) +
        line(
          userEntry({
            uuid: "command-compact",
            timestamp: "2026-07-01T10:05:00.000Z",
            message: {
              role: "user",
              content:
                "<command-name>/compact</command-name>\n<command-message>compact</command-message>\n<command-args></command-args>",
            },
          }),
        ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.text, message.command === true]),
      [
        ["Fix the login bug", false],
        ["I found the bug.", false],
        ["/compact", true],
      ],
    );
  });

  it("is ineligible without at least one user and one assistant message", () => {
    assert.isFalse(isEligibleCliSession(parseClaudeSessionJsonl(line(userEntry()))));
    assert.isFalse(isEligibleCliSession(parseClaudeSessionJsonl(line(assistantEntry()))));
  });

  const taskReport =
    "<task-notification>\n<task-id>b1</task-id>\n<tool-use-id>toolu_1</tool-use-id>\n" +
    "<output-file>/tmp/tasks/b1.output</output-file>\n<status>completed</status>\n" +
    '<summary>Background command "Wait for the build &amp; tests" completed (exit code 0)</summary>\n' +
    "</task-notification>";

  it("reads a background task reporting back as the harness's notice, in the report's words", () => {
    const parsed = parseClaudeSessionJsonl(
      line(userEntry()) +
        line(assistantEntry()) +
        line(
          userEntry({
            uuid: "task-1",
            timestamp: "2026-07-01T10:05:00.000Z",
            origin: { kind: "task-notification" },
            promptSource: "system",
            message: { role: "user", content: taskReport },
          }),
        ) +
        // A record from before prompts were stamped with who wrote them.
        line(
          userEntry({
            uuid: "task-2",
            timestamp: "2026-07-01T10:06:00.000Z",
            message: {
              role: "user",
              content:
                '<task-notification>\n<task-id>m1</task-id>\n<summary>Monitor event: "deploy log"</summary>\n<event>ERROR &gt; disk</event>\n</task-notification>',
            },
          }),
        ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.role, message.notice ?? null]),
      [
        ["user", null],
        ["assistant", null],
        [
          "user",
          {
            summary: 'Background command "Wait for the build & tests" completed (exit code 0)',
            outcome: "completed",
          },
        ],
        ["user", { summary: 'Monitor event: "deploy log"', outcome: "updated" }],
      ],
    );
    // The row's own text stays: a thread's live copy of it is told by it.
    assert.strictEqual(parsed.messages[2]!.text, taskReport);
  });

  it("leaves a line the person typed as theirs, whatever it opens with", () => {
    const parsed = parseClaudeSessionJsonl(
      line(
        userEntry({
          origin: { kind: "human" },
          promptSource: "typed",
          message: { role: "user", content: "<task-notification> is what the harness writes?" },
        }),
      ),
    );
    assert.isUndefined(parsed.messages[0]!.notice);
  });

  it("words a report by how the task ended, and one with no summary by its first sentence", () => {
    assert.deepEqual(
      claudeTaskNotice(
        '<task-notification>\n<status>failed</status>\n<summary>Agent "Stage 2" failed: API error</summary>\n</task-notification>',
      ),
      { summary: 'Agent "Stage 2" failed: API error', outcome: "failed" },
    );
    assert.strictEqual(
      claudeTaskNotice(
        "<task-notification>\n<status>killed</status>\n<summary>Background command was stopped</summary>\n</task-notification>",
      ).outcome,
      "cancelled",
    );
    assert.deepEqual(
      claudeTaskNotice(
        "<task-notification>\n<fork-source>\nThis session began as a fork (copy) of another session that is still running: a session whose self-reported name is 'trusted-9f'. The conversation up to 2026-10-11T02:50:44.043Z is shared history with it.\n</fork-source>\n</task-notification>",
      ),
      {
        summary:
          "This session began as a fork (copy) of another session that is still running: a session whose self-reported name is 'trusted-9f'.",
        outcome: "updated",
      },
    );
    assert.strictEqual(
      claudeTaskNotice("<task-notification></task-notification>").summary,
      "Background activity updated",
    );
  });

  it("never titles a session by a task's report, and reads the permission mode the record names", () => {
    const parsed = parseClaudeSessionJsonl(
      line({ type: "permission-mode", permissionMode: "default", sessionId: SESSION_ID }) +
        line(
          userEntry({
            uuid: "task-1",
            origin: { kind: "task-notification" },
            message: { role: "user", content: taskReport },
          }),
        ) +
        line({ type: "permission-mode", permissionMode: "acceptEdits", sessionId: SESSION_ID }) +
        line(
          userEntry({
            uuid: "user-2",
            timestamp: "2026-07-01T10:01:00.000Z",
            message: { role: "user", content: "Fix the login bug" },
          }),
        ),
    );
    assert.strictEqual(parsed.title, "Fix the login bug");
    assert.strictEqual(parsed.permissionMode, "acceptEdits");
  });

  it("shows a project command that starts a turn as the line the person typed", () => {
    const parsed = parseClaudeSessionJsonl(
      line(
        userEntry({
          origin: { kind: "human" },
          message: {
            role: "user",
            content:
              "<command-message>d7-echo</command-message>\n<command-name>/d7-echo</command-name>\n<command-args>kiwi 91dbf</command-args>",
          },
        }),
      ),
    );
    assert.deepEqual(
      parsed.messages.map((message) => [message.text, message.command === true]),
      [["/d7-echo kiwi 91dbf", true]],
    );
  });
});
