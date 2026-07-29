import { assert, describe, it } from "@effect/vitest";

import { parseClaudeSessionJsonl } from "./claudeSessionParser.ts";
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

  it("is ineligible without at least one user and one assistant message", () => {
    assert.isFalse(isEligibleCliSession(parseClaudeSessionJsonl(line(userEntry()))));
    assert.isFalse(isEligibleCliSession(parseClaudeSessionJsonl(line(assistantEntry()))));
  });
});
