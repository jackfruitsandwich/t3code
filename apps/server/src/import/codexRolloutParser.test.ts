import { assert, describe, it } from "@effect/vitest";

import { parseCodexRolloutJsonl } from "./codexRolloutParser.ts";
import { isEligibleCliSession } from "./sessionImportTypes.ts";

const SESSION_ID = "22222222-2222-4222-8222-222222222222";

function line(entry: Record<string, unknown>): string {
  return `${JSON.stringify(entry)}\n`;
}

const sessionMeta = (payload: Record<string, unknown> = {}) =>
  line({
    timestamp: "2026-07-02T08:00:00.000Z",
    type: "session_meta",
    payload: {
      id: SESSION_ID,
      cwd: "/repo/codex-project",
      timestamp: "2026-07-02T08:00:00.000Z",
      ...payload,
    },
  });

const userMessage = (text: string, timestamp = "2026-07-02T08:00:01.000Z") =>
  line({
    timestamp,
    type: "event_msg",
    payload: { type: "user_message", message: text },
  });

const agentMessage = (text: string, timestamp = "2026-07-02T08:00:09.000Z") =>
  line({
    timestamp,
    type: "event_msg",
    payload: { type: "agent_message", message: text },
  });

const completedMessage = (type: "UserMessage" | "AgentMessage", text: string, timestamp: string) =>
  line({
    timestamp,
    type: "event_msg",
    payload: {
      type: "item_completed",
      item: { type, content: [{ type: "text", text }] },
    },
  });

describe("parseCodexRolloutJsonl", () => {
  it("reads session meta and user/agent messages with line timestamps", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        line({ timestamp: "t", type: "turn_context", payload: {} }) +
        userMessage("Refactor the parser") +
        line({ timestamp: "t", type: "response_item", payload: { type: "reasoning" } }) +
        agentMessage("Done, parser refactored.") +
        line({ timestamp: "t", type: "token_count", payload: { total: 5 } }),
    );

    assert.equal(parsed.sessionId, SESSION_ID);
    assert.equal(parsed.cwd, "/repo/codex-project");
    assert.equal(parsed.startedAt, "2026-07-02T08:00:00.000Z");
    assert.deepEqual(
      parsed.messages.map((message) => ({
        role: message.role,
        text: message.text,
        timestamp: message.timestamp,
        lineIndex: message.lineIndex,
      })),
      [
        {
          role: "user",
          text: "Refactor the parser",
          timestamp: "2026-07-02T08:00:01.000Z",
          lineIndex: 2,
        },
        {
          role: "assistant",
          text: "Done, parser refactored.",
          timestamp: "2026-07-02T08:00:09.000Z",
          lineIndex: 4,
        },
      ],
    );
    assert.isTrue(isEligibleCliSession(parsed));
  });

  it("accepts session_id as the meta id field and truncates the title", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta({ id: undefined, session_id: SESSION_ID }) +
        userMessage("y".repeat(90)) +
        agentMessage("ok"),
    );

    assert.equal(parsed.sessionId, SESSION_ID);
    assert.equal(parsed.title, "y".repeat(60));
  });

  it("reads Codex Desktop completed message items", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta({ source: "vscode", originator: "Codex Desktop" }) +
        completedMessage("UserMessage", "Import this session", "2026-07-02T08:00:01.000Z") +
        completedMessage("AgentMessage", "Imported.", "2026-07-02T08:00:09.000Z"),
    );

    assert.deepEqual(
      parsed.messages.map(({ role, text }) => ({ role, text })),
      [
        { role: "user", text: "Import this session" },
        { role: "assistant", text: "Imported." },
      ],
    );
    assert.isTrue(isEligibleCliSession(parsed));
  });

  it("never throws on malformed lines and does not consume a partial tail", () => {
    const parsed = parseCodexRolloutJsonl(
      sessionMeta() +
        `garbage line\n` +
        userMessage("hello") +
        agentMessage("hi") +
        `{"timestamp":"2026-07-02T08:00:10.000Z","type":"event_msg","payl`,
    );

    assert.equal(parsed.malformedLineCount, 1);
    assert.equal(parsed.lineCount, 4);
    assert.lengthOf(parsed.messages, 2);
  });

  it("is ineligible without both a user and an agent message", () => {
    assert.isFalse(isEligibleCliSession(parseCodexRolloutJsonl(sessionMeta() + userMessage("x"))));
    assert.isFalse(isEligibleCliSession(parseCodexRolloutJsonl(sessionMeta() + agentMessage("x"))));
  });
});
